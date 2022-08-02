use crate::slurm;
use derive_more::{Display, From};
use log::error;
use std::error::Error;
use std::ffi::OsStr;
use std::fmt::{Display, Formatter};
use std::path::{Path, PathBuf};
use std::{fs, io};

#[derive(Debug)]
pub struct FileIoError {
    trace: String,
    path: PathBuf,
    error: Box<dyn Error + 'static>,
}

impl Display for FileIoError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}: {}", self.trace, self.path.display(), self.error)
    }
}

impl Error for FileIoError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.error.as_ref())
    }
}

impl FileIoError {
    pub fn new(
        trace: impl Into<String>,
        path: impl Into<PathBuf>,
        error: impl Into<Box<dyn Error + 'static>>,
    ) -> Self {
        Self {
            trace: trace.into(),
            path: path.into(),
            error: error.into(),
        }
    }
}

pub struct LockFile {
    path: PathBuf,
}

#[derive(Debug, Display, From)]
pub enum LockError {
    #[display(fmt = "Lock file occupied by job {}", _0)]
    Occupied(slurm::JobId),
    #[display(fmt = "{}", _0)]
    #[from]
    FileIo(FileIoError),
}

impl Error for LockError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            LockError::Occupied(_) => None,
            LockError::FileIo(e) => Some(e),
        }
    }
}

fn add_extension(path: PathBuf, extension: impl AsRef<OsStr>) -> PathBuf {
    let mut string = path.into_os_string();
    string.push(extension);
    PathBuf::from(string)
}

impl LockFile {
    fn try_read(path: &Path) -> Result<Option<slurm::JobId>, FileIoError> {
        use std::io::Read;

        match fs::OpenOptions::new().read(true).open(&path) {
            Ok(mut lock) => {
                let mut buf = Vec::new();
                lock.read_to_end(&mut buf)
                    .map_err(|e| FileIoError::new("reading existing lock file", path, e))?;
                let contents = String::from_utf8(buf)
                    .map_err(|e| FileIoError::new("parsing lock file", path, e))?;
                let occupant = contents
                    .trim()
                    .parse()
                    .map_err(|e| FileIoError::new("parsing lock file", path, e))?;
                Ok(Some(occupant))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(FileIoError::new("opening existing lock file", path, e)),
        }
    }

    pub fn create(path: &Path, job: slurm::JobId) -> Result<LockFile, LockError> {
        use std::io::Write;

        // loop around remove <-> read race
        loop {
            // first, try creating the new lock file exclusively
            match fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
            {
                Ok(mut file) => match write!(file, "{}", job.0) {
                    Ok(_) => {
                        return Ok(LockFile {
                            path: path.to_owned(),
                        })
                    }
                    Err(e) => {
                        drop(file); // close the file so we can remove it
                        let _ = fs::remove_file(&path); // swallow the second error
                        return Err(FileIoError::new("writing to lock file", path, e).into());
                    }
                },
                Err(e) => {
                    if e.kind() != io::ErrorKind::AlreadyExists {
                        return Err(FileIoError::new("creating lock file", path, e).into());
                    } else {
                        // continue trying to read the existing lock file
                    }
                }
            }

            // otherwise, read the existing file and identify the occupying job
            match Self::try_read(&path) {
                Ok(Some(occupant)) => return Err(LockError::Occupied(occupant)),
                Ok(None) => (), // race: the file was deleted since our creation attempt, try again
                Err(e) => return Err(e.into()),
            };
        }
    }

    pub fn create_or_adopt(
        path: &Path,
        job: slurm::JobId,
        active_jobs: &[slurm::JobId],
    ) -> Result<LockFile, LockError> {
        loop {
            match Self::create(path, job) {
                // If a lock file is present but the corresponding job has terminated, we can safely
                // remove the lock file and replace it with our own.
                Err(LockError::Occupied(occupant))
                    if slurm::job_has_terminated(occupant, active_jobs) =>
                {
                    // Potential race condition: a sibling process could adopt the existing lock
                    // file right before our call to `fs::remove_file`, which would cause us to
                    // silently  remove the sibling's active lock. Since we don't have a
                    // filesystem-level compare-exchange primitive, we recursively create or adopt
                    // an additional lock for replacing the current lock file.
                    let adopting_path = add_extension(path.to_owned(), ".adopting");
                    let _adopting_lock = Self::create_or_adopt(&adopting_path, job, active_jobs)?;

                    // We have successfully acquired the .adopting lock, but a sibling process might
                    // still have completed adoption between us reading the existing lock file and
                    // taking the .adopting lock. Re-read the existing lock file to detect this.
                    match Self::try_read(path)? {
                        Some(new_occupant) if new_occupant == occupant => {
                            // We can safely assume that we are the only ones trying to re-create
                            // the lock file.
                            fs::remove_file(path).map_err(|e| {
                                FileIoError::new("removing stale lock file", path, e)
                            })?;
                        }
                        _ => {
                            // A sibling process raced us and adopted the lock (the Some(x) case
                            // with a new x) or adopted and released the lock (the None case) before
                            // we were able to acquire it. Treat this like a successful removal
                        }
                    }
                }
                // 1. unoccupied case, or
                // 2. occupied case where the occupant is still active, or
                // 3. FileIoError case
                other => return other,
            }
        }
    }
}

impl Drop for LockFile {
    fn drop(&mut self) {
        if let Err(e) = fs::remove_file(&self.path) {
            error!("removing lock file {p}: {e}", p = self.path.display());
        }
    }
}

pub struct WorkDir {
    pub path: PathBuf,
    _lock: LockFile,
}

impl WorkDir {
    pub fn lock(
        base_dir: &Path,
        job: slurm::JobId,
        active_jobs: &Vec<slurm::JobId>,
    ) -> Result<WorkDir, FileIoError> {
        for concurrent_id in 0u32.. {
            let path = base_dir.join(PathBuf::from(concurrent_id.to_string()));
            if let Err(e) = fs::create_dir_all(&path) {
                return Err(FileIoError::new("creating working directory", path, e));
            }

            match LockFile::create_or_adopt(&path.join(".lock"), job, active_jobs) {
                Ok(lock) => return Ok(WorkDir { path, _lock: lock }),
                Err(LockError::Occupied(_)) => (), // retry with the next concurrent_id
                Err(LockError::FileIo(e)) => return Err(e),
            }
        }
        unreachable!(); // not technically, but would require 2^32 existing locked work dirs
    }
}

#[test]
fn test_immediate_lock_simple() {
    let dir = tempfile::tempdir().unwrap();
    let lock_path = dir.path().join(".lock");

    let job = slurm::JobId(42);

    assert_eq!(LockFile::try_read(&lock_path).unwrap(), None);

    let lock = LockFile::create(&lock_path, job).unwrap();
    assert_eq!(LockFile::try_read(&lock_path).unwrap(), Some(job));

    drop(lock);
    assert_eq!(LockFile::try_read(&lock_path).unwrap(), None);
}

#[cfg(test)]
fn assert_occupied(result: Result<LockFile, LockError>, concurrent_job: slurm::JobId) {
    match result {
        Ok(_) => panic!("lock creation must fail"),
        Err(LockError::FileIo(e)) => panic!("{}", e),
        Err(LockError::Occupied(occupant)) => assert_eq!(occupant, concurrent_job),
    }
}

#[test]
fn test_immediate_lock_occupied() {
    let dir = tempfile::tempdir().unwrap();
    let lock_path = dir.path().join(".lock");

    let concurrent_job = slurm::JobId(10);
    fs::write(&lock_path, format!("{}", concurrent_job.0).as_bytes()).unwrap();
    assert_eq!(
        LockFile::try_read(&lock_path).unwrap(),
        Some(concurrent_job)
    );

    let job = slurm::JobId(42);
    assert_occupied(LockFile::create(&lock_path, job), concurrent_job);

    assert_eq!(
        LockFile::try_read(&lock_path).unwrap(),
        Some(concurrent_job)
    );
}

#[test]
fn test_recursive_lock_simple() {
    let dir = tempfile::tempdir().unwrap();
    let lock_path = dir.path().join(".lock");

    let job = slurm::JobId(42);
    let active_jobs = [job];

    assert_eq!(LockFile::try_read(&lock_path).unwrap(), None);

    let lock = LockFile::create_or_adopt(&lock_path, job, &active_jobs).unwrap();
    assert_eq!(LockFile::try_read(&lock_path).unwrap(), Some(job));

    drop(lock);
    assert_eq!(LockFile::try_read(&lock_path).unwrap(), None);
}

#[test]
fn test_recursive_lock_occupied_by_active_job() {
    let dir = tempfile::tempdir().unwrap();
    let lock_path = dir.path().join(".lock");

    let job = slurm::JobId(42);
    let concurrent_job = slurm::JobId(25);
    let active_jobs = [job, concurrent_job];

    fs::write(&lock_path, format!("{}", concurrent_job.0).as_bytes()).unwrap();
    assert_eq!(LockFile::try_read(&lock_path).unwrap(), Some(concurrent_job));

    assert_occupied(LockFile::create_or_adopt(&lock_path, job, &active_jobs), concurrent_job);
    assert_eq!(LockFile::try_read(&lock_path).unwrap(), Some(concurrent_job));
    assert_eq!(LockFile::try_read(&add_extension(lock_path.clone(), ".adopting")).unwrap(), None);
}

#[test]
fn test_recursive_lock_occupied_by_newer_job() {
    let dir = tempfile::tempdir().unwrap();
    let lock_path = dir.path().join(".lock");
    let adopting_lock_path = add_extension(lock_path.clone(), ".adopting");

    let job = slurm::JobId(42);
    let concurrent_job = slurm::JobId(55);
    let active_jobs = [job]; // see documentation in slurm::has_job_terminated

    fs::write(&lock_path, format!("{}", concurrent_job.0).as_bytes()).unwrap();
    assert_eq!(LockFile::try_read(&lock_path).unwrap(), Some(concurrent_job));

    assert_occupied(LockFile::create_or_adopt(&lock_path, job, &active_jobs), concurrent_job);
    assert_eq!(LockFile::try_read(&lock_path).unwrap(), Some(concurrent_job));
    assert_eq!(LockFile::try_read(&adopting_lock_path).unwrap(), None);
}

#[test]
fn test_recursive_lock_adopt() {
    let dir = tempfile::tempdir().unwrap();
    let lock_path = dir.path().join(".lock");
    let adopting_lock_path = add_extension(lock_path.clone(), ".adopting");

    let job = slurm::JobId(42);
    let active_jobs = [job];

    let stale_job = slurm::JobId(10);
    fs::write(&lock_path, format!("{}", stale_job.0).as_bytes()).unwrap();
    assert_eq!(LockFile::try_read(&lock_path).unwrap(), Some(stale_job));
    assert_eq!(LockFile::try_read(&adopting_lock_path).unwrap(), None);

    let lock = LockFile::create_or_adopt(&lock_path, job, &active_jobs).unwrap();
    assert_eq!(LockFile::try_read(&lock_path).unwrap(), Some(job));
    assert_eq!(LockFile::try_read(&adopting_lock_path).unwrap(), None);

    drop(lock);
    assert_eq!(LockFile::try_read(&lock_path).unwrap(), None);
}

#[test]
fn test_recursive_lock_adopt_occupied_by_active_job() {
    let dir = tempfile::tempdir().unwrap();
    let lock_path = dir.path().join(".lock");
    let adopting_lock_path = add_extension(lock_path.clone(), ".adopting");

    let job = slurm::JobId(42);
    let stale_job = slurm::JobId(13);
    let concurrent_job = slurm::JobId(25);
    let active_jobs = [job, concurrent_job];

    fs::write(&lock_path, format!("{}", stale_job.0).as_bytes()).unwrap();
    fs::write(&adopting_lock_path, format!("{}", concurrent_job.0).as_bytes()).unwrap();
    assert_eq!(LockFile::try_read(&lock_path).unwrap(), Some(stale_job));
    assert_eq!(LockFile::try_read(&adopting_lock_path).unwrap(), Some(concurrent_job));

    // The occupant must be reported as the adopting concurrent job
    assert_occupied(LockFile::create_or_adopt(&lock_path, job, &active_jobs), concurrent_job);

    // The "concurrent" job does not actually do anything, so the adopting lock will still be there
    assert_eq!(LockFile::try_read(&lock_path).unwrap(), Some(stale_job));
    assert_eq!(LockFile::try_read(&adopting_lock_path).unwrap(), Some(concurrent_job));
}

#[test]
fn test_recursive_lock_adopt_twice() {
    let dir = tempfile::tempdir().unwrap();
    let lock_path = dir.path().join(".lock");
    let adopting_lock_path = add_extension(lock_path.clone(), ".adopting");
    let adopting_twice_lock_path = add_extension(adopting_lock_path.clone(), ".adopting");

    let job = slurm::JobId(42);
    let active_jobs = [job];

    let stale_job_1 = slurm::JobId(10);
    let stale_job_2 = slurm::JobId(19);
    fs::write(&lock_path, format!("{}", stale_job_1.0).as_bytes()).unwrap();
    fs::write(&adopting_lock_path, format!("{}", stale_job_2.0).as_bytes()).unwrap();
    assert_eq!(LockFile::try_read(&lock_path).unwrap(), Some(stale_job_1));
    assert_eq!(
        LockFile::try_read(&adopting_lock_path).unwrap(),
        Some(stale_job_2)
    );
    assert_eq!(LockFile::try_read(&adopting_twice_lock_path).unwrap(), None);

    let lock = LockFile::create_or_adopt(&lock_path, job, &active_jobs).unwrap();
    assert_eq!(LockFile::try_read(&lock_path).unwrap(), Some(job));
    assert_eq!(LockFile::try_read(&adopting_lock_path).unwrap(), None);
    assert_eq!(LockFile::try_read(&adopting_twice_lock_path).unwrap(), None);

    drop(lock);
    assert_eq!(LockFile::try_read(&lock_path).unwrap(), None);
}
