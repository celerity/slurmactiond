mod actions;
mod restapi;
mod slurm;

fn main() -> std::io::Result<()> {
    println!("Hello, world!");
    restapi::main()
}
