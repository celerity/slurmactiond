function humanDuration(ms) {
    if (ms < 1000) return "now";
    let sec = Math.round(ms / 1000);
    if (sec === 1) return "1 second ago";
    if (sec < 60) return sec + " seconds ago";
    let min = Math.round(ms / (60 * 1000));
    if (min === 1) return "1 minute ago";
    if (min < 60) return min + " minutes ago";
    let hours = Math.round(ms / (60 * 60 * 1000));
    if (hours === 1) return "1 hour ago";
    if (hours < 60) return hours + " hours ago";
    let days = Math.round(ms / (24 * 60 * 60 * 1000));
    if (days === 1) return "1 day ago";
    return days + " days ago";
}

let now = new Date();
document.querySelectorAll("#active-jobs .time, #active-runners .time").forEach(e => {
    let when = new Date(e.getAttribute("data-when"));
    e.setAttribute("title", when.toString());
    e.textContent = " since " + humanDuration(now - when)
});
document.querySelectorAll("#job-history .time, #runner-history .time").forEach(e => {
    let when = new Date(e.getAttribute("data-when"));
    e.setAttribute("title", when.toString());
    e.textContent = humanDuration(now - when)
});
