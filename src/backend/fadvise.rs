#[cfg(os = "linux")]
use libc::{c_int, posix_fadvise, POSIX_FADV_DONTNEED};
use std::fs::File;
#[cfg(os = "linux")]
use std::os::unix::io::AsRawFd;

#[cfg(os = "linux")]
pub fn fadvise(f: &File, advise: c_int) {
    unsafe {
        posix_fadvise(f.as_raw_fd(), 0, 0, advise);
        // Swallow return code since we wouldn't bail out on error anyway
    }
}

#[cfg(not(os = "linux"))]
pub fn fadvise(_f: &File, _advise: i32) {}
#[cfg(not(os = "linux"))]
pub const POSIX_FADV_DONTNEED: i32 = 4;
