#![cfg(os = "linux")]

use libc::{c_int, posix_fadvise, POSIX_FADV_DONTNEED};
use std::fs::File;
use std::os::unix::io::AsRawFd;

pub fn dontneed(f: File) {
    unsafe {
        posix_fadvise(f.as_raw_fd(), 0, 0, POSIX_FADV_DONTNEED);
        // Swallow file since it should not be used anyway ;-)
        // Swallow return code since we wouldn't bail out on error anyway
    }
}
