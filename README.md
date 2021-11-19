backy-extract
=============

High-performance, standalone backup restore tool for
[backy](https://github.com/flyingcircusio/backy). backy-extract restores a
specified backup revision to stdout or a file/block device target.


Usage
-----

1. Pick a backup revision, for example using `backy status`.
2. Create a restore target, for example with `lvm` or `rbd image`.
3. Extract backup: `backy-extract /srv/backy/vm/Nym6uacWoXGb8VnbksM3yH /dev/rbd0`


Interaction with backy
----------------------

`backy-extract` tries to acquire the *purge lock* before proceeding. So if it
does not seem to start, check if there are running `backy` processes operating
on the same backup.


Sparse mode
-----------

Block devices are assumed to be zeroed (discarded) before restoring. If this is
not the case, invoke `backy-extract` with `--sparse=never`.


Compiling
---------

In general, `cargo build --release` should do the right thing. This application
depends on [liblzo2](http://www.oberhumer.com/opensource/lzo/) being availabe.
If you get compiler/linker errors, try compiling again with `export
LIBRARY_PATH=/path/to/liblzo2`.

A Makefile is supplied to create a statically linked release which should run on
virtually every Linux x86_64 system.


FUSE driver (backy-fuse)
========================

`backy-fuse` provides access to individual revisions via FUSE (filesystem in
userspace). This allows to mount backup images and retrieve single files.

Usage
-----

1. Start `backy-fuse` to access all backups of a given VM via FUSE:
   `backy-fuse -d /srv/backy/vm /mnt/backy-fuse`
2. Mount the image of interest:
   `mount -o ro,loop /mnt/backy-fuse/tAGKE5rrxReggVMtoPSr7 /mnt/restore`

When finished, the above stops must be reversed:

1. Unmount loop device:
   `umount /mnt/restore`
2. Finish FUSE:
   `fusermount -u /mnt/backy-fuse`

Caching
-------

`backy-fuse` maintains two caches: A read-only cache to speed up read operations
and a dirty cache to store pages which have been written to. Both caches have
the same size, which may be specified with the `-c` flag. Note that chunk will
spill into the backy directory if the dirty cache gets too full. This is
generally not a problem because the next `backy purge` run will clean it up.
However, it is strongly recommended to mount FUSE volumes with the **ro** flag.

Compiling
---------

`backy-fuse` is not compiled by default due to restricted portability. To
compile it, run `cargo build --release --features fuse_driver`. Note that both
liblzo2 and libfuse must be detectable by the linker.


Hacking
=======

Please create issues and submit pull requests at
https://github.com/flyingcircusio/backy-extract/.


Author
======

Contact [Christian Kauhaus](mailto:christian@kauhaus.de) for questions,
suggestions, and bug fixes.
