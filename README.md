backy-extract
=============

High-performance, standalone backup restore tool for
[backy](https://bitbucket.org/flyingcircus/backy). backy-extract restores a
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
LIBRABY_PATH=/path/to/liblzo2`.

A Makefile is supplied to create a statically linked release which should run on
virtually every Linux x86_64 system.


Hacking
-------

Please create issues and submit pull requests at
https://github.com/ckauhaus/backy-extract/.


Author
------

Contact [Christian Kauhaus](kc@flyingcircus.io) for questions, suggestions, and
bug fixes.
