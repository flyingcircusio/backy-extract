==========
backy-fuse
==========

------------------------------------------------------
export backy images via filesystem in userspace (FUSE)
------------------------------------------------------

:Author: Christian Kauhaus <christian@kauhaus.de>
:Version: @version@
:Manual section: 1
:Manual group: User commands


SYNOPSIS
========

**backy-fuse** [**-d** *BACKUPDIR*] *MOUNTPOINT*


DESCRIPTION
===========

Provide access to all backy backup images of a source via a virtual filesystem.
They can be loop-mounted to retrieve individual files from backups. Each
revision found will be exported as individual image. Images are made accessible
in read-write mode, but modifications are never written to the underlying chunk
store.

See **Examples** below for a restore walk-through.


OPTIONS
=======

**-d** *BACKUPDIR*, **--basedir** *BACKUPDIR*
    Backy data directory containing `*.rev` files.

**-o** *MOUNTOPTS*, **--mountopts** *MOUNTOPTS*
    Additional options to pass to the underlying **mount(8)** invocation.
    Defaults to **allow_root** which lifts the restriction that the superuser is
    normally unable to access FUSE filesystems. See **fuse(8)** for a list of
    allowed options.

**-V**, **--version**
    Show version.

**-h**, **--help**
    Show brief or detailed help screen.


EXIT STATUS
===========

Please note that backy-fuse runs as long as the filesystem is mounted. Use
**fusermount -u** *MOUNTPOINT* to unmount the filesystem recovery has been
finished.

0
    FUSE filesystem has been mounted and unmounted successfully.
1
    Backup directory could not be locked or revision list could not be initialized.

backy-fuse does not exit on I/O error but reports them through the mounted
filesystem.


ENVIRONMENT
===========

RUST_LOG
    Enable additional logging to stderr. Set to **info** or **debug** to
    increase level of verbosity.


FILES
=====

/etc/fuse.conf
    Must contain **user_allow_other** so that the FUSE filesystem can be mounted
    with the default **allow_root** option. Invoke backy-fuse with **-o ""** if
    that setting is not available.


EXAMPLE
=======

1. Explore revisions
--------------------

Mount backup images to `/mnt/backy`::

    $ backy-fuse -d /srv/backy/testvm /mnt/backy

(backy-fuse keeps running)

Then explore or copy images from a second terminal::

    $ ls -l /mnt/backy

Don't forget to unmount the FUSE filesystem when finished::

    $ fusermount -u /mnt/backy


2. Restore files
----------------

Mount backup images::

    $ backy-fuse -d /srv/backy/testvm /mnt/backy-fuse

Select revision to restore from. In this example, we will be restoring from
revision `tAGKE5rrxReggVMtoPSr7`.

Set up loop device to access partitions inside the image::

    # losetup -f -P --show /mnt/backy-fuse/tAGKE5rrxReggVMtoPSr7
    /dev/loop0

Loop mount the image::

    # mount -oro /dev/loop0p1 /mnt/restore

If the image does not contain a partition table, use `/dev/loop0` instead of
`/dev/loop0p1` for example.

After finishing restore operations, unmount the loop mount first::

    # umount /mnt/restore

Then unregister the loop device::

    # losetup -d /dev/loop0

Finally, unmount the FUSE filesystem::

    $ fusermount -u /mnt/backy-fuse


NOTES
=====

backy-fuse employs a two-tier cache scheme. Chunks are kept in a fixed-size read
only cache (256 MiB large by default). In case backup images are written to,
dirty pages are copied into an unbounded page cache which is kept in memory as
long as backy-fuse is running. So avoid read-write mounts of backup images.


SEE ALSO
========

fuse(8), mount(8), fusermount(1)

https://github.com/flyingcircusio/backy
