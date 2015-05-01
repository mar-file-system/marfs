#!/bin/bash
#
# mounts gpfs on an unshared directory and then puts mhddfs on top of that

# Arguments
# first argument:  Script to be unshared

# NOTE:  the following needs to be ran once
# mount --make-rshared /     # remove the ability for unshared directories to
# exist.
# mount --rbind /gpfs /gpfs  # bind /gpfs to /gpfs, recursively. This might seem
# silly.
# mount --make-private /gpfs # allow unshare to work under /gpfs

# Now, create an unshared process that can make its own mount points.
# Then, have that process mount the GPFS and export it using MHDDFS
# NOTE: right now, this script works for our setup. You will need to change the
# following arguments for your setup.

unshare -m $1 g1 /gpfs/g1/dir1,/gpfs/g1/dir2 /tmp/mountpoint

