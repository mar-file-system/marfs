# marfs
MarFS provides a scalable near-POSIX file system by using one or more POSIX file systems as a scalable metadata component and one or more data stores (object, file, etc.) as a scalable data component.

Our initial implementation used GPFS file systems as the metadata component and a Scality object store as the data component.

Our next implementation adds the ability to use our own erasure implementation over underlying RAID-Z3 ZFS pools as the data component.

There is extensive documentation in the Documents directory, including theory and an install guide.
