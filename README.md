MarFS provides a scalable near-POSIX file system by using one or more POSIX file systems as a scalable metadata component and one or more data stores (object, file, etc) as a scalable data component.

Our default implementation uses GPFS file systems as the metadata component and multiple ZFS instances, exported over NFS, as the data component.

The MetaData Abstraction Layer and Data Abstraction Layer (MDAL/DAL) provide a modular way to introduce alternative implementations (metadata and data implementations, respectively). The DAL is provided by LibNE ( hosted in the erasureUtils Repo: "https://github.com/mar-file-system/erasureUtils" ), which also adds the ability to use our own erasure + checksumming implementation, utilizing Intel's Storage Acceleration Library ( "https://github.com/intel/isa-l" ).

There is extensive documentation in the Documents directory, including theory and an install guide.

The following additional repos are required to build MarFS:
  - Intel's Storage Acceleration Library
    - Required to build Erasure Utilities
    - Location : "https://github.com/intel/isa-l"
  - MarFS Erasure Utilities
    - Required for the MarFS data path to function
    - Location : "https://github.com/mar-file-system/erasureUtils"

The following additional repos are recommended:
  - Pftool ( Parallel File Tool )
    - A highly performant data transfer utilitiy with special MarFS integration
    - Location : "https://github.com/pftool/pftool"

