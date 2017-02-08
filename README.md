# marfs
MarFS provides a scalable near-POSIX file system by using one or more POSIX file systems as a scalable metadata component and one or more data stores (object, file, etc) as a scalable data component. Our initial implementation will use GPFS file systems as the metadata component and Scality and EMC ECS ViPR object stores as the data component.
There is extensive documentation in the Documents directory, including theory and an install-guide.
