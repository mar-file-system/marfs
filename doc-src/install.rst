Installing
**********

This section describes the install process for MarFS.

.. contents::
   :depth: 2
   :local:


Overview
========
MarFS is a distributed parallel filesystem, thusly setup is complicated and
relies on a number of different technologies and systems. In this guide we
will go over the setup of an example system. This guide assumes knowledge of
ZFS, GPFS. MPI, and general Linux operations.


Storage
-------
MarFS stores data and metadata differently. Data is stored as erasure-coded
objects. When data is written it is broken up into N number of pieces. Erasure
data is calculated on our N objects to create E number of erasure objects. 
N+E number of objects are then mapped onto N+E identical filesystems. We refer
to these N+E filesystems as a "pod". In our example cluster we will have four
storage nodes in a single pod giving us a 3+1 erasure coding scheme. You can
have multiple pods in a cluster. When you have multiple pods the pod is
selected with a hash, and data is written to that pod. Data will never be
written across multiple pods. So if you have 4 pods each matching our single
pod with a 3+1 scheme those four objects will always be in the same pod.

Metadata can be stores on any filesystem that supports extended attributes and
sparse-files. For scalability purposes a distributed filesystem is highly
recommended. In our example cluster we will use two nodes running General
Parallel Filesystem (GPFS).

Data Access
-----------
With object data being stored across a number of pods it is reasonable to
provide a way to interact with the filesystem in a unified matter. Most users
would expect a single mount point they can look through for various tasks.
This is provided through FUSE, allowing users to look at their data. This
FUSE mount is read only, and is there for users to locate their files for
parallel movement. Nodes with this FUSE mount are referred to as "interactive"
nodes. Interactive nodes are unique in the MarFS cluster, as it is the only
node users will have direct access.

Data Movement
-------------
Data is moved in parallel using PFTool. Nodes running PFTool are called
"File Transfer Agent" nodes, or FTAs.

Example cluster summary
=======================
  * 4 Storage Nodes
  * 2 Metadata Nodes
  * 2 File Transfer Nodes
  * 1 Interactive Node

Storage Nodes
-------------
Each storage node uses ZFS for MarFS block storage. Each node will have four
zpools in a RAIDZ3(17+3) configuration. We have a single pod configured to
use 3+1 erasure coding. Must have high performance network such as Infiniband.

Metadata Nodes
--------------
We will be using GPFS as metadata storage in this example. Your GPFS cluster
should already be setup and ready to create filesets. You can still follow the
example using some other filesystem. Should have high performance network such
as Infiniband when using GPFS. It is important to note that while GPFS is not
required to use MarFS it is required for some MarFS utilities like quota
management and garbage collection.

File Transfer Nodes
-------------------
These nodes will be used to move data in parallel from one place to another.
We will use `PFtool <https://github.com/pftool/pftool>`_ for this.
Must have high performance network such as Infiniband.

Interactive Nodes
-----------------
These nodes will be used to present MarFS to users through a FUSE mount.


MarFS abstractions
==================
Remember how earlier we talked about the pod? There are more things to
understand about the pod. There are logical data abstractions we will see
later when understanding the configuration file. We will talk about them
briefly here first. 

The Repository
--------------
A repo is where all the object data for a MarFS Filesystem lives; it’s a
logical description of a MarFS object-store, with details on the number of
storage servers, etc.

#The repo currently includes configuration details 
#specific to MC-NFS versus MC-RDMA.

Data Abstraction Layer
----------------------
Multi component stuff here maybe?

Metadata Abstraction Layer
--------------------------

The Namespace
-------------
A namespace in MarFS is a logical partition of the MarFS filesystem with a
unique (virtual) mount point and attributes like permissions, similar to ZFS
datasets. It also includes configuration details regarding MarFS metadata
storage for that namespace.  Each namespace in MarFS must be associated with a
repo, and you can have multiple namespaces per repo. Both repos and namespaces
are arbitrarily named.

Pods
----
A collection of storage nodes.

Blocks
------
A storage node in a pod.

Capacity Units
--------------
Each capacity unit (cap) is a datastore on a ZFS zpool on a block in a pod :)

Dependencies
============

Depending on things you may need different things. To install and make use of
MarFS you will need the following tools.

Fortunately many dependencies can be acquired through a package manager.

.. code-block:: bash

   yum install gcc glibc-devel fuse-devel libattr-devel make curl-devel
   curl openssl-devel openssl git libxml2-devel yasm libtool openmpi 
   openmpi-devel

Others can be obtained from source.

.. code-block:: bash

   git clone https://github.com/mar-file-system/marfs.git
   git clone https://github.com/mar-file-system/PA2X.git
   git clone https://github.com/mar-file-system/erasureUtils.git
   git clone https://github.com/mar-file-system/aws4c.git
   git clone https://github.com/pftool/pftool.git
   git clone https://github.com/01org/isa-l.git

A quick description of tools acquired from source::

   MarFS: The core MarFS libraries
   PA2X: An XML parser for parsing the MarFS configuration file
   ErasureUtils: The erasure coding layer used for Multi-Component storage
   Aws4c: C library for AWS, used for S3 and RDMA authentication
   Pftool: A tool for parallel data movement
   ISA-L: Intel’s Intelligent Storage Acceleration Library


Setup Process
=============

You will need yasm 1.2.0 or later for ISA-L.

It is helpful to have a shared filesystem among all the nodes in the cluster,
in this guide we will have a NFS share mounted on all nodes. We will keep all
our source code and other files that must be shared here.

For machines that have Infiniband:
Ensure MPI is in your :code:`$PATH` environment variable.
It may also be required to add OpenMPI's library directory to the
:code:`$LD_LIBRARY_PATH` environment variable.

Your metadata nodes and FTA nodes should all be in a GPFS cluster that is set
up.

Your storage nodes should all have ZFS installed, with your zpools set up.

Our example cluster will have a single pod containing four blocks. Each block
will have four capacity units.
In human terms, we have one set of storage servers comprised of four storage
servers. Each of these storage servers will have four ZFS zpools set up.

Storage Nodes
-------------

MarFS object data is stored in zpools on each storage node. The path to the
objects must match a pattern similar to 
:code:`FTAMountPoint/RepoName/podNum/blockNum/capNum`
examle:
:code:`/zfs/repo3+1/pod0/block0/cap3`
This path corresponds to storage pool number 3 on storage node 0 in pod 0 in
repo "repo3+1".
On storage nodes this path matching is not required. The data can actually be
stored in any arbitrary directory. On FTA nodes that path structure is
required, as the MarFS library is hard coded to use that path. We will be
using the same path on our storage nodes for symmetry between the FTA nodes
and storage nodes. Each storage node will only need the unique path that
corresponds to the capacity units. Hostnames are arbitrary, but can help in
the brain battle of keeping things oraginzed. Our hostnames for storage nodes
will be::

   sn01
   sn02
   sn03
   sn04

We'll start with sn01:

.. code-block:: bash

   [sn01 ~]# zpool list
   NAME             SIZE  ALLOC   FREE  CKPOINT  EXPANDSZ   FRAG    CAP  DEDUP    HEALTH  ALTROOT
   sn01-pool0       146T  12.7M   146T        -         -     0%     0%  1.00x    ONLINE  -
   sn01-pool1       146T  11.0M   146T        -         -     0%     0%  1.00x    ONLINE  -
   sn01-pool2       146T  10.8M   146T        -         -     0%     0%  1.00x    ONLINE  -
   sn01-pool3       146T  11.0M   146T        -         -     0%     0%  1.00x    ONLINE  -


First we want to set the optimal zpool settings on all our zpools.

.. code-block:: bash

   for i in {0..3}; do zfs set recordsize=1M sn01-pool$i; done
   for i in {0..3}; do zfs set mountpoint=none sn01-pool$i; done
   for i in {0..3}; do zfs set compression=lz4 sn01-pool$i; done
   for i in {0..3}; do zfs set atime=off sn01-pool$i; done

We are using a diskless sever for our storage nodes. We need to create a NFS
exported ZFS datastore, with the mountpoint at :code:`/zfs`. This datastore
must be mounted before all the others on reboot because NFS will stat the
mountpoint which is on :code:`tmpfs` in a diskless setup. When it does the
stat the wrong block size will be returned.

.. code-block:: bash

   zfs create sn01-pool0/nfs
   zfs set mountpoint=/zfs sn01-pool0/nfs

We want a datastore on each zpool that will be mounted at a path made with the
above guidelines. The name of the datastore is irrelevant.

.. code-block:: bash

   for i in {0..3}; do zfs create sn002-pool$i/datastore; done

On each storage node we want to make a directory under our /zfs mountpoint
where we will create out special path

.. code-block:: bash

   mkdir /zfs/exports

Now we want to make our :code:`pod/block/cap` directories
under :code:`/zfs/exports`. For sn01 it looks like:

.. code-block:: bash

   mkdir /zfs/exports/repo3+1/pod0/block0/cap0 
   mkdir /zfs/exports/repo3+1/pod0/block0/cap1
   mkdir /zfs/exports/repo3+1/pod0/block0/cap2 
   mkdir /zfs/exports/repo3+1/pod0/block0/cap3

Storage node sn01 is in pod 0, is block 0 of the pod, and will have 4 capacity
units. We will want to create the correct path on every storage node in the
cluster. For sn02 it would look like:

.. code-block:: bash

   mkdir /zfs/exports/repo3+1/pod0/block1/cap0 
   mkdir /zfs/exports/repo3+1/pod0/block1/cap1
   mkdir /zfs/exports/repo3+1/pod0/block1/cap2 
   mkdir /zfs/exports/repo3+1/pod0/block1/cap3

For loops are very helpful for this with minor adjustments on each node.

.. code-block:: bash

   for i in {0..3}; do mkdir -p /zfs/exports/repo3+1/pod0/block3/cap$i

All you need to do is change the pod and block to the correct number for each
storage node. If everything is in sequence you can just wrap that loop in
more loops to handle that with SSH. After we create the directories we need,
we will mount our datastores on each node into the correct folder. on sn01 it
will look like:

.. code-block:: bash

   [sn01 ~]# zfs list
   NAME                       USED  AVAIL     REFER  MOUNTPOINT
   sn01-pool0                 9.29M 113T      307K   none
   sn01-pool0/datastore       5.14M 113T      5.14M  /zfs/exports/repo3+1/pod0/block0/cap0
   sn01-pool0/nfs             332K  113T      332K   /zfs
   sn01-pool1                 8.44M 113T      307K   none
   sn01-pool1/datastore       5.14M 113T      5.14M  /zfs/exports/repo3+1/pod0/block0/cap1
   sn01-pool2                 8.25M 113T      307K   none
   sn01-pool2/datastore       5.14M 113T      5.14M  /zfs/exports/repo3+1/pod0/block0/cap2
   sn01-pool3                 8.40M 113T      307K   none
   sn01-pool3/datastore       5.14M 113T      5.14M  /zfs/exports/repo3+1/pod0/block0/cap3

Once we have our capacity units mounted we must create "scatter" directories
under the mount point for each capacity unit.

.. code-block:: bash

   for c in {0..3}; do
      for s in {0..1024}; do
         mkdir /zfs/exports/repo3+1/pod0/block0/cap$c/scatter$s
      done
   done

The purpose of these directories is just to prevent all objects destined for a
particular capacity-dir from being stored in a single-directory. The specific
scatter-dir used for each object is computed at run-time by a hash. In our
example we will only create 1024 scatter directories, but in bigger systems
you can have many more.

Now we can NFS export out datasets. Edit the file :code:`/etc/exports` to
look like:

.. code-block:: bash

   [sn01 ~]# cat /etc/exports
   /zfs/exports *(rw,fsid=0,no_subtree_check,sync,crossmnt)

*Important*
If you plan on using NFS over RDMA (you should) you will need to change the
export options in :code:`/etc/exports`:

.. code-block:: bash

   [sn01 ~]# cat /etc/exports
   /zfs/exports *(rw,fsid=0,no_root_squash,no_subtree_check,sync,insecure,crossmnt)

NFS over RDMA requires the extra options.

Metadata Nodes
--------------

Phew we made it. Now that the easy part is over we will configure our metadata
nodes with GPFS and get them ready to hold metadata. Just kidding. I made you
do all the hard work for metadata nodes on GPFS way before now.

We have our GPFS filesystem all set up and mounted under :code:`/gpfs`.
Create a directory :code:`mkdir /gpfs/metadata`. We will create a GPFS fileset
for each namespace that we want to create in our config file. In this example
we will have a single namespace. We will link our filesets under the directory
we made. We will create some directories we need under those links.

.. code-block:: bash

   mmcrfileset /dev/marfs namespace_one
   mmlinkfileset /dev/marfs namespace_one -J /gpfs/metadata/namespace_one
   mkdir /gpfs/metadata/namespace_one/mdfs

All MDFS directories should be readable by everyone. We also want to set
ownership of the MDFS directory here. The permissions and ownership of this
directory will be reflected as the permissions in the MarFS mount later. So
:code:`chown` this directory to the right group now if needed.

There is a file MarFS will always look for under the mdfs directory called
:code:`fsinfo`. Lets create that now.

:code:`touch /gpfs/metadata/namespace_one/mdfs/fsinfo`

FTA nodes
---------
Now that we have storage and metadata all up and running we have to unite the
two systems so we can read and write data. FTA nodes will have the capacity
units mounted that we exported earlier, and should have the metadata
filesystem mounted as well. The FTAs in our setup are part of the GPFS cluster
so we should already have /gpfs mounted on these nodes. We need to use the
:code:`pod/block/cap/` directory tree we created earlier on the storage nodes.
We already have our metadata mounted at :code:`/gpfs/metadata` so lets create
a new directory to hold the :code:`pod/block/cap/` structure.

.. code-block:: bash

   mkdir /gpfs/repo_data

   for b in {0..3}; do
      for c in {0..3}; do
         mkdir -p /gpfs/data/repo3+1/pod0/block$b/cap$c; done; done

Behold. Now mount the datastores.

.. code-block:: bash

   mount -o "_netdev,async,rw,nfsvers=3,nolock,wsize=1048576,rsize=1048576,rdma,soft,port=20049" -t nfs \
            sn01:/zfs/exports/repo3+1/pod0/block0/cap0 /gpfs/data/repo3+1/pod0/block0/cap0

Do that for all caps.

