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
Each capacity unit (cap) is a ZFS zpool on a block in a pod :)

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

