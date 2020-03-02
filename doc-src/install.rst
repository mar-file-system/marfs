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
use 3+1 erasure coding.

Metadata Nodes
--------------
We will be using GPFS as metadata storage in this example. Your GPFS cluster
should already be setup and ready to create filesets. You can still follow the
example using some other filesystem.

File Transfer Nodes
-------------------
These nodes will be used to move data in parallel from one place to another.
We will use `PFtool <https://github.com/pftool/pftool>`_ for this.

Interactive Nodes
-----------------
These nodes will be used to present MarFS to users through a FUSE mount.


Dependencies
============

Depending on things you may need different things. To install and make use of
MarFS you will need the following tools.

Fortunately many dependencies can be acquired through a package manager

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


MarFS Overview
--------------




Build and install from source
-----------------------------

Using release tarball
---------------------







