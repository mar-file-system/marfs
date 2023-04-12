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
will go over the setup of an example system on one node. This guide assumes
knowledge of ZFS, MPI, and general Linux operations.

Storage
-------
MarFS stores data and metadata differently. Data is stored as erasure-coded
objects. When data is written it is broken up into N number of pieces. Erasure
data is calculated on our N objects to create E number of erasure objects.
N+E number of objects are then mapped onto N+E identical filesystems. We refer
to these N+E filesystems as a "pod". In our example cluster we will have four
filesystems in a single pod giving us a 3+1 erasure coding scheme. You can have
multiple pods in a cluster. When you have multiple pods the pod is selected with
a hash, and data is written to that pod. Data will never be written across
multiple pods. So if you have 4 pods each matching our single pod with a 3+1
scheme those four objects will always be in the same pod.

Metadata can be stored on any filesystem that supports extended attributes and
sparse-files. For scalability purposes a distributed filesystem is highly
recommended. In our example system we will just use a directory, but our
production clusters typically run General Parallel Filesystem (GPFS).

Data Access
-----------
With object data being stored across a number of pods it is reasonable to
provide a way to interact with the filesystem in a unified matter. Most users
would expect a single mount point they can look through for various tasks. This
is provided through FUSE, allowing users to look at their data. In production
systems, this FUSE mount is read only, and is there for users to locate their
files for parallel movement, although MarFS allows FUSE to be configured to
allow read/write.

Data Movement
-------------
Data is moved in parallel using PFTool. In our production systems, nodes running
PFTool are called "File Transfer Agent" nodes, or FTAs.

Production Cluster Summary
==========================
In a typical production system, we have three types of nodes, each handling a
different role. For the purpose of our example system, one node will handle all
three roles.

Storage Nodes
-------------
Each storage node uses ZFS for MarFS block storage. Each node will have four
zpools in a RAIDZ3(17+3) configuration. We have multiple pods configured to use
10+2 erasure coding. Must have a high performance network such as Infiniband.

Metadata Nodes
--------------
We use GPFS as metadata storage in production systems. Your GPFS cluster should
already be setup and ready to create filesets. You should have a high
performance network such as Infiniband when using GPFS.

File Transfer Nodes
-------------------
These nodes will be used to move data in parallel from one place to another. We
will use `PFtool <https://github.com/pftool/pftool>`_ for this. You must have
high performance network such as Infiniband. They will also be used to present
MarFS to users through a FUSE mount.

MarFS abstractions
==================
Remember how earlier we talked about the pod? There are more things to
understand about the pod. There are logical data abstractions we will see later
when understanding the configuration file. We will talk about them briefly here
first.

The Repository
--------------
A repo is where all the object data for a MarFS Filesystem lives; it’s a logical
description of a MarFS object-store, with details on the number of storage
servers, etc.

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
storage for that namespace. Each namespace in MarFS must be associated with a
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
Each capacity unit (cap) is a datastore on a ZFS zpool on a block in a pod.

Create Base Directory Structure
===============================

In the case we are building MarFS on a multi-node system, it is helpful to have
a shared filesystem among all the nodes in the cluster, such as a NFS share
mounted on all nodes. We will keep all our source code and other files that must
be shared here. In our example we will use :code:`/opt/campaign`. Before we
start installing, let's create this directory and a subdirectory to function as
an install target, and add it to our PATH environment variable.

.. code-block:: bash

   mkdir -p /opt/campaign/install/bin
   cd /opt/campaign
   export PATH=$PATH:/opt/campaign/install/bin

We will also need a root directory for our metadata store

.. code-block:: bash

   mkdir -p /marfs/mdal-root

and one for our data store

.. code-block:: bash

   mkdir /marfs/dal-root

The last directory we need to create will be for our filesystem mount point.

.. code-block:: bash

   mkdir /campaign

MarFS Config File
-----------------

MarFS uses a config file to set up repositories and namespaces. We will use this
example config file which we will create at
:code:`/opt/campaign/install/etc/marfs-config.xml`:

.. literalinclude:: _static/new_config.xml
   :language: xml

We now need to export the config path as an environment variable so it can be
found by the MarFS binaries:

.. code-block:: bash

   export MARFS_CONFIG_PATH=/opt/campaign/install/etc/marfs-config.xml

Dependencies
============

Depending on your environment you may need different things. To install and make
use of MarFS you will need the following tools.

Fortunately many dependencies can be acquired through a package manager.
* FUSE (and development packages)
* G++
* Git
* Libattr (and development packages)
* Libtool
* Libxml2 (and development packages)
* Make
* Nasm
* Open MPI (and development packages)
* Open SSL (and development packages)

.. yum install libattr-devel curl-devel \
   curl openssl-devel openssl

Others can be obtained from source.

.. code-block:: bash

   git clone https://github.com/01org/isa-l.git
   git clone https://github.com/mar-file-system/erasureUtils.git
   git clone https://github.com/mar-file-system/marfs.git
   git clone https://github.com/pftool/pftool.git

A quick description of tools acquired from source::

   ISA-L: Intel’s Intelligent Storage Acceleration Library
   ErasureUtils: The erasure coding layer used for Multi-Component storage
   MarFS: The core MarFS libraries
   PfTool: A tool for parallel data movement

You will need yasm 1.2.0 or later for ISA-L.

You may also need to ensure that MPI is in your :code:`$PATH` environment
averiable and OpenMPI's library directory is in your :code:`$LD_LIBRARY_PATH`
environment variable.

ISA-L
-----
.. code-block:: bash

   cd isa-l
   ./autogen.sh
   ./configure --prefix=/opt/campaign/install
   make
   make install

ErasureUtils
------------
.. code-block:: bash

   cd ../erasureUtils
   autoreconf -i
   ./configure --prefix=/opt/campaign/install/ CFLAGS="-I/opt/campaign/install/include" LDFLAGS="-L/opt/campaign/install/lib"
   make
   make install

MarFS
-----
.. code-block:: bash

   cd ../marfs
   autoreconf -i
   ./configure --prefix=/opt/campaign/install/ CFLAGS="-I/opt/campaign/install/include" LDFLAGS="-L/opt/campaign/install/lib"
   make
   make install

PFTool
------
.. code-block:: bash

   cd ../pftool
   ./autogen
   ./configure --prefix=/opt/campaign/install/ CFLAGS="-I/opt/campaign/install/include" CXXFLAGS="-I/opt/campaign/install/include" LDFLAGS="-L/opt/campaign/install/lib" --enable-marfs
   make
   make install

Starting MarFS
==============

We now have all our utilites built and are almost ready to boot the filesystem.
On a simple one node system not leveraging GPFS or ZFS like this one, the
process is as simple as launching the FUSE mount. On more complex systems, you
will have to initialize the meta and data stores and any network mounts. These
tasks can be automated using tools such as Ansible, but that is beyond the scope
of this document.

Verifying the Configuration
---------------------------

Before we start the filesystem, we need to first verify that our configuration
file is valid, and the meta and data spaces match the structure specified in the
config file. We do this by running

.. code-block:: bash

   marfs-verifyconf -a

which will print ":code:`Config Verified`" to the console if the system is ready
to boot.

Launching FUSE
--------------

To launch FUSE (and our filesystem), we execute:

.. code-block:: bash

   marfs-fuse -o allow_other,use_ino,intr /campaign

If everything goes according to plan, marfs-fuse will start in the background.
You should now be able to access our MarFS filesystem through the
mount at :code:`/campaign`. If the :code:`marfs-fuse` call returned a nonzero
value, something went wrong. In case this happens, reconfigure MarFS with the
:code:`--enable-debugALL` flag.

See if it works!
----------------

.. code-block:: bash

   $ ls /campaign
   full-access-subspace

   $ echo test > /campaign/full-access-subspace/test
   $ cat /campaign/full-access-subspace/test
   test

Build and run PFTool
--------------------

In the previous section we mounted MarFS through FUSE. It is possible to use
this FUSE mount for all accesses to MarFS, but performance is limited by going
through a single host (and through FUSE), even though in multi-node systems,
writes to the underlying storage-servers are performed in parallel. Therefore,
for a large datacenter, it may make more sense to use FUSE solely for metadata
access (rename, delete, stat, ls etc). This could be achieved by changing the
"perms" for every namespace in the config file.

We recommend using pftool for the "heavy lifting" of transferring big datasets.
PFTool provides fast, parallel MarFS data movement, and is preferred over using
the FUSE mount.

The PFTool binary can be invoked directly, but the preferred method is through
provided helper scripts (:code:`pfls`, :code:`pfcm`, :code:`pfcp`).

