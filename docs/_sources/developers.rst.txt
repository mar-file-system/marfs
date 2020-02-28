Developers
**********

This section describes the install process for MarFS.

.. contents::
   :depth: 2
   :local:

Dependencies
============

To build MarFS online docs you need python and python-pip installed

.. code-block:: bash

   pip install -U sphinx 
   pip install sphinx_rtd_theme


Contribute
==========
To build the documentation you need to clone the repo first.

.. code-block:: bash

   git clone https://github.com/mar-file-system/marfs.git
   ./marfs/doc-src/publish

Then simply stage your changes with git and push. This system requires
the changes to me merged to master. The docs will only reflect if master
has the changes.

