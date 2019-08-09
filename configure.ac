# This file is part of MarFS, which is released under the BSD license.
# 
# 
# Copyright (c) 2015, Los Alamos National Security (LANS), LLC
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
# 
# 1. Redistributions of source code must retain the above copyright
# notice, this list of conditions and the following disclaimer.
# 
# 2. Redistributions in binary form must reproduce the above copyright
# notice, this list of conditions and the following disclaimer in the
# documentation and/or other materials provided with the distribution.
# 
# 3. Neither the name of the copyright holder nor the names of its
# contributors may be used to endorse or promote products derived from
# this software without specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
# FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE
# COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
# ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
# 
# -----
# NOTE:
# -----
# MarFS uses libaws4c for Amazon S3 object communication. The original
# version is at https://aws.amazon.com/code/Amazon-S3/2601 and under
# the LGPL license.  LANS, LLC added functionality to the original
# work. The original work plus LANS, LLC contributions is found at
# https://github.com/jti-lanl/aws4c.
# 
# GNU licenses can be found at <http://www.gnu.org/licenses/>.
# 
# 
# From Los Alamos National Security, LLC:
# LA-CC-15-039
# 
# Copyright (c) 2015, Los Alamos National Security, LLC All rights
# reserved.  Copyright 2015. Los Alamos National Security, LLC. This
# software was produced under U.S. Government contract
# DE-AC52-06NA25396 for Los Alamos National Laboratory (LANL), which
# is operated by Los Alamos National Security, LLC for the
# U.S. Department of Energy. The U.S. Government has rights to use,
# reproduce, and distribute this software.  NEITHER THE GOVERNMENT NOR
# LOS ALAMOS NATIONAL SECURITY, LLC MAKES ANY WARRANTY, EXPRESS OR
# IMPLIED, OR ASSUMES ANY LIABILITY FOR THE USE OF THIS SOFTWARE.  If
# software is modified to produce derivative works, such modified
# software should be clearly marked, so as not to confuse it with the
# version available from LANL.
# 
# THIS SOFTWARE IS PROVIDED BY LOS ALAMOS NATIONAL SECURITY, LLC AND
# CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
# INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL LOS ALAMOS NATIONAL SECURITY, LLC OR
# CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
# USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
# OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
# SUCH DAMAGE.

#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.63])
AC_INIT([MarFS], [0.92], [http://github.com/mar-file-system/marfs])
AM_INIT_AUTOMAKE([foreign -Wall subdir-objects])
AC_CONFIG_SRCDIR([COPYRIGHT.h])
AC_CONFIG_HEADERS([marfs_auto_config.h])

# Disable fuse
AC_ARG_ENABLE([fuse],
        AS_HELP_STRING([  --disable-fuse],
                       [only build libmarfs, do not build the MarFS fuse daemon]))
# Disable MDAL
AC_ARG_ENABLE([mdal],
        AS_HELP_STRING([  --disable-mdal],
                       [do not build the metadata abstraction layer]))
# Disable DAL
AC_ARG_ENABLE([dal],
        AS_HELP_STRING([  --disable-dal],
                       [do not build the data abstraction layer]))
# Enable Muti-Component Storage
AC_ARG_ENABLE([mc],
        AS_HELP_STRING([  --enable-mc],
                       [one of {yes|sockets|no}.  Build the Multi-Component storage framework.  Requires DAL enabled.]),
        [case "${enableval}" in
           (yes) ;;
           (sockets) ;;
           (no) ;;
           (*)  AC_MSG_FAILURE([--enable-mc={yes,no,sockets}]) ;;
         esac])

# Don't build GPFS utilities.
AC_ARG_WITH([gpfs],
        AS_HELP_STRING([  --without-gpfs],
                       [do not build GPFS-specific utilities]))

AC_ARG_ENABLE([debug],
        AS_HELP_STRING([  --enable-debug], [one of {S3|yes|no}.  build w/ -O0 (Also show verbose S3, if selected)]))

# NOTE: In the case of '--enable-debug=S3', the S3 part of the diagnostics (produced by libaws)
#       always go to stdout.  If you are running fuse, you won't see any stdout diagnostics from fuse
#       unless you start it with '-f', for example:
#
#          marfs_fuse -f -o ... > fuse.log 2>&1
#
AC_ARG_ENABLE([syslog],
        AS_HELP_STRING([  --enable-syslog],
                       [send --enable-debug output to syslog (default to stdout/stderr)]))

AC_ARG_ENABLE([spinlocks],
        AS_HELP_STRING([  --enable-spinlocks],
                       [use custom polite spinlocks instead of pthreads semaphores]))

AC_ARG_ENABLE([nfs-threads],
        AS_HELP_STRING([  --enable-nfs-threads],
                       [tell marfs to queue out-of-order read requests and reorder them so the file can be read sequentially from start to finish. NOT A GOOD IDEA, you probably don't want to enable this.]))

AC_ARG_WITH([aws-auth],
        AS_HELP_STRING([  --without-aws-auth], [tell FUSE it is okay if ~/.awsAuth doesn't exist]))

AC_ARG_ENABLE([gprof],
        AS_HELP_STRING([  --enable-gprof], [adds "-pg" compiler-flag, to dump profiling-info at run-time]))


# Checks for programs.
AC_PROG_AWK
AC_PROG_MAKE_SET
AC_PROG_RANLIB
AC_PROG_LN_S

# default to empty flags, instead of "-g -O2"
: ${CFLAGS=""}
AC_PROG_CC



# autoreconf says we need this since spinlocks uses asembly
AM_PROG_AS
# because we are in the situation of needing to support two versions
# of autotools right now, we need to chech whether this symbol exists
# before using it. With newer versions of automake we get a warning
# when AM_PROG_AR isn't included, but for older versions we get an
# error when it is.
m4_ifdef([AM_PROG_AR], [AM_PROG_AR])
AM_PROG_CC_C_O

# Exit with an error if we got conflicting options
AS_IF([test x"$enable_mc" != xno && test x"$enable_dal" = xno],
      AC_ERROR([cannot specify --disable-dal if building multi-component]))


# --- Checks for libraries.
#
# NOTE: Associated seachpaths, lib-specs, etc, are *prepended* for each
#       find, so search in the reverse order of dependencies.

# we use floorf for managing quotas
AC_CHECK_LIB([m], [floorf])
AC_CHECK_LIB([pthread], [pthread_create])

# only need librt if we are using spinlocks
AS_IF([test x"$enable_spinlocks" = xyes],
      AC_CHECK_LIB([rt], [aio_cancel]))

# needed for the DAL
AC_CHECK_LIB([dl], [dlsym])

# These are needed for accessing object-stores, or DAL=MC+RDMA.
AC_CHECK_LIB([crypto], [MD5_Init])
AC_CHECK_LIB([curl], [curl_easy_cleanup])
AC_CHECK_LIB([xml2], [xmlSAX2GetEntity])
AC_CHECK_LIB([aws4c], [s3_enable_Scality_extensions], [], [AC_MSG_ERROR(["Failed to locate libaws4c!  Please ensure the library is installed in a default location or that your AWS4C env var is properly set."])])
AC_CHECK_LIB([aws4c_extra], [s3_create_bucket], [], [AC_MSG_ERROR(["Failed to locate libaws4c_extra!  Please ensure the library is installed in a default location or that your AWS4C env var is properly set."])])

# DAL=MC+RDMA also needs rdma_sockets and rdmacm
AS_IF([test x"$enable_mc" = xsockets],
      [AC_CHECK_LIB([rdmacm], [rsocket])
       AC_CHECK_LIB([ft], [fast_timer_inits])
       AC_CHECK_LIB([rdma_sockets], [skt_open]) ])

# link lib fuse if building the fuse daemon.
# we don't want to link libfuse for everything. Just for the marfs_fuse
# program. This is now handled in fuse/src/Makefile.am
AS_IF([test x"$enable_fuse" != xno],
    AC_CHECK_LIB([fuse], [fuse_main_real]))

AS_IF([test x"$enable_mc" != xno],
    [AC_CHECK_LIB([isal], [crc32_ieee])])

# need libne for any MC variant
AS_IF([test x"$enable_mc" != xno],
      [AC_CHECK_LIB([ft], [fast_timer_inits])
       AC_CHECK_LIB([rt], [timer_settime])
       AC_CHECK_LIB([dl], [dlclose]) ])


# DAL=MC+RDMA needs libne     configured with --enable-sockets={rdma|ip|unix}
# DAL=MC      needs libne NOT configured with --enable-sockets={rdma|ip|unix}
#
AS_IF([test x"$enable_mc" = xsockets],
      [AC_CHECK_LIB([ne], [udal_skt_fd_init],      , AC_ERROR(m4_normalize[
                                                         libne was not built with --enable-sockets
                                                         You must either rebuild libne with --enable-sockets
                                                         or  build marfs without --enable-mc=sockets])) ])
AS_IF([test x"$enable_mc" = xyes],
      [AC_CHECK_LIB([ne], [udal_non_socket_build], , AC_ERROR(m4_normalize[
                                                         libne was built with --enable-sockets
                                                         You must either rebuild libne without --enable-sockets
                                                         or build marfs with --enable-mc=sockets])) ])


AS_IF([test x"$enable_mc" != xno],
      [AC_CHECK_LIB([ne], [ne_open], , AC_ERROR([libne is needed for an MC build])) ])



# automake conditionals 
AM_CONDITIONAL([USE_DAL],  [test x"$enable_dal" != xno])
AM_CONDITIONAL([USE_MDAL], [test x"$enable_mdal" != xno])

AM_CONDITIONAL([USE_MC],         [test x"$enable_mc" != xno])
AM_CONDITIONAL([USE_MC_SOCKETS], [test x"$enable_mc" = xsockets])

AM_CONDITIONAL([WANT_FUSE], [test x"$enable_fuse" != xno])

# bug in autoconf?
#   not specifying '--enable-debug' leaves x"$enable_debug" = x, rather than xno
AM_CONDITIONAL([DEBUG_S3], [test x"$enable_debug"  = xS3])
AM_CONDITIONAL([DEBUG],    [test x"$enable_debug"  = xyes || test x"$enable_debug"  = xS3])

AM_CONDITIONAL([USE_SYSLOG], [test x"$enable_syslog"  = xyes])
AM_CONDITIONAL([USE_STDOUT], [test x"$enable_syslog" != xyes])

AM_CONDITIONAL([SPINLOCKS], [test x"$enable_spinlocks" = xyes])

AM_CONDITIONAL([NFS_THREADS], [test x"$enable_nfs_threads" = xyes])

AM_CONDITIONAL([NO_AWSAUTH], [test x"$with_aws_auth" = xno])

AM_CONDITIONAL([GPROF], [test x"$enable_gprof" = xyes])


# if the gpfs header is present then build the gpfs utilities.
build_gpfs_utilities=no
AS_IF([test x"$with_gpfs" != xno], [AC_CHECK_HEADER([gpfs.h], [build_gpfs_utilities=yes])])

# we don't want to link gpfs with anything other than the utilities.
# handle this in utilities/gpfs/?Makefile.am
#     AC_CHECK_LIB([gpfs], [gpfs_open_inodescan_with_xattrs])

AM_CONDITIONAL([GPFS_UTILITIES], [test x"$build_gpfs_utilities" = xyes])

# We depend on a number of environment variables
AC_ARG_VAR(AWS4C, [The location of the aws4c installation])
#AC_ARG_VAR(PA2X,  [The location of the PA2X installation])
AC_ARG_VAR(LIBNE, [The location of the erasureUtils libne installation])
AC_ARG_VAR(PARSE_DIR, [The location of PA2X (need this and PA2X)])

# XXX: We should simply define this as: 
# $(srcdir)/common/configuration/src/marfs_configuration_blueprint.cfg
# rather the asking the user to pass it in. It will always be the same thing.
AC_ARG_VAR(conf, [The path to the marfs configuration blueprint])
AC_ARG_VAR(MARFS_MNT, [The MarFS mount point (optional)])
AC_ARG_VAR(MARFSCONFIGRC, [The path to the marfs config (or any valid config)])

AS_IF([test -z "$PARSE_DIR"], [AC_ERROR([PARSE_DIR is not set])])
# We've already verified we have the libraries.  If we have found them, we don't necessarily need the env vars
#AS_IF([test -z "$AWS4C"], [AC_ERROR([AWS4C is not set])])
AM_CONDITIONAL([AWS4C],[test "$AWS4C"]) #just note if the var is set
#AS_IF([test x"$enable_mc" != xno && test -z "$LIBNE"], [AC_ERROR([LIBNE is not set])])
AM_CONDITIONAL([LIBNE],[test "$LIBNE"]) #just note if the var is set

AS_IF([test x"$enable_mc" = xsockets && test x"$with_aws_auth" = xno],
      [AC_ERROR([sockets MC cant have aws-auth disabled])])

AS_IF([test -z "$MARFSCONFIGRC"],
      [AC_SUBST(CONFIG, ${srcdir}/common/configuration/src/marfs_cctest.cfg)],
      [AC_SUBST(CONFIG, $MARFSCONFIGRC)])

# Checks for header files.
AC_CHECK_HEADERS([arpa/inet.h fcntl.h inttypes.h limits.h stddef.h stdint.h stdlib.h string.h sys/statvfs.h syslog.h unistd.h utime.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_HEADER_STDBOOL
AC_TYPE_UID_T
AC_TYPE_INT32_T
AC_TYPE_MODE_T
AC_TYPE_OFF_T
AC_C_RESTRICT
AC_TYPE_SIZE_T
AC_TYPE_SSIZE_T
AC_TYPE_UINT16_T
AC_TYPE_UINT32_T
AC_TYPE_UINT64_T
AC_TYPE_UINT8_T
AC_TYPE_INT64_T

# Checks for library functions.
AC_FUNC_ERROR_AT_LINE
AC_FUNC_GETGROUPS
AC_FUNC_LSTAT_FOLLOWS_SLASHED_SYMLINK
AC_FUNC_MKTIME
AC_FUNC_REALLOC

AC_CHECK_FUNCS([clock_gettime floor ftruncate getcwd gethostname gettimeofday realpath regcomp utime])

# AC_FUNC_MALLOC
AC_CHECK_FUNCS([malloc])


AC_CONFIG_FILES([Makefile])
AC_OUTPUT
