#Copyright (c) 2015, Los Alamos National Security, LLC
#All rights reserved.
#
#Copyright 2015.  Los Alamos National Security, LLC. This software was produced
#under U.S. Government contract DE-AC52-06NA25396 for Los Alamos National
#Laboratory (LANL), which is operated by Los Alamos National Security, LLC for
#the U.S. Department of Energy. The U.S. Government has rights to use, reproduce,
#and distribute this software.  NEITHER THE GOVERNMENT NOR LOS ALAMOS NATIONAL
#SECURITY, LLC MAKES ANY WARRANTY, EXPRESS OR IMPLIED, OR ASSUMES ANY LIABILITY
#FOR THE USE OF THIS SOFTWARE.  If software is modified to produce derivative
#works, such modified software should be clearly marked, so as not to confuse it
#with the version available from LANL.
#
#Additionally, redistribution and use in source and binary forms, with or without
#modification, are permitted provided that the following conditions are met:
#1. Redistributions of source code must retain the above copyright notice, this
#list of conditions and the following disclaimer.
#
#2. Redistributions in binary form must reproduce the above copyright notice,
#this list of conditions and the following disclaimer in the documentation
#and/or other materials provided with the distribution.
#3. Neither the name of Los Alamos National Security, LLC, Los Alamos National
#Laboratory, LANL, the U.S. Government, nor the names of its contributors may be
#used to endorse or promote products derived from this software without specific
#prior written permission.
#
#THIS SOFTWARE IS PROVIDED BY LOS ALAMOS NATIONAL SECURITY, LLC AND CONTRIBUTORS
#"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
#THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
#ARE DISCLAIMED. IN NO EVENT SHALL LOS ALAMOS NATIONAL SECURITY, LLC OR
#CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY,
#OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
#SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
#INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
#STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
#OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
#-----
#NOTE:
#-----
#
#MarFS is released under the BSD license.
#
#MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier:
#LA-CC-15-039.
#
#MarFS uses libaws4c for Amazon S3 object communication. The original version
#is at https://aws.amazon.com/code/Amazon-S3/2601 and under the LGPL license.
#LANL added functionality to the original work. The original work plus
#LANL contributions is found at https://github.com/jti-lanl/aws4c.
#
#GNU licenses can be found at http://www.gnu.org/licenses/.
#
#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.


# NOTE: Instead of generating libne_auto_config.h for inclusion in our
#    .h files, which would contain #defines matching AC_DEFINE() calls
#    in this config, we now generate erasure_config.h which contains
#    hand-built #defines matching AC_SUBST() calls in this config.  As
#    a result, we generate only the symbols we really want to export,
#    which lets us avoid name-conflicts with #defines that might be
#    used within other libraries (e.g. pftool) which include our .h
#    files, plus their own config.h.
#
#    HOWEVER, erasure.c still uses HAVE_LIBISAL internally.  Therefore,
#    we do generate this file, but we avoid exporting it.

AC_PREREQ([2.69])
AC_INIT([MarFS], [1.0], [gransom@lanl.gov])
AC_CONFIG_SRCDIR([COPYRIGHT.h])
AC_CONFIG_HEADERS([src/marfs_auto_config.h])
AC_CONFIG_MACRO_DIR([m4])
AM_INIT_AUTOMAKE([subdir-objects])
AC_PROG_CC([mpicc mpiCC])
AM_PROG_CC_C_O
AC_PROG_LIBTOOL

# default to "-g", instead of "-g -O2"
CFLAGS="$CFLAGS -g"
# AC_PROG_CC

# AC_PROG_RANLIB

# check for '-Wall' support and use it if we can
AC_MSG_CHECKING(whether compiler understands -Wall)
old_CFLAGS="$CFLAGS"
CFLAGS="$CFLAGS -Wall"
AC_COMPILE_IFELSE([AC_LANG_PROGRAM([],[])],
  AC_MSG_RESULT(yes),
  AC_MSG_RESULT(no)
  CFLAGS="$old_CFLAGS")

# Checks for header files.
AC_CHECK_HEADERS([fcntl.h stdint.h stdlib.h string.h unistd.h])
AXATTR_CHECK

# Checks for typedefs, structures, and compiler characteristics.
AC_TYPE_OFF_T
AC_TYPE_SSIZE_T
AC_TYPE_SIZE_T
AC_CHECK_HEADER_STDBOOL
AC_TYPE_UINT32_T
AC_TYPE_UINT64_T
AC_TYPE_UINT8_T

# Checks for library functions.
AC_CHECK_FUNCS([bzero ftruncate memset strerror strtol strtoul malloc])

AXATTR_GET_FUNC_CHECK
AXATTR_SET_FUNC_CHECK

AC_CHECK_LIB([m], [log10], [], [AC_MSG_ERROR(["Could not locate libm!])])
AC_CHECK_LIB([xml2], [xmlReadFile], [], [AC_MSG_ERROR(["Could not locate libxml2!"])])
AC_CHECK_LIB([fuse], [fuse_main], [], [AC_MSG_ERROR(["Could not locate libfuse!"])])
AC_CHECK_LIB([ne], [ne_init], [], [AC_MSG_ERROR(["Could not locate libne!  Please build and install the 'erasureUtils' repo.  If using a custom install location, you may need to add that 'lib' dir to your LDFLAGS='-L*' environment var."])])
AC_CHECK_LIB([TQ], [tq_init], [], [AC_MSG_ERROR(["Could not locate libTQ!  Please build and install the 'erasureUtils' repo.  If using a custom install location, you may need to add that 'lib' dir to your LDFLAGS='-L*' environment var."])])
AC_CHECK_LIB([mpi], [MPI_Abort], [], [AC_MSG_ERROR(["Could not locate libmpi!"])])

PKG_CHECK_MODULES( XML, libxml-2.0 )

#AC_CHECK_LIB([isal],    [crc32_ieee],
#    [],
#    [AC_CHECK_LIB([isal], [crc32_ieee_base],
#       [echo
#        echo "WARNING:  Could not properly link 'crc32_ieee' from isa-l, defaulting to '_base' implementation"
#        echo "          IN THIS STATE, THESE UTILITES MAY NOT FUNCTION AS EXPECTED AND WILL ALMOST CERTAINLY HAVE VERY POOR PERFORMANCE"
#        echo],
#       [AC_MSG_ERROR(["Could not locate Intel's Intelligent Storage Acceleration Library (isa-l) on this system!  Please verify that the proper version of the library (v2.16.0 or higher) is installed."])])])

AC_DEFINE( [GTESTNEVAR], [], [Testing variable] )

AC_CHECK_LIB([logging], [printf_log], [], [AC_MSG_ERROR(["Could not locate liblogging!  Please verify that you have installed the erasureUtils libraries and that those libraries are properly referenced."])])

## check for presence of zlib
#warnlibs=""
#AC_CHECK_LIB([z], [adler32], [], [warnlibs="zlib "])
#AC_CHECK_LIB([m], [ceil], [], [warnlibs="$warnlibs""libm "])
#AC_CHECK_LIB([isal], [crc32_gzip_refl], [:], [warnlibs="$warnlibs""libisal(need-v2.19+) "])
#librdmacm_present=true
#AC_CHECK_LIB([rdmacm], [rdma_getaddrinfo], [], [librdmacm_present=false  &&  warnlibs="$warnlibs""librdmacm "])
#AM_CONDITIONAL([BENCH], [test "$warnlibs" = ""  ])
#AM_COND_IF([BENCH], [],
#   [echo
#    echo "WARNING: failed to properly link all necessary functions from $( echo "$warnlibs" | sed 's/ $/\./' | sed 's/ /, /g' )"
#    echo "         These libraries may be missing, or below the required versions."
#    echo "         The 'isal_benchmark' subdir will not be built!"
#    echo ])

# Allow turning on debugging flags
# NOTE: There are currently no fast-timer (i.e. 'ft') diagnostics.
#       If there were, we would probably only want to see them for specialized debugging.
# NOTE: The only point of 'gdb' is to allow sockets timeouts to be made long, in the
#       case where a developer wants to be able to take their time stepping through
#       protocol steps, without worrying about timeouts.  In all other ways, it is the
#       same as 'yes' (which is the default if you just configure with '--enable-debug')
#AC_ARG_ENABLE([debug],
#             AS_HELP_STRING([--enable-debug],
#                            [Turn on internal debugging.  One of {yes,no,ne,sockets,ft,gdb}]),
#             [ AS_IF( [ x"${enableval}" =~ ^x\(.*,\)?all\(,.*\)?$ ], [AC_DEFINE( [DEBUG_ALL] )], [] ) ],
#             [debug_all=no; debug_ne=no; debug_io=no; debug_dal=no; debug_tq=no])

fulldebugstr=""
AC_ARG_ENABLE( [debugALL],
               [ AS_HELP_STRING( [--enable-debugALL], [Enable all debug output] ) ],
                  [ AS_CASE( [ x"${enableval}" ],
                           [[ x[Ee]* ]],
                              [ AC_DEFINE( [DEBUG_ALL], [3], [Enable all debug output] ) ],
                           [[ x[Ww]* ]],
                              [ AC_DEFINE( [DEBUG_ALL], [2], [Enable all debug output] ) ],
                           [ AC_DEFINE( [DEBUG_ALL], [1], [Enable all debug output] ) ]
                  ) && fulldebugstr="$fulldebugstr""ALL=\"$enableval\" " ], []
             )

AC_ARG_ENABLE( [debugMDAL],
               [ AS_HELP_STRING( [--enable-debugMDAL], [Enable 'mdal'-subdir debug output] ) ],
                  [ AS_CASE( [ x"${enableval}" ],
                           [[ x[Ee]* ]],
                              [ AC_DEFINE( [DEBUG_MDAL], [3], [Enable 'mdal'-subdir debug output] ) ],
                           [[ x[Ww]* ]],
                              [ AC_DEFINE( [DEBUG_MDAL], [2], [Enable 'mdal'-subdir debug output] ) ],
                           [ AC_DEFINE( [DEBUG_MDAL], [1], [Enable 'mdal'-subdir debug output] ) ]
                  ) && fulldebugstr="$fulldebugstr""MDAL=\"$enableval\" " ], []
             )

AC_ARG_ENABLE( [debugHASH],
               [ AS_HELP_STRING( [--enable-debugHASH], [Enable 'hash'-subdir debug output] ) ],
                  [ AS_CASE( [ x"${enableval}" ],
                           [[ x[Ee]* ]],
                              [ AC_DEFINE( [DEBUG_HASH], [3], [Enable 'hash'-subdir debug output] ) ],
                           [[ x[Ww]* ]],
                              [ AC_DEFINE( [DEBUG_HASH], [2], [Enable 'hash'-subdir debug output] ) ],
                           [ AC_DEFINE( [DEBUG_HASH], [1], [Enable 'hash'-subdir debug output] ) ]
                  ) && fulldebugstr="$fulldebugstr""HASH=\"$enableval\" " ], []
             )

AC_ARG_ENABLE( [debugCONFIG],
               [ AS_HELP_STRING( [--enable-debugCONFIG], [Enable 'config'-subdir debug output] ) ],
                  [ AS_CASE( [ x"${enableval}" ],
                           [[ x[Ee]* ]],
                              [ AC_DEFINE( [DEBUG_CONFIG], [3], [Enable 'config'-subdir debug output] ) ],
                           [[ x[Ww]* ]],
                              [ AC_DEFINE( [DEBUG_CONFIG], [2], [Enable 'config'-subdir debug output] ) ],
                           [ AC_DEFINE( [DEBUG_CONFIG], [1], [Enable 'config'-subdir debug output] ) ]
                  ) && fulldebugstr="$fulldebugstr""CONFIG=\"$enableval\" " ], []
             )

AC_ARG_ENABLE( [debugRECOVERY],
               [ AS_HELP_STRING( [--enable-debugRECOVERY], [Enable 'recovery'-subdir debug output] ) ],
                  [ AS_CASE( [ x"${enableval}" ],
                           [[ x[Ee]* ]],
                              [ AC_DEFINE( [DEBUG_RECOVERY], [3], [Enable 'recovery'-subdir debug output] ) ],
                           [[ x[Ww]* ]],
                              [ AC_DEFINE( [DEBUG_RECOVERY], [2], [Enable 'recovery'-subdir debug output] ) ],
                           [ AC_DEFINE( [DEBUG_RECOVERY], [1], [Enable 'recovery'-subdir debug output] ) ]
                  ) && fulldebugstr="$fulldebugstr""RECOVERY=\"$enableval\" " ], []
             )

AC_ARG_ENABLE( [debugTAGGING],
               [ AS_HELP_STRING( [--enable-debugTAGGING], [Enable 'tagging'-subdir debug output] ) ],
                  [ AS_CASE( [ x"${enableval}" ],
                           [[ x[Ee]* ]],
                              [ AC_DEFINE( [DEBUG_TAGGING], [3], [Enable 'tagging'-subdir debug output] ) ],
                           [[ x[Ww]* ]],
                              [ AC_DEFINE( [DEBUG_TAGGING], [2], [Enable 'tagging'-subdir debug output] ) ],
                           [ AC_DEFINE( [DEBUG_TAGGING], [1], [Enable 'tagging'-subdir debug output] ) ]
                  ) && fulldebugstr="$fulldebugstr""TAGGING=\"$enableval\" " ], []
             )

AC_ARG_ENABLE( [debugDS],
               [ AS_HELP_STRING( [--enable-debugDS], [Enable 'ds'-subdir debug output] ) ],
                  [ AS_CASE( [ x"${enableval}" ],
                           [[ x[Ee]* ]],
                              [ AC_DEFINE( [DEBUG_DS], [3], [Enable 'ds'-subdir debug output] ) ],
                           [[ x[Ww]* ]],
                              [ AC_DEFINE( [DEBUG_DS], [2], [Enable 'ds'-subdir debug output] ) ],
                           [ AC_DEFINE( [DEBUG_DS], [1], [Enable 'ds'-subdir debug output] ) ]
                  ) && fulldebugstr="$fulldebugstr""DS=\"$enableval\" " ], []
             )

AC_ARG_ENABLE( [debugRM],
               [ AS_HELP_STRING( [--enable-debugRM], [Enable 'rsrc_mgr'-subdir debug output] ) ],
                  [ AS_CASE( [ x"${enableval}" ],
                           [[ x[Ee]* ]],
                              [ AC_DEFINE( [DEBUG_RM], [3], [Enable 'rsrc_mgr'-subdir debug output] ) ],
                           [[ x[Ww]* ]],
                              [ AC_DEFINE( [DEBUG_RM], [2], [Enable 'rsrc_mgr'-subdir debug output] ) ],
                           [ AC_DEFINE( [DEBUG_RM], [1], [Enable 'rsrc_mgr'-subdir debug output] ) ]
                  ) && fulldebugstr="$fulldebugstr""RM=\"$enableval\" " ], []
             )

AC_ARG_ENABLE( [debugAPI],
               [ AS_HELP_STRING( [--enable-debugAPI], [Enable 'api'-subdir debug output] ) ],
                  [ AS_CASE( [ x"${enableval}" ],
                           [[ x[Ee]* ]],
                              [ AC_DEFINE( [DEBUG_API], [3], [Enable 'api'-subdir debug output] ) ],
                           [[ x[Ww]* ]],
                              [ AC_DEFINE( [DEBUG_API], [2], [Enable 'api'-subdir debug output] ) ],
                           [ AC_DEFINE( [DEBUG_API], [1], [Enable 'api'-subdir debug output] ) ]
                  ) && fulldebugstr="$fulldebugstr""API=\"$enableval\" " ], []
             )

AC_ARG_ENABLE( [debugFUSE],
               [ AS_HELP_STRING( [--enable-debugFUSE], [Enable 'fuse'-subdir debug output] ) ],
                  [ AS_CASE( [ x"${enableval}" ],
                           [[ x[Ee]* ]],
                              [ AC_DEFINE( [DEBUG_FUSE], [3], [Enable 'fuse'-subdir debug output] ) ],
                           [[ x[Ww]* ]],
                              [ AC_DEFINE( [DEBUG_FUSE], [2], [Enable 'fuse'-subdir debug output] ) ],
                           [ AC_DEFINE( [DEBUG_FUSE], [1], [Enable 'fuse'-subdir debug output] ) ]
                  ) && fulldebugstr="$fulldebugstr""FUSE=\"$enableval\" " ], []
             )

# debugging-output should go to syslog, instead of stdout/stderr?
AC_ARG_ENABLE([syslog],
               AS_HELP_STRING( [--enable-syslog], [Send debugging output to syslog, instead of stdout/stderr] ),
               AC_DEFINE( [USE_SYSLOG], [], [Send debugging output to syslog, instead of stdout/stderr] ), [] )

#AM_COND_IF( [DEBUG_ALL], [test "$debug_all" = yes], [AC_DEFINE( [DEBUG_ALL] )], [])


#AM_CONDITIONAL([DEBUG_SOCKETS],     [test "$enable_debug" = sockets || test "$enable_debug" = all ])
#AM_CONDITIONAL([DEBUG_SOCKETS_GDB], [test "x$debug_mode" = "xgdb" ])
#AM_CONDITIONAL([DEBUG_FT],          [test "$enable_debug" = ft ])




AM_CONDITIONAL([USE_SYSLOG], [test x"$enable_syslog" != xno])

AC_CONFIG_FILES([Makefile
                 src/Makefile
                 src/hash/Makefile
                 src/mdal/Makefile
                 src/tagging/Makefile
                 src/recovery/Makefile
                 src/config/Makefile
                 src/datastream/Makefile
                 src/api/Makefile
                 src/fuse/Makefile
                 src/rsrc_mgr/Makefile
                 etc/Makefile
               ])

AC_OUTPUT

# let the user know the configuration

cat << EOF

marfs configuration
#####################
CC       : $CC
CFLAGS   : $CFLAGS
LDFLAGS  : $LDFLAGS
LIBS     : $LIBS
DEBUG    : $fulldebugstr

EOF

