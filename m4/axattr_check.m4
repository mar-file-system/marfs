AC_DEFUN([AXATTR_CHECK], dnl Checks for the proper configuration of xattr headers and C funciton calls
   [AC_CHECK_HEADER([sys/xattr.h], 
      [AC_DEFINE([AXATTR_RES], [1], [1='Apple/Linux with sys/xattr.h', 2='Linux with attr/xattr.h'])
       AC_SUBST(AXATTR_INC, [sys/xattr.h]) ],
      [AC_CHECK_HEADER([attr/xattr.h], 
         [AC_DEFINE([AXATTR_RES], [2], [1='Apple/Linux with sys/xattr.h', 2='Linux with attr/xattr.h'])
          AC_SUBST(AXATTR_INC, [attr/xattr.h]) ],
         [AC_MSG_ERROR([Could not locate <sys/xattr.h> nor <attr/xattr.h> for this Linux system])])])])

AC_DEFUN([AXATTR_GET_FUNC_CHECK],
   [AC_LANG([C])
   AC_COMPILE_IFELSE([dnl Performs compilation tests with various xattr function formats
      AC_LANG_PROGRAM([[
      #if (AXATTR_RES == 2)
      #   include <attr/xattr.h>
      #else
      #   include <sys/xattr.h>
      #endif
      char xattrval[20];
      ]], [[
      getxattr("test","user.test",&xattrval[0],sizeof(xattrval));
      ]])], 
      [echo "verified format of getxattr() with 4 args..."
       AC_SUBST(AXATTR_GET_FUNC, [4]) ], 
      [echo "checking for alternate format getxattr()..."
      AC_COMPILE_IFELSE([
         AC_LANG_PROGRAM([[
         #if (AXATTR_RES == 2)
         #   include <attr/xattr.h>
         #else
         #   include <sys/xattr.h>
         #endif
         char xattrval[20];
         ]], [[
         getxattr("test","user.test",&xattrval[0],sizeof(xattrval),0,0);
         ]])], 
         [echo "verified format of getxattr() with 6 args..."
          AC_SUBST(AXATTR_GET_FUNC, [6]) ], 
         [AC_MSG_ERROR([Could not identify a getxattr() function on this system])])])])

AC_DEFUN([AXATTR_SET_FUNC_CHECK],
   [AC_LANG([C])
   AC_COMPILE_IFELSE([dnl Performs compilation tests with various xattr function formats
      AC_LANG_PROGRAM([[
      #if (AXATTR_RES == 2)
      #   include <attr/xattr.h>
      #else
      #   include <sys/xattr.h>
      #endif
      char xattrval[20];
      ]], [[
      fsetxattr(12,"user.test", xattrval,strlen(xattrval),0);
      ]])], 
      [echo "verified format of fsetxattr() with 5 args..."
       AC_SUBST(AXATTR_SET_FUNC, [5]) ], 
      [echo "checking for alternate format fsetxattr()..."
      AC_COMPILE_IFELSE([
         AC_LANG_PROGRAM([[
         #if (AXATTR_RES == 2)
         #   include <attr/xattr.h>
         #else
         #   include <sys/xattr.h>
         #endif
         char xattrval[20];
         ]], [[
         fsetxattr(12,"user.test", xattrval,strlen(xattrval),0,0);
         ]])], 
         [echo "verified format of fsetxattr() with 6 args..."
          AC_SUBST(AXATTR_GET_FUNC, [6]) ], 
         [AC_MSG_ERROR([Could not identify a getxattr() function on this system])])])])

