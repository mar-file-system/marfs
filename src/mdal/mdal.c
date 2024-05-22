/*
Copyright (c) 2015, Los Alamos National Security, LLC
All rights reserved.

Copyright 2015.  Los Alamos National Security, LLC. This software was produced
under U.S. Government contract DE-AC52-06NA25396 for Los Alamos National
Laboratory (LANL), which is operated by Los Alamos National Security, LLC for
the U.S. Department of Energy. The U.S. Government has rights to use, reproduce,
and distribute this software.  NEITHER THE GOVERNMENT NOR LOS ALAMOS NATIONAL
SECURITY, LLC MAKES ANY WARRANTY, EXPRESS OR IMPLIED, OR ASSUMES ANY LIABILITY
FOR THE USE OF THIS SOFTWARE.  If software is modified to produce derivative
works, such modified software should be clearly marked, so as not to confuse it
with the version available from LANL.
 
Additionally, redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.
3. Neither the name of Los Alamos National Security, LLC, Los Alamos National
Laboratory, LANL, the U.S. Government, nor the names of its contributors may be
used to endorse or promote products derived from this software without specific
prior written permission.

THIS SOFTWARE IS PROVIDED BY LOS ALAMOS NATIONAL SECURITY, LLC AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL LOS ALAMOS NATIONAL SECURITY, LLC OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY,
OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

-----
NOTE:
-----
Although these files reside in a seperate repository, they fall under the MarFS copyright and license.

MarFS is released under the BSD license.

MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier:
LA-CC-15-039.

These erasure utilites make use of the Intel Intelligent Storage
Acceleration Library (Intel ISA-L), which can be found at
https://github.com/01org/isa-l and is under its own license.

MarFS uses libaws4c for Amazon S3 object communication. The original version
is at https://aws.amazon.com/code/Amazon-S3/2601 and under the LGPL license.
LANL added functionality to the original work. The original work plus
LANL contributions is found at https://github.com/jti-lanl/aws4c.

GNU licenses can be found at http://www.gnu.org/licenses/.
*/

#include "marfs_auto_config.h"
#ifdef DEBUG_MDAL
#define DEBUG DEBUG_MDAL
#elif (defined DEBUG_ALL)
#define DEBUG DEBUG_ALL
#endif
#define LOG_PREFIX "mdal"

#include "logging/logging.h"
#include "mdal.h"

#include <ctype.h>


// Function to provide specific MDAL initialization calls based on name
MDAL init_mdal( xmlNode* mdal_conf_root ) {
   // make sure we start on a 'MDAL' node
   if ( mdal_conf_root->type != XML_ELEMENT_NODE  ||  strncmp( (char*)mdal_conf_root->name, "MDAL", 5 ) != 0 ) {
      LOG( LOG_ERR, "root xml node is not an element of type \"MDAL\"!\n" );
      errno = EINVAL;
      return NULL;
   }

   // make sure we have a valid 'type' attribute
   xmlAttr* type = mdal_conf_root->properties;
   xmlNode* typetxt = NULL;
   for ( ; type; type = type->next ) {
      if ( typetxt == NULL  &&  type->type == XML_ATTRIBUTE_NODE  &&  strncmp( (char*)type->name, "type", 5 ) == 0 ) {
         typetxt = type->children;
      }
      else {
         LOG( LOG_WARNING, "encountered unrecognized or redundant MDAL attribute: \"%s\"\n", (char*)type->name );
      }
   }
   if ( typetxt == NULL ) {
      LOG( LOG_ERR, "failed to find a 'type' attribute for the given MDAL node!\n" );
      errno = EINVAL;
      return NULL;
   }

   // make sure we have a text value for the 'type' attribute
   if ( typetxt->type != XML_TEXT_NODE  ||  typetxt->content == NULL ) {
      LOG( LOG_ERR, "MDAL type attribute does not have a text value!\n" );
      errno = EINVAL;
      return NULL;
   }

   // name comparison for each MDAL type
   if (  strncasecmp( (char*)typetxt->content, "posix", 6 ) == 0 ) {
      return posix_mdal_init( mdal_conf_root->children );
   }

   // if no MDAL found, return NULL
   LOG( LOG_ERR, "failed to identify an MDAL of type: \"%s\"\n", typetxt->content );
   errno = ENODEV;
   return NULL;
}



