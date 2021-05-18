/*
Copyright (c) 2015, Los Alamos National Security, LLC
All rights reserved.

Copyright 2015.  Los Alamos National Security, LLC. This software was
produced under U.S. Government contract DE-AC52-06NA25396 for Los
Alamos National Laboratory (LANL), which is operated by Los Alamos
National Security, LLC for the U.S. Department of Energy. The
U.S. Government has rights to use, reproduce, and distribute this
software.  NEITHER THE GOVERNMENT NOR LOS ALAMOS NATIONAL SECURITY,
LLC MAKES ANY WARRANTY, EXPRESS OR IMPLIED, OR ASSUMES ANY LIABILITY
FOR THE USE OF THIS SOFTWARE.  If software is modified to produce
derivative works, such modified software should be clearly marked, so
as not to confuse it with the version available from LANL.

Additionally, redistribution and use in source and binary forms, with
or without modification, are permitted provided that the following
conditions are met: 1. Redistributions of source code must retain the
above copyright notice, this list of conditions and the following
disclaimer.

2. Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in the
documentation and/or other materials provided with the distribution.
3. Neither the name of Los Alamos National Security, LLC, Los Alamos
National Laboratory, LANL, the U.S. Government, nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY LOS ALAMOS NATIONAL SECURITY, LLC AND
CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING,
BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL LOS
ALAMOS NATIONAL SECURITY, LLC OR CONTRIBUTORS BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER
IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

-----
NOTE:
-----
MarFS is released under the BSD license.

MarFS was reviewed and released by LANL under Los Alamos Computer Code
identifier: LA-CC-15-039.

MarFS uses libaws4c for Amazon S3 object communication. The original
version is at https://aws.amazon.com/code/Amazon-S3/2601 and under the
LGPL license.  LANL added functionality to the original work. The
original work plus LANL contributions is found at
https://github.com/jti-lanl/aws4c.

GNU licenses can be found at http://www.gnu.org/licenses/.
*/


/*
 * EXAMPLE CONFIG -- excluding XML header
 * <marfs_config version="1.10">
 *    <!-- Mount Point -->
 *    <mnt_top>/campaign</mnt_top>
 *
 *    <!-- Host Definitions ( ignored by this code ) -->
 *    <hosts> ... </hosts>
 *
 *    <!-- Repos -->
 *    <repo name="mc10+2">
 *
 *       <!-- Per-Repo Data Scheme -->
 *       <data>
 *
 *          <!-- Erasure Protection -->
 *          <protection>
 *             <N>10</N>
 *             <E>2</E>
 *             <PSZ>1024</PSZ>
 *          </protection>
 *
 *          <!-- Packing -->
 *          <packing enabled="yes">
 *             <max_files>4096</max_files>
 *          </packing>
 *
 *          <!-- Chunking -->
 *          <chunking enabled="yes">
 *             <max_size>1073741824</max_size>
 *          </chunking>
 *
 *          <!-- Object Distribution -->
 *          <distribution>
 *             <pods dweight=2>4:0=1,3=5</pods>
 *             <caps>5:2=0</caps>
 *             <scatters>4096</scatters>
 *          </distribution>
 *
 *          <!-- DAL Definition ( ignored by this code ) -->
 *          <DAL type="posix"> ... </DAL>
 *
 *       </data>
 *
 *       <!-- Per-Repo Metadata Scheme -->
 *
 */


//   -------------   INTERNAL DEFINITIONS    -------------



//   -------------   INTERNAL FUNCTIONS    -------------

int count_nodes( xmlNode* root, const char* tag ) {
   // iterate over all nodes at this level, checking for instances of 'tag'
   int tagcnt = 0;
   for ( ; root; root = root->next ) {
      if ( root->type == XML_ELEMENT_NODE  &&  strncmp( (char*)root->name, tag, strlen(tag) ) == 0 ) {
         tagcnt++;
      }
   }
   return tagcnt;
}


marfs_ns* find_ns( HASH_NODE* nslist, int nscount, const char* nsname ) {
   // iterate over the elements of the nslist, searching for a matching name
   int index;
   for( index = 0; index < nscount; index++ ) {
      if( strncmp( nslist[index].name, nsname, strlen(nsname) ) == 0 ) {
         return ( nslist + index );
      }
   }
   return NULL;
}


marfs_repo* find_repo( marfs_repo* repolist, int repocount, const char* reponame ) {
   // iterate over the elements of the repolist, searching for a matching name
   int index;
   for ( index = 0; index < repocount; index++ ) {
      if ( strncmp( repolist[index].name, reponame, strlen(reponame) ) == 0 ) {
         return ( repolist + index );
      }
   }
   return NULL;
}


int parse_perms( ns_perms* iperms, ns_perms* bperms, xmlNode* permroot ) {
   // define an unused character value and use as an indicator of a completed perm string
   #define EOPERM 'x'

   // iterate over nodes at this level
   for ( ; permroot; permroot = permroot->next ) {
      if ( permroot->type == XML_ELEMENT_NODE ) {
         // determine if we're parsing iteractive or batch perms
         char ptype = '\0';
         if ( strncmp( (char*)permroot->name, "interactive", 12 ) == 0 ) {
            if ( *iperms != NS_NOACCESS ) {
               // this is a duplicate 'interactive' def
               LOG( LOG_ERR, "encountered duplicate 'interactive' perm set\n" );
               return -1;
            }
            ptype = 'i';
         }
         else if ( strncmp( (char*)permroot->name, "batch", 6 ) == 0 ) {
            if ( *bperms != NS_NOACCESS ) {
               // this is a duplicate 'batch' def
               LOG( LOG_ERR, "encountered duplicate 'batch' perm set\n" );
               return -1;
            }
            ptype = 'b';
         }
         else {
            LOG( LOG_ERR, "encountered unexpected perm node: \"%s\"\n", (char*)permroot->name );
            return -1;
         }

         // parse the actual permission set
         if ( permroot->children != NULL  &&  
              permroot->children->type == XML_TEXT_NODE  &&  
              permroot->children->content != NULL ) {
            char* permstr = (char*)permroot->children->content;
            char fchar = '\0';
            ns_perms tmpperm = NS_NOACCESS;
            for ( ; *permstr != '\0'; permstr++ ) {
               switch ( *permstr ) {
                  case 'R':
                  case 'W':
                     // 'R'/'W' are only acceptable as the first character of a pair
                     if ( fchar != '\0' ) {
                        if ( fchar == EOPERM ) LOG( LOG_ERR, "trailing '%c' character is unrecognized\n", *permstr );
                        else LOG( LOG_ERR, "perm string '%c%c' is unrecognized\n", fchar, *permstr );
                        return -1;
                     }
                     fchar = *permstr;
                     break;
                  
                  case 'M':
                  case 'D':
                     // 'R' and 'W' must preceed 'M' or 'D'
                     if ( fchar == '\0' ) {
                        LOG( LOG_ERR, "perm field cannot begin with '%c'\n", *permstr );
                        return -1;
                     }
                     else if ( fchar == EOPERM ) {
                        LOG( LOG_ERR, "trailing '%c' character is unrecognized\n", *permstr );
                        return -1;
                     }
                     // remember our original perm value, to check for duplicate permstrings
                     ns_perms operm = tmpperm;
                     if ( fchar == 'R' ) {
                        if ( *permstr == 'M' ) tmpperm &= NS_READMETA;
                        else tmpperm &= NS_READDATA;
                     }
                     else if ( fchar == 'W' ) {
                        if ( *permstr == 'M' ) tmpperm &= NS_WRITEMETA;
                        else tmpperm &= NS_WRITEDATA;
                     }
                     else {
                        LOG( LOG_ERR, "perm string '%c%c' is unrecognized\n", fchar, *permstr );
                        return -1;
                     }
                     // verify that this permision string actually resulted in a change
                     if ( operm == tmpperm ) {
                        LOG( LOG_WARNING, "detected repeated '%c%c' perm string\n", fchar, *permstr );
                     }
                     fchar = EOPERM; // set to unused character val, indicating a complete permstring
                     break;

                  case ',':
                     fchar = '\0';
                     break;

                  default:
                     LOG( LOG_ERR, "encountered unrecognized character value '%c'\n", *permstr );
                     return -1;
               }
            }
            if ( ptype == 'i' )
               *iperms = tmpperms;
            else
               *bperms = tmpperms;
         }
         else {
            LOG( LOG_ERR, "encountered an unrecognized xml node type within the perms definition\n" );
            return -1;
         }
      }
      else {
         // don't know what this is supposed to be
         LOG( LOG_ERR, "encountered unknown tag within 'perms' definition\n" );
         return -1;
      }
   }
   // we have iterated over all perm nodes
   return 0;
}


int parse_quotas( char* enforcefq, size_t* fquota, char* enforcedq, size_t* dquota, xmlNode* quotaroot ) {
   // iterate over nodes at this level
   for ( ; quotaroot; quotaroot = quotaroot->next ) {
      if ( quotaroot->type == XML_ELEMENT_NODE ) {
         // determine if we're parsing file or data quotas
         char qtype = '\0';
         if ( strncmp( (char*)quotaroot->name, "files", 6 ) == 0 ) {
            if ( *fquota != 0  ||  *enforcefq != 0 ) {
               // this is a duplicate 'files' def
               LOG( LOG_ERR, "encountered duplicate 'files' quota set\n" );
               return -1;
            }
            qtype = 'f';
         }
         else if ( strncmp( (char*)quotaroot->name, "data", 5 ) == 0 ) {
            if ( *dquota != 0  ||  *enforcedq != 0 ) {
               // this is a duplicate 'data' def
               LOG( LOG_ERR, "encountered duplicate 'data' quota set\n" );
               return -1;
            }
            qtype = 'd';
         }
         else {
            LOG( LOG_ERR, "encountered unexpected quota sub-node: \"%s\"\n", (char*)quotaroot->name );
            return -1;
         }

         // parse the actual quota values
         if ( quotaroot->children != NULL  &&  
              quotaroot->children->type == XML_TEXT_NODE  &&  
              quotaroot->children->content != NULL ) {
            char* qstr = (char*)quotaroot->children->content;
            if ( *qstr == '\0'  ||  strncmp( qstr, "none", 5 ) == 0  ||  strncmp( qstr, "NONE", 5 ) == 0 ) {
               if ( qtype == 'f' ) {
                  enforcefq = 0;
               }
               else {
                  enforcedq = 0;
               }
            }
            else {
               // we have real quota values to parse
               size_t unitmult = 1;
               char* endptr = NULL;
               unsigned long long qvalue = strtoull( qstr, &(endptr), 10 );
               // check for any trailing unit specification
               if ( *endptr != '\0' ) {
                  if ( *endptr == 'K' ) { unitmult = 1024; }
                  else if ( *endptr == 'M' ) { unitmult = (1024 * 1024); }
                  else if ( *endptr == 'G' ) { unitmult = (1024 * 1024 * 1024); }
                  else if ( *endptr == 'T' ) { unitmult = (1024 * 1024 * 1024 * 1024); }
                  else if ( *endptr == 'P' ) { unitmult = (1024 * 1024 * 1024 * 1024 * 1024); }
                  else {
                     LOG( LOG_ERR, "encountered unrecognized character in quota value: \"%c\"", *endptr );
                     return -1;
                  }
                  // check for unacceptable trailing characters
                  endptr++;
                  if ( *endptr == 'B' ) { endptr++; } // skip acceptable 'B' character
                  if ( *endptr != '\0' ) {
                     LOG( LOG_ERR, "encountered unrecognized trailing character in quota value: \"%c\"", *endptr );
                     return -1;
                  }
               }
               if ( (qvalue * unitmult) >= SIZE_MAX ) {  // check for possible overflow
                  LOG( LOG_ERR, "specified quota value is too large to store: \"%s\"\n", qstr );
                  return -1;
               }
               // actually store the quota value
               if ( qtype == 'f' ) { *enforcefq = 1; *fquota = (qvalue * unitmult); }
               else { *enforcedq = 1; *dquota = (qvalue * unitmult); }
            }
         }
         else {
            LOG( LOG_ERR, "encountered an unrecognized xml node type within 'quota' definition\n" );
            return -1;
         }
      }
      else {
         // don't know what this is supposed to be
         LOG( LOG_ERR, "encountered unknown tag within 'quota' definition\n" );
         return -1;
      }
   }
   // we have iterated over all perm nodes
   return 0;
}


int free_ns( HASH_NODE* nsnode ) {
   // we are assuming this function will ONLY be called on completed NS structures
   //   As in, this function will not check for certain NULL pointers, like name
   marfs_ns* ns = (marfs_ns*)nsnode->content;
   const char* nsname = nsnode->name;
   int retval = 0;
   // check if this is a remote namespace def
   if ( ns->prepo == NULL ) {
      // 'subspaces' is just a string, and can be directly freed
      free( (char*)ns->subspaces );
   }
   else {
      // need to properly free the subspace hash table
      HASH_NODE* subspacelist;
      size_t subspacecount;
      if ( hash_term( ns->subspaces, &(subspacelist), &(subspacecount) ) ) {
         LOG( LOG_WARNING, "failed to free the subspace table of NS \"%s\"\n", nsname );
         retval = -1;
      }
      else {
         // free all subspaces which are not owned by a different repo
         size_t subspaceindex = 0;
         for( ; subspaceindex < subspacecount; subspaceindex++ ) {
            marfs_ns* subspace = (marfs_ns*)subspacelist[subspaceindex]->content;
            // omit any completed remote namespace reference
            if ( subspace->prepo !=NULL  &&  subspace->prepo != ns->prepo ) {
               continue; // this namespace is referenced by another repo, so we must skip it
            }
            // recursively free this subspace
            if ( free_ns( subspacelist[subspaceindex] ) ) {
               LOG( LOG_WARNING, "failed to free subspace of NS \"%s\"\n", nsname );
               retval = -1;
            }
         }
      }
   }
   // free the namespace itself
   free( ns );
   // free the namespace name
   free( nsname );
   return retval;
}
   

int create_ns( HASH_NODE* nsnode, marfs_ns* pnamespace, marfs_repo* prepo, xmlNode* nsroot ) {
   // need to check if this is a real NS or a remote one
   char rns = 0;
   if ( strncmp( (char*)nsroot->name, "ns", 3 ) ) {
      if ( strncmp( (char*)nsroot->name, "rns", 4 ) ) {
         LOG( LOG_ERR, "received an unexpected node as NSroot: \"%s\"\n", (char*)nsroot->name );
         errno = EINVAL;
         return -1;
      }
      // this is a remote namespace
      rns = 1;
   }

   // find the name of this namespace
   char* nsname = NULL;
   xmlAttr* attr = nsroot->properties;
   for ( ; attr; attr = attr->next ) {
      if ( attr->type == XML_ATTRIBUTE_NODE ) {
         if ( strncmp( (char*)attr->name, "name", 5 ) == 0 ) {
            if ( nsname != NULL ) {
               // we already found a 'name'
               LOG( LOG_WARNING, "encountered a duplicate 'name' value for NS \"%s\"\n", nsname );
               continue;
            }
            // we've found our 'name' attribute
            if ( attr->children->type == XML_TEXT_NODE  &&  attr->children->content != NULL ) {
               nsname = strdup( (char*)attr->children->content );
            }
            else {
               LOG( LOG_WARNING, "encountered NS name attribute with unrecognized value\n" );
            }
         }
      }
   }

   // make sure we now have a name
   if ( nsname == NULL ) {
      LOG( LOG_ERR, "found a namespace without a 'name' value\n" );
      errno = EINVAL;
      return -1;
   }

   // make sure we don't have any forbidden name characters
   char* parse = nsname;
   for ( ; *parse != '\0'; parse++ ) {
      if ( *parse == '/' ) {
         LOG( LOG_ERR, "found namespace \"%s\" with disallowed '/' character in name value\n", nsname );
         errno = EINVAL;
         free( nsname );
         return -1;
      }
   }

   // iterate over all attributes again, looking for remote namespace values and anything unusual
   char* reponame = NULL;
   for( attr = nsroot->properties; attr; attr = attr->next ) {
      if ( attr->type == XML_ATTRIBUTE_NODE ) {
         if ( rns  &&  strncmp( (char*)attr->name, "repo", 5 ) == 0 ) {
            if ( reponame ) {
               // we already found a 'repo'
               LOG( LOG_WARNING, "encountered a duplicate 'repo' value on NS \"%s\"\n", nsname );
               continue;
            }
            // we've found our 'repo' attribute
            if ( attr->children->type == XML_TEXT_NODE  &&  attr->children->content != NULL ) {
               reponame = strdup( (char*)attr->children->content );
            }
            else {
               LOG( LOG_WARNING, "encountered unrecognized 'repo' attribute value for NS \"%s\"\n", nsname );
            }

         }
         else if ( strncmp( (char*)attr->name, "name", 5 ) ) {
            LOG( LOG_WARNING, "encountered unrecognized \"%s\" attribute of NS \"%s\"\n", (char*)attr->name, nsname );
         }
      }
      else {
         LOG( LOG_WARNING, "encountered unrecognized tag on NS \"%s\"\n", nsname );
      }
   }

   // additional checks for remote namespaces
   if ( rns ) {
      // make sure this isn't a remote namespace at the root of a repo
      if ( pnamespace == NULL ) {
         LOG( LOG_ERR, "remote namespace \"%s\" was found at the root of a repo\n", nsname );
         free( nsname );
         errno = EINVAL;
         return -1;
      }
      // make sure we actually found a 'repo' name
      if ( reponame == NULL ) {
         LOG( LOG_ERR, "remote namespace \"%s\" is missing a 'repo' value\n", nsname );
         free( nsname );
         errno = EINVAL;
         return -1;
      }
   }

   // time to populate the NS name
   nsnode->name = nsname;

   // weight of these nodes will always be zero
   nsnode->weight = 0;

   // finally time to allocate the 'real' namespace struct
   nsnode->content = malloc( sizeof( marfs_ns ) );
   if ( nsnode->content == NULL ) {
      LOG( LOG_ERR, "failed to allocate space for namespace \"%s\"\n", nsname );
      free( nsname );
      if ( rns ) free( reponame );
      return -1;
   }
   marfs_ns* ns = (marfs_ns*)nsnode->content; //shorthand ref

   // set parent values
   ns->prepo = prepo;
   ns->pnamespace = pnamespace;
   if ( rns ) {
      // we'll indicate this is a remote namespace by NULLing out the prepo ref
      ns->prepo = NULL;
      // we need to know what repo to look for the full namespace def in; so...
      //  I've jammed the string pointer into the subspace pointer
      // TODO : this is pretty ugly, is there a better place to store this info?
      ns->subspaces = (HASH_TABLE)reponame;
      // make sure there aren't any other XML nodes lurking below here
      if ( nsroot->children ) {
         LOG( LOG_WARNING, "ignoring all xml elements below remote NS \"%s\"\n", nsname );
      }
      // for remote namespaces, we're done; no other values are relevant
      return 0;
   }

   // set some default namespace values
   ns->enforcefq = 0;
   ns->fquota = 0;
   ns->enforcedq = 0;
   ns->dquota = 0;
   ns->iperms = NS_NOACCESS;
   ns->bperms = NS_NOACCESS;

   // real namespaces may have additional namespace defs below them
   int subspcount = count_nodes( nsroot->children, "ns" );
   subspcount += count_nodes( nsroot->children, "rns" );
   ns->subspaces = NULL;
   HASH_NODE* subspacelist = NULL;
   if ( subspcount ) {
      subspacelist = malloc( sizeof( HASH_NODE ) * subspcount );
      if ( subspacelist == NULL ) {
         LOG( LOG_ERR, "failed to allocate space for subspace list of NS \"%s\"\n", nsname );
         free( nsname );
         free( ns );
         return -1;
      }
   }

   // iterate over children, populating perms, quotas, and subspaces
   xmlNode* subnode = nsroot->children;
   int allocsubspaces = 0;
   int retval = 0;
   for ( ; subnode; subnode = subnode->next ) {
      if ( subnode->type == XML_ELEMENT_NODE ) {
         if ( strncmp( (char*)subnode->name, "quotas", 7 ) == 0 ) {
            // parse NS quota info
            if( parse_quotas( &(ns->enforcefq), &(ns->fquota), &(ns->enforcedq), &(ns->dquota), subnode->children ) ) {
               LOG( LOG_ERR, "failed to parse quota info for NS \"%s\"\n", nsname );
               retval = -1;
               break;
            }
         }
         else if ( strncmp( (char*)subnode->name, "perms", 6 ) == 0 ) {
            // parse NS perm info
            if ( parse_perms( &(ns->iperms), &(ns->bperms), subnode->children ) ) {
               LOG( LOG_ERR, "failed to parse perm info for NS \"%s\"\n", nsname );
               retval = -1;
               break;
            }
         }
         else if ( strncmp( (char*)subnode->name, "ns", 3 ) == 0  ||  
                   strncmp( (char*)subnode->name, "rns", 4 ) == 0  ) {
            // allocate a subspace
            if ( allocsubspaces >= subspcount ) {
               LOG( LOG_ERR, "insufficient subspace allocation for NS \"%s\"\n", nsname );
               retval = -1;
               errno = EFAULT;
               break;
            }
            if ( create_ns( subspacelist + allocsubspaces, ns, prepo, subnode ) ) {
               LOG( LOG_ERR, "failed to initialize subspace of NS \"%s\"\n", nsname );
               retval = -1;
               break;
            }
            allocsubspaces++;
         }
         else {
            LOG( LOG_ERR, "encountered unrecognized \"%s\" child node of NS \"%s\"\n", (char*)subnode->name, nsname );
            retval = -1;
            errno = EINVAL;
            break;
         }
      }
      else {
         LOG( LOG_ERR, "encountered unrecognized xml child node of NS \"%s\"\n", nsname );
         retval = -1;
         errno = EINVAL;
         break;
      }
   }

   // ensure we found all of the subspaces we allocated references for
   if ( allocsubspaces != subspcount ) {
      LOG( LOG_ERR, "encountered a reduced count of subspaces for NS \"%s\"\n", nsname );
      retval = -1;
      errno = EFAULT;
   }

   // allocate our subspace hash table only if we aren't already aborting
   if ( retval == 0 ) {
      ns->subspaces = hash_init( subspacelist, subspcount );
      if ( ns->subspaces == NULL ) {
         LOG( LOG_ERR, "failed to create the subspace table of NS \"%s\"\n", nsname );
         retval = -1;
      }
   }

   // catch any failure cases, and free all memory
   if ( retval != 0 ) {
      while ( allocsubspaces ) {
         allocsubspaces--;
         if ( free_ns( subspacelist + allocsubspaces ) ) {
            // nothing to do besides complain; we're already failing out
            LOG( LOG_WARNING, "failed to free subspace %d of NS \"%s\"\n", nsname );
         }
      }
      if ( subspacelist ) { free( subspacelist ); }
      free( nsname );
      free( ns );
      return retval;
   }

   return 0;
}



int create_repo( marfs_repo* reporef, XmlNode* reporoot );
int establish_nshierarchy( marfs_config* config );


//   -------------   EXTERNAL FUNCTIONS    -------------


