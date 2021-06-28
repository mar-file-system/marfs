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
 *             <max_size>1G</max_size>
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


marfs_ns* find_namespace( HASH_NODE* nslist, size_t nscount, const char* nsname ) {
   // iterate over the elements of the nslist, searching for a matching name
   size_t index;
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


int parse_size_node( size_t* target, XmlNode* node ) {
   // note the node name, just for logging messages
   char* nodename = (char*)node->name;
   // check for an included value
   if ( node->children != NULL  &&
        node->children->type == XML_TEXT_NODE  &&
        node->children->content != NULL ) {
      char* valuestr = (char*)node->children->content;
      size_t unitmult = 1;
      char* endptr = NULL;
      unsigned long long parsevalue = strtoull( valuestr, &(endptr), 10 );
      // check for any trailing unit specification
      if ( *endptr != '\0' ) {
         if ( *endptr == 'K' ) { unitmult = 1024; }
         else if ( *endptr == 'M' ) { unitmult = (1024 * 1024); }
         else if ( *endptr == 'G' ) { unitmult = (1024 * 1024 * 1024); }
         else if ( *endptr == 'T' ) { unitmult = (1024 * 1024 * 1024 * 1024); }
         else if ( *endptr == 'P' ) { unitmult = (1024 * 1024 * 1024 * 1024 * 1024); }
         else {
            LOG( LOG_ERR, "encountered unrecognized character in \"%s\" value: \"%c\"", nodename, *endptr );
            return -1;
         }
         // check for unacceptable trailing characters
         endptr++;
         if ( *endptr != '\0' ) {
            LOG( LOG_ERR, "encountered unrecognized trailing character in \"%s\" value: \"%c\"", nodename, *endptr );
            return -1;
         }
      }
      if ( (parsevalue * unitmult) >= SIZE_MAX ) {  // check for possible overflow
         LOG( LOG_ERR, "specified \"%s\" value is too large to store: \"%s\"\n", nodename, valuestr );
         return -1;
      }
      // actually store the value
      *target = parsevalue;
      return 0;
   }
   LOG( LOG_ERR, "failed to identify a value string within the \"%s\" definition\n", nodename );
   return -1;
}


int parse_int_node( int* target, XmlNode* node ) {
   // note the node name, just for logging messages
   char* nodename = (char*)node->name;
   // check for an included value
   if ( node->children != NULL  &&
        node->children->type == XML_TEXT_NODE  &&
        node->children->content != NULL ) {
      char* valuestr = (char*)node->children->content;
      char* endptr = NULL;
      unsigned long long parsevalue = strtoull( valuestr, &(endptr), 10 );
      // check for any trailing unit specification
      if ( *endptr != '\0' ) {
         LOG( LOG_ERR, "encountered unrecognized trailing character in \"%s\" value: \"%c\"", nodename, *endptr );
         return -1;
      }
      if ( parsevalue >= INT_MAX ) {  // check for possible overflow
         LOG( LOG_ERR, "specified \"%s\" value is too large to store: \"%s\"\n", nodename, valuestr );
         return -1;
      }
      // actually store the value
      *target = parsevalue;
      return 0;
   }
   LOG( LOG_ERR, "failed to identify a value string within the \"%s\" definition\n", nodename );
   return -1;
}


int parse_quotas( char* enforcefq, size_t* fquota, char* enforcedq, size_t* dquota, xmlNode* quotaroot ) {
   // iterate over nodes at this level
   for ( ; quotaroot; quotaroot = quotaroot->next ) {
      if ( quotaroot->type == XML_ELEMENT_NODE ) {
         // determine if we're parsing file or data quotas
         if ( strncmp( (char*)quotaroot->name, "files", 6 ) == 0 ) {
            if ( *fquota != 0  ||  *enforcefq != 0 ) {
               // this is a duplicate 'files' def
               LOG( LOG_ERR, "encountered duplicate 'files' quota set\n" );
               return -1;
            }
            // parse the value and set quota enforcement
            if ( parse_size_node( fquota, quotaroot ) ) {
               LOG( LOG_ERR, "failed to parse 'files' quota value\n" );
               return -1;
            }
            *enforcefq = 1;
         }
         else if ( strncmp( (char*)quotaroot->name, "data", 5 ) == 0 ) {
            if ( *dquota != 0  ||  *enforcedq != 0 ) {
               // this is a duplicate 'data' def
               LOG( LOG_ERR, "encountered duplicate 'data' quota set\n" );
               return -1;
            }
            // parse the value and set quota enforcement
            if ( parse_size_node( dquota, quotaroot ) ) {
               LOG( LOG_ERR, "failed to parse 'data' quota value\n" );
               return -1;
            }
            *enforcedq = 1;
         }
         else {
            LOG( LOG_ERR, "encountered unexpected quota sub-node: \"%s\"\n", (char*)quotaroot->name );
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


int free_namespace( HASH_NODE* nsnode ) {
   // we are assuming this function will ONLY be called on completed NS structures
   //   As in, this function will not check for certain NULL pointers, like name
   marfs_ns* ns = (marfs_ns*)nsnode->content;
   const char* nsname = nsnode->name;
   int retval = 0;
   if ( ns->subspaces ) {
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
            if ( free_namespace( subspacelist[subspaceindex] ) ) {
               LOG( LOG_WARNING, "failed to free subspace of NS \"%s\"\n", nsname );
               retval = -1;
            }
         }
         // regardless, we need to free the list of HASH_NODEs
         free( subspacelist );
      }
   }
   // free the namespace id string
   free( ns->idstr );
   // free the namespace itself
   free( ns );
   // free the namespace name
   free( nsname );
   return retval;
}
   

int create_namespace( HASH_NODE* nsnode, marfs_ns* pnamespace, marfs_repo* prepo, xmlNode* nsroot ) {
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
      if ( *parse == '/'  ||  *parse == '|'  ||  *parse == '('  ||  *parse == ')'  ||  *parse == '#' ) {
         LOG( LOG_ERR, "found namespace \"%s\" with disallowed '%c' character in name value\n", nsname, *parse );
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
      // make sure we actually found a 'repo' name
      if ( reponame == NULL ) {
         LOG( LOG_ERR, "remote namespace \"%s\" is missing a 'repo' value\n", nsname );
         free( nsname );
         errno = EINVAL;
         return -1;
      }
      // make sure this isn't a remote namespace at the root of a repo
      if ( pnamespace == NULL ) {
         LOG( LOG_ERR, "remote namespace \"%s\" was found at the root of a repo\n", nsname );
         free( nsname );
         free( reponame );
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
      // we need to know what repo to look for the full namespace def in; so jam it in as the id string
      ns->idstr = reponame;
      // make sure there aren't any other XML nodes lurking below here
      if ( nsroot->children ) {
         LOG( LOG_WARNING, "ignoring all xml elements below remote NS \"%s\"\n", nsname );
      }
      // for remote namespaces, we're done; no other values are relevant
      return 0;
   }

   // populate the namespace id string
   int idstrlen = strlen(nsname) + 2; // nsname plus '|' prefix and NULL terminator
   if ( pnamespace ) { idstrlen += strlen( pnamespace->idstr ); } // append to parent name
   else { idstrlen += strlen( prepo->name ); } // or parent repo name, if we are at the top
   ns->idstr = malloc( sizeof(char) * idstrlen );
   if ( ns->idstr == NULL ) {
      LOG( LOG_ERR, "failed to allocate space for the id string of NS \"%s\"\n", nsname );
      free( nsname );
      free( ns );
      return -1;
   }
   int prres = 0;
   if ( pnamespace ) {
      prres = snprintf( ns->idstr, idstrlen, "%s|%s", pnamespace->idstr, nsname );
   }
   else {
      prres = snprintf( ns->idstr, idstrlen, "%s|%s", prepo->name, nsname );
   }
   // check for successful print (probably unnecessary)
   if ( prres != (idstrlen - 1) ) {
      LOG( LOG_ERR, "failed to populate id string of NS \"%s\"\n", nsname );
      free( ns->idstr );
      free( nsname );
      free( ns );
      return -1;
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
         free( ns->idstr );
         free( nsname );
         free( ns );
         return -1;
      }
   }

   // iterate over children, populating perms, quotas, and subspaces
   xmlNode* subnode = nsroot->children;
   size_t allocsubspaces = 0;
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
            // ensure we haven't encountered more nodes than expected
            if ( allocsubspaces >= subspcount ) {
               LOG( LOG_ERR, "insufficient subspace allocation for NS \"%s\"\n", nsname );
               retval = -1;
               errno = EFAULT;
               break;
            }
            // allocate a subspace
            HASH_NODE* subnsnode = subspacelist + allocsubspaces;
            if ( create_ns( subnsnode, ns, prepo, subnode ) ) {
               LOG( LOG_ERR, "failed to initialize subspace of NS \"%s\"\n", nsname );
               retval = -1;
               break;
            }
            // ensure we don't have any name collisions
            if ( find_namespace( subspacelist, allocsubspaces, subnsnode->name ) ) {
               LOG( LOG_ERR, "encountered subspace name collision: \"%s\"\n", subnsnode->name );
               free_namespace( subnsnode );
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

   // check for namespace collisions
   int index;
   for ( index = 1; index < allocsubspaces; index++ ) {
      int prevns = 0;
      for ( ; prevns < index; prevns++ ) {
         if ( strcmp( subspacelist[prevns]->name, subspacelist[index]->name ) == 0 ) {
            LOG( LOG_ERR, "detected repeated \"%s\" subspace name below NS \"%s\"\n", nsname );
            retval = -1;
            errno = EINVAL;
            break;
         }
      }
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
         if ( free_namespace( subspacelist + allocsubspaces ) ) {
            // nothing to do besides complain; we're already failing out
            LOG( LOG_WARNING, "failed to free subspace %d of NS \"%s\"\n", nsname );
         }
      }
      if ( subspacelist ) { free( subspacelist ); }
      free( ns->idstr );
      free( nsname );
      free( ns );
      return retval;
   }

   return 0;
}


HASH_TABLE create_distribution_table( int* count, XmlNode* distroot ) {
   // get the node name, just for the sake of log messages
   char* distname = (char*)distroot->name;
   // iterate over attributes, looking for cnt and dweight values
   int dweight = 1;
   size_t nodecount = 0;
   xmlAttr* attr = distroot->properties;
   for ( ; attr; attr = attr->next ) {
      char*  attrtype = NULL;
      char* attrval = NULL;
      if ( attr->type == XML_ATTRIBUTE_NODE ) {
         // attempt to stash the attribute type and value, for later parsing
         attrtype = (char*)attr->name;
         if ( attr->children->type == XML_TEXT_NODE  &&  attr->children->content != NULL ) {
            attrval = (char*)attr->children->content;
         }
         else {
            LOG( LOG_ERR, "encountered an unrecognized '%s' value for %s distribution\n", attrtype, distname );
            errno = EINVAL;
            return NULL;
         }

         // perform some checks, specific to the attribute type
         if ( strncmp( attrtype, "cnt", 4 ) == 0  &&  nodecount != 0 ) {
            // we already found a 'cnt'
            LOG( LOG_ERR, "encountered a duplicate 'cnt' value for %s distribution\n", distname );
            errno = EINVAL;
            return NULL;
         }
         else if ( strncmp( attrtype, "dweight", 8 ) == 0  &&  dweight != 1 ) {
            // we already found a 'dweight' ( note - this will fail to detect prior dweight='1' )
            LOG( LOG_ERR, "encountered a duplicate 'dweight' value for %s distribution\n", distname );
            errno = EINVAL;
            return NULL;
         }
         else {
            // reject any unrecognized attributes
            LOG( LOG_WARNING, "ignoring unrecognized '%s' attr of %s distribution\n", attrtype, distname );
            attrval = NULL; // don't try to interpret
         }
      }
      else {
         // not even certain this is possible; warn just in case our config is in a very odd state
         LOG( LOG_WARNING, "encountered an unrecognized property of %s distribution\n", distname );
      }

      // if we found a new attribute value, parse it
      if ( attrval ) {
         char* endptr = NULL;
         unsigned long long parseval = strtoull( attrval, &(endptr), 10 );
         if ( *endptr != '\0' ) {
            LOG( LOG_ERR, "detected a trailing '%c' character in '%s' value for %s distribution\n", *endptr, attrtype, distname );
            errno = EINVAL;
            return NULL;
         }
         // check for possible value overflow
         if ( parseval >= INT_MAX ) {
            LOG( LOG_ERR, "%s distribution has a '%s' value which exceeds memory bounds: \"%s\"\n", disname, attrtype, attrval );
            errno = EINVAL;
            return NULL;
         }

         // assign value to the appropriate var
         // Note -- this check can be lax, as we already know attrype == "dweight" OR "cnt"
         if ( *attrtype = 'c' ) {
            nodecount = parseval;
         }
         else {
            dweight = parseval;
         }
      }
   }
   // all attributes of this distribution have been parsed
   // make sure we have the required 'cnt' value
   if ( nodecount == 0 ) {
      LOG( LOG_ERR, "failed to identify a valid 'cnt' of %s distribution\n", distname );
      errno = EINVAL;
      return NULL;
   }

   // allocate space for all hash nodes
   HASH_NODE* nodelist = malloc( sizeof(HASH_NODE) * nodecount );
   if ( nodelist == NULL ) {
      LOG( LOG_ERR, "failed to allocate space for hash nodes of %s distribution\n", distname );
      return NULL;
   }

   // populate all node names and set weights to default
   size_t curnode;
   for ( curnode = 0; curnode < nodecount; curnode++ ) {
      nodelist[curnode]->content = NULL; // we don't care about content
      nodelist[curnode]->weight = dweight; // every node gets the default weight, for now
      // identify the number of characers needed for this nodename via some cheeky use of snprintf
      int namelen = snprintf( NULL, 0, "%zu", curnode );
      nodelist[curnode]->name = malloc( sizeof(char) * (namelen + 1) );
      if ( nodelist[curnode]->name == NULL ) {
         LOG( LOG_ERR, "failed to allocate space for node names of %s distribution\n", distname );
         break;
      }
      if ( snprintf( nodelist[curnode]->name, namelen + 1, "%zu", curnode ) > namelen ) {
         LOG( LOG_ERR, "failed to populate nodename \"%zu\" of %s distribution\n", curnode, distname );
         errno = EFAULT;
         free( nodelist[curnode]->name );
         break;
      }
   }
   // check for any 'break' conditions and clean up our allocated memory
   if ( curnode != nodecount ) {
      // free all name strings
      size_t freenode;
      for ( freenode = 0; freenode < curnode; freenode++ ) {
         free( nodelist[freenode]->name );
      }
      // free our nodelist and terminate
      free( nodelist );
      return NULL;
   }

   // now, we need to check for specifically defined weight values
   char errorflag = 0;
   if ( distroot->children != NULL  &&
        distroot->children->type == XML_TEXT_NODE  &&
        distroot->children->content != NULL ) {
      char* weightstr = (char*)distroot->children->content;
      size_t tgtnode = nodecount;
      while ( *weightstr != '\0' ) {
         char* endptr;
         unsigned long long parseval = strtoull( weightstr, &(endptr), 10 );
         // perform checks for invalid trailing characters
         if ( tgtnode == nodecount  &&  *endptr != '=' ) {
            LOG( LOG_ERR, "improperly formatted node definition for %s distribution\n", distname );
            errno = EINVAL;
            errorflag = 1;
            break;
         }
         else if ( tgtnode != nodecount  &&  *endptr != ';' ) {
            LOG( LOG_ERR, "improperly formatted weight value of node %zu for %s distribution\n", tgtnode, distname );
            errno = EINVAL;
            errorflag = 1;
            break;
         }

         // attempt to assign our parsed numeric value to the proper var
         if ( tgtnode == nodecount ) {
            // this is a definition of target node
            // check for an out of bounds value
            if ( parseval >= nodecount ) {
               LOG( LOG_ERR, "%s distribution has a node value which exceeds the defined limit of %zu\n", distname, nodecount );
               errno = EINVAL;
               errorflag = 1;
               break;
            }
            tgtnode = parseval; // assign the tgtnode value
         }
         else {
            // this is a weight definition
            // check for an out of bounds value
            if ( parseval >= INT_MAX ) {
               LOG( LOG_ERR, "%s distribution has a weight value for node %zu which exceeds memory limits\n", distname, tgtnode );
               errno = EINVAL;
               errorflag = 1;
               break;
            }
            nodelist[tgtnode]->weight = parseval; // assign the specified weight to the target node
            tgtnode = nodecount; // reset our target, so we know to expect a new one
         }

         // if we've gotten here, endptr either references '=' or ';'
         // either way, we need to go one character further to find our next def
         weightstr = (endptr + 1);

      }
      if ( tgtnode != nodecount ) {
         LOG( LOG_ERR, "%s distribution has node %zu reference, but no defined weight value\n", disname, tgtnode );
         errno = EINVAL;
         errorflag = 1;
      }
   }
   // check if we hit any error
   if ( errorflag ) {
      // free all name strings
      size_t freenode;
      for ( freenode = 0; freenode < nodecount; freenode++ ) {
         free( nodelist[freenode]->name );
      }
      // free our nodelist and terminate
      free( nodelist );
      return NULL;
   }

   // finally, initialize the hash table
   HASH_TABLE table = hash_init( nodelist, nodecount );
   // verify success
   if ( table == NULL ) {
      LOG( LOG_ERR, "failed to initialize hash table for %s distribution\n", distname );
      // free all name strings
      size_t freenode;
      for ( freenode = 0; freenode < nodecount; freenode++ ) {
         free( nodelist[freenode]->name );
      }
      // free our nodelist and terminate
      free( nodelist );
      return NULL;
   }

   // populate the provided count value
   *count = (int)nodecount;
   // return the created table
   return table;
}


int free_repo( marfs_repo* repo ) {
   // note any errors
   int retval = 0;

   // free metadata scheme components
   if ( repo->metascheme.mdal ) {
      if ( mdal_term( repo->metascheme.mdal ) ) {
         LOG( LOG_WARNING, "failed to terminate MDAL of \"%s\" repo\n", repo->name );
         retval = -1;
      }
   }
   size_t nodecount;
   HASH_NODE* nodelist;
   if ( hash_term( ms->reftable, &(nodelist), &(nodecount) ) ) {
      LOG( LOG_WARNING, "failed to free the reference path hash table of \"%s\" repo\n", repo->name );
      retval = -1;
   }
   else {
      // free all hash nodes
      size_t nodeindex = 0;
      for( ; nodeindex < nodecount; nodeindex++ ) {
         free( nodelist[nodeindex]->name );
      }
      free( nodelist );
   }
   int nsindex;
   for ( nsindex = 0; nsindex < repo->metascheme.nscount; nsindex++ ) {
      // recursively free all namespaces below this repo
      if ( free_namespace( repo->metascheme.nslist + nsindex ) ) {
         LOG( LOG_WARNING, "failed to free NS %d of \"%s\" repo\n", nsindex, repo->name );
         retval = -1;
      }
   }
   if ( repo->metascheme.nslist ) { free( repo->metascheme.nslist ); }

   // free data scheme components
   if ( repo->datascheme.nectxt ) {
      if ( ne_term( repo->datascheme.nectxt ) ) {
         LOG( LOG_WARNING, "failed to terminate NE context of \"%s\" repo\n", repo->name );
         retval = -1;
      }
   }
   int target;
   for( target = 0; target < 3; target++ ) {
      HASH_TABLE ttable;
      if ( target == 0 ) { ttable = repo->datascheme.podtable; }
      else if ( target == 1 ) { ttable = repo->datascheme.captable; }
      else { ttable = repo->datascheme.scattertable; }
      // skip this table if it was never allocated
      if ( ttable == NULL ) { continue; }
      // otherwise, free the table
      if ( hash_term( ttable, &(nodelist), &(nodecount) ) ) {
         LOG( LOG_WARNING, "failed to free a distribution hash table of \"%s\" repo\n", repo->name );
         retval = -1;
      }
      else {
         // free all hash nodes
         size_t nodeindex = 0;
         for( ; nodeindex < nodecount; nodeindex++ ) {
            free( nodelist[nodeindex]->name );
         }
         free( nodelist );
      }
   }

   // finally, free the repo structure itself
   free( repo );

   return retval;
}


int parse_datascheme( marfs_ds* ds, XmlNode* dataroot ) {
   XmlNode* dalnode = NULL;
   ne_location maxloc = { .pod = 0, .cap = 0, .scatter = 0 };
   // iterate over nodes at this level
   for ( ; dataroot; dataroot = dataroot->next ) {
      if ( dataroot->type == XML_ELEMENT_NODE ) {
         // first, check for a DAL definition
         if ( strncmp( (char*)dataroot->name, "DAL", 4 ) == 0 ) {
            if ( dalnode ) {
               LOG( LOG_ERR, "detected duplicate DAL definition\n" );
               return -1;
            }
            dalnode = dataroot; // save our DAL node reference for later
            continue;
         }
         // check for an 'enabled' attr
         xmlAttr* attr = dataroot->properties;
         char enabled = 2; // assume enabled, but track if we actually get a value
         if ( attr ) {
            if ( attr->type == XML_ATTRIBUTE_NODE ) {
               if ( strncmp( (char*)attr->name, "enabled", 8 ) == 0 ) {
                  if ( attr->children->type == XML_TEXT_NODE  &&  attr->children->content != NULL ) {
                     if ( strncmp( (char*)attr->children->content, "no", 3 ) == 0 ) {
                        enabled = 0;
                     }
                     else if ( strncmp( (char*)attr->children->content, "yes", 4 ) == 0 ) {
                        enabled = 1;
                     }
                  }
               }
            }
            // if we found any attribute value, ensure it made sense
            if ( enabled == 2 ) {
               LOG( LOG_ERR, "encountered an unrecognized attribute of a '%s' node within a 'data' definition\n", (char*)dataroot->name );
               return -1;
            }
            // make sure this is the only attribute
            if ( attr->next ) {
               LOG( LOG_ERR, "detected trailing attributes of a '%s' node within a 'data' definition\n", (char*)dataroot->name );
               return -1;
            }
            // if this node has been disabled, don't even bother parsing it
            if ( !(enabled) ) { continue; }
         }
         // determine what definition we are parsing
         XmlNode* subnode = dataroot->children;
         if ( strncmp( (char*)dataroot->name, "protection", 11 ) == 0 ) {
            // iterate over child nodes, populating N/E/PSZ
            char haveN = 0;
            char haveE = 0;
            char haveP = 0;
            for( ; subnode; subnode->next ) {
               if ( subnode->type != XML_ELEMENT_NODE ) {
                  LOG( LOG_ERR, "encountered unknown node within a 'protection' definition\n" );
                  return -1;
               }
               if ( strncmp( (char*)dataroot->name, "N", 2 ) == 0 ) {
                  haveN = 1;
                  if( parse_int_node( &(ds->protection.N), dataroot ) ) {
                     LOG( LOG_ERR, "failed to parse 'N' value within a 'protection' definition\n" );
                     return -1;
                  }
               }
               else if ( strncmp( (char*)dataroot->name, "E", 2 ) == 0 ) {
                  haveE = 1;
                  if( parse_int_node( &(ds->protection.E), dataroot ) ) {
                     LOG( LOG_ERR, "failed to parse 'E' value within a 'protection' definition\n" );
                     return -1;
                  }
               }
               else if ( strncmp( (char*)dataroot->name, "PSZ", 4 ) == 0 ) {
                  haveP = 1;
                  if( parse_int_node( &(ds->protection.partsz), dataroot ) ) {
                     LOG( LOG_ERR, "failed to parse 'PSZ' value within a 'protection' definition\n" );
                     return -1;
                  }
               }
               else {
                  LOG( LOG_ERR, "encountered an unrecognized \"%s\" node within a 'protection' definition\n", (char*)dataroot->name );
                  return -1;
               }
            }
            // verify that all expected values were populated
            if ( !(haveN)  ||  !(haveE)  ||  !(haveP) ) {
               LOG( LOG_ERR, "encountered a 'protection' definition without all N/E/PSZ values\n" );
               return -1;
            }
         }
         else if ( strncmp( (char*)dataroot->name, "packing", 8 ) == 0 ) {
            // iterate over child nodes, populating max_files
            char haveM = 0;
            for( ; subnode; subnode->next ) {
               if ( subnode->type != XML_ELEMENT_NODE ) {
                  LOG( LOG_ERR, "encountered unknown node within a 'packing' definition\n" );
                  return -1;
               }
               if ( strncmp( (char*)dataroot->name, "max_files", 10 ) == 0 ) {
                  haveM = 1;
                  if( parse_size_node( &(ds->objfiles), dataroot ) ) {
                     LOG( LOG_ERR, "failed to parse 'max_files' value within a 'packing' definition\n" );
                     return -1;
                  }
               }
               else {
                  LOG( LOG_ERR, "encountered an unrecognized \"%s\" node within a 'packing' definition\n", (char*)dataroot->name );
                  return -1;
               }
            }
            // verify that all expected values were populated
            if ( !(haveM) ) {
               LOG( LOG_ERR, "encountered a 'packing' definition without a 'max_files' value\n" );
               return -1;
            }
         }
         else if ( strncmp( (char*)dataroot->name, "chunking", 9 ) == 0 ) {
            // iterate over child nodes, populating max_size
            char haveM = 0;
            for( ; subnode; subnode->next ) {
               if ( subnode->type != XML_ELEMENT_NODE ) {
                  LOG( LOG_ERR, "encountered unknown node within a 'chunking' definition\n" );
                  return -1;
               }
               if ( strncmp( (char*)dataroot->name, "max_size", 9 ) == 0 ) {
                  haveM = 1;
                  if( parse_size_node( &(ds->objsize), dataroot ) ) {
                     LOG( LOG_ERR, "failed to parse 'max_size' value within a 'chunking' definition\n" );
                     return -1;
                  }
               }
               else {
                  LOG( LOG_ERR, "encountered an unrecognized \"%s\" node within a 'chunking' definition\n", (char*)dataroot->name );
                  return -1;
               }
            }
            // verify that all expected values were populated
            if ( !(haveM) ) {
               LOG( LOG_ERR, "encountered a 'chunking' definition without a 'max_size' value\n" );
               return -1;
            }
         }
         else if ( strncmp( (char*)dataroot->name, "distribution", 13 ) == 0 ) {
            // iterate over child nodes, creating our distribution tables
            for( ; subnode; subnode->next ) {
               if ( subnode->type != XML_ELEMENT_NODE ) {
                  LOG( LOG_ERR, "encountered unknown node within a 'distribution' definition\n" );
                  return -1;
               }
               if ( strncmp( (char*)dataroot->name, "pods", 5 ) == 0 ) {
                  if ( (ds->podtable = create_dist_table( &(maxloc.pod), dataroot )) = NULL ) {
                     LOG( LOG_ERR, "failed to create 'pods' distribution table\n" );
                     return -1;
                  }
               }
               else if ( strncmp( (char*)dataroot->name, "caps", 5 ) == 0 ) {
                  if ( (ds->podtable = create_dist_table( &(maxloc.cap), dataroot )) = NULL ) {
                     LOG( LOG_ERR, "failed to create 'caps' distribution table\n" );
                     return -1;
                  }
               }
               else if ( strncmp( (char*)dataroot->name, "scatters", 9 ) == 0 ) {
                  if ( (ds->podtable = create_dist_table( &(maxloc.scatter), dataroot )) = NULL ) {
                     LOG( LOG_ERR, "failed to create 'scatters' distribution table\n" );
                     return -1;
                  }
               }
               else {
                  LOG( LOG_ERR, "encountered an unrecognized \"%s\" node within a 'distribution' definition\n", (char*)dataroot->name );
                  return -1;
               }
            }
         }
         else {
            LOG( LOG_ERR, "encountered unexpected 'data' sub-node: \"%s\"\n", (char*)dataroot->name );
            return -1;
         }
      }
      else {
         // don't know what this is supposed to be
         LOG( LOG_ERR, "encountered unknown node within a 'data' definition\n" );
         return -1;
      }
   }
   // now that we're done iterating through our config, make sure we found a DAL definition
   if ( !(dalnode) ) {
      LOG( LOG_ERR, "failed to locate a DAL definition\n" );
      return -1;
   }
   // attempt to create our NE context
   if ( (ds->nectxt = ne_init( dalnode, maxloc, ds->protection.N + ds->protection.E )) == NULL ) {
      LOG( LOG_ERR, "failed to initialize an NE context\n" );
      return -1;
   }

   return 0;
}


int parse_metascheme( marfs_ds* ms, marfs_repo* repo, XmlNode* metaroot ) {
   XmlNode* mdalnode = NULL;
   // iterate over nodes at this level
   for ( ; metaroot; metaroot = metaroot->next ) {
      // make sure this is a real ELEMENT_NODE
      if ( metaroot->type != XML_ELEMENT_NODE ) {
         LOG( LOG_ERR, "detected unrecognized node within a 'meta' definition\n" );
         return -1;
      }
      // first, check for an MDAL definition
      if ( strncmp( (char*)metaroot->name, "MDAL", 5 ) == 0 ) {
         if ( mdalnode ) {
            LOG( LOG_ERR, "detected duplicate MDAL definition\n" );
            return -1;
         }
         mdalnode = metaroot; // save our MDAL node reference for later
         continue;
      }
      // determine what definition we are parsing
      xmlAttr* attr = metaroot->properties;
      XmlNode* subnode = metaroot->children;
      if ( strncmp( (char*)metaroot->name, "namespaces", 11 ) == 0 ) {
         // check if we have already created a reference table, as this indicates a duplicate node
         if ( ms->reftable ) {
            LOG( LOG_ERR, "detected a duplicate 'namespaces' definition\n" );
            return -1;
         }
         for ( ; attr; attr = attr->next ) {
            int attrvalue = 0;
            if ( attr->type == XML_ATTRIBUTE_NODE ) {
               if ( attr->children->type == XML_TEXT_NODE  &&  attr->children->content != NULL ) {
                  // all attributes of this node should be integer strings
                  char* endptr = NULL;
                  unsigned long long parseval = strtoull( (char*)attr->children->content, &(endptr), 10 );
                  if ( parseval >= INT_MAX ) {
                     LOG( LOG_ERR, "value of \"%s\" attribute is to large to store\n", (char*)attr->name );
                     return -1;
                  }
                  if ( *endptr != '\0' ) {
                     LOG( LOG_ERR, "detected trailing '%c' character in value of \"%s\" attribute\n", (char*)attr->name );
                     return -1;
                  }
                  attrvalue = parseval;
               }
            }
            // check that we have a sensible attribute value
            if ( attrvalue <= 0 ) {
               LOG( LOG_ERR, "inappropriate value for a \"%s\" attribute\n", (char*)attr->name );
               return -1;
            }
            // check which value this attribute provides
            if ( strncmp( (char*)attr->name, "rbreadth", 9 ) == 0 ) {
               ms->refbreadth = attrvalue;
            }
            else if ( strncmp( (char*)attr->name, "rdepth", 7 ) == 0 ) {
               ms->refdepth = attrvalue;
            }
            else if ( strncmp( (char*)attr->name, "rdigits", 8 ) == 0 ) {
               ms->refdigits = attrvalue;
            }
            else {
               LOG( LOG_ERR, "encountered an unrecognized attribute of a 'namespaces' node: \"%s\"\n", (char*)attr->name );
               return -1;
            }
         }
         // create a string to hold temporary reference paths
         int breadthdigits = 1;
         int tmpbreadth = ms->refbreadth;
         while ( ( (int)( tmpbreadth = tmpbreadth / 10 ) ) ) { breadthdigits++; }
         size_t rpathlen = ( ms->refdepth * (breadthdigits + 1) ) + 1;
         char* rpathtmp = malloc( sizeof(char) * rpathlen ); // used to populate node name strings
         if ( rpathtmp == NULL ) {
            LOG( LOG_ERR, "failed to allocate space for namespace refpaths\n" );
            return -1;
         }
         // create an array of integers to hold reference indexes
         int* refvals = malloc( sizeof(int) * ms->refdepth );
         if ( refvals == NULL ) {
            LOG( LOG_ERR, "failed to allocate space for namespace reference indexes\n" );
            free( rpathtmp );
            return -1;
         }
         // create an array of hash nodes
         size_t rnodecount = 1;
         int curdepth = ms->refdepth;
         while ( curdepth ) { rnodecount *= ms->refbreadth; curdepth--; } // equiv of breadth to the depth power
         HASH_NODE* rnodelist = malloc( sizeof(struct hash_node_struct) * rnodecount );
         if ( rnodelist == NULL ) {
            LOG( LOG_ERR, "failed to allocate space for namespace reference hash nodes\n" );
            free( rpathtmp );
            free( refvals );
            return -1;
         }
         // populate all hash nodes
         size_t curnode;
         for ( curnode = 0; curnode < rnodecount; curnode++ ) {
            // populate the index for each rnode, starting at the depest level
            size_t tmpnode = curnode;
            for ( curdepth = ms->refdepth; curdepth; curdepth-- ) {
               refvals[curdepth-1] = tmpnode % ms->refbreadth; // what is our index at this depth
               tmpnode /= ms->refdepth; // find how many groups we have already traversed at this depth
            }
            // now populate the reference pathname
            char* outputstr = rpathtmp;
            int pathlenremaining = rpathlen;
            for ( curdepth = 0; curdepth < ms->refdepth; curdepth++ ) {
               int prlen = snprintf( outputstr, pathlenremaining, "%.*d/", ms->refdigits, refvals[curdepth] );
               if ( prlen <= 0  ||  prlen >= pathlenremaining ) {
                  LOG( LOG_ERR, "failed to generate reference path string\n" );
                  free( rpathtmp );
                  free( refvals );
                  free( rnodelist );
                  return -1;
               }
               pathlenremaining -= prlen;
               outputstr += prlen;
            }
            // copy the reference pathname into the hash node
            rnodelist[curnode].name = strndup( rpathtmp, rpathlen );
            if ( rnodelist[curnode].name == NULL ) {
               LOG( LOG_ERR, "failed to allocate reference path hash node name\n" );
               free( rpathtmp );
               free( refvals );
               free( rnodelist );
               return -1;
            }
            rnodelist[curnode].weight = 0;
            rnodelist[curnode].content = NULL;
         }
         // free data structures which we no longer need
         free( rpathtmp );
         free( refvals );
         // create the reference tree hash table
         ms->reftable = hash_init( rnodelist, rnodecount, 1 );
         if ( ms->reftable == NULL ) {
            LOG( LOG_ERR, "failed to create reference path table\n" );
            free( rnodelist );
            return -1;
         } // can't free the node list now, as it is in use by the hash table
         // count all subnodes
         ms->nscount = 0;
         int subspaces = 0;
         for( ; subnode; subnode->next ) {
            if ( subnode->type != XML_ELEMENT_NODE ) {
               LOG( LOG_ERR, "encountered unknown node within a 'namespaces' definition\n" );
               return -1;
            }
            // only 'ns' nodes are acceptible; for now, just assume every node is a NS def
            subspaces++;
         }
         // if we have no subspaces at all, just warn and continue on
         if ( !(subspaces) ) {
            LOG( LOG_WARNING, "repo \"%s\" has no namespaces defined\n", repo->name );
            continue;
         }
         // allocate an array of namespace nodes
         ms->nslist = malloc( sizeof(HASH_NODE) * subspaces );
         if ( ms->nslist == NULL ) {
            LOG( LOG_ERR, "failed to allocate space for repo namespaces\n" );
            return -1;
         }
         // parse and allocate all subspaces
         for( subnode = metaroot->children; subnode; subnode->next ) {
            // set default namespace values
            HASH_NODE* subspace = ms->nslist + ms->nscount;
            subspace->name = NULL;
            subspace->weight = 0;
            subspace->content = NULL;
            if ( create_namespace( subspace, NULL, repo, subnode ) ) {
               LOG( LOG_ERR, "failed to create subspace %d\n", ms->nscount );
               return -1;
            }
            // ensure we don't have any name collisions
            if ( find_namespace( ms->nslist, ms->nscount, subspace->name ) ) {
               LOG( LOG_ERR, "encountered subspace name collision: \"%s\"\n", subspace->name );
               free_namespace( subspace );
               return -1;
            }
            ms->nscount++;
         }
      }
      else if ( strncmp( (char*)metaroot->name, "direct", 7 ) == 0 ) {
         // parse through attributes, looking for read/write attrs with yes/no values
         for ( ; attr; attr = attr->next ) {
            char enabled = -1;
            if ( attr->type == XML_ATTRIBUTE_NODE ) {
               if ( attr->children->type == XML_TEXT_NODE  &&  attr->children->content != NULL ) {
                  if ( strncmp( (char*)attr->children->content, "no", 3 ) == 0 ) {
                     enabled = 0;
                  }
                  else if ( strncmp( (char*)attr->children->content, "yes", 4 ) == 0 ) {
                     enabled = 1;
                  }
               }
            }
            // check that we have a sensible attribute value
            if ( enabled < 0 ) {
               LOG( LOG_ERR, "inappropriate value for a \"%s\" attribute\n", (char*)attr->name );
               return -1;
            }
            // check which value this attribute provides
            if ( strncmp( (char*)attr->name, "read", 5 ) == 0 ) {
               ms->directread = enabled;
            }
            else if ( strncmp( (char*)attr->name, "write", 6 ) == 0 ) {
               ms->directwrite = enabled;
            }
            else {
               LOG( LOG_ERR, "encountered an unrecognized attribute of a 'direct' node: \"%s\"\n", (char*)attr->name );
               return -1;
            }
         }
         // iterate over child nodes, populating write_chunks
         char haveC = 0;
         for( ; subnode; subnode->next ) {
            if ( subnode->type != XML_ELEMENT_NODE ) {
               LOG( LOG_ERR, "encountered unknown node within a 'direct' definition\n" );
               return -1;
            }
            if ( strncmp( (char*)metaroot->name, "write_chunks", 13 ) == 0 ) {
               haveC = 1;
               if( parse_int_node( &(ds->directchunks), metaroot ) ) {
                  LOG( LOG_ERR, "failed to parse 'write_chunks' value within a 'direct' definition\n" );
                  return -1;
               }
            }
            else {
               LOG( LOG_ERR, "encountered an unrecognized \"%s\" node within a 'direct' definition\n", (char*)metaroot->name );
               return -1;
            }
         }
         // verify that all expected values were populated
         if ( ms->directwrite  &&  !(haveC) ) {
            LOG( LOG_ERR, "encountered a 'direct' definition with write enabled, but no 'write_chunks' value\n" );
            return -1;
         }
      }
      else {
         LOG( LOG_ERR, "encountered unexpected 'metadata' sub-node: \"%s\"\n", (char*)metaroot->name );
         return -1;
      }
   }
   // ensure we found an MDAL def
   if ( !(mdalnode) ) {
      LOG( LOG_ERR, "failed to locate MDAL definition\n" );
      return -1;
   }
   // initialize the MDAL
   if ( (ms->mdal = mdal_init( mdalnode )) == NULL ) {
      LOG( LOG_ERR, "failed to initialize MDAL\n" );
      return -1;
   }

   return 0;
}


int create_repo( marfs_repo* repo, XmlNode* reporoot ) {
   // check for a name attribute
   xmlAttr* attr = reporoot->properties;
   for ( ; attr; attr = attr->next ) {
      if ( strncmp( (char*)attr->name, "name", 5 ) == 0 ) {
         if ( attr->type == XML_ATTRIBUTE_NODE ) {
            if ( attr->children->type == XML_TEXT_NODE  &&  attr->children->content != NULL ) {
               if ( repo->name ) {
                  LOG( LOG_ERR, "detected a repeated 'name' attribute for repo \"%s\"\n", repo->name );
                  return -1;
               }
               repo->name = strcpy( (char*)attr->children->content );
            }
         }
         // verify that we properly stored this attr value
         if ( !(repo->name) ) {
            LOG( LOG_ERR, "failed to parse name value for repo\n" );
            return -1;
         }
      }
      else {
         LOG( LOG_ERR, "encountered an unrecognized attribute of a 'repo' node: \"%s\"\n", (char*)attr->name );
         return -1;
      }
   }
   // verify that we did find a name value
   if ( !(repo->name) ) {
      LOG( LOG_ERR, "failed to identify name value for repo\n" );
      return -1;
   }
}


int establish_nshierarchy( marfs_config* config );


//   -------------   EXTERNAL FUNCTIONS    -------------


