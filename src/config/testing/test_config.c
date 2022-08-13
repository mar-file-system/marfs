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

#include <unistd.h>
#include <stdio.h>
#include <ftw.h>
// directly including the C file allows more flexibility for these tests
#include "config/config.c"


// WARNING: error-prone and ugly method of deleting dir trees, written for simplicity only
//          don't replicate this junk into ANY production code paths!
size_t dirlistpos = 0;
char** dirlist = NULL;
int ftwnotedir( const char* fpath, const struct stat* sb, int typeflag ) {
   if ( typeflag != FTW_D ) {
      printf( "Encountered non-directory during tree deletion: \"%s\"\n", fpath );
      return -1;
   }
   dirlist[dirlistpos] = strdup( fpath );
   if ( dirlist[dirlistpos] == NULL ) {
      printf( "Failed to duplicate dir name: \"%s\"\n", fpath );
      return -1;
   }
   dirlistpos++;
   if ( dirlistpos >= 1024 ) { printf( "Dirlist has insufficient lenght!\n" ); return -1; }
   return 0;
}
int deletesubdirs( const char* basepath ) {
   dirlist = malloc( sizeof(char*) * 1024 );
   if ( dirlist == NULL ) {
      printf( "Failed to allocate dirlist\n" );
      return -1;
   }
   if ( ftw( basepath, ftwnotedir, 100 ) ) {
      printf( "Failed to identify reference dirs of \"%s\"\n", basepath );
      return -1;
   }
   int retval = 0;
   while ( dirlistpos ) {
      dirlistpos--;
      if ( strcmp( dirlist[dirlistpos], basepath ) ) {
         printf( "Deleting: \"%s\"\n", dirlist[dirlistpos] );
         if ( rmdir( dirlist[dirlistpos] ) ) {
            printf( "ERROR -- failed to delete \"%s\"\n", dirlist[dirlistpos] );
            retval = -1;
         }
      }
      free( dirlist[dirlistpos] );
   }
   free( dirlist );
   return retval;
}

int main(int argc, char **argv)
{
   // NOTE -- I'm ignoring memory leaks for error conditions 
   //         which result in immediate termination


   // Initialize the libxml lib and check for API mismatches
   LIBXML_TEST_VERSION

   char xmlbuffer[1024];
   if ( snprintf( xmlbuffer, 1024, "%s", 
                  "<perms>\
                        <interactive>RM</interactive>\
                        <batch>RD,WD</batch>\
                   </perms>" ) < 1 ) {
      printf( "failed to generate initial perm string\n" );
      return -1;
   }

   // open the test config file and produce an XML tree
   xmlDoc* doc = xmlReadMemory(xmlbuffer, 1024, "noexist.xml", NULL, XML_PARSE_NOBLANKS);
   if (doc == NULL) {
      printf("could not parse initial perm xml string\n");
      return -1;
   }
   xmlNode* root_element = xmlDocGetRootElement(doc);
   if ( root_element == NULL ) {
      printf( "failed to identify the root element of initial perm doc\n" );
      return -1;
   }

   // test permission parsing
   ns_perms iperms = NS_NOACCESS;
   ns_perms bperms = NS_NOACCESS;
   if ( parse_perms( &(iperms), &(bperms), root_element->children ) ) {
      printf( "parse_perms() failed on initial perm doc\n" );
      return -1;
   }
   // verify expected perm values
   if ( iperms != NS_READMETA  ||  bperms != NS_RWDATA ) {
      printf( "permission values differ from initial doc\n" );
      return -1;
   }

   // free the xml doc and cleanup parser vars
   xmlFreeDoc(doc);
   xmlCleanupParser();


   // open a new xml doc, with size_t and int node content
   if ( snprintf( xmlbuffer, 1024, "%s", 
                  "<root><size>10T</size>\
                   <int>1048576</int></root>" ) < 1 ) {
      printf( "failed to generate initial perm string\n" );
      return -1;
   }
   doc = xmlReadMemory(xmlbuffer, 1024, "noexist.xml", NULL, XML_PARSE_NOBLANKS);
   if (doc == NULL) {
      printf("could not parse size/int xml string\n");
      return -1;
   }
   root_element = xmlDocGetRootElement(doc);
   if ( root_element == NULL ) {
      printf( "failed to identify the root element of size/int xml doc\n" );
      return -1;
   }

   // parse the size node and verify the result
   size_t sizeres = 0;
   if ( parse_size_node( &(sizeres), root_element->children )  ||  sizeres != 10995116277760ULL ) {
      printf( "failed parse of size node or invalid size result of %zu\n", sizeres );
      return -1;
   }
   // parse the int node and verify the result
   int intres = 0;
   if ( parse_int_node( &(intres), root_element->children->next )  ||  intres != 1048576ULL ) {
      printf( "failed parse of int node or invalid int result of %d\n", intres );
      return -1;
   }

   // free the xml doc and cleanup parser vars
   xmlFreeDoc(doc);
   xmlCleanupParser();


   // open a new xml doc, with size_t and int node content
   if ( snprintf( xmlbuffer, 1024, "%s", 
              "<quotas>\
                  <files>1M</files>\
                  <data>123M</data>\
               </quotas>" ) < 1 ) {
      printf( "failed to generate quota xml string\n" );
      return -1;
   }
   doc = xmlReadMemory(xmlbuffer, 1024, "noexist.xml", NULL, XML_PARSE_NOBLANKS);
   if (doc == NULL) {
      printf("could not parse quota xml string\n");
      return -1;
   }
   root_element = xmlDocGetRootElement(doc);
   if ( root_element == NULL ) {
      printf( "failed to identify the root element of quota xml doc\n" );
      return -1;
   }

   // parse the quota node and verify the result
   size_t fquota = 0;
   size_t dquota = 0;
   if ( parse_quotas( &(fquota), &(dquota), root_element->children )  ||
        fquota != 1048576  ||
        dquota != 128974848ULL ) {
      printf( "failed to parse quota node or invalid result (dquota=%zu,fquota=%zu)\n", dquota, fquota );
      return -1;
   }

   // free the xml doc and cleanup parser vars
   xmlFreeDoc(doc);
   xmlCleanupParser();


   // now, move on to the complete config
   doc = xmlReadFile("testing/config.xml", NULL, XML_PARSE_NOBLANKS);
   if (doc == NULL) {
      printf("could not parse quota xml string\n");
      return -1;
   }
   root_element = xmlDocGetRootElement(doc);
   if ( root_element == NULL ) {
      printf( "failed to identify the root element of quota xml doc\n" );
      return -1;
   }
   // locate the first namespace of the first repo
   xmlNode* nsroot = root_element->children;
   while ( strcmp( (char*)(nsroot->name), "repo" ) ) { nsroot = nsroot->next; }
   if ( nsroot == NULL ) { printf( "failed to find 'exampleREPO' in config.xml\n" ); return -1; }
   nsroot = nsroot->children;
   while ( strcmp( (char*)(nsroot->name), "meta" ) ) { nsroot = nsroot->next; }
   nsroot = nsroot->children;
   while ( strcmp( (char*)(nsroot->name), "namespaces" ) ) { nsroot = nsroot->next; }
   nsroot = nsroot->children;
   while ( strcmp( (char*)(nsroot->name), "ns" ) ) { nsroot = nsroot->next; }

   // parse the NS node
   marfs_repo parentrepo;
   parentrepo.name = "parentrepo"; // need a parent repo reference for every NS
   HASH_NODE nsnode;
   if ( create_namespace( &(nsnode), NULL, &(parentrepo), nsroot ) ) {
      printf( "failed to parse NS xml node\n" );
      return -1;
   }

   // verify NS content
   if ( strcmp( nsnode.name, "gransom-allocation" ) ) {
      printf( "unexpected NS name value: \"%s\"\n", nsnode.name );
      return -1;
   } 
   marfs_ns* ns = (marfs_ns*)(nsnode.content);
   if ( ns->fquota != 10240U  ||  ns->dquota != 11258999068426240ULL ) {
      printf( "unexpected NS quota values: (fquota=%zu,dquota=%zu)\n", ns->fquota, ns->dquota );
      return -1;
   }
   if ( ns->iperms != NS_RWMETA  ||  ns->bperms != NS_FULLACCESS ) {
      printf( "unexpected NS perm values: (iperms=%d,bperms=%d)\n", ns->iperms, ns->bperms );
      return -1;
   }
   if ( ns->pnamespace  ||  ns->prepo != &(parentrepo) ) {
      printf( "unexpected parent values of NS \"%s\"\n", nsnode.name );
      return -1;
   }
   HASH_NODE* subnode = NULL;
   if ( hash_lookup( ns->subspaces, "read-only-data", &(subnode) ) != 0 ) {
      printf( "failed to locate 'read-only-data' subspace\n" );
      return -1;
   }
   if ( strcmp( subnode->name, "read-only-data" ) ) {
      printf( "lookup of 'read-only-data' produced unexpected NS: \"%s\"\n", subnode->name );
      return -1;
   }
   marfs_ns* subspace = (marfs_ns*)(subnode->content);
   if ( subspace->fquota  ||  subspace->dquota ) {
      printf( "detected non-zero quota values for \"%s\"\n", subnode->name );
      return -1;
   }
   if ( subspace->iperms != (NS_READMETA | NS_READDATA)  ||  subspace->bperms != (NS_READMETA | NS_READDATA) ) {
      printf( "unexpected \"%s\" subspace perm values: (iperms=%d,bperms=%d)\n", subnode->name, subspace->iperms, subspace->bperms );
      return -1;
   }
   if ( subspace->subspaces ) {
      printf( "\"%s\" subspace has non-NULL subspaces table\n", subnode->name );
      return -1;
   }
   if ( subspace->pnamespace != nsnode.content ) {
      printf( "\"%s\" subspace has unexpected parent namespace value\n", subnode->name );
      return -1;
   }
   if ( subspace->prepo != &(parentrepo) ) {
      printf( "\"%s\" subspace has unexpected parent repo value\n", subnode->name );
      return -1;
   }

   // free the created namespace
   if ( free_namespace( &(nsnode) ) ) {
      printf( "failed to free initial NS\n" );
      return -1;
   }

   // locate the first distribution node
   nsroot = root_element->children;
   while ( nsroot  &&  strcmp( (char*)(nsroot->name), "repo" ) ) { nsroot = nsroot->next; }
   if ( nsroot == NULL ) { printf( "config.xml has unexpected format (no repo)\n" ); return -1; }
   nsroot = nsroot->children;
   while ( nsroot  &&  strcmp( (char*)(nsroot->name), "data" ) ) { nsroot = nsroot->next; }
   if ( nsroot == NULL ) { printf( "config.xml has unexpected format (no data)\n" ); return -1; }
   nsroot = nsroot->children;
   while ( nsroot  &&  strcmp( (char*)(nsroot->name), "distribution" ) ) { nsroot = nsroot->next; }
   if ( nsroot == NULL ) { printf( "config.xml has unexpected format (no dist)\n" ); return -1; }

   // parse the distribution into hash tables
   int tgtcnt = 0;
   HASH_TABLE distable = create_distribution_table( &(tgtcnt), nsroot->children );
   if ( distable == NULL ) {
      printf( "failed to create dist table for \"%s\" node\n", (char*)(nsroot->children->name) );
      return -1;
   }
   HASH_NODE* nodelist = NULL;
   size_t nodecount = 0;
   if ( hash_term( distable, &(nodelist), &(nodecount) ) ) {
      printf( "failed to term dist table for \"%s\" node\n", (char*)(nsroot->children->name) );
      return -1;
   }
   if ( nodecount != 4 ) {
      printf( "expected 4 distribution nodes for \"%s\", but found %zu\n", 
               (char*)(nsroot->children->name), nodecount );
      return -1;
   }
   if ( (nodelist)->weight != 1 ) {
      printf( "expected a weight of 1 for node 0\n" );
      return -1;
   }
   if ( (nodelist+3)->weight != 5 ) {
      printf( "expected a weight of 5 for node 3\n" );
      return -1;
   }

   // free nodenames
   while ( nodecount ) {
      nodecount--;
      if ( nodecount != 3  &&  nodecount != 0 ) {
         if ( (nodelist + nodecount)->weight != 2 ) {
            printf( "expected a weight of 2 for node %zu\n", nodecount );
         }
      }
      free( (nodelist + nodecount)->name );
   }
   // free the retrieved nodelist
   free( nodelist );

   // locate the first data node
   nsroot = root_element->children;
   while ( nsroot  &&  strcmp( (char*)(nsroot->name), "repo" ) ) { nsroot = nsroot->next; }
   if ( nsroot == NULL ) { printf( "config.xml has unexpected format (no repo)\n" ); return -1; }
   nsroot = nsroot->children;
   while ( nsroot  &&  strcmp( (char*)(nsroot->name), "data" ) ) { nsroot = nsroot->next; }
   if ( nsroot == NULL ) { printf( "config.xml has unexpected format (no data)\n" ); return -1; }

   // create the dirs necessary for DAL initialization (ignore EEXIST)
   errno = 0;
   if ( mkdir( "./test_config_topdir", S_IRWXU )  &&  errno != EEXIST ) {
      printf( "failed to create test_config_topdir\n" );
      return -1;
   }
   errno = 0;
   if ( mkdir( "./test_config_topdir/dal_root", S_IRWXU )  &&  errno != EEXIST ) {
      printf( "failed to create test_config_topdir/dal_root\n" );
      return -1;
   }

   // populate some default repo values
   marfs_repo newrepo;
   newrepo.name = strdup( "test-scheme-parsing-repo" );
   newrepo.datascheme.protection.N = 1;
   newrepo.datascheme.protection.E = 0;
   newrepo.datascheme.protection.O = 0;
   newrepo.datascheme.protection.partsz = 10;
   newrepo.datascheme.nectxt = NULL;
   newrepo.datascheme.objfiles = 1;
   newrepo.datascheme.objsize = 0;
   newrepo.datascheme.podtable = NULL;
   newrepo.datascheme.captable = NULL;
   newrepo.datascheme.scattertable = NULL;
   newrepo.metascheme.mdal = NULL;
   newrepo.metascheme.directread = 0;
   newrepo.metascheme.reftable = NULL;
   newrepo.metascheme.nscount = 0;
   newrepo.metascheme.nslist = NULL;

   // parse the data node
   if ( newrepo.name == NULL ) { printf( "failed strdup\n" ); return -1; }
   if ( parse_datascheme( &(newrepo.datascheme), nsroot->children ) ) {
      printf( "failed to parse first data node\n" );
      return -1;
   }
   // verify ds elements
   marfs_ds* ds = &(newrepo.datascheme);
   if ( ds->protection.N != 10  || ds->protection.E != 2  ||  ds->protection.partsz != 1024 ) {
      printf( "unexpected protection values for datascheme: (N=%d,E=%d,psz=%zu)\n", ds->protection.N, ds->protection.E, ds->protection.partsz );
      return -1;
   }
   if ( ds->nectxt == NULL ) {
      printf( "datascheme has NULL nectxt\n" );
      return -1;
   }
   if ( ds->objfiles != 4096 ) {
      printf( "unexpected objfiles value for datascheme: %zu\n", ds->objfiles );
      return -1;
   }
   if ( ds->objsize != 1073741824ULL ) {
      printf( "unexpected objsize value for datascheme: %zu\n", ds->objsize );
      return -1;
   }
   if ( ds->podtable == NULL  ||  ds->captable == NULL  ||  ds->scattertable == NULL ) {
      printf( "not all pod/cap/scatter tables were initialized for datascheme\n" );
      return -1;
   }

   // locate the first meta node
   nsroot = root_element->children;
   while ( nsroot  &&  strcmp( (char*)(nsroot->name), "repo" ) ) { nsroot = nsroot->next; }
   if ( nsroot == NULL ) { printf( "config.xml has unexpected format (no repo)\n" ); return -1; }
   nsroot = nsroot->children;
   while ( nsroot  &&  strcmp( (char*)(nsroot->name), "meta" ) ) { nsroot = nsroot->next; }
   if ( nsroot == NULL ) { printf( "config.xml has unexpected format (no data)\n" ); return -1; }

   // create the dir necessary for mdal initialization
   errno = 0;
   if ( mkdir( "./test_config_topdir/mdal_root", S_IRWXU )  &&  errno != EEXIST ) {
      printf( "failed to create \"./test_config_topdir/mdal_root\"\n" );
      return -1;
   }

   // parse the meta node
   if ( parse_metascheme( &(newrepo), nsroot->children ) ) {
      printf( "failed to parse metascheme\n" );
      return -1;
   }
   // verify the metascheme content
   if ( newrepo.metascheme.mdal == NULL ) {
      printf( "metascheme has a NULL mdal ref\n" );
      return -1;
   }
   if ( newrepo.metascheme.directread != 1 ) {
      printf( "directread not set for metascheme\n" );
      return -1;
   }
   if ( newrepo.metascheme.reftable == NULL ) {
      printf( "reftable is NULL for metascheme\n" );
      return -1;
   }
   if ( newrepo.metascheme.nscount != 1 ) {
      printf( "unexpected metascheme NS count: %d\n", newrepo.metascheme.nscount );
      return -1;
   }
   if ( newrepo.metascheme.nslist == NULL ) {
      printf( "NULL nslist ref for metascheme\n" );
      return -1;
   }

   // verify that the reference table contains all expected entries
   int refvals[3] = { 0 , 0 , 0 };
   int totalrefs = 0;
   while ( refvals[0] < 3 ) {
      while ( refvals[1] < 3 ) {
            while ( refvals[2] < 3 ) {
               if ( snprintf( xmlbuffer, 1024, "%.3d/%.3d/%.3d/", refvals[0], refvals[1], refvals[2] ) != 12 ) {
                  printf( "unexpected length of refstring: \"%s\"\n", xmlbuffer );
                  return -1;
               }
               if ( strcmp( xmlbuffer, newrepo.metascheme.refnodes[totalrefs].name ) ) {
                  printf( "unexpected name of reference node %d: \"%s\"\n", totalrefs, newrepo.metascheme.refnodes[totalrefs].name );
                  return -1;
               }
               totalrefs++;
               refvals[2]++;
            }
         refvals[1]++;
      }
      refvals[0]++;
   }

   // free the repo
   if ( free_repo( &(newrepo) ) ) {
      printf( "failed to free newrepo struct\n" );
      return -1;
   }

   // free the xml doc and cleanup parser vars
   xmlFreeDoc(doc);
   xmlCleanupParser();

   // finally, parse the entire config
   marfs_config* config = config_init( "./testing/config.xml" );
   if ( config == NULL ) {
      printf( "failed to parse the full config file\n" );
      return -1;
   }
   // verify config contents
   if ( strcmp( config->version, "0.0001-beta-notarealversion" ) ) {
      printf( "unexpected config version string: \"%s\"\n", config->version );
      return -1;
   }
   if ( strcmp( config->mountpoint, "/campaign" ) ) {
      printf( "unexpected config mountpoint string: \"%s\"\n", config->mountpoint );
      return -1;
   }
   if ( strcmp( config->ctag, "UNKNOWN" ) ) {
      printf( "unexpected config ctag string: \"%s\"\n", config->ctag );
      return -1;
   }
   if ( config->rootns == NULL ) {
      printf( "NULL value for root NS of config\n" );
      return -1;
   }
   if ( config->repocount != 2 ) {
      printf( "unexpected repocount for config: %d\n", config->repocount );
      return -1;
   }
   if ( strcmp( config->repolist->name, "exampleREPO" ) ) {
      printf( "unexpected name of first config repo: \"%s\"\n", config->repolist->name );
      return -1;
   }
   if ( strcmp( (config->repolist + 1)->name, "3+2repo" ) ) {
      printf( "unexpected name of first config repo: \"%s\"\n", (config->repolist + 1)->name );
      return -1;
   }


   // prepare for full path traversal by actually creating config namespaces
   if ( config_verify(config,"/campaign/",1,1,1,1) ) {
      printf( "Config validation failure\n" );
      return -1;
   }

   // test NS identification
   // 1st shift -- TGT = "gransom-allocation/notaNS"
   //           -- NS = 'gransom-allocation'
   marfs_position pos = { .depth = 0, .ns = config->rootns, .ctxt = NULL };
   pos.ctxt = config->rootns->prepo->metascheme.mdal->newctxt( "/.", config->rootns->prepo->metascheme.mdal->ctxt );
   if ( pos.ctxt == NULL ) {
      printf( "Failed to populate initial rootNS CTXT for NS shifts\n" );
      return -1;
   }
   char* shiftres = NULL;
   if ( snprintf( xmlbuffer, 1024, "gransom-allocation/notaNS" ) < 1 ) {
      printf( "Failed to populate 1st NS shift path\n" );
      return -1;
   }
   if ( (shiftres = config_shiftns( config, &(pos), xmlbuffer )) == NULL ) {
      printf( "Failure of 1st NS shift: \"%s\"\n", xmlbuffer );
      return -1;
   }
   if ( strcmp( shiftres, "notaNS" ) ) {
      printf( "Unexpected path for 1st NS shift: \"%s\"\n", shiftres );
      return -1;
   }
   printf( "1st NS shift: \"%s\"\n", shiftres );
   // 2nd -- TGT = "../gransom-allocation/heavily-protected-data/" 
   //     -- NS = 'gransom-allocation/heavily-protected-data'
   if ( snprintf( xmlbuffer, 1024, "../gransom-allocation/heavily-protected-data/" ) < 1 ) {
      printf( "Failed to populate 2nd NS shift path\n" );
      return -1;
   }
   if ( (shiftres = config_shiftns( config, &(pos), xmlbuffer )) == NULL ) {
      printf( "Failure of 2nd NS shift: \"%s\"\n", xmlbuffer );
      return -1;
   }
   if ( strcmp( shiftres, "" ) ) {
      printf( "Unexpected path for 2nd NS shift: \"%s\"\n", shiftres );
      return -1;
   }
   printf( "2nd NS shift: \"%s\"\n", shiftres );
   // 3rd -- TGT = "read-only-data/../../"
   //     -- NS = 'gransom-allocation/heavily-protected-data'
   if ( snprintf( xmlbuffer, 1024, "read-only-data/../../" ) < 1 ) {
      printf( "Failed to populate 3rd NS shift path\n" );
      return -1;
   }
   if ( (shiftres = config_shiftns( config, &(pos), xmlbuffer )) == NULL ) {
      printf( "Failure of 3rd NS shift: \"%s\"\n", xmlbuffer );
      return -1;
   }
   if ( strcmp( shiftres, "read-only-data/../../" ) ) {
      printf( "Unexpected path for 3rd NS shift: \"%s\"\n", shiftres );
      return -1;
   }
   printf( "3rd NS shift: \"%s\"\n", shiftres );
   // 4th -- TGT = "../read-only-data/.//read-only-file"
   //     -- NS = 'gransom-allocation/read-only-data'
   if ( snprintf( xmlbuffer, 1024, "../read-only-data/.//read-only-file" ) < 1 ) {
      printf( "Failed to populate 4th NS shift path\n" );
      return -1;
   }
   if ( (shiftres = config_shiftns( config, &(pos), xmlbuffer )) == NULL ) {
      printf( "Failure of 4th NS shift: \"%s\"\n", xmlbuffer );
      return -1;
   }
   if ( strcmp( shiftres, "read-only-file" ) ) {
      printf( "Unexpected path for 4th NS shift: \"%s\"\n", shiftres );
      return -1;
   }
   printf( "4th NS shift: \"%s\"\n", shiftres );
   // 5th -- TGT = "./../..//..//campaign/./gransom-allocation/heavily-protected-data/./test"
   //     -- NS = 'gransom-allocation/heavily-protected-data'
   if ( snprintf( xmlbuffer, 1024, "./../..//..//campaign/./gransom-allocation/heavily-protected-data/./test" ) < 1 ) {
      printf( "Failed to populate 5th NS shift path\n" );
      return -1;
   }
   if ( (shiftres = config_shiftns( config, &(pos), xmlbuffer )) == NULL ) {
      printf( "Failure of 5th NS shift: \"%s\"\n", xmlbuffer );
      return -1;
   }
   if ( strcmp( shiftres, "test" ) ) {
      printf( "Unexpected path for 5th NS shift: \"%s\"\n", shiftres );
      return -1;
   }
   printf( "5th NS shift: \"%s\"\n", shiftres );
   // 6th -- TGT = "../../noexist"
   //     -- NS = '/'
   if ( snprintf( xmlbuffer, 1024, "../../noexist" ) < 1 ) {
      printf( "Failed to populate 6th NS shift path\n" );
      return -1;
   }
   if ( (shiftres = config_shiftns( config, &(pos), xmlbuffer )) == NULL ) {
      printf( "Failure of 6th NS shift: \"%s\"\n", xmlbuffer );
      return -1;
   }
   if ( strcmp( shiftres, "noexist" ) ) {
      printf( "Unexpected path for 6th NS shift: \"%s\"\n", shiftres );
      return -1;
   }
   printf( "6th NS shift: \"%s\"\n", shiftres );
   if ( pos.ns != config->rootns ) {
      printf( "NS != rootns following final NS shift\n" );
      return -1;
   }


   // test full path traversal ( no link-check )
   // 1st -- TGT = "/campaign/rootNSfile"
   //     -- NS = '/'
   char* travbuf = NULL;
   if ( (travbuf = strdup( "/campaign/rootNSfile" )) == NULL ) {
      printf( "Failed to populate 1st NS traversal path\n" );
      return -1;
   }
   int travdepth = config_traverse( config, &(pos), &(travbuf), 0 );
   if ( travdepth != 1 ) {
      printf( "Failure of 1st traversal: \"%s\" (depth: %d)\n", travbuf, travdepth );
      return -1;
   }
   if ( strcmp( travbuf, "rootNSfile" ) ) {
      printf( "Unexpected subpath for 1st traversal: \"%s\"\n", travbuf );
      return -1;
   }
   if ( pos.ns != config->rootns ) {
      printf( "Non-root NS following 1st traversal\n" );
      return -1;
   }
   if ( pos.depth ) {
      printf( "Non-zero depth value following 1st traversal: %u\n", pos.depth );
      return -1;
   }
   printf( "1st Traversal: \"%s\"\n", travbuf );
   free( travbuf );
   // 2nd -- TGT = ".././/campaign/gransom-allocation/test/./this//../"
   //     -- NS = '/gransom-allocation'
   if ( (travbuf = strdup( ".././/campaign/gransom-allocation/test/./this//../" )) == NULL ) {
      printf( "Failed to populate 2nd NS traversal path\n" );
      return -1;
   }
   travdepth = config_traverse( config, &(pos), &(travbuf), 0 );
   if ( travdepth != 1 ) {
      printf( "Failure of 2nd traversal: \"%s\" (depth: %d)\n", travbuf, travdepth );
      return -1;
   }
   if ( strcmp( travbuf, "test/./this//../" ) ) {
      printf( "Unexpected subpath for 2nd traversal: \"%s\"\n", travbuf );
      return -1;
   }
   if ( pos.ns == config->rootns ) {
      printf( "Root NS following 2nd traversal\n" );
      return -1;
   }
   if ( pos.depth ) {
      printf( "Non-zero depth value following 2nd traversal: %u\n", pos.depth );
      return -1;
   }
   printf( "2nd Traversal: \"%s\"\n", travbuf );
   free( travbuf );
   // 3rd -- TGT = "./not//../relevant/./../////read-only-data/./f"
   //     -- NS = '/gransom-allocation/read-only-data'
   if ( (travbuf = strdup( "./not//../relevant/./../////read-only-data/./f" )) == NULL ) {
      printf( "Failed to populate 3rd NS traversal path\n" );
      return -1;
   }
   travdepth = config_traverse( config, &(pos), &(travbuf), 0 );
   if ( travdepth != 1 ) {
      printf( "Failure of 3rd traversal: \"%s\" (depth: %d)\n", travbuf, travdepth );
      return -1;
   }
   if ( strcmp( travbuf, "f" ) ) {
      printf( "Unexpected subpath for 3rd traversal: \"%s\"\n", travbuf );
      return -1;
   }
   if ( pos.ns == config->rootns ) {
      printf( "Root NS following 3rd traversal\n" );
      return -1;
   }
   if ( pos.depth ) {
      printf( "Non-zero depth value following 3rd traversal: %u\n", pos.depth );
      return -1;
   }
   printf( "3rd Traversal: \"%s\"\n", travbuf );
   free( travbuf );
   // 4th -- TGT = "/campaign/somethin///../gransom-allocation/heavily-protected-data/../.././myfile"
   //     -- NS = '/'
   if ( (travbuf = strdup( "/campaign/somethin///../gransom-allocation/heavily-protected-data/../.././myfile" )) == NULL ) {
      printf( "Failed to populate 4th NS traversal path\n" );
      return -1;
   }
   travdepth = config_traverse( config, &(pos), &(travbuf), 0 );
   if ( travdepth != 1 ) {
      printf( "Failure of 4th traversal: \"%s\" (depth: %d)\n", travbuf, travdepth );
      return -1;
   }
   if ( strcmp( travbuf, "myfile" ) ) {
      printf( "Unexpected subpath for 4th traversal: \"%s\"\n", travbuf );
      return -1;
   }
   if ( pos.ns != config->rootns ) {
      printf( "Non-root NS following 4th traversal\n" );
      return -1;
   }
   if ( pos.depth ) {
      printf( "Non-zero depth value following 4th traversal: %u\n", pos.depth );
      return -1;
   }
   printf( "4th Traversal: \"%s\"\n", travbuf );
   free( travbuf );
   // 5th -- TGT = "../../../gransom-allocation/read-only-data/../heavily-protected-data/tgt/subdir/"
   //     -- NS = '/gransom-allocation/heavily-protected-data'
   pos.depth = 3; // make our position relative to some 3-deep subdir of the rootNS
   if ( (travbuf = strdup( "../../../gransom-allocation/read-only-data/../heavily-protected-data/tgt/subdir/" )) == NULL ) {
      printf( "Failed to populate 5th NS traversal path\n" );
      return -1;
   }
   travdepth = config_traverse( config, &(pos), &(travbuf), 0 );
   if ( travdepth != 2 ) {
      printf( "Failure of 5th traversal: \"%s\" (depth: %d)\n", travbuf, travdepth );
      return -1;
   }
   if ( strcmp( travbuf, "tgt/subdir/" ) ) {
      printf( "Unexpected subpath for 5th traversal: \"%s\"\n", travbuf );
      return -1;
   }
   if ( pos.ns == config->rootns ) {
      printf( "Root NS following 5th traversal\n" );
      return -1;
   }
   if ( pos.depth ) {
      printf( "Non-zero depth value following 5th traversal: %u\n", pos.depth );
      return -1;
   }
   printf( "5th Traversal: \"%s\"\n", travbuf );
   free( travbuf );
   // 6th -- TGT = ".././/MDAL_test"
   //     -- NS = '/gransom-allocation/heavily-protected-data'
   pos.depth = 2; // make our position relative to some 2-deep subdir
   if ( (travbuf = strdup( ".././/MDAL_test" )) == NULL ) {
      printf( "Failed to populate 6th NS traversal path\n" );
      return -1;
   }
   travdepth = config_traverse( config, &(pos), &(travbuf), 0 );
   if ( travdepth != 2 ) {
      printf( "Failure of 6th traversal: \"%s\" (depth: %d)\n", travbuf, travdepth );
      return -1;
   }
   if ( strcmp( travbuf, ".././/MDAL_test" ) ) {
      printf( "Unexpected subpath for 6th traversal: \"%s\"\n", travbuf );
      return -1;
   }
   if ( pos.ns == config->rootns ) {
      printf( "Root NS following 6th traversal\n" );
      return -1;
   }
   if ( pos.depth != 2 ) {
      printf( "Depth value modified following 6th traversal: %u\n", pos.depth );
      return -1;
   }
   printf( "6th Traversal: \"%s\"\n", travbuf );
   free( travbuf );

   // verify rejection of MDAL_ prefix
   // -- TGT = "..///../MDAL_shouldreject"
   // -- NS = '/gransom-allocation/heavily-protected-data'
   pos.depth = 2; // make our position relative to some 2-deep subdir
   if ( (travbuf = strdup( "..///../MDAL_shouldreject" )) == NULL ) {
      printf( "Failed to populate 6th NS traversal path\n" );
      return -1;
   }
   travdepth = config_traverse( config, &(pos), &(travbuf), 0 );
   if ( travdepth >= 0 ) {
      printf( "Traversal expected to fail: \"%s\" (depth: %d)\n", travbuf, travdepth );
      return -1;
   }
   if ( strcmp( travbuf, "..///../MDAL_shouldreject" ) ) {
      printf( "Unexpected subpath for failed traversal: \"%s\"\n", travbuf );
      return -1;
   }
   if ( pos.ns == config->rootns ) {
      printf( "Root NS following failed traversal\n" );
      return -1;
   }
   if ( pos.depth != 2 ) {
      printf( "Depth value modified following failed traversal: %u\n", pos.depth );
      return -1;
   }
   printf( "Failed Traversal (expected failure): \"%s\"\n", travbuf );
   free( travbuf );
   pos.depth = 0; // reset position to appropriate depth

   // construct some real dirs/links/files to verify linkchk flag
   MDAL heavymdal = pos.ns->prepo->metascheme.mdal;
   errno = 0;
   if ( heavymdal->mkdir( pos.ctxt, "subdir", S_IRWXU )  &&  errno != EEXIST ) {
      printf( "Failed to mkdir \"/gransom-allocation/heavily-protected-data/subdir\"\n" );
      return -1;
   }
   errno = 0;
   MDAL_FHANDLE fh = heavymdal->openref( pos.ctxt, "reflink", O_CREAT | O_EXCL, S_IRWXU );
   if ( (fh == NULL  &&  errno != EEXIST)  ||  (fh != NULL  && heavymdal->close( fh )) ) {
      printf( "Failed to create 'reflink' reference file\n" );
      return -1;
   }
   errno = 0;
   if ( heavymdal->linkref( pos.ctxt, 0, "reflink", "subdir/tgtfile" )  &&  errno != EEXIST ) {
      printf( "Failed to link reference file 'reflink' to real tgt 'subdir/tgtfile'\n" );
      return -1;
   }
   errno = 0;
   if ( heavymdal->symlink( pos.ctxt, "./subdir/tgtfile", "relativelink" )  &&  errno != EEXIST ) {
      printf( "Failed to create 'relativelink'\n" );
      return -1;
   }
   errno = 0;
   if ( heavymdal->symlink( pos.ctxt, "//./campaign/gransom-allocation/heavily-protected-data/../heavily-protected-data/subdir/", "./subdir/absolutelink" )  &&  errno != EEXIST ) {
      printf( "Failed to create 'absolutelink'\n" );
      return -1;
   }
   errno = 0;
   if ( heavymdal->symlink( pos.ctxt, "../noexist/../subdir/tgtfile", "subdir/brokenlink" )  &&  errno != EEXIST ) {
      printf( "Failed to create 'brokenlink'\n" );
      return -1;
   }

   // Test traversal - Dirs/files
   // TGT = "subdir//tgtfile"
   marfs_ns* heavyns = pos.ns;
   if ( (travbuf = strdup( "subdir//tgtfile" )) == NULL ) {
      printf( "Failed to populate dir/file traversal path\n" );
      return -1;
   }
   travdepth = config_traverse( config, &(pos), &(travbuf), 1 );
   if ( travdepth != 2 ) {
      printf( "Failure of dir/file traversal: \"%s\" (depth: %d)\n", travbuf, travdepth );
      return -1;
   }
   if ( strcmp( travbuf, "subdir//tgtfile" ) ) {
      printf( "Unexpected subpath for dir/file traversal: \"%s\"\n", travbuf );
      return -1;
   }
   if ( pos.ns != heavyns ) {
      printf( "Unexpected NS following dir/file traversal\n" );
      return -1;
   }
   if ( pos.depth ) {
      printf( "Depth value modified following dir/file traversal: %u\n", pos.depth );
      return -1;
   }
   printf( "Dir/File Traversal: \"%s\"\n", travbuf );
   free( travbuf );
   // Test traversal - Relative Link
   // TGT = "relativelink"
   if ( (travbuf = strdup( "../heavily-protected-data/relativelink" )) == NULL ) {
      printf( "Failed to populate rellink traversal path\n" );
      return -1;
   }
   travdepth = config_traverse( config, &(pos), &(travbuf), 1 );
   if ( travdepth != 2 ) {
      printf( "Failure of rellink traversal: \"%s\" (depth: %d)\n", travbuf, travdepth );
      return -1;
   }
   if ( strcmp( travbuf, "./subdir/tgtfile" ) ) {
      printf( "Unexpected subpath for rellink traversal: \"%s\"\n", travbuf );
      return -1;
   }
   if ( pos.ns != heavyns ) {
      printf( "Unexpected NS following rellink traversal\n" );
      return -1;
   }
   if ( pos.depth ) {
      printf( "Depth value modified following rellink traversal: %u\n", pos.depth );
      return -1;
   }
   printf( "Rellink Traversal: \"%s\"\n", travbuf );
   free( travbuf );
   // Test traversal - Absolute Link
   // TGT = "subdir/absolutelink/.."
   if ( (travbuf = strdup( "subdir/absolutelink/.." )) == NULL ) {
      printf( "Failed to populate abslink traversal path\n" );
      return -1;
   }
   travdepth = config_traverse( config, &(pos), &(travbuf), 1 );
   if ( travdepth != 0 ) {
      printf( "Failure of abslink traversal: \"%s\" (depth: %d)\n", travbuf, travdepth );
      return -1;
   }
   if ( strcmp( travbuf, "subdir//.." ) ) {
      printf( "Unexpected subpath for abslink traversal: \"%s\"\n", travbuf );
      return -1;
   }
   if ( pos.ns != heavyns ) {
      printf( "Unexpected NS following abslink traversal\n" );
      return -1;
   }
   if ( pos.depth ) {
      printf( "Non-zero depth value modified following abslink traversal: %u\n", pos.depth );
      return -1;
   }
   printf( "Abslink Traversal: \"%s\"\n", travbuf );
   free( travbuf );
   // Test traversal - Broken Link
   // TGT = "subdir/brokenlink"
   if ( (travbuf = strdup( "subdir/brokenlink" )) == NULL ) {
      printf( "Failed to populate brokenlink traversal path\n" );
      return -1;
   }
   errno = 0;
   travdepth = config_traverse( config, &(pos), &(travbuf), 1 );
   if ( travdepth != 2 ) {
      printf( "Failure of brokenlink traversal: \"%s\" (depth: %d)\n", travbuf, travdepth );
      return -1;
   }
   if ( strcmp( travbuf, "subdir/../noexist/../subdir/tgtfile" ) ) {
      printf( "Unexpected subpath for brokenlink traversal: \"%s\"\n", travbuf );
      return -1;
   }
   if ( pos.ns != heavyns ) {
      printf( "Unexpected NS following brokenlink traversal\n" );
      return -1;
   }
   if ( pos.depth ) {
      printf( "Non-zero depth value modified following brokenlink traversal: %u\n", pos.depth );
      return -1;
   }
   printf( "Brokenlink Traversal: \"%s\"\n", travbuf );
   free( travbuf );

   // cleanup dirs/links/files
   if ( heavymdal->unlink( pos.ctxt, "subdir/brokenlink" ) ) {
      printf( "Failed to unlink 'subdir/brokenlink'\n" );
      return -1;
   }
   if ( heavymdal->unlink( pos.ctxt, "subdir/absolutelink" ) ) {
      printf( "Failed to unlink 'absolutelink'\n" );
      return -1;
   }
   if ( heavymdal->unlink( pos.ctxt, "relativelink" ) ) {
      printf( "Failed to unlink 'relativelink'\n" );
      return -1;
   }
   if ( heavymdal->unlink( pos.ctxt, "subdir/tgtfile" ) ) {
      printf( "Failed to unlink 'tgtfile'\n" );
      return -1;
   }
   if ( heavymdal->rmdir( pos.ctxt, "subdir" ) ) {
      printf( "Failed to rmdir 'subdir'\n" );
      return -1;
   }
   if ( heavymdal->unlinkref( pos.ctxt, "reflink" ) ) {
      printf( "Failed to unlinkref 'reflink'\n" );
      return -1;
   }

   // cleanup position
   if ( pos.ns->prepo->metascheme.mdal->destroyctxt( pos.ctxt ) ) {
      printf( "Failed to destory final postion MDAL_CTXT\n" );
      return -1;
   }

   // cleanup namespaces
   MDAL rootmdal = config->rootns->prepo->metascheme.mdal;
   if ( deletesubdirs( "./test_config_topdir/mdal_root/MDAL_subspaces/gransom-allocation/MDAL_subspaces/heavily-protected-data/MDAL_reference" ) ) {
      printf( "Failed to delete refdirs of heavily-protected-data\n" );
      return -1;
   }
   if ( rootmdal->destroynamespace( rootmdal->ctxt, "/gransom-allocation/heavily-protected-data" ) ) {
      printf( "Failed to destroy /gransom-allocation/heavily-protected-data NS\n" );
      return -1;
   }
   if ( deletesubdirs( "./test_config_topdir/mdal_root/MDAL_subspaces/gransom-allocation/MDAL_subspaces/read-only-data/MDAL_reference" ) ) {
      printf( "Failed to delete refdirs of read-only-data\n" );
      return -1;
   }
   if ( rootmdal->destroynamespace( rootmdal->ctxt, "/gransom-allocation/read-only-data" ) ) {
      printf( "Failed to destroy /gransom-allocation/read-only-data NS\n" );
      return -1;
   }
   if ( deletesubdirs( "./test_config_topdir/mdal_root/MDAL_subspaces/gransom-allocation/MDAL_reference" ) ) {
      printf( "Failed to delete refdirs of gransom-allocation\n" );
      return -1;
   }
   if ( rootmdal->destroynamespace( rootmdal->ctxt, "/gransom-allocation" ) ) {
      printf( "Failed to destroy /gransom-allocation NS\n" );
      return -1;
   }
   if ( deletesubdirs( "./test_config_topdir/mdal_root/MDAL_reference" ) ) {
      printf( "Failed to delete refdirs of rootNS\n" );
      return -1;
   }
   if ( rootmdal->destroynamespace( rootmdal->ctxt, "/root-ghost/gransom-allocation/heavily-protected-data" ) ) {
      printf( "Failed to destroy /root-ghost/gransom-allocation/heavily-protected-data NS\n" );
      return -1;
   }
   if ( rootmdal->destroynamespace( rootmdal->ctxt, "/root-ghost/gransom-allocation/read-only-data" ) ) {
      printf( "Failed to destroy /root-ghost/gransom-allocation/read-only-data NS\n" );
      return -1;
   }
   if ( rootmdal->destroynamespace( rootmdal->ctxt, "/root-ghost/gransom-allocation" ) ) {
      printf( "Failed to destroy /root-ghost/gransom-allocation NS\n" );
      return -1;
   }
   if ( rootmdal->destroynamespace( rootmdal->ctxt, "/root-ghost" ) ) {
      printf( "Failed to destroy /root-ghost NS\n" );
      return -1;
   }
   rootmdal->destroynamespace( rootmdal->ctxt, "/." ); // TODO : fix MDAL edge case?


   // free the config
   if ( config_term( config ) ) {
      printf( "failed to terminate the config\n" );
      return -1;
   }

   // delete dal/mdal dir structure
   rmdir( "./test_config_topdir/dal_root" );
   rmdir( "./test_config_topdir/mdal_root" );
   rmdir( "./test_config_topdir" );

   return 0;
}


