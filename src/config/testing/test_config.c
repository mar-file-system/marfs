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
// directly including the C file allows more flexibility for these tests
#include "config/config.c"

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
   newrepo.metascheme.directwrite = 0;
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
   if ( newrepo.metascheme.directwrite != 1 ) {
      printf( "directwrite not set for metascheme\n" );
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
               HASH_NODE* noderef = NULL;
               if ( hash_lookup( newrepo.metascheme.reftable, xmlbuffer, &(noderef) ) ) {
                  printf( "unexpected hash_lookup return for ref path: \"%s\"\n", xmlbuffer );
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


   // prepare for full path traversal by actually creating config namespaces via MDAL
   MDAL rootmdal = config->rootns->prepo->metascheme.mdal;
   if ( rootmdal->createnamespace( rootmdal->ctxt, "/." ) ) {
      printf( "Failed to create root NS\n" );
      return -1;
   }
   if ( rootmdal->createnamespace( rootmdal->ctxt, "/gransom-allocation" ) ) {
      printf( "Failed to create /gransom-allocation NS\n" );
      return -1;
   }
   if ( rootmdal->createnamespace( rootmdal->ctxt, "/gransom-allocation/read-only-data" ) ) {
      printf( "Failed to create /gransom-allocation/read-only-data NS\n" );
      return -1;
   }
   if ( rootmdal->createnamespace( rootmdal->ctxt, "/gransom-allocation/heavily-protected-data" ) ) {
      printf( "Failed to create /gransom-allocation/heavily-protected-data NS\n" );
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
   free( travbuf );

   // cleanup position
   if ( pos.ns->prepo->metascheme.mdal->destroyctxt( pos.ctxt ) ) {
      printf( "Failed to destory final postion MDAL_CTXT\n" );
      return -1;
   }

   // cleanup namespaces
   if ( rootmdal->destroynamespace( rootmdal->ctxt, "/gransom-allocation/heavily-protected-data" ) ) {
      printf( "Failed to destroy /gransom-allocation/heavily-protected-data NS\n" );
      return -1;
   }
   if ( rootmdal->destroynamespace( rootmdal->ctxt, "/gransom-allocation/read-only-data" ) ) {
      printf( "Failed to destroy /gransom-allocation/read-only-data NS\n" );
      return -1;
   }
   if ( rootmdal->destroynamespace( rootmdal->ctxt, "/gransom-allocation" ) ) {
      printf( "Failed to destroy /gransom-allocation NS\n" );
      return -1;
   }
   rootmdal->destroynamespace( rootmdal->ctxt, "/." ); // TODO : fix MDAL edge case


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


