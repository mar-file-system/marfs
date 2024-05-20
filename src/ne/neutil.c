#ifndef __MARFS_COPYRIGHT_H__
#define __MARFS_COPYRIGHT_H__

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

#endif

//int crc_status() {
//#ifdef INT_CRC
//   printf("Intermediate-CRCs: Active\n");
//   return 0;
//#else
//   printf("Intermediate-CRCs: Inactive\n");
//   return 1;
//#endif
//}

#include "erasureUtils_auto_config.h"
#if defined(DEBUG_ALL) || defined(DEBUG_NE)
#define DEBUG
#endif
#define preFMT "%s: "

#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <time.h>

#include "ne.h"

#define PRINTout(FMT, ...) fprintf(stdout, preFMT FMT, "neutil", ##__VA_ARGS__)
#ifdef DEBUG
#define PRINTdbg(FMT, ...) fprintf(stdout, preFMT FMT, "neutil", ##__VA_ARGS__)
#else
#define PRINTdbg(...)
#endif

//#if (SOCKETS != SKT_none)
//
//// <dest> is the buffer to receive the snprintf'ed path
//// <size> is the size of that buffer
//// <format> is a path template.  For sockets, on the VLE,
////           it might look like 192.168.0.%d:/zfs/exports/repo10+2/pod1/block%d/my_file
//// <block> is the current 0-based block-number (from libne)
//// <state> is whatever state was passed into ne_open1()
////
////
//// WARNING: When MarFS calls libne on behalf of RMDA-sockets-based repos,
////     it also passes in an snprintf function, plus an argument that
////     contains parts of the parsed MarFS configuration, which are used by
////     that snprintf function to compute the proper values for host-number
////     and block-number in the partially-rehydrated path-template (i.e. it
////     already has scatter-dir and cap-unit filled in), using information
////     from the configuration.
////
////     Here, we don't have that.  Instead, this is just hardwired to match
////     the latest config on the pre-prod testbed.  If the testbed changes, and
////     you change your MARFSCONFIGRC to try to fix things ... things still
////     won't be fixed, this hardwired thing will be the thing that's
////     broken.
////
////     see marfs/fuse/src/dal.c
////
//int snprintf_for_vle(char*       dest,
//                     size_t      size,
//                     const char* format,
//                     uint32_t    block,
//                     void*       state) {
//
//  int pod_offset   = 0;
//  // int host_offset  = 1 + (block / 2);  // VLE
//  int host_offset  = 7 + block;  // pre-prod
//  int block_offset = 0;
//
//  return snprintf(dest, size, format,
//                  pod_offset + host_offset, // "192.168.0.%d"
//                  block_offset + block);    // "block%d"
//}
//#endif

// Show all the usage options in one place, for easy reference
// An arrow appears next to the one you tried to use.
//
void usage(const char* prog_name, const char* op) {

   PRINTout("Usage: %s <op> [args ...]\n", prog_name);
   PRINTout("  <op> and args are like one of the following lines\n");
   PRINTout("\n");

#define USAGE(CMD, ARGS)        \
   PRINTout("  %2s %-10s %s\n", \
            (!strncmp(op, CMD, 10) ? "->" : ""), (CMD), (ARGS))

   USAGE("write", "( -c config_spec | erasure_path )               N E O partsz   [-P pod] [-C cap] [-S scatter] [-O objID] [-e] [-r] ( -s input_size | -i input_file )");
   USAGE("read", "( -c config_spec | erasure_path ) ( -n swidth | N E O partsz ) [-P pod] [-C cap] [-S scatter] [-O objID] [-e] [-r]  [-s input_size] [-o output_file]");
   USAGE("verify", "( -c config_spec | erasure_path ) ( -n swidth | N E O partsz ) [-P pod] [-C cap] [-S scatter] [-O objID] [-e]");
   USAGE("rebuild", "( -c config_spec | erasure_path ) ( -n swidth | N E O partsz ) [-P pod] [-C cap] [-S scatter] [-O objID] [-e]");
   USAGE("delete", "( -c config_spec | erasure_path )      swidth                  [-P pod] [-C cap] [-S scatter] [-O objID] [-f]");
   USAGE("stat", "( -c config_spec | erasure_path )      swidth                  [-P pod] [-C cap] [-S scatter] [-O objID]");
   //   USAGE("crc-status", "");
   USAGE("help", "");
   PRINTout("\n");

   if (strncmp(op, "help", 5)) // if help was not explicitly specified, avoid printing the entire usage block
      return;

   PRINTout("  Operations:\n");
   PRINTout("      read               Reads the content of the specified erasure stripe, utilizing erasure info only if necessary.\n");
   PRINTout("\n");
   PRINTout("      verify             Reads the content of the specified erasure stripe, including all erasure info.\n");
   PRINTout("\n");
   PRINTout("      write              Writes data to a new erasure stripe, overwriting any existing data.\n");
   PRINTout("\n");
   PRINTout("      rebuild            Reconstructs any damaged data/erasure blocks from valid blocks, if possible.\n");
   PRINTout("\n");
   PRINTout("      delete             Deletes all data, erasure, meta, and partial blocks of the given erasure stripe.  By default, \n");
   PRINTout("                          this operation prompts for confirmation before performing the deletion.\n");
   PRINTout("\n");
   PRINTout("      stat               Performs a sequential (ignoring stripe offset) read of meta information for the specified stripe \n");
   PRINTout("                          in order to determine N/E/O values.  Once these have been established, all remaining meta info \n");
   PRINTout("                          is read/verified and all data/erasure blocks are opened.  Stripe info and/or errors discovered \n");
   PRINTout("                          during this process are then displayed in a manner similar to that of '-e' option output for \n");
   PRINTout("                          for other commands (see NOTES for important output differences).\n");
   //   PRINTout("\n");
   //   PRINTout("      crc-status         Prints MAXN and MAXE values supported by libne, as well as whether intermediate crcs are active.\n");
   PRINTout("\n");
   PRINTout("      help               Prints this usage information and exits.\n");
   PRINTout("\n");
   PRINTout("  Options:\n");
   PRINTout("      -n swidth          For read/verfiy/write operations, specifies the use of the NE_NOINFO flag.\n");
   PRINTout("                          This will result in the automatic setting of N/E/start_file values based on stripe metadata.\n");
   //   PRINTout("\n");
   //   PRINTout("      -t timing_flags    Specifies flags to be passed to the libne internal timer functions.  See 'NOTES' below.\n");
   PRINTout("\n");
   PRINTout("      -e                 For read/verify/write/rebuild, specifies the use of the NE_ESTATE flag.\n");
   PRINTout("                          This will allow an e_state struct to be retrieved following the operation.  Some content of \n");
   PRINTout("                          the structure will be printed out to the console (N/E/O/bsz/totsz/meta_status/data_status).\n");
   PRINTout("                          See 'NOTES' for an explanation of subtle differences between this output and that of 'stat'.\n");
   PRINTout("\n");
   PRINTout("      -r                 Randomizes the read/write sizes used for data movement during the specified operation.\n");
   PRINTout("\n");
   PRINTout("      -s input_size      Specifies the quantity of data to be read from the data source (stripe, file, or zero-buffer).\n");
   PRINTout("\n");
   PRINTout("      -o ontput_file     Specifies a standard POSIX file to which data retrieved from an erasure stripe should be stored.\n");
   PRINTout("\n");
   PRINTout("      -i input_file      Specifies a standard POSIX file from which data should be copied to the output erasure stripe.\n");
   PRINTout("\n");
   PRINTout("      -f                 Used to perform a deletion without prompting for confirmation first.\n");
   PRINTout("\n");
   PRINTout("      -c config_spec     Specifies a XML file containing the DAL configuration. config_spec is of the form\n");
   PRINTout("                          <file_path>:/<tag>[ <attribute>=<value>]/ where the series of tags form a path through the\n");
   PRINTout("                          configuration file to the DAL configuration node.\n");
   PRINTout("                          NOTE: The configuration file overrides erasure_path, although the tool still requires an\n");
   PRINTout("                          erasure_path to be given.\n");
   PRINTout("\n");
   PRINTout("      -P pod             Specifies the pod location of the target object.\n");
   PRINTout("                          NOTE: This argument is only necessary when either the erasure path or the configuration file's\n");
   PRINTout("                          dir_template field requires a pod value to be substituted (when the path contains '{p}').\n");
   PRINTout("\n");
   PRINTout("      -C cap             Specifies the cap location of the target object.\n");
   PRINTout("                          NOTE: This argument is only necessary when either the erasure path or the configuration file's\n");
   PRINTout("                          dir_template field requires a cap value to be substituted (when the path contains '{c}').\n");
   PRINTout("\n");
   PRINTout("      -S scatter         Specifies the scatter location of the target object.\n");
   PRINTout("                          NOTE: This argument is only necessary when either the erasure path or the configuration file's\n");
   PRINTout("                          dir_template field requires a scatter value to be substituted (when the path contains '{s}').\n");
   PRINTout("\n");
   PRINTout("      -O objID           Specifies the object ID of the target object.\n");
   PRINTout("                          NOTE: This argument should only be used when either the erasure path or the configuration\n");
   PRINTout("                          file's dir_template field specifies the path format, but does not provide an objID (when the\n");
   PRINTout("                          path ends in a '/').\n");
   PRINTout("\n");
   PRINTout("  NOTES:\n");
   PRINTout("     If an input file is not specified for write, a stream of zeros will be stored to the erasure stripe up to the given \n");
   PRINTout("      input_size.  A failure to specify at least one of '-s' or '-i' for a write operation will result in an error.\n");
   PRINTout("\n");
   PRINTout("     The erasure state output produced by a 'stat' operation may differ slightly from that of '-e'.  The erasure structs \n");
   PRINTout("      returned by '-e' operations are adjusted by 'start_file' offset values, and thus indicate data/erasure status \n");
   PRINTout("      relative to the stripe format.\n");
   PRINTout("      The struct returned by ne_stat() has no such adjustment, and is thus relative to the actual file locations.\n");
   PRINTout("      Return codes for all operations are relative to actual file locations (no erasure offset).\n");
   PRINTout("\n");
   PRINTout("     <swidth> refers to the total number of data/erasure parts in the target stripe (N+E).\n");
   //   PRINTout("\n");
   //   PRINTout("     <timing_flags> can be decimal, or can be hex-value starting with \"0x\"\n");
   //   PRINTout("                   OPEN    =  0x0001\n");
   //   PRINTout("                   RW      =  0x0002     /* each individual read/write, in given stream */\n");
   //   PRINTout("                   CLOSE   =  0x0004     /* cost of close */\n");
   //   PRINTout("                   RENAME  =  0x0008\n");
   //   PRINTout("                   STAT    =  0x0010\n");
   //   PRINTout("                   XATTR   =  0x0020\n");
   //   PRINTout("                   ERASURE =  0x0040\n");
   //   PRINTout("                   CRC     =  0x0080\n");
   //   PRINTout("                   THREAD  =  0x0100     /* from beginning to end  */\n");
   //   PRINTout("                   HANDLE  =  0x0200     /* from start/stop, all threads, in 1 handle */\n");
   //   PRINTout("                   SIMPLE  =  0x0400     /* diagnostic output uses terse numeric formats */\n");
   PRINTout("\n");
   PRINTout("     <erasure_path> is of the following format\n");
   PRINTout("                    /NFS/blah/block%%d/.../fname\n");
   PRINTout("                     ('/NFS/blah/'  is some NFS path on the client nodes)\n");
   //   PRINTout("       [RDMA] xx.xx.xx.%%d:pppp/local/blah/block%%d/.../fname\n");
   //   PRINTout("               ('/local/blah' is some local path on all accessed storage nodes)\n");
   //   PRINTout("       [MC]   /NFS/blah/block%%d/.../fname\n");
   //   PRINTout("               ('/NFS/blah/'  is some NFS path on the client nodes)\n");
   PRINTout("\n");

#undef USAGE
}

//int parse_flags(TimingFlagsValue* flags, const char* str) {
//   if (! str)
//      *flags = 0;
//   else {
//      errno = 0;
//      // strtol() already detects the '0x' prefix for us
//      *flags = (TimingFlagsValue)strtol(str, NULL, 0);
//      if (errno) {
//         PRINTout("couldn't parse flags from '%s'\n", str);
//         return -1;
//      }
//   }
//
//   return 0;
//}
//
//
//uDALType
//select_impl(const char* path) {
//   return (strchr(path, ':')
//           ? UDAL_SOCKETS
//           : UDAL_POSIX);
//}
//
//SnprintfFunc
//select_snprintf(const char* path) {
//#if (SOCKETS != SKT_none)
//   return (strchr(path, ':')
//           ? snprintf_for_vle      // MC over RDMA-sockets
//           : ne_default_snprintf); // MC over NFS
//#else
//   return ne_default_snprintf; // MC over NFS
//#endif
//}

void print_erasure_state(ne_erasure* epat, ne_state* state) {
   PRINTout("====================== Erasure State ======================\n");
   PRINTout("N: %d  E: %d  O: %d  partsz: %zu  versz: %zu  blocksz: %zu  totsz: %llu\n",
      epat->N, epat->E, epat->O, epat->partsz, state->versz, state->blocksz, (unsigned long long)state->totsz);
   // this complicated declaration is simply meant to ensure that we have space for
   //  a null terminator and up to 5 chars per potential array element
   char* output_string = malloc(((epat->N + epat->E) * 5) + 1);
   if (output_string == NULL) {
      PRINTout("Failed to allocate space for an internal string!\n");
      return;
   }
   output_string[0] = '\0'; // the initial strncat() call will expect a null terminator
   int tmp;
   // construct a list of physical block numbers based on the provided start_block
   for (tmp = 0; tmp < (epat->N + epat->E); tmp++) {
      char append_str[12] = { '\0' };
      snprintf(append_str, 12, "%4d", (tmp + epat->O) % (epat->N + epat->E));
      strcat(output_string, append_str);
   }

   PRINTout("%s%s\n", "Physical Block:     ", output_string);
   output_string[0] = '\0'; // this is effectively the same as clearing the string

   int eerr = 0;
   // construct a list of meta_status array elements for later printing
   for (tmp = 0; tmp < (epat->N + epat->E); tmp++) {
      if (state->meta_status[tmp])
         eerr++;
      char append_str[6] = { '\0' };
      snprintf(append_str, 6, "%4d", state->meta_status[tmp]);
      strcat(output_string, append_str);
   }

   PRINTout("%s%s\n", "Metadata Errors:    ", output_string);
   output_string[0] = '\0'; // this is effectively the same as clearing the string

   int nerr = 0;
   // construct a list of data_status array elements for later printing
   for (tmp = 0; tmp < (epat->N + epat->E); tmp++) {
      if (state->data_status[tmp])
         nerr++;
      char append_str[6] = { '\0' };
      snprintf(append_str, 6, "%4d", state->data_status[tmp]);
      strcat(output_string, append_str);
   }

   PRINTout("%s%s\n", "Data/Erasure Errors:", output_string);
   free(output_string);

   if (nerr > epat->E || eerr > epat->E)
      PRINTout("WARNING: excessive errors were found, and the data may be unrecoverable!\n");
   else if (nerr > 0 || eerr > 0)
      PRINTout("WARNING: errors were found, be sure to rebuild this object before data loss occurs!\n");
   PRINTout("===========================================================\n");
}

int main(int argc, const char** argv) {
   errno = 0; // init to zero (apparently not guaranteed)
   void* buff;

   char wr = -1; // defines the operation being performed ( 0 = read, 1 = write, 2 = rebuild, 3 = verify, 4 = stat, 5 = delete )
   int N = -1;
   int E = -1;
   int O = -1;
   size_t partsz = 0;
   char* erasure_path = NULL;
   //   TimingFlagsValue   timing_flags = 0;
   char size_arg = 0;
   char rand_size = 0;
   char no_info = 0;
   char force_delete = 0;
   char show_state = 0;
   char* output_file = NULL;
   char* input_file = NULL;
   char* config_path = NULL;
   char* objID = "";

   ne_location neloc = { .pod = 0, .cap = 0, .scatter = 0 };

   size_t totbytes = 0;

   char pr_usage = 0;
   int c;
   // parse all position-independent arguments
   while ((c = getopt(argc, (char* const*)argv, "t:i:o:s:n:c:refhP:C:S:O:")) != -1) {
      switch (c) {
         char* endptr;
         //         case 't':
         //            if ( parse_flags(&timing_flags, optarg) ) {
         //               PRINTout( "failed to parse timing flags value: \"%s\"\n", optarg );
         //               pr_usage = 1;
         //            }
         //            break;
      case 'i':
         input_file = optarg;
         break;
      case 'o':
         output_file = optarg;
         break;
      case 's':
         size_arg = 1;
         totbytes = strtoll(optarg, &endptr, 10);
         // afterwards, check for a parse error
         if (*endptr != '\0') {
            PRINTout("%s: failed to parse argument for '-s' option: \"%s\"\n", argv[0], optarg);
            usage(argv[0], "help");
            return -1;
         }
         break;
      case 'r':
         rand_size = 1;
         break;
      case 'n':
         no_info = 1;
         N = strtoll(optarg, &endptr, 10);
         // afterwards, check for a parse error
         if (*endptr != '\0') {
            PRINTout("%s: failed to parse argument for '-n' option: \"%s\"\n", argv[0], optarg);
            usage(argv[0], "help");
            return -1;
         }
         E = 0; // so N+E is still stripe width
         break;
      case 'c':
         config_path = optarg;
         break;
      case 'e':
         show_state = 1;
         break;
      case 'f':
         force_delete = 1;
         break;
      case 'h':
         usage(argv[0], "help");
         return 0;
      case 'P':
         neloc.pod = strtol(optarg, &endptr, 10);
         // afterwards, check for a parse error
         if (*endptr != '\0') {
            PRINTout("%s: failed to parse argument for '-P' option: \"%s\"\n", argv[0], optarg);
            usage(argv[0], "help");
            return -1;
         }
         break;
      case 'C':
         neloc.cap = strtol(optarg, &endptr, 10);
         // afterwards, check for a parse error
         if (*endptr != '\0') {
            PRINTout("%s: failed to parse argument for '-C' option: \"%s\"\n", argv[0], optarg);
            usage(argv[0], "help");
            return -1;
         }
         break;
      case 'S':
         neloc.scatter = strtol(optarg, &endptr, 10);
         // afterwards, check for a parse error
         if (*endptr != '\0') {
            PRINTout("%s: failed to parse argument for '-S' option: \"%s\"\n", argv[0], optarg);
            usage(argv[0], "help");
            return -1;
         }
         break;
      case 'O':
         objID = optarg;
         break;
      case '?':
         pr_usage = 1;
         break;
      default:
         PRINTout("failed to parse command line options\n");
         return -1;
      }
   }

   char* operation = NULL;
   // parse all position/command-dependent arguments
   for (c = optind; c < argc; c++) {
      if (wr < 0) { // if no operation specified, the first arg should define it
         if (strcmp(argv[c], "read") == 0)
            wr = 0;
         else if (strcmp(argv[c], "write") == 0)
            wr = 1;
         else if (strcmp(argv[c], "verify") == 0)
            wr = 2;
         else if (strcmp(argv[c], "rebuild") == 0)
            wr = 3;
         else if (strcmp(argv[c], "delete") == 0)
            wr = 4;
         else if (strcmp(argv[c], "stat") == 0)
            wr = 5;
         //         else if ( strcmp( argv[c], "crc-status" ) == 0 ) {
         //            PRINTout( "MAXN: %d     MAXE: %d\n", MAXN, MAXE );
         //            crc_status();
         //            return 0;
         //         }
         else if (strcmp(argv[c], "help") == 0) {
            usage(argv[0], argv[c]);
            return 0;
         }
         else {
            PRINTout("%s: unrecognized operation argument provided: \"%s\"\n", argv[0], argv[c]);
            usage(argv[0], "help");
            return -1;
         }
         operation = (char*)argv[c];
      }
      else if (erasure_path == NULL && config_path == NULL) { // all operations require this as the next argument
         erasure_path = (char*)argv[c];
      }
      else if ((wr < 4) && !(no_info) && (partsz == 0)) { // loop through here until N/E/O/partsz are populated, if this operation needs them
         char* arg = "";
         char initval = '\0';
         char* endptr = &initval;
         if (N == -1) {
            arg = "N";
            N = strtol(argv[c], &endptr, 10);
         }
         else if (E == -1) {
            arg = "E";
            E = strtol(argv[c], &endptr, 10);
         }
         else if (O == -1) {
            arg = "O";
            O = strtol(argv[c], &endptr, 10);
         }
         else {
            arg = "partsz";
            partsz = strtol(argv[c], &endptr, 10);
         }
         // afterwards, check for a parse error
         if (*endptr != '\0') {
            PRINTout("%s: failed to parse value for %c: \"%s\"\n", argv[0], *arg, argv[c]);
            usage(argv[0], operation);
            return -1;
         }
      }
      else if ((wr >= 4) && (N == -1)) { // for delete/stat, store the stripe width to 'N'
         char* endptr;
         N = strtol(argv[c], &endptr, 10);
         if (*endptr != '\0') {
            PRINTout("%s: failed to parse value for stripe-width: \"%s\"\n", argv[0], argv[c]);
            usage(argv[0], operation);
            return -1;
         }
         E = 0;
      }
      else {
         PRINTout("%s: encountered unrecognized argument: \"%s\"\n", argv[0], argv[c]);
         usage(argv[0], operation);
         return -1;
      }
   }

   // verify that we received all required args
   if (operation == NULL) {
      PRINTout("%s: no operation specified\n", argv[0]);
      usage(argv[0], "help");
      return -1;
   }
   if ((erasure_path == NULL && config_path == NULL) || ((wr >= 4) && (N == -1)) || ((wr < 4) && !(no_info) && (partsz == 0))) {
      PRINTout("%s: missing required arguments for operation: \"%s\"\n", argv[0], operation);
      usage(argv[0], operation);
      return -1;
   }

   // warn if improper options were specified for a given operation
   if ((input_file != NULL) && (wr != 1)) {
      PRINTout("%s: the '-i' flag is not applicable to operation: \"%s\"\n", argv[0], operation);
      usage(argv[0], operation);
      return -1;
   }
   if ((rand_size) && (wr > 2)) {
      PRINTout("%s: the '-r' flag is not applicable to operation: \"%s\"\n", argv[0], operation);
      usage(argv[0], operation);
      return -1;
   }
   if ((size_arg) && (wr > 2)) {
      PRINTout("%s: the '-s' flag is not applicable to operation: \"%s\"\n", argv[0], operation);
      usage(argv[0], operation);
      return -1;
   }
   if ((no_info) && (wr != 0 && wr != 2 && wr != 3)) {
      PRINTout("%s: the '-n' flag is not applicable to operation: \"%s\"\n", argv[0], operation);
      usage(argv[0], operation);
      return -1;
   }
   if ((show_state) && (wr > 3)) {
      PRINTout("%s: the '-e' flag is not applicable to operation: \"%s\"\n", argv[0], operation);
      usage(argv[0], operation);
      return -1;
   }
   if ((output_file != NULL) && (wr != 0 && wr != 2)) {
      PRINTout("%s: the '-o' flag is not applicable to operation: \"%s\"\n", argv[0], operation);
      usage(argv[0], operation);
      return -1;
   }

   // check specifically that a write operation has at least an input file and/or a write size
   if ((wr == 1) && (input_file == NULL) && !(size_arg)) {
      PRINTout("%s: missing required arguments for operation: \"%s\"\n", argv[0], operation);
      PRINTout("%s: write operations require one or both of the '-s' and '-i' options\n", argv[0]);
      usage(argv[0], operation);
      return -1;
   }

   // if we've made it all the way here without hitting a hard error, make sure to still print usage from a previous 'soft' error
   if (pr_usage) {
      usage(argv[0], operation);
      return -1;
   }

   PRINTout("performing a '%s' command\n", operation);

   // first, establish an ne_context
   ne_ctxt ctxt = NULL;
   xmlNode* root = NULL;
   if (config_path) {
      xmlDoc* doc = NULL;

      char* fields;
      if (!(fields = strchr(config_path, ':'))) {
         printf("error: could not separate path from fields %s\n", config_path);
         return -1;
      }
      *fields = '\0';
      fields += 2;

      /*
      * this initialize the library and check potential ABI mismatches
      * between the version it was compiled for and the actual shared
      * library used.
      */
      LIBXML_TEST_VERSION

         /*parse the file and get the DOM */
         doc = xmlReadFile(config_path, NULL, XML_PARSE_NOBLANKS);

      if (doc == NULL) {
         printf("error: could not parse file %s\n", config_path);
         return -1;
      }

      /*Get the root element node */
      root = xmlDocGetRootElement(doc);

      char* tag = NULL;
      char* attr = NULL;
      char* val = NULL;
      int last = 0;
      while (root) {
         char* next;
         if (!tag) {
            if (!(next = strchr(fields, '/'))) {
               next = strchr(fields + 1, '\0');
               last = 1;
            }
            *next = '\0';
            if ((attr = strchr(fields, ' '))) {
               *attr = '\0';
               attr++;
               if (!(val = strchr(attr, '='))) {
                  printf("error: could not find field value for attribute %s in field with tag %s\n", attr, fields);
                  return -1;
               }
               else {
                  *val = '\0';
                  val++;
               }
            }
            tag = fields;
            fields = next + 1;
         }
         if (root->type == XML_ELEMENT_NODE && !strcmp((char*)root->name, tag)) {
            if (val) {
               xmlAttr* type = root->properties;
               while (type) {
                  if (type->type == XML_ATTRIBUTE_NODE && !strcmp((char*)type->name, attr) && type->children->type == XML_TEXT_NODE && !strcmp((char*)type->children->content, val)) {
                     if (!last) {
                        root = root->children;
                     }
                     tag = NULL;
                     attr = NULL;
                     val = NULL;
                     break;
                  }
                  type = type->next;
               }
               if (!type) {
                  root = root->next;
                  /*printf("error: could not find attribute %s with value %s in field %s within file %s\n", attr, val, tag, config_path);
                  return -1;*/
               }
               if (last) {
                  break;
               }
            }
            else {
               if (last) {
                  break;
               }
               root = root->children;
               tag = NULL;
               attr = NULL;
               val = NULL;
            }
         }
         else {
            root = root->next;
         }
      }
      if (!root) {
         printf("error: could not find DAL in file %s\n", config_path);
         return -1;
      }

      ctxt = ne_init(root, neloc, N + E, NULL);

      /* Free the xml Doc */
      xmlFreeDoc(doc);
      /*
      *Free the global variables that may
      *have been allocated by the parser.
      */
      xmlCleanupParser();
   }
   else {
      ctxt = ne_path_init(erasure_path, neloc, N + E, NULL);
   }
   if (!ctxt) {
      PRINTout("Failed to establish an ne_ctxt!\n");
      return -1;
   }

   // -----------------------------------------------------------------
   // delete
   // -----------------------------------------------------------------

   if (wr == 4) {
      char iter = 0;
      while (!(force_delete)) {
         char response[20] = { 0 };
         *(response) = '\n';
         PRINTout("deleting striping corresponding to path \"%s\" with width %d...\n"
            "Are you sure you wish to continue? (y/n): ",
            (char*)argv[2], N);
         fflush(stdout);
         while (*(response) == '\n') {
            if (response != fgets(response, 20, stdin)) {
               PRINTout("failed to read input\n");
               return -1;
            }
         }
         // check for y/n response
         if (*(response) == 'n' || *(response) == 'N')
            return -1;
         if (*(response) == 'y' || *(response) == 'Y')
            break;
         PRINTout("input unrecognized\n");
         // clear excess chars from stdin, one at a time
         while (*(response) != '\n' && *(response) != EOF)
            *(response) = getchar();
         if (*(response) == EOF) {
            PRINTout("terminating due to lack of user input\n");
            return -1;
         }
         iter++; // see if this has happened a lot
         if (iter > 4) {
            PRINTout("terminating due to excessive unrecognized user input\n");
            return -1;
         }
      }
      if (ne_delete(ctxt, objID, neloc)) {
         PRINTout("deletion attempt indicates a failure for path \"%s\": errno=%d (%s)\n",
            (char*)argv[2], errno, strerror(errno));
         return -1;
      }
      PRINTout("deletion successful\n");
      if (ne_term(ctxt)) {
         PRINTout("Failed to properly free ne_ctxt!\n");
         return -1;
      }
      return 0;
   }

   ne_handle handle = NULL;
   ne_erasure epat = { .N = N, .E = E, .O = O, .partsz = partsz };
   ne_state state;
   state.meta_status = NULL;
   state.data_status = NULL;
   state.csum = NULL;

   // check if we need to use stat to get stripe structure
   if (no_info || wr == 5) {
      handle = ne_stat(ctxt, objID, neloc);
      if (!handle) {
         PRINTout("Failed to open a handle reference with ne_stat()!\n");
         return -1;
      }
      if (ne_get_info(handle, &epat, NULL) < 0) {
         PRINTout("Failed to retrieve info from stat handle!\n");
         return -1;
      }
      // set stripe values based on the modified epat struct
      N = epat.N;
      E = epat.E;
      O = epat.O;
      partsz = epat.partsz;
   }
   state.meta_status = calloc(sizeof(char), (N + E) * 2);
   if (state.meta_status == NULL) {
      PRINTout("Failed to allocate space for a meta_status array!\n");
      return -1;
   }
   state.data_status = state.meta_status + (N + E);

   // -----------------------------------------------------------------
   // stat
   // -----------------------------------------------------------------

   if (wr == 5) {
      if (erasure_path) {
         PRINTout("retrieving status of erasure striping with path \"%s%s\"\n", erasure_path, objID);
      }
      else {
         PRINTout("retrieving status of erasure striping with config path \"%s\" and objID \"%s\"\n", config_path, objID);
      }

      int ret;
      if ((ret = ne_close(handle, &epat, &state)) < 0) {
         PRINTout("ne_close failed: errno=%d (%s)\n", errno, strerror(errno));
         return -1;
      }

      // the positions of these meta/data errors DO NOT take stripe offset into account
      print_erasure_state(&epat, &state);
      // display the ne_stat return value
      PRINTout("stat rc: %d\n", ret);

      if (ne_term(ctxt)) {
         PRINTout("Failed to properly free ne_ctxt!\n");
         return -1;
      }
      free(state.meta_status);
      return 0;
   }

   //   SktAuth  auth;
   //   if (DEFAULT_AUTH_INIT(auth)) {
   //      PRINTout("%s: failed to initialize default socket-authentication credentials\n", argv[0] );
   //      return -1;
   //   }
   int tmp;

   // -----------------------------------------------------------------
   // rebuild
   // -----------------------------------------------------------------

   if (wr == 3) {
      if (handle == NULL) {
         handle = ne_open(ctxt, objID, neloc, epat, NE_REBUILD);
         if (handle == NULL) {
            PRINTout("Failed to open handle for REBUILD!\n");
            return -1;
         }
      }
      else {
         if ((handle = ne_convert_handle(handle, NE_REBUILD)) == NULL) {
            PRINTout("Failed to convert stat handle to REBUILD handle!\n");
            return -1;
         }
      }

      PRINTout("rebuilding erasure striping (N=%d,E=%d,O=%d)\n", N, E, O);

      int attempts = 1;
      while ((attempts < 4) && (tmp = ne_rebuild(handle, &epat, &state)) > 0) {
         PRINTout("%d errors remain after rebuild %d\n", tmp, attempts);
         if ((show_state)) {
            PRINTout("Stripe state pre-rebuild %d:\n", attempts);
            // the positions of these meta/data errors DO take stripe offset into account
            print_erasure_state(&epat, &state);
         }
         attempts++;
      }

      if ((show_state)) {
         PRINTout("Stripe state pre-rebuild %d:\n", attempts);
         // the positions of these meta/data errors DO take stripe offset into account
         print_erasure_state(&epat, &state);
      }

      if ((tmp)) {
         PRINTout("Rebuild failed to correct all errors: errno=%d (%s)\n", errno, strerror(errno));
         if (tmp < 0)
            PRINTout("rebuild failed!\n");
         else
            PRINTout("rebuild indicates only partial success: rc = %d\n", tmp);
      }
      else
         PRINTout("rebuild complete\n");

      PRINTout("rebuild rc: %d\n", tmp);

      if (ne_close(handle, NULL, NULL) < 0) {
         PRINTout("Failed to close ne_handle!\n");
         return -1;
      }

      if (ne_term(ctxt)) {
         PRINTout("Failed to properly free ne_ctxt!\n");
         return -1;
      }
      free(state.meta_status);
      return 0;
   }

   // -----------------------------------------------------------------
   // read / write / verify
   // -----------------------------------------------------------------

   unsigned long long buff_size = (epat.partsz + 1) * N; //choose a buffer size that can potentially read/write beyond a stripe boundary
                                                         //this is meant to hit more edge cases, not performance considerations
   if (totbytes == 0) {
      totbytes = buff_size;
   }

   srand(time(NULL));

   // allocate space for a data buffer and zero out so that we could zero write using it
   buff = NULL;
   if (output_file != NULL || wr == 1) { // only allocate this buffer if we are writing to something
      buff = memset(malloc(sizeof(char) * buff_size), 0, buff_size);
      if (buff == NULL) {
         PRINTout("failed to allocate space for a data buffer\n");
         return -1;
      }
   }

   int std_fd = 0; // no way this FD gets reused, so safe to initialize to this
   if (output_file != NULL)
      std_fd = open(output_file, (O_WRONLY | O_CREAT), 0600); //  | O_EXCL
   else if (input_file != NULL)
      std_fd = open(input_file, O_RDONLY);

   // verify a proper open of our standard file
   if (std_fd < 0) {
      if (output_file != NULL)
         PRINTout("failed to open output file \"%s\": errno=%d (%s)\n",
            output_file, errno, strerror(errno));
      else
         PRINTout("failed to open input file \"%s\": errno=%d (%s)\n",
            input_file, errno, strerror(errno));
      if (buff)
         free(buff);
      if (ne_term(ctxt)) {
         PRINTout("Failed to properly free ne_ctxt!\n");
      }
      free(state.meta_status);
      return -1;
   }

   ne_mode mode = NE_RDALL; //verify
   if (wr == 0) { // read
      mode = NE_RDONLY;
   }
   else if (wr == 1) { // write
      mode = NE_WRONLY;
   }

   // open our handle
   if (handle == NULL) {
      handle = ne_open(ctxt, objID, neloc, epat, mode);
      // check for a successful open of the handle
      if (handle == NULL) {
         PRINTout("failed to open the requested erasure path for a %s operation: errno=%d (%s)\n",
            operation, errno, strerror(errno));
         if (buff)
            free(buff);
         return -1;
      }
   }
   // otherwise, convert the existing handle
   else {
      if (ne_convert_handle(handle, mode) == NULL) {
         PRINTout("Failed to convert stat handle to new mode!\n");
         if (buff) {
            free(buff);
         }
         if (ne_term(ctxt)) {
            PRINTout("Failed to properly free ne_ctxt!\n");
         }
         free(state.meta_status);
         return -1;
      }
   }

   unsigned long long toread;

   if (rand_size)
      toread = (rand() % buff_size) + 1;
   else
      toread = buff_size;
   if (toread > totbytes)
      toread = totbytes;

   off_t bytes_moved = 0;
   while (toread > 0) {
      // READ DATA
      ssize_t nread = toread; // assume success if no read takes place
      if ((wr == 1) && (std_fd)) { // if input_file was defined, writes get data from it
         PRINTdbg("reading %llu bytes from \"%s\"\n", toread, input_file);
         nread = read(std_fd, buff, toread);
      }
      else if (wr != 1) { // read/verify get data from the erasure stripe
         PRINTdbg("reading %llu bytes from erasure stripe\n", toread);
         nread = ne_read(handle, buff, toread);
         // Note: if buff is NULL here, retrieved data will simply be thrown out
      }

      // check for a read error
      if ((nread < 0) || ((size_arg) && (nread < toread))) {
         PRINTout("expected to read %llu bytes from source, but instead received %zd: errno=%d (%s)\n",
            toread, nread, errno, strerror(errno));
         if (buff)
            free(buff);
         ne_close(handle, NULL, NULL);
         if (std_fd)
            close(std_fd);
         return -1;
      }

      // WRITE DATA
      size_t written = nread; // no write performed -> success
      if (wr == 1) { // for write, just output to the stripe
         PRINTdbg("writing %zd bytes to erasure stripe\n", nread);
         written = ne_write(handle, buff, nread);
      }
      else if (std_fd) { // for read/verify, only write out if given the -o flag
         PRINTdbg("writing %zd bytes to \"%s\"\n", nread, output_file);
         written = write(std_fd, buff, nread);
      }

      // check for a write error
      if (nread != written) {
         PRINTout("expected to write %zd bytes to destination, but instead wrote %zd: errno=%d (%s)\n",
            nread, written, errno, strerror(errno));
         if (buff)
            free(buff);
         ne_close(handle, NULL, NULL);
         if (std_fd)
            close(std_fd);
         return -1;
      }

      // increment our counters
      bytes_moved += nread;

      // if size wasn't specified, only read until we can't any more
      if (!(size_arg) && (nread < toread)) {
         toread = 0;
      }
      else {
         // determine how much to read next time
         if (rand_size)
            toread = (rand() % buff_size) + 1;
         else
            toread = buff_size;
         // if we are going beyond the specified size, limit our reads
         if ((size_arg) && (toread > (totbytes - bytes_moved)))
            toread = totbytes - bytes_moved;
      }
   }

   PRINTout("all data movement completed (%lld bytes)\n", (long long int)bytes_moved);

   if (std_fd && close(std_fd)) {
      if (wr == 1)
         PRINTout("encountered an error when trying to close input file\n");
      else
         PRINTout("encountered an error when trying to close output file\n");
   }

   // free our work buffer, if we allocated one
   if (buff)
      free(buff);

   // close the handle and indicate it's close condition
   tmp = ne_close(handle, &epat, &state);

   if ((show_state)) {
      // the positions of these meta/data errors DO take stripe offset into account
      print_erasure_state(&epat, &state);
   }

   PRINTout("close rc = %d\n", tmp);

   if (ne_term(ctxt)) {
      PRINTout("Failed to properly free ne_ctxt!\n");
      return -1;
   }
   free(state.meta_status);
   return 0;
}
