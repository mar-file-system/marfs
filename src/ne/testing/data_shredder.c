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


#include <stdio.h>
//#include <stdarg.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <math.h>
#include <limits.h>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>



#define PRINTERR(...)   fprintf( stderr, "data_shredder: "__VA_ARGS__)
#define PRINTOUT(...)   fprintf( stdout, "data_shredder: "__VA_ARGS__)

// global var for start time of this prog
time_t curtime;

// helper function to print this programs usage info
void print_usage() {
   printf( "This program is inteneded to exercise the libne rebuild functionality by \n"
           "  inserting data corruption into an existing erasure stripe.  This should be used \n"
           "  with EXTREME CAUTION, as it has the potential to render data permanently \n"
           "  unrecoverable if errors were already present.\n"
           "  As a safety measure, backups of the original files are created (by appending a \n"
           "  '.shrd_backup.<TS>' suffix) before the corruption is inserted.  Note, however, \n"
           "  that these backup files will not have any xattrs/manifest info attached.\n"
           "  To reiterate, NEVER RUN THIS PROGRAM AGAINST USER DATA!\n\n" );
   printf( "usage:  data_shredder -d <corruption_distribution> -o <start_offset>:<end_offset> \n"
           "                      [-b <libne_block-size>] [-n <N>] [-e <E>] [-f] \n"
           "                      <erasure_path_fmt>\n" );
   printf( "  <erasure_path_fmt>   The format string for the erasure stripe to be corrupted, \n"
           "                        including a '%%d' character to be replaced by each block-\n"
           "                        file index\n" );
   printf( "        -d   Specifies the pattern in which data corruption should be inserted\n"
           "               Options = shotgun (random distribution), diagonal_up (adjacent \n"
           "               corruption along a stripe, shifted to higher index block-files in \n"
           "               later stripes), diagonal_down (adjacent corruption along a stripe, \n"
           "               shifted to lower index block-files in later stripes)\n" );
   printf( "        -o   Specifies the offsets within each block-file which should have their \n"
           "               content corrupted\n"
           "               Note that the offsets provided will be alligned to the next lower \n"
           "               (for lower bound) or next higher stripe-boundary (for upper bound)\n" );
   printf( "        -b   Specifies the libne block size\n" );
   printf( "        -n   Specifies the libne stripe data-width \n" );
   printf( "        -e   Specifies the libne stripe erasure-width\n" );
   printf( "        -f   Forces the corruption operation to take place without prompting the \n"
           "               user for confirmation beforehand\n" );
}


// helper function to copy a given file to a new location
off_t backup_file( const char* original ) {
   char* destination = malloc( strlen(original) + 30 );
   if( destination == NULL ) {
      PRINTERR( "failed to allocate space for the name of a new backup file\n" );
      return 0;
   }

   sprintf( destination, "%s.shrd_backup.", original );
   char* tdest = destination + strlen(original) + 13;
   strftime( tdest, 16, "%m%d%y-%H%M%S", localtime(&curtime) );

   PRINTOUT( "backing up file \"%s\" to \"%s\"\n", original, destination );

   char buffer[4096];
   size_t bytes_read;
   size_t bytes_written;
   off_t total_written = 0;
   char fail = 0;

   // open both the input and output files
   int fd_orig = open( original, O_RDONLY );
   if( fd_orig < 0 ) {
      PRINTERR( "failed to open file \"%s\" for read\n", original );
      free(destination);
      return 0;
   }
   mode_t mask = umask(0000);
   int fd_dest = open( destination, O_WRONLY | O_CREAT | O_EXCL, 0666 );
   umask( mask );
   if( fd_dest < 0 ) {
      PRINTERR( "failed to open file \"%s\" for write\n", destination );
      close( fd_orig );
      free(destination);
      return 0;
   }

   while( fail == 0  &&  (bytes_read = read( fd_orig, buffer, sizeof(buffer) )) > 0 ) {
      bytes_written = write( fd_dest, buffer, sizeof(buffer) );
      
      if( bytes_written != bytes_read ) {
         PRINTERR( "failed to write to output file \"%s\"\n", destination );
         bytes_read = 0;
         fail = 1;
      }
      total_written += bytes_written;
   }

   if( close( fd_orig ) ) {
      PRINTERR( "failed to properly close input file \"%s\"\n", original );
   }
   if( close( fd_dest ) ) {
      PRINTERR( "failed to properly close output file \"%s\"\n", destination );
      fail = 1;
   }

   if( bytes_read != 0 ) {
      PRINTERR( "failed to read input file \"%s\"\n", original );
      fail = 1;
   }

   free(destination);

   if( fail )
      return 0;

   return total_written;
}


// return a pseudo-random int in the range [0,limit)
int rand_under( int limit ) {
   return (int)(rand() % limit);
}


// write a corrupted block into a erasure/data file at the given offset
int corrupt_block( int FDArray[ MAXPARTS ], char* randbuff, unsigned long bsz, unsigned long long coff, int cblock ) {
   if( lseek( FDArray[ cblock ], coff, SEEK_SET ) < 0 ) {
      PRINTERR( "failed to seek data/erasure file with index %d\n", cblock );
      return -1;
   }

   if( write( FDArray[ cblock ], randbuff, bsz ) != bsz ) {
      PRINTERR( "failed to write to data/erasure file wiht index %d at offset %llu\n", cblock, coff );
      return -1;
   }


   return 0;
}


int main( int argc, char** argv ) {
   unsigned char distrib = 0;
   char offsetP = 0;
   char* pathpat = NULL;
   unsigned long long shred_range[2] = {0};
   unsigned long bsz = 0;
   int N = -1;
   int E = -1;
   
   int opt;
   char fflag = 0;
   char* endptr;
   unsigned long input;

   // get the current time
   time(&curtime);

   // parse arguments
   while( (opt = getopt( argc, argv, "d:o:b:n:e:fh" )) != -1 ) {
      switch( opt ) {
         case 'd':
            if( distrib != 0 ) {
               PRINTERR( "received duplicate '-d' argument, only the last argument will be honored\n" );
               distrib = 0;
            }
            // looking for shotgun or diagonals
            if( strncmp( optarg, "shotgun", 78 ) == 0 ) {
               distrib = 1;
            }
            else if( strncmp( optarg, "diagonal", 8 ) == 0 ) {
               char* tmp = optarg+8;
               if( strncmp( tmp, "_up", 4 ) == 0 ) {
                  distrib = 2;
               }
               else if( strncmp( tmp, "_down", 6 ) == 0 ) {
                  distrib = 3;
               }
            }
            // warn if the distribution was not recognized
            if( distrib == 0 ) {
               PRINTERR( "received unrecognized error distribution arg: \"%s\"\n", optarg );
            }
            break;
         case 'o':
            // get the start offset for the corruption pattern
            errno = 0;
            if( offsetP )
               PRINTERR( "received multiple '-o' arguements: only the last valid argument will be used\n" );
            offsetP = 0;
            shred_range[0] = strtoull( optarg, &(endptr), 10 );
            if( *endptr != ':' ) {
               PRINTERR( "expected a '<low_offset>:<high_offset>' argument following the '-o' option"
                     " but encountered unexpected char: \"%c\"\n", *endptr );
               return -1;
            }
            if( errno != 0 ) {
               PRINTERR( "failed to properly parse offset range \"%s\": expected '<low_offset>:<high_offset>'\n", 
                     optarg );
               return -1;
            }
            // now get the end offset
            char* secstr = endptr + 1;
            if( strncmp( secstr, "end", 4 ) == 0 ) { 
               // handle special value of "end" as max offset
               shred_range[1] = 0;
            }
            else {
               // parse the remaining string to get the max offset
               shred_range[1] = strtoull( secstr, &(endptr), 10 );
               if( *endptr != '\0' ) {
                  PRINTERR( "expected a '<low_offset>:<high_offset>' argument following the '-o' option"
                        " but encountered unexpected char: \"%c\"\n", *endptr );
                  return -1;
               }
               if( errno != 0 ) {
                  PRINTERR( "failed to properly parse offset range \"%s\": expected '<low_offset>:<high_offset>'\n", 
                        optarg );
                  return -1;
               }
               // the end offset must be non-zero for us to do anything
               if( shred_range[1] < 1 ) {
                  PRINTERR( "received an invalid shred range: ending offset < 1 implies nothing to be done\n" );
                  return -1;
               }
            }
            offsetP = 1;
            break;
         case 'b':
            // parse and set the new bsz
            errno = 0;
            bsz = strtoul( optarg, &(endptr), 10 );
            if( *endptr != '\0' ) {
               PRINTERR( "expected an unsigned numeric argument following the '-b' option"
                     " but encountered unexpected char: \"%c\"\n", *endptr );
               return -1;
            }  
            if( errno != 0 ) {
               PRINTERR( "failed to properly parse block-size \"%s\"\n",
                     optarg );
               return -1;
            }
            if( bsz > MAXBLKSZ ) {
               PRINTERR( "input value for block-size exceeds the limits defined in libne: %lu\n", bsz );
               return -1;
            }
            break;
         case 'n':
            // parse and set the new N
            errno = 0;
            input = strtoul( optarg, &(endptr), 10 );
            if( *endptr != '\0' ) {
               PRINTERR( "expected an unsigned numeric argument following the '-n' option"
                         " but encountered unexpected char: \"%c\"\n", *endptr );
               return -1;
            }
            if( errno != 0 ) {
               PRINTERR( "failed to properly parse n value \"%s\"\n",
                          optarg );
               return -1;
            }
            // check for a bounds violation
            if( input > MAXN  ||  input < 1 ) {
               PRINTERR( "input value for N exceeds the limits defined in libne: MAXN = %d\n", MAXN );
               return -1;
            }
            N = (int) input;
            break;
         case 'e':
            // parse and set the new E
            errno = 0;
            input = strtoul( optarg, &(endptr), 10 );
            if( *endptr != '\0' ) {
               PRINTERR( "expected an unsigned numeric argument following the '-e' option"
                         " but encountered unexpected char: \"%c\"\n", *endptr );
               return -1;
            }
            if( errno != 0 ) {
               PRINTERR( "failed to properly parse e value \"%s\"\n",
                          optarg );
               return -1;
            }
            // check for a bounds violation
            if( input > MAXE ) {
               PRINTERR( "input value for E exceeds the limits defined in libne: MAXE = %d\n", MAXE );
               return -1;
            }
            E = (int) input;
            break;
         case 'f':
            fflag = 1;
            break;
         case 'h':
            print_usage();
            return 0;
         case '?':
            PRINTERR( "encountered unexpected argument: '-%c' (ignoring)\n", optopt );
            break;
         default:
            PRINTERR( "encountered unexpected error while parsing arguments\n" );
            print_usage();
            return -1;
      }
   }

   // parse any remaining args
   int index;
   unsigned int strln;
   for( index = optind; index < argc; index++ ) {
      // only set pathpat if it has not already been set
      if( pathpat == NULL ) {
         strln = strlen( argv[index] );
         pathpat = malloc( sizeof(char) * ( strln + 1 ) );
         if( pathpat == NULL ) {
            PRINTERR( "failed to allocate memory for erasure path string\n" );
            return -1;
         }
         if( strncpy( pathpat, argv[index], strln+1 ) == NULL ) {
            PRINTERR( "failed to copy pattern string into buffer\n" );
            return -1;
         }
      }
      else {
         // any additional args are in error
         PRINTERR( "received unexpected argument: \"%s\" (ignoring)\n", argv[index] );
      }
   }

   // check for required args
   if( pathpat == NULL ) {
      PRINTERR( "missing required argument: <erasure_path_fmt>\n" );
      return -1;
   }
   if( !offsetP ) {
      PRINTERR( "a valid offset range must be specified via the '-o' option\n" );
      return -1;
   } 
   if( distrib == 0 ) {
      PRINTERR( "a valid corruption distribution pattern must be specified via '-d'\n" );
      return -1;
   }
   if( E == 0 ) {
      PRINTERR( "E==0 for this erasure stripe implies nothing to be done\n" );
      return -1;
   }
   if( N < 0  ||  E < 0  ||  bsz <= 0 ) {
      PRINTERR( "missing required \"-n\", \"-e\", or \"-b\" argument(s)\n" );
      return -1;
   }

   // align the starting and ending offsets with the block-size
   shred_range[0] -= (shred_range[0] % bsz);
   if( shred_range[1] != 0 )
      shred_range[1] += (bsz - (shred_range[1] % bsz) - 1);

   // print info for this run
   PRINTOUT( "using path = %s, n = %d, e = %d, bsz = %lu, distrib = %d, low_off = %llu, high_off = %llu, force = %d\n", 
              pathpat,N,E,bsz,distrib,shred_range[0],shred_range[1],fflag);

   // prompt for confirmation
   if( !fflag ) {
      printf( "Are you sure you wish to perform this operation? (y/n): " );
      fflush( stdout );
      char uin;
      char prev;
      index = 0;
      while( (uin = fgetc(stdin)) != '\n' ) { prev = uin; index++; }
      if( index != 1  ||  prev != 'y' ) {
         PRINTOUT( "did not recieve 'y' confirmation: terminating early\n" );
         free( pathpat );
         return -1;
      }
   }

   // allocate space for both the pattern string and extra for all possible indexes
   char* bfile = malloc( sizeof(char) * ( strln + log10( MAXPARTS ) ) );
   if( bfile == NULL ) {
      PRINTERR( "failed to allocate space for the name of each block-file\n" );
      return -1;
   }

   // use /dev/urandom as the source of random bits
   int rand_fd = open( "/dev/urandom", O_RDONLY );
   if( rand_fd < 0 ) {
      PRINTERR( "failed to open device \"/dev/urandom\"" );
      return -1;
   }
   // initialize a random number seed
   srand(time(NULL));


   int FDArray[ MAXPARTS ];                   // array of file-descriptors for files to be corrupted
   char randbuf[ bsz ];                       // buffer to be filled with random corruption bits
   int stripewidth = N+E;                     // the total data/erasure stripe width
   char backup_array[ MAXPARTS ] = {0};       // indicates whether a given block-file has been backed-up yet
   unsigned long stripecnt = 0;               // the total number of stripes currently processed
   unsigned long long coff = shred_range[0];  // the current offset being dealt with in all block-files
   unsigned long long totsz = 0;              // the total size of each data/erasure part (set properly later)
   unsigned long long limit = shred_range[1]; // the upper offset limit for corruption insertions
   char eflag = 0;                            // error flag
   
   // perform some initialization
   for( index = 0; index < MAXPARTS; index++ ) {
      FDArray[ index ] = -1;
   }
   if( limit == 0 )
      limit = bsz + coff;

   // fill in corruption between the designated offsets
   for( coff = shred_range[0]; coff < limit  &&  eflag == 0; coff += bsz ) {
      int corrupted;
      int block;
      // TODO: this could be determined randomly
      // TODO: it would be nice to have a 'min-protection' setting taken into account here
      int to_corrupt = E;
      char cparts[ MAXPARTS ] = {0};
      PRINTOUT( "corrupting stripe %lu at file offset %llu with %d randomized blocks\n", stripecnt, coff, to_corrupt );
      for( corrupted = 0; corrupted < to_corrupt; corrupted++ ) {
         // fill the buffer with random bits
         if( read( rand_fd, randbuf, bsz ) != bsz ) {
            PRINTERR( "failed to fill random buffer from /dev/urandom\n" );
            eflag = 1;
            break;
         }

         // find a new part to corrupt, depending on the distribution
         if( distrib == 1 ) {
            while( cparts[(block = rand_under( stripewidth ))] == 1 ) {
               ;
            }
         }
         else if( distrib == 2 ) {
            block = ( corrupted + stripecnt ) % stripewidth;
         }
         else if( distrib == 3 ) {
            block = (stripewidth - (stripecnt % stripewidth)) - corrupted - 1;
            if( block < 0 )
               block += stripewidth;
         }
         else {
            PRINTERR( "encountered an unexpected distribution value (%d)\n", distrib );
            eflag = 1;
            break;
         }

         // ensure that a backup of this file exists
         if( backup_array[ block ] == 0 ) {
            // generate a string for the file name
            sprintf( bfile, pathpat, block );

            // backup the original file
            off_t fsize = backup_file( bfile );
            backup_array[ block ] = 1;

            if( fsize == 0 ) {
               PRINTERR( "encountered an unexpected error while attempting to backup file \"%s\"\n", bfile );
               eflag = 1;
               break;
            }
            else if( stripecnt == 0  &&  corrupted == 0 ) {
               // set totsz to an actually appropriate value
               totsz = fsize;
               if( shred_range[1] == 0 )
                  limit = totsz;
            }
            else if ( totsz != fsize ) {
               PRINTERR( "encountered file \"%s\" with unexpected size %zd (expected %llu)\n", bfile, fsize, totsz );
               eflag = 1;
               break;
            }

            PRINTOUT( "opening file \"%s\" for write\n", bfile );

            // if we've never dealt with this file before, we'll need to open it as well
            FDArray[ block ] = open( bfile, O_WRONLY );
            if( FDArray[ block ] < 0 ) {
               PRINTERR( "failed to open block file \"%s\"\n", bfile );
               eflag = 1;
               break;
            }
         }

         PRINTOUT( "corrupting file with index %d\n", block );

         // corrupt the appropriate block
         if( corrupt_block( FDArray, randbuf, bsz, coff, block ) ) {
            PRINTERR( "detected a failure to properly corrupt part %d\n", block );
            eflag = 1;
            break;
         }
         // indicate that this part is corrupted
         cparts[ block ] = 1;
      }
      stripecnt++;
   }
    
   // cleanup
   close( rand_fd );
   free( pathpat );
   free( bfile );

   // close any opened file descriptors
   for( index = 0; index < MAXPARTS; index++ ) {
      if( FDArray[ index ] >= 0  &&  close( FDArray[ index ] ) != 0 ) {
         PRINTERR( "failed to close the FD for index %d (value = %d)\n", index, FDArray[ index ] );
         eflag = 1;
      }
   }

   // notify if an error occured
   if( eflag ) {
      PRINTERR( "an error was encountered and the erasure stripe may not have been properly or fully corrupted\n" );
      return -1;
   }

   return 0;
}

