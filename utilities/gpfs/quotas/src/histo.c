/*
 * This file is part of MarFS, which is released under the BSD license.
 *
 *
 * Copyright (c) 2015, Los Alamos National Security (LANS), LLC
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * 3. Neither the name of the copyright holder nor the names of its contributors
 * may be used to endorse or promote products derived from this software without
 * specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
 * OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * -----
 *  NOTE:
 *  -----
 *  MarFS uses libaws4c for Amazon S3 object communication. The original version
 *  is at https://aws.amazon.com/code/Amazon-S3/2601 and under the LGPL license.
 *  LANS, LLC added functionality to the original work. The original work plus
 *  LANS, LLC contributions is found at https://github.com/jti-lanl/aws4c.
 *
 *  GNU licenses can be found at <http://www.gnu.org/licenses/>.
 *
 *
 *  From Los Alamos National Security, LLC:
 *  LA-CC-15-039
 *
 *  Copyright (c) 2015, Los Alamos National Security, LLC All rights reserved.
 *  Copyright 2015. Los Alamos National Security, LLC. This software was produced
 *  under U.S. Government contract DE-AC52-06NA25396 for Los Alamos National
 *  Laboratory (LANL), which is operated by Los Alamos National Security, LLC for
 *  the U.S. Department of Energy. The U.S. Government has rights to use,
 *  reproduce, and distribute this software.  NEITHER THE GOVERNMENT NOR LOS
 *  ALAMOS NATIONAL SECURITY, LLC MAKES ANY WARRANTY, EXPRESS OR IMPLIED, OR
 *  ASSUMES ANY LIABILITY FOR THE USE OF THIS SOFTWARE.  If software is
 *  modified to produce derivative works, such modified software should be
 *  clearly marked, so as not to confuse it with the version available from
 *  LANL.
 *
 *  THIS SOFTWARE IS PROVIDED BY LOS ALAMOS NATIONAL SECURITY, LLC AND CONTRIBUTORS
 *  "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 *  THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *  ARE DISCLAIMED. IN NO EVENT SHALL LOS ALAMOS NATIONAL SECURITY, LLC OR
 *  CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 *  EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
 *  OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
 *  IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 *  OF SUCH DAMAGE.
 *  */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <math.h>
#include "histo.h"
/******************************************************************************
Name:  print_histo

This function prints the histogram to a file handle specified by the input
argument.  It prints two types of histograms:  log base 2 and non-log.

******************************************************************************/


void print_histo(size_t increment, size_t max_bucket, int *count_value, enum histo_type type, FILE *file_fd)
{
   int i;
   int iter_limit;
   // determine what type of histogram to print
   // if base 2 then set up buckets in power of 2 starting at 0
   if (type == BASE_2) {
      // Determine number of buckets
      iter_limit = log10(max_bucket)/log10(2);
      for (i = 0; i< iter_limit; i++) {
         if (i==0)
            fprintf(file_fd,"bucket %d,%d,%d\n", i, (int)pow(2,i+1), count_value[i]);
         else
            fprintf(file_fd,"bucket %d,%d,%d\n", (int)pow(2,i), (int)pow(2,i+1), count_value[i]);
      }
   }
   // This is the non-log case so buckets start and and are increment in size
   else if (type == NON_LOG) {
      for (i = 0; i <max_bucket; i+=increment) {
         if (i==0)
            fprintf(file_fd,"bucket %d,%d,%d\n", i, increment, count_value[i]);
         else
            fprintf(file_fd,"bucket %d,%d,%d\n", i, i+increment, count_value[i/increment]);
      }
   }
}
/******************************************************************************
Name:  fill_histogram

This function takes the value passed into and determines what bucket needs to 
be incremented.  Current histograms supported are log base 2 and non-log 
that is based on two values passed in (max_bucket, and increment).  The 
count_value array corresponds to each of the buckets and keeps track of the 
counts for print purposes.
Example of base 2 histogram:

0,2,count
2,4,count
4,8,count

Example of non log histogram (increment = 1024 and max_bucket = 3072)
0,1024,count
1024,2048,count
2048,3072,count

Here is an example of how you would fill and print the histogram:
this is determining age in days (atime) for a file - the buckets are days

  int count_value[MAX_BUCKET_COUNT];
  time_t now=time(0);    // get epoch time

  FILE *outfd = fopen("histo.out", "w");
 
  // determine atime age in days
  age = (long int)floor((now - atime_for_file)/(60*60*24)); 
                                            
  // call fill histogram
  fill_histogram(age, 0, 512, &count_value[0], BASE_2);
  .
  .
  continue to fill
  . 
  done
  // call print histogram
  // histogram is written to 
  print_histo(0, 512, &count_value[0], BASE_2, outfd);
  . 
  .
 

******************************************************************************/
void fill_histogram(size_t value, size_t increment, size_t max_bucket, int *count_value, enum histo_type type)
{
   int j;
   int max_iter;
   // log base 2 histogram, use max bucket to determine how many buckets will created
   if (type == BASE_2) {
      max_iter = log10(max_bucket)/log10(2);

      // Determine which bucket a value belongs to and increment count 
      // in that bucket accordingly 
      for (j = 1; j<max_iter; j++) {
         if (value < pow(2,1)) {
            count_value[0]+=1;
            break;
         }
         else if (value >= pow(2,j)) {
            continue;
         }
         else {
            count_value[j-1]+=1;
            break;
         }
      }
   } 
   // Check if non-log histogram
   else if (type == NON_LOG) {
      //Each bucket is increment in size so iterate through buckets to
      //find corresponding bucket for value
      for (j = 0; j < max_bucket; j+=increment) {
         if (value < increment) {
            count_value[0]+=1;
            break;
         }
         else if ( value >= j ) {
            continue;
         }
         else {
            count_value[(j-increment)/increment]+=1;
            break;
         }
      }
   }
}


// TEMP TEMP TEMP
// Using as a tester
int main()
{
   int i;
   int j;
   long int time_a[3];
   long int value_b[3];
   long int age;
   
   time_a[2] = 1437775307; 
   time_a[1] = 1437901307;
   time_a[0] = 1436695822;

   value_b[0] = 1048576;
   value_b[1] = 2097152;
   value_b[2] = 5242880;

   int count_value[11];
   //int log_2;
//   enum histo_type {BASE_2, NON_LOG);

   //log_2 = log10(1024)/log10(2);
   //printf("%d\n", log_2);

   memset(&count_value[0],0,sizeof(count_value));

   time_t now=time(0);


   FILE *outfd;
   outfd = fopen("./tmp_histo.out","w");

   for (i = 0; i <3; i++) {
      age = (long int)floor((now-time_a[i])/(60*60*24)); 
      printf("age:  %ld days\n", age);
      fill_histogram(age, 0, 512, &count_value[0], BASE_2);
   }
   fprintf(outfd,"LOG BASE 2 HISTOGRAM\n");
   print_histo(0, 512, &count_value[0], BASE_2, outfd);
   /***********
   for (j = 0; j< 10; j++) {
      if (j==0)
         printf("bucket %d,%d,%d\n", j, (int)pow(2,j+1), count_value[j]);
      else
         printf("bucket %d,%d,%d\n", (int)pow(2,j), (int)pow(2,j+1), count_value[j]);
   }
   ***********/
   printf("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX\n");
   memset(&count_value[0],0,sizeof(count_value));
   int increment = 1048576;
   for (i = 0; i <3; i++) {
      fill_histogram(value_b[i], 1048576, 9*1048576, &count_value[0], NON_LOG);
   }
   //print_histo(1048576, 10485760, &count_value[0], NON_LOG);
   fprintf(outfd,"NON-LOG HISTOGRAM\n");
   print_histo(1048576, 9437184, &count_value[0], NON_LOG, outfd);
   fclose(outfd);

   /**************
   for (j = 0; j <10*1048576; j+=increment) {
      if (j==0)
         printf("bucket %d,%d,%d\n", j, increment, count_value[j]);
      else
         printf("bucket %d,%d,%d\n", j, j+increment, count_value[j/increment]);
   }
   ****************/
}

