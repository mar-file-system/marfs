// log file is sorted by the thread_id field before coming to us.
// We just have to split the output into distinct files, by thread_id

#include <stdio.h>
#include <stdlib.h>
#include <string.h>


void
usage(const char* prog) {
   fprintf(stderr, "Usage: %s <thread_id_col>\n");
   fprintf(stderr, " where 'columns' are white-space separated blobs on each line\n");
   fprintf(stderr, " and whitespace is ' \\t|'\n");
}


// find n-th token (0-based).
// strtok() is destructive
// return a ptr to the tail, as well, so caller can install/remove '\0'
char*
find_tok(int tok_num, char* buf, size_t bufsize, char** tok_end) {

   const char* whitespace = " |\t";
   char*       ptr = buf + strspn(buf, whitespace);
   int         i;
   for (i=0; i<tok_num; ++i) {
      ptr += strcspn(ptr, whitespace);
      ptr += strspn(ptr, whitespace);
   }

   if (tok_end)
      *tok_end = ptr + strcspn(ptr, whitespace);

   // printf("find_tok(%ld): '%s'\n", tok_num, ptr);
   return ptr;
}

int
main(int argc, char* argv[]) {

   if (argc != 2) {
      usage(argv[0]);
      exit(EXIT_FAILURE);
   }
   int          tid_col = atoi(argv[1]);

   const size_t MAX_TID = 32;
   char         tid[MAX_TID+1];
   tid[0] = 0;

   const size_t BUFSIZE = 2048;
   char         buf[BUFSIZE+1];

   const char*  thr_fname_prefix = "foo.log.thr_";
   const size_t MAX_THR_FNAME = 128;
   char         thr_fname[MAX_THR_FNAME];

   FILE*  thr_file = 0;
   while (fgets(buf, BUFSIZE, stdin)) {

      char* tid1_end;
      char  tid1_end_char = 0;
      char* tid1 = find_tok(tid_col -1, buf, BUFSIZE, &tid1_end);
      if (! *tid1) {
         fprintf(stderr, "failed to find token %d in this line: %s\n", buf);
         exit(EXIT_FAILURE);
      }
      tid1_end_char = *tid1_end;
      *tid1_end = 0;            /* temporarily isolate token */

      // maybe change output file
      if (strcmp(tid1, tid)) {

         if (strlen(tid1) >= MAX_TID) {
            fprintf(stderr, "tid token '%s' is too big\n", tid1);
            exit(EXIT_FAILURE);
         }

         if (thr_file)
            fclose(thr_file);

         // create and open fname for new thread
         size_t count = snprintf(thr_fname, MAX_THR_FNAME, "%s%s", thr_fname_prefix, tid1);
         if (! count || (count == MAX_THR_FNAME)) {
            fprintf(stderr, "couldn't generate fname for tid '%s'\n", tid1);
            exit(EXIT_FAILURE);
         }
         printf("opening %s\n", thr_fname);
         if (! (thr_file = fopen(thr_fname, "w"))) {
            perror("fopen failed");
            exit(EXIT_FAILURE);
         }
         
         // prepare to detect next change of thread_id
         strncpy(tid, tid1, MAX_TID);
         tid[MAX_TID] = 0;
      }

      // restore original line
      *tid1_end = tid1_end_char;

      // stream lines to new thread-file
      fprintf(thr_file, "%s", buf);
   }

   printf("done\n");
}
