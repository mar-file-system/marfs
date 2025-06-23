/**
 * Copyright 2015. Triad National Security, LLC. All rights reserved.
 *
 * Full details and licensing terms can be found in the License file in the main development branch
 * of the repository.
 *
 * MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier: LA-CC-15-039.
 */

#include "thread_queue/thread_queue.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>

#define NUM_CONS 0
#define NUM_PROD 5
#define QDEPTH 100
#define TOT_WRK 10
#define HLT_AT -1
#define ABT_AT -1
#define SLP_PER_CONS 200000 // 0.2 sec
#define SLP_PER_PROD 100000 // 0.1 sec

typedef struct global_state_struct
{
   pthread_mutex_t lock;
   int pkgcnt;
} * GlobalState;

typedef struct thread_state_struct
{
   unsigned int tID;
   GlobalState gstate;
   int wkcnt;
} * ThreadState;

typedef struct work_package_struct
{
   int pkgnum;
} * WorkPkg;

int my_thread_init(unsigned int tID, void *global_state, void **state)
{
   *state = malloc(sizeof(struct thread_state_struct));
   if (*state == NULL)
   {
      return -1;
   }
   ThreadState tstate = ((ThreadState)*state);

   tstate->tID = tID;
   tstate->gstate = (GlobalState)global_state;
   tstate->wkcnt = 0;
   return 0;
}

int my_consumer(void **state, void **work)
{
   WorkPkg wpkg = ((WorkPkg)*work);
   ThreadState tstate = ((ThreadState)*state);

   //tstate->wkcnt++;
   fprintf(stdout, "Thread %u received work package %d ( wkcnt = %d)\n", tstate->tID, wpkg->pkgnum, tstate->wkcnt);
   int num = wpkg->pkgnum;
   free(wpkg);
   usleep(rand() % (SLP_PER_CONS));
   // pause the queue, if necessary
   if (num == HLT_AT)
   {
      fprintf(stdout, "Thread %u is pausing the queue!\n", tstate->tID);
      return 2;
   }
   // issue an abort, if necessary
   else if (num == ABT_AT)
   {
      fprintf(stdout, "Thread %u is aborting the queue!\n", tstate->tID);
      return -1;
   }
   return 0;
}

int my_producer(void **state, void **work)
{
   ThreadState tstate = ((ThreadState)*state);

   WorkPkg wpkg = malloc(sizeof(struct work_package_struct));
   if (wpkg == NULL)
   {
      fprintf(stdout, "Thread %u failed to allocate space for a new work package!\n", tstate->tID);
      return -1;
   }
   usleep(rand() % (SLP_PER_PROD));
   if (pthread_mutex_lock(&(tstate->gstate->lock)))
   {
      fprintf(stdout, "Thread %u failed to acquire global state lock\n", tstate->tID);
      free(wpkg);
      return -1;
   }
   wpkg->pkgnum = tstate->gstate->pkgcnt;
   tstate->gstate->pkgcnt++;
   tstate->wkcnt++;
   fprintf(stdout, "Thread %u created work package %d ( wkcnt = %d )\n", tstate->tID, wpkg->pkgnum, tstate->wkcnt);
   *work = (void *)wpkg;
   pthread_mutex_unlock(&(tstate->gstate->lock));
   if (wpkg->pkgnum >= TOT_WRK)
   {
      return 1;
   }
   return 0;
}

int my_pause(void **state, void **prev_work)
{
   return 0;
}

int my_resume(void **state, void **prev_work)
{
   return 0;
}

void my_thread_term(void **state, void **prev_work, TQ_Control_Flags flg)
{
   WorkPkg wpkg = ((WorkPkg)*prev_work);
   ThreadState tstate = ((ThreadState)*state);
   if (wpkg != NULL)
   {
      fprintf(stdout, "Thread %u is freeing unused work package %d\n", tstate->tID, wpkg->pkgnum);
      free(wpkg);
      *prev_work = NULL;
   }
   return;
}

int main(int argc, char **argv)
{
   srand(time(NULL));
   struct global_state_struct gstruct;
   if (pthread_mutex_init(&(gstruct.lock), NULL))
   {
      return -1;
   }
   gstruct.pkgcnt = 0;

   TQ_Init_Opts tqopts;
   tqopts.log_prefix = "MyTQ";
   tqopts.init_flags = TQ_HALT;
   tqopts.global_state = (void *)&gstruct;
   tqopts.num_threads = NUM_PROD + NUM_CONS;
   tqopts.num_prod_threads = NUM_PROD;
   tqopts.max_qdepth = QDEPTH;
   tqopts.thread_init_func = my_thread_init;
   tqopts.thread_consumer_func = my_consumer;
   tqopts.thread_producer_func = my_producer;
   tqopts.thread_pause_func = my_pause;
   tqopts.thread_resume_func = my_resume;
   tqopts.thread_term_func = my_thread_term;

   printf("Initializing ThreadQueue...\n");
   ThreadQueue tq = tq_init(&tqopts);
   if (tq == NULL)
   {
      printf("tq_init() failed!  Terminating...\n");
      return -1;
   }

   printf("checking if queue is finished...\n");
   TQ_Control_Flags flags = 0;
   WorkPkg wpkg = NULL;
   int count = 0;
   while (!(flags & TQ_FINISHED) && !(flags & TQ_ABORT))
   {
      tq_get_flags(tq, &flags);
      if (flags & TQ_HALT)
      {
         printf("queue has halted!  Waiting for all threads to pause...\n");
         if (tq_wait_for_pause(tq))
         {
            printf("unexpected return from tq_wait_for_pause!\n");
         }
         printf("...sleeping for 1 second (should see no activity)...\n");
         sleep(1);
         printf("...resuming queue...\n");
         if (tq_unset_flags(tq, TQ_HALT))
         {
            printf("unexpected return from tq_unset_flags!\n");
         }
      }
      if (tq_dequeue(tq, TQ_ABORT, (void **)&wpkg) > 0)
      {
         printf("Received work package %d from queue\n", wpkg->pkgnum);
         free(wpkg);
         count++;
      }
      else
      {
         printf("Queue empty. No package received\n");
      }
      usleep(rand() % (SLP_PER_PROD));
   }
   if (flags & TQ_FINISHED)
   {
      printf("queue is finished!\n");
   }
   else if (flags & TQ_ABORT)
   {
      printf("queue has aborted!\n");
   }

   int tnum = 0;
   int tres = 0;
   ThreadState tstate = NULL;
   while ((tres = tq_next_thread_status(tq, (void **)&tstate)) > 0)
   {
      if (tstate != NULL)
      {
         printf("State for thread %d = { tID=%d, wkcnt=%d }\n", tnum, tstate->tID, tstate->wkcnt);
         free(tstate);
         tnum++;
      }
      else
      {
         printf("Received NULL status for thread %d\n", tnum);
      }
   }
   if (tres != 0)
   {
      printf("Failure of tq_next_thread_status()!\n");
   }

   printf("Global state: %d\n", gstruct.pkgcnt);

   printf("Finally, closing thread queue. %u packages dequeued already...\n", count);
   int cres = tq_close(tq);
   count = 0;
   if (cres > 0)
   {
      printf("Elements still remain on ABORTED queue!  Using tq_dequeue() to retrieve...\n");
      while (tq_dequeue(tq, TQ_ABORT, (void **)&wpkg) > 0)
      {
         printf("Received work package %d from aborted queue\n", wpkg->pkgnum);
         free(wpkg);
         count++;
      }
      cres = tq_close(tq);
   }
   printf("Dequeued %d items from aborted queue\n", count);

   if (cres)
   {
      printf("Received unexpected return from tq_close() %d\n", cres);
   }

   printf("Done\n");
   return 0;
}
