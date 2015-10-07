// Experimenting with a test implementation of faster locking, using
// compare-and-swap (CAS), instead of semaphores.
//
// The idea is that stream_put() (called from marfs_write()), and the PUT
// thread, need to synchronize many times per second as data is being moved
// from the write-caller to curl.
//
// I now suspect that in many cases, these locks are more expensive to
// acquire than the time needed until the releasing process is ready to
// release them.  Hence, we might gain some performance by using a
// lighter-weight locking scheme.
//
// However, if the lighter-weight locking requires spinning the CPU, that's
// going to impede the performance of concurrent writers in the same
// address-space.  Therefore, instead of spinning, I'm thinking a
// sched_yield() inside the loop might work well.
//
// This code is just to work out the mechanics.  Then I'll port it to
// object_stream.c and see if it helps.
//
// RESULT:
//
// This seems to be working well.  The spinlocks are indeed ~10x more-efficient
// than semaphores.  However, by iterating in sched_yield() before falling into the semaphore,
// we can avoid most of the
//
// --- output [using spinlocks]
//     
//     *** NOTE: context-switches ~2000/sec during this test
//     *** NOTE: prod/cons are so lightweight that we avoid much spinning
//
//     starting threads
//     sleeping for 10 sec ...
//     stopping threads
//     
//     locks/sec
//       prod    1442109.12
//       cons    1442109.12
//     total 2884218
//
// --- output [using semaphores]
//     
//     *** NOTE: context-switches ~500k/sec, during this test
//     
//     locks/sec
//       prod     133478.70
//       cons     133478.70
//     total 266957
//
// --- output [using PSL]
//     
//     *** NOTE: context-switches at ~200/sec, during this test
//     *** NOTE: prod/cons are so lightweight that we avoid much overhead
//     
//     locks/sec
//       prod    2125945.50
//       cons    2125945.50
//     total 4251890



#include <stdint.h>
#include <stdlib.h>             // srand(), rand()
#include <unistd.h>             // sleep()
#include <pthread.h>
#include <signal.h>             // pthread_kill()
#include <semaphore.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <assert.h>


#include "spinlock.h"
#include <signal.h>
#include <time.h>


// lifted from MarFS object_stream.c
int timed_sem_wait(sem_t* sem, size_t timeout_sec) {

   struct timespec timeout;
   if (clock_gettime(CLOCK_REALTIME, &timeout))
      return -1;

   // timeout.tv_sec += os->timeout_sec; // TBD
   timeout.tv_sec += timeout_sec;

   // wait for a little while on the semaphore
   while (1) {
      if (! sem_timedwait(sem, &timeout))
         return 0;              // got it

      if (errno == EINTR) {
         fprintf(stderr, "interrupted.  resuming wait ...\n");
         continue;              // interrupted (try again?)
      }

      if (errno == ETIMEDOUT)
         return -1;             // timed-out

      return -1;                // something else went wrong
   }
}









struct ThreadInfo {

   // testing maximal rate of spinlocks/sec
   spin_lock_t lock1;
   spin_lock_t lock2;

   // testing maximal rate of semaphores/sec
   sem_t    sem1;
   sem_t    sem2;

   // testing PSL
   struct PoliteSpinLock empty;
   struct PoliteSpinLock full;

   // common
   volatile uint8_t  go;
   size_t            prod_ticks;
   size_t            cons_ticks;
};



// producer/consumer
void* prod(void* arg) {

   struct ThreadInfo* tinfo = (struct ThreadInfo*) arg;

   while (tinfo->go) {
#if 0
      // using spinlocks
      spin_lock(&tinfo->lock1);
      tinfo->prod_ticks +=1;
      spin_unlock(&tinfo->lock2);
#elif 0
      // using semaphores
      sem_wait(&tinfo->sem1);
      tinfo->prod_ticks +=1;
      sem_post(&tinfo->sem2);
#else
      // using PoliteSpinLocks
      PSL_wait(&tinfo->empty);
      tinfo->prod_ticks +=1;
      PSL_post(&tinfo->full);
#endif
   }
}

void* cons(void* arg) {

   struct ThreadInfo* tinfo = (struct ThreadInfo*) arg;

   while (tinfo->go) {
#if 0
      // using spinlocks
      spin_lock(&tinfo->lock2);
      tinfo->cons_ticks +=1;
      spin_unlock(&tinfo->lock1);
#elif 0
      // using semaphores
      sem_wait(&tinfo->sem2);
      tinfo->cons_ticks +=1;
      sem_post(&tinfo->sem1);
#else
      // using PoliteSpinLocks
      PSL_wait(&tinfo->full);
      tinfo->cons_ticks +=1;
      PSL_post(&tinfo->empty);
#endif
   }
}



int main(int argc, char* argv[]) {
   struct ThreadInfo tinfo;
   pthread_t  t1;
   pthread_t  t2;

   srand(0);


   // tinfo.thread_name = ;

   tinfo.lock1 = 0;
   tinfo.lock2 = 1;             // consumer initially waits

   sem_init(&tinfo.sem1, 0, 0);
   sem_init(&tinfo.sem2, 0, 1); // consumer initially waits

   PSL_init(&tinfo.full, 0);
   PSL_init(&tinfo.empty, 1); // consumer initially waits

   tinfo.go = 1;
   tinfo.prod_ticks = 0;
   tinfo.cons_ticks = 0;

   printf("starting threads\n");
   if (pthread_create(&t1, NULL, prod, (void*)&tinfo)) {
      fprintf(stderr, "thread1 start failed\n");
      exit(1);
   }
   if (pthread_create(&t2, NULL, cons, (void*)&tinfo)) {
      fprintf(stderr, "thread2 start failed\n");
      exit(1);
   }

   const unsigned int sleep_sec = 10;
   printf("sleeping for %d sec ...\n", sleep_sec);
   sleep(sleep_sec);            // let threads run ...


   printf("stopping threads\n");
   tinfo.go = 0;
   if (pthread_join(t1, NULL)) {
      fprintf(stderr, "thread1 join failed\n");
      fflush(stderr);
      exit(1);
   }
   if (pthread_join(t2, NULL)) {
      fprintf(stderr, "thread2 join failed\n");
      fflush(stderr);
      exit(1);
   }

   printf("\n");
   printf("locks/sec\n");
   printf("  prod  %12.2f\n", (float)tinfo.prod_ticks / sleep_sec);
   printf("  cons  %12.2f\n", (float)tinfo.cons_ticks / sleep_sec);
   printf("total %d\n", (tinfo.prod_ticks + tinfo.cons_ticks) / sleep_sec);

   return 0;
}
