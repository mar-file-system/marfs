// The following is necessary to get timer_t defined in timer.h, if compiling with -std=c99
// see http://stackoverflow.com/questions/3875197/linux-gcc-with-std-c99-complains-about-not-knowing-struct-timespec
#if __STDC_VERSION__ >= 199901L
#define _XOPEN_SOURCE 600
#else
#define _XOPEN_SOURCE 500
#endif /* __STDC_VERSION__ */

#include "spinlock.h"

#include <signal.h>
#include <sched.h>
#include <time.h>





void spin_unlock(spin_lock_t* lock) {
   *lock = 0;
}






// not thread-safe
void PSL_init(struct PoliteSpinLock* psl, unsigned int value) {
   psl->master = 0;
   psl->post_count = value;
}

void PSL_wait(struct PoliteSpinLock* psl) {
   spin_lock(&psl->master);

   while (! psl->post_count) {
      spin_unlock(&psl->master);
      sched_yield();
      spin_lock(&psl->master);
   }

   --psl->post_count;
   spin_unlock(&psl->master);
}

// try to acquire PSL until timeout_sec have elapsed.
// return 0 if we got the lock
// return -1 if we didn't.
int PSL_wait_with_timeout(struct PoliteSpinLock* psl, size_t timeout_sec) {
   int retval = 0;
#if 0
   struct timespec timeout;
   struct timespec now;
   if (clock_gettime(CLOCK_REALTIME, &timeout))
      return -1;

   now = timeout;
   timeout.tv_sec += timeout_sec;

   spin_lock(&psl->master);
   while (! psl->post_count && (now.tv_sec < timeout.tv_sec)) {
      spin_unlock(&psl->master);
      sched_yield();
      clock_gettime(CLOCK_REALTIME, &now);
      spin_lock(&psl->master);
   }
   if (! psl->post_count)
      retval = -1;
   else {
      --psl->post_count;
      spin_unlock(&psl->master);
   }

#else
   // create timer
   timer_t         timer_id;       // see timer_create()
   struct sigevent evp;
   evp.sigev_notify          = SIGEV_NONE; // we'll poll
   if (timer_create(CLOCK_MONOTONIC, &evp, &timer_id)) {
      // fprintf(stderr, "timer_create() failed: %s\n", strerror(errno));
      // exit(1);
      return -1;
   }

   // set timer (no async notification)
   struct itimerspec its;
   its.it_value.tv_sec     = timeout_sec;
   its.it_value.tv_nsec    = 0;
   its.it_interval.tv_sec  = 0; // just expire once
   its.it_interval.tv_nsec = 0;
   if (timer_settime(timer_id, 0, &its, NULL)) {
      // fprintf(stderr, "timer_settimer() failed: %s\n", strerror(errno));
      // exit(1);
      timer_delete(timer_id);
      return -1;
   }

   struct itimerspec cur;
   timer_gettime(timer_id, &cur);

   spin_lock(&psl->master);
   while (! psl->post_count && cur.it_value.tv_nsec) {
      spin_unlock(&psl->master);
      sched_yield();
      timer_gettime(timer_id, &cur);
      spin_lock(&psl->master);
   }
   if (! psl->post_count)
      retval = -1;
   else {
      --psl->post_count;
      spin_unlock(&psl->master);
   }
   timer_delete(timer_id);
#endif

   return retval;
}

void PSL_post(struct PoliteSpinLock* psl) {
   spin_lock(&psl->master);
   ++ psl->post_count;
   spin_unlock(&psl->master);
}

