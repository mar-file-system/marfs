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


int main(int argc, char* argv[]) {

   // test: will we time-out on a lock that is never freed?
   struct PoliteSpinLock psl;
   PSL_init(&psl, 0);

   printf("waiting for stuck-lock with timeoput in 3 sec ...\n");
   int rc = PSL_wait_with_timeout(&psl, 3);

   printf("done.  returned %d (%s)\n", rc, strerror(errno));
}

