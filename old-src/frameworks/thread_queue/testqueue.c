
#include "thread_queue.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

typedef struct thread_state_struct {
	unsigned int tID;
	int gstate;
	int wkcnt;
}* ThreadState;

typedef struct work_package_struct {
	int pkgnum;
}* WorkPkg;


int my_thread_init( unsigned int tID, void* global_state, void** state ) {
	*((ThreadState*) state) = malloc( sizeof( struct thread_state_struct ) );
	ThreadState tstate = *((ThreadState*) state);
	if ( tstate == NULL ) { return -1; }

	tstate->tID = tID;
	tstate->gstate = *( (int*) global_state );
	tstate->wkcnt = 0;
	return 0;
}


int my_thread_work( void** state, void* work ) {
	WorkPkg wpkg = (WorkPkg) work;
	ThreadState tstate = *((ThreadState*) state);

	tstate->wkcnt++;
	fprintf( stdout, "Thread %u received work package %d (gstate = %d, wkcnt = %d)\n", tstate->tID, wpkg->pkgnum, tstate->gstate, tstate->wkcnt );
	int num = wpkg->pkgnum;
	free( wpkg );
	// pause the queue after completing work package 30
	if ( num == 30 ) { fprintf( stdout, "Thread %u is pausing the queue!\n", tstate->tID ); return 1; }
	// issue an abort after completing work package 50
	else if (  num == 50 ) { fprintf( stdout, "Thread %u is aborting the queue!\n", tstate->tID ); return -1; }
	return 0;
}


void my_thread_term( void** state ) {
	return;
}



int main( int argc, char** argv ) {
	int gstate = 124;

	TQ_Init_Opts tqopts;
	tqopts.num_threads = 10;
	tqopts.max_qdepth = 5;
	tqopts.global_state = &gstate;
	tqopts.thread_init_func = my_thread_init;
	tqopts.thread_work_func = my_thread_work;
	tqopts.thread_term_func = my_thread_term;

	printf( "Initializing ThreadQueue...\n" );
	ThreadQueue tq = tq_init( &tqopts );
	if ( tq == NULL ) { printf( "tq_init() failed!  Terminating...\n" ); return -1; }

	int pkgnum;
	for ( pkgnum = 0; pkgnum < 75; pkgnum++ ) {
		printf( "Enqueuing work package %d...\n", pkgnum );
		WorkPkg wpkg = malloc( sizeof( struct work_package_struct ) );
		if ( wpkg == NULL ) {
			printf( "Failed to allocate struct for work package %d!\n", pkgnum );
			break;
		}
		wpkg->pkgnum = pkgnum;
		if ( tq_enqueue( tq, (void*) wpkg ) ) {
			printf( "Failed to enqueue new work!\n" );
			if ( tq_halt_set( tq ) ) {
				free( wpkg );
				pkgnum--;
				printf( "It appears that a worker thread paused the queue.  Sleeping for 3 seconds...\n" );
				sleep( 3 );
				printf( "Resuming queue...\n" );
				tq_resume( tq );
			}
			else {
				if( tq_abort_set( tq ) ) {
					printf( "It appears that a worker thread issued a queue abort.  Terminating early...\n" );
				}
				else { printf( "An unknown error prevented the enqueuing of work.  Terminating early...\n" ); }
				free(wpkg);
				break;
			}
		}
		if ( pkgnum == 25 ) {
			printf( "Halting queue...\n" );
			tq_halt( tq );
			printf( "Sleeping for 3 seconds, output should pause here...\n" );
			sleep( 3 );
			printf( "Resuming queue...\n" );
			tq_resume( tq );
		}
	}
	printf( "Signaling work is done...\n" );
	tq_work_done( tq );
	
	int tnum = 0;
	int tres = 0;
	ThreadState tstate = NULL;
	while ( (tres = tq_next_thread_status( tq, (void**)&tstate )) > 0 ) {
		if ( tstate != NULL ) {
			printf( "State for thread %d = { tID=%d, gstate=%d, wkcnt=%d }\n", tnum, tstate->tID, tstate->gstate, tstate->wkcnt );
			free( tstate );
			tnum++;
		}
		else {
			printf( "Received NULL status for thread %d\n", tnum );
		}
	}
	if ( tres != 0 ) { printf( "Failure of tq_next_thread_status()!\n" ); }

	printf( "Finally, closing thread queue...\n" );
	WorkPkg wpkg = NULL;
	while ( (tres=tq_close( tq, (void**)&wpkg )) > 0 ) {
		printf( "Received remaining work package from ABORTED queue!\n" );
		free( wpkg );
	}
	if ( tres != 0 ) { printf( "Failure of tq_close()!\n" ); }
	printf( "Done\n" );
	return 0;
}





