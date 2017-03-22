#include <mpi.h>

#include "common.h"
#include "marfs_configuration.h"
#include "marfs_base.h" // MARFS_MAX_HOST_SIZE

int main(int argc, char** argv) {

   // load the marfs config.
   if(read_configuration()) {
      fprintf(stderr, "failed to read marfs configuration\n");
      return -1;
   }
   if(validate_configuration()) {
      fprintf(stderr, "failed to validate marfs configuration\n");
      return -1;
   }

   char* repo_name = "mc10+2";
   MarFS_Repo *repo = find_repo_by_name(repo_name);

   if( !repo ) {
      fprintf( stderr, "drebuilder: failed to retrieve repo by name \"%s\", check your config file\n", repo_name );
      return -1;
   }

   DAL        *dal    = repo->dal;
   MC_Config  *config = (MC_Config*)dal->global_state;

   // Initialize the MPI environment
   MPI_Init(NULL, NULL);

   // Get the number of processes
   int world_size;
   MPI_Comm_size(MPI_COMM_WORLD, &world_size);

   // Get the rank of the process
   int world_rank;
   MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

   // Get the name of the processor
   char processor_name[MPI_MAX_PROCESSOR_NAME];
   int name_len;
   MPI_Get_processor_name(processor_name, &name_len);

   int low = (config->scatter_width * world_rank / world_size);
   int high = (config->scatter_width * (world_rank + 1) / world_size) - 1;

   struct timespec tspec;
   tspec.tv_nsec = 0;
   tspec.tv_sec = world_rank;
   nanosleep( &tspec, NULL );
   // Print off a message
   printf("processor %s, rank %d"
         " out of %d processes  Repo = %s  scatter_width= %d  Range[%d,%d]\n",
         processor_name, world_rank, world_size, repo->name, config->scatter_width, low, high);

   // Finalize the MPI environment.
   MPI_Finalize();
}

