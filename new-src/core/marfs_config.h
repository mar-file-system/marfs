#ifndef MARFS_CONFIG_H_INCLUDE
#define MARFS_CONFIG_H_INCLUDE


typedef struct marfs_repo_struct {
   // MarFS Info
   char*          name;
   size_t         min_pack_file_size;
   size_t         max_pack_file_size;
   unsigned int   min_pack_file_count;
   unsigned int   max_pack_file_count;
   size_t         chunk_size;
   size_t         chunk_at_size;

   // Data Info
   int            data_pods;
   int            data_caps;
   int            data_scatters;
   int            data_N;
   int            data_E;
   unsigned int   data_Blksz;
   void*          DAL_ctxt;

   // Metadata Info
   void*          MDAL_ctxt;

#endif

