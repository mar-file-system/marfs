# 
# Copyright 2015. Triad National Security, LLC. All rights reserved.
# 
# Full details and licensing terms can be found in the License file in the main development branch
# of the repository.
# 
# MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier: LA-CC-15-039.
#

# automake requires '=' before '+=', even for these built-in vars
AM_CPPFLAGS =
AM_CFLAGS   =
AM_LDFLAGS  =


#if debugALL
#  AM_CFLAGS   += -g -O0
#  DEFS        += -DDEBUG_NE=1  -fstack-protector-all -fstack-check
#else
#  AM_CFLAGS   += -g -O3
#  DEFS        += -DDEBUG_NE=0
#endif


#if GPROF
#  AM_CFLAGS  += -pg
#  AM_LDFLAGS += -pg
#endif



# ---
bin_PROGRAMS = ec_parallel_benchmark ec_rdma_client

ec_parallel_benchmark_SOURCES = ec_parallel_benchmark.c

ec_rdma_client_SOURCES = ec_rdma_client.c


