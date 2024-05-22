/**********************************************************************
  Copyright(c) 2011-2018 Intel Corporation All rights reserved.

  Redistribution and use in source and binary forms, with or without
  modification, are permitted provided that the following conditions
  are met:
    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in
      the documentation and/or other materials provided with the
      distribution.
    * Neither the name of Intel Corporation nor the names of its
      contributors may be used to endorse or promote products derived
      from this software without specific prior written permission.

  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
  "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
  LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
  A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
  OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
  LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
  DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
  THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
  OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
**********************************************************************/

#define _GNU_SOURCE  //(added by gransom)

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <time.h>
#include <math.h>
#include <float.h>
#include <fcntl.h>
#include <getopt.h>
#include <pthread.h>
#include <assert.h>
#include <sys/stat.h>
#include <rdma/rdma_cma.h>
#include <rdma/rsocket.h>
#include <arpa/inet.h>
#include <limits.h>
#include "zlib.h"
#include "isa-l.h"
//#include "crc.h"		// use isa-l.h instead
//#include "erasure_code.h"	// use isa-l.h instead

/* default values */
#define K_DEFAULT 8
#define P_DEFAULT 2
#define BLK_SIZE_DEFAULT 1
#define BLK_UNIT_DEFAULT 'm'
#define DATA_SIZE_DEFAULT 10
#define DATA_UNIT_DEFAULT 'g'
#define THREAD_CNT_DEFAULT 1
#define COMP_RATIO_DEFAULT 0.25


#define MMAX 255
#define KMAX 255

#define K 1024LL
#define M 1048576LL
#define G 1073741824LL
#define T 1024LL*1024LL*1024LL*1024LL
#define P 1024LL*1024LL*1024LL*1024LL*1024LL

#define ENCODE_LOGICAL 0
#define DECODE 1
#define ENCODE_PHYSICAL 2

#define NO_COMP 		0
#define COMP_BEFORE_ENCODE 	1
#define COMP_AFTER_ENCODE 	2

#define NO_CRC 0
#define CRC_FIRST 1
#define CRC_SECOND 2
#define CRC_LAST 3

#define CRC_ZLIB 1
#define CRC_IEEE 2
#define CRC_RFC  3

#define CHUNK_SIZE 131072UL

#define TEST_SEED 0x1234

#define IOMAPSIZE 128 //max mapped rdma bufs

int numa_nodes;
int cpus_per_numa;

typedef unsigned char u8;

typedef void * (*encode_func)(void *);
typedef void * (*deceode_func)(void *);
typedef void (*crc_func)(long long, int, long long, u8 **);

typedef struct encode_thread_args_t {
	int thread_id;
	int crc_opt;
	crc_func crc_func_ptr;
	int k;
	int p;
	long long data_size_abs;
	long long blk_size_abs;
	unsigned long comp_data_size;
	u8 *comp_data;
	u8 *g_tbls;
	u8 **frag_ptrs;
	double *bws;
	double *pbws;
} encode_thread_args;

typedef struct decode_thread_args_t {
	int thread_id;
	int k;
	int p;
	int nerrs;
	long long data_size_abs;
	long long blk_size_abs;
	u8 *g_tbls;
	u8 *decode_index;
	double *bws;
} decode_thread_args;

typedef struct rdma_thread_args_t {
        int thread_id;
        int fd;
        long long blk_size_abs;
        int k;
        int p;
        u8 *g_tbls;
        int comp_opt;
        int crc_opt;
        int crc_type;
        crc_func crc_func_ptr;
} rdma_thread_args;

int usage(void)
{
	fprintf(stderr,
		"Usage: ec_simple_example [options]\n"
		"  -h        Help\n"
		"  -D	     Enable RDMA server\n"
		"  -P <val>  Port number\n"
		"  -s <val>  RDMA server hostname\n"
		"  -c <val>  Compression (0 - No Comp, 1 - Comp Before Encode, 2 - Comp After Encode)\n"
		"  -C <val>  Checksum options (0 - no CRC, 1 - CRC First, 2 - CRC between Comp/Encode, 3 - CRC at the end)\n"
		"  -T <val>  CRC types (ZLIB - zlib adler32, IEEE - intel isal crc32_ieee, RFC - intel isal crc32_gzip_refl)\n"
		"  -R <val>  Compression ratio. 0.25, 0.50, 0.75, 0\n"
		"  -k <val>  Number of source fragments\n"
		"  -p <val>  Number of parity fragments\n"
		"  -b <val>  block size, eg 1M, 4K. Unit values are K and M\n"
		"  -e <val>  Simulate erasure on frag index val. Zero based. Can be repeated.\n"
		"  -r <seed> Pick random (k, p) with seed\n"
		"  -t <val>  Number of threads\n"
		"  -N <val>  Number of numa nodes\n"
		"  -n <val>  CPUs per numa nodes\n"
		"  -d <val>  Per-thread input data size with unit, eg 10G/10M. Smallest unit is M; Largest unit is P\n");
	exit(0);
}

static int _create_server_listenfd(char *server_name, int port, int num_client)
{
        struct rdma_addrinfo addr;
        struct rdma_addrinfo *res;
        memset(&addr, 0, sizeof(addr));

        addr.ai_port_space = RDMA_PS_TCP;
        if (rdma_getaddrinfo(server_name, NULL, &addr, &res))
        {
                printf("rdma_getaddrinfo failed\n");
                return -1;
        }

        struct sockaddr* addr_ptr = (struct sockaddr *)res->ai_dst_addr;
        socklen_t addr_len = res->ai_dst_len;

        if (addr_ptr->sa_family != AF_INET)
        {
                printf("server sa_family NOT AF_INET\n");
                return -1;
        }

        struct sockaddr_in *sin_ptr = (struct sockaddr_in *)addr_ptr;
        char dot_addr[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &sin_ptr->sin_addr, dot_addr, INET_ADDRSTRLEN);
        printf("rdma_getaddrinfo: %s\n", dot_addr);

        int fd;
        int opt_val = 1;
        fd = rsocket(res->ai_family, SOCK_STREAM, 0);
        printf("rsocket fd %d\n", fd);

        if (rsetsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt_val, sizeof(int)))
        {
                printf("rsetsockopt SOL_SOCKET, SO_REUSEADDR failed\n");
                return -1;
        }

        unsigned mapsize = (unsigned)num_client;
        if (rsetsockopt(fd, SOL_RDMA, RDMA_IOMAPSIZE, &mapsize, sizeof(mapsize)))
        {
                printf("rsetsockopt RDMA_IOMAPSIZE failed\n");
                return -1;
        }
        /* Set linger to zero so server gets noticed immediately once client disconnects */
        struct linger lo = {1, 0};
        if (rsetsockopt(fd, SOL_SOCKET, SO_LINGER, &lo, sizeof(lo)))
        {
                printf("rsetsockopt SO_LINGER failed\n");
                return -1;
        }

        /* Set server port */
        ((struct sockaddr_in *)addr_ptr)->sin_port = htons((unsigned short)port);

        if (rbind(fd, (struct sockaddr *)addr_ptr, addr_len))
        {
                printf("rbind failed %m\n");
                return -1;
        }

        if (rlisten(fd, SOMAXCONN))
        {
                printf("rlisten failed\n");
                return -1;
        }

        return fd;
}

static int _gf_gen_decode_matrix_simple(u8 * encode_matrix,
				       u8 * decode_matrix,
				       u8 * invert_matrix,
				       u8 * temp_matrix,
				       u8 * decode_index,
				       u8 * frag_err_list, int nerrs, int k, int m);
static u8 * _read_comp_data(double comp_ratio, unsigned long *comp_data_size);
static double _get_comp_ratio(double comp_ratio);
static long long _get_size_abs(long long base, char unit);
static void _print_bandwidth(char mode, double *bws, int thread_cnt);
static long _compress(u8 *in, u8 *out, unsigned long blk_size, z_stream *strm, int flush, long long *total_comp_size);
static void _crc_ieee(long long stripe_cnt, int k, long long blk_size_abs, u8 **frag_ptrs);
static void _crc_rfc(long long stripe_cnt, int k, long long blk_size_abs, u8 **frag_ptrs);
static void _crc_zlib(long long stripe_cnt, int k, long long blk_size_abs, u8 **frag_ptrs);
static void _set_numa(int thread_id);
static void _rdma_benchmark(char *server_name, int port, int thread_cnt, long long blk_size_abs,
                                int k, int p, u8 *g_tbls, int comp_opt, int crc_opt, crc_func crc_func_ptr);

void * encode_data(void *args);
void * encode_data_compress_after_encode(void *args);
void * encode_data_compress_before_encode(void *args);
void * decode_data(void *args);
void * decode_data_compress(void *args);
void * server_thread(void *args);


int main(int argc, char *argv[])
{
		
	int i, m, c, e, ret;
	int k = -1;
	int p = -1;
	int blk_size = -1;
	int data_size = -1;
	int thread_cnt = -1;
	int crc_opt = -1;
	int crc_type = NO_CRC;
	int nerrs = 0;
	int comp_opt = -1;
	int rdma = 0;
	int port = 0;
	double comp_ratio = -1;
	long long blk_size_abs = 0;
	long long data_size_abs = 0;
	char blk_unit = 0, data_unit = 0;
	char server_name[256];
	u8 *comp_data = NULL;
	unsigned long comp_data_size = 0;
	double diff;
	double *bws, *pbws;
	struct timespec start, end;
	pthread_t *tid;
	encode_thread_args *encode_args;
	decode_thread_args *decode_args;
	pthread_barrier_t barrier;
	encode_func encode_func_ptr;
	crc_func crc_func_ptr;
	// Fragment buffer pointers
	u8 frag_err_list[MMAX];

	// Coefficient matrices
	u8 *encode_matrix, *decode_matrix;
	u8 *invert_matrix, *temp_matrix;
	u8 *g_tbls;
	u8 decode_index[MMAX];

	encode_func_ptr = NULL;
	crc_func_ptr = NULL;
	numa_nodes = 0;
	cpus_per_numa = 0;
	server_name[0] = 0;

	if (argc == 1)
		for (i = 0; i < p; i++)
			frag_err_list[nerrs++] = rand() % (k + p);

	while ((c = getopt(argc, argv, "Ds:C:T:c:k:N:n:P:p:b:t:d:e:r:R:h")) != -1) {
		switch (c) {
		case 'D':
			rdma = 1;
			break;
		case 's':
			strncpy(server_name, optarg, 256);
			break;
		case 'P':
			port = atoi(optarg);
			break;
		case 'N':
			numa_nodes = atoi(optarg);
			break;
		case 'n':
			cpus_per_numa = atoi(optarg);
			break;
		case 'c':
			comp_opt = atoi(optarg);
			break;
		case 'C':
			crc_opt = atoi(optarg);
			break;
		case 'R':
			comp_ratio = atof(optarg);
			comp_ratio = _get_comp_ratio(comp_ratio);
			if (comp_ratio < 0) {
				usage();
				return 1;
			}
			break;
		case 'T':
			if (!strncmp("IEEE", optarg, 4))
				crc_type = CRC_IEEE;
			else if (!strncmp("RFC", optarg, 3))
				crc_type = CRC_RFC;
			else if (!strncmp("ZLIB", optarg, 4))
				crc_type = CRC_ZLIB;
			break;
		case 'k':
			k = atoi(optarg);
			break;
		case 'p':
			p = atoi(optarg);
			break;
		case 'b':
			blk_size = atoi(optarg);
			blk_unit = tolower(optarg[strlen(optarg)-1]);
			if (blk_unit < 0) {
				usage();
				return 1;
			}
			blk_size_abs = _get_size_abs(blk_size, blk_unit);
			break;
		case 't':
			thread_cnt = atoi(optarg);
			if (thread_cnt <= 0) {
				usage();
				return 1;
			}
			break;
		case 'd':
			data_size = atoi(optarg);
			data_unit = tolower(optarg[strlen(optarg)-1]);
			if (data_size < 0) {
				usage();
				return 1;
			}
			data_size_abs = _get_size_abs(data_size, data_unit);
			break;
		case 'e':
			e = atoi(optarg);
			frag_err_list[nerrs++] = e;
			break;
		case 'r':
			srand(atoi(optarg));
			k = (rand() % (MMAX - 1)) + 1;	// Pick k {1 to MMAX - 1}
			p = (rand() % (MMAX - k)) + 1;	// Pick p {1 to MMAX - k}

			for (i = 0; i < k + p && nerrs < p; i++)
				if (rand() & 1)
					frag_err_list[nerrs++] = i;
			break;
		case 'h':
		default:
			usage();
			break;
		}
	}

	/* check for options, if not specified, print out default value */
	if (k == -1) {
		fprintf(stdout, "Number of data blocks not specified, using default value %d\n", K_DEFAULT);
		k = K_DEFAULT;
	}
	if (p == -1) {
		fprintf(stdout, "Number of parity blocks not specified, using default value %d\n", P_DEFAULT);
		p = P_DEFAULT;
	}
	if (blk_size == -1) {
		fprintf(stdout, "Block size not specified, using default value %d%cB\n", BLK_SIZE_DEFAULT, BLK_UNIT_DEFAULT);
		blk_size = BLK_SIZE_DEFAULT;
		blk_unit = BLK_UNIT_DEFAULT;
		blk_size_abs = _get_size_abs(blk_size, blk_unit);
	}
	if (data_size == -1) {
		fprintf(stdout, "Data size not specified, using default value %d%cB\n", DATA_SIZE_DEFAULT, DATA_UNIT_DEFAULT);
		data_size = DATA_SIZE_DEFAULT;
		data_unit = DATA_UNIT_DEFAULT;
		data_size_abs = _get_size_abs(data_size, data_unit);
	}
	if (thread_cnt == -1) {
		fprintf(stdout, "Thread count not specified, using default value %d\n", THREAD_CNT_DEFAULT);
		thread_cnt = THREAD_CNT_DEFAULT;
	}
	if (comp_opt == -1) {
		fprintf(stdout, "Compression option is not set, using no compression\n");
		comp_opt = NO_COMP;
	}
	if (comp_opt > NO_COMP && (comp_ratio == -1)) {
		fprintf(stdout, "Compression option used but compression ratio not used, using default compression ratio %f\n", COMP_RATIO_DEFAULT);
		comp_ratio = COMP_RATIO_DEFAULT;
	}
	if (crc_opt == -1) {
		fprintf(stdout, "CRC option not specified, using no CRC\n");
		crc_opt = NO_CRC;
	}
	if (crc_opt > NO_CRC && crc_type == -1) {
		fprintf(stdout, "CRC option used but CRC type not specified, using default Zlib CRC\n");
		crc_type = CRC_ZLIB;
	}
	
	m = k + p;

	/* Sanity check */
	if (rdma == 1 && (server_name[0] == 0)) {
		fprintf(stderr, "RDMA enabled but server name is not specified\n");
		return 1;
	}

	if (rdma == 1 && (port == 0)) {
		fprintf(stderr, "RDMA enabled but port is not specified\n");
		return 1;
	}

	if (blk_unit != 'k' && (blk_unit != 'm')) {
		fprintf(stderr, "Invalid unit for blk size\n");
		usage();
		return 1;
	}
	if (data_unit != 'm' && (data_unit != 'g') && (data_unit != 't') && (data_unit != 'p')) {
		fprintf(stderr, "Invalid unit for total data size\n");
		usage();
		return 1;
	}
	

	if (m > MMAX || k > KMAX || m < 0 || p < 1 || k < 1) {
		fprintf(stderr, "Input test parameter error m=%d, k=%d, p=%d, erasures=%d\n",
			m, k, p, nerrs);
		return 1;
	}

	if (nerrs > p) {
		fprintf(stderr, "Number of erasures chosen exceeds power of code erasures=%d p=%d\n",
			nerrs, p);
			usage();
		return 1;
	}

	for (i = 0; i < nerrs; i++) {
		if (frag_err_list[i] >= m) {
			fprintf(stderr, "Fragment %d not in range\n", frag_err_list[i]);
			usage();
			return 1;
		}
	}
	if (numa_nodes != 0 && cpus_per_numa == 0) {
		fprintf(stderr, "Number of numa nodes is set but cpus per numa node is set\n");
		return 1;
	}

	if (numa_nodes == 0 && cpus_per_numa != 0) {
		fprintf(stderr, "Number of numa nodes is not set but cpu per numa node is set\n");
		return 1;
	}

	/* check compression option */
	if (comp_opt < NO_COMP || comp_opt > COMP_AFTER_ENCODE) {
		fprintf(stderr, "Invalid compression option\n");
		return 1;
	}

	/* check CRC option */
	if (crc_opt < NO_CRC || crc_opt > CRC_LAST) {
		fprintf(stderr, "Invalid CRC option\n");
		return 1;
	}

	if (crc_opt > NO_CRC && crc_type == NO_CRC) {
		fprintf(stdout, "CRC options specified but no CRC type specified, using default ZLIB crc\n");
		crc_type = CRC_ZLIB;
	}

	/* check CRC type */
	if (crc_type < NO_CRC || crc_type > CRC_RFC) {
		fprintf(stderr, "Invalid CRC type\n");
		return 1;
	}

	/* setup encode and decode function pointers */
        switch (comp_opt) {
                case NO_COMP:
                        encode_func_ptr = encode_data;
                        break;
                case COMP_BEFORE_ENCODE:
                        encode_func_ptr = encode_data_compress_before_encode;
                        break;
                case COMP_AFTER_ENCODE:
                        encode_func_ptr = encode_data_compress_after_encode;
			break;
        }

	/* setup compression input data based on options */
	if (comp_opt > NO_COMP && (comp_ratio > 0) && (rdma == 0)) {
		comp_data = _read_comp_data(comp_ratio, &comp_data_size);
		if (comp_data == NULL) {
			fprintf(stderr, "Failed to read compression input data\n");
			return 1;
		}
	}
	else if (comp_opt > NO_COMP && (comp_ratio == 0) && (rdma == 0)) {
		/* init random data */	
		comp_data = (u8 *)malloc(blk_size_abs * sizeof(u8));
		for (i = 0; i < blk_size_abs; i++)
			comp_data[i] = rand();
		comp_data_size = blk_size_abs;
		printf("random data init \n");
	}

	/* setup crc function */
	switch (crc_type) {
		case CRC_IEEE:
			printf("Using Isa-l IEEE crc\n");
			crc_func_ptr = _crc_ieee;
			break;
		case CRC_RFC:
			printf("Using Isa-l RFC crc\n");
			crc_func_ptr = _crc_rfc;
			break;
		case CRC_ZLIB:
			printf("Using Zlib crc\n");
			crc_func_ptr = _crc_zlib;
			break;
	}

	if (rdma == 0 )
		fprintf(stdout, "*****************************************************\nErasure code benchmark settings:\nTotal data size: %d %cB\nNumber of data blocks: %d\nNumber of parity blocks: %d\nNumber of blocks to recover: %d\nBlock size: %d %cB\nThread count: %d\n", data_size, data_unit, k, p, nerrs, blk_size, blk_unit, thread_cnt);
	else 
		fprintf(stdout, "*****************************************************\nErasure code benchmark settings:\nRDMA enabled\n\nNumber of data blocks: %d\nNumber of parity blocks: %d\nBlock size: %d %cB\nThread count: %d\n", k, p, blk_size, blk_unit, thread_cnt);

	switch (comp_opt) {
		case NO_COMP:
			fprintf(stdout, "Compression: No compression\n");
			break;
		case COMP_BEFORE_ENCODE:
			fprintf(stdout, "Compression: Before encoding\n");
			break;
		case COMP_AFTER_ENCODE:
			fprintf(stdout, "Compression: After encoding\n");
			break;
	}
	
	if (comp_opt > NO_COMP) {
		if (comp_ratio != 0)
			fprintf(stdout, "Compression ratio: %.2f\n", comp_ratio);
		else
			fprintf(stdout, "Compression ratio: 0 - Random data\n");
	}
	switch (crc_opt) {
		case NO_CRC:
			fprintf(stdout, "CRC placement: NO_CRC\n");
			break;
		case CRC_FIRST:
			fprintf(stdout, "CRC placement: CRC first\n");
			break;
		case CRC_SECOND:
			fprintf(stdout, "CRC placement: CRC between compression/encoding\n");
		 	break;
		case CRC_LAST:
			fprintf(stdout, "CRC placement: CRC fter both compression/encoding\n");
			break;
	}

	switch(crc_type) {
		case NO_CRC:
			fprintf(stdout, "CRC library: No CRC\n");
			break;
		case CRC_ZLIB:
			fprintf(stdout, "CRC library: Zlib adler32\n");
			break;
		case CRC_IEEE:
			fprintf(stdout, "CRC library: Intel Isa-l crc32_ieee\n");
			break;
		case CRC_RFC:
			fprintf(stdout, "CRC library: Intel Isa-l crc32_gzip_refl\n");
			break;
	}
	
	if (numa_nodes != 0 && cpus_per_numa != 0)
		fprintf(stdout, "Number of numa nodes: %d\nCPUs per numa node: %d\n", numa_nodes, cpus_per_numa);

	fprintf(stdout, "*****************************************************\n");
	/* allocate coding matrices for each thread */	
	encode_matrix = malloc(m * k);
	decode_matrix = malloc(m * k);
	invert_matrix = malloc(m * k);
	temp_matrix   = malloc(m * k);
	g_tbls 	      = malloc(k * p * 32);
	if (encode_matrix == NULL || (decode_matrix == NULL) ||
               	(invert_matrix == NULL) || (temp_matrix == NULL) ||
               	(g_tbls == NULL)) {
		fprintf(stderr, "Benchmark failure! Error with malloc\n");
		return 1;
	}

	/* generate encode matrix and init tables */
	fprintf(stdout, "**** Generating encode matrix and table ****\n");
	clock_gettime(CLOCK_MONOTONIC, &start);
        gf_gen_rs_matrix(encode_matrix, m, k);
        ec_init_tables(k, p, &encode_matrix[k * k], g_tbls);	
	clock_gettime(CLOCK_MONOTONIC, &end);
	diff = (double)(end.tv_sec - start.tv_sec) + ((double)(end.tv_nsec - start.tv_nsec)/1000000000L);
	fprintf(stdout, "**** Finished generating encode matrix and table ****\nTime taken for generating encode matrix and table: %f seconds\n", diff);
	
	if (rdma == 1) {
		_rdma_benchmark(server_name, port, thread_cnt, blk_size_abs, k, p, g_tbls, comp_opt, crc_opt, crc_func_ptr);
                free(encode_matrix);
                free(decode_matrix);
                free(invert_matrix);
                free(temp_matrix);
                free(g_tbls);
		exit(1);
	}

	tid = (pthread_t *)malloc(thread_cnt * (sizeof(pthread_t)));
	encode_args = (encode_thread_args *)malloc(thread_cnt * (sizeof(encode_thread_args)));
	if (pthread_barrier_init(&barrier, NULL, thread_cnt)) {
		fprintf(stderr, "Failed to init pthread barrier\n");
		return -1;
	}

	bws = (double *)malloc(thread_cnt * sizeof(double));
	pbws = (double *)malloc(thread_cnt * sizeof(double));

	/* start encoding */
	fprintf(stdout, "**** Start encoding ****\n");
	for (i = 0; i < thread_cnt; i++) {
		encode_args[i].thread_id = i;
		encode_args[i].crc_opt = crc_opt;
		encode_args[i].crc_func_ptr = crc_func_ptr;
		encode_args[i].k = k;
		encode_args[i].p = p;
		encode_args[i].data_size_abs = data_size_abs;
		encode_args[i].blk_size_abs = blk_size_abs;
		encode_args[i].comp_data_size = comp_data_size;
		encode_args[i].comp_data = comp_data;
		encode_args[i].g_tbls = g_tbls;
		encode_args[i].bws = bws;
		if (comp_opt != NO_COMP)
			encode_args[i].pbws = pbws;
		if (pthread_create(&tid[i], NULL, encode_func_ptr, &encode_args[i])) {
			fprintf(stderr, "Failed creating threads\n");
			return 1;
		}
	}

	for (i = 0; i < thread_cnt; i++) 
		pthread_join(tid[i], NULL);

	free(encode_args);
	/* print out stats */
	_print_bandwidth(ENCODE_LOGICAL, bws, thread_cnt);
	if (comp_opt != NO_COMP)
		_print_bandwidth(ENCODE_PHYSICAL, pbws, thread_cnt);
	free(comp_data);

	/* we are not simulating failures, so nothing to decode */
	if (nerrs <= 0) {
		free(bws);
		free(pbws);
		free(encode_matrix);
		free(decode_matrix);
		free(invert_matrix);
		free(temp_matrix);
		free(g_tbls);
		free(tid);
		return 0;
	}
	/* generate decode matrix */
	fprintf(stdout, "**** Generating decode matrix and table ****\n");
	clock_gettime(CLOCK_MONOTONIC, &start);
	ret = _gf_gen_decode_matrix_simple(encode_matrix, decode_matrix,
						invert_matrix, temp_matrix,
						decode_index, frag_err_list, nerrs, k, m);

	if (ret < 0) {
		fprintf(stderr, "Failed to generate decode matrix\n");
		return -1;
	}

	ec_init_tables(k, nerrs, decode_matrix, g_tbls);
	clock_gettime(CLOCK_MONOTONIC, &end);
        diff = (double)(end.tv_sec - start.tv_sec) + ((double)(end.tv_nsec - start.tv_nsec)/1000000000L);
        fprintf(stdout, "**** Finished generating decode matrix and table ****\nTime taken for generating decode matrix and table: %f seconds\n", diff);
	/* lets decode */
	fprintf(stdout, "**** Start recovering data ****\nNumber of blocks to recover: %d\n", nerrs);
	decode_args = (decode_thread_args *)malloc(thread_cnt * (sizeof(decode_thread_args)));
	for (i = 0; i < thread_cnt; i++) {
		/* set up recovery array pointers */
		decode_args[i].thread_id = i;
		decode_args[i].k = k;
		decode_args[i].p = p;
		decode_args[i].nerrs = nerrs;
		decode_args[i].data_size_abs = data_size_abs;
		decode_args[i].blk_size_abs = blk_size_abs;
		decode_args[i].g_tbls = g_tbls;
		decode_args[i].decode_index = decode_index;
		decode_args[i].bws = bws;
		if (pthread_create(&tid[i], NULL, decode_data, &decode_args[i])) {
			fprintf(stderr, "Failed creating threads\n");
			return 1;
		}
	}

	for (i = 0; i < thread_cnt; i++)
		pthread_join(tid[i], NULL);

	_print_bandwidth(DECODE, bws, thread_cnt);
	free(decode_args);
	return 0;
}

/*****************************************/
/**** Static Function Definitions ********/
/*****************************************/
static double _get_comp_ratio(double comp_ratio)
{
	double ret = 0;
	
	if (comp_ratio < 0)
		ret = -1;
	else if (comp_ratio == 0)
		ret = 0;
	else if (comp_ratio > 0 &&
			(comp_ratio <= 0.25))
		ret = 0.25;
	else if (comp_ratio > 0.25 && 
			(comp_ratio <= 0.5))
		ret = 0.5;
	else if (comp_ratio > 0.5)
		ret = 0.75;

	return ret;
}

static u8 * _read_comp_data(double comp_ratio, unsigned long *comp_data_size)
{
	int fd;
	ssize_t n;
	char fpath[PATH_MAX];
	u8 *data = NULL;
	struct stat sbuf;
	if (comp_ratio == 0)
		return NULL;
	else if (comp_ratio == 0.25)
		snprintf(fpath, PATH_MAX, "comp_4_to_1.bin");
	else if (comp_ratio == 0.5)
		snprintf(fpath, PATH_MAX, "comp_2_to_1.bin");
	else if (comp_ratio == 0.75)
		snprintf(fpath, PATH_MAX, "comp_1.3_to_1.bin");

	if (stat(fpath, &sbuf)) {
		printf("Fail to stat compression input file %s\n", fpath);
		return NULL;
	}

	/* allocate space for input file */
	data = (u8 *)malloc(sbuf.st_size * sizeof(u8));
	if ((fd = open(fpath, O_RDONLY)) < 0) {
		printf("Fail to open compression input file %s\n", fpath);
		free(data);
		return NULL;
	}

	if ((n = read(fd, data, sbuf.st_size)) != sbuf.st_size) {
		printf("Error reading compression input file\n");
		free(data);
		return NULL;
	}
	close(fd);
	*comp_data_size = sbuf.st_size;

	return data;
}

static long long _get_size_abs(long long base, char unit)
{
	long long size_abs = 0;
	switch (unit) {
		case 'k':
			size_abs = ((long long)base) * K;
			break;
		case 'm':
			size_abs = ((long long)base) * M;
			break;
		case 'g':
			size_abs = ((long long)base) * G;
			break;
		case 't':
			size_abs = ((long long)base) * T;
			break;
	}

	return size_abs;
}

static void _print_bandwidth(char mode, double *bws, int thread_cnt)
{
	int i;
	double min;
	min = DBL_MAX;

	for (i = 0; i < thread_cnt; i++) {
		if (min >= bws[i])
			min = bws[i];
	}

	switch (mode) {
		case ENCODE_LOGICAL:
			fprintf(stdout, "Encoding logical aggregate bandwidth: %f MB/s\n", min * thread_cnt);
			break;
		case DECODE:
			fprintf(stdout, "Decoding aggregate bandwidth: %f MB/s\n", min * thread_cnt);
			break;
		case ENCODE_PHYSICAL:
			fprintf(stdout, "Encoding physical aggregate bandwidth: %f MB/s\n", min * thread_cnt);
			break;
	}
}

/*
 * Generate decode matrix from encode matrix and erasure list
 *
 */

static int _gf_gen_decode_matrix_simple(u8 * encode_matrix,
				       u8 * decode_matrix,
				       u8 * invert_matrix,
				       u8 * temp_matrix,
				       u8 * decode_index, u8 * frag_err_list, int nerrs, int k,
				       int m)
{
	int i, j, p, r;
	int nsrcerrs = 0;
	u8 s, *b = temp_matrix;
	u8 frag_in_err[MMAX];

	memset(frag_in_err, 0, sizeof(frag_in_err));

	// Order the fragments in erasure for easier sorting
	for (i = 0; i < nerrs; i++) {
		if (frag_err_list[i] < k)
			nsrcerrs++;
		frag_in_err[frag_err_list[i]] = 1;
	}

	// Construct b (matrix that encoded remaining frags) by removing erased rows
	for (i = 0, r = 0; i < k; i++, r++) {
		while (frag_in_err[r])
			r++;
		for (j = 0; j < k; j++)
			b[k * i + j] = encode_matrix[k * r + j];
		decode_index[i] = r;
	}

	// Invert matrix to get recovery matrix
	if (gf_invert_matrix(b, invert_matrix, k) < 0)
		return -1;

	// Get decode matrix with only wanted recovery rows
	for (i = 0; i < nerrs; i++) {
		if (frag_err_list[i] < k)	// A src err
			for (j = 0; j < k; j++)
				decode_matrix[k * i + j] =
				    invert_matrix[k * frag_err_list[i] + j];
	}

	// For non-src (parity) erasures need to multiply encode matrix * invert
	for (p = 0; p < nerrs; p++) {
		if (frag_err_list[p] >= k) {	// A parity err
			for (i = 0; i < k; i++) {
				s = 0;
				for (j = 0; j < k; j++)
					s ^= gf_mul(invert_matrix[j * k + i],
						    encode_matrix[k * frag_err_list[p] + j]);
				decode_matrix[k * p + i] = s;
			}
		}
	}
	return 0;
}

static long _compress(u8 *in, u8* out, unsigned long buf_size, z_stream *strm, int flush, long long *total_comp_size)
{
	int ret;
	long comp_size;

	strm->avail_in = buf_size;
	strm->next_in = in;
	comp_size = 0;
	do {
		strm->avail_out = CHUNK_SIZE;
		strm->next_out = out;
		ret = deflate(strm, flush);
		assert(ret != Z_STREAM_ERROR);
		comp_size += CHUNK_SIZE - strm->avail_out;
		*total_comp_size += CHUNK_SIZE - strm->avail_out;
	} while (strm->avail_out == 0);
	assert(strm->avail_in == 0);

	return comp_size;
}

static void _crc_ieee(long long stripe_cnt, int k, long long blk_size_abs, u8 **frag_ptrs)
{
	int i, j;
	uint32_t crc = TEST_SEED;

	for(i = 0; i < stripe_cnt; i++) {
		for (j = 0; j < k; j++) {
			crc = crc32_ieee(crc, frag_ptrs[j], blk_size_abs);
		}
	}
}

static void _crc_rfc(long long stripe_cnt, int k, long long blk_size_abs, u8 **frag_ptrs)
{
	int i, j;
	uint32_t crc = TEST_SEED;
	for (i = 0; i < stripe_cnt; i++) {
		for (j = 0; j < k; j++) {
			crc = crc32_gzip_refl(crc, frag_ptrs[j], blk_size_abs);
		}
	}
}

static void _crc_zlib(long long stripe_cnt, int k, long long blk_size_abs, u8 **frag_ptrs)
{
	int i, j;
	unsigned long adler = adler32(0L, Z_NULL, 0);
	for (i = 0; i < stripe_cnt; i++) 
		for (j = 0; j < k; j++) 
			adler = adler32(adler, frag_ptrs[j], blk_size_abs);
}

static void _set_numa(int thread_id)
{
	int which_numa = thread_id % numa_nodes;
	int which_core = (which_numa * cpus_per_numa) + ((thread_id - which_numa) / numa_nodes);
	pthread_t tid = pthread_self();
	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	CPU_SET(which_core, &cpuset);
	if (pthread_setaffinity_np(tid, sizeof(cpu_set_t), &cpuset)) {
		fprintf(stderr, "Failed to pin thread %d to cpu %d\n", thread_id, thread_id);
		exit(1);
	}
}

void *encode_data_compress_before_encode(void *args)
{
        int i, j, thread_id, k, p, m, flush, crc_opt;
        long long stripe_cnt, stripe_cnt_comp, blk_size_abs, total_comp_size, total_comp_chunks, mod_bytes;
	long long data_size_abs;
	int crc_size = 0;
	unsigned int seed;
        struct timespec start, end;
	u8 *comp_data;
	unsigned long comp_data_size;
	u8 *out;
	u8 *frag_ptrs[MMAX];
        u8 *g_tbls;
        double total_time, bw, crc_time;
	crc_func crc_func_ptr;
        z_stream strm;

        encode_thread_args *encode_args = (encode_thread_args *)args;
        thread_id = encode_args->thread_id;
	crc_opt = encode_args->crc_opt;
	crc_func_ptr = encode_args->crc_func_ptr;
        k = encode_args->k;
        p = encode_args->p;
        m = p + k;
        data_size_abs = encode_args->data_size_abs;
        blk_size_abs = encode_args->blk_size_abs;
	comp_data_size = encode_args->comp_data_size;
	comp_data = (u8 *)malloc(comp_data_size * sizeof(u8));
	memcpy(comp_data, encode_args->comp_data, comp_data_size);
	out = (u8 *)malloc(comp_data_size * sizeof(u8));
	total_comp_size = 0;
	total_comp_chunks = data_size_abs / comp_data_size;
	mod_bytes = data_size_abs % comp_data_size;
	stripe_cnt = ceil(((double)data_size_abs) / ((double)(k * blk_size_abs)));
	fprintf(stdout, "thread_id %d: stripe cnt %lld, comp chunks %lld\n", thread_id, stripe_cnt, total_comp_chunks);
	total_time = 0;
	crc_time = 0;

	if (numa_nodes != 0)
		_set_numa(thread_id);
       
	 /* allocate local input */
        g_tbls = malloc(k * p * 32);
        memcpy(g_tbls, encode_args->g_tbls, k * p * 32);
        for (i = 0; i < m; i++) {
                if (NULL == (frag_ptrs[i] = malloc(blk_size_abs))) {
                        fprintf(stderr, "malloc error\n");
                        exit(1);
                }
        }

        /* give random data to compressed buf */
        seed = thread_id;
        for (i = 0; i < k; i++)
                for (j = 0; j < blk_size_abs; j++)
                        frag_ptrs[i][j] = rand_r(&seed);

	/* add crc size */
	if (crc_opt > NO_CRC)
		crc_size = sizeof(uint32_t);
	else
		crc_size = 0;

	/* crc before compression? */
	if (crc_opt == CRC_FIRST) {
		clock_gettime(CLOCK_MONOTONIC, &start);
		crc_func_ptr(stripe_cnt, k, blk_size_abs, frag_ptrs);
		clock_gettime(CLOCK_MONOTONIC, &end);
		crc_time += (double)(end.tv_sec - start.tv_sec) + ((double)(end.tv_nsec - start.tv_nsec)/1000000000L);
	}
        /* setup zlib */
        strm.zalloc = Z_NULL;
        strm.zfree = Z_NULL;
        strm.opaque = Z_NULL;
        if (deflateInit(&strm, Z_DEFAULT_COMPRESSION)) {
                fprintf(stderr, "Failed deflateInit\n");
                exit(1);
        }

	/* compress */
	for (i = 0; i < total_comp_chunks; i++) {
		if (mod_bytes != 0)
			flush = Z_NO_FLUSH;
		else if (i < total_comp_chunks - 1)
			flush = Z_NO_FLUSH;
		else
			flush = Z_FINISH;
		clock_gettime(CLOCK_MONOTONIC, &start);
		_compress(comp_data, out, comp_data_size, &strm, flush, &total_comp_size);
		clock_gettime(CLOCK_MONOTONIC, &end);
		total_time += (double)(end.tv_sec - start.tv_sec) + ((double)(end.tv_nsec - start.tv_nsec)/1000000000L);
	}

	if (mod_bytes != 0) {
		flush = Z_FINISH;
		clock_gettime(CLOCK_MONOTONIC, &start);
		_compress(comp_data, out, mod_bytes, &strm, flush, &total_comp_size);
		clock_gettime(CLOCK_MONOTONIC, &end);
		total_time += (double)(end.tv_sec - start.tv_sec) + ((double)(end.tv_nsec - start.tv_nsec)/1000000000L);
	}
	(void)deflateEnd(&strm);
	/* calculate stripe count after compression */
	stripe_cnt_comp = ceil(((double)total_comp_size) / ((double)k * blk_size_abs));
	fprintf(stdout, "thread %d: total data before compress %lld. Total data after compress %lld, stripe count after compress %lld\n", thread_id, data_size_abs, total_comp_size, stripe_cnt_comp);

	/* crc fater compression? */
	if (crc_opt == CRC_SECOND) {
		clock_gettime(CLOCK_MONOTONIC, &start);
		crc_func_ptr(stripe_cnt_comp, k, blk_size_abs, frag_ptrs);
		clock_gettime(CLOCK_MONOTONIC, &end);
		crc_time += (double)(end.tv_sec - start.tv_sec) + ((double)(end.tv_nsec - start.tv_nsec)/1000000000L);
	}
        /* process each stripe */
        for (i = 0; i < stripe_cnt_comp; i++) {
		clock_gettime(CLOCK_MONOTONIC, &start);
                ec_encode_data(blk_size_abs, k, p, g_tbls, frag_ptrs, &frag_ptrs[k]);
		clock_gettime(CLOCK_MONOTONIC, &end);
		total_time += (double)(end.tv_sec - start.tv_sec) + ((double)(end.tv_nsec - start.tv_nsec)/1000000000L);
		if (crc_opt == CRC_LAST) {
			clock_gettime(CLOCK_MONOTONIC, &start);
			crc_func_ptr(1, m, blk_size_abs, frag_ptrs);
			clock_gettime(CLOCK_MONOTONIC, &end);
			crc_time += (double)(end.tv_sec - start.tv_sec) + ((double)(end.tv_nsec - start.tv_nsec)/1000000000L);
		}
        }

	/* calculate logical bandwidth */
	bw = (((double)(data_size_abs + crc_size + (blk_size_abs * p * stripe_cnt_comp))) / 1000000LL) / (total_time + crc_time);
	encode_args->bws[thread_id] = bw;
	/* calculate physical bandwidth */
	bw = ((((double)(stripe_cnt_comp * blk_size_abs * m + crc_size))) / 1000000LL) / (total_time + crc_time);
	encode_args->pbws[thread_id] = bw;
	free(g_tbls);
	for (i = 0; i < m; i++)
		free(frag_ptrs[i]);
	free(comp_data);
	free(out);
	printf("crc time %f\n", crc_time);
	return NULL;
}

void * encode_data_compress_after_encode(void *args)
{
        int i, j, thread_id, k, p, m, flush, crc_opt;
	unsigned int seed;
        long long stripe_cnt, blk_size_abs, total_comp_size, data_size_abs;
        struct timespec start, end;
        u8 *frag_ptrs[MMAX];
        u8 *g_tbls;
	u8 **out;
	u8 *comp_data;
	u8 *comp_data_ptr;
	unsigned long comp_data_size;
	unsigned long comp_data_remain;
	crc_func crc_func_ptr;
        double bw, total_time, crc_time;
	int crc_size = 0;
	z_stream strm;

        encode_thread_args *encode_args = (encode_thread_args *)args;
        thread_id = encode_args->thread_id;
	crc_opt = encode_args->crc_opt;
        k = encode_args->k;
        p = encode_args->p;
        m = p + k;
        data_size_abs = encode_args->data_size_abs;
        blk_size_abs = encode_args->blk_size_abs;
	comp_data_size = encode_args->comp_data_size;
	comp_data = (u8 *)malloc(comp_data_size * sizeof(u8));
	memcpy(comp_data, encode_args->comp_data, comp_data_size);
	crc_func_ptr = encode_args->crc_func_ptr;
	out = (u8 **)malloc(m * sizeof(u8 *));
	for (i = 0; i < m; i++) {
		out[i] = (u8 *)malloc(blk_size_abs * sizeof(u8) * 2);
	}

        if (numa_nodes != 0) {
		_set_numa(thread_id);
        }

	stripe_cnt = ceil(((double)data_size_abs) / ((double)(k * blk_size_abs)));
	fprintf(stdout, "thread id %d: stripe cnt %lld\n", thread_id, stripe_cnt);
	/* allocate local input */
        g_tbls = malloc(k * p * 32);
        memcpy(g_tbls, encode_args->g_tbls, k * p * 32);
        for (i = 0; i < m; i++) {
                if (NULL == (frag_ptrs[i] = malloc(blk_size_abs))) {
                        fprintf(stderr, "malloc error\n");
                        exit(1);
                }
        }
	
	total_time = 0;
	crc_time = 0;
        /* add crc size */
        if (crc_opt > NO_CRC)
                crc_size = sizeof(uint32_t);
        else
                crc_size = 0;

	/* give random data */
	seed = thread_id;
        for (i = 0; i < k; i++)
                for (j = 0; j < blk_size_abs; j++)
                        frag_ptrs[i][j] = rand_r(&seed);
	if (crc_opt == CRC_FIRST) {
                clock_gettime(CLOCK_MONOTONIC, &start);
                crc_func_ptr(stripe_cnt, k, blk_size_abs, frag_ptrs);
                clock_gettime(CLOCK_MONOTONIC, &end);
                crc_time += (double)(end.tv_sec - start.tv_sec) + ((double)(end.tv_nsec - start.tv_nsec)/1000000000L);
	}

	/* setup zlib */
	strm.zalloc = Z_NULL;
	strm.zfree = Z_NULL;
	strm.opaque = Z_NULL;
	if (deflateInit(&strm, Z_DEFAULT_COMPRESSION)) {
		fprintf(stderr, "Failed deflateInit\n");
		exit(1);
	}
	total_comp_size = 0;
	comp_data_remain = comp_data_size;
	comp_data_ptr = comp_data;
        /* process each stripe */
        for (i = 0; i < stripe_cnt; i++) {
		clock_gettime(CLOCK_MONOTONIC, &start);
                ec_encode_data(blk_size_abs, k, p, g_tbls, frag_ptrs, &frag_ptrs[k]);
		clock_gettime(CLOCK_MONOTONIC, &end);
		total_time += (double)(end.tv_sec - start.tv_sec) + ((double)(end.tv_nsec - start.tv_nsec)/1000000000L);
		if (crc_opt == CRC_SECOND) {
			clock_gettime(CLOCK_MONOTONIC, &start);
			crc_func_ptr(1, m, blk_size_abs, frag_ptrs);
			clock_gettime(CLOCK_MONOTONIC, &end);
			crc_time += (double)(end.tv_sec - start.tv_sec) + ((double)(end.tv_nsec - start.tv_nsec)/1000000000L);
		}
		flush = (i == stripe_cnt - 1? Z_FINISH : Z_NO_FLUSH);
		for (j = 0; j < m; j++) {
			if ((j < m - 1) || (j == m - 1 && (i < stripe_cnt - 1)))
				flush = Z_NO_FLUSH;
			else if ((i == stripe_cnt - 1) && (j == m - 1))
				flush = Z_FINISH;
			/* best effort to use compression input data to achieve desired compression ratio */
			if (comp_data_remain < blk_size_abs) {
				comp_data_ptr = comp_data;
				comp_data_remain = comp_data_size;
			}
			clock_gettime(CLOCK_MONOTONIC, &start);
			//long comp_size = _compress(frag_ptrs[i], out[j], blk_size_abs, &strm, flush, &total_comp_size);
			long comp_size = _compress(comp_data_ptr, out[j], blk_size_abs, &strm, flush, &total_comp_size);
			clock_gettime(CLOCK_MONOTONIC, &end);
			total_time += (double)(end.tv_sec - start.tv_sec) + ((double)(end.tv_nsec - start.tv_nsec)/1000000000L);
			comp_data_remain -= blk_size_abs;
			comp_data_ptr += blk_size_abs;
			if (crc_opt == CRC_LAST) {
				clock_gettime(CLOCK_MONOTONIC, &start);
				crc_func_ptr(1, 1, comp_size, &out[j]);
				clock_gettime(CLOCK_MONOTONIC, &end);
				crc_time += (double)(end.tv_sec - start.tv_sec) + ((double)(end.tv_nsec - start.tv_nsec)/1000000000L);
			}
		}
        }

	fprintf(stdout, "total data before compress %lld, after compress %lld\n", blk_size_abs * stripe_cnt * m, total_comp_size);
        free(g_tbls);
        for (i = 0; i < m; i++) {
                free(frag_ptrs[i]);
		free(out[i]);
        }
	free(out);
	free(comp_data);
        bw = (((double)(crc_size + stripe_cnt * blk_size_abs * (k + p))) / 1000000LL) / (total_time + crc_time);
        encode_args->bws[thread_id] = bw;
	bw = (((double)(total_comp_size + crc_size)) / 1000000LL) / (total_time + crc_time);
	encode_args->pbws[thread_id] = bw;
	printf("crc_time %f\n", crc_time);
        return NULL;
}

void * encode_data(void *args)
{
	int i, j, thread_id, k, p, m, crc_opt;
	unsigned int seed;
        long long stripe_cnt, blk_size_abs, data_size_abs;
	struct timespec start, end;
        u8 *frag_ptrs[MMAX];
        u8 *g_tbls;
	crc_func crc_func_ptr;
	double total_time, crc_time, bw;

	encode_thread_args *encode_args = (encode_thread_args *)args;
        thread_id = encode_args->thread_id;
	crc_opt = encode_args->crc_opt;
	crc_func_ptr = encode_args->crc_func_ptr;
        k = encode_args->k;
        p = encode_args->p;
	m = p + k;
        data_size_abs = encode_args->data_size_abs;
        blk_size_abs = encode_args->blk_size_abs;

	/* evenly spread out each thread to a numa domain */
	if (numa_nodes != 0) {
		_set_numa(thread_id);
	}

	total_time = 0;
	crc_time = 0;
	stripe_cnt = ceil(((double)data_size_abs) / ((double)(k * blk_size_abs)));
	fprintf(stdout, "thread id %d: stripe cnt %lld\n", thread_id, stripe_cnt);
	/* each thread access local g_tbls to reduce NUMA contention */
        g_tbls = malloc(k * p * 32);
	memcpy(g_tbls, encode_args->g_tbls, k * p * 32);

	/* setup local input */
	seed = thread_id;
	for (i = 0; i < m; i++) {
		if (NULL == (frag_ptrs[i] = malloc(blk_size_abs))) {
			fprintf(stderr, "malloc error\n");
			exit(1);
		}
	}
	for (i = 0; i < k; i++)
		for (j = 0; j < blk_size_abs; j++)
			frag_ptrs[i][j] = rand_r(&seed);

	if (crc_opt == CRC_FIRST) {
		printf("here?\n");
		clock_gettime(CLOCK_MONOTONIC, &start);
		crc_func_ptr(stripe_cnt, k, blk_size_abs, frag_ptrs);
		clock_gettime(CLOCK_MONOTONIC, &end);
		crc_time += (double)(end.tv_sec - start.tv_sec) + ((double)(end.tv_nsec - start.tv_nsec)/1000000000L);
	}
	clock_gettime(CLOCK_MONOTONIC, &start);

	/* process each stripe */
	for (i = 0; i < stripe_cnt; i++) {
		ec_encode_data(blk_size_abs, k, p, g_tbls, frag_ptrs, &frag_ptrs[k]);
	}

	clock_gettime(CLOCK_MONOTONIC, &end);
	total_time += (double)(end.tv_sec - start.tv_sec) + ((double)(end.tv_nsec - start.tv_nsec)/1000000000L);
	if (crc_opt == CRC_LAST || crc_opt == CRC_SECOND) {
		printf("here?\n");
		clock_gettime(CLOCK_MONOTONIC, &start);
		crc_func_ptr(stripe_cnt, m, blk_size_abs, frag_ptrs);
		clock_gettime(CLOCK_MONOTONIC, &end);
		crc_time += (double)(end.tv_sec - start.tv_sec) + ((double)(end.tv_nsec - start.tv_nsec)/1000000000L);
	}

	free(g_tbls);	
	for (i = 0; i < m; i++) {
		free(frag_ptrs[i]);
	}

        bw = (((double)(stripe_cnt * blk_size_abs * (k + p))) / 1000000LL) / (total_time + crc_time);
        encode_args->bws[thread_id] = bw;
        printf("Thread %d crc_time %f encode bw: %f MB/s\n", thread_id, crc_time, bw);

	return NULL;
}

void * decode_data(void *args)
{
	int i, j, thread_id, k, p, m, nerrs;
	unsigned int seed;
        long long stripe_cnt, blk_size_abs, data_size_abs;
        struct timespec start, end;
        u8 *g_tbls;
	u8 *frag_ptrs[MMAX];
	u8 *recover_srcs[KMAX];
	u8 *recover_outp[KMAX];
	u8 decode_index[MMAX];
	double diff, bw;

	decode_thread_args *decode_args = (decode_thread_args *)args;
	thread_id = decode_args->thread_id;
	k = decode_args->k;
	p = decode_args->p;
	m = k + p;
	nerrs = decode_args->nerrs;
	data_size_abs = decode_args->data_size_abs;
	blk_size_abs = decode_args->blk_size_abs;
	stripe_cnt = ceil(((double)data_size_abs) / ((double)(k * blk_size_abs)));
	fprintf(stdout, "thread id %d: stripe cnt %lld\n", thread_id, stripe_cnt);
	/* allocate and init local data */
	g_tbls = malloc(k * p * 32);
	memcpy(g_tbls, decode_args->g_tbls, k * p * 32);
	memcpy(decode_index, decode_args->decode_index, (MMAX * sizeof(u8)));

        seed = thread_id;
        for (i = 0; i < m; i++) {
                if (NULL == (frag_ptrs[i] = malloc(blk_size_abs))) {
                        fprintf(stderr, "malloc error\n");
                        exit(1);
                }
        }

        for (i = 0; i < k; i++)
                for (j = 0; j < blk_size_abs; j++)
                        frag_ptrs[i][j] = rand_r(&seed);


	for (i = 0; i < p; i++) {
		if (NULL == (recover_outp[i] = malloc(blk_size_abs))) {
			fprintf(stderr, "malloc error\n");
			exit(1);
		}
	}
	
	for (i = 0; i < k; i++) {
		recover_srcs[i] = frag_ptrs[decode_index[i]];
	}
	

	clock_gettime(CLOCK_MONOTONIC, &start);

	/* recover each stripe */
	for (i = 0; i < stripe_cnt; i++) {
		ec_encode_data(blk_size_abs, k, nerrs, g_tbls, recover_srcs, recover_outp);
	}

	clock_gettime(CLOCK_MONOTONIC, &end);
	
	free(g_tbls);
	for (i = 0; i < m; i ++)
		free(frag_ptrs[i]);

	for (i = 0; i < p; i++)
		free(recover_outp[i]);
	
	diff = (double)(end.tv_sec - start.tv_sec) + ((double)(end.tv_nsec - start.tv_nsec)/1000000000L);
	bw = (((double)(stripe_cnt * blk_size_abs * (k + p))) / 1000000LL) / diff;
	decode_args->bws[thread_id] = bw;
	printf("Thread %d decode bw: %f MB/s\n", thread_id, bw);

	return NULL;
}

void * server_thread(void *arg)
{
        int i;
        rdma_thread_args *args;
        int thread_id;
        int fd;
        long long blk_size_abs;
        long long stripe_size;
        int k;
        int p;
        int m;
        u8 *g_tbls;
        int comp_opt;
        int crc_opt;
        crc_func crc_func_ptr;
        u8 **frag_ptrs;
        u8 *buf, *out;
        struct timespec start, end;
        z_stream strm;

        args = (rdma_thread_args *)arg;
	thread_id = args->thread_id;
        fd = args->fd;
        blk_size_abs = args->blk_size_abs;
        k = args->k;
        p = args->p;
        m = k + p;
        comp_opt = args->comp_opt;
        crc_opt = args->crc_opt;
        crc_func_ptr = args->crc_func_ptr;
        out = NULL; /* ger rid of warning */
        /* make local data */
        g_tbls = (u8 *)malloc(k * p * 32);
        memcpy(g_tbls, args->g_tbls, k * p * 32);
        /* evenly spread out each thread to a numa domain */
        if (numa_nodes != 0) {
                _set_numa(thread_id);
        }

        stripe_size = blk_size_abs * k;
        buf = (u8 *)malloc(m * blk_size_abs * sizeof(u8));
        frag_ptrs = (u8 **)malloc(m * sizeof(u8 *));
        for (i = 0; i < m; i++)
                frag_ptrs[i] = buf + (i * blk_size_abs);

        switch (comp_opt) {
                case COMP_BEFORE_ENCODE:
                        /* compression output buf should be stripe size plus more to accomodate bad compression input */
                        out = (u8 *)malloc(stripe_size * sizeof(u8) * 2);
                        break;
                case COMP_AFTER_ENCODE:
                        /* compression output buf should be each block size since we are compressing each block */
                        out = (u8 *)malloc(blk_size_abs * sizeof(u8) * 2);
                        break;
        }

        if (comp_opt > NO_COMP) {
                /* initialize compression struct */
                strm.zalloc = Z_NULL;
                strm.zfree = Z_NULL;
                strm.opaque = Z_NULL;
                if (deflateInit(&strm, Z_DEFAULT_COMPRESSION)) {
                        fprintf(stderr, "Thread %d failed deflateInit\n", args->thread_id);
                        rclose(fd);
                        exit(1);
                }
        }
        printf("Client connected! start processing!\n");
        /* map a memory region based on block size */
        int ack = 1;
        int req = 2;
        int flush = Z_NO_FLUSH;
        long long total_comp_size = 0;
        long long current_comp_size = 0;
        off_t offset = riomap(fd, buf, stripe_size, PROT_WRITE, 0, -1);

        /* send the offset value to client */
        if (rsend(fd, &offset, sizeof(off_t), 0) < 0)
        {
                printf("Failed to send offset to client\n");
                goto exit;
        }

        /* Now we enter receive loop */
        while (ack > 0)
        {
                /* Notify client that we are ready to receive the next block */
                if(rsend(fd, &req, sizeof(int), 0) < 0) {
                        fprintf(stderr, "rsend failed %m\n");
                        goto exit;
                }
                /* Recv notification from client that it has done rdma write */
                if (rrecv(fd, &ack, sizeof(int), 0) < 0)
                {
                        fprintf(stderr, "rrecv failed %m\n");
                        goto exit;
                }
                /* process data */
                if (crc_opt == CRC_FIRST) {
                        //fprintf(stdout, "crc first\n");
                        clock_gettime(CLOCK_MONOTONIC, &start);
                        crc_func_ptr(1, k, blk_size_abs, frag_ptrs);
                        clock_gettime(CLOCK_MONOTONIC, &end);
                }
                if (comp_opt == COMP_BEFORE_ENCODE) {
                        current_comp_size += _compress(buf, out, stripe_size, &strm, flush, &total_comp_size);
                        /* compress only when there is enough data that can fill a data stripe size */
                        if (current_comp_size >= stripe_size) {
                                if (crc_opt == CRC_SECOND) {
                                        crc_func_ptr(1, k, blk_size_abs, frag_ptrs);
                                }
                                /* encode */
                                ec_encode_data(blk_size_abs, k, p, g_tbls, frag_ptrs, &frag_ptrs[k]);
                                current_comp_size -= stripe_size;
                                if (crc_opt == CRC_LAST) {
                                        crc_func_ptr(1, m, blk_size_abs, frag_ptrs);
                                }
                        }
                }
                else if (comp_opt == COMP_AFTER_ENCODE) {
                        ec_encode_data(blk_size_abs, k, p, g_tbls, frag_ptrs, &frag_ptrs[k]);
                        if (crc_opt == CRC_SECOND) {
                                crc_func_ptr(1, m, blk_size_abs, frag_ptrs);
                        }
                        for (i = 0; i < m; i++) {
                                long comp_size = _compress(frag_ptrs[i], out, blk_size_abs, &strm, flush, &total_comp_size);
                                if (crc_opt == CRC_LAST) {
                                        crc_func_ptr(1, 1, comp_size, &frag_ptrs[i]);
                                }
                        }
                }
                else if (comp_opt == NO_COMP) {
                        ec_encode_data(blk_size_abs, k, p, g_tbls, frag_ptrs, &frag_ptrs[k]);
                        if (crc_opt == CRC_LAST || crc_opt == CRC_SECOND) {
                                crc_func_ptr(1, m, blk_size_abs, frag_ptrs);
                        }
                }
        }
        /* client has finished sending data */
        if (comp_opt == COMP_BEFORE_ENCODE && (current_comp_size > 0)) {
                _compress(buf, out, 0, &strm, Z_FINISH, &total_comp_size);
                /* pad remaining data to data stripe size, which we have already, and encode once more */
                if (crc_opt == CRC_SECOND) {
                        crc_func_ptr(1, k, blk_size_abs, frag_ptrs);
                }
                ec_encode_data(blk_size_abs, k, p, g_tbls, frag_ptrs, &frag_ptrs[k]);
                if (crc_opt == CRC_LAST) {
                        crc_func_ptr(1, m, blk_size_abs, frag_ptrs);
                }
                deflateEnd(&strm);
        }
        else if ((comp_opt == COMP_BEFORE_ENCODE && (current_comp_size == 0))
                        || (comp_opt == COMP_AFTER_ENCODE)) {
                /* we only need to close z_stream because there is no remaining data to encode */
                _compress(buf, out, 0, &strm, Z_FINISH, &total_comp_size);
                deflateEnd(&strm);
        }

exit:   
        /* cleanup */
        if (riounmap(fd, buf, stripe_size) < 0)
        {
                printf("Unmapped failed\n");
        }
        free(buf);
        rclose(fd);
        free(g_tbls);
        free(frag_ptrs);
        if (comp_opt > NO_COMP)
        	free(out);
        return NULL;
}

static void _rdma_benchmark(char *server_name, int port, int thread_cnt,
                                long long blk_size_abs, int k, int p, u8 *g_tbls,
                                int comp_opt, int crc_opt, crc_func crc_func_ptr)
{
        int fd, i;
        pthread_t *tids;
        rdma_thread_args *thread_args;
        fd = _create_server_listenfd(server_name, port, thread_cnt);
        thread_args = (rdma_thread_args *)malloc(thread_cnt * sizeof(rdma_thread_args));
        tids = (pthread_t *)malloc(thread_cnt * sizeof(pthread_t));
        for (i = 0; i < thread_cnt; i++) {
                thread_args[i].thread_id = i;
                thread_args[i].blk_size_abs = blk_size_abs;
                thread_args[i].k = k;
                thread_args[i].p = p;
                thread_args[i].g_tbls = g_tbls;
                thread_args[i].comp_opt = comp_opt;
                thread_args[i].crc_opt = crc_opt;
                thread_args[i].crc_func_ptr = crc_func_ptr;
                while ((thread_args[i].fd = raccept(fd, NULL, 0)) < 0 && ((errno == EAGAIN) || (errno == EINTR)));
                if (pthread_create(&tids[i], NULL, server_thread, &thread_args[i])) {
                        fprintf(stderr, "Failed to create rdma thread\n");
                        rclose(thread_args[i].fd);
                        exit(1);
                }
        }
        for (i = 0; i < thread_cnt; i++)
                pthread_join(tids[i], NULL);

	free(thread_args);
	free(tids);
	close(fd);
}
