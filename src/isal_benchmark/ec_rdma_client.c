#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <getopt.h>
#include <math.h>
#include <limits.h>
#include <errno.h>
#include <sys/time.h>
#include <rdma/rdma_cma.h>
#include <rdma/rsocket.h>

#define NO_COMP                 0
#define COMP_BEFORE_ENCODE      1
#define COMP_AFTER_ENCODE       2

#define NO_CRC 0
#define CRC_FIRST 1
#define CRC_SECOND 2
#define CRC_LAST 3

#define CRC_ZLIB 1
#define CRC_IEEE 2
#define CRC_RFC  3

#define set_size(val, unit) do {                                 \
        switch(unit[0]) {                                        \
                case 'k':                                        \
                case 'K':                                        \
                        val *= 1024;                             \
                        break;                                   \
                case 'm':                                        \
                case 'M':                                        \
                        val *= 1024 * 1024;                      \
                        break;                                   \
                case 'g':                                        \
                case 'G':                                        \
                        val *= 1024 * 1024 * 1024;               \
                        break;                                   \
                default:                                         \
                        val = 0;                                 \
        }                                                        \
} while (0)

typedef struct client_arg
{
        int thread_id;
        char *input;
        char *server_name;
        double thread_time;
        size_t input_size;
        size_t blk_size;
        int k;
        int port;
        int comp_opt;
        double comp_ratio;
        double *bws;
} client_arg_t;

typedef struct svr_arg
{
        int fd;
        size_t blk_size;
} svr_arg_t;

int client_connect(char *server_name, int port)
{
        struct rdma_addrinfo addr;
        struct rdma_addrinfo *res;
        memset(&addr, 0, sizeof(addr));
        addr.ai_port_space = RDMA_PS_TCP;

        if (rdma_getaddrinfo(server_name, NULL, &addr, &res))
        {
                printf("Client failed to get server addr info\n");
                return -1;
        }

        struct sockaddr *addr_ptr = (struct sockaddr *)res->ai_dst_addr;
        socklen_t addr_len = res->ai_dst_len;

        struct sockaddr_in *sin_ptr = (struct sockaddr_in *)addr_ptr;
        char dot_addr[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &sin_ptr->sin_addr, dot_addr, INET_ADDRSTRLEN);
        printf("server ip %s port %d\n", dot_addr, port);

        int fd = rsocket(res->ai_family, SOCK_STREAM, 0);
        if (fd <= 0)
        {
                printf("Client failed to create rsocket fd\n");
        }

        unsigned mapsize = 1;
        if (rsetsockopt(fd, SOL_RDMA, RDMA_IOMAPSIZE, &mapsize, sizeof(mapsize)))
        {
                printf("Client failed rsetsockopt RDMA_IOMAPSIZE\n");
                return -1;
        }

        struct linger lo = {1, 0};
        if (rsetsockopt(fd, SOL_SOCKET, SO_LINGER, &lo, sizeof(lo)))
        {
                printf("Client failed rsetsockopt SO_LINGER\n");
                return -1;
        }

        ((struct sockaddr_in *)addr_ptr)->sin_port = htons((unsigned short)port);

        if (rconnect(fd, addr_ptr, addr_len) < 0)
        {
                printf("Client failed to connect to server %m\n");
                return -1;
        }

        return fd;
}

void _init_comp_data(unsigned char *write_buf, size_t buf_size, double comp_ratio)
{
        int i;
        long rand_count = ceil(((double)buf_size) * comp_ratio);
        printf("rand_count %ld\n", rand_count);
        for (i = 0; i < buf_size; i++)
        {
                if (i < rand_count)
                        write_buf[i] = rand();
                else
                        write_buf[i] = 1;
        }
}

void * client_thread(void *arg)
{
        client_arg_t *args = arg;
        int fd, ack, ready, k, comp_opt, thread_id;
        double comp_ratio;
        off_t offset;
        ssize_t n;
        unsigned char *write_buf;
        size_t blk_size, remain, written, to_write, buf_size;
        struct timespec start, end;

        fd = client_connect(args->server_name, args->port);
        if (fd <= 0)
        {
                printf("Client failed to connect\n");
                return NULL;
        }
        remain = args->input_size;
        blk_size = args->blk_size;
        k = args->k;
        comp_opt = args->comp_opt;
        comp_ratio = args->comp_ratio;
        thread_id = args->thread_id;
        ack = 1;
        buf_size = blk_size * k;
        write_buf = (unsigned char *)malloc(buf_size);
        if (comp_opt > NO_COMP) {
                _init_comp_data(write_buf, buf_size, comp_ratio);
        }
        /* Prepare offset buffer */
        clock_gettime(CLOCK_MONOTONIC, &start);
        n = rrecv(fd, &offset, sizeof(off_t), 0);
        if (n < sizeof(off_t))
        {
                printf("Failed to get remote memory region\n");
                goto exit;
        }

        while(remain)
        {
                /* first wait for server ready message */
                n = rrecv(fd, &ready, sizeof(int), 0);
                if (n < sizeof(int))
                        goto exit;
                /* do rdma write to server */
                if (remain >= buf_size)
                        to_write = buf_size;
                else
                        to_write = remain;
                written =riowrite(fd, write_buf, to_write, offset, 0);
                if (written != to_write) {
                        fprintf(stderr, "Failed to write %zu\n", to_write);
                        ack = -1;
                        rsend(fd, &ack, sizeof(int), 0);
                        goto exit;
                }
                remain -= written;
                if (!remain)
                        ack = -1; /* -1 indicates this is the last block */

                if (rsend(fd, &ack, sizeof(int), 0) < sizeof(int))
                {
                        goto exit;
                }

        }

exit:
        clock_gettime(CLOCK_MONOTONIC, &end);
        rclose(fd);
        double total_time = (double)(end.tv_sec - start.tv_sec) + ((double)(end.tv_nsec - start.tv_nsec)/1000000000L);
        args->bws[thread_id] = ((double)args->input_size) / total_time / 1000000L;
        return NULL;
}

void _print_bandwidth(int num_client, double *bws)
{
        int i;
        double min = INT_MAX;

        for (i = 0; i < num_client; i++)
        {
                if (min >= bws[i])
                        min = bws[i];
        }
        printf("Client side bandwidth: %f\n", num_client * min);
}

int main(int argc, char **argv)
{
        pthread_attr_t attr;


        static struct option long_options[] = {
                { "client",     required_argument,      0,              'c' },
                { "server",     no_argument,            0,              's' },
                { "port",       required_argument,      0,              'p' },
                { "num_client", required_argument,      0,              'n' },
                { "block-size", required_argument,      0,              'b' },
                { "input-size", required_argument,      0,              'i' },
                { 0,            0,                      0,               0  }
        };

        int op;
        int k = 0;
        int comp_opt = 0;
        int crc_opt = 0;
        int crc_type = 0;
        int option_index = 0;
        size_t input_size = 0;
        size_t blk_size = 0; //1M default
        char *tmp_s;
        int num_client = 1;
        int port = -1;
        char *server_name = NULL;
        double comp_ratio = 0;
        while ((op = getopt_long(argc, argv, "hvqms:n:b:i:S:C:R:T:c:p:k:r:x", long_options, &option_index)) != -1) {
                switch(op) {
                        case 'T':
                                if (!strncmp("IEEE", optarg, 4))
                                        crc_type = CRC_IEEE;
                                else if (!strncmp("RFC", optarg, 3))
                                        crc_type = CRC_RFC;
                                else if (!strncmp("ZLIB", optarg, 4))
                                        crc_type = CRC_ZLIB;
                                break;
                        case 'C':
                                crc_opt = atoi(optarg);
                                break;
                        case 'k':
                                k = atoi(optarg);
                                break;
                        case 'n':
                                num_client = atoi(optarg);
                                break;
                        case 'b':
                                blk_size = strtoul(optarg, &tmp_s, 0);
                                if (errno || blk_size == 0) {
                                        blk_size = 0;
                                        break;
                                }
                                if (tmp_s[0] != 0) {
                                        set_size(blk_size, tmp_s);
                                }
                                break;
                        case 'i':
                                input_size = strtoul(optarg, &tmp_s, 0);
                                if (errno || input_size == 0)
                                {
                                        input_size = 0;
                                }
                                if (tmp_s[0] != 0)
                                {
                                        set_size(input_size, tmp_s);
                                }
                                break;
                        case 'c':
                                comp_opt = atoi(optarg);
                                break;
                        case 'R':
                                comp_ratio = atof(optarg);
                                break;
                        case 's':
                                server_name = strdup(optarg);
                                if (server_name[0] == ' ' || server_name[0] == '\0' || server_name[0] == '-')
                                {
                                        printf("Error: empty server name\n");
                                        exit(EINVAL);
                                }
                                break;
                        case 'p':
                                port = atoi(optarg);
                                break;
                        default:
                                printf("Invalid option\n");
                                exit(EINVAL);
                }
        }

        if (port < 0)
        {
                fprintf(stderr, "Must specify port\n");
                return 1;
        }

        if (!server_name)
        {
                printf("Must provide a server hostname\n");
                return -1;
        }

        if (input_size == 0)
        {
                input_size = 4294967296; //4G
        }

        if (blk_size == 0)
        {
                blk_size = 1048576; //1M
        }

        if (pthread_attr_init(&attr) != 0)
        {
                printf("Failed to init pthread attr\n");
                return -1;
        }

        if (pthread_attr_setscope(&attr, PTHREAD_SCOPE_SYSTEM) != 0)
        {
                printf("Failed to set pthread scope\n");
                return -1;
        }

        if (k == 0)
        {
                printf("Must specify number of data blocks\n");
                return -1;
        }

        printf("************* Erasure RDMA Benchmark Setting *************\n");
        printf("Block size: %zu\n", blk_size);
        printf("Number of data blocks: %d\n", k);
        printf("Number of client thread: %d\n", num_client);
        switch (comp_opt)
        {
                case NO_COMP:
                        printf("Compression: No compression\n");
                        break;
                case COMP_BEFORE_ENCODE:
                        printf("Compression: Before encode\nCompression Ratio: %f\n", comp_ratio);
                        break;
                case COMP_AFTER_ENCODE:
                        printf("Compression: After encode\nCompression Ratio: %f\n", comp_ratio);
                        break;
        }


        switch (crc_opt)
        {
                case NO_CRC:
                        printf("CRC option: No CRC\n");
                        break;
                case CRC_FIRST:
                        printf("CRC option: CRC first\n");
                        break;
                case CRC_SECOND:
                        printf("CRC option: CRC between compression/encode\n");
                        break;
                case CRC_LAST:
                        printf("CRC option: CRC after both compression/encode\n");
                        break;
        }

        switch(crc_type)
        {
                case NO_CRC:
                        printf("CRC library: No CRC\n");
                        break;
                case CRC_ZLIB:
                        printf("CRC library: Zlib adler32\n");
                        break;
                case CRC_IEEE:
                        printf("CRC library: Intel Isa-l crc32_ieee\n");
                        break;
                case CRC_RFC:
                        printf("CRC library: Intel Isa-l crc32_gzip_refl\n");
                        break;
        }
        printf("*************************************************************\n");

        /* Client code */
        int i;
        pthread_t *tids = malloc(num_client * sizeof(pthread_t));
        client_arg_t *args = malloc(num_client * sizeof(client_arg_t));
        memset(args, 0, sizeof(num_client * sizeof(client_arg_t)));
        double *bws = (double *)malloc(num_client * sizeof(double));
        for (i = 0; i < num_client; i++)
        {
                args[i].thread_id = i;
                args[i].server_name = server_name;
                args[i].input_size = input_size;
                args[i].blk_size = blk_size;
                args[i].k = k;
                args[i].port = port;
                args[i].comp_opt = comp_opt;
                args[i].comp_ratio = comp_ratio;
                args[i].bws = bws;
                pthread_create(&tids[i], &attr, client_thread, &args[i]);
        }

        for(i = 0; i < num_client; i++)
        {
                pthread_join(tids[i], NULL);
        }
        free(args);
        free(tids);
        _print_bandwidth(num_client, bws);
        free(bws);
        return 0;
}
