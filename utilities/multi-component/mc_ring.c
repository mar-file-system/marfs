#include <stdio.h>
#include <aio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <pthread.h>

#include "marfs_configuration.h"
#include "marfs_base.h"
#include "mc_hash.h"
#include "dal.h"

#include "erasure.h"

#define BUFF_SIZE   10485760 /* 10 MB */
#define QUEUE_SIZE  256
#define MAX_THREADS 1024

struct {
   //   MarFS_Repo *repo; /* the repo we are working in */
   int         num_scatter;
   const char *path_template;
   ring_t     *ring;
} migration_config;

typedef struct migration_spec {
   char *src;
   char *dest;
} migration_spec_t;

struct {
   pthread_mutex_t  lock;
   pthread_cond_t   full_cv;
   pthread_cond_t   empty_cv;
   migration_spec_t queue[QUEUE_SIZE];
   int              head;
   int              tail;
   int              size;
   pthread_t        threads[MAX_THREADS];
} work_queue;

#if 0
int move_object(const char *object_name,
                const char *from, const char *to,
                const char *scatter) {
   int src_fd, dest_fd;
   char *src_path[2048];
   char *dest_path[2048];
   unsigned char *data_buffer[BUFF_SIZE];

   sprintf(src_path, migration_config.path_template, from,);
   strcat(src_path, object_name);
   src_fd = open(src_path, O_RDONLY);
   sprintf(full_path, path_template, to, <scatter>);
   strcat(full_path, object_name);
   dest_fd = open(src_path, O_WRONLY|O_CREAT, 0666);

   int read;
   while((read = read(src_fd, data_buffer, BUFF_SIZE)) != 0) {
      if(read == -1) {
         perror("read()");
         close(src_fd);
         close(dest_fd);
         unlink(dest_path);
         return -1;
      }
      int written = write(dest_fd, data_buffer, read);
      // TODO: retry write when written < read ?
      // will anything interrupt this?
      if(written < read && errno != EINTR) {
         perror("write()");
         close(src_fd);
         close(dest_fd);
         unlink(dest_path);
         return -1;
      }
   }
   if(ne_get_xattr(src_path, data_buffer) == -1) {
      close(src_fd);
      close(dest_fd);
      unlink(dest_path);
      return -1;
   }
   if(ne_set_xattr(dest_path, data_buffer, strlen(data_buffer)) == -1) {
      close(src_fd);
      close(dest_fd);
      unlink(dest_path);
      return -1;
   }

   close(src_fd);
   close(dest_fd);
   ne_delete_block(src_path);

   return 0;
}
#endif // commented out.

int link_block(const char *object_name,
               const char *target, const char *link,
               int scatter) {
   char target_path[2048];
   sprintf(target_path, migration_config.path_template,
           target, scatter);
   strcat(target_path, "/");
   strcat(target_path, object_name);

   char link_path[2048];
   sprintf(link_path, migration_config.path_template,
           link, scatter);
   strcat(link_path, "/");
   strcat(link_path, object_name);

   return ne_link_block(link_path, target_path);
}

int migrate_light(node_list_t *from, ring_t *ring) {
   int ret = 0;
   node_iterator_t *it = node_iterator(from);
   const char *n;
   while((n = next_node(it)) != NULL) {
      int scatter;
      for(scatter = 0; scatter < migration_config.num_scatter; scatter++) {
         char scatter_path[2048];
         sprintf(scatter_path, migration_config.path_template, n, scatter);
         DIR *scatter_dir = opendir(scatter_path);
         struct dirent *obj_dent;
         while((obj_dent = readdir(scatter_dir)) != NULL) {
            // skip special libne files, ".", and ".."
            if(obj_dent->d_name[0] == '.' ||
               !fnmatch("*" REBUILD_SFX, obj_dent->d_name) ||
               !fnmatch("*" META_SFX, obj_dent->d_name)    ||
               !fnmatch("*" WRITE_SFX, obj_dent->d_name)) {
               continue;
            }
            char objid[MARFS_MAX_OBJID_SIZE];
            strncpy(objid, obj_dent->d_name, MARFS_MAX_OBJID_SIZE);
            unflatten_objid(objid);
            node_t *new_node = successor(ring, objid);
            if(strcmp(n, new_node->name)) {
               if(link_block(obj_dent->d_name, n, new_node->name, scatter)) {
                  fprintf(stderr, "failed to create symlink for object "
                          "%s (%s -> %s) in scatter %d\n",
                          obj_dent->d_name, new_node->name, n, scatter);
                  ret = -1;
               }
            }
         }
         closedir(scatter_dir);
      }
   }
   destroy_node_iterator(it);
   return ret;
}

int load_configuration(char *repo_name) {
      // load the marfs config and look up the repo.
   if(read_configuration() == -1) {
      fprintf(stderr, "failed to read marfs configuration\n");
      return -1;
   }
   if(validate_configuration() == -1) {
      fprintf(stderr, "failed to validate marfs configuration\n");
      return -1;
   }

   MarFS_Repo *repo = find_repo_by_name(repo_name);
   if(repo == NULL) {
      fprintf(stderr, "Could not find repo %s\n", repo_name);
      return -1;
   }
   DAL *dal = repo->dal;
   MC_Config *config = (MC_Config*)dal->global_state;
   migration_config.num_scatter = config->scatter_width;
   migration_config.ring = config->ring;
   return 0;
}

ring_t *get_ring(char *nodes) {
   char *node_list[256];
   char *n;
   int i;
   for(n = strtok(nodes, ","), i = 0;
       n && i < 256;
       n = strtok(NULL, ","), i++) {
      node_list[i] = n;
   }

   return new_ring((const char **)node_list, NULL, i);
}

void join(int argc, char **argv) {
   // The arguments for join are the repo, the capacity unit being
   // joined, and the path template
   if(argc != 5) {
      fprintf(stderr, "wrong number of arguments for join\n"
              "expected: join <repo name> <new capacity unit> "
              "<existing units (CSV)> <path template> <num scatter>\n");
      exit(-1);
   }

   migration_config.path_template = argv[3];
   migration_config.num_scatter = strtol(argv[4], NULL, 10);
#if 0
   // commented out. PA2X won't work on the storage nodes
   if(load_configuration(argv[0]) == -1) {
      exit(-1);
   }
#endif

   migration_config.ring = get_ring(argv[2]);

   if(ring_join(migration_config.ring, argv[1], 0, migrate_light) != 0) {
      fprintf(stderr, "ring_join failed\n");
      exit(-1);
   }
   else {
      printf("Successfully joined %s to the repo %s. "
             "To complete migration please run rebalance.\n",
             argv[1], argv[0]);
   }
}

static inline
void copy_fail(struct aiocb *aiocbs[], size_t len) {
   // cancel all outstanding aio ops
   // free all buffers
   // free all aiocbs
   aio_cancel(aiocbs[0]->aio_fildes, NULL);
   
   int i;
   for(i = 0; i < len; i++) {
      if(aio_error(aiocbs[i]) == -1 && errno == ECANCELED) {
         aio_return(aiocbs[i]); // need to clean up?
      }
      free((void *)aiocbs[i]->aio_buf);
      free(aiocbs[i]);
   }
}

int copy_data(int src_fd, int dest_fd) {
   int aiocb_index = 0;
   struct aiocb *aiocbs[256];
   bzero(aiocbs, 256 * sizeof(void *));

   unsigned char *buf = malloc(BUFF_SIZE);
   if(buf == NULL) {
      copy_fail(aiocbs, aiocb_index);
      return -1;
   }

   // read the source file and queue a bunch of async writes.
   size_t read_size;
   while((read_size = read(src_fd, buf, BUFF_SIZE)) != 0) {
      if(read_size == -1) {
         free(buf);
         copy_fail(aiocbs, aiocb_index);
         return -1;
      }
      aiocbs[aiocb_index] = malloc(sizeof(struct aiocb));
      if(aiocbs[aiocb_index] == NULL) {
         perror("malloc()");
         copy_fail(aiocbs, aiocb_index-1);
         exit(-1); // can't recover for being out of memory.
      }
      bzero(aiocbs[aiocb_index], sizeof(struct aiocb));
      aiocbs[aiocb_index]->aio_buf = buf;
      aiocbs[aiocb_index]->aio_fildes = dest_fd;
      aiocbs[aiocb_index]->aio_nbytes = read_size;
      
      if(aio_write(aiocbs[aiocb_index]) == -1) {
         perror("aio_write()");
         copy_fail(aiocbs, aiocb_index);
         free(aiocbs[aiocb_index]);
         free(buf);
         return -1;
      }
      buf = malloc(BUFF_SIZE);
      if(buf == NULL) {
         copy_fail(aiocbs, aiocb_index+1);
         return -1;
      }
      aiocb_index++;
   }
   free(buf); // the last buffer we allocate will be unused.
   // wait for the io to finish. Then free all the buffers.  Using a
   // busy wait, because that is the easiest. Can optimize later.
   int error = 0;
   int i;
   for(i = 0; i < aiocb_index; i++) {
      while(aio_error(aiocbs[i]) == EINPROGRESS) pthread_yield();
      if(aio_return(aiocbs[i]) < aiocbs[i]->aio_nbytes) {
         // not short circuiting. just return error after waiting for
         // all aios to finish.
         error == -1;
      }
      free(aiocbs[i]->aio_buf);
      free(aiocbs[i]);
   }
   return error;
}

void *rebalance_worker(void *arg) {
   while( 1 ) {
      int error = 0;
      // wait for stuff to be in the queue.
      pthread_mutex_lock(&work_queue.lock);

      while(work_queue.size == 0 && work_queue.tail != -1) {
         pthread_cond_wait(&work_queue.full_cv, &work_queue.lock);
      }
      if(work_queue.size == 0 && work_queue.tail == -1) {
         pthread_mutex_unlock(&work_queue.lock);
         return NULL;
      }

      migration_spec_t spec = work_queue.queue[work_queue.head++];
      if(work_queue.head >= QUEUE_SIZE) {
         work_queue.head = 0; // wrap around.
      }
      work_queue.size--;

      // release locks.
      pthread_cond_signal(&work_queue.empty_cv);
      pthread_mutex_unlock(&work_queue.lock);
      
      // copy_data() it.
      int src_fd = open(spec.src, O_RDONLY);
      if(src_fd == -1) {
         fprintf(stderr, "src: %s - ", spec.src);
         perror("open()");
         free(spec.src);
         free(spec.dest);
         continue;
      }
      int dest_fd = open(spec.dest, O_CREAT|O_TRUNC|O_APPEND|O_WRONLY,
                         0666);
      if(dest_fd == -1) {
         fprintf(stderr, "dest: %s - ", spec.dest);
         perror("open()");
         free(spec.src);
         free(spec.dest);
         continue;
      }
      if(copy_data(src_fd, dest_fd) == -1) {
         fprintf(stderr, "failed to migrate %s\n", spec.src);
         error = 1;
      }

      close(src_fd);
      close(dest_fd);
      
      // do the rename of the destination.
      char dest_path[2048];
      strcpy(dest_path, spec.dest);
      dest_path[strlen(spec.dest) - strlen(".migrate")] = '\0';
      if(error == 0 && rename(spec.dest, dest_path) == -1) {
         fprintf(stderr, "failed to migrate %s -> %s\n",
                 spec.src, spec.dest);
         unlink(spec.dest); // clean up.
         error = 1;
      }

      // remove the original copy only if migration was successful
      if(error == 0) {
         unlink(spec.src);
      }
      
      // clean up the spec.
      free(spec.src);
      free(spec.dest);
   }
}

void start_rebalance_threads(int num_threads) {
   if(num_threads == 1) return;

   // initialize the queue structure
   pthread_mutex_init(&work_queue.lock, NULL);
   pthread_cond_init(&work_queue.full_cv, NULL);
   pthread_cond_init(&work_queue.empty_cv, NULL);

   work_queue.head = 0;
   work_queue.tail = 0;
   work_queue.size = 0;

   // spin up threads.
   int i;
   for(i = 0; i < num_threads; i++) {
      if(pthread_create(&(work_queue.threads[i]), NULL, rebalance_worker, NULL) == -1) {
         fprintf(stderr, "failed to start threads\n");
         exit(-1);
      }
   }
}

void rebalance(int argc, char **argv) {
   // Args: repo, path to capacity unit being rebalanced (with scatter dir template).
   // getopt for thread count.
   int num_threads = 1;

   if(!strncmp(argv[0], "-t", 2) && argc >= 2) {
      // get the thread count
      num_threads = strtol(argv[1], NULL, 10);
      if(num_threads <= 0 || num_threads >= MAX_THREADS) {
         fprintf(stderr, "num_threads must be between 1 and %d\n", MAX_THREADS);
         exit(-1);
      }
      argv += 2;
      argc -= 2;
   }
   else if(!strncmp(argv[0], "-h", 2)) {
      fprintf(stderr,
              "rebalance [-t <num threads>] <cap to rebalance> <num scatter>\n");
      exit(-1);      
   }

   if(argc != 2) {
      fprintf(stderr, "wrong number of arguments for rebalance\n");
      fprintf(stderr,
              "rebalance [-t <num threads>] <cap to rebalance> <num scatter>\n");
      exit(-1);
   }

   migration_config.path_template = argv[0];
   migration_config.num_scatter = strtol(argv[1], NULL, 10);

   // start some threads.
   start_rebalance_threads(num_threads);

   // open each scatter dir. readdir-stat looking for symlinks.
   int i;
   for(i = 0; i < migration_config.num_scatter; i++) {
      char scatter_path[2048];
      sprintf(scatter_path, migration_config.path_template, i);
      DIR *scatter = opendir(scatter_path);
      if(scatter == NULL) {
         fprintf(stderr, "could not open scatter dir %s: %s\n",
                 scatter_path, strerror(errno));
         exit(-1);
      }
      struct dirent *obj_dent;
      while((obj_dent = readdir(scatter)) != NULL) {
         if(obj_dent->d_name[0] == '.') {
            continue;
         }
         struct stat obj_stat;

         if(fstatat(dirfd(scatter), obj_dent->d_name,
                    &obj_stat, AT_SYMLINK_NOFOLLOW) == -1) {
            fprintf(stderr, "could not stat %s\n", obj_dent->d_name);
            continue;
         }

         if(!S_ISLNK(obj_stat.st_mode)) {
            // only moving links
            continue;
         }

         // readlink(), and copy the data to a temp file.
         // sync() the temp file.
         // rename it over the symlink.
         char src_path[2048];
         ssize_t path_len = 0;
         if((path_len = readlinkat(dirfd(scatter),
                                   obj_dent->d_name,
                                   src_path, sizeof(src_path))) == -1) {
            perror("readlink()");
         }
         src_path[path_len] = '\0'; // readlink does not null-terminate

         char dest_path[2048];
         sprintf(dest_path, "%s.%s", obj_dent->d_name, "migrate");

         if(num_threads == 1) {
            int src_fd = open(src_path, O_RDONLY);
            if(src_fd == -1) {
               perror("failed to open src file");
               continue;
            }
            int dest_fd = openat(dirfd(scatter), dest_path,
                                 O_WRONLY|O_TRUNC|O_CREAT|O_APPEND,
                                 0666);
            if(dest_fd == -1) {
               close(src_fd);
               perror("failed to open dest file");
               continue;
            }

            if(copy_data(src_fd, dest_fd) == -1) {
               close(src_fd);
               close(dest_fd);
               unlinkat(dirfd(scatter), dest_path, 0);
               fprintf(stderr, "failed to copy %s\n", obj_dent->d_name);
               continue;
            }

            close(src_fd);
            close(dest_fd);

            if(renameat(dirfd(scatter), dest_path,
                        dirfd(scatter), obj_dent->d_name) == -1) {
               perror("rename()");
               unlinkat(dirfd(scatter), dest_path, 0);
               continue;
            }

            if(unlink(src_path) == -1) {
               fprintf(stderr, "WARNING - failed to unlink source: %s (%s)\n",
                       src_path, strerror(errno));
               continue;
            }
         }
         else {
            // build the two full paths, and queue them for a worker
            // thread to handle.
            char src_full[2048];
            char dest_full[2048];
            // src path is the current location of the data.  the link
            // is relative to the scatter path, so the following
            // should produce something like:
            //
            // /repo/pod*/block*/CapFoo/scatterX/../../CapFoo/scatterX/object
            sprintf(dest_full, "%s/%s", scatter_path, dest_path);

            pthread_mutex_lock(&work_queue.lock);
            while(work_queue.size == QUEUE_SIZE) {
               pthread_cond_wait(&work_queue.empty_cv, &work_queue.lock);
            }
            work_queue.queue[work_queue.tail].src = strdup(src_path);
            if(work_queue.queue[work_queue.tail].src == NULL) {
               perror("strdup()");
               pthread_mutex_unlock(&work_queue.lock);
               continue;
            }
            work_queue.queue[work_queue.tail].dest = strdup(dest_full);
            if(work_queue.queue[work_queue.tail].dest == NULL) {
               perror("strdup()");
               free(work_queue.queue[work_queue.tail].src);
               pthread_mutex_unlock(&work_queue.lock);
               continue;
            }
            work_queue.tail++;
            work_queue.size++;
            if(work_queue.tail >= QUEUE_SIZE) {
               work_queue.tail = 0;
            }
            pthread_cond_signal(&work_queue.full_cv);
            pthread_mutex_unlock(&work_queue.lock);
         }
      }
      closedir(scatter);
   }

   if(num_threads > 1) {
      pthread_mutex_lock(&work_queue.lock);
      work_queue.tail = -1; // signal that we are done.
      pthread_cond_broadcast(&work_queue.full_cv);
      pthread_mutex_unlock(&work_queue.lock);

      // join on the threads.
      for(i = 0; i < num_threads; i++) {
         pthread_join(work_queue.threads[i], NULL);
      }
   }
}

void usage() {
   printf("no usage\n");
}

int main(int argc, char **argv) {

   if(argc < 2) {
      fprintf(stderr, "too few arguments\n");
      exit(-1);
   }

   // the first argument is the command : "join" or "leave" or "rebalance"
   if(strcmp(argv[1], "join") == 0) {
      join(argc - 2, argv + 2);
   }
   else if(strcmp(argv[1], "leave") == 0) {
      // TODO
      fprintf(stderr, "leave not implemented\n");
      exit(-1);
   }
   else if(strcmp(argv[1], "rebalance") == 0) {
      rebalance(argc - 2, argv + 2);
   }
   else {
      fprintf(stderr, "invalid option: %s\n", argv[1]);
      usage(argv[0]);
      return -1;
   }

   return 0;
}
