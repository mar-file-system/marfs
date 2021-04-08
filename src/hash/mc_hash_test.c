#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/time.h>
#include <assert.h>

#include "mc_hash.h"

#define HISTOGRAMS

#ifdef HISTOGRAMS
#define HISTOGRAM_STRING                                                        \
   "##########################################################################" \
   "##########################################################################" \
   "##########################################################################" \
   "##########################################################################" \
   "##########################################################################" \
   "##########################################################################" \
   "##########################################################################" \
   "##########################################################################" \
   "##########################################################################" \
   "##########################################################################" \
   "##########################################################################" \
   "########################################################################->"
#endif // HISTOGRAMS

double chi_squared(unsigned long *counts, double *expected_distribution,
                   size_t len, unsigned long num_samples) {
   const static double critical_values[31][10] = {
      {0.995, 0.990, 0.975, 0.950, 0.90, 0.10, 0.050, 0.025, 0.010, 0.005},
      {0.000, 0.000, 0.001, 0.004, 0.016, 2.706, 3.841, 5.024, 6.635, 7.879},
      {0.010, 0.020, 0.051, 0.103, 0.211, 4.605, 5.991, 7.378, 9.210, 10.597},
      {0.072, 0.115, 0.216, 0.352, 0.584, 6.251, 7.815, 9.348, 11.345, 12.838},
      {0.207, 0.297, 0.484, 0.711, 1.064, 7.779, 9.488, 11.143, 13.277, 14.860},
      {0.412, 0.554, 0.831, 1.145, 1.610, 9.236, 11.070, 12.833, 15.086, 16.750},
      {0.676, 0.872, 1.237, 1.635, 2.204, 10.645, 12.592, 14.449, 16.812, 18.548},
      {0.989, 1.239, 1.690, 2.167, 2.833, 12.017, 14.067, 16.013, 18.475, 20.278},
      {1.344, 1.646, 2.180, 2.733, 3.490, 13.362, 15.507, 17.535, 20.090, 21.955},
      {1.735, 2.088, 2.700, 3.325, 4.168, 14.684, 16.919, 19.023, 21.666, 23.589},
      {2.156, 2.558, 3.247, 3.940, 4.865, 15.987, 18.307, 20.483, 23.209, 25.188},
      {2.603, 3.053, 3.816, 4.575, 5.578, 17.275, 19.675, 21.920, 24.725, 26.757},
      {3.074, 3.571, 4.404, 5.226, 6.304, 18.549, 21.026, 23.337, 26.217, 28.300},
      {3.565, 4.107, 5.009, 5.892, 7.042, 19.812, 22.362, 24.736, 27.688, 29.819},
      {4.075, 4.660, 5.629, 6.571, 7.790, 21.064, 23.685, 26.119, 29.141, 31.319},
      {4.601, 5.229, 6.262, 7.261, 8.547, 22.307, 24.996, 27.488, 30.578, 32.801},
      {5.142, 5.812, 6.908, 7.962, 9.312, 23.542, 26.296, 28.845, 32.000, 34.267},
      {5.697, 6.408, 7.564, 8.672, 10.085, 24.769, 27.587, 30.191, 33.409, 35.718},
      {6.265, 7.015, 8.231, 9.390, 10.865, 25.989, 28.869, 31.526, 34.805, 37.156},
      {6.844, 7.633, 8.907, 10.117, 11.651, 27.204, 30.144, 32.852, 36.191, 38.582},
      {7.434, 8.260, 9.591, 10.851, 12.443, 28.412, 31.410, 34.170, 37.566, 39.997},
      {8.034, 8.897, 10.283, 11.591, 13.240, 29.615, 32.671, 35.479, 38.932, 41.401},
      {8.643, 9.542, 10.982, 12.338, 14.041, 30.813, 33.924, 36.781, 40.289, 42.796},
      {9.260, 10.196, 11.689, 13.091, 14.848, 32.007, 35.172, 38.076, 41.638, 44.181},
      {9.886, 10.856, 12.401, 13.848, 15.659, 33.196, 36.415, 39.364, 42.980, 45.559},
      {10.520, 11.524, 13.120, 14.611, 16.473, 34.382, 37.652, 40.646, 44.314, 46.928},
      {11.160, 12.198, 13.844, 15.379, 17.292, 35.563, 38.885, 41.923, 45.642, 48.290},
      {11.808, 12.879, 14.573, 16.151, 18.114, 36.741, 40.113, 43.195, 46.963, 49.645},
      {12.461, 13.565, 15.308, 16.928, 18.939, 37.916, 41.337, 44.461, 48.278, 50.993},
      {13.121, 14.256, 16.047, 17.708, 19.768, 39.087, 42.557, 45.722, 49.588, 52.336},
      {13.787, 14.953, 16.791, 18.493, 20.599, 40.256, 43.773, 46.979, 50.892, 53.672},
   };

   double chi_sq = 0.0;
   int i;
   for(i = 0; i < len; i++) {
      double expected_value = expected_distribution == NULL ?
         (double)num_samples / len : expected_distribution[i] * num_samples;
      double term = ((counts[i] - expected_value)
                     * (counts[i] - expected_value)) / expected_value;
      chi_sq += term;
   }

   printf("chi squared: %f\n", chi_sq);

   if(len > 31) {
      fprintf(stderr, "ERROR: too many degrees of freedom. This function only works"
              " with up to 31 categories (30 degrees of freedom)\n");
      //errno = EINVAL;
      return 1.0;
   }
   else if(len == 1) {
      return 0.0;
   }

   for(i = 9; i >= 0; i--) {
      if(chi_sq < critical_values[len-1][i]) {
         return critical_values[0][i];
      }
   }

   return 1.0; // confidence too low.
}

static int test_successor() {
   int fail = 0;
#define MAX_CAP 32
   const char *nodes[MAX_CAP];
   int i;
   int n = random() % 9999;
   for(i = 0; i < MAX_CAP; i++) {
      char key[64];
      sprintf(key, "ht10TB-PMR-%0.4d", n+i);
      nodes[i] = strdup(key);
   }

#define KEY_LEN 1024
   char key[KEY_LEN];
   const char *key_template =
      "mc/ver.001_006/ns.mc/N___/inode.%0.10d/mc_ctime.2016%0.2d%o.2d_%0.6d-0600_1/"
      "obj_ctime.20161102_123445-0600_1/unq.0/chnksz.20000000/chnkno.%d";

   int ring_size;
   long num_samples = 5000000;
   for(ring_size = 1; ring_size < MAX_CAP+1; ring_size *= 2) {
      unsigned long node_counts[MAX_CAP];
      memset(node_counts, '\0', MAX_CAP * sizeof(unsigned long));
      ring_t *r = new_ring(nodes, NULL, ring_size);
      int chunk = 0;
      int inode = 1234567;
      int time = 0;
      int day = 1;
      for(i = 0; i < num_samples; i++) {
         if((random() % 256) == 0) {
            inode = random() % 9999999999;
            day = (day + 1) % 30;
            chunk = 0;
         }
         time = (time + 1) % 999999;
         
         sprintf(key, key_template, inode, day, 3, time, chunk++);

         node_t *succ = successor(r, key);
         int j;

         // count how many keys go to each node.
         for(j = 0; j < ring_size; j++) {
            if(!strcmp(succ->name, nodes[j])) {
               node_counts[j]++;
               break;
            }
         }
      }

#ifdef HISTOGRAMS
      printf("distribution -> {\n");
      for(i = 0; i < ring_size; i++) {
         printf("\t%s : %.*s\n", nodes[i],
                (ring_size != 0 ? node_counts[i] - ((num_samples / ring_size) - 100000)
                 : node_counts[i])/1024, HISTOGRAM_STRING);
      }
      printf("}\n");
#endif
      printf("Chi-square test:\n");
      double p = chi_squared(node_counts, NULL, ring_size, num_samples);
      printf("p = %f\n", p);
      destroy_ring(r);
   }
}

void uneven_distribution() {

   const int ring_size = 16;
   const char *nodes[] = {"cap00", "cap01", "cap02", "cap03",
                          "cap04", "cap05", "cap06", "cap07", "cap08", "cap09",
                          "cap10",
                          "cap11", "cap12", "cap13", "cap14", "cap15"};
   const int weights[] = {8, 8, 8, 8, 10, 10, 10,
                          10, 6, 6, 6, 6, 6, 12, 12, 12};
   const int num_samples = 5250000;

   ring_t *r = new_ring(nodes, weights, ring_size);
   int i;
   char key[KEY_LEN];
   for(i = 0; i < KEY_LEN-1; i++) {
      key[i] = 'a';
   }
   key[KEY_LEN-1] = 0;
   unsigned long node_counts[] = {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0};
   for(i = 0; i < num_samples; i++) {
      node_t *succ = successor(r, key);
      int j;

      // count how many keys go to each node.
      for(j = 0; j < ring_size; j++) {
         if(!strcmp(succ->name, nodes[j])) {
            node_counts[j]++;
            break;
         }
      }

      // mutate the key for the next iteration.
      for(j = 0; j < 3; j++) {
         int index = random() % (KEY_LEN-1);
         key[index] = '!' + (random() % ('~' - '!'));
      }
   }
   destroy_ring(r);

#ifdef HISTOGRAMS
   printf("distribution -> {\n");
   for(i = 0; i < ring_size; i++) {
      printf("\t%s : %.*s\n", nodes[i], node_counts[i]/1024, HISTOGRAM_STRING);
   }
   printf("}\n");
#endif

   /// compute statistics:
   int total_weight = 0;
   for(i = 0; i < ring_size; i++) {
      total_weight += weights[i];
   }
   double ring_distribution[128];
   for(i = 0; i < ring_size; i++) {
      ring_distribution[i] = ((double)weights[i]/total_weight);
   }
   double p = chi_squared(node_counts, ring_distribution, ring_size, num_samples);
   printf("p = %f\n");
}

char *random_objid() {
   static unsigned int inode = 1234;
   if(((double)rand() / (double) RAND_MAX) < 0.01) {
      inode = random();
   }
   static char objid[1024];
   sprintf(objid,
           "bucket/ver.1_6/ns.foo/P___/inode.%010ld/md_ctime.%d/unq.0/chnksz.2000000/chnkno.%lu",
           inode++, random(), random(), 0);
   return objid;
}

void four_node_variance(int hdd_size /* HDD size in TB */) {
   const unsigned long capacity_per_node = 17*hdd_size; // in TB
   const unsigned long total_capacity = capacity_per_node * 4 * 10;
   const unsigned long objects_per_cap = capacity_per_node * 1024 * 1024 / 102;
   const unsigned long max_objects = objects_per_cap * 4;

   /* do it a bunch of times. */
   int iteration;
   for(iteration = 0; iteration < 10; iteration++) {
      int first_node = random() % 100;
      char *nodes[4];
      int n;
      for(n = 0; n < 4; n++) {
         char key[16];
         sprintf(key, "cap%d", first_node++);
         nodes[n] = strdup(key); // XXX memory leak
      }
      ring_t *ring = new_ring((const char **)nodes, NULL, 4);
      assert(ring != NULL);
      int i;
      unsigned long counts[4] = {0, 0, 0, 0};
      for(i = 0; i < max_objects * 0.95; i++) {
         node_t *succ = successor(ring, random_objid()); // TODO
         // count how many keys go to each node.
         int j;
         for(j = 0; j < 4; j++) {
            if(!strcmp(succ->name, nodes[j])) {
               counts[j]++;
               break;
            }
         }
      }
      destroy_ring(ring);
      // compute statistics.
      printf("%3d (%6d keys){ ", iteration, i);
      double percent_uesd[4] = {0.0,0.0,0.0,0.0};
      for(i = 0; i < 4; i++) {
         assert(counts[i] < objects_per_cap);
         printf("%2.4f ", (double)counts[i] / (double)objects_per_cap);
      }
      printf("}\n");
   }
}

#define ELAPSED_MILLIS(START, END)                                      \
   (((END).tv_sec * 1000 + (END).tv_usec / 1000)                        \
    - ((START).tv_sec * 1000 + (START).tv_usec / 1000))

#define ELAPSED_MICROS(START, END)                                      \
   (((uint64_t)(END).tv_sec * 1000000 + (uint64_t)(END).tv_usec)        \
    - ((uint64_t)(START).tv_sec * 1000000 + (uint64_t)(START).tv_usec))

void test_performance() {
   // ring creation performance.
   long num_iterations = 240;
   long i;
   const char *nodes[16] = {"cap0","cap1","cap2","cap3","cap4","cap5",
                            "cap6","cap7","cap8","cap9","cap10","cap11",
                            "cap12","cap13","cap14","cap15"};
   printf("node: %s\n", nodes[8]);
   int weights[16] = {8,8,8,8,10,10,10,10,10,10,10,10,16,16,16,16};
   long long elapsed_millis = 0;
   for(i = 0; i < num_iterations; i++) {
      struct timeval start, end;
      gettimeofday(&start, NULL);
      ring_t *r = new_ring(nodes, weights, 16);
      gettimeofday(&end, NULL);
      elapsed_millis += ELAPSED_MILLIS(start, end);
      destroy_ring(r);
   }
   printf("average time to create ring: %d ms\n", elapsed_millis / num_iterations);

   num_iterations = 100000;
   long long elapsed_micros = 0;
   ring_t *r = new_ring(nodes, weights, 16);
   char key[256];
   int iterations_per_key = 10;
   for(i = 0; i < num_iterations; i++) {
      struct timeval start, end;
      sprintf(key, "asdfjiowefawefg;aweijogawgweg%dafqwfwgea%0.5d",
              random(), random());
      int j;
      gettimeofday(&start, NULL);
      for(j = 0; j < iterations_per_key; j++) {
         successor(r, key); // drop the return value.
      }
      gettimeofday(&end, NULL);
      elapsed_micros += ELAPSED_MICROS(start, end);
   }
   destroy_ring(r);

   printf("average time to compute successor: %f us\n",
          (double)elapsed_micros / (num_iterations * iterations_per_key));
}

void test_node_list() {
   const char * name = "asdf";
   // 1. try to pop from an empty list.
   node_list_t *list1 = new_node_list();
   node_t *n = node_pop(list1);
   printf("test_node_list: pop from empty list: %s\n",
          n == NULL ? "SUCCESS!": "FAILURE!");
   destroy_node_list(list1);
   // 2. try to push to an empty list once and pop twice.
   //    ensure the the second pop returns NULL
   printf("test_node_list: push once, pop twice: ");
   node_list_t *list2 = new_node_list();
   node_t l2_n = { .name = name, .id = {0,0}, .ticket_number = 0};
   if( node_push(list2, &l2_n) == 0
       && node_pop(list2) == &l2_n
       && node_pop(list2) == NULL) {
      printf("SUCCESS!\n");
   }
   else {
      printf("FAILURE!\n");
   }
   destroy_node_list(list2);
   // 3. make iterator on empty list
   //    ensure iteration ends immedately
   node_list_t *list3 = new_node_list();
   node_iterator_t *it3 = node_iterator(list3);
   printf("test_node_list: empty iterator: %s\n",
          next_node(it3) == NULL ? "SUCCESS!" : "FAILURE!");
   destroy_node_iterator(it3);
   destroy_node_list(list3);
   // 4. make iterator on list with three nodes
   //    ensure the iteration ends after returning three things.
   const char *node_names[3] = {"one", "two", "three"};
   node_list_t *list4 = new_node_list();
   node_t nodes[3];
   int i;
   for(i = 0; i < 3; i++) {
      nodes[i].name = node_names[i];
      nodes[i].ticket_number = i;
      assert(node_push(list3, &nodes[i]) == 0);
   }
   node_iterator_t *it4 = node_iterator(list4);
   const char *n4;
   while((n4 = next_node(it4)) != NULL) {
      i--;
      assert(strcmp(n4, node_names[i]) == 0);
   }
   printf("test_node_list: three node iterator: %s\n",
          i == 0 ? "SUCCESS!" : "FAILURE!");
   destroy_node_iterator(it4);
   destroy_node_list(list4);

   // I will use the same node and list for all of the contains()
   // tests.
   node_t search_node = { .name = "foo", .id = {123,234}, .ticket_number = 10};
   node_list_t *list5 = new_node_list();
   printf("test_node_list: contains() on empty list: %s\n",
          contains(list5, &search_node) ? "FAILURE!" : "SUCCESS!");
   
   // 6. contains() returns false for a list that contains elements
   // other than the one we are looking for.
   assert(node_push(list5, &nodes[0]) == 0);
   assert(node_push(list5, &nodes[1]) == 0);
   assert(node_push(list5, &nodes[2]) == 0);
   printf("test_node_list: contains() should fail: %s\n",
          contains(list5, &search_node) ? "FAILURE!" : "SUCCESS!");

   // 7. contains() returns true for a list that contains what we are
   // looking for.
   assert(node_push(list5, &search_node) == 0);
   // a. The list contains the exact node we are searching for.
   assert(contains(list5, &search_node));
   // b. The list contains something with the same name.
   search_node.id[0] = 4584684;
   search_node.ticket_number = 23;
   assert(contains(list5, &search_node));
   printf("test_node_list: contains() succeeds: SUCCESS!\n");

   // 8. contains succeeds when the search element is not the head of
   // the list
   assert(contains(list5, &nodes[0]));
   assert(contains(list5, &nodes[1]));
   assert(contains(list5, &nodes[2]));
   printf("test_node_list: contains() for non-head: SUCCESS!\n");
   destroy_node_list(list5);

   // 9. Test list_length
   node_list_t *list9 = new_node_list();
   assert(list_length(list9) == 0);
   node_push(list9, &search_node);
   assert(list_length(list9) == 1);
   node_pop(list9);
   assert(list_length(list9) == 0);
   node_pop(list9);
   assert(list_length(list9) == 0);
   node_push(list9, &search_node);
   node_push(list9, &search_node);
   assert(list_length(list9) == 2);
   printf("test_node_list: list_length(): SUCCESS!\n");
   destroy_node_list(list9);
}

static int check_ring(ring_t *ring, const char *target) {
   int iterations = 0;
   char key[50] = "asdfjiwoaefawaegawgaoiwegsohawegnagiuwaewjoawiejf0";
   while(strcmp(successor(ring, key)->name, target)) {
      iterations++;
      // I should definitely get the new node in less than 50? right?
      if(iterations >= 100) {
         return 0;
      }
      key[random() % 50] = '!' + random() % ('~' - '!');
   }
   return 1;
}

// Stubs that represent successful and unsuccessful migration.
int stub_migrate_success(node_list_t *from, ring_t *new_ring) { return  0; }
int stub_migrate_failure(node_list_t *from, ring_t *new_ring) { return -1; }

void get_counts(ring_t *ring, const char **nodes,
                unsigned int *counts, int size) {
   int i;
   for(i = 0; i < size; i++) {
      counts[i] = 0;
   }
   
   for(i = 0; i < 10000; i++) {
      char key[256];
      sprintf(key, "/.s,zdf;asdjweioefnavi %dagniow%d.%d",
              random(), random(), random());
      node_t *succ = successor(ring, key);
      int j;
      for(j = 0; j < 5; j++) {
         if(strcmp(succ->name, nodes[j]) == 0) {
            counts[j]++;
            break;
         }
      }
   }
}

int migrate_check_nodes(node_list_t *from, ring_t *new_ring) {
   node_iterator_t *it = node_iterator(from);
   const char *name;
   while((name = next_node(it)) != NULL) {
      assert(strcmp(name, "check_nodes") != 0);
   }
   return 0;
}

void test_ring_join() {
   const char *node_names[] = { "cap0", "cap1", "cap2", "cap3", "cap4",
                                "cap5", "cap6", "cap7", "cap8", "cap9" };
   // 1.1 Default weights - one node ring -> two nodes
   ring_t *ring1 = new_ring(node_names, NULL, 1);
   // How do I verify the new ring has the new node, and the new node
   // has its fair share of identifiers?
   //
   // A. verify that ring_join returns success.
   // B. verify that successor() works for the new ring.
   // C. verify that successor() will return the new node.
   assert(ring_join(ring1, node_names[1], 0, stub_migrate_success) == 0);
   assert(successor(ring1, "key") != NULL); // the ring still works.
   assert(check_ring(ring1, node_names[1]));
   destroy_ring(ring1);

   // 1.2 Default weights - four node ring -> five nodes
   ring_t *ring2 = new_ring(node_names, NULL, 4);
   assert(ring_join(ring2, node_names[4], 0, stub_migrate_success) == 0);
   assert(check_ring(ring2, node_names[4]));
   destroy_ring(ring2);
   
   // 2. Default weights in original ring, new node has double weight
   ring_t *ring3 = new_ring(node_names, NULL, 4);
   assert(ring_join(ring3, node_names[4], 2, stub_migrate_success) == 0);
   unsigned int counts[5] = {0,0,0,0,0};
   int i;
   get_counts(ring3, node_names, counts, 5);
   for(i = 0; i < 4; i++) {
      assert(counts[i] < counts[4]);
   }
   destroy_ring(ring3);
   // 3. Non-default weights, uniform distribution.
   int weights[4] = {4,4,4,4};
   ring_t *ring4 = new_ring(node_names, weights, 4);
   assert(ring_join(ring4, node_names[4], 4, stub_migrate_success) == 0);
   destroy_ring(ring4);
   // 4. Non-default weights, new node has double weight
   ring_t *ring5 = new_ring(node_names, weights, 4);
   assert(ring_join(ring5, node_names[4], 8, stub_migrate_success) == 0);
   get_counts(ring5, node_names, counts, 5);
   for(i = 0; i < 4; i++) {
      assert(counts[i] < counts[4]);
   }
   destroy_ring(ring5);
   // 5. Non-default weights, new node has half weight
   ring_t *ring6 = new_ring(node_names, weights, 4);
   assert(ring_join(ring6, node_names[4], 2, stub_migrate_success) == 0);
   get_counts(ring6, node_names, counts, 5);
   for(i = 0; i < 4; i++) {
      assert(counts[i] > counts[4]);
   }
   destroy_ring(ring6);
   // 6. migration failure causes join failure
   ring_t *ring7 = new_ring(node_names, NULL, 6);
   assert(ring_join(ring7, node_names[6], 0, stub_migrate_failure) == -1);
   assert(check_ring(ring7, node_names[6]) == 0);
   assert(ring_join(ring7, node_names[6], 0, stub_migrate_success) == 0);
   destroy_ring(ring7);
   // 7. previous nodes are still present after join
   ring_t *ring8 = new_ring(node_names, NULL, 2);
   assert(ring_join(ring8, node_names[2], 0, stub_migrate_success) == 0);
   assert(check_ring(ring8, node_names[0]));
   assert(check_ring(ring8, node_names[1]));
   destroy_ring(ring8);
   // 8. the node list passed to the migration function should not
   // include the node being joined.
   ring_t *ring9 = new_ring(node_names, NULL, 9);
   assert(ring_join(ring9, "check_nodes", 0, migrate_check_nodes) == 0);
   destroy_ring(ring9);
   
   printf("test_ring_join: SUCCESS!\n"); 
}

int migrate_test_leave(node_list_t *from, ring_t *new_ring) {
   node_iterator_t *it = node_iterator(from);
   const char *n = next_node(it);
   assert(strcmp(n, "baz") == 0);
   assert(next_node(it) == NULL);
   destroy_node_iterator(it);
   return 0;
}

void test_ring_leave() {
   const char *nodes[4] = { "foo", "bar", "baz", "biz"};
   // 1. ring_leave fails for ring with one node.
   ring_t *ring1 = new_ring(nodes, NULL, 1);
   assert(ring_leave(ring1, nodes[0], stub_migrate_success) == -1);
   assert(check_ring(ring1, nodes[0]));
   destroy_ring(ring1);
   // 2. ring_leave produces a working ring that never returns the
   // node that left the ring.
   ring_t *ring2 = new_ring(nodes, NULL, 2);
   assert(ring_leave(ring2, nodes[0], stub_migrate_success) == 0);
   assert(check_ring(ring2, nodes[0]) == 0);
   destroy_ring(ring2);
   // 3. migration failure results in leave failure
   ring_t *ring3 = new_ring(nodes, NULL, 2);
   assert(ring_leave(ring3, nodes[1], stub_migrate_failure) == -1);
   // both nodes should still exist
   assert(check_ring(ring3, nodes[0]));
   assert(check_ring(ring3, nodes[1]));
   destroy_ring(ring3);
   // 4. migration function gets a list with _only_ the node that is
   // leaving.
   ring_t *ring4 = new_ring(nodes, NULL, 4);
   assert(ring_leave(ring4, nodes[2], migrate_test_leave) == 0);
   destroy_ring(ring4);

   printf("test_ring_leave: SUCCESS!\n");
}

int migrate_test(node_list_t *from, ring_t *new_ring) {
   // check that the list contains at least one node.
   // check the the new ring is functional (ie. can call successor on it.)?
   int i = 0;
   node_iterator_t *it = node_iterator(from);
   while(next_node(it) != NULL) {
      i++;
   }
   destroy_node_iterator(it);
   assert(i >= 1);
   return 0;
}

void test_migration() {
   const char *nodes[] = {"foo", "bar", "baz"};
   const char *new_node = "biz";
   ring_t *ring = new_ring(nodes, NULL, 3);
   assert(ring != NULL);
   assert(ring_join(ring, new_node, 0, migrate_test) == 0);
   destroy_ring(ring);
   printf("test_migration: SUCCESS!\n");
}

void test_successor_iterator() {
   // mock up a ring and an iterator for basic functionality tests.
   char *nodes[9]   = {"foo", "bar", "baz", "biz", "fiz", "siz", "niz", "riz", "roo"};
   int  weights[3]  = {1, 1, 1};
   node_t vnodes[9] = {
      {
         .name = nodes[0],
         .id = {0,0},
         .ticket_number = 0,
      },
      {
         .name = nodes[0],
         .id = {0,1},
         .ticket_number = 1,
      },
      {
         .name = nodes[0],
         .id = {0,2},
         .ticket_number = 2,
      },
      {
         .name = nodes[1],
         .id = {1,0},
         .ticket_number = 0,
      },
      {
         .name = nodes[1],
         .id = {1,1},
         .ticket_number = 1,
      },
      {
         .name = nodes[1],
         .id = {1,2},
         .ticket_number = 2,
      },
      {
         .name = nodes[2],
         .id = {2,0},
         .ticket_number = 0,
      },
      {
         .name = nodes[2],
         .id = {2,1},
         .ticket_number = 1,
      },
      {
         .name = nodes[2],
         .id = {2,2},
         .ticket_number = 2,
      }
   };
   ring_t mock_ring = {
      .num_nodes = 3,
      .weights = weights,
      .nodes = nodes,
      .total_tickets = 9,
      .virtual_nodes = vnodes
   };

   successor_it_t mock_iterator = {
      .visited = new_node_list(),
      .position = &vnodes[2],
      .ring = &mock_ring,
      .start_index = 2 //???
   };

   node_t *n;
   n = next_successor(&mock_iterator);
   assert(n != NULL);
   assert(strcmp(n->name, "foo") == 0);
   n = next_successor(&mock_iterator);
   assert(n != NULL);
   assert(strcmp(n->name, "bar") == 0);
   n = next_successor(&mock_iterator);
   assert(n != NULL);
   assert(strcmp(n->name, "baz") == 0);
   assert(next_successor(&mock_iterator) == NULL);
   assert(next_successor(&mock_iterator) == NULL);

   // TODO:
   // can test the start index by asserting that after creation,
   // &vnodes[start_index] == position
   ring_t *ring = new_ring(nodes, NULL, 9);
   successor_it_t *it = successor_iterator(ring, "key");
   assert(&it->ring->virtual_nodes[it->start_index] == it->position);
   int count_nodes = 0;
   while(next_successor(it) != NULL) {
      count_nodes++;
   }
   assert(count_nodes == 9);
   printf("test_successor_iterator: SUCCESS!\n");
}

/**
 * Compute the total utilization once one capacity unit is at 100%
 * utilization for many different ring sizes, increasing the ring size
 * by adding uniform capacity.
 */
void uniform_utilization() {
   // generate random node names
   char *nodes[128];
   int i, n;
   n = random();
   for(i = 0; i < 128; i++) {
      char name[32];
      sprintf(name, "cap%d", n++);
      nodes[i] = strdup(name);
   }
   
   // make a 4/8/16/32/64 node ring with uniform weight
   for(i = 4; i <= 64; i *= 2) {
      unsigned long counts[128];
      memset(counts, '\0', 128 * sizeof(unsigned long));
      ring_t *ring = new_ring(nodes, NULL, i);
      // fill the ring. But make a simplifying assumption that the
      // capacity is 1 millon objects per capacity unit.
      uint64_t j;
      unsigned long max_count = 0;
      for(j = 0; max_count < 1000000; j++) {
         node_t *n = successor(ring, random_objid());
         int k;
         for(k = 0; k < i; k++) {
            if(strcmp(n->name, nodes[k]) == 0) {
               counts[k]++;
               if(counts[k] > max_count) {
                  max_count = counts[k];
               }
            }
         }
      }
      unsigned long long total_count = 0;
      for(j = 0; j < i; j++) {
         total_count += counts[j];
      }
      printf("Utilization-uniform (%d): %f\n", i, (double)total_count/(double)(i * 1000000));
      destroy_ring(ring);
   }
}

int not_full(unsigned long *counts, int *weights, size_t len) {
   int i;
   for(i = 0; i < len; i++) {
      if(counts[i] >= weights[i] * 1000000L) {
         return 0;
      }
   }
   return 1;
}

void exponential_utilization() {
   int weights[28] = {1,1,1,1,
                      2,2,2,2,
                      4,4,4,4,
                      8,8,8,8,
                      16,16,16,16,
                      32,32,32,32,
                      64,64,64,64};
   char *nodes[28];
   int i, n;
   n = random();
   for(i = 0; i < 28; i++) {
      char name[32];
      sprintf(name, "cap%d", n++);
      nodes[i] = strdup(name);
   }

   for(i = 4; i <= 28; i+=4) {
      unsigned long counts[32];
      memset(counts, '\0', 32 * sizeof(unsigned long));
      ring_t *ring = new_ring(nodes, weights, i);
      // fill the ring. But make a simplifying assumption that the
      // capacity is 1 millon objects per capacity unit.
      uint64_t j;
      for(j = 0; not_full(counts, weights, i); j++) {
         node_t *n = successor(ring, random_objid());
         int k;
         for(k = 0; k < i; k++) {
            if(strcmp(n->name, nodes[k]) == 0) {
               counts[k]++;
            }
         }
      }
      unsigned long long total_count = 0;
      uint64_t max_cap = 0;
      for(j = 0; j < i; j++) {
         total_count += counts[j];
         max_cap += weights[j] * 1000000;
      }
      printf("Utilization-exponential (%d): %f\n", i, (double)total_count/(double)max_cap);
      destroy_ring(ring);
   }
}

int main(int argc, char **argv) {
   if(argc > 1) {
      srandom(strtol(argv[1], NULL, 10));
      exponential_utilization();
      uniform_utilization();
      return 0;
   }
   test_successor();
   test_node_list();
   test_ring_join();
   test_migration();
   test_ring_leave();
   test_successor_iterator();
   four_node_variance(6);
   test_performance();
   uneven_distribution();

   return 0;
}
