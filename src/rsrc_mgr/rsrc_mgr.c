#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <dirent.h>
#include <time.h>
#include <math.h>
#include <sys/types.h>
#include <attr/xattr.h>

#include "config/config.h"
#include "mdal/mdal.h"
#include "tagging/tagging.h"
#include "datastream/datastream.h"

#define RECENT_THRESH 0

#ifndef ENOATTR
#define ENOATTR ENODATA
#endif

#define N_SKIP "gc_skip"
#define IN_PROG "IN_PROG"

typedef struct stream_data_struct {
  size_t size;
  size_t count;
} stream_data;
/**
 * @param head_ref keep
 * @param tail_ref keep
 * @param first_obj keep
 * @param curr_obj keep
 * @param end flags
 * @return int
 */
int gc(const marfs_ms* ms, const marfs_ds* ds, MDAL_CTXT ctxt, FTAG ftag, int head_ref, int tail_ref, int first_obj, int curr_obj, int eos) {
  printf("gc: refs %d:%d objs %d:%d\n", head_ref, tail_ref, first_obj, curr_obj);

  if(tail_ref <= head_ref + 1) {
    return 0;
  }

  int skip = -1;
  if (!eos) {
    if (head_ref < 0) {
    head_ref = 0;
    }
    skip = tail_ref - head_ref - 1;
  }

  // determine if the specified refs/objs have already been deleted
  char* rpath;
  ftag.fileno = head_ref;
  if(head_ref < 0) {
    ftag.fileno = 0;
  }
  if ((rpath = datastream_genrpath(&ftag, ms)) == NULL) {
    fprintf(stderr, "Failed to create rpath\n");
    return -1;
  }

  MDAL_FHANDLE handle;
  if ((handle = ms->mdal->openref(ctxt, rpath, O_RDWR, 0)) == NULL) {
    fprintf(stderr, "Failed to open handle for reference path \"%s\"\n", rpath);
    free(rpath);
    return -1;
  }

  char* xattr = NULL;
  int xattr_len = ms->mdal->fgetxattr(handle, 1, N_SKIP, xattr, 0);
  if (xattr_len <= 0 && errno != ENOATTR) {
    fprintf(stderr, "Failed to retrieve xattr skip for reference path \"%s\"\n", rpath);
    ms->mdal->close(handle);
    free(rpath);
    return -1;
  }
  else if (xattr_len >= 0) {
    xattr = calloc(0, xattr_len + 1);
    if (xattr == NULL) {
      fprintf(stderr, "Failed to allocate space for a xattr string value\n");
      ms->mdal->close(handle);
      free(rpath);
      return -1;
    }

    if (ms->mdal->fgetxattr(handle, 1, N_SKIP, xattr, 0) != xattr_len) {
      fprintf(stderr, "xattr skip value for \"%s\" changed while reading\n", rpath);
      ms->mdal->close(handle);
      free(xattr);
      free(rpath);
      return -1;
    }

    // the same deletion has already been completed, no need to repeat
    if (strtol(xattr, NULL, 10) == skip) {
      printf("we don't have to repeat\n");
      ms->mdal->close(handle);
      free(rpath);
      return 0;
    }

    printf("free(xattr)\n");
    free(xattr);
  }

  int i;
  char* objname;
  ne_erasure erasure;
  ne_location location;
  for (i = first_obj + 1; i < curr_obj; i++) {
    ftag.objno = i;
    if (datastream_objtarget(&ftag, ds, &objname, &erasure, &location)) {
      fprintf(stderr, "Failed to generate object name\n");
      free(xattr);
      free(rpath);
      return -1;
    }

    printf("garbage collecting object %d %s\n", i, objname);
    if(ne_delete(ds->nectxt, objname, location) && errno != ENOENT) {
      fprintf(stderr, "Failed to delete \"%s\" (%s)\n", objname, strerror(errno));
      free(objname);
      free(xattr);
      free(rpath);
      return -1;
    }

    free(objname);
  }

  if (eos) {
    xattr_len = 4 + strlen(IN_PROG);
  }
  else {
    xattr_len = (int)log10(skip) + 3 + strlen(IN_PROG);
  }

  if ((xattr = malloc(sizeof(char) * xattr_len)) == NULL) {
    fprintf(stderr, "Failed to allocate xattr string\n");
    free(rpath);
    return -1;
  }

  if (snprintf(xattr, xattr_len, "%s %d", IN_PROG, skip) != (xattr_len - 1)) {
    fprintf(stderr, "Failed to populate rpath string\n");
    free(xattr);
    free(rpath);
    return -1;
  }

  if (ms->mdal->fsetxattr(handle, 1, N_SKIP, xattr, xattr_len, 0)) {
    fprintf(stderr, "Failed to set temporary xattr string\n");
    ms->mdal->close(handle);
    free(xattr);
    free(rpath);
    return -1;
  }

  xattr_len -= strlen(IN_PROG) + 1;
  xattr += strlen(IN_PROG) + 1;

  if (ms->mdal->close(handle)) {
    fprintf(stderr, "Failed to close handle\n");
    free(xattr);
    free(rpath);
    return -1;
  }

  // delete intermediate refs
  char* dpath;
  for (i = head_ref + 1; i < tail_ref; i++) {
    ftag.fileno = i;
    if ((dpath = datastream_genrpath(&ftag, ms)) == NULL) {
      fprintf(stderr, "Failed to create dpath\n");
      free(xattr - strlen(IN_PROG) - 1);
      free(rpath);
      return -1;
    }

    printf("garbage collecting ref %d\n", i);
    ms->mdal->unlinkref(ctxt, dpath);
    if (ms->mdal->unlinkref(ctxt, dpath) && errno != ENOENT){
      fprintf(stderr, "failed to unlink \"%s\" (%s)\n", dpath, strerror(errno));
      free(dpath);
      free(xattr - strlen(IN_PROG) - 1);
      free(rpath);
      return -1;
    }

    free(dpath);
  }

  // rewrite xattr
  if ((handle = ms->mdal->openref(ctxt, rpath, O_WRONLY, 0)) == NULL) {
    fprintf(stderr, "Failed to open handle for reference path \"%s\"\n", rpath);
    free(xattr - strlen(IN_PROG) - 1);
    free(rpath);
    return -1;
  }

  if (ms->mdal->fsetxattr(handle, 1, N_SKIP, xattr, xattr_len, 0)) {
    fprintf(stderr, "Failed to set temporary xattr string\n");
    ms->mdal->close(handle);
    free(xattr - strlen(IN_PROG) - 1);
    free(rpath);
    return -1;
  }


  if (ms->mdal->close(handle)) {
    fprintf(stderr, "Failed to close handle\n");
    free(xattr - strlen(IN_PROG) - 1);
    free(rpath);
    return -1;
  }

  free(xattr - strlen(IN_PROG) - 1);
  free(rpath);

  return 0;
}

int get_ftag(const marfs_ms* ms, const marfs_ds* ds, MDAL_CTXT ctxt, FTAG* ftag, const char* rpath) {
  MDAL_FHANDLE handle;
  if ((handle = ms->mdal->openref(ctxt, rpath, O_RDONLY, 0)) == NULL) {
    fprintf(stderr, "Failed to open handle for reference path \"%s\"\n", rpath);
    return -1;
  }

  char* ftagstr = NULL;
  int ftagsz;
  if ((ftagsz = ms->mdal->fgetxattr(handle, 1, FTAG_NAME, ftagstr, 0)) <= 0) {
    fprintf(stderr, "Failed to retrieve FTAG value for reference path \"%s\"\n", rpath);
    ms->mdal->close(handle);
    return -1;
  }

  ftagstr = calloc(1, ftagsz + 1);
  if (ftagstr == NULL) {
    fprintf(stderr, "Failed to allocate space for a FTAG string value\n");
    ms->mdal->close(handle);
    return -1;
  }

  if (ms->mdal->fgetxattr(handle, 1, FTAG_NAME, ftagstr, ftagsz) != ftagsz) {
    fprintf(stderr, "FTAG value for \"%s\" changed while reading \n", rpath);
    free(ftagstr);
    ms->mdal->close(handle);
    return -1;
  }

  char* skipstr = NULL;
  int skipsz;
  int skip = 0;
  if ((skipsz = ms->mdal->fgetxattr(handle, 1, N_SKIP, skipstr, 0)) > 0) {

    skipstr = calloc(0, skipsz + 1);
    if (skipstr == NULL) {
      fprintf(stderr, "Failed to allocate space for a skip string value\n");
      free(ftagstr);
      ms->mdal->close(handle);
      return -1;
    }

    if (ms->mdal->fgetxattr(handle, 1, N_SKIP, skipstr, skipsz) != skipsz) {
      fprintf(stderr, "skip value for \"%s\" changed while reading \n", rpath);
      free(skipstr);
      free(ftagstr);
      ms->mdal->close(handle);
      return -1;
    }

    char* end;
    errno = 0;
    skip = strtol(skipstr, &end, 10);
    if(errno) {
      fprintf(stderr, "failed to parse skip value for \"%s\" (%s)", rpath, strerror(errno));
      free(skipstr);
      free(ftagstr);
      ms->mdal->close(handle);
      return -1;
    }
    else if (end == skipstr) {
      int eos = 0;
      skip = strtol(skipstr + strlen(IN_PROG), &end, 10);
      if(errno || (end == skipstr) || (skip == 0)) {
        fprintf(stderr, "failed to parse skip value for \"%s\" (%s)", rpath, strerror(errno));
        free(skipstr);
        free(ftagstr);
        ms->mdal->close(handle);
        return -1;
      }

      if (skip < 0) {
        skip *= -1;
        eos = 1;
      }

      gc(ms, ds, ctxt, *ftag, ftag->fileno, ftag->fileno + skip + 1, -1, -1, eos);
    }

    free(skipstr);
  }

  if (ms->mdal->close(handle)) {
    fprintf(stderr, "Failed to close handle for reference path \"%s\"\n", rpath);
  }

  if (ftag_initstr(ftag, ftagstr)) {
    fprintf(stderr, "Failed to parse FTAG string for \"%s\"\n", rpath);
    free(ftagstr);
    return -1;
  }

  free(ftagstr);
  return skip + 1;
}

int end_obj(FTAG* ftag, size_t headerlen) {
  size_t dataperobj = ftag->objsize - (headerlen + ftag->recoverybytes);
  size_t finobjbounds = (ftag->bytes + ftag->offset - headerlen) / dataperobj;
  // special case check
  if ((ftag->state & FTAG_DATASTATE) >= FTAG_FIN  &&  finobjbounds  && (ftag->bytes + ftag->offset - headerlen) % dataperobj == 0 ) {
    // if we exactly align to object bounds AND the file is FINALIZED,
    //   we've overestimated by one object
    finobjbounds--;
  }

  return ftag->objno + finobjbounds;
}

int walk_stream(const marfs_ms* ms, const marfs_ds* ds, MDAL_CTXT ctxt, const char* node_name, const char* ref_name, stream_data* s_data) {
  s_data->size = 0;
  s_data->count = 0;

  MDAL mdal = ms->mdal;

  size_t rpath_len = strlen(node_name) + strlen(ref_name) + 1;
  char* rpath = malloc(sizeof(char) * rpath_len);
  if (rpath == NULL) {
    fprintf(stderr, "Failed to allocate rpath string\n");
    return -1;
  }

  if (snprintf(rpath, rpath_len, "%s%s", node_name, ref_name) != (rpath_len - 1)) {
    fprintf(stderr, "Failed to populate rpath string\n");
    free(rpath);
    return -1;
  }

  FTAG ftag;
  int next = get_ftag(ms, ds, ctxt, &ftag, rpath);
  if(next < 0) {
    fprintf(stderr, "Failed to retrieve FTAG for \"%s\"\n", rpath);
    free(rpath);
    return -1;
  }

  RECOVERY_HEADER header = {
    .majorversion = RECOVERY_CURRENT_MAJORVERSION,
    .minorversion = RECOVERY_CURRENT_MINORVERSION,
    .ctag = ftag.ctag,
    .streamid = ftag.streamid
  };
  size_t headerlen;
  if ((headerlen = recovery_headertostr(&(header), NULL, 0)) < 1) {
    fprintf(stderr, "Failed to identify recovery header length for final file\n");
    headerlen = 0;
  }

  struct stat st;
  int inactive = 0;
  int last_ref = -1;
  int last_obj = -1;
  while (ftag.endofstream == 0 && next > 0 && (ftag.state & FTAG_DATASTATE) >= FTAG_FIN) {
    if (mdal->statref(ctxt, rpath, &st)) {
      fprintf(stderr, "Failed to stat \"%s\" rpath\n", rpath);
      return -1;
    }

    if (difftime(time(NULL), st.st_ctime) > RECENT_THRESH) { // file is not young enough to be ignored
      if (st.st_nlink < 2) { // file has been deleted (needs to be gc'ed)
        printf("GC stream file %s\n", rpath);
        if (!inactive) {
          inactive = 1;
        }
      }
      else {
        if(inactive) {
          gc(ms, ds, ctxt, ftag, last_ref, ftag.fileno, last_obj, ftag.objno, 0);
        }
        inactive = 0;
        s_data->size += st.st_size;
        s_data->count++;
        last_ref = ftag.fileno;
        last_obj = end_obj(&ftag, headerlen);
      }
    }
    else {
      if(inactive) {
        gc(ms, ds, ctxt, ftag, last_ref, ftag.fileno, last_obj, ftag.objno, 0);
      }
      inactive = 0;
      last_ref = ftag.fileno;
      last_obj = end_obj(&ftag, headerlen);
    }

    printf("%lu %d\n", ftag.fileno, inactive);
    ftag.fileno += next;

    free(rpath);
    if ((rpath = datastream_genrpath(&ftag, ms)) == NULL) {
      fprintf(stderr, "Failed to create rpath\n");
      return -1;
    }


    if ((next = get_ftag(ms, ds, ctxt, &ftag, rpath)) < 0) {
      fprintf(stderr, "failed to retrieve ftag for %s\n", rpath);
      free(rpath);
      return -1;
    }
  }

  if (mdal->statref(ctxt, rpath, &st)) {
    fprintf(stderr, "Failed to stat \"%s\" rpath\n", rpath);
    free(rpath);
    return -1;
  }


  if (difftime(time(NULL), st.st_ctime) > RECENT_THRESH) {
    if (st.st_nlink < 2) {
      printf("GC stream file %s\n", rpath);

      gc(ms, ds, ctxt, ftag, last_ref, ftag.fileno + 1, last_obj, end_obj(&ftag, headerlen) + 1, 1);

      inactive = 1;
    }
    else {
      if (inactive) {
        gc(ms, ds, ctxt, ftag, last_ref, ftag.fileno, last_obj, ftag.objno, 0);
      }
      s_data->size += st.st_size;
      s_data->count++;
      inactive = 0;
    }
  }
  else if (inactive) {
    gc(ms, ds, ctxt, ftag, last_ref, ftag.fileno, last_obj, ftag.objno, 0);
    inactive = 0;
  }

  printf("%lu %d\n", ftag.fileno, inactive);

  printf("total stream: %lu %lu\n", s_data->count, s_data->size);

  free(rpath);

  return 0;
}

int ref_paths(const marfs_ns* ns) {
  marfs_ms* ms = &ns->prepo->metascheme;
  marfs_ds* ds = &ns->prepo->datascheme;

  char* ns_path;
  if(config_nsinfo(ns->idstr, NULL, &ns_path)) {
    fprintf(stderr, "Failed to retrieve path of NS: \"%s\"\n", ns->idstr);
    return -1;
  }

  MDAL_CTXT ctxt;
  if ((ctxt = ms->mdal->newctxt(ns_path, ms->mdal->ctxt)) == NULL) {
    fprintf(stderr, "Failed to create new MDAL context for NS: \"%s\"\n", ns_path);
    return -1;
  }

  HASH_NODE* ref_node = NULL;
  struct stat stbuf;
  MDAL_SCANNER scanner;
  struct dirent* dent;
  int i;
  stream_data s_data;
  size_t total_size = 0;
  size_t total_count = 0;
  for (i = 0; i < ms->refnodecount; i++) {
    ref_node = &ms->refnodes[i];
    if (ms->mdal->statref(ctxt, ref_node->name, &stbuf)) {
      fprintf(stderr, "failed to stat %s\n", ref_node->name);
    }
    else {
      scanner = ms->mdal->openscanner(ctxt, ref_node->name);
      while ((dent = ms->mdal->scan(scanner)) != NULL) {
        if (*dent->d_name != '.' && ftag_metatgt_fileno(dent->d_name) == 0) {
          printf("fileid %s has fileno 0\n", dent->d_name);
          if (walk_stream(ms, ds, ctxt, ref_node->name, dent->d_name, &s_data)) {
            fprintf(stderr, "failed to walk stream\n");
            return -1;
          }
          total_size += s_data.size;
          total_count += s_data.count;
        }
      }
      ms->mdal->closescanner(scanner);
    }
  }
  printf("namespace %s has total size (%lu) and count (%lu)\n", ns->idstr, total_size, total_count);
  if (ms->mdal->setdatausage(ctxt, total_size)) {
    fprintf(stderr, "failed to set data usage for namespace %s\n", ns->idstr);
  }
  if (ms->mdal->setinodeusage(ctxt, total_count)) {
    fprintf(stderr, "failed to set inode usage for namespace %s\n", ns->idstr);
  }

  ms->mdal->destroyctxt(ctxt);

  return 0;
}

int find_rank_ns(const marfs_ns* ns, int idx, int rank, int n_ranks, const char* name){
  if (idx % n_ranks == rank) {

    printf("%s\n", name);
    ref_paths(ns);

    char msg[1024];
    printf("%d %s\n", rank, name);
    snprintf(msg, 1024, "%d %s", rank, name);
    MPI_Send(msg, 1024, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
  }
  idx++;

  int i;
  for (i = 0; i < ns->subnodecount; i++) {
    idx = find_rank_ns(ns->subnodes[i].content, idx, rank, n_ranks, ns->subnodes[i].name);
  }

  return idx;
}

int main(int argc, char **argv) {
  errno = 0;

  if (MPI_Init(&argc, &argv) != MPI_SUCCESS)
    {
        fprintf(stderr, "Error in MPI_Init\n");
        return -1;
    }

  int n_ranks;
  if (MPI_Comm_size(MPI_COMM_WORLD, &n_ranks) != MPI_SUCCESS) {
    fprintf(stderr, "Error in MPI_Comm_size\n");
    return -1;
  }

  int rank;
  if (MPI_Comm_rank(MPI_COMM_WORLD, &rank) != MPI_SUCCESS) {
    fprintf(stderr, "Error in MPI_Comm_rank\n");
    return -1;
  }

  if ( argc < 2 || argv[1] == NULL ) {
    fprintf(stderr, "no config path defined\n");
    return -1;
  }

  marfs_config* cfg = config_init(argv[1]);
  if (cfg == NULL) {
    fprintf(stderr, "Failed to initialize config: %s %s\n", argv[1], strerror(errno));
    return -1;
  }

  if (config_verify(cfg, 1)) {
    fprintf(stderr, "Failed to verify config: %s\n", strerror(errno));
  }

  int total_ns = find_rank_ns(cfg->rootns, 0, rank, n_ranks, "root");

  int i;
  if (rank == 0) {
    char* msg = malloc(sizeof(char) * 1024);
    for (i = 0; i < total_ns; i++) {
      MPI_Recv(msg, 1024, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      printf("%s\n", msg);
    }
    free(msg);
  }

  MPI_Finalize();

  return 0;
}