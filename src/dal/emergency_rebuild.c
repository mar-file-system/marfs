/*
Copyright (c) 2015, Los Alamos National Security, LLC
All rights reserved.

Copyright 2015.  Los Alamos National Security, LLC. This software was
produced under U.S. Government contract DE-AC52-06NA25396 for Los
Alamos National Laboratory (LANL), which is operated by Los Alamos
National Security, LLC for the U.S. Department of Energy. The
U.S. Government has rights to use, reproduce, and distribute this
software.  NEITHER THE GOVERNMENT NOR LOS ALAMOS NATIONAL SECURITY,
LLC MAKES ANY WARRANTY, EXPRESS OR IMPLIED, OR ASSUMES ANY LIABILITY
FOR THE USE OF THIS SOFTWARE.  If software is modified to produce
derivative works, such modified software should be clearly marked, so
as not to confuse it with the version available from LANL.

Additionally, redistribution and use in source and binary forms, with
or without modification, are permitted provided that the following
conditions are met: 1. Redistributions of source code must retain the
above copyright notice, this list of conditions and the following
disclaimer.

2. Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in the
documentation and/or other materials provided with the distribution.
3. Neither the name of Los Alamos National Security, LLC, Los Alamos
National Laboratory, LANL, the U.S. Government, nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY LOS ALAMOS NATIONAL SECURITY, LLC AND
CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING,
BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL LOS
ALAMOS NATIONAL SECURITY, LLC OR CONTRIBUTORS BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER
IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

-----
NOTE:
-----
MarFS is released under the BSD license.

MarFS was reviewed and released by LANL under Los Alamos Computer Code
identifier: LA-CC-15-039.

MarFS uses libaws4c for Amazon S3 object communication. The original
version is at https://aws.amazon.com/code/Amazon-S3/2601 and under the
LGPL license.  LANL added functionality to the original work. The
original work plus LANL contributions is found at
https://github.com/jti-lanl/aws4c.

GNU licenses can be found at http://www.gnu.org/licenses/.
*/

#include "erasureUtils_auto_config.h"

// NOTE -- this tool is specific to the POSIX DAL implementation
#include "posix_dal.c"
#include "../thread_queue/thread_queue.h"

#include <ctype.h>
#include <errno.h>
#include <dirent.h>
#include <fcntl.h>
#include <mpi.h>
#include <pwd.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <libxml/tree.h>
#include <sys/stat.h>
#include <sys/types.h>

#define preFMT "%s: "

#define PROGNAME "emerg_reb"

#define PRINTout(FMT, ...) fprintf(stdout, preFMT FMT, PROGNAME, ##__VA_ARGS__)
#ifdef DEBUG
#define PRINTdbg(FMT, ...) fprintf(stdout, preFMT FMT, PROGNAME, ##__VA_ARGS__)
#else
#define PRINTdbg(...)
#endif

#ifndef LIBXML_TREE_ENABLED
#error "Included Libxml2 does not support tree functionality!"
#endif

// Compare and return the maximum value of a DAL_location field (pod/block/cap/scatter)
// between a target and distribution.
#define MAX_LOC(FIELD) ( tgt->FIELD[tgt->FIELD##_size - 1] > dist->FIELD[dist->FIELD##_size - 1] ? \
    tgt->FIELD[tgt->FIELD##_size - 1] : dist->FIELD[dist->FIELD##_size - 1])

// Name of status file created by phase 1 for phase 2
#define STATUS_FNAME "erasureUtils-emerg_reb.log"

// Suffix for temporary dirs during migration
#define TMPDIR_SFX ".tmp"

// Thread queue args
#define NUM_CONS 10
#define QDEPTH 100

/* NOTE: Apologies for the varying (and possibly unclear) terminology. I
 * understand that I use different terms to describe the same directories. Part
 * of the discrepancy comes from the data being moved in opposite directions at
 * different times in the overall rebuild process.Here's an effort to clarify.
 * Assume our config includes a dir_template of the form
 * "pod{p}/block{b}/cap{c}/scatter{s}", we are dealing with location
 * <p>/<b>/<c>/<s>, and this location hashes to location <P>/<B>/<C>/<S>. For
 * each location, we have up to three different associated directories:
 * - Real directory (pod<p>/block<b>/cap<c>/scatter<s>) The directory where
 * objects associated with this location actually belong. In an effort that one
 * (or several close) location(s) suffer a catastrophic failure that requires a
 * complete rebuild of that location(s), we are attempting to distribute the
 * load belonging to those location(s) across a larger number of locations, in
 * order to maximize the bandwidth MarFS' resource manager has available to
 * rebuild the failed locations, enabling the system to be restored to a
 * functional state and returned to the users sooner. To perform this
 * distribution, for each failed real directory we create a rebuild directory
 * underneath another unharmed location, and instead leave a symlink that points
 * to this rebuild directory in place of our real directory. The resource
 * manager will then write objects that correspond to our given location to the
 * rebuild directory under the other location. After all the data is rebuilt, we
 * will then migrate the data back from the rebuild directory to our real
 * directory before deleting the symlink and the rebuild directory once all the
 * data is back in its appropriate location. Other names I use:
 * rebuild target (posix_verify), pathname (posix_verify), link_path (reb_cons),
 * write_dir (only when reb_cons is recovering from a failure that occurs
 * between renaming the real directory and deleting the rebuild directory)
 * - Rebuild directory
 * (pod<p>/block<b>/cap<c>/scatter<s>/rebuild-<ts>/p<p>-b<b>-c<c>-s<s>) The
 * directory linked to by our real directory that will hold our rebuilt data
 * until it is able to be migrated back to the real directory. Once the
 * migration has been performed, this directory (and its parent, that indicates
 * which rebuild run this directory corresponds to) will be deleted. Other names
 * I use: write target (posix_verify), wtgt (posix_verify), reb_path (reb_cons),
 * read_dir (reb_cons)
 * - Temporary directory (pod<p>/block<b>/cap<c>/scatter<s>.tmp) To ensure that
 * the migration can happen while the system is online and the users can still
 * access their data during the migration, we read every file in the rebuild
 * directory and write it to an identical file in a temporary directory. Once
 * all data migration has occurred, the temporary directory is renamed to the
 * real directory, overwriting the symlink that existed there before. Only after
 * the rename has occurred and the users can start accessing the data in the
 * newly migrated real directory does the system attempt to delete the rebuild
 * directory and its contained files. Other names I use: (all in reb_cons)
 * tmp_path, write_dir (everywhere except the case described above)
 */

// So we can peform a single proc run without MPI
int rank = 0;
int n_ranks = 1;

enum mode {
    I_MODE = -1, // Invalid
    P_MODE = 1,  // Prepare for rebuild mode
    M_MODE = 2   // Migrate after rebuild mode
};

enum work_flag {
    WORK_LNK, // Path exists as link, perform full migrate
    WORK_DIR, // Path exists as dir, perform deletion
    WORK_NOENT // Path does not exist, perform rename and delete
};

// Structs for thread queue
typedef struct tq_global_struct {
    POSIX_DAL_CTXT dctxt;
} * GlobalState;

typedef struct tq_thread_struct {
    unsigned int tID;
    GlobalState global;
} * ThreadState;

typedef struct work_pkg_struct {
    char* link_path;
    enum work_flag flg;
    char* reb_path;
} * WorkPkg;

// Determine if a string is a suffix of another string
char* is_sfx(char* haystack, char* needle) {
    char* sfx = strstr(haystack, needle);
    if (sfx == NULL || sfx + strlen(needle) != haystack + strlen(haystack)) {
        return NULL;
    }

    return sfx;
}

// Thread queue thread state initialization behavior
int reb_thread_init(unsigned int tID, void* global_state, void** state) {
    *state = calloc(1, sizeof(struct tq_thread_struct));
    if (!*state) {
        return -1;
    }
    ThreadState tstate = ((ThreadState)*state);

    tstate->tID = tID;
    tstate->global = (GlobalState)global_state;

    return 0;
}

// Thread queue consumer thread behavior. Receives a path that symlinks to a rebuild
// directory, migrates all files within that directory to a directory that replaces
// the symlink, then cleans up the rebuild directory and its associated resources
int reb_cons(void** state, void** work_todo) {
    ThreadState tstate = ((ThreadState)*state);
    GlobalState gstate = tstate->global;
    WorkPkg work = ((WorkPkg)*work_todo);

    char reb_path_arr[256] = {0};
    char* reb_path = reb_path_arr;

    char tmp_path[256] = {0};

    struct stat st;
    struct dirent* dent;

    char* rblock_ptr = NULL;

    if (!work) {
        PRINTout("Thread %u received empty work package. Nothing to do!\n", tstate->tID);
        return 0;
    }

    // If we are given a link, find the rebuild directory, which is the target
    // it points to. Otherwise, the rebuild directory will be provided with the
    // work package
    if (work->flg == WORK_LNK) {
        // We should never take this branch. "link" work packages should not have
        // a rebuild path. Just an extra safety check
        if (work->reb_path) {
            free(work->reb_path);
        }

        if (readlinkat(gstate->dctxt->sec_root, work->link_path, reb_path, 255) < 0) {
            PRINTout("failed to read link\n");
            free(work->link_path);
            free(work);
            return -1;
        }
    }
    else if (work->reb_path) {
        if (strncpy(reb_path, work->reb_path, 255) != reb_path) {
            PRINTout("failed to copy rebuild path \"%s\" from work package\n", work->reb_path);
            free(work->reb_path);
            free(work->link_path);
            free(work);
            return -1;
        }
        free(work->reb_path);
    }
    else {
        PRINTout("\"directory\" and \"no entry\" work packages require a rebuild path\n");
        return -1;
    }

    // Strip any leading "../"s so the rebuild path is relative to the secure
    // root, not the symlink
    while(*reb_path == '.' || *reb_path == '/') {
        reb_path++;
    }

    // Open a file descriptor for the rebuild directory
    int read_dir = openat(gstate->dctxt->sec_root, reb_path, O_DIRECTORY);
    if (read_dir < 0) {
        // In the case where we are recovering from a failure that occurred
        // after the rebuild directory was already deleted, jump to the end
        // where we attempt to delete the rebuild directory's parent dir
        if (work->flg == WORK_DIR && errno == ENOENT) {
            goto del_dirs;
        }
        PRINTout("failed to open fd for %s\n", reb_path);
        free(work->link_path);
        free(work);
        return -1;
    }

    // Save the user/group/permissions so we can restore them after changing them
    if (fstat(read_dir, &st)) {
        PRINTout("failed to stat %s\n", reb_path);
        close(read_dir);
        free(work->link_path);
        free(work);
        return -1;
    }
    uid_t uid = st.st_uid;
    gid_t gid = st.st_gid;
    mode_t mode = st.st_mode;

    // Change the user/group/permissions to lock users out of writing files into
    // the rebuild directory as we read data out of it
    if (fchown(read_dir, getuid(), getgid())) {
        PRINTout("failed to chown %s to uid %d, gid %d\n", reb_path, getuid(), getgid());
        close(read_dir);
        free(work->link_path);
        free(work);
        return -1;
    }

    if (fchmod(read_dir, mode & ~S_IWOTH)) {
        PRINTout("failed to chmod %s\n", reb_path);
        (void)!fchown(read_dir, uid, gid);
        close(read_dir);
        free(work->link_path);
        free(work);
        return -1;
    }
    exit(-1);

    int write_dir;
    // If the real path is a directory, the temporary directory was already
    // renamed to the real path, so it does not exist and shouldn't be
    // recreated. Otherwise, we want to open a file descriptor for it
    if (work->flg != WORK_DIR) {
        if (strcpy(tmp_path, work->link_path) != tmp_path) {
            PRINTout("failed to duplicate link path\n");
            fchmod(read_dir, S_IRWXU);
            (void)!fchown(read_dir, uid, gid);
            close(read_dir);
            free(work->link_path);
            free(work);
        }

        if (strcat(tmp_path, TMPDIR_SFX) != tmp_path) {
            PRINTout("failed to append write suffix to path\n");
            fchmod(read_dir, S_IRWXU);
            (void)!fchown(read_dir, uid, gid);
            close(read_dir);
            free(work->link_path);
            free(work);
            return -1;
        }

        // We already eliminated the possibility of the real path being a
        // directory. If the real path does not exist, it means we failed
        // between destroying the symlink and renaming the temporary directory
        // to the real path. The temporary directory must already exist, so we
        // don't want to recreate it. If we have a link, we want to create the
        // temporary directory and ensure it has the right permissions
        if (work->flg == WORK_LNK) {
            if (mkdirat(gstate->dctxt->sec_root, tmp_path, mode) && errno != EEXIST) {
                PRINTout("failed to create temporary dir %s\n", tmp_path);
                fchmod(read_dir, S_IRWXU);
                (void)!fchown(read_dir, uid, gid);
                close(read_dir);
                free(work->link_path);
                free(work);
                return -1;
            }

            if(errno != EEXIST) {
                if (fchownat(gstate->dctxt->sec_root, tmp_path, uid, gid, 0)) {
                    PRINTout("failed to chown %s\n", tmp_path);
                    fchmod(read_dir, S_IRWXU);
                    (void)!fchown(read_dir, uid, gid);
                    close(read_dir);
                    free(work->link_path);
                    free(work);
                    return -1;
                }

                if (fchmodat(gstate->dctxt->sec_root, tmp_path, mode | S_IWOTH | S_IXOTH, 0)) {
                    PRINTout("failed to chmod %s\n", tmp_path);
                    fchmod(read_dir, S_IRWXU);
                    (void)!fchown(read_dir, uid, gid);
                    close(read_dir);
                    free(work->link_path);
                    free(work);
                    return -1;
                }
            }
        }

        if ((write_dir = openat(gstate->dctxt->sec_root, tmp_path, O_DIRECTORY)) < 0) {
            PRINTout("failed to open fd for %s\n", tmp_path);
            fchmod(read_dir, S_IRWXU);
            (void)!fchown(read_dir, uid, gid);
            close(read_dir);
            free(work->link_path);
            free(work);
            return -1;
        }
    }
    // Open a file descriptor for the real directory so we can compare the files
    // in the rebuild directory against the ones here before deletion
    else if ((write_dir = openat(gstate->dctxt->sec_root, work->link_path, O_DIRECTORY)) < 0) {
        PRINTout("failed to open fd for %s\n", work->link_path);
        fchmod(read_dir, S_IRWXU);
        (void)!fchown(read_dir, uid, gid);
        close(read_dir);
        free(work->link_path);
        free(work);
        return -1;
    }

    DIR* dirp;
    if ((dirp = fdopendir(read_dir)) == NULL) {
        PRINTout("failed to open directory stream for %s\n", reb_path);
        close(write_dir);
        fchmod(read_dir, S_IRWXU);
        (void)!fchown(read_dir, uid, gid);
        close(read_dir);
        free(work->link_path);
        free(work);
        return -1;
    }

    // If the real path isn't a symlink, the previous migration failed after
    // moving all the data into the temporary directory. If it is a symlink, we
    // still need to move this data
    if (work->flg == WORK_LNK) {
        struct timespec times[2];
        struct stat wst;
        int read_fd;
        int write_fd;
        int read_len;
        int write_len;
        char buf[IO_SIZE];
        void* buf_p;
        errno = 0;
        // Iterate through the files in the rebuild directory, creating
        // duplicates in the temporary directory
        while ((dent = readdir(dirp)) != NULL) {
            // Skip hidden files, and LibNE partial and rebuild files
            if (*dent->d_name == '.' || is_sfx(dent->d_name, WRITE_SFX) || is_sfx(dent->d_name, REBUILD_SFX)) {
                continue;
            }

            if (fstatat(read_dir, dent->d_name, &st, 0)) {
                PRINTout("failed to stat file %s/%s\n", reb_path, dent->d_name);
                fchmod(read_dir, S_IRWXU);
                (void)!fchown(read_dir, uid, gid);
                closedir(dirp);
                close(write_dir);
                free(work->link_path);
                free(work);
                return -1;
            }
            times[0].tv_sec = st.st_atime;
            times[1].tv_sec = st.st_mtime;

            // If a file in the temporary directory exists with the same name,
            // size, and mtime as a file in the rebuild directory, we will
            // assume they are the same
            if (!fstatat(write_dir, dent->d_name, &wst, 0) && st.st_mtime == wst.st_mtime && st.st_size == wst.st_size) {
                // This file was already migrated. We can skip it
                errno = 0;
                continue;
            }

            if ((read_fd = openat(read_dir, dent->d_name, O_RDONLY)) < 0) {
                PRINTout("failed to open file %s%s for read\n", reb_path, dent->d_name);
                fchmod(read_dir, S_IRWXU);
                (void)!fchown(read_dir, uid, gid);
                closedir(dirp);
                close(write_dir);
                free(work->link_path);
                free(work);
                return -1;
            }

            if ((write_fd = openat(write_dir, dent->d_name, O_WRONLY | O_CREAT | O_TRUNC, st.st_mode)) < 0) {
                PRINTout("failed to open file %s%s for write\n", tmp_path, dent->d_name);
                close(read_fd);
                fchmod(read_dir, S_IRWXU);
                (void)!fchown(read_dir, uid, gid);
                closedir(dirp);
                close(write_dir);
                free(work->link_path);
                free(work);
                return -1;
            }

            while((read_len = read(read_fd, buf, IO_SIZE)) > 0) {
                buf_p = buf;
                while(read_len > 0) {
                    if ((write_len = write(write_fd, buf_p, read_len)) < 0) {
                        PRINTout("failed to write to %s\n", dent->d_name);
                        close(write_fd);
                        unlinkat(write_dir, dent->d_name, 0);
                        close(read_fd);
                        fchmod(read_dir, S_IRWXU);
                        (void)!fchown(read_dir, uid, gid);
                        closedir(dirp);
                        close(write_dir);
                        free(work->link_path);
                        free(work);
                        return -1;
                    }
                    read_len -= write_len;
                    buf_p += write_len;
                }
            }
            if (read_len < 0) {
                PRINTout("failed to read from %s\n", dent->d_name);
                close(write_fd);
                unlinkat(write_dir, dent->d_name, 0);
                close(read_fd);
                fchmod(read_dir, S_IRWXU);
                (void)!fchown(read_dir, uid, gid);
                closedir(dirp);
                close(write_dir);
                free(work->link_path);
                free(work);
                return -1;
            }

            if (futimens(write_fd, times)) {
                PRINTout("failed to read from %s\n", dent->d_name);
                close(write_fd);
                unlinkat(write_dir, dent->d_name, 0);
                close(read_fd);
                fchmod(read_dir, S_IRWXU);
                (void)!fchown(read_dir, uid, gid);
                closedir(dirp);
                close(write_dir);
                free(work->link_path);
                free(work);
                return -1;
            }

            close(write_fd);
            close(read_fd);
            errno = 0;
        }
        if (errno != 0) {
            PRINTout("failed to read from directory %s\n", reb_path);
            fchmod(read_dir, S_IRWXU);
            (void)!fchown(read_dir, uid, gid);
            closedir(dirp);
            close(write_dir);
            free(work->link_path);
            free(work);
            return -1;
        }

        // We only need to remove the link if there is a link to make room for
        // the real directory
        if (unlinkat(gstate->dctxt->sec_root, work->link_path, 0)) {
            PRINTout("failed to unlink symlink %s (%s)\n", work->link_path, strerror(errno));
            fchmod(read_dir, S_IRWXU);
            (void)!fchown(read_dir, uid, gid);
            closedir(dirp);
            close(write_dir);
            free(work->link_path);
            free(work);
            return -1;
        }

        // We'll be iterating through the files in the rebuild directory again
        // for delete. Rewind the stream
        rewinddir(dirp);
    }

    // Rename the temporary directory over the link path
    if (work->flg != WORK_DIR) {
        if (renameat(gstate->dctxt->sec_root, tmp_path, gstate->dctxt->sec_root, work->link_path)) {
            PRINTout("failed to rename %s to %s (%s)\n", tmp_path, work->link_path, strerror(errno));
            fchmod(read_dir, S_IRWXU);
            (void)!fchown(read_dir, uid, gid);
            closedir(dirp);
            close(write_dir);
            free(work->link_path);
            free(work);
            return -1;
        }
    }

    errno = 0;
    // Iterate through and delete the files in the rebuild directory now that
    // we've completed the real directory
    while ((dent = readdir(dirp)) != NULL) {
        // Skip hidden files again, but delete LibNE partial and rebuild files.
        // LibNE will just throw an error when attempting to commit the changes
        // and rebuild.
        // This time, we also want to skip any files that were not migrated to
        // the real directory so we don't potentially lose any data
        if (*dent->d_name == '.' || (fstatat(write_dir, dent->d_name, &st, 0) && !is_sfx(dent->d_name, WRITE_SFX) && !is_sfx(dent->d_name, REBUILD_SFX))) {
            errno = 0;
            continue;
        }

        if (unlinkat(read_dir, dent->d_name, 0)) {
            PRINTout("failed to unlink %s/%s\n", reb_path, dent->d_name);
            fchmod(read_dir, S_IRWXU);
            (void)!fchown(read_dir, uid, gid);
            closedir(dirp);
            close(write_dir);
            free(work->link_path);
            free(work);
            return -1;
        }
        errno = 0;
    }
    if (errno != 0) {
        PRINTout("failed to read from directory %s (%s)\n", reb_path, strerror(errno));
        fchmod(read_dir, S_IRWXU);
        (void)!fchown(read_dir, uid, gid);
        closedir(dirp);
        close(write_dir);
        free(work->link_path);
        free(work);
        return -1;
    }

    closedir(dirp);
    close(write_dir);

    // Delete the rebuild directory, it should be empty and we don't need it
    // anymore
    errno = 0;
    if (unlinkat(gstate->dctxt->sec_root, reb_path, AT_REMOVEDIR) && errno != ENOTEMPTY) {
        PRINTout("failed to unlink %s (%s)\n", reb_path, strerror(errno));
        free(work->link_path);
        free(work);
        return -1;
    }
    // If any files were not deleted, it is because they were not migrated to
    // the real directory. Notify the admin so this can be handled manually
    else if (errno == ENOTEMPTY) {
        PRINTout("directory %s is not empty. Skipping unlink\n", reb_path);
        fchmod(read_dir, S_IRWXU);
        (void)!fchown(read_dir, uid, gid);
        free(work->link_path);
        free(work);
        return 0;
    }

del_dirs:
    // Strip the name of the rebuild directory off its path and attempt to
    // delete its parent directory (which indicates when the rebuild started).
    // Ignore the cases where the parent was already deleted (when the thread
    // was handed a real directory that was completely migrated and cleaned up,
    // but we weren't able to tell) or the parent is not empty (it is the
    // parent of the rebuild directory for more than one location)
    rblock_ptr = strstr(reb_path, REB_DIR);
    if (rblock_ptr == NULL) {
        PRINTout("failed to locate rblock dir in %s\n", reb_path);
        free(work->link_path);
        free(work);
        return -1;
    }
    while (*rblock_ptr != '/') {
        rblock_ptr++;
    }
    *rblock_ptr = '\0';
    if (unlinkat(gstate->dctxt->sec_root, reb_path, AT_REMOVEDIR) && errno != ENOTEMPTY && errno != ENOENT) {
        PRINTout("failed to unlink %s (%s)\n", reb_path, strerror(errno));
        free(work->link_path);
        free(work);
        return -1;
    }

    free(work->link_path);
    free(work);

    return 0;
}

// Thread queue pause behavior
int reb_pause(void** state, void** prev_work) {
    return 0;
}

// Thread queue resume behavior
int reb_resume(void** state, void** prev_work) {
    return 0;
}

// Thread queue termination behavior
void reb_term(void** state, void** prev_work, TQ_Control_Flags flg) {
    if (*prev_work != NULL) {
        PRINTout("prev_work allocated!\n");
        free(*prev_work);
    }
}

// Populates a DAL location struct with the nth rebuild target location
void map_loc(int n, DAL_location* loc, POSIX_REBUILD_LOC tgt) {
    if (n < 0) {
        loc->pod = -1;
        loc->block = -1;
        loc->cap = -1;
        loc->scatter = -1;
        return;
    }
    loc->scatter = tgt->scatter[n % tgt->scatter_size];
    n /= tgt->scatter_size;
    loc->cap = tgt->cap[n % tgt->cap_size];
    n /= tgt->cap_size;
    loc->block = tgt->block[n % tgt->block_size];
    n /= tgt->block_size;
    loc->pod = tgt->pod[n % tgt->pod_size];
}

// Adds a range of integers from start to end(inclusive, or -1 if adding only one value)
// to an array
int add_range(int* arr, int* size, int start, int end) {
    if (start < 0 || (end >= 0 && end <= start)) {
        PRINTout("given invalid range!\n");
        return -1;
    }

    if (end < 0) {
        end = start;
    }

    int i;
    for (i = start; i <= end; i++) {
        if (*size >= MAX_LOC_BUF) {
            PRINTout("exceeded size of location buffer!\n");
            return -1;
        }
        arr[*size] = i;
        (*size)++;
    }

    return 0;
}

// Generate a rebuild location struct corresponding to a given string representation
POSIX_REBUILD_LOC generate_locs(char* loc_str) {
    POSIX_REBUILD_LOC r_loc = calloc(1, sizeof(struct posix_rebuild_location_struct));
    if (r_loc == NULL) {
        return NULL; // calloc will set errno
    }

    // Locate the substrings that correspond to each location field
    char* pod_str = loc_str;

    char* block_str = strchr(pod_str, '/');
    if (block_str == NULL) {
        free(r_loc);
        return NULL;
    }
    *block_str = '\0';
    block_str++;

    char* cap_str = strchr(block_str, '/');
    if (cap_str == NULL) {
        free(r_loc);
        return NULL;
    }
    *cap_str = '\0';
    cap_str++;

    char* scatter_str = strchr(cap_str, '/');
    if (scatter_str == NULL) {
        free(r_loc);
        return NULL;
    }
    *scatter_str = '\0';
    scatter_str++;

    // Form arrays so we can just iterate through location fields
    char* str_arr[] = {pod_str, block_str, cap_str, scatter_str};
    char char_arr[] = {'p', 'b', 'c', 's'};
    int* loc_arr[] = {r_loc->pod, r_loc->block, r_loc->cap, r_loc->scatter};
    int* size_arr[] = {&r_loc->pod_size, &r_loc->block_size, &r_loc->cap_size, &r_loc->scatter_size};

    // Iterate through the location fields, forming the rebuild location arrays
    int i = 0;
    for (i = 0; i < 4; i++) {
        // Double check this substring corresponds to the field we think it does
        char* parse = str_arr[i];
        if (*parse != char_arr[i]) {
            PRINTout("location argument indicator should be \"%c\" but is \"%c\"!\n", char_arr[i], *parse);
            free(r_loc);
            return NULL;
        }
        parse++;

        int start = -1;
        int end = -1;
        // parse through the string, adding ranges to the rebuild struct
        while (*parse != '\0') {
            switch (*parse) {
                // we've reached the end of a range entry, add that range to the
                // rebuild struct and reset the indexes
                case ',':
                    if (add_range(loc_arr[i], size_arr[i], start, end) < 0) {
                        free(r_loc);
                        return NULL;
                    }
                    start = -1;
                    end = -1;
                    break;
                // we're dealing with a range, not a a single value. The start
                // index is done, we need to start working on the end index
                case '-':
                    end = 0;
                    break;
                default:
                    // add the current digit to the appropriate start/end index
                    if (isdigit(*parse)) {
                        int dig = *parse - '0';
                        if(start < 0) {
                            start = dig;
                        }
                        else if (end >= 0) {
                            end = (end * 10) + dig;
                        }
                        else {
                            start = (start * 10) + dig;
                        }
                    }
                    // fail if we try to parse an invalid character
                    else {
                        PRINTout("location argument given invalid character \"%c\"\n", *parse);
                        free(r_loc);
                        return NULL;
                    }
                    break;
            }
            parse++;
        }
        // add the last range to the rebuild struct
        if (add_range(loc_arr[i], size_arr[i], start, end) < 0) {
            free(r_loc);
            return NULL;
        }
        // each field must have at least one location. If we don't have any, fail
        if (*size_arr[i] == 0) {
            free(r_loc);
            return NULL;
        }
    }

    return r_loc;
}

// Parse through a config spec to create and configure a POSIX_DAL
// Taken from neutil.c -c parsing
DAL parse_dal(char* config_spec, DAL_location max_loc) {
    DAL dal = NULL;
    // parse the config path for the correct DAL root node
    char* fields;
    if (!(fields = strchr(config_spec, ':'))) {
        PRINTout("error: could not separate path from fields in config path \"%s\" (%s)", config_spec, strerror(errno));
        return NULL;
    }
    *fields = '\0';
    fields += 2;

    // this initializes the library and checks potential ABI mismatches
    // between the version it was compiled for anad the actual shared
    // library used.
    LIBXML_TEST_VERSION

    // parse the file and get the DOM
    xmlDoc* doc = xmlReadFile(config_spec, NULL, XML_PARSE_NOBLANKS);
    if (doc == NULL) {
        PRINTout("error: could not parse config file \"%s\"\n", config_spec);
        return NULL;
    }

    // Get the root element node
    xmlNode* root = xmlDocGetRootElement(doc);
    xmlAttr* props = NULL;

    char* tag = NULL;
    char* attr = NULL;
    char* val = NULL;
    int last = 0;
    char* next;
    while (root) {
        // Get details for the next node to find within the config structure
        if (!tag) {
            if (!(next = strchr(fields, '/'))) {
                next = strchr(fields + 1, '\0');
                last = 1;
            }
            *next = '\0';
            if ((attr = strchr(fields, ' '))) {
                *attr = '\0';
                attr++;
                if (!(val = strchr(attr, '='))) {
                    PRINTout("could not find a value for attribute \"%s\" in field with tag \"%s\"\n", attr, fields);
                    return NULL;
                }
                *val = '\0';
                val++;
            }
            tag = fields;
            fields = next + 1;
        }
        // Find that node
        if (root->type == XML_ELEMENT_NODE && !strcmp((char*)root->name, tag)) {
            if (val) {
                props = root->properties;
                while (props) {
                    if (props->type == XML_ATTRIBUTE_NODE && !strcmp((char*)props->name, attr) && props->children->type == XML_TEXT_NODE && !strcmp((char*)props->children->content, val)) {
                        if (!last) {
                            root = root->children;
                        }
                        tag = NULL;
                        attr = NULL;
                        val = NULL;
                        break;
                    }
                    props = props->next;
                }
                if (!props) {
                    root = root->next;
                }
                if (last) {
                    break;
                }
            }
            else {
                if (last) {
                    break;
                }
                root = root->children;
                tag = NULL;
                attr = NULL;
                val = NULL;
            }
        }
        else {
            root = root->next;
        }
    }
    if (!root) {
        PRINTout("could not find DAL in config file \"%s\"\n", config_spec);
        return NULL;
    }

    // Some extra sanity checks pulled from init_dal() to make sure we were passed a POSIX DAL config

    // Make sure we start on a 'DAL' node
    if (root->type != XML_ELEMENT_NODE || strncmp((char*)root->name, "DAL", 4) != 0) {
        PRINTout("root xml node is not an element of type \"DAL\"!\n");
        return NULL;
    }

    // Make sure we have a valid 'type' attribute
    props = root->properties;
    xmlNode *typetxt = NULL;
    for (; props; props = props->next) {
        if (typetxt == NULL && props->type == XML_ATTRIBUTE_NODE && strncmp((char *)props->name, "type", 5) == 0) {
            typetxt = props->children;
        }
        else {
            PRINTout("encountered unrecognized or redundant DAL attribute: \"%s\"\n", (char *)props->name);
        }
    }
    if (typetxt == NULL) {
        PRINTout("failed to find a 'type' attribute for the given DAL node!\n");
        return NULL;
    }

    // Make sure we have a text value for the 'type' attribute
    if (typetxt->type != XML_TEXT_NODE || typetxt->content == NULL) {
        PRINTout("DAL type attribute does not have a text value!\n");
        return NULL;
    }

    // Make sure we're dealing with a POSIX DAL
    if (strncasecmp((char *)typetxt->content, "posix", 6) == 0) {
        dal = posix_dal_init(root->children, max_loc);
    }
    else {
        return NULL;
    }

    return dal;
}

int main(int argc, char** argv) {
    char ts[22];
    sprintf(ts, "%lu", time(NULL));
    errno = 0;
    char* config_path = NULL;
    int mode = I_MODE;
    DAL dal = NULL;
    DAL_location max_loc;
    POSIX_REBUILD_LOC tgt = NULL;
    POSIX_REBUILD_LOC dist = NULL;
    int n_tgt = 0;
    int n_dist = 0;

    char pr_usage = 0;
    int c;
    // parse all arguments
    while ((c = getopt(argc, argv, "c:d:t:12z")) != -1) {
        switch (c) {
            case 'c':
                config_path = strdup(optarg);
                break;
            case 'd':
                dist = generate_locs(strdup(optarg));
                dist->ts = ts;
                n_dist = dist->pod_size * dist->block_size * dist->cap_size * dist->scatter_size;
                break;
            case 't':
                tgt = generate_locs(strdup(optarg));
                tgt->ts = ts;
                n_tgt = tgt->pod_size * tgt->block_size * tgt->cap_size * tgt->scatter_size;
                break;
            case '1':
            case '2':
                if (mode > 0) {
                    PRINTout("cannot select more than one mode! ( '-1' or '-2' args )\n");
                    return -1;
                }
                mode = c - '0';
                break;
            case '?':
                pr_usage = 1;
                break;
            case 'z':
            default:
                fprintf(stderr, "Failed to parse command line options\n");
                return -1;
        }
    }

    // check if we need to print usage info
    if (pr_usage) {
        PRINTout("Usage info --\n");
        PRINTout("%s ( -1 | -2 ) -c config_spec\n", PROGNAME);
        PRINTout("   -1 :             Run the tool in 'Prepare to Rebuild' mode\n");
        PRINTout("   -2 :             Run the tool in 'Migrate afer Rebuild' mode\n");
        PRINTout("   -c config_spec : Specifies a XML file containing the DAL configuration. config_spec is of the form\n");
        PRINTout("                      \"<file_path>:/<tag>[ <attribute>=<value> ]/...\" where the series of tags form a path through the\n");
        PRINTout("                      configuration file to the DAL configuration node.\n");
        PRINTout("   -h :             Print this usage info\n");
    }

    // verify that a valid mode was selected
    if (mode == I_MODE) {
        PRINTout("no mode selected ( '-1' or '-2' args )\n");
	return -1;
    }
    else if (mode == P_MODE) {
        // verify that a config was defined
        if (config_path == NULL) {
            PRINTout("no config path defined ( '-c' arg )\n");
            return -1;
        }

        // verify that a rebuild target was defined
        if (tgt == NULL) {
            PRINTout("no or invalid rebuild target defined ( '-t' arg )\n");
            return -1;
        }

        // verify that a write distribution was defined
        if (dist == NULL) {
            PRINTout("no or invalid write distribution defined ( '-d' arg )\n");
            return -1;
        }
    }
    else { // mode == M_MODE
        // NOTE: we need these values, but they should come from the status file
        // left by the preparation run of this tool

        // verify that a config was not defined
        if (config_path) {
            PRINTout("config path should not be defined ( '-c' arg )\n");
            return -1;
        }

        // verify that a rebuild target was not defined
        if (tgt) {
            PRINTout("rebuild target should not be defined ( '-t' arg )\n");
            return -1;
        }

        // verify that a write distribution was not defined
        if (dist) {
            PRINTout("write distribution should not be defined ( '-d' arg )\n");
            return -1;
        }
    }

    // MPI_Initialization
    if (MPI_Init(&argc, &argv) != MPI_SUCCESS) {
        LOG(LOG_ERR, "Error in MPI_Init\n");
        return -1;
    }

    if (MPI_Comm_size(MPI_COMM_WORLD, &n_ranks) != MPI_SUCCESS) {
        LOG(LOG_ERR, "Error in MPI_Comm_size\n");
        return -1;
    }

    if (MPI_Comm_rank(MPI_COMM_WORLD, &rank) != MPI_SUCCESS) {
        LOG(LOG_ERR, "Error in MPI_Comm_rank\n");
        return -1;
    }

    // Form the path for the status file in the user's home directory. If we are
    // preparing a rebuild, we will write our args to the file. If we are
    // migrating after rebuild, we will read these args back to ensure we are
    // using the same values
    char* homedir;
    if (!(homedir = getenv("HOME"))) {
        homedir = getpwuid(getuid())->pw_dir;
    }
    char status_path[strlen(homedir) + strlen(STATUS_FNAME) + 1];
    sprintf(status_path, "%s/%s", homedir, STATUS_FNAME);

    int fd;
    if (mode == P_MODE) { // Prepare for rebuild
        // Determine maximum DAL location from target and distribution structs
        max_loc.pod = MAX_LOC(pod);
        max_loc.block = MAX_LOC(block);
        max_loc.cap = MAX_LOC(cap);
        max_loc.scatter = MAX_LOC(scatter);

        // Create our DAL
        if ((dal = parse_dal(config_path, max_loc)) == NULL) {
            PRINTout("failed to initialize POSIX DAL context!\n");
            return -1;
        }

        // Create our status file, which should not already exist
        fd = open(status_path, O_WRONLY | O_CREAT | O_EXCL, S_IRUSR);
        if (fd < 0) {
            if (errno == EEXIST) {
                PRINTout("status file \"%s\" already exists!\n", status_path);
                return -1;
            }
            PRINTout("failed to create status file \"%s\"! (%s)\n", status_path, strerror(errno));
            return -1;
        }

        // Write our args out to the status file
        int i;
        char fmt_arg[1024] = {0};
        for (i = 0; i < argc; i++) {
            sprintf(fmt_arg, "\"%s\" ", argv[i]);
            if (write(fd, fmt_arg, strlen(fmt_arg)) < 0) {
                PRINTout("failed to write argument \"%s\" to status file \"%s\"! (%s)\n", argv[i], status_path, strerror(errno));
                close(fd);
                unlink(status_path);
                return -1;
            }
        }
        // Also add our timestamp
        sprintf(fmt_arg, "\"-z\" \"%s\"\n", ts);

        if (write(fd, fmt_arg, strlen(fmt_arg)) < 0) {
            PRINTout("failed to write newline ('\\n') to status file \"%s\"!\n", status_path);
            close(fd);
            unlink(status_path);
            return -1;
        }

        // Double check with the admin that we actually want to initiate this
        // rebuild
        PRINTout("Preparing to rebuild. %d directories will be redirected to %d temporary directories. Do you want to continue? (y/n) ", n_tgt, n_dist);
        char c = getchar();
        if (c != 'y' && c != 'Y') {
            PRINTout("Abort.\n");
            close(fd);
            unlink(status_path);
            return -1;
        }

        // Create our data directory tree, along with rebuild symlinks and
        // rebuild directories
        _posix_verify(dal->ctxt, CFG_FIX, tgt, dist);

        close(fd);

    }
    else { // Migrate after rebuild (M_MODE)
        // Open our status file, which should already exist from the preparation
        // run
        fd = open(status_path, O_RDONLY);
        if (fd < 0) {
            if (errno == ENOENT) {
                PRINTout("status file \"%s\" does not exist!\n", status_path);
                return -1;
            }
            PRINTout("failed to open status file \"%s\"! (%s)\n", status_path, strerror(errno));
            return -1;
        }

        // Read args from the previous run left in the status file and parse
        char args[1024] = {0};
        int ret = read(fd, args, 1024);
        errno = 0;
        if (ret <= 0 || ret == 1024) {
            PRINTout("failed to read from status file \"%s\"! Returned (0 < %d < 1024) (%s)\n", status_path, ret, strerror(errno));
            return -1;
        }

        char* c_st = strstr(args, "\"-c\" \"") + 6;
        char* c_end = strstr(c_st, "\"");

        char* d_st = strstr(args, "\"-d\" \"") + 6;
        char* d_end = strstr(d_st, "\"");

        char* t_st = strstr(args, "\"-t\" \"") + 6;
        char* t_end = strstr(t_st, "\"");

        char* z_st = strstr(args, "\"-z\" \"") + 6;
        char* z_end = strstr(z_st, "\"");
        *c_end = '\0';
        *d_end = '\0';
        *t_end = '\0';
        *z_end = '\0';

        // Form distribution location struct
        dist = generate_locs(d_st);
        dist->ts = z_st;
        n_dist = dist->pod_size * dist->block_size * dist->cap_size * dist->scatter_size;

        // Form target location struct
        tgt = generate_locs(t_st);
        tgt->ts = z_st;
        n_tgt = tgt->pod_size * tgt->block_size * tgt->cap_size * tgt->scatter_size;

        // Determine maximum DAL location from target and distribution structs
        max_loc.pod = MAX_LOC(pod);
        max_loc.block = MAX_LOC(block);
        max_loc.cap = MAX_LOC(cap);
        max_loc.scatter = MAX_LOC(scatter);

        // Create our DAL
        if ((dal = parse_dal(c_st, max_loc)) == NULL) {
            PRINTout("failed to initialize POSIX DAL context!\n");
            return -1;
        }

        // Determine how many locations each rank needs to operate on
        int n_locs = n_tgt / n_ranks;

        if (rank == 0) {
            PRINTout("Preparing to migrate. %d directories were be redirected to %d temporary directories.\n", n_tgt, n_dist);
        }

        struct tq_global_struct gstate;
        gstate.dctxt = (POSIX_DAL_CTXT)dal->ctxt;

        TQ_Init_Opts tqopts;
        tqopts.log_prefix = "MyTQ";
        tqopts.init_flags = TQ_NONE;
        tqopts.global_state = (void *)&gstate;
        tqopts.num_threads = NUM_CONS;
        tqopts.num_prod_threads = 0;
        tqopts.max_qdepth = QDEPTH;
        tqopts.thread_init_func = reb_thread_init;
        tqopts.thread_consumer_func = reb_cons;
        tqopts.thread_producer_func = NULL;
        tqopts.thread_pause_func = reb_pause;
        tqopts.thread_resume_func = reb_resume;
        tqopts.thread_term_func = reb_term;

        // Initiate our thread queue
        ThreadQueue tq = tq_init(&tqopts);
        if (!tq) {
            PRINTout("Failed to initialize tq\n");
            return -1;
        }
        if (tq_check_init(tq)) {
            PRINTout("Initialization failure in itq\n");
            return -1;
        }

        int i;
        int last_loc = (rank + 1) * n_locs;
        DAL_location loc;
        char link_path[256] = {0};
        char reb_path[256] = {0};
        struct stat st;
        WorkPkg work = NULL;
        if (rank == n_ranks - 1) {
            last_loc = n_tgt;
        }
        // Iterate through our target locations, distributing them to our
        // consumer threads
        for (i = rank * n_locs; i < last_loc; i++) {
            // Form the DAL location that corresponds with the location number
            map_loc(i, &loc, tgt);

            // Generate the path that corresponds to this location
            expand_path(gstate.dctxt->dirtmp, link_path, loc, NULL, 1);
            link_path[strlen(link_path) - 1] = '\0';

            work = calloc(1, sizeof(struct work_pkg_struct));
            if (!work) {
                PRINTout("failed to allocate work package");
                return -1;
            }

            // Determine if we're dealing with a link (normal behavior),
            // directory (recovering from a failure), or no entry (also failure)
            errno = 0;
            if (fstatat(gstate.dctxt->sec_root, link_path, &st, AT_SYMLINK_NOFOLLOW) && errno != ENOENT) {
                PRINTout("failed to stat target dir \"%s\" (%s)\n", link_path, strerror(errno));
                free(work);
                return -1;
            }
            else if (errno == ENOENT) {
                work->flg = WORK_NOENT;
            }
            else if (S_ISLNK(st.st_mode)) {
                work->flg = WORK_LNK;
            }
            else if (S_ISDIR(st.st_mode)) {
                work->flg = WORK_DIR;
            }
            else {
                PRINTout("target dir \"%s\" is not a directory or symlink!\n", link_path);
                free(work);
                return -1;
            }

            // If we do not have a link, we will have to hash our path to
            // recover the path of the rebuild directory
            if (work->flg != WORK_LNK) {
                DAL_location reb_loc = hash_loc(link_path, dist);

                // If our target path hashes to another rebuild target, the
                // directory should have been created locally instead of a link,
                // so we don't have to migrate
                if (!tgtMatch(&reb_loc, tgt)) {
                    if (work->flg == WORK_DIR) {
                        free(work);
                        continue;
                    }
                    else {
                        PRINTout("failed to locate \"%s\", which does not map to a rebuild location\n", link_path);
                        free(work);
                        return -1;
                    }
                }

                // Form the path for our rebuild directory
                expand_path(gstate.dctxt->dirtmp, reb_path, reb_loc, NULL, 1);
                work->reb_path = reb_path + strlen(reb_path);

                if (strcat(work->reb_path, REB_DIR) != work->reb_path) {
                    PRINTout("failed to form rebuild path\n");
                    free(work);
                    return -1;
                }
                work->reb_path += strlen(REB_DIR);

                if (strcat(work->reb_path, tgt->ts) != work->reb_path) {
                    PRINTout("failed to form rebuild path\n");
                    free(work);
                    return -1;
                }
                work->reb_path += strlen(tgt->ts);

                sprintf(work->reb_path, RBLOCK_DIR, loc.pod, loc.block, loc.cap, loc.scatter);
                work->reb_path += strlen(RBLOCK_DIR) + num_digits(loc.pod) + num_digits(loc.block) + num_digits(loc.cap) + num_digits(loc.scatter) - 8;
                *work->reb_path = '\0';

                work->reb_path = strdup(reb_path);
            }

            // Add our path to the work package
            work->link_path = strdup(link_path);

            // Place our work package on the thread queue for a consumer to
            // operate on
            if(tq_enqueue(tq, TQ_HALT, work)) {
                PRINTout("failed to enqueue package\n");
                if (work->reb_path) {
                    free(work->reb_path);
                }
                free(work->link_path);
                free(work);
                return -1;
            }
        }
        // Once we're done adding packages to the queue, signal to the consumer
        // threads
        if (tq_set_flags(tq, TQ_FINISHED)) {
            PRINTout("failed to set thread queue to finished\n");
            return -1;
        }

        // Wait until all work packages have beeb consumed
        if (tq_wait_for_completion(tq)) {
            PRINTout("failed to wait for thread queue to complete\n");
            return -1;
        }

        // Collect our consumer threads
        int tres = 0;
        ThreadState tstate = NULL;
        while ((tres = tq_next_thread_status(tq, (void**)&tstate)) > 0) {
            if (!tstate) {
                PRINTout("received NULL status for thread\n");
                return -1;
            }

            free(tstate);
        }
        if (tres < 0) {
            PRINTout("failed to retrieve next thread status\n");
            return -1;
        }

        // Attempt to close our thread queue. If any packages remain on the
        // queue, manually consume them
        if (tq_close(tq) > 0) {
            work = NULL;
            while(tq_dequeue(tq, TQ_ABORT, (void**)&work) > 0) {
                if (!tstate && reb_thread_init(tqopts.num_threads, (void*)&gstate, (void**)&tstate)) {
                    PRINTout("failed to initialize thread state for cleanup\n");
                    return -1;
                }

                if (reb_cons((void**)&tstate, (void**)&work)) {
                    PRINTout("failed to consume work left on queue\n");
                    free(tstate);
                    return -1;
                }
            }
            tq_close(tq);
        }

        close(fd);

        // Wait until all ranks are done before deleting the status file
        MPI_Barrier(MPI_COMM_WORLD);
        if (rank == 0 && unlink(status_path)) {
            PRINTout("failed to unlink status file \"%s\"! (%s)\n", status_path, strerror(errno));
            return -1;
        }

    }

    dal->cleanup(dal);

    free(tgt);
    free(dist);

    MPI_Finalize();

    return 0;
}
