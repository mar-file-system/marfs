#ifndef __MARFS_SPINLOCK_H__
#define __MARFS_SPINLOCK_H__

/*
Copyright (c) 2015, Los Alamos National Security, LLC
All rights reserved.

Copyright 2015.  Los Alamos National Security, LLC. This software was produced
under U.S. Government contract DE-AC52-06NA25396 for Los Alamos National
Laboratory (LANL), which is operated by Los Alamos National Security, LLC for
the U.S. Department of Energy. The U.S. Government has rights to use, reproduce,
and distribute this software.  NEITHER THE GOVERNMENT NOR LOS ALAMOS NATIONAL
SECURITY, LLC MAKES ANY WARRANTY, EXPRESS OR IMPLIED, OR ASSUMES ANY LIABILITY
FOR THE USE OF THIS SOFTWARE.  If software is modified to produce derivative
works, such modified software should be clearly marked, so as not to confuse it
with the version available from LANL.
 
Additionally, redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.
3. Neither the name of Los Alamos National Security, LLC, Los Alamos National
Laboratory, LANL, the U.S. Government, nor the names of its contributors may be
used to endorse or promote products derived from this software without specific
prior written permission.

THIS SOFTWARE IS PROVIDED BY LOS ALAMOS NATIONAL SECURITY, LLC AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL LOS ALAMOS NATIONAL SECURITY, LLC OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY,
OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

-----
NOTE:
-----
MarFS is released under the BSD license.

MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier:
LA-CC-15-039.

MarFS uses libaws4c for Amazon S3 object communication. The original version
is at https://aws.amazon.com/code/Amazon-S3/2601 and under the LGPL license.
LANL added functionality to the original work. The original work plus
LANL contributions is found at https://github.com/jti-lanl/aws4c.

GNU licenses can be found at http://www.gnu.org/licenses/.
*/





#include <stdint.h>
#include <stdlib.h>


typedef uint32_t __attribute((aligned(4))) spin_lock_t;


// [written in asm, linked directly.  see spinlock_asm.s]
//
// Don't return until you've written a 1 into the lock.  If there's already
// a 1 there, then "spin" until it has 0.  Then CMPXCHG to atomically install a 1.
// If that fails (some other thread gets in), then resume spinning.
extern void spin_lock(spin_lock_t* lock);


// there's no race-condition for the unlock, so we can just clear the
// value.
extern void spin_unlock(spin_lock_t* lock);



// ---------------------------------------------------------------------------
// PoliteSpinLock
//
// This repeatedly calls sched_yield(), if the protected lock-value has not
// been asserted.  We use a spin-lock to control access to the lock-value.
// These can be acquired on the order of 4M/sec, without context-switches
// or dramatic CPU overhead.
//
// NOTE: In MarFS object-streams, we could probably avoid the master lock
//     entirely.  It just makes me nervous to have threads synchronizing on
//     anything that isn't protected.
//
// ---------------------------------------------------------------------------

struct PoliteSpinLock {
   spin_lock_t       master;              // spin_lock
   volatile uint8_t  post_count;          // must lock master to change
};


// not thread-safe
void PSL_init(struct PoliteSpinLock* psl, unsigned int value);

void PSL_wait(struct PoliteSpinLock* psl);

// try to acquire PSL until timeout_sec have elapsed.
// return 0 if we got the lock
// return -1 if we didn't.
int  PSL_wait_with_timeout(struct PoliteSpinLock* psl, size_t timeout_sec);

void PSL_post(struct PoliteSpinLock* psl);



#endif
