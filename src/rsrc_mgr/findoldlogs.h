/**
 * Copyright 2015. Triad National Security, LLC. All rights reserved.
 *
 * Full details and licensing terms can be found in the License file in the main development branch
 * of the repository.
 *
 * MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier: LA-CC-15-039.
 */

#include <time.h>

#include "rsrc_mgr/rmanstate.h"

int findoldlogs(rmanstate* rman, const char* scanroot, time_t skipthresh);
