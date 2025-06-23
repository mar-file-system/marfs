#ifndef _RESOURCE_MANAGER_LOGINFO_H
#define _RESOURCE_MANAGER_LOGINFO_H
/**
 * Copyright 2015. Triad National Security, LLC. All rights reserved.
 *
 * Full details and licensing terms can be found in the License file in the main development branch
 * of the repository.
 *
 * MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier: LA-CC-15-039.
 */

#include "work.h"

typedef struct {
   size_t nsindex;
   size_t logcount;
   workrequest* requests;
} loginfo;

#endif
