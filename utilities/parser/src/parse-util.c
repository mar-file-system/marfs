#include <stdlib.h>
#include <stdlib.h>

#include "parse-types.h"
#include "parse-inc/config-structs.h"



void *configPtr(int type)
{
switch(type) {
   #include "parse-inc/struct-switch.inc"
   }
}
