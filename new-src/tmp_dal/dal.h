
#ifndef DAL_H_INCLUDE
#define DAL_H_INCLUDE

#include <libxml/tree.h>
#include <string.h>

#ifndef LIBXML_TREE_ENABLED
#error "Included Libxml2 does not support tree functionality!"
#endif


typedef struct DAL_location_struct {
   int      pod;
   int      cap;
   int      scatter;
} *DAL_location;


typedef struct DAL_struct {
   // Name -- Used to identify and configure the DAL
   const char*    name;

   // DAL Internal Context -- passed to each DAL function
   void*          ctxt;

   // DAL Functions -- 
   int  (*dal_verify)  ( void* ctxt, char fix )                                  verify;
   int  (*dal_migrate) ( void* ctxt, DAL_location from, DAL_location to )       migrate;
   void (*dal_cleanup) ( void* ctxt )                                           cleanup;
} *DAL;


// Forward decls of specific DAL initializations
DAL posix_dal_init( xmlNode* dal_conf_root );


// Function to provide specific DAL initialization calls based on name
DAL init_dal_by_name( const char* name, xmlNode* dal_conf_root ) {
   if (  strncmp( name, "posix", 6 ) == 0 ) {
      return posix_dal_init( dal_conf_root );
   }
   // if no DAL found, return NULL
   return NULL;
}



#endif

