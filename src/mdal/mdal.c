/**
 * Copyright 2015. Triad National Security, LLC. All rights reserved.
 *
 * Full details and licensing terms can be found in the License file in the main development branch
 * of the repository.
 *
 * MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier: LA-CC-15-039.
 */

#include "marfs_auto_config.h"
#ifdef DEBUG_MDAL
#define DEBUG DEBUG_MDAL
#elif (defined DEBUG_ALL)
#define DEBUG DEBUG_ALL
#endif
#define LOG_PREFIX "mdal"

#include "logging/logging.h"
#include "mdal.h"

#include <ctype.h>


// Function to provide specific MDAL initialization calls based on name
MDAL init_mdal( xmlNode* mdal_conf_root ) {
   // make sure we start on a 'MDAL' node
   if ( mdal_conf_root->type != XML_ELEMENT_NODE  ||  strncmp( (char*)mdal_conf_root->name, "MDAL", 5 ) != 0 ) {
      LOG( LOG_ERR, "root xml node is not an element of type \"MDAL\"!\n" );
      errno = EINVAL;
      return NULL;
   }

   // make sure we have a valid 'type' attribute
   xmlAttr* type = mdal_conf_root->properties;
   xmlNode* typetxt = NULL;
   for ( ; type; type = type->next ) {
      if ( typetxt == NULL  &&  type->type == XML_ATTRIBUTE_NODE  &&  strncmp( (char*)type->name, "type", 5 ) == 0 ) {
         typetxt = type->children;
      }
      else {
         LOG( LOG_WARNING, "encountered unrecognized or redundant MDAL attribute: \"%s\"\n", (char*)type->name );
      }
   }
   if ( typetxt == NULL ) {
      LOG( LOG_ERR, "failed to find a 'type' attribute for the given MDAL node!\n" );
      errno = EINVAL;
      return NULL;
   }

   // make sure we have a text value for the 'type' attribute
   if ( typetxt->type != XML_TEXT_NODE  ||  typetxt->content == NULL ) {
      LOG( LOG_ERR, "MDAL type attribute does not have a text value!\n" );
      errno = EINVAL;
      return NULL;
   }

   // name comparison for each MDAL type
   if (  strncasecmp( (char*)typetxt->content, "posix", 6 ) == 0 ) {
      return posix_mdal_init( mdal_conf_root->children );
   }

   // if no MDAL found, return NULL
   LOG( LOG_ERR, "failed to identify an MDAL of type: \"%s\"\n", typetxt->content );
   errno = ENODEV;
   return NULL;
}



