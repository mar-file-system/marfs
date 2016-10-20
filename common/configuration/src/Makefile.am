LOG_DIR  = $(srcdir)/../../log/src


#LIBS      = -lpthread
LIBS       =
AM_CPPFLAGS = -D_GNU_SOURCE
PARSE_OPT  = -DDATAPARSE

# set the debugging prefix
AM_CPPFLAGS+= -I$(LOG_DIR) -DLOG_PREFIX='"config"'

# TODO: Add debugging as an AM_CONDITIONAL set by configure.
if USE_SYSLOG
AM_CPPFLAGS += -DUSE_SYSLOG
endif

if USE_STDOUT
AM_CPPFLAGS += -DUSE_STDOUT
endif


BLUEPRINT   = $(abs_srcdir)/marfs_configuration_blueprint.cfg
CONFIG      = $(abs_srcdir)/marfs_cctest.cfg



# ---------------------------------------------------------------------------
# Generate Ron's parser code, based on the BLUEPRINT
# ---------------------------------------------------------------------------

PARSE_OBJ_files = confpars.o path-switch.o parsedata.o
PARSE_OBJ       = $(PARSE_DIR)/confpars.o $(PARSE_DIR)/path-switch.o $(PARSE_DIR)/parsedata.o

PARSE_H_file    = config-structs.h
PARSE_H         = $(PARSE_DIR)/parse-inc/$(PARSE_H_file)

.PHONY: parser
# This generates a 'datapars' in PARSE_DIR, using the BLUEPRINT config.
# This parser should then be able to parse MarFS configs that are
# consistent with the blueprint.
#
# NOTE: If you get errors about "redefining struct namespace",
#     You probably have a mistake in the tags in your config.
#
parser:
	@ echo; echo "#-- cleaning $(PARSE_DIR)"
	@ $(MAKE) -C $(PARSE_DIR) pristine

	@ echo; echo "#-- making 'base'"
	@ # $(MAKE) -C $(PARSE_DIR) confpars-d
	@ $(MAKE) -C $(PARSE_DIR) base

	@ echo; echo "#-- running confpars with $(BLUEPRINT)"
	@ # (cd $(PARSE_DIR); ./confpars $(BLUEPRINT); echo 0 )
	(cd $(PARSE_DIR); mkdir -p parse-inc; ./confpars $(BLUEPRINT) )

	@ echo; echo "#-- making datapars"
	@ # $(MAKE) -C $(PARSE_DIR) datapars-d   [fails]
	@ $(MAKE) -C $(PARSE_DIR) datapars

	@ echo; echo "#-- running datapars with $(CONFIG)"
	(cd $(PARSE_DIR); ./datapars $(CONFIG) )

	@ # NOTE: This is the only time PA2X's Makefile will honor our CFLAGS settings.
	@ echo; echo "#-- making $(PARSE_DIR)/*.o (again, because makefile deleted them)"
	@ $(MAKE) -C $(PARSE_DIR) $(PARSE_OBJ_files) CFLAGS="$(OPTIONS) $(PARSE_OPT)"

	@ echo; echo "#-- making a symlink to $(PARSE_DIR)/$(PARSE_h_file)"
	ln -fs $(PARSE_H) $(PARSE_H_file)


# a super nasty way to make sure that parser is always built first.
BUILT_SOURCES = parser
CLEAN_FILES = $(PARSE_h_file)

# ---------------------------------------------------------------------------
# build a library with Ron's parser, plus the marfs config-parser, plus logging
# ---------------------------------------------------------------------------
noinst_LIBRARIES = libconfig.a

libconfig_a_LIBADD = $(PARSE_OBJ)
AM_CPPFLAGS += -I$(PARSE_DIR) -DPARSE_DIR=$(PARSE_DIR) $(PARSE_OPT)
libconfig_a_SOURCES = parser PA2X_interface.c PA2X_interface.h $(LOG_DIR)/logging.c $(LOG_DIR)/logging.h
