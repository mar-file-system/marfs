SHELL = /bin/bash
CC = gcc

# To make this useful to people who have Ron's parser installed in
# arbitrary locations, I've added a requirement that you define an
# environment variable PARSE_DIR which is the path to your install of Ron's
# code.  You can define this in your shell-init, or on the command-line,
# using 'make ... PARSE_DIR=/my/install' [jti]

# all:   test_marfs_configuration
# debug: test_marfs_configuration-d

all:   libconfig.a tester marfs_config
debug: test_marfs_configuration-d


LOG_DIR = $(PWD)/../../log/src

#LIBS      = -lpthread
LIBS       =
OPTIONS   := -D_GNU_SOURCE -g
PARSE_OPT  = -DDATAPARSE
LOG_OPT    = -DLOG_PREFIX='"config"'

ifdef DEBUG
	LOG_OPT  += -DUSE_STDOUT 
	DBG_OPT   = -D_DEBUG_MARFS_CONFIGURATION
endif

BLUEPRINT   := $(PWD)/marfs_cctest_blueprint.cfg
CONFIG_FILE := $(PWD)/marfs_cctest.cfg



# ---------------------------------------------------------------------------
# Generate Ron's parser code, based on the BLUEPRINT
# ---------------------------------------------------------------------------

.PHONY: parse_dir
parse_dir:
ifeq ($(PARSE_DIR),)
	$(error $$PARSE_DIR is empty.  Consider 'make ... PARSE_DIR=<DIR>')
endif

PARSE_OBJ_files = confpars.o path-switch.o parsedata.o
PARSE_OBJ       = $(patsubst %, $(PARSE_DIR)/%, $(PARSE_OBJ_files))


# This generates a 'datapars' in PARSE_DIR, using the BLUEPRINT config.
# This parser should then be able to parse MarFS configs that are
# consistent with the blueprint.
#
# It also generates the files in $(PARSE_OBJ)
#
# NOTE: If you get errors about "redefining struct namespace",
#     You probably have a mistake in the tags in your config.
#
parser_old: parse_dir
	@ echo; echo "#-- cleaning $(PARSE_DIR)"
	mkdir -p $(PARSE_DIR)/parse-inc
	@ $(MAKE) -C $(PARSE_DIR) clean
	@ $(RM) $(PARSE_DIR)/confpars
	@ $(RM) $(PARSE_DIR)/datapars
	@ echo; echo "#-- making confpars"
	@ $(MAKE) -C $(PARSE_DIR) confpars CFLAGS="$(CFLAGS) $(OPTIONS) $(PARSE_OPT)"
	@ # echo; echo "#-- running confpars (to generate buggy include-files)"
	@ # $(MAKE) -C $(PARSE_DIR) confpars-run conf=$(BLUEPRINT)
	@ # echo; echo "#-- experimental hack: fix parse-inc/path-names.inc"
	@ # cat $(PARSE_DIR)/parse-inc/path-names.inc | awk -f fix_parser_internals.awk > foo
	@ # mv foo $(PARSE_DIR)/parse-inc/path-names.inc
	@ # echo; echo "#-- experimental hack: fix parse-inc/path-switch.inc"
	@ # cat $(PARSE_DIR)/parse-inc/path-switch.inc | awk -f fix_parser_internals.awk > foo
	@ # mv foo $(PARSE_DIR)/parse-inc/path-switch.inc
	@ echo; echo "#-- making datapars with $(CONFIG_FILE)"
	@ $(MAKE) -C $(PARSE_DIR) datapars CFLAGS="$(CFLAGS) $(OPTIONS) $(PARSE_OPT)" conf=$(CONFIG_FILE)
	@ echo; echo "#-- making $(PARSE_DIR)/*.o (again, because makefile deleted them)"
	@ $(MAKE) -C $(PARSE_DIR) $(PARSE_OBJ_files) CFLAGS="$(OPTIONS) $(PARSE_OPT)"

parser: parse_dir
	@ echo; echo "#-- cleaning $(PARSE_DIR)"
	@ $(MAKE) -C $(PARSE_DIR) pristine
	@ echo; echo "#-- making 'base'"
	@ # $(MAKE) -C $(PARSE_DIR) confpars-d
	@ $(MAKE) -C $(PARSE_DIR) base
	@ echo; echo "#-- running confpars (with blueprint file)"
	(cd $(PARSE_DIR); ./confpars $(BLUEPRINT) )
	@ echo; echo "#-- making datapars"
	@ # $(MAKE) -C $(PARSE_DIR) datapars-d   [fails]
	@ $(MAKE) -C $(PARSE_DIR) datapars
	@ echo; echo "#-- running datapars with $(CONFIG_FILE)"
	(cd $(PARSE_DIR); ./datapars $(CONFIG_FILE) )
	@ echo; echo "#-- making $(PARSE_DIR)/*.o (again, because makefile deleted them)"
	@ $(MAKE) -C $(PARSE_DIR) $(PARSE_OBJ_files) CFLAGS="$(OPTIONS) $(PARSE_OPT)"


CFLAGS += -I$(PARSE_DIR) -DPARSE_DIR=$(PARSE_DIR)


# ---------------------------------------------------------------------------
# The logging support we depend on.  (Make the .o file here, not in LOG_DIR)
# ---------------------------------------------------------------------------

LOG_OBJ    = logging.o

logging.o: $(LOG_DIR)/logging.c $(LOG_DIR)/logging.h
	@ echo; echo "#-- cleaning $(LOG_DIR)/logging.o"
	rm -f $(LOG_DIR)/logging.o
	@ echo; echo "#-- building $(LOG_DIR)/logging.o"
	$(MAKE) -C $(LOG_DIR) logging.o CFLAGS="$(CFLAGS) $(LOG_OPT)"
	@ echo; echo "#-- moving $(LOG_DIR)/logging.o to here"
	mv $(LOG_DIR)/logging.o .

CFLAGS += -I$(LOG_DIR) $(LOG_OPT)


# ---------------------------------------------------------------------------
# build a library with Ron's parser, plus the marfs config-parser, plus logging
# ---------------------------------------------------------------------------

OBJ = marfs_configuration.o
SRC = marfs_configuration.c
H   = marfs_configuration.h



%.o: %.c %.h parse_dir
	@ echo; echo "#-- building $@"
	gcc -c $(CFLAGS) $(PARSE_OPT) -DPARSE_DIR=$(PARSE_DIR) $(OPTIONS) -DTBD -o $@ $<


libconfig.a: parser $(LOG_OBJ) $(OBJ)
	@ echo; echo "#-- building $@"
	ar -cvr $@ $(PARSE_OBJ)
	ar -cvr $@ $(LOG_OBJ)
	ar -cvr $@ $(OBJ)

LDFLAGS += -L.


# ---------------------------------------------------------------------------
# Alternate build of Brett's config-tester, using the above
# ---------------------------------------------------------------------------

# CFLAGS  += -I$(PARSE_DIR) -I$(LOG_DIR) $(OPTIONS)
# LDFLAGS += -L.
tester: libconfig.a test_marfs_configuration.c
	@ echo; echo "#-- building $@"
	$(CC) -o $@ $(CFLAGS) $(LDFLAGS) $(DBG_OPT) test_marfs_configuration.c -lconfig



# ---------------------------------------------------------------------------
# tool to allow scripts to get access to configuration details
#
# for example:
#   $ marfs_config names repo   [print repo names]
#   $ marfs_config names ns     [print namespace names]
#   $ marfs_config repo s3      [print contents of repo with name 's3']
#   $ marfs_config ns jti       [print contents of namespace with name 'jti']
#   $ marfs_config config       [print top-level config details]
#
# ---------------------------------------------------------------------------

marfs_config: libconfig.a marfs_config.c
	@ echo; echo "#-- building $@"
	$(CC) -o $@ $(CFLAGS) $(LDFLAGS) marfs_config.c -lconfig



# ---------------------------------------------------------------------------
# build Brett's configuration tester test_marfs_configuration
# ---------------------------------------------------------------------------

test_marfs_configuration: parse_dir
	$(CC) -c $(PARSE_DIR)/confpars.c -o $(PARSE_DIR)/confpars.o $(OPTIONS) $(PARSE_OPT)
	$(CC) -c $(PARSE_DIR)/path-switch.c -o $(PARSE_DIR)/path-switch.o $(OPTIONS) $(PARSE_OPT)
	$(CC) -c $(PARSE_DIR)/parsedata.c -o $(PARSE_DIR)/parsedata.o $(OPTIONS) $(PARSE_OPT)
	$(CC) -c $(LOG_DIR)/logging.c -o $(LOG_DIR)/logging.o $(OPTIONS) $(LOG_OPT)
	$(CC) -I$(PARSE_DIR) -I$(LOG_DIR) -c marfs_configuration.c -o marfs_configuration.o $(OPTIONS) $(PARSE_OPT) $(LOG_OPT)
	$(CC) -I$(PARSE_DIR) -I$(LOG_DIR) -c test_marfs_configuration.c -o test_marfs_configuration.o $(OPTIONS) $(PARSE_OPT) $(LOG_OPT)
	$(CC) $(LIBS) $(PARSE_DIR)/confpars.o $(PARSE_DIR)/parsedata.o $(PARSE_DIR)/path-switch.o $(LOG_DIR)/logging.o marfs_configuration.o test_marfs_configuration.o -o test_marfs_configuration

test_marfs_configuration-d: parse_dir
	$(MAKE) test_marfs_configuration OPTIONS="$(DBG_OPT)"



clean: parse_dir
	@ # returns non-zero if files don't exist:  $(MAKE) -C $(PARSE_DIR) clean
	@ $(MAKE) -C $(PARSE_DIR) clean
	$(RM) $(PARSE_DIR)/*.o
	$(RM) $(PARSE_DIR)/parse-inc/*
	@ #
	$(RM) $(LOG_DIR)/*.o
	$(RM) *.o
	$(RM) test_marfs_configuration
	$(RM) libconfig*.a

pristine: clean
	@ # $(RM) $(PARSE_DIR)/parse-inc/*
	@ # $(RM) $(PARSE_DIR)/confpars
	@ # $(RM) $(PARSE_DIR)/datapars
	@ $(MAKE) -C $(PARSE_DIR) pristine
