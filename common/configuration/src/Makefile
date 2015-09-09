SHELL = /bin/bash
CC = gcc

<<<<<<< HEAD
# To make this useful to people who have Ron's parser installed in
# arbitrary locations, I've added a requirement that you define an
# environment variable PARSE_DIR which is the path to your install.
# You can define this in your shell-init, or on the command-line,
# using 'make ... PARSE_DIR=/my/install'    [jti]

all:   test_marfs_configuration
debug: test_marfs_configuration-d


# PARSE_DIR = ../../../../../PA2X/PA2X
LOG_DIR =   $(PWD)/../../log/src

#LIBS = -lpthread
LIBS =
OPTIONS   = -D_GNU_SOURCE -g
PARSE_OPT = -DDATAPARSE
LOG_OPT   = -DUSE_STDOUT -DLOG_PREFIX='"config"'

DEBUG   = -D_DEBUG_MARFS_CONFIGURATION
#DEBUG   = -DDEBUG
#DEBUG   = -DDEBUG -D_DEBUG_MARFS_CONFIGURATION

# BLUEPRINT=$(PWD)/marfs_configuration_blueprint.cfg
BLUEPRINT=$(PWD)/marfs_cctest.cfg



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
parser: parse_dir
	@ echo; echo "#-- cleaning $(PARSE_DIR)"
	mkdir -p $(PARSE_DIR)/parse-inc
	@ $(MAKE) -C $(PARSE_DIR) clean
	@ $(RM) $(PARSE_DIR)/confpars
	@ $(RM) $(PARSE_DIR)/datapars
	@ echo; echo "#-- making confpars"
	@ $(MAKE) -C $(PARSE_DIR) confpars CFLAGS="$(CFLAGS) $(OPTIONS) $(PARSE_OPT)"
	@ echo; echo "#-- making datapars with $(BLUEPRINT)"
	@ $(MAKE) -C $(PARSE_DIR) datapars CFLAGS="$(CFLAGS) $(OPTIONS) $(PARSE_OPT)" conf=$(BLUEPRINT)
	@ echo; echo "#-- making $(PARSE_DIR)/*.o (again, because makefile deleted them)"
	@ $(MAKE) -C $(PARSE_DIR) $(PARSE_OBJ_files) CFLAGS="$(CFLAGS) $(OPTIONS) $(PARSE_OPT)"

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
	gcc -c $(CFLAGS) $(PARSE_OPT) -DPARSE_DIR=$(PARSE_DIR) $(OPTIONS) -o $@ $<


libconfig.a: parser $(LOG_OBJ) $(OBJ)
	ar -cvr $@ $(PARSE_OBJ)
	ar -cvr $@ $(LOG_OBJ)
	ar -cvr $@ $(OBJ)

lib: libconfig.a


# ---------------------------------------------------------------------------
# Alternate build of Brett's config-tester, using the above
# ---------------------------------------------------------------------------

# CFLAGS  += -I$(PARSE_DIR) -I$(LOG_DIR) $(OPTIONS)
# LDFLAGS += -L.
tester: libconfig.a
	@ echo; echo "#-- building $@"
	# $(CC) -o $@ $(CFLAGS) $(LDFLAGS) test_marfs_configuration.c -lconfig
	$(CC) -o $@ -I$(PARSE_DIR) -I$(LOG_DIR) -DPARSE_DIR=$(PARSE_DIR) $(OPTIONS) $(LOG_OPT) -L. test_marfs_configuration.c -lconfig


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
	$(CC) -c $(PARSE_DIR)/confpars.c -o $(PARSE_DIR)/confpars.o $(OPTIONS) $(PARSE_OPT) $(DEBUG)
	$(CC) -c $(PARSE_DIR)/path-switch.c -o $(PARSE_DIR)/path-switch.o $(OPTIONS) $(PARSE_OPT) $(DEBUG)
	$(CC) -c $(PARSE_DIR)/parsedata.c -o $(PARSE_DIR)/parsedata.o $(OPTIONS) $(PARSE_OPT) $(DEBUG)
	$(CC) -c $(LOG_DIR)/logging.c -o $(LOG_DIR)/logging.o $(OPTIONS) $(LOG_OPT) $(DEBUG)
	$(CC) -I$(PARSE_DIR) -I$(LOG_DIR) -c marfs_configuration.c -o marfs_configuration.o $(OPTIONS) $(PARSE_OPT) $(LOG_OPT) $(DEBUG)
	$(CC) -I$(PARSE_DIR) -I$(LOG_DIR) -c test_marfs_configuration.c -o test_marfs_configuration.o $(OPTIONS) $(PARSE_OPT) $(LOG_OPT) $(DEBUG)
	$(CC) $(LIBS) $(PARSE_DIR)/confpars.o $(PARSE_DIR)/parsedata.o $(PARSE_DIR)/path-switch.o $(LOG_DIR)/logging.o marfs_configuration.o test_marfs_configuration.o -o test_marfs_configuration



clean: parse_dir
	$(RM) $(PARSE_DIR)/*.o
	$(RM) $(LOG_DIR)/*.o
	$(RM) *.o
	$(RM) test_marfs_configuration

pristine: clean
	$(RM) $(PARSE_DIR)/parse-inc/*
	$(RM) $(PARSE_DIR)/confpars
	$(RM) $(PARSE_DIR)/datapars
