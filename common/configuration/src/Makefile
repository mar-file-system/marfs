SHELL = /bin/bash
CC = gcc

PARSE_DIR = ../../../../../PA2X/PA2X
LOG_DIR =   ../../log/src

#LIBS = -pthread
LIBS =
OPTIONS = -D_GNU_SOURCE -g
PARSE_OPT = -DDATAPARSE
LOG_OPT = -DUSE_STDOUT
DEBUG   = -D_DEBUG_MARFS_CONFIGURATION
#DEBUG   = -DDEBUG
#DEBUG   = -DDEBUG -D_DEBUG_MARFS_CONFIGURATION

all:   test_marfs_configuration
debug: test_marfs_configuration-d


test_marfs_configuration:
	$(CC) -c $(PARSE_DIR)/confpars.c -o $(PARSE_DIR)/confpars.o $(OPTIONS) $(PARSE_OPT)
	$(CC) -c $(PARSE_DIR)/path-switch.c -o $(PARSE_DIR)/path-switch.o $(OPTIONS) $(PARSE_OPT)
	$(CC) -c $(PARSE_DIR)/parsedata.c -o $(PARSE_DIR)/parsedata.o $(OPTIONS) $(PARSE_OPT)
	$(CC) -c $(LOG_DIR)/logging.c -o $(LOG_DIR)/logging.o $(OPTIONS) $(LOG_OPT)
	$(CC) -I$(PARSE_DIR) -I$(LOG_DIR) -c marfs_configuration.c -o marfs_configuration.o $(OPTIONS) $(PARSE_OPT) $(LOG_OPT)
	$(CC) -I$(PARSE_DIR) -I$(LOG_DIR) -c test_marfs_configuration.c -o test_marfs_configuration.o $(OPTIONS) $(PARSE_OPT) $(LOG_OPT)
	$(CC) $(LIBS) $(PARSE_DIR)/confpars.o $(PARSE_DIR)/parsedata.o $(PARSE_DIR)/path-switch.o $(LOG_DIR)/logging.o marfs_configuration.o test_marfs_configuration.o -o test_marfs_configuration

test_marfs_configuration-d:
	$(CC) -c $(PARSE_DIR)/confpars.c -o $(PARSE_DIR)/confpars.o $(OPTIONS) $(PARSE_OPT) $(DEBUG)
	$(CC) -c $(PARSE_DIR)/path-switch.c -o $(PARSE_DIR)/path-switch.o $(OPTIONS) $(PARSE_OPT) $(DEBUG)
	$(CC) -c $(PARSE_DIR)/parsedata.c -o $(PARSE_DIR)/parsedata.o $(OPTIONS) $(PARSE_OPT) $(DEBUG)
	$(CC) -c $(LOG_DIR)/logging.c -o $(LOG_DIR)/logging.o $(OPTIONS) $(LOG_OPT) $(DEBUG)
	$(CC) -I$(PARSE_DIR) -I$(LOG_DIR) -c marfs_configuration.c -o marfs_configuration.o $(OPTIONS) $(PARSE_OPT) $(LOG_OPT) $(DEBUG)
	$(CC) -I$(PARSE_DIR) -I$(LOG_DIR) -c test_marfs_configuration.c -o test_marfs_configuration.o $(OPTIONS) $(PARSE_OPT) $(LOG_OPT) $(DEBUG)
	$(CC) $(LIBS) $(PARSE_DIR)/confpars.o $(PARSE_DIR)/parsedata.o $(PARSE_DIR)/path-switch.o $(LOG_DIR)/logging.o marfs_configuration.o test_marfs_configuration.o -o test_marfs_configuration

clean:
	-rm $(PARSE_DIR)/*.o $(LOG_DIR)/*.o *.o test_marfs_configuration

pristine:
	-rm $(PARSE_DIR)/*.o $(LOG_DIR)/*.o *.o test_marfs_configuration $(PARSE_DIR)/parse-inc/*
