#all: logging marfs_base marfs_quota
all: marfs_quota

#CFLAGS += -Wall -g 
CFLAGS += -Wall -g -std=c99 -D_POSIX_C_SOURCE=200809L
#CFLAGS += -g 

#relative paths to main marfs build
MARFS_FUSE = ../../../../fuse/src
LOGGING = ../../../../common/log/src
MARFS_CONFIG = ../../../../common/configuration/src

#PARSER_DIR = /root/atorrez-test/current
##PARSER_DIR = /root/atorrez-test/marfs-config/PA2X
##CONFIG_DIR = /root/atorrez-test/marfs/common/configuration/src

####OBJS += $(MARFS_FUSE)/marfs_base.o $(MARFS_FUSE)/logging.o 
###OBJS += marfs_base.o logging.o 
##OBJS += $(PARSER_DIR)/confpars.o $(PARSER_DIR)/parsedata.o $(PARSER_DIR)/path-switch.o
##OBJS += $(CONFIG_DIR)/marfs_configuration.o

H    = marfs_quota.h

CFLAGS += -I$(MARFS_FUSE)
CFLAGS += -I$(LOGGING)
CFLAGS += -I$(MARFS_CONFIG)
##CFLAGS += -I$(PARSER_DIR)
##CFLAGS += -I$(PARSER_DIR)/parse-inc
##CFLAGS += -I$(CONFIG_DIR)
LDFLAGS += -L$(MARFS_FUSE)
LDFLAGS += -L$(MARFS_CONFIG)

##LIBS += -lgpfs -lcurl -lm
LIBS += -lgpfs -lcurl -lm -lmarfs -lconfig

#For data parser
##PARSE_OPT = -DDATAPARSE
#For data parser
#For debug
DEBUG = -DUSE_STDOUT
#

#marfs_quota: $(H) $(OBJS) marfs_quota.c
#	gcc $(CFLAGS) $(PARSE_OPT) -o marfs_quota $(LDFLAGS) marfs_quota.c $(OBJS) $(LIBS) 
#marfs_quota_debug: $(H) $(OBJS) marfs_quota.c
#	gcc $(CFLAGS) $(PARSE_OPT) $(DEBUG) -o marfs_quota $(LDFLAGS) marfs_quota.c $(OBJS) $(LIBS) 
#marfs_base: $(MARFS_FUSE)/marfs_base.c logging.o
#	gcc -c $(CFLAGS) -o marfs_base.o $(LDFLAGS) $(MARFS_FUSE)/marfs_base.c 
#logging: $(LOGGING)/logging.c $(LOGGING)/logging.h
#	gcc -c $(CFLAGS) -o  logging.o $(LOGGING)/logging.c
#gcc -c $(CFLAGS) -o $@ $<
marfs_quota: $(H) $(OBJS) marfs_quota.c
	gcc $(CFLAGS) $(PARSE_OPT) -o marfs_quota $(LDFLAGS) marfs_quota.c $(OBJS) $(LIBS) 
marfs_quota_debug: $(H) $(OBJS) marfs_quota.c
	gcc $(CFLAGS) $(PARSE_OPT) $(DEBUG) -o marfs_quota $(LDFLAGS) marfs_quota.c $(OBJS) $(LIBS) 










##OBJS = marfs_quota.o
#
##For data parser
#PARSE_OPT = -DDATAPARSE
##For data parser
##For debug
#DEBUG = -DUSE_STDOUT
##
#
#CFLAGS += -g -lgpfs -I /root/atorrez-test/aws4c-latest -l curl ../../../../fuse/src/marfs_base.o ../../../../fuse/src/logging.o -lm
##CFLAGS += -g -lgpfs -D DEBUG -I /root/atorrez-test/aws4c-latest -l curl ../../../../fuse/src/marfs_base.o -lm
##CFLAGS += -lgpfs
#PARSER_DIR = /root/atorrez-test/current
#LDFLAGS += -I../../../../fuse/src -I../../../../common/log/src -I$(PARSER_DIR) -I$(PARSER_DIR)/parse-inc 
#OBJS = $(PARSER_DIR)/confpars.o $(PARSER_DIR)/parsedata.o $(PARSER_DIR)/path-switch.o 
#
## currently a simple top-level FUSE daemon
#marfs_quota: marfs_quota.c
#	gcc $(CFLAGS) $(PARSE_OPT) $(DEBUG) -o marfs_quota $(LDFLAGS) $(OBJS) marfs_quota.c
#
#




%: %.o
	gcc -o $@ $< $(LDFLAGS) $(OBJS) $(LIBS)

%.o: %.c
	gcc $(CFLAGS) -c -o $@ $< 


# Seriously?  Make can't figure this out?
% : %.c
	@ $(MAKE) $*.o
	@ $(MAKE) $*


clean:
	$(RM) *.{o,i}
	$(RM) *~
	$(RM) marfs_quota

