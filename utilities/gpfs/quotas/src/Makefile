all: marfs_quota

#OBJS = marfs_quota.o

#For data parser
PARSE_OPT = -DDATAPARSE
#For data parser

CFLAGS += -g -lgpfs -I /root/atorrez-test/aws4c-latest -l curl ../../../../fuse/src/marfs_base.o -lm
#CFLAGS += -lgpfs
PARSER_DIR = /root/atorrez-test/current
LDFLAGS += -I../../../../fuse/src -I$(PARSER_DIR) -I$(PARSER_DIR)/parse-inc 
OBJS = $(PARSER_DIR)/confpars.o $(PARSER_DIR)/parsedata.o $(PARSER_DIR)/path-switch.o 

# currently a simple top-level FUSE daemon
marfs_quota: marfs_quota.c
	gcc $(CFLAGS) $(PARSE_OPT) -o marfs_quota $(LDFLAGS) $(OBJS) marfs_quota.c






%: %.o
	gcc -o $@ $< $(LDFLAGS) $(OBJS)

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

