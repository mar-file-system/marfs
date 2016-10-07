all: marfs_rpack


CFLAGS += -Wall -g 

ifdef DEBUG
   CFLAGS += -DUSE_STDOUT
endif

# relative paths to main marfs build
MARFS_FUSE = ../../../../fuse/src
LOGGING = ../../../../common/log/src
MARFS_CONFIG = ../../../../common/configuration/src
UTILITIES_COMMON = ../../common/src


#OBJS = $(UTILITIES_COMMON)/utilities_common.o
H    = marfs_repack.h

CFLAGS += -I$(MARFS_FUSE)
CFLAGS += -I$(LOGGING)
CFLAGS += -I$(LIBAWS4C)
CFLAGS += -I$(MARFS_CONFIG)
CFLAGS += -I$(UTILITIES_COMMON)
LDFLAGS += -L$(LIBAWS4C)
LDFLAGS += -L$(MARFS_FUSE)
LDFLAGS += -L$(MARFS_CONFIG)
LDFLAGS += -L$(UTILITIES_COMMON)
CFLAGS += -I$(LIBNE)
LDFLAGS += -L$(LIBNE)
LIBMARFS = -lmarfs -lxml2 -laws4c -lcurl -lcrypto
LIBS += -laws4c -lgpfs -lcurl -lm -lmarfs -lconfig -lutil_comm $(LIBMARFS) -lisal -lne

DEBUG = -DUSE_STDOUT

#Build regular marfs_repack or debug version
marfs_repack: $(H) $(OBJS)  marfs_repack.c 
	gcc $(CFLAGS) $(LDFLAGS) -o marfs_repack marfs_repack.c $(OBJS) $(LIBS)
marfs_repack_debug: $(H) $(OBJS)  marfs_repack.c 
	gcc $(CFLAGS) $(LDFLAGS) $(DEBUG) -o marfs_repack marfs_repack.c $(OBJS) $(LIBS)

# From Jeff's marfs makefile.  This make sure an AWS4C path exists
# in order to build.
pre_req:
ifeq ($(LIBAWS4C),)
	$(error $$LIBAWS4C is empty.  Consider 'make ... LIBAWS4C=<DIR>')
else ifeq ($(wildcard $(LIBAWS4C)/libaws4c.a),)
	$(error couldnt find $$LIBAWS4C (in $(LIBAWS4C)/*).  Consider 'make ... LIBAWS4C=<DIR>')
endif

#Dependancy on AWS4C path
marfs_rpack: marfs_repack.std
marfs_repack.std: pre_req
	@ $(MAKE) marfs_repack
       
%: %.o
	gcc -o $@ $< $(LDFLAGS) $(LIBS)

%.o: %.c %.h 
	gcc $(CFLAGS) -c -o $@ $<


# Seriously?  Make can't figure this out?
% : %.c
	@ $(MAKE) $*.o
	@ $(MAKE) $*

clean:

	$(RM) *.{o,i}
	$(RM) *~
	$(RM) marfs_repack
