all: marfs_pack


CFLAGS += -Wall -g 

# relative paths to main marfs build
MARFS_FUSE = ../../../../fuse/src
LOGGING = ../../../../common/log/src
MARFS_CONFIG = ../../../../common/configuration/src


#OBJS = $(MARFS_FUSE)/marfs_base.o $(MARFS_FUSE)/logging.o 
H    = marfs_packer.h

CFLAGS += -I$(MARFS_FUSE)
CFLAGS += -I$(LOGGING)
CFLAGS += -I$(LIBAWS4C)
CFLAGS += -I$(MARFS_CONFIG)
LDFLAGS += -L$(LIBAWS4C)
LDFLAGS += -L$(MARFS_FUSE)
LDFLAGS += -L$(MARFS_CONFIG)
LIBS += -laws4c -lgpfs -lcurl -lm -lmarfs -lconfig

DEBUG = -DUSE_STDOUT

#Build regular marfs_packer or debug version
marfs_packer: $(H) $(OBJS)  marfs_packer.c 
	gcc $(CFLAGS) $(LDFLAGS) -o marfs_packer marfs_packer.c $(OBJS) $(LIBS)
marfs_packer_debug: $(H) $(OBJS)  marfs_packer.c 
	gcc $(CFLAGS) $(LDFLAGS) $(DEBUG) -o marfs_packer marfs_packer.c $(OBJS) $(LIBS)

# From Jeff's marfs makefile.  This make sure an AWS4C path exists
# in order to build.
pre_req:
ifeq ($(LIBAWS4C),)
	$(error $$LIBAWS4C is empty.  Consider 'make ... LIBAWS4C=<DIR>')
else ifeq ($(wildcard $(LIBAWS4C)/libaws4c.a),)
	$(error couldnt find $$LIBAWS4C (in $(LIBAWS4C)/*).  Consider 'make ... LIBAWS4C=<DIR>')
endif

#Dependancy on AWS4C path
marfs_pack: marfs_packer.std
marfs_packer.std: pre_req
	@ $(MAKE) marfs_packer
       
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
	$(RM) marfs_packer   
