all: utilities_com 


CFLAGS += -Wall -g -c

# relative paths to main marfs build
MARFS_FUSE = ../../../../fuse/src
LOGGING = ../../../../common/log/src
MARFS_CONFIG = ../../../../common/configuration/src


#OBJS = $(MARFS_FUSE)/marfs_base.o $(MARFS_FUSE)/logging.o 
H    = utilities_common.h

CFLAGS += -I$(MARFS_FUSE)
CFLAGS += -I$(LOGGING)
CFLAGS += -I$(LIBAWS4C)
CFLAGS += -I$(MARFS_CONFIG)
LDFLAGS += -L$(LIBAWS4C)
LDFLAGS += -L$(MARFS_FUSE)
LDFLAGS += -L$(MARFS_CONFIG)
#LIBS += -laws4c -lgpfs -lcurl -lm -lmarfs -lconfig
LIBMARFS = -lmarfs -lxml2 -laws4c -lcurl -lcrypto
LIBS += -laws4c $(LIBMARFS) -lconfig

DEBUG = -DUSE_STDOUT

#Build regular marfs_repack or debug version
utilities_common: $(H) $(OBJS) utilities_common.c 
	gcc $(CFLAGS) $(LDFLAGS) utilities_common.c $(OBJS) $(LIBS)
	ar -cvr libutil_comm.a *.o

# From Jeff's marfs makefile.  This make sure an AWS4C path exists
# in order to build.
pre_req:
ifeq ($(LIBAWS4C),)
	$(error $$LIBAWS4C is empty.  Consider 'make ... LIBAWS4C=<DIR>')
else ifeq ($(wildcard $(LIBAWS4C)/libaws4c.a),)
	$(error couldnt find $$LIBAWS4C (in $(LIBAWS4C)/*).  Consider 'make ... LIBAWS4C=<DIR>')
endif

#Dependancy on AWS4C path
utilities_com: utilities_common.std
utilities_common.std: pre_req
	@ $(MAKE) utilities_common
       
%: %.o
	gcc -o $@ $< $(LDFLAGS) $(LIBS)

%.o: %.c %.h 
	gcc $(CFLAGS) -c -o $@ $<


# Seriously?  Make can't figure this out?
% : %.c
	@ $(MAKE) $*.o
	@ $(MAKE) $*

clean:

	$(RM) *.{o,i,a}
	$(RM) *~
	$(RM) marfs_repack   
