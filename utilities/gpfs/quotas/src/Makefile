#all: logging marfs_base marfs_quota
all: marfs_quota

CFLAGS += -Wall -g -std=c99 -D_POSIX_C_SOURCE=200809L

#relative paths to main marfs build
MARFS_FUSE = ../../../../fuse/src
LOGGING = ../../../../common/log/src
MARFS_CONFIG = ../../../../common/configuration/src

H    = marfs_quota.h

CFLAGS += -I$(MARFS_FUSE)
CFLAGS += -I$(LOGGING)
CFLAGS += -I$(MARFS_CONFIG)
LDFLAGS += -L$(MARFS_FUSE)
LDFLAGS += -L$(MARFS_CONFIG)
LDFLAGS += -L$(AWS4C)

LIBMARFS = -lmarfs -lxml2 -laws4c -lcurl -lcrypto
LIBS += -lgpfs -lcurl -lm $(LIBMARFS) -lconfig

#For debug 
ifdef DEBUG
  CFLAGS += -DUSE_STDOUT
endif 

ifdef USE_MC
	LIBS += -lisal -lne
	LDFLAGS += -L$(LIBNE)
endif

# Use this for specific make (make marfs_quota_debug)
DEBUG = -DUSE_STDOUT
#
marfs_quota: $(H) $(OBJS) marfs_quota.c
	gcc $(CFLAGS) $(PARSE_OPT) -o marfs_quota $(LDFLAGS) marfs_quota.c $(OBJS) $(LIBS) 
marfs_quota_debug: $(H) $(OBJS) marfs_quota.c
	gcc $(CFLAGS) $(PARSE_OPT) $(DEBUG) -o marfs_quota $(LDFLAGS) marfs_quota.c $(OBJS) $(LIBS) 

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

