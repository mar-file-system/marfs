all: marfs_gc

OBJS = /root/atorrez-test/aws4c-latest/aws4c.o

CFLAGS += -Wall -g -lgpfs -I /root/atorrez-test/aws4c-latest -lcurl ../../../../fuse/src/marfs_base.o -lm
LDFLAGS += -I../../../../fuse/src 

# currently a simple top-level FUSE daemon
marfs_gc: marfs_gc.c marfs_gc.h
	gcc $(CFLAGS) -o marfs_gc $(LDFLAGS) $(OBJS) marfs_gc.c






%: %.o
	gcc -o $@ $< $(LDFLAGS)

%.o: %.c 
	gcc $(CFLAGS) -c -o $@ $<


# Seriously?  Make can't figure this out?
% : %.c
	@ $(MAKE) $*.o
	@ $(MAKE) $*


clean:
	$(RM) *.{o,i}
	$(RM) *~
	$(RM) marfs_gc   

