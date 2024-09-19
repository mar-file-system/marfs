CC := gcc
#CFLAGS := -std=gnu99 -Wall -Wextra -Og -g
CFLAGS := -std=gnu99 -Wall -Wextra -Og -g
LDFLAGS := 
LDLIBS := -lpthread

# Should be defined according to the MarFS install prefix, i.e., the argument
# passed to `./configure --prefix=` when installing MarFS
MARFS_PREFIX := /opt/software/campaign/install

# Target the MarFS-relevant include and source dirs for symbol definitions
MARFS_CFLAGS := -I$(MARFS_PREFIX)/include 
#MARFS_SRC_INCLUDE := -I/$(MARFS_PREFIX)/marfs/src/
MARFS_SRC_INCLUDE := -I../ -I../api
INCLUDE_XML := -I/usr/include/libxml2/
MARFS_INCLUDE := $(MARFS_SRC_INCLUDE) $(INCLUDE_XML)

# Link against MarFS libs using the system-specific installation prefix
MARFS_LDFLAGS := -L$(MARFS_PREFIX)/lib

# mustang_engine will be called by the mustang frontend/wrapper, but perform
# all the significant computational work via the thread routines
all: mustang_engine

# The executable recipe
mustang_engine: mustang_engine.c libmustang.a
	$(CC) $(CFLAGS) $(MARFS_CFLAGS) $(MARFS_INCLUDE) $< -o $@ $(LDFLAGS) \
		-lpthread -L. -lmustang $(MARFS_LDFLAGS) \
		-Wl,-rpath,$(MARFS_PREFIX)/lib -lmarfs -llogging

# A static library bundling all the data structures and threading code that the
# mustang engine relies on. mustang *must* still be invoked via the
# mustang_engine executable and/or via its "mustang" frontend---use in other
# applications by simply linking against the static library is insufficient
libmustang.a: mustang_threading.o hashtable.o id_cache.o task_queue.o
	ar rcs $@ $^

# thread_main and retcode_ll (specifically retcode_ll_flush) need to link 
# against the logging library. So, create special rules for those object files.
mustang_threading.o: mustang_threading.c mustang_threading.h mustang_logging.h
	$(CC) $(CFLAGS) $(MARFS_INCLUDE) -c $< -llogging

%.o: %.c %.h
	$(CC) $(CFLAGS) $(MARFS_INCLUDE) $< -c $(LDLIBS)

all: mustang_engine

# Copy binaries and frontend script to MarFS install location to also make them
# accessible in PATH
install: mustang_engine
	install ./$< $(MARFS_PREFIX)/bin/
	install ./mustang $(MARFS_PREFIX)/bin/
	install ./libmustang.a $(MARFS_PREFIX)/lib/

clean:
	rm -f libmustang.a ./*.o mustang_engine ./*.tar.gz
	rm -f ./mustang-output-* ./*.log

archive:
	make clean
	./archive.sh
