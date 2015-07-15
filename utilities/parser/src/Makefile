SHELL = /bin/bash
CC = gcc

LOG_OPT = -lm
MD5_OPT = -lssl -lcrypto

#LIBS = -pthread
LIBS =
OPTIONS = -D_GNU_SOURCE -g
PARSE_OPT = -DDATAPARSE
DEBUG   = -DDEBUG

all:	datapars checksum
base:   confpars
debug:  confpars-d datapars-d

examples: example-1 example-2 example-3 example-4

confpars:
	$(CC) -c confpars.c -o confpars.o $(OPTIONS)
	$(CC) -c path-switch.c -o path-switch.o $(OPTIONS)
	$(CC) -c mainpars.c -o mainpars.o $(OPTIONS)
	$(CC) $(LIBS) confpars.o path-switch.o mainpars.o -o confpars $(OPTIONS)
	-rm *.o

datapars:
	$(shell ./confpars '$(conf)')
	$(CC) -c confpars.c -o valpars.o $(OPTIONS) $(PARSE_OPT)
	$(CC) -c path-switch.c -o pars-path-switch.o $(OPTIONS) $(PARSE_OPT)
	$(CC) -c parsedata.c -o parsedata.o $(OPTIONS) $(PARSE_OPT)
	$(CC) -c mainpars.c -o maindatapars.o $(OPTIONS) $(PARSE_OPT)
	$(CC) $(LIBS) valpars.o pars-path-switch.o parsedata.o maindatapars.o -o datapars
	-rm *.o

confpars-d:
	$(CC) -c confpars.c -o confpars.o $(OPTIONS) $(DEBUG)
	$(CC) -c path-switch.c -o path-switch.o $(OPTIONS) $(DEBUG)
	$(CC) -c mainpars.c -o mainpars.o $(OPTIONS) $(DEBUG)
	$(CC) $(LIBS) confpars.o path-switch.o mainpars.o -o confpars $(DEBUG)
	-rm *.o

datapars-d:
	$(CC) -c confpars.c -o valpars.o $(OPTIONS) $(PARSE_OPT) $(DEBUG)
	$(CC) -c mainpars.c -o maindatapars.o $(OPTIONS) $(PARSE_OPT) $(DEBUG)
	$(CC) $(LIBS) valpars.o maindatapars.o -o datapars
	-rm *.o

checksum:
	$(CC) -c checksum.c -o checksum.o $(MD5_OPT)
	$(CC) $(LIBS) checksum.o -o checksum $(MD5_OPT)
	-rm *.o

example-1:
	$(CC) -c example-1.c -o example-1.o
	$(CC) -c parsedata.c -o parsedata.o $(OPTIONS) $(PARSE_OPT)
	$(CC) $(LIBS) example-1.o parsedata.o -o example-1
	-rm example-1.o parsedata.o

example-2:
	$(CC) -c confpars.c -o confpars.o $(OPTIONS) $(PARSE_OPT)
	$(CC) -c path-switch.c -o path-switch.o $(OPTIONS) $(PARSE_OPT)
	$(CC) -c parsedata.c -o parsedata.o $(OPTIONS) $(PARSE_OPT)
	$(CC) -c example-2.c -o example-2.o $(OPTIONS) $(PARSE_OPT)
	$(CC) $(LIBS) confpars.o parsedata.o path-switch.o example-2.o -o example-2
	-rm *.o

example-3:
	$(CC) -c confpars.c -o confpars.o $(OPTIONS) $(PARSE_OPT)
	$(CC) -c path-switch.c -o path-switch.o $(OPTIONS) $(PARSE_OPT)
	$(CC) -c parsedata.c -o parsedata.o $(OPTIONS) $(PARSE_OPT)
	$(CC) -c example-3.c -o example-3.o $(OPTIONS) $(PARSE_OPT)
	$(CC) $(LIBS) confpars.o parsedata.o path-switch.o example-3.o -o example-3
	-rm *.o

example-4:
	$(CC) -c confpars.c -o confpars.o $(OPTIONS) $(PARSE_OPT)
	$(CC) -c path-switch.c -o path-switch.o $(OPTIONS) $(PARSE_OPT)
	$(CC) -c parsedata.c -o parsedata.o $(OPTIONS) $(PARSE_OPT)
	$(CC) -c example-4.c -o example-4.o $(OPTIONS) $(PARSE_OPT)
	$(CC) $(LIBS) confpars.o parsedata.o path-switch.o example-4.o -o example-4
	-rm *.o

clean:
	-rm -f *.o datapars checksum example-1 example-2 example-3 example-4

pristine:
	-rm -f *.o confpars datapars checksum example-1 example-2 example-3 example-4 ./parse-inc/*
