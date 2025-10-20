CFLAGS := -Wall $(shell pkg-config --cflags glib-2.0 libpq)
LDFLAGS := $(shell pkg-config --libs glib-2.0 libpq)
IFLAGS := -I../deps/mlib/

all: SrtToPostgres

SrtToPostgres: source = srt.c

SrtToPostgres: 
	cc -o $@ $(source) $(IFLAGS) $(CFLAGS) $(LDFLAGS)
