CFLAGS := -Wall $(shell pkg-config --cflags libpq sqlite3)
LDFLAGS := $(shell pkg-config --libs libpq sqlite3)

all:AozoraToPostgres 

AozoraToPostgres: source = ./AozoraToPostgres.c

AozoraToPostgres: 
	cc -o $@ $(source) $(CFLAGS) $(LDFLAGS)
