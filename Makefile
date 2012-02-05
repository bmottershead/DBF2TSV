CFLAGS	=	-g -Wall -fPIC
TARGETS = dbf2tsv tsv2dbf

all : $(TARGETS)

dbf2tsv: dbf2tsv.c dbf.c dbf.h
	$(CC) $(CFLAGS) dbf2tsv.c dbf.c -o dbf2tsv

tsv2dbf: tsv2dbf.c dbf.c dbf.h
	$(CC) $(CFLAGS) tsv2dbf.c dbf.c -o tsv2dbf

clean:
	rm -f *.o $(TARGETS)
