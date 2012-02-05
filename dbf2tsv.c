/*
** dbf2tsv.c
** Author: Brian Mottershead, brian@mottershead.us, Feb, 2012.  
** Released into the public domain by the author.
**
** Converts an xBase/dBase format (DBF) file specified by the argument
** to a Tab-Separated Value (TSV) file, written on stdout.
**
** DBF functions based on shapelib (shapelib.maptools.org).
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "dbf.h"

int main(int argc, char **argv){
  DBFHandle dbf_file = NULL; 
  int       width, decimals, i, r;
  char      title[12];

  // Check that there is one argument, the input filename
  if (argc!=2) {
    fprintf(stderr, "Usage: dbf2tsv dbf-file\n");
    return EXIT_FAILURE;
  }

  // Open the DBF file.
  dbf_file = DBFOpen(argv[1], "rb");
  if (dbf_file == NULL) {
    fprintf(stderr, "%s can't be read or is not a DBF file\n", argv[1]);
    return EXIT_FAILURE;
  }
    
  // Header row. Prints names of fields, tab-separated.
  for (i = 0; i < DBFGetFieldCount(dbf_file); i++ ) {
    DBFGetFieldInfo(dbf_file, i, title, &width, &decimals);
    if (i>0)
      printf("\t");
    printf("%s", title);
  }
  printf( "\n" );

  // Data rows. Prints values of fields, tab-separated.
  for (r = 0; r < DBFGetRecordCount(dbf_file); r++) {
    for (i = 0; i < DBFGetFieldCount(dbf_file); i++) {
      if (i>0)
        printf("\t");
      if (!DBFIsAttributeNULL(dbf_file, r, i)) {
        switch (DBFGetFieldInfo(dbf_file, i, title, &width, &decimals)) {
        case FTString:
          // String values not quoted, which will be a problem if 
          // a field value includes a tab.
          printf("%s", DBFReadStringAttribute(dbf_file,r,i));
          break;
        case FTInteger:
          printf("%d", DBFReadIntegerAttribute(dbf_file,r,i));
          break;
        case FTDouble:
          printf("%f", DBFReadDoubleAttribute(dbf_file,r,i));
          break;
        default:
          break;
        }
      }
    }
    printf("\n");
    fflush( stdout );
  }

  // Finished
  DBFClose(dbf_file);
  return EXIT_SUCCESS;
}
