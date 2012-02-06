/*
** tsv2dbf.c
** Author: Brian Mottershead, brian@mottershead.us, Feb, 2012.  
** Released into the public domain by the author.
**
** Converts a Tab-Separated Value (TSV) file to a DBF format file, 
** determining the appropriate field types for the columns, by inspecting
** the column values in the rows.
**
** DBF functions based on shapelib (shapelib.maptools.org).
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include "dbf.h"

#define MAX_COLUMN_WIDTH 4096
#define MAX_LINE_LENGTH  4096
#define MAX_COLUMNS      30

#define  max(a,b) (((a)>=(b))?(a):(b))

int debug = 0;

/*
** Struct for holding columns (headers and field values)
*/

typedef struct column_t {
  DBFFieldType type;
  int          width;
  int          decimals;
  char         value[MAX_COLUMN_WIDTH];
} column;

/*
** Forward declarations
*/

int get_columns(FILE* tsv_file, column** columns, int row);
void dump_column(char* tag, int i, int j, column* col);

/*
** Main
*/

int main( int argc, char ** argv ) {
  FILE*     tsv_file = NULL;
  DBFHandle dbf_file = NULL;
  int       num_columns, i, j;
  column    *columns = NULL;
  column    *fields = NULL;
  char      c;

  // Check that there are two arguments, the input TSV file and
  // the output DBF file.
  if (argc!=3) {
    fprintf(stderr, "Usage: tsv2dbf tsv-file dbf-file\n");
    return EXIT_FAILURE;
  }
  
  // Open DBF file
  dbf_file = DBFCreate(argv[2]);
  if (dbf_file == NULL) {
    fprintf(stderr, "%s file cannot be created\n", argv[2]);
    return EXIT_FAILURE;
  }

  // Open TSV file.
  tsv_file = fopen(argv[1],"r");
  if (tsv_file == NULL) {
    fprintf(stderr, "%s cannot be opened\n", argv[1]);
    DBFClose(dbf_file);
    return EXIT_FAILURE;
  }

  // Read  header row of TSV file for titles.
  num_columns = get_columns(tsv_file, &fields, 1);
  if (num_columns <= 0) {
    fprintf(stderr, "%s can't be read or is not a DBF file\n", argv[1]);
    return EXIT_FAILURE;
  }

  // Initialize type, widths in header array
  for (j=0; j<num_columns; j++) {
    fields[j].type=FTInteger;
    fields[j].width=0;
    fields[j].decimals=0;
  }

  // Loop through TSV data rows, determining the 
  // most restrictive data type for each column which will
  // permit all the actual TSV values to be loaded.
  for (i=1; !feof(tsv_file); i++) {
    int num_field_columns = get_columns(tsv_file, &columns, i+1);

    if (num_field_columns==0)
      continue;
    if (num_field_columns != num_columns) {
      fprintf(stderr, "Wrong number of fields at row %d. Row ignored.", i+1);
      continue;
    } 
    for (j=0; j<num_columns; j++) {
      fields[j].width=max(columns[j].width, fields[j].width);
      switch(columns[j].type) {
      case FTString:
        fields[j].type=FTString;
        break;
      case FTDouble:
        if (fields[j].type==FTInteger) {
          fields[j].type=FTDouble;
          fields[j].decimals=columns[j].decimals;
        } else if (fields[j].type==FTDouble) {
          fields[j].decimals=max(columns[j].decimals, fields[j].decimals);
        } 
        break;
      default:
        break;
      }
    }
  }

  // Make columns that had all empty values into strings.
  for (j=0; j<num_columns; j++) {
    if (fields[j].width==0) {
      fields[j].type=FTString;
      fields[j].width=1;
    }
  }
  
  // Define the fields of the  DBF file.
  for (i=0; i<num_columns; i++) {
    if (debug) 
      dump_column("fld", 0, i, &fields[i]);
    int ret=DBFAddField(dbf_file, fields[i].value, fields[i].type, 
                fields[i].width, fields[i].decimals);
    if (ret==-1) 
      fprintf(stderr,"Error adding field %d to DBF file\n",i);
  }

  // Reset TSV file and skip over header row.
  fseek(tsv_file, 0, SEEK_SET);
  while((c = getc(tsv_file)) != '\n') { /* empty loop */}

  // Read the data rows and write the column values into the DBF file.
  for (i=0; !feof(tsv_file); i++) {
    int num_field_columns = get_columns(tsv_file, &columns, i+1);

    if (num_field_columns != num_columns)
      continue;

    for (j=0; j<num_columns; j++) {
      int ret=0;

      if (columns[j].value[0]==0)
        continue;
      if (debug==2)
        dump_column("row", i, j, &columns[j]);

      switch (fields[j].type) {
      case FTInteger:
        ret=DBFWriteIntegerAttribute(dbf_file, i, j, atoi(columns[j].value));
        break;
      case FTDouble:
        ret=DBFWriteDoubleAttribute(dbf_file, i, j, atof(columns[j].value));
        break;
      case FTString:
        ret=DBFWriteStringAttribute(dbf_file, i, j, columns[j].value);
        break;
      // These won't occur, but are mentioned to avoid a compiler warning.
      case FTLogical:
      case FTInvalid:
        break;
      }
      if (ret==0) 
        fprintf(stderr,"Error writing column %d of row %d\n",j,i+1);
    }
  }
  
  if (debug) {
    for (i=0; i<DBFGetFieldCount(dbf_file); i++) {
      int width, decimals; 
      char title[12];
      DBFFieldType type = DBFGetFieldInfo(dbf_file, i, title, &width, &decimals);
      fprintf(stderr, "%d %s %d %d %d\n", i, title, type, width, decimals);
    }
  }

  // Finish
  free(columns);
  free(fields);
  DBFClose(dbf_file);
  return EXIT_SUCCESS;
}

// Reads a data row from a TSV file and puts fields into
// "columns" array.
int get_columns(FILE* tsv_file, column** columns, int row) {
  int n=0, width=0, decimals=0;
  char c = 0;
  DBFFieldType type = FTInteger;

  // Initialize columns array.
  if (*columns==NULL)
    *columns = malloc(MAX_COLUMNS*sizeof(column));
  memset(*columns, 0, MAX_COLUMNS*sizeof(column));

  // Split line of TSV file into columns and set type, decimals, and width.
  while ((c=getc(tsv_file)) 
         && !feof(tsv_file) 
         && width<MAX_COLUMN_WIDTH 
         && n<MAX_COLUMNS) {
    if (c=='\n' || c=='\t') {
      (*columns)[n].type = type;
      (*columns)[n].width = width;
      if (type==FTDouble) 
        (*columns)[n].decimals = decimals;
      n++;
      type = FTInteger;
      width = 0;
      decimals = 0;
      if (c=='\n')
        break;
    } else {
      (*columns)[n].value[width++] = c;
      if (isdigit(c)) {
        if (type==FTDouble)
          decimals++;
      } else {
        switch(type) {
        case FTInteger:
          type = (c=='.'? FTDouble: FTString);
          break;
        default:
          type = FTString;
        }
      }
    }
  }
  
  if (width>=MAX_COLUMN_WIDTH)
    fprintf(stderr,"Column exceeds maximum width at row %d\n", row);
  if (n>=MAX_COLUMNS)
    fprintf(stderr,"Too many columns at row %d\n", row);
  return  n;
}


// Helper for dump_column, converts field type to string.
char* type_to_str(DBFFieldType t) {
  switch(t) {
  case FTInteger: 
    return "FTInteger";
  case FTString: 
    return "FTString";
  case FTDouble: 
    return "FTDouble";
  case FTLogical: 
    return "FTLogical";
  default:
  case FTInvalid: 
    return "FTInvalid";
  }
}
  
// For debugging, dumps column information on stderr.
void dump_column(char* tag, int i, int j, column* col) {
  fprintf(stderr,"%s [%d,%d] %s %d %d %s\n", tag, i, j, 
          type_to_str(col->type), col->width, col->decimals, col->value);
}
