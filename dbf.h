#ifndef DBF_H_INCLUDED
#define DBF_H_INCLUDED
/******************************************************************************
 * Copyright (c) 1999, Frank Warmerdam
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 *
 * (Extracted from shapelib by Brian Mottershead)
 *
 ******************************************************************************/

#include <stdio.h>

#define TRIM_DBF_WHITESPACE
#define DISABLE_MULTIPATCH_MEASURE

typedef struct {
  FILE*   fp;
  int     nRecords;
  int     nRecordLength;
  int     nHeaderLength;
  int     nFields;
  int     *panFieldOffset;
  int     *panFieldSize;
  int     *panFieldDecimals;
  char    *pachFieldType;
  char    *pszHeader;
  int     nCurrentRecord;
  int     bCurrentRecordModified;
  char    *pszCurrentRecord;
  int     nWorkFieldLength;
  char    *pszWorkField;
  int     bNoHeader;
  int     bUpdated;
  double  dfDoubleField;
  int     iLanguageDriver;
  char    *pszCodePage;
} DBFInfo;

typedef DBFInfo* DBFHandle;

typedef enum {
  FTString,
  FTInteger,
  FTDouble,
  FTLogical,
  FTInvalid
} DBFFieldType;

DBFHandle DBFOpen(const char* filename, const char* pszAccess);
DBFHandle DBFCreate(const char* filename);
DBFHandle DBFCreateEx(const char* filename, const char* pszCodePage);
int DBFGetFieldCount(DBFHandle);
int DBFGetRecordCount(DBFHandle);
int DBFAddField(DBFHandle, const char* field, DBFFieldType, int nWidth, int nDecimals);
int DBFAddNativeFieldType(DBFHandle,const char* field,char chType, int nWidth, int nDecimals);
int DBFDeleteField(DBFHandle, int iField);
int DBFReorderFields(DBFHandle, int* panMap);
int DBFAlterFieldDefn(DBFHandle, int iField,const char* field,char chType,int nWidth,int nDecimals);
DBFFieldType DBFGetFieldInfo(DBFHandle, int iField, char* field, int* pnWidth, int* pnDecimals);
int DBFGetFieldIndex(DBFHandle, const char *field);
int DBFReadIntegerAttribute(DBFHandle, int iShape, int iField);
double DBFReadDoubleAttribute(DBFHandle, int iShape, int iField);
const char* DBFReadStringAttribute(DBFHandle, int iShape, int iField);
const char* DBFReadLogicalAttribute(DBFHandle, int iShape, int iField);
int DBFIsAttributeNULL(DBFHandle, int iShape, int iField);
int DBFWriteIntegerAttribute(DBFHandle, int iShape, int iField, int nFieldValue);
int DBFWriteDoubleAttribute(DBFHandle, int iShape, int iField, double dFieldValue);
int DBFWriteStringAttribute(DBFHandle, int iShape, int iField, const char* pszFieldValue);
int DBFWriteNULLAttribute(DBFHandle, int iShape, int iField);
int DBFWriteLogicalAttribute(DBFHandle, int iShape, int iField,const char lFieldValue);
int DBFWriteAttributeDirectly(DBFHandle, int hEntity, int iField, void * pValue);
const char* DBFReadTuple(DBFHandle, int hEntity);
int DBFWriteTuple(DBFHandle, int hEntity, void* pRawTuple);
int DBFIsRecordDeleted(DBFHandle, int iShape);
int DBFMarkRecordDeleted(DBFHandle, int iShape, int bIsDeleted);
DBFHandle DBFCloneEmpty(DBFHandle, const char* pszFilename);
void DBFClose(DBFHandle);
void DBFUpdateHeader(DBFHandle);
char DBFGetNativeFieldType(DBFHandle, int iField);
const char* DBFGetCodePage(DBFHandle);

#endif /* DBF_H_INCLUDED */
