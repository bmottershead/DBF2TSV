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

#include "dbf.h"
#include <assert.h>
#include <ctype.h>
#include <limits.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>

#ifndef FALSE
#define FALSE       0
#define TRUE        1
#endif

#define XBASE_FLDHDR_SZ 32

static void *SfRealloc(void *pMem, int nNewSize) {
  return (void *) (pMem == NULL ? malloc(nNewSize) : realloc(pMem, nNewSize));
}

static void DBFWriteHeader(DBFHandle psDBF) {
  unsigned char abyHeader[XBASE_FLDHDR_SZ];
  int i;

  if (!psDBF->bNoHeader)
    return;
  psDBF->bNoHeader = FALSE;

  /* Initialize the file header information. */
  for (i = 0; i < XBASE_FLDHDR_SZ; i++)
    abyHeader[i] = 0;

  /* write out a dummy date */
  abyHeader[0] = 0x03;  /* memo field? - just copying */
  abyHeader[1] = 95;    /* YY */
  abyHeader[2] = 7;     /* MM */
  abyHeader[3] = 26;    /* DD */

  /* record count preset at zero */
  abyHeader[8] = (unsigned char) (psDBF->nHeaderLength % 256);
  abyHeader[9] = (unsigned char) (psDBF->nHeaderLength / 256);
  abyHeader[10] = (unsigned char) (psDBF->nRecordLength % 256);
  abyHeader[11] = (unsigned char) (psDBF->nRecordLength / 256);
  abyHeader[29] = (unsigned char) (psDBF->iLanguageDriver);

  /* Write the initial 32 byte file header, and all the field desc */
  fseek(psDBF->fp, 0, 0);
  fwrite(abyHeader, XBASE_FLDHDR_SZ, 1, psDBF->fp);
  fwrite(psDBF->pszHeader, XBASE_FLDHDR_SZ, psDBF->nFields, psDBF->fp);

  /* Write out the newline character if there is room for it. */
  if (psDBF->nHeaderLength > 32 * psDBF->nFields + 32) {
    char cNewline = 0x0d;
    fwrite(&cNewline, 1, 1, psDBF->fp);
  }
}

/* DBFFlushRecord */
static int DBFFlushRecord(DBFHandle psDBF) {
  unsigned long nRecordOffset;

  if (psDBF->bCurrentRecordModified && psDBF->nCurrentRecord > -1) {
    psDBF->bCurrentRecordModified = FALSE;
    nRecordOffset = psDBF->nRecordLength * (unsigned long) psDBF->nCurrentRecord
      + psDBF->nHeaderLength;
    if (fseek(psDBF->fp, nRecordOffset, 0) != 0
        || fwrite(psDBF->pszCurrentRecord,psDBF->nRecordLength, 1, psDBF->fp) != 1) {
      char szMessage[128];
      sprintf(szMessage, "Failure writing DBF record %d.", psDBF->nCurrentRecord);
      fprintf(stderr, szMessage);
      return FALSE;
    }
  }
  return TRUE;
}

/* DBFLoadRecord */
static int DBFLoadRecord(DBFHandle psDBF, int iRecord) {
  if (psDBF->nCurrentRecord != iRecord) {
    unsigned long nRecordOffset;
    char szMessage[128];

    if (!DBFFlushRecord(psDBF))
      return FALSE;
    nRecordOffset = psDBF->nRecordLength * (unsigned long) iRecord + psDBF->nHeaderLength;
    if (fseek(psDBF->fp, nRecordOffset, SEEK_SET) != 0) {
      sprintf(szMessage, "fseek(%ld) failed on DBF file.\n",(long) nRecordOffset);
      fprintf(stderr,szMessage);
      return FALSE;
    }
    if (fread(psDBF->pszCurrentRecord,psDBF->nRecordLength, 1, psDBF->fp) != 1) {
      sprintf(szMessage, "fread(%d) failed on DBF file.\n",psDBF->nRecordLength);
      fprintf(stderr,szMessage);
      return FALSE;
    }
    psDBF->nCurrentRecord = iRecord;
  }
  return TRUE;
}

/* DBFUpdateHeader */
void  DBFUpdateHeader(DBFHandle psDBF) {
  unsigned char abyFileHeader[32];

  if (psDBF->bNoHeader)
    DBFWriteHeader(psDBF);
  DBFFlushRecord(psDBF);
  fseek(psDBF->fp, 0, 0);
  fread(abyFileHeader, 32, 1, psDBF->fp);
  abyFileHeader[4] = (unsigned char) (psDBF->nRecords % 256);
  abyFileHeader[5] = (unsigned char) ((psDBF->nRecords / 256) % 256);
  abyFileHeader[6] = (unsigned char) ((psDBF->nRecords / (256 * 256)) % 256);
  abyFileHeader[7] = (unsigned char) ((psDBF->nRecords / (256 * 256 * 256)) % 256);
  fseek(psDBF->fp, 0, 0);
  fwrite(abyFileHeader, 32, 1, psDBF->fp);
  fflush(psDBF->fp);
}

/* DBFOpen */
DBFHandle  DBFOpen(const char *pszFilename, const char *pszAccess) {
  DBFHandle psDBF;
  FILE* pfCPG;
  unsigned char *pabyBuf;
  int nFields, nHeadLen, iField, i;
  char *pszBasename, *pszFullname;
  int nBufSize = 500;

  /* We only allow the access strings "rb" and "r+". */
  if (strcmp(pszAccess, "r") != 0 && strcmp(pszAccess, "r+") != 0
      && strcmp(pszAccess, "rb") != 0 && strcmp(pszAccess, "rb+") != 0
      && strcmp(pszAccess, "r+b") != 0)
    return (NULL);

  if (strcmp(pszAccess, "r") == 0)
    pszAccess = "rb";
  if (strcmp(pszAccess, "r+") == 0)
    pszAccess = "rb+";

  /* Compute the base (layer) name.  If there is any extension */
  /* on the passed in filename we will strip it off. */
  pszBasename = (char *) malloc(strlen(pszFilename) + 5);
  strcpy(pszBasename, pszFilename);
  for (i = strlen(pszBasename) - 1;
       i > 0 && pszBasename[i] != '.' && pszBasename[i] != '/'
         && pszBasename[i] != '\\'; i--) {
  }

  if (pszBasename[i] == '.')
    pszBasename[i] = '\0';

  pszFullname = (char *) malloc(strlen(pszBasename) + 5);
  sprintf(pszFullname, "%s.dbf", pszBasename);
  psDBF = (DBFHandle) calloc(1, sizeof(DBFInfo));
  psDBF->fp = fopen(pszFullname, pszAccess);
  if (psDBF->fp == NULL) {
    sprintf(pszFullname, "%s.DBF", pszBasename);
    psDBF->fp = fopen(pszFullname, pszAccess);
  }
  sprintf(pszFullname, "%s.cpg", pszBasename);
  pfCPG = fopen(pszFullname, "r");
  if (pfCPG == NULL) {
    sprintf(pszFullname, "%s.CPG", pszBasename);
    pfCPG = fopen(pszFullname, "r");
  }

  free(pszBasename);
  free(pszFullname);

  if (psDBF->fp == NULL) {
    free(psDBF);
    if (pfCPG)
      fclose(pfCPG);
    return (NULL);
  }

  psDBF->bNoHeader = FALSE;
  psDBF->nCurrentRecord = -1;
  psDBF->bCurrentRecordModified = FALSE;

  /* Read Table Header info */
  pabyBuf = (unsigned char *) malloc(nBufSize);
  if (fread(pabyBuf, 32, 1, psDBF->fp) != 1) {
    fclose(psDBF->fp);
    if (pfCPG)
      fclose(pfCPG);
    free(pabyBuf);
    free(psDBF);
    return NULL;
  }

  psDBF->nRecords =
    pabyBuf[4] + pabyBuf[5] * 256 + pabyBuf[6] * 256 * 256 +
    pabyBuf[7] * 256 * 256 * 256;

  psDBF->nHeaderLength = nHeadLen = pabyBuf[8] + pabyBuf[9] * 256;
  psDBF->nRecordLength = pabyBuf[10] + pabyBuf[11] * 256;
  psDBF->iLanguageDriver = pabyBuf[29];

  if (nHeadLen < 32) {
    fclose(psDBF->fp);
    if (pfCPG)
      fclose(pfCPG);
    free(pabyBuf);
    free(psDBF);
    return NULL;
  }

  psDBF->nFields = nFields = (nHeadLen - 32) / 32;
  psDBF->pszCurrentRecord = (char *) malloc(psDBF->nRecordLength);

  /* Figure out the code page from the LDID and CPG */
  psDBF->pszCodePage = NULL;
  if (pfCPG) {
    size_t n;
    memset(pabyBuf, 0, nBufSize);
    fread(pabyBuf, nBufSize - 1, 1, pfCPG);
    n = strcspn((char *) pabyBuf, "\n\r");
    if (n > 0) {
      pabyBuf[n] = '\0';
      psDBF->pszCodePage = (char *) malloc(n + 1);
      memcpy(psDBF->pszCodePage, pabyBuf, n + 1);
    }
    fclose(pfCPG);
  }
  if (psDBF->pszCodePage == NULL && pabyBuf[29] != 0) {
    sprintf((char *) pabyBuf, "LDID/%d", psDBF->iLanguageDriver);
    psDBF->pszCodePage = (char *) malloc(strlen((char *) pabyBuf) + 1);
    strcpy(psDBF->pszCodePage, (char *) pabyBuf);
  }


  /* Read in Field Definitions */
  pabyBuf = (unsigned char *) SfRealloc(pabyBuf, nHeadLen);
  psDBF->pszHeader = (char *) pabyBuf;

  fseek(psDBF->fp, 32, 0);
  if (fread(pabyBuf, nHeadLen - 32, 1, psDBF->fp) != 1) {
    fclose(psDBF->fp);
    free(pabyBuf);
    free(psDBF->pszCurrentRecord);
    free(psDBF);
    return NULL;
  }

  psDBF->panFieldOffset = (int *) malloc(sizeof(int) * nFields);
  psDBF->panFieldSize = (int *) malloc(sizeof(int) * nFields);
  psDBF->panFieldDecimals = (int *) malloc(sizeof(int) * nFields);
  psDBF->pachFieldType = (char *) malloc(sizeof(char) * nFields);

  for (iField = 0; iField < nFields; iField++) {
    unsigned char *pabyFInfo;

    pabyFInfo = pabyBuf + iField * 32;
    if (pabyFInfo[11] == 'N' || pabyFInfo[11] == 'F') {
      psDBF->panFieldSize[iField] = pabyFInfo[16];
      psDBF->panFieldDecimals[iField] = pabyFInfo[17];
    } else {
      psDBF->panFieldSize[iField] = pabyFInfo[16];
      psDBF->panFieldDecimals[iField] = 0;
    }
    psDBF->pachFieldType[iField] = (char) pabyFInfo[11];
    if (iField == 0)
      psDBF->panFieldOffset[iField] = 1;
    else
      psDBF->panFieldOffset[iField] =
        psDBF->panFieldOffset[iField - 1] +
        psDBF->panFieldSize[iField - 1];
  }

  return (psDBF);
}

/* DBFClose */
void  DBFClose(DBFHandle psDBF) {
  if (psDBF != NULL) {
    if (psDBF->bNoHeader)
      DBFWriteHeader(psDBF);
    DBFFlushRecord(psDBF);
    if (psDBF->bUpdated)
      DBFUpdateHeader(psDBF);
    fclose(psDBF->fp);
    free(psDBF->panFieldOffset);
    free(psDBF->panFieldSize);
    free(psDBF->panFieldDecimals);
    free(psDBF->pachFieldType);
    free(psDBF->pszWorkField);
    free(psDBF->pszHeader);
    free(psDBF->pszCurrentRecord);
    free(psDBF->pszCodePage);
    free(psDBF);
  }
}

/* DBFCreate */
DBFHandle  DBFCreate(const char *pszFilename) {
  return DBFCreateEx(pszFilename, "LDID/87");   // 0x57
}

/* DBFCreateEx */
DBFHandle  DBFCreateEx(const char *pszFilename, const char *pszCodePage) {
  DBFHandle psDBF;
  FILE* fp;
  char *pszFullname, *pszBasename;
  int i, ldid = -1;
  char chZero = '\0';

  /* Compute the base (layer) name.  If there is any extension */
  /* on the passed in filename we will strip it off. */
  pszBasename = (char *) malloc(strlen(pszFilename) + 5);
  strcpy(pszBasename, pszFilename);
  for (i = strlen(pszBasename) - 1; i > 0 && pszBasename[i] != '.' && pszBasename[i] != '/'
         && pszBasename[i] != '\\'; i--) {/* empty loop */ }
  if (pszBasename[i] == '.')
    pszBasename[i] = '\0';
  pszFullname = (char *) malloc(strlen(pszBasename) + 5);
  sprintf(pszFullname, "%s.dbf", pszBasename);

  /* Create the file. */
  fp = fopen(pszFullname, "wb");
  if (fp == NULL)
    return (NULL);
  fwrite(&chZero, 1, 1, fp);
  fclose(fp);

  fp = fopen(pszFullname, "rb+");
  if (fp == NULL)
    return (NULL);

  sprintf(pszFullname, "%s.cpg", pszBasename);
  if (pszCodePage != NULL) {
    if (strncmp(pszCodePage, "LDID/", 5) == 0) {
      ldid = atoi(pszCodePage + 5);
      if (ldid > 255)
        ldid = -1;  // don't use 0 to indicate out of range as LDID/0 is a valid one
    }
    if (ldid < 0) {
      FILE* fpCPG = fopen(pszFullname, "w");
      fwrite((char *) pszCodePage, strlen(pszCodePage), 1, fpCPG);
      fclose(fpCPG);
    }
  }
  if (pszCodePage == NULL || ldid >= 0)
    remove(pszFullname);
  free(pszBasename);
  free(pszFullname);

  /* Create the info structure. */
  psDBF = (DBFHandle) calloc(1, sizeof(DBFInfo));
  psDBF->fp = fp;
  psDBF->nRecords = 0;
  psDBF->nFields = 0;
  psDBF->nRecordLength = 1;
  psDBF->nHeaderLength = 33;
  psDBF->panFieldOffset = NULL;
  psDBF->panFieldSize = NULL;
  psDBF->panFieldDecimals = NULL;
  psDBF->pachFieldType = NULL;
  psDBF->pszHeader = NULL;
  psDBF->nCurrentRecord = -1;
  psDBF->bCurrentRecordModified = FALSE;
  psDBF->pszCurrentRecord = NULL;
  psDBF->bNoHeader = TRUE;
  psDBF->iLanguageDriver = ldid > 0 ? ldid : 0;
  psDBF->pszCodePage = NULL;
  if (pszCodePage) {
    psDBF->pszCodePage = (char *) malloc(strlen(pszCodePage) + 1);
    strcpy(psDBF->pszCodePage, pszCodePage);
  }
  return (psDBF);
}

/* DBFAddField */
int  DBFAddField(DBFHandle psDBF, const char *pszFieldName,
                 DBFFieldType eType, int nWidth, int nDecimals) {
  char chNativeType = 'C';

  if (eType == FTLogical)
    chNativeType = 'L';
  else if (eType == FTString)
    chNativeType = 'C';
  else
    chNativeType = 'N';
  return DBFAddNativeFieldType(psDBF, pszFieldName, chNativeType,nWidth, nDecimals);
}

/* DBFGetNullCharacter */
static char DBFGetNullCharacter(char chType) {
  switch (chType) {
  case 'N':
  case 'F':
    return '*';
  case 'D':
    return '0';
  case 'L':
    return '?';
  default:
    return ' ';
  }
}

/* DBFAddNativeFieldType */
int  DBFAddNativeFieldType(DBFHandle psDBF, const char *pszFieldName,
                           char chType, int nWidth, int nDecimals) {
  char *pszFInfo;
  int i;
  int nOldRecordLength, nOldHeaderLength;
  char *pszRecord;
  char chFieldFill;
  unsigned long nRecordOffset;

  /* make sure that everything is written in .dbf */
  if (!DBFFlushRecord(psDBF))
    return -1;

  /* Do some checking to ensure we can add records to this file. */
  if (nWidth < 1)
    return -1;

  if (nWidth > 255)
    nWidth = 255;
  nOldRecordLength = psDBF->nRecordLength;
  nOldHeaderLength = psDBF->nHeaderLength;

  /* SfRealloc all the arrays larger to hold the additional fields */
  psDBF->nFields++;
  psDBF->panFieldOffset = (int *)SfRealloc(psDBF->panFieldOffset, sizeof(int) * psDBF->nFields);
  psDBF->panFieldSize = (int *) SfRealloc(psDBF->panFieldSize, sizeof(int) * psDBF->nFields);
  psDBF->panFieldDecimals = (int *)SfRealloc(psDBF->panFieldDecimals, sizeof(int) * psDBF->nFields);
  psDBF->pachFieldType = (char *)SfRealloc(psDBF->pachFieldType, sizeof(char) * psDBF->nFields);

  /* Assign the new field information fields. */
  psDBF->panFieldOffset[psDBF->nFields - 1] = psDBF->nRecordLength;
  psDBF->nRecordLength += nWidth;
  psDBF->panFieldSize[psDBF->nFields - 1] = nWidth;
  psDBF->panFieldDecimals[psDBF->nFields - 1] = nDecimals;
  psDBF->pachFieldType[psDBF->nFields - 1] = chType;

  /* Extend the required header information. */
  psDBF->nHeaderLength += 32;
  psDBF->bUpdated = FALSE;
  psDBF->pszHeader = (char *) SfRealloc(psDBF->pszHeader, psDBF->nFields * 32);
  pszFInfo = psDBF->pszHeader + 32 * (psDBF->nFields - 1);
  for (i = 0; i < 32; i++)
    pszFInfo[i] = '\0';
  if ((int) strlen(pszFieldName) < 10)
    strncpy(pszFInfo, pszFieldName, strlen(pszFieldName));
  else
    strncpy(pszFInfo, pszFieldName, 10);
  pszFInfo[11] = psDBF->pachFieldType[psDBF->nFields - 1];
  if (chType == 'C') {
    pszFInfo[16] = (unsigned char) (nWidth % 256);
    pszFInfo[17] = (unsigned char) (nWidth / 256);
  } else {
    pszFInfo[16] = (unsigned char) nWidth;
    pszFInfo[17] = (unsigned char) nDecimals;
  }

  /* Make the current record buffer appropriately larger. */
  psDBF->pszCurrentRecord = (char *) SfRealloc(psDBF->pszCurrentRecord,psDBF->nRecordLength);

  /* we're done if dealing with new .dbf */
  if (psDBF->bNoHeader)
    return (psDBF->nFields - 1);

  /* For existing .dbf file, shift records */
  pszRecord = (char *) malloc(sizeof(char) * psDBF->nRecordLength);
  chFieldFill = DBFGetNullCharacter(chType);

  for (i = psDBF->nRecords - 1; i >= 0; --i) {
    nRecordOffset = nOldRecordLength * (unsigned long) i + nOldHeaderLength;
    fseek(psDBF->fp, nRecordOffset, 0);
    fread(pszRecord, nOldRecordLength, 1, psDBF->fp);
    memset(pszRecord + nOldRecordLength, chFieldFill, nWidth);
    nRecordOffset = psDBF->nRecordLength * (unsigned long) i + psDBF->nHeaderLength;
    fseek(psDBF->fp, nRecordOffset, 0);
    fread(pszRecord, psDBF->nRecordLength, 1, psDBF->fp);
  }

  /* force update of header with new header, record length and new field */
  free(pszRecord);
  psDBF->bNoHeader = TRUE;
  DBFUpdateHeader(psDBF);
  psDBF->nCurrentRecord = -1;
  psDBF->bCurrentRecordModified = FALSE;
  return (psDBF->nFields - 1);
}

/* DBFReadAttribute */
static void *DBFReadAttribute(DBFHandle psDBF, int hEntity, int iField,
                              char chReqType) {
  unsigned char *pabyRec;
  void *pReturnField = NULL;

  /* Verify selection. */
  if (hEntity < 0 || hEntity >= psDBF->nRecords)
    return (NULL);
  if (iField < 0 || iField >= psDBF->nFields)
    return (NULL);

  /* Have we read the record? */
  if (!DBFLoadRecord(psDBF, hEntity))
    return NULL;
  pabyRec = (unsigned char *) psDBF->pszCurrentRecord;

  /* Ensure we have room to extract the target field. */
  if (psDBF->panFieldSize[iField] >= psDBF->nWorkFieldLength) {
    psDBF->nWorkFieldLength = psDBF->panFieldSize[iField] + 100;
    if (psDBF->pszWorkField == NULL)
      psDBF->pszWorkField = (char *) malloc(psDBF->nWorkFieldLength);
    else
      psDBF->pszWorkField = (char *) realloc(psDBF->pszWorkField,psDBF->nWorkFieldLength);
  }

  /* Extract the requested field. */
  strncpy(psDBF->pszWorkField,
          ((const char *) pabyRec) + psDBF->panFieldOffset[iField],
          psDBF->panFieldSize[iField]);
  psDBF->pszWorkField[psDBF->panFieldSize[iField]] = '\0';

  pReturnField = psDBF->pszWorkField;

  /* Decode the field. */
  if (chReqType == 'N') {
    psDBF->dfDoubleField = atof(psDBF->pszWorkField);
    pReturnField = &(psDBF->dfDoubleField);
  }

  /* Should we trim white space off the string attribute value? */
#ifdef TRIM_DBF_WHITESPACE
  else {
    char *pchSrc, *pchDst;

    pchDst = pchSrc = psDBF->pszWorkField;
    while (*pchSrc == ' ')
      pchSrc++;
    while (*pchSrc != '\0')
      *(pchDst++) = *(pchSrc++);
    *pchDst = '\0';
    while (pchDst != psDBF->pszWorkField && *(--pchDst) == ' ')
      *pchDst = '\0';
  }
#endif

  return (pReturnField);
}

/* DBFReadIntAttribute */
int  DBFReadIntegerAttribute(DBFHandle psDBF, int iRecord, int iField) {
  double *pdValue = (double *) DBFReadAttribute(psDBF, iRecord, iField, 'N');
  return pdValue == NULL?  0.0 : *pdValue;
}

/* DBFReadDoubleAttribute */
double  DBFReadDoubleAttribute(DBFHandle psDBF, int iRecord, int iField) {
  double *pdValue = (double *) DBFReadAttribute(psDBF, iRecord, iField, 'N');
  return pdValue == NULL?  0.0 : *pdValue;
}

/* DBFReadStringAttribute */
const char* DBFReadStringAttribute(DBFHandle psDBF, int iRecord, int iField) {
  return ((const char *) DBFReadAttribute(psDBF, iRecord, iField, 'C'));
}

/* DBFReadLogicalAttribute */
const char* DBFReadLogicalAttribute(DBFHandle psDBF, int iRecord, int iField) {
  return ((const char *) DBFReadAttribute(psDBF, iRecord, iField, 'L'));
}


/* DBFIsValueNULL */
static int DBFIsValueNULL(char chType, const char *pszValue) {
  int i;

  if (pszValue == NULL)
    return TRUE;

  switch (chType) {
  case 'N':
  case 'F':
    /*
    ** We accept all asterisks or all blanks as NULL
    ** though according to the spec I think it should be all
    ** asterisks.
    */
    if (pszValue[0] == '*')
      return TRUE;
    for (i = 0; pszValue[i] != '\0'; i++) {
      if (pszValue[i] != ' ')
        return FALSE;
    }
    return TRUE;

  case 'D':
    /* NULL date fields have value "00000000" */
    return strncmp(pszValue, "00000000", 8) == 0;

  case 'L':
    /* NULL boolean fields have value "?" */
    return pszValue[0] == '?';

  default:
    /* empty string fields are considered NULL */
    return strlen(pszValue) == 0;
  }
}

/* DBFIsAttributeNULL */
int  DBFIsAttributeNULL(DBFHandle psDBF, int iRecord, int iField) {
  const char *pszValue;

  pszValue = DBFReadStringAttribute(psDBF, iRecord, iField);

  if (pszValue == NULL)
    return TRUE;

  return DBFIsValueNULL(psDBF->pachFieldType[iField], pszValue);
}

/* DBFGetFieldCount */
int  DBFGetFieldCount(DBFHandle psDBF) {
  return (psDBF->nFields);
}

/* DBFGetRecordCount */
int  DBFGetRecordCount(DBFHandle psDBF) {
  return (psDBF->nRecords);
}

/* DBFGetFieldInfo */
DBFFieldType  DBFGetFieldInfo(DBFHandle psDBF, int iField, char *pszFieldName,
                                         int *pnWidth, int *pnDecimals) {
  if (iField < 0 || iField >= psDBF->nFields)
    return (FTInvalid);

  if (pnWidth != NULL)
    *pnWidth = psDBF->panFieldSize[iField];
  if (pnDecimals != NULL)
    *pnDecimals = psDBF->panFieldDecimals[iField];
  if (pszFieldName != NULL) {
    int i;
    strncpy(pszFieldName, (char *) psDBF->pszHeader + iField * 32, 11);
    pszFieldName[11] = '\0';
    for (i = 10; i > 0 && pszFieldName[i] == ' '; i--)
      pszFieldName[i] = '\0';
  }

  if (psDBF->pachFieldType[iField] == 'L')
    return (FTLogical);
  else if (psDBF->pachFieldType[iField] == 'N'
           || psDBF->pachFieldType[iField] == 'F') {
    if (psDBF->panFieldDecimals[iField] > 0
        || psDBF->panFieldSize[iField] > 10)
      return (FTDouble);
    else
      return (FTInteger);
  } else {
    return (FTString);
  }
}


/* DBFWriteAttribute */
static int DBFWriteAttribute(DBFHandle psDBF, int hEntity, int iField, void *pValue) {
  int i, j, nRetResult = TRUE;
  unsigned char *pabyRec;
  char szSField[400], szFormat[20];

  /* Is this a valid record? */
  if (hEntity < 0 || hEntity > psDBF->nRecords)
    return (FALSE);
  if (psDBF->bNoHeader)
    DBFWriteHeader(psDBF);

  /* Is this a brand new record? */
  if (hEntity == psDBF->nRecords) {
    if (!DBFFlushRecord(psDBF))
      return FALSE;
    psDBF->nRecords++;
    for (i = 0; i < psDBF->nRecordLength; i++)
      psDBF->pszCurrentRecord[i] = ' ';
    psDBF->nCurrentRecord = hEntity;
  }

  /* Is this an existing record, but different than the last one */
  /* we accessed? */
  if (!DBFLoadRecord(psDBF, hEntity))
    return FALSE;
  pabyRec = (unsigned char *) psDBF->pszCurrentRecord;
  psDBF->bCurrentRecordModified = TRUE;
  psDBF->bUpdated = TRUE;

  /* Translate NULL value to valid DBF file representation. */
  if (pValue == NULL) {
    memset((char *) (pabyRec + psDBF->panFieldOffset[iField]),
           DBFGetNullCharacter(psDBF->pachFieldType[iField]),
           psDBF->panFieldSize[iField]);
    return TRUE;
  }

  /* Assign all the record fields. */
  switch (psDBF->pachFieldType[iField]) {
  case 'D':
  case 'N':
  case 'F':
    if (psDBF->panFieldDecimals[iField] == 0) {
      int nWidth = psDBF->panFieldSize[iField];
      if ((int) sizeof(szSField) - 2 < nWidth)
        nWidth = sizeof(szSField) - 2;
      sprintf(szFormat, "%%%dd", nWidth);
      sprintf(szSField, szFormat, (int) *((double *) pValue));
      if ((int) strlen(szSField) > psDBF->panFieldSize[iField]) {
        szSField[psDBF->panFieldSize[iField]] = '\0';
        nRetResult = FALSE;
      }
      strncpy((char *) (pabyRec + psDBF->panFieldOffset[iField]),szSField, strlen(szSField));
    } else {
      int nWidth = psDBF->panFieldSize[iField];
      if ((int) sizeof(szSField) - 2 < nWidth)
        nWidth = sizeof(szSField) - 2;
      sprintf(szFormat, "%%%d.%df",
              nWidth, psDBF->panFieldDecimals[iField]);
      sprintf(szSField, szFormat, *((double *) pValue));
      if ((int) strlen(szSField) > psDBF->panFieldSize[iField]) {
        szSField[psDBF->panFieldSize[iField]] = '\0';
        nRetResult = FALSE;
      }
      strncpy((char *) (pabyRec + psDBF->panFieldOffset[iField]),szSField, strlen(szSField));
    }
    break;

  case 'L':
    if (psDBF->panFieldSize[iField] >= 1 &&
        (*(char *) pValue == 'F' || *(char *) pValue == 'T'))
      *(pabyRec + psDBF->panFieldOffset[iField]) = *(char *) pValue;
    break;

  default:
    if ((int) strlen((char *) pValue) > psDBF->panFieldSize[iField]) {
      j = psDBF->panFieldSize[iField];
      nRetResult = FALSE;
    } else {
      memset(pabyRec + psDBF->panFieldOffset[iField], ' ',psDBF->panFieldSize[iField]);
      j = strlen((char *) pValue);
    }
    strncpy((char *) (pabyRec + psDBF->panFieldOffset[iField]), (char *) pValue, j);
    break;
  }

  return (nRetResult);
}

/* DBFWriteAttributeDirectly */
int  DBFWriteAttributeDirectly(DBFHandle psDBF, int hEntity, int iField,void *pValue) {
  int i, j;
  unsigned char *pabyRec;

  /* Is this a valid record? */
  if (hEntity < 0 || hEntity > psDBF->nRecords)
    return (FALSE);

  if (psDBF->bNoHeader)
    DBFWriteHeader(psDBF);

  /* Is this a brand new record? */
  if (hEntity == psDBF->nRecords) {
    if (!DBFFlushRecord(psDBF))
      return FALSE;

    psDBF->nRecords++;
    for (i = 0; i < psDBF->nRecordLength; i++)
      psDBF->pszCurrentRecord[i] = ' ';
    psDBF->nCurrentRecord = hEntity;
  }

  /* Is this an existing record, but different than the last one */
  /* we accessed? */
  if (!DBFLoadRecord(psDBF, hEntity))
    return FALSE;
  pabyRec = (unsigned char *) psDBF->pszCurrentRecord;

  /* Assign all the record fields. */
  if ((int) strlen((char *) pValue) > psDBF->panFieldSize[iField])
    j = psDBF->panFieldSize[iField];
  else {
    memset(pabyRec + psDBF->panFieldOffset[iField], ' ',psDBF->panFieldSize[iField]);
    j = strlen((char *) pValue);
  }

  strncpy((char *) (pabyRec + psDBF->panFieldOffset[iField]),(char *) pValue, j);
  psDBF->bCurrentRecordModified = TRUE;
  psDBF->bUpdated = TRUE;
  return (TRUE);
}

/* DBFWriteDoubleAttribute */
int  DBFWriteDoubleAttribute(DBFHandle psDBF, int iRecord, int iField, double dValue) {
  return (DBFWriteAttribute(psDBF, iRecord, iField, (void *) &dValue));
}

/* DBFWriteIntegerAttribute */
int  DBFWriteIntegerAttribute(DBFHandle psDBF, int iRecord, int iField,int nValue) {
  double dValue = nValue;
  return (DBFWriteAttribute(psDBF, iRecord, iField, (void *) &dValue));
}

/* DBFWriteStringAttribute */
int  DBFWriteStringAttribute(DBFHandle psDBF, int iRecord, int iField, const char *pszValue) {
  return (DBFWriteAttribute(psDBF, iRecord, iField, (void *) pszValue));
}


/* DBFWriteNULLAttribute */
int  DBFWriteNULLAttribute(DBFHandle psDBF, int iRecord, int iField) {
  return (DBFWriteAttribute(psDBF, iRecord, iField, NULL));
}

/* DBFWriteLogicalAttribute */
int  DBFWriteLogicalAttribute(DBFHandle psDBF, int iRecord, int iField, const char lValue) {
  return (DBFWriteAttribute(psDBF, iRecord, iField, (void *) (&lValue)));
}

/* DBFWriteTuple */
int  DBFWriteTuple(DBFHandle psDBF, int hEntity, void *pRawTuple) {
  int i;
  unsigned char *pabyRec;

  /* Is this a valid record? */
  if (hEntity < 0 || hEntity > psDBF->nRecords)
    return (FALSE);

  if (psDBF->bNoHeader)
    DBFWriteHeader(psDBF);

  /* Is this a brand new record? */
  if (hEntity == psDBF->nRecords) {
    if (!DBFFlushRecord(psDBF))
      return FALSE;
    psDBF->nRecords++;
    for (i = 0; i < psDBF->nRecordLength; i++)
      psDBF->pszCurrentRecord[i] = ' ';
    psDBF->nCurrentRecord = hEntity;
  }

  /* Is this an existing record, but different than the last one */
  /* we accessed? */
  if (!DBFLoadRecord(psDBF, hEntity))
    return FALSE;
  pabyRec = (unsigned char *) psDBF->pszCurrentRecord;
  memcpy(pabyRec, pRawTuple, psDBF->nRecordLength);
  psDBF->bCurrentRecordModified = TRUE;
  psDBF->bUpdated = TRUE;
  return (TRUE);
}

/* DBFReadTuple */
const char* DBFReadTuple(DBFHandle psDBF, int hEntity) {
  if (hEntity < 0 || hEntity >= psDBF->nRecords)
    return (NULL);

  if (!DBFLoadRecord(psDBF, hEntity))
    return NULL;

  return (const char *) psDBF->pszCurrentRecord;
}

/* DBFCloneEmpty */
DBFHandle  DBFCloneEmpty(DBFHandle psDBF, const char *pszFilename) {
  DBFHandle newDBF;

  newDBF = DBFCreateEx(pszFilename, psDBF->pszCodePage);
  if (newDBF == NULL)
    return (NULL);

  newDBF->nFields = psDBF->nFields;
  newDBF->nRecordLength = psDBF->nRecordLength;
  newDBF->nHeaderLength = psDBF->nHeaderLength;
  newDBF->pszHeader = (char *) malloc(newDBF->nHeaderLength);
  memcpy(newDBF->pszHeader, psDBF->pszHeader, newDBF->nHeaderLength);
  newDBF->panFieldOffset = (int *) malloc(sizeof(int) * psDBF->nFields);
  memcpy(newDBF->panFieldOffset, psDBF->panFieldOffset,  sizeof(int) * psDBF->nFields);
  newDBF->panFieldSize = (int *) malloc(sizeof(int) * psDBF->nFields);
  memcpy(newDBF->panFieldSize, psDBF->panFieldSize, sizeof(int) * psDBF->nFields);
  newDBF->panFieldDecimals = (int *) malloc(sizeof(int) * psDBF->nFields);
  memcpy(newDBF->panFieldDecimals, psDBF->panFieldDecimals, sizeof(int) * psDBF->nFields);
  newDBF->pachFieldType = (char *) malloc(sizeof(char) * psDBF->nFields);
  memcpy(newDBF->pachFieldType, psDBF->pachFieldType, sizeof(char) * psDBF->nFields);

  newDBF->bNoHeader = TRUE;
  newDBF->bUpdated = TRUE;
  DBFWriteHeader(newDBF);
  DBFClose(newDBF);
  newDBF = DBFOpen(pszFilename, "rb+");
  return (newDBF);
}

/* DBFGetNativeFieldType */
char  DBFGetNativeFieldType(DBFHandle psDBF, int iField) {
  if (iField >= 0 && iField < psDBF->nFields)
    return psDBF->pachFieldType[iField];
  return ' ';
}

/* str_to_upper */
static void str_to_upper(char *string) {
  int len = strlen(string);
  short i = -1;
  while (++i < len)
    if (isalpha(string[i]) && islower(string[i]))
      string[i] = (char) toupper((int) string[i]);
}

/* DBFGetFieldIndex */
int  DBFGetFieldIndex(DBFHandle psDBF, const char *pszFieldName) {
  char name[12], name1[12], name2[12];
  int i;

  strncpy(name1, pszFieldName, 11);
  name1[11] = '\0';
  str_to_upper(name1);
  for (i = 0; i < DBFGetFieldCount(psDBF); i++) {
    DBFGetFieldInfo(psDBF, i, name, NULL, NULL);
    strncpy(name2, name, 11);
    str_to_upper(name2);
    if (!strncmp(name1, name2, 10))
      return (i);
  }
  return (-1);
}

/* DBFIsRecordDeleted */
int  DBFIsRecordDeleted(DBFHandle psDBF, int iShape) {
  /* Verify selection. */
  if (iShape < 0 || iShape >= psDBF->nRecords)
    return TRUE;

  /* Have we read the record? */
  if (!DBFLoadRecord(psDBF, iShape))
    return FALSE;

  /* '*' means deleted. */
  return psDBF->pszCurrentRecord[0] == '*';
}

/* DBFMarkRecordDeleted */
int  DBFMarkRecordDeleted(DBFHandle psDBF, int iShape, int bIsDeleted) {
  char chNewFlag;

  /* Verify selection. */
  if (iShape < 0 || iShape >= psDBF->nRecords)
    return FALSE;

  /* Is this an existing record, but different than the last one */
  /* we accessed? */
  if (!DBFLoadRecord(psDBF, iShape))
    return FALSE;

  /* Assign value, marking record as dirty if it changes. */
  if (bIsDeleted)
    chNewFlag = '*';
  else
    chNewFlag = ' ';

  if (psDBF->pszCurrentRecord[0] != chNewFlag) {
    psDBF->bCurrentRecordModified = TRUE;
    psDBF->bUpdated = TRUE;
    psDBF->pszCurrentRecord[0] = chNewFlag;
  }

  return TRUE;
}

/* DBFGetCodePage */
const char* DBFGetCodePage(DBFHandle psDBF) {
  if (psDBF == NULL)
    return NULL;
  return psDBF->pszCodePage;
}

/* DBFDeleteField */
int  DBFDeleteField(DBFHandle psDBF, int iField) {
  int nOldRecordLength, nOldHeaderLength;
  int nDeletedFieldOffset, nDeletedFieldSize;
  unsigned long nRecordOffset;
  char *pszRecord;
  int i, iRecord;

  if (iField < 0 || iField >= psDBF->nFields)
    return FALSE;

  /* make sure that everything is written in .dbf */
  if (!DBFFlushRecord(psDBF))
    return FALSE;

  /* get information about field to be deleted */
  nOldRecordLength = psDBF->nRecordLength;
  nOldHeaderLength = psDBF->nHeaderLength;
  nDeletedFieldOffset = psDBF->panFieldOffset[iField];
  nDeletedFieldSize = psDBF->panFieldSize[iField];

  /* update fields info */
  for (i = iField + 1; i < psDBF->nFields; i++) {
    psDBF->panFieldOffset[i - 1] =
      psDBF->panFieldOffset[i] - nDeletedFieldSize;
    psDBF->panFieldSize[i - 1] = psDBF->panFieldSize[i];
    psDBF->panFieldDecimals[i - 1] = psDBF->panFieldDecimals[i];
    psDBF->pachFieldType[i - 1] = psDBF->pachFieldType[i];
  }

  /* resize fields arrays */
  psDBF->nFields--;
  psDBF->panFieldOffset = (int *) SfRealloc(psDBF->panFieldOffset, sizeof(int) * psDBF->nFields);
  psDBF->panFieldSize = (int *) SfRealloc(psDBF->panFieldSize, sizeof(int) * psDBF->nFields);
  psDBF->panFieldDecimals = (int *) SfRealloc(psDBF->panFieldDecimals, sizeof(int) * psDBF->nFields);
  psDBF->pachFieldType = (char *) SfRealloc(psDBF->pachFieldType, sizeof(char) * psDBF->nFields);

  /* update header information */
  psDBF->nHeaderLength -= 32;
  psDBF->nRecordLength -= nDeletedFieldSize;
  memmove(psDBF->pszHeader + iField * 32, psDBF->pszHeader + (iField + 1) * 32,
          sizeof(char) * (psDBF->nFields - iField) * 32);
  psDBF->pszHeader = (char *) SfRealloc(psDBF->pszHeader, psDBF->nFields * 32);

  /* update size of current record appropriately */
  psDBF->pszCurrentRecord = (char *) SfRealloc(psDBF->pszCurrentRecord,psDBF->nRecordLength);

  /* we're done if we're dealing with not yet created .dbf */
  if (psDBF->bNoHeader && psDBF->nRecords == 0)
    return TRUE;

  /* force update of header with new header and record length */
  psDBF->bNoHeader = TRUE;
  DBFUpdateHeader(psDBF);

  /* shift records to their new positions */
  pszRecord = (char *) malloc(sizeof(char) * nOldRecordLength);
  for (iRecord = 0; iRecord < psDBF->nRecords; iRecord++) {
    /* load record */
    nRecordOffset = nOldRecordLength * (unsigned long) iRecord + nOldHeaderLength;
    fseek(psDBF->fp, nRecordOffset, 0);
    fread(pszRecord, nOldRecordLength, 1, psDBF->fp);

    /* move record in two steps */
    nRecordOffset = psDBF->nRecordLength * (unsigned long) iRecord + psDBF->nHeaderLength;
    fseek(psDBF->fp, nRecordOffset, 0);
    fwrite(pszRecord, nDeletedFieldOffset, 1, psDBF->fp);
    fwrite(pszRecord + nDeletedFieldOffset +  nDeletedFieldSize,
           nOldRecordLength - nDeletedFieldOffset -
           nDeletedFieldSize, 1, psDBF->fp);
  }

  /* free record */
  free(pszRecord);
  psDBF->nCurrentRecord = -1;
  psDBF->bCurrentRecordModified = FALSE;
  return TRUE;
}

/* DBFReorderFields */
int  DBFReorderFields(DBFHandle psDBF, int *panMap) {
  unsigned long nRecordOffset;
  int i, iRecord;
  int *panFieldOffsetNew;
  int *panFieldSizeNew;
  int *panFieldDecimalsNew;
  char *pachFieldTypeNew;
  char *pszHeaderNew;
  char *pszRecord;
  char *pszRecordNew;

  if (psDBF->nFields == 0)
    return TRUE;

  /* make sure that everything is written in .dbf */
  if (!DBFFlushRecord(psDBF))
    return FALSE;

  panFieldOffsetNew = (int *) malloc(sizeof(int) * psDBF->nFields);
  panFieldSizeNew = (int *) malloc(sizeof(int) * psDBF->nFields);
  panFieldDecimalsNew = (int *) malloc(sizeof(int) * psDBF->nFields);
  pachFieldTypeNew = (char *) malloc(sizeof(char) * psDBF->nFields);
  pszHeaderNew = (char *) malloc(sizeof(char) * 32 * psDBF->nFields);

  /* shuffle fields definitions */
  for (i = 0; i < psDBF->nFields; i++) {
    panFieldSizeNew[i] = psDBF->panFieldSize[panMap[i]];
    panFieldDecimalsNew[i] = psDBF->panFieldDecimals[panMap[i]];
    pachFieldTypeNew[i] = psDBF->pachFieldType[panMap[i]];
    memcpy(pszHeaderNew + i * 32, psDBF->pszHeader + panMap[i] * 32,  32);
  }
  panFieldOffsetNew[0] = 1;
  for (i = 1; i < psDBF->nFields; i++)
    panFieldOffsetNew[i] = panFieldOffsetNew[i - 1] + panFieldSizeNew[i - 1];
  free(psDBF->pszHeader);
  psDBF->pszHeader = pszHeaderNew;

  /* we're done if we're dealing with not yet created .dbf */
  if (!(psDBF->bNoHeader && psDBF->nRecords == 0)) {
    /* force update of header with new header and record length */
    psDBF->bNoHeader = TRUE;
    DBFUpdateHeader(psDBF);

    /* alloc record */
    pszRecord = (char *) malloc(sizeof(char) * psDBF->nRecordLength);
    pszRecordNew = (char *) malloc(sizeof(char) * psDBF->nRecordLength);

    /* shuffle fields in records */
    for (iRecord = 0; iRecord < psDBF->nRecords; iRecord++) {
      nRecordOffset = psDBF->nRecordLength * (unsigned long) iRecord + psDBF->nHeaderLength;

      /* load record */
      fseek(psDBF->fp, nRecordOffset, 0);
      fread(pszRecord, psDBF->nRecordLength, 1,psDBF->fp);
      pszRecordNew[0] = pszRecord[0];
      for (i = 0; i < psDBF->nFields; i++) {
        memcpy(pszRecordNew + panFieldOffsetNew[i],pszRecord + psDBF->panFieldOffset[panMap[i]],
               psDBF->panFieldSize[panMap[i]]);
      }

      /* write record */
      fseek(psDBF->fp, nRecordOffset, 0);
      fwrite(pszRecordNew, psDBF->nRecordLength, 1, psDBF->fp);
    }

    /* free record */
    free(pszRecord);
    free(pszRecordNew);
  }

  free(psDBF->panFieldOffset);
  free(psDBF->panFieldSize);
  free(psDBF->panFieldDecimals);
  free(psDBF->pachFieldType);
  psDBF->panFieldOffset = panFieldOffsetNew;
  psDBF->panFieldSize = panFieldSizeNew;
  psDBF->panFieldDecimals = panFieldDecimalsNew;
  psDBF->pachFieldType = pachFieldTypeNew;
  psDBF->nCurrentRecord = -1;
  psDBF->bCurrentRecordModified = FALSE;
  return TRUE;
}


/* DBFAlterFieldDefn */
int DBFAlterFieldDefn(DBFHandle psDBF, int iField, const char *pszFieldName,
                  char chType, int nWidth, int nDecimals) {
  int i;
  int iRecord;
  int nOffset;
  int nOldWidth;
  int nOldRecordLength;
  int nRecordOffset;
  char *pszFInfo;
  char chOldType;
  int bIsNULL;
  char chFieldFill;

  if (iField < 0 || iField >= psDBF->nFields)
    return FALSE;

  /* make sure that everything is written in .dbf */
  if (!DBFFlushRecord(psDBF))
    return FALSE;

  chFieldFill = DBFGetNullCharacter(chType);
  chOldType = psDBF->pachFieldType[iField];
  nOffset = psDBF->panFieldOffset[iField];
  nOldWidth = psDBF->panFieldSize[iField];
  nOldRecordLength = psDBF->nRecordLength;

  /* Do some checking to ensure we can add records to this file. */
  if (nWidth < 1)
    return -1;

  if (nWidth > 255)
    nWidth = 255;

  /* Assign the new field information fields. */
  psDBF->panFieldSize[iField] = nWidth;
  psDBF->panFieldDecimals[iField] = nDecimals;
  psDBF->pachFieldType[iField] = chType;

  /* Update the header information. */
  pszFInfo = psDBF->pszHeader + 32 * iField;
  for (i = 0; i < 32; i++)
    pszFInfo[i] = '\0';
  if ((int) strlen(pszFieldName) < 10)
    strncpy(pszFInfo, pszFieldName, strlen(pszFieldName));
  else
    strncpy(pszFInfo, pszFieldName, 10);
  pszFInfo[11] = psDBF->pachFieldType[iField];
  if (chType == 'C') {
    pszFInfo[16] = (unsigned char) (nWidth % 256);
    pszFInfo[17] = (unsigned char) (nWidth / 256);
  } else {
    pszFInfo[16] = (unsigned char) nWidth;
    pszFInfo[17] = (unsigned char) nDecimals;
  }

  /* Update offsets */
  if (nWidth != nOldWidth) {
    for (i = iField + 1; i < psDBF->nFields; i++)
      psDBF->panFieldOffset[i] += nWidth - nOldWidth;
    psDBF->nRecordLength += nWidth - nOldWidth;
    psDBF->pszCurrentRecord = (char *) SfRealloc(psDBF->pszCurrentRecord, psDBF->nRecordLength);
  }

  /* we're done if we're dealing with not yet created .dbf */
  if (psDBF->bNoHeader && psDBF->nRecords == 0)
    return TRUE;

  /* force update of header with new header and record length */
  psDBF->bNoHeader = TRUE;
  DBFUpdateHeader(psDBF);

  if (nWidth < nOldWidth || (nWidth == nOldWidth && chType != chOldType)) {
    char *pszRecord = (char *) malloc(sizeof(char) * nOldRecordLength);
    char *pszOldField = (char *) malloc(sizeof(char) * (nOldWidth + 1));

    pszOldField[nOldWidth] = 0;

    /* move records to their new positions */
    for (iRecord = 0; iRecord < psDBF->nRecords; iRecord++) {

      /* load record */
      nRecordOffset = nOldRecordLength * (unsigned long) iRecord + psDBF->nHeaderLength;
      fseek(psDBF->fp, nRecordOffset, 0);
      fread(pszRecord, nOldRecordLength, 1, psDBF->fp);
      memcpy(pszOldField, pszRecord + nOffset, nOldWidth);
      bIsNULL = DBFIsValueNULL(chOldType, pszOldField);

      if (nWidth != nOldWidth) {
        if ((chOldType == 'N' || chOldType == 'F') && pszOldField[0] == ' ') {
          /* Strip leading spaces when truncating a numeric field */
          memmove(pszRecord + nOffset,pszRecord + nOffset + nOldWidth - nWidth, nWidth);
        }
        if (nOffset + nOldWidth < nOldRecordLength) {
          memmove(pszRecord + nOffset + nWidth,
                  pszRecord + nOffset + nOldWidth,
                  nOldRecordLength - (nOffset + nOldWidth));
        }
      }

      /* Convert null value to the appropriate value of the new type */
      if (bIsNULL)
        memset(pszRecord + nOffset, chFieldFill, nWidth);

      /* write record */
      nRecordOffset = psDBF->nRecordLength * (unsigned long) iRecord + psDBF->nHeaderLength;
      fseek(psDBF->fp, nRecordOffset, 0);
      fwrite(pszRecord, psDBF->nRecordLength, 1,psDBF->fp);
    }

    free(pszRecord);
    free(pszOldField);
  } else if (nWidth > nOldWidth) {
    char *pszRecord = (char *) malloc(sizeof(char) * psDBF->nRecordLength);
    char *pszOldField = (char *) malloc(sizeof(char) * (nOldWidth + 1));

    pszOldField[nOldWidth] = 0;

    /* move records to their new positions */
    for (iRecord = psDBF->nRecords - 1; iRecord >= 0; iRecord--) {

      /* load record */
      nRecordOffset =  nOldRecordLength * (unsigned long) iRecord + psDBF->nHeaderLength;
      fseek(psDBF->fp, nRecordOffset, 0);
      fread(pszRecord, nOldRecordLength, 1, psDBF->fp);
      memcpy(pszOldField, pszRecord + nOffset, nOldWidth);
      bIsNULL = DBFIsValueNULL(chOldType, pszOldField);
      if (nOffset + nOldWidth < nOldRecordLength)
        memmove(pszRecord + nOffset + nWidth,pszRecord + nOffset + nOldWidth,
                nOldRecordLength - (nOffset + nOldWidth));

      /* Convert null value to the appropriate value of the new type */
      if (bIsNULL) {
        memset(pszRecord + nOffset, chFieldFill, nWidth);
      } else {
        if ((chOldType == 'N' || chOldType == 'F')) {
          /* Add leading spaces when expanding a numeric field */
          memmove(pszRecord + nOffset + nWidth - nOldWidth, pszRecord + nOffset, nOldWidth);
          memset(pszRecord + nOffset, ' ', nWidth - nOldWidth);
        } else {
          /* Add trailing spaces */
          memset(pszRecord + nOffset + nOldWidth, ' ', nWidth - nOldWidth);
        }
      }
      /* write record */
      nRecordOffset = psDBF->nRecordLength * (unsigned long) iRecord + psDBF->nHeaderLength;
      fseek(psDBF->fp, nRecordOffset, 0);
      fwrite(pszRecord, psDBF->nRecordLength, 1,psDBF->fp);
    }
    free(pszRecord);
    free(pszOldField);
  }
  psDBF->nCurrentRecord = -1;
  psDBF->bCurrentRecordModified = FALSE;
  return TRUE;
}
