/*
   ----------------------------------------------------------------------------
   | mg-dbx-bdb.node                                                          |
   | Author: Chris Munt cmunt@mgateway.com                                    |
   |                    chris.e.munt@gmail.com                                |
   | Copyright (c) 2019-2024 MGateway Ltd                                     |
   | Surrey UK.                                                               |
   | All rights reserved.                                                     |
   |                                                                          |
   | http://www.mgateway.com                                                  |
   |                                                                          |
   | Licensed under the Apache License, Version 2.0 (the "License"); you may  |
   | not use this file except in compliance with the License.                 |
   | You may obtain a copy of the License at                                  |
   |                                                                          |
   | http:www.apache.org/licenses/LICENSE-2.0                                 |
   |                                                                          |
   | Unless required by applicable law or agreed to in writing, software      |
   | distributed under the License is distributed on an "AS IS" BASIS,        |
   | WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. |
   | See the License for the specific language governing permissions and      |
   | limitations under the License.                                           |      
   |                                                                          |
   ----------------------------------------------------------------------------
*/

#ifndef MG_DBX_H
#define MG_DBX_H

#include <node_version.h>

#define DBX_NODE_VERSION         (NODE_MAJOR_VERSION * 10000) + (NODE_MINOR_VERSION * 100) + NODE_PATCH_VERSION

#define DBX_VERSION_MAJOR        "1"
#define DBX_VERSION_MINOR        "3"
#define DBX_VERSION_BUILD        "13"

#define DBX_VERSION              DBX_VERSION_MAJOR "." DBX_VERSION_MINOR "." DBX_VERSION_BUILD

#define DBX_STR_HELPER(x)        #x
#define DBX_STR(x)               DBX_STR_HELPER(x)


/*
#if defined(_WIN32)
#define DBX_USE_SECURE_CRT       1
#endif
*/

/*
#if !defined(SOLARIS)
#define DBX_CHK_SECURE_CRT       1
#endif
*/

#define DBX_DBNAME               dbxbdb
#define DBX_DBNAME_STR           "dbxbdb"
#define DBX_MAGIC_NUMBER         120861
#define DBX_MAGIC_NUMBER_MGLOBAL 100863
#define DBX_MAGIC_NUMBER_MCURSOR 200438

#if defined(_WIN32)

#define BUILDING_NODE_EXTENSION     1
#if defined(_MSC_VER)
/* Check for MS compiler later than VC6 */
#if (_MSC_VER >= 1400)
#if !defined(DBX_USE_SECURE_CRT)
#define _CRT_SECURE_NO_DEPRECATE    1
#define _CRT_NONSTDC_NO_DEPRECATE   1
#endif
#endif
#endif

#elif defined(__linux__) || defined(__linux) || defined(linux)

#if !defined(LINUX)
#define LINUX                       1
#endif

#elif defined(__APPLE__)

#if !defined(MACOSX)
#define MACOSX                      1
#endif

#endif

#if defined(SOLARIS)
#ifndef __GNUC__
#  define  __attribute__(x)
#endif
#endif


#if defined(_WIN32)
#include <string>
#include <time.h>

#if defined(DBX_WINSOCK2)
#define INCL_WINSOCK_API_TYPEDEFS 1
#include <winsock2.h>
#include <ws2tcpip.h>
#endif

#endif

#if !defined(_WIN32)
#include <string>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <time.h>
#include <errno.h>
#include <unistd.h>
#include <sys/time.h>
#include <stdarg.h>
#endif

#if defined(__GNUC__) && __GNUC__ >= 8
#define DISABLE_WCAST_FUNCTION_TYPE _Pragma("GCC diagnostic push") _Pragma("GCC diagnostic ignored \"-Wcast-function-type\"")
#define DISABLE_WCAST_FUNCTION_TYPE_END _Pragma("GCC diagnostic pop")
#else
#define DISABLE_WCAST_FUNCTION_TYPE
#define DISABLE_WCAST_FUNCTION_TYPE_END
#endif

DISABLE_WCAST_FUNCTION_TYPE
#include <v8.h>
#include <node.h>
#include <node_buffer.h>
#include <uv.h>
#include <node_object_wrap.h>

#if defined(_WIN32)

#include "db_windows.h" /* bdb */
#define LMDBWINDOWSDLL_API __declspec(dllexport)
#include "lmdb-dll.h" /* lmdb */

#else

#include <pthread.h>
#include <dlfcn.h>

#if defined __has_include
#if __has_include(<db.h>)
#include <db.h>
#define DBX_DB_HEADER_INCLUDED 1
#elif __has_include("db.h")
#include "db.h"
#define DBX_DB_HEADER_INCLUDED 1
#endif
#endif

#if !defined(DBX_DB_HEADER_INCLUDED)
#if defined(__arm__)
#include "db_pi.h" /* bdb */
#else
#include "db_unix.h" /* bdb */
#endif
#endif

#if defined __has_include
#if __has_include(<lmdb.h>)
#include <lmdb.h>
#define DBX_LMDB_HEADER_INCLUDED 1
#elif __has_include("lmdb.h")
#include "lmdb.h"
#define DBX_LMDB_HEADER_INCLUDED 1
#endif
#endif

#if !defined(DBX_LMDB_HEADER_INCLUDED)
#include "lmdb.h" /* lmdb */
#endif

#endif


#define DBX_MAXARGS              64
#define DBX_DEFAULT_TIMEOUT      30

#define DBX_THREADPOOL_MAX       8

#define DBX_ERROR_SIZE           512

#define DBX_THREAD_STACK_SIZE    0xf0000

#define DBX_TEXT_E_ASYNC         "Unable to process task asynchronously"

#define DBX_DSORT_INVALID        0
#define DBX_DSORT_DATA           1
#define DBX_DSORT_SUBSCRIPT      2
#define DBX_DSORT_GLOBAL         3
#define DBX_DSORT_EOD            9
#define DBX_DSORT_STATUS         10
#define DBX_DSORT_ERROR          11

#define DBX_DSORT_ISVALID(a)     ((a == DBX_DSORT_GLOBAL) || (a == DBX_DSORT_SUBSCRIPT) || (a == DBX_DSORT_DATA) || (a == DBX_DSORT_EOD) || (a == DBX_DSORT_STATUS) || (a == DBX_DSORT_ERROR))

#define DBX_DTYPE_NONE           0
#define DBX_DTYPE_STR            1
#define DBX_DTYPE_STR8           2
#define DBX_DTYPE_STR16          3
#define DBX_DTYPE_INT            4
#define DBX_DTYPE_INT64          5
#define DBX_DTYPE_DOUBLE         6
#define DBX_DTYPE_OREF           7
#define DBX_DTYPE_NULL           10
#define DBX_DTYPE_STROBJ         11

#define DBX_CMND_OPEN            1
#define DBX_CMND_CLOSE           2
#define DBX_CMND_NSGET           3
#define DBX_CMND_NSSET           4

#define DBX_CMND_GSET            11
#define DBX_CMND_GGET            12
#define DBX_CMND_GNEXT           13
#define DBX_CMND_GNEXTDATA       131
#define DBX_CMND_GPREVIOUS       14
#define DBX_CMND_GPREVIOUSDATA   141
#define DBX_CMND_GDELETE         15
#define DBX_CMND_GDEFINED        16
#define DBX_CMND_GINCREMENT      17
#define DBX_CMND_GLOCK           18
#define DBX_CMND_GUNLOCK         19
#define DBX_CMND_GMERGE          20

#define DBX_CMND_GNNODE          21
#define DBX_CMND_GNNODEDATA      211
#define DBX_CMND_GPNODE          22
#define DBX_CMND_GPNODEDATA      221

#define DBX_CMND_GNAMENEXT       51
#define DBX_CMND_GNAMEPREVIOUS   52

#define DBX_DB_SIZE              50000000

#if defined(MAX_PATH) && (MAX_PATH>511)
#define DBX_MAX_PATH             MAX_PATH
#else
#define DBX_MAX_PATH             512
#endif

#if defined(_WIN32)
#define DBX_NULL_DEVICE          "//./nul"
#else
#define DBX_NULL_DEVICE          "/dev/null/"
#endif

#if defined(_WIN32)
#define _dbxso(a)                _countof(a)
#else
#define _dbxso(a)                sizeof(a)
#endif

#if defined(DBX_USE_SECURE_CRT)
/* #define T_STRCPY(a, b, c)        _tcscpy_s(a, b, c) */
/* #define T_STRNCPY(a, b, c, d)    _tcsncpy_s(a, b, c, d) */
#define T_STRCPY(a, b, c)        strcpy_s(a, b, c)
#define T_STRNCPY(a, b, c, d)    memcpy_s(a, b, c, d)

#define T_STRCAT(a, b, c)        strcat_s(a, b, c)
#define T_STRNCAT(a, b, c, d)    strncat_s(a, b, c, d)
#define T_SPRINTF(a, b, c, ...)  sprintf_s(a, b, c, __VA_ARGS__)
#if defined(LINUX)
#define T_MEMCPY(a,b,c)          memmove(a,b,c)
#else
#define T_MEMCPY(a,b,c)          memcpy(a,b,c)
#endif
#elif defined(DBX_CHK_SECURE_CRT)
#define T_STRCPY(a, b, c)        dbx_strcpy_s(a, b, c, __FILE__, __FUNCTION__, __LINE__)
#define T_STRNCPY(a, b, c, d)    dbx_strncpy_s(a, b, c, d, __FILE__, __FUNCTION__, __LINE__)
#define T_STRCAT(a, b, c)        dbx_strcat_s(a, b, c, __FILE__, __FUNCTION__, __LINE__)
#define T_STRNCAT(a, b, c, d)    dbx_strncat_s(a, b, c, d, __FILE__, __FUNCTION__, __LINE__)
#define T_SPRINTF(a, b, c, ...)  dbx_sprintf_s(a, b, c,  __FILE__, __FUNCTION__, __LINE__, __VA_ARGS__)
#if defined(LINUX)
#define T_MEMCPY(a,b,c)          memmove(a,b,c)
#else
#define T_MEMCPY(a,b,c)          memcpy(a,b,c)
#endif
#else
#define T_STRCPY(a, b, c)        strcpy(a, c)
#define T_STRNCPY(a, b, c, d)    strncpy(a, c, d)

#define T_STRCAT(a, b, c)        strcat(a, c)
#define T_STRNCAT(a, b, c, d)    strncat(a, c, d)
#define T_SPRINTF(a, b, c, ...)  sprintf(a, c, __VA_ARGS__)
#if defined(LINUX)
#define T_MEMCPY(a,b,c)          memmove(a,b,c)
#else
#define T_MEMCPY(a,b,c)          memcpy(a,b,c)
#endif
#endif

#define DBX_INTEGER_NEW(a)          Integer::New(isolate, a)
#define DBX_INT32_NEW(a)            Int32::New(isolate, a)
#define DBX_UINT32_NEW(a)           Uint32::New(isolate, a)
#define DBX_NUMBER_NEW(a)           Number::New(isolate, a)
#define DBX_BOOLEAN_NEW(a)          Boolean::New(isolate, a)
#define DBX_NULL()                  Null(isolate)
#define DBX_DATE(a)                 Date::New(isolate, a)

#define DBX_OBJECT_NEW()            Object::New(isolate)
#define DBX_ARRAY_NEW(a)            Array::New(isolate, a)

#define DBX_NODE_SET_PROTOTYPE_METHOD(a, b, c)    NODE_SET_PROTOTYPE_METHOD(a, b, c);
/*
#define DBX_NODE_SET_PROTOTYPE_METHOD(a, b, c)    dbx_set_prototype_method(a, c, b, b);
*/

#if DBX_NODE_VERSION >= 100000

#if 0

#define DBX_GET_ISOLATE \
   if (c->isolate == NULL) { \
      c->isolate = args.GetIsolate(); \
      HandleScope scope(c->isolate); \
   } \
   Isolate* isolate = c->isolate; \

#define DBX_GET_ICONTEXT \
   if (c->isolate == NULL) { \
      c->isolate = args.GetIsolate(); \
      HandleScope scope(c->isolate); \
   } \
   if (c->got_icontext == 0) { \
      c->icontext = c->isolate->GetCurrentContext(); \
      c->got_icontext = 1; \
   } \
   Isolate* isolate = c->isolate; \
   Local<Context> icontext = c->icontext; \

#elif 1

#define DBX_GET_ISOLATE \
   if (c->isolate == NULL) { \
      c->isolate = args.GetIsolate(); \
      HandleScope scope(c->isolate); \
   } \
   Isolate* isolate = c->isolate; \

#define DBX_GET_ICONTEXT \
   if (c->isolate == NULL) { \
      c->isolate = args.GetIsolate(); \
      HandleScope scope(c->isolate); \
   } \
   Isolate* isolate = c->isolate; \
   Local<Context> icontext = isolate->GetCurrentContext(); \
   c->icontext = icontext;\
   c->got_icontext = 1; \

#else

#define DBX_GET_ISOLATE \
   c->isolate = args.GetIsolate(); \
   HandleScope scope(c->isolate); \
   Isolate* isolate = c->isolate; \

#define DBX_GET_ICONTEXT \
   c->isolate = args.GetIsolate(); \
   HandleScope scope(c->isolate); \
   c->icontext = c->isolate->GetCurrentContext(); \
   Isolate* isolate = c->isolate; \
   Local<Context> icontext = c->icontext; \

#endif

#else

#define DBX_GET_ISOLATE \
   if (c->isolate == NULL) { \
      c->isolate = args.GetIsolate(); \
      HandleScope scope(c->isolate); \
   } \
   Isolate* isolate = c->isolate; \

#define DBX_GET_ICONTEXT \
   if (c->isolate == NULL) { \
      c->isolate = args.GetIsolate(); \
      HandleScope scope(c->isolate); \
   } \
   Isolate* isolate = c->isolate; \

#endif


#if DBX_NODE_VERSION >= 120000

#define DBX_DBFUN_START(C, PCON, PMETH) \
   if (!C->open) { \
      if (PCON && PCON->error[0]) { \
         isolate->ThrowException(Exception::Error(String::NewFromUtf8(isolate, PCON->error, NewStringType::kNormal).ToLocalChecked())); \
         dbx_request_memory_free(PCON, PMETH, 0); \
         return; \
      } \
      else { \
         isolate->ThrowException(Exception::Error(String::NewFromUtf8(isolate, "Database not open", NewStringType::kNormal).ToLocalChecked())); \
         dbx_request_memory_free(PCON, PMETH, 0); \
         return; \
      } \
   } \

#else

#define DBX_DBFUN_START(C, PCON, PMETH) \
   if (!C->open) { \
      if (PCON && PCON->error[0]) { \
         isolate->ThrowException(Exception::Error(String::NewFromUtf8(isolate, PCON->error))); \
         dbx_request_memory_free(PCON, PMETH, 0); \
         return; \
      } \
      else { \
         isolate->ThrowException(Exception::Error(String::NewFromUtf8(isolate, "Database not open"))); \
         dbx_request_memory_free(PCON, PMETH, 0); \
         return; \
      } \
   } \

#endif

#if DBX_NODE_VERSION >= 120000
#define DBX_GET(a,b)                a->Get(icontext,b).ToLocalChecked()
#define DBX_SET(a,b,c)              a->Set(icontext,b,c).FromJust()
#define DBX_TO_OBJECT(a)            a->ToObject(icontext).ToLocalChecked()
#define DBX_TO_STRING(a)            a->ToString(icontext).ToLocalChecked()
#define DBX_TO_BOOLEAN(a)           a->ToBoolean(isolate)
#define DBX_NUMBER_VALUE(a)         a->NumberValue(icontext).ToChecked()
#define DBX_INT32_VALUE(a)          a->Int32Value(icontext).FromJust()
#define DBX_WRITE_UTF8(a,b)         a->WriteUtf8(isolate, b)
#define DBX_WRITE(a,b)              a->Write(isolate, (uint16_t *) b)
#define DBX_WRITE_ONE_BYTE(a,b)     a->WriteOneByte(isolate, b)
#define DBX_UTF8_LENGTH(a)          a->Utf8Length(isolate)
#define DBX_LENGTH(a)               a->Length()
#elif DBX_NODE_VERSION >= 100000
#define DBX_GET(a,b)                a->Get(icontext,b).ToLocalChecked()
#define DBX_SET(a,b,c)              a->Set(icontext,b,c).FromJust()
#define DBX_TO_OBJECT(a)            a->ToObject(icontext).ToLocalChecked()
#define DBX_TO_STRING(a)            a->ToString(icontext).ToLocalChecked()
#define DBX_TO_BOOLEAN(a)           a->ToBoolean(icontext).ToLocalChecked()
#define DBX_NUMBER_VALUE(a)         a->NumberValue(icontext).ToChecked()
#define DBX_INT32_VALUE(a)          a->Int32Value(icontext).FromJust()
#define DBX_WRITE_UTF8(a,b)         a->WriteUtf8(b)
#define DBX_WRITE(a,b)              a->Write((uint16_t *) b)
#define DBX_WRITE_ONE_BYTE(a,b)     a->WriteOneByte(b)
#define DBX_UTF8_LENGTH(a)          a->Utf8Length()
#define DBX_LENGTH(a)               a->Length()
#else
#define DBX_GET(a,b)                a->Get(b)
#define DBX_SET(a,b,c)              a->Set(b,c)
#define DBX_TO_OBJECT(a)            a->ToObject()
#define DBX_TO_STRING(a)            a->ToString()
#define DBX_TO_BOOLEAN(a)           a->ToBoolean()
#define DBX_NUMBER_VALUE(a)         a->NumberValue()
#define DBX_INT32_VALUE(a)          a->Int32Value()
#define DBX_WRITE_UTF8(a,b)         a->WriteUtf8(b)
#define DBX_WRITE(a,b)              a->Write((uint16_t *) b)
#define DBX_WRITE_ONE_BYTE(a,b)     a->WriteOneByte(b)
#define DBX_UTF8_LENGTH(a)          a->Utf8Length()
#define DBX_LENGTH(a)               a->Length()
#endif

#define DBX_INTEGER_NEW(a)          Integer::New(isolate, a)
#define DBX_OBJECT_NEW()            Object::New(isolate)
#define DBX_ARRAY_NEW(a)            Array::New(isolate, a)
#define DBX_ARRAY_NEW(a)            Array::New(isolate, a)
#define DBX_NUMBER_NEW(a)           Number::New(isolate, a)
#define DBX_BOOLEAN_NEW(a)          Boolean::New(isolate, a)
#define DBX_NULL()                  Null(isolate)

#define DBX_CALLBACK_FUN(JSNARG, CB, ASYNC) \
   JSNARG = args.Length(); \
   if (JSNARG > 0 && args[JSNARG - 1]->IsFunction()) { \
      ASYNC = 1; \
      JSNARG --; \
   } \
   else { \
      ASYNC = 0; \
   } \

#define DBX_DBFUN_END(C)

#define DBX_DB_LOCK(TIMEOUT) \
   if (pcon->use_mutex) { \
      dbx_mutex_lock(pcon->p_mutex, TIMEOUT); \
   } \

#define DBX_DB_LOCK_EX(RC, TIMEOUT) \
   if (pcon->use_mutex) { \
      RC = dbx_mutex_lock(pcon->p_mutex, TIMEOUT); \
   } \

#define DBX_DB_UNLOCK() \
   if (pcon->use_mutex) { \
      dbx_mutex_unlock(pcon->p_mutex); \
   } \

#define DBX_DB_UNLOCK_EX(RC) \
   if (pcon->use_mutex) { \
      RC = dbx_mutex_unlock(pcon->p_mutex); \
   } \


typedef void      async_rtn;

#if defined(_WIN32)

typedef DWORD           DBXTHID;
typedef HINSTANCE       DBXPLIB;
typedef FARPROC         DBXPROC;

#else /* #if defined(_WIN32) */
   
typedef pthread_t       DBXTHID;
typedef void            *DBXPLIB;
typedef void            *DBXPROC;
#if !defined(NODE_ENGINE_CHAKRACORE)
typedef unsigned long   DWORD;
#endif
typedef unsigned long   WORD;
typedef int             SOCKET;

#endif /* #if defined(_WIN32) */


typedef struct tagDBXZV {
   unsigned char  dbtype;
   double         dbx_version;
   int            family;
   int            release;
   int            majorversion;
   int            minorversion;
   int            patch;
   int            dbx_build;
   unsigned long  vnumber; /* yymbbbb */
   char           version[64];
   char           db_version[128];
} DBXZV, *PDBXZV;


typedef struct tagDBXMUTEX {
   unsigned char     created;
   int               stack;
#if defined(_WIN32)
   HANDLE            h_mutex;
#else
   pthread_mutex_t   h_mutex;
#endif /* #if defined(_WIN32) */
   DBXTHID           thid;
} DBXMUTEX, *PDBXMUTEX;

typedef struct tagDBXTID {
   int         thread_id;
   DBXMUTEX    *p_mutex;
   DBXZV       *p_zv;
} DBXTID, *PDBXTID;


/* Cache/IRIS */

#define CACHE_MAXSTRLEN	32767
#define CACHE_MAXLOSTSZ	3641144

typedef char		Callin_char_t;
#define CACHE_INT64 long long
#define CACHESTR	CACHE_ASTR

typedef struct {
   unsigned short len;
   Callin_char_t  str[CACHE_MAXSTRLEN];
} CACHE_ASTR, *CACHE_ASTRP;

typedef struct {
   unsigned int	len;
   union {
      Callin_char_t * ch;
      unsigned short *wch;
      unsigned short *lch;
   } str;
} CACHE_EXSTR, *CACHE_EXSTRP;

#define CACHE_TTALL     1
#define CACHE_TTNEVER   8
#define CACHE_PROGMODE  32

#define CACHE_INCREMENTAL_LOCK   1
#define CACHE_SHARED_LOCK        2
#define CACHE_IMMEDIATE_RELEASE  4

#define CACHE_INT	      1
#define CACHE_DOUBLE	   2
#define CACHE_ASTRING   3

#define CACHE_CHAR      4
#define CACHE_INT2      5
#define CACHE_INT4      6
#define CACHE_INT8      7
#define CACHE_UCHAR     8
#define CACHE_UINT2     9
#define CACHE_UINT4     10
#define CACHE_UINT8     11
#define CACHE_FLOAT     12
#define CACHE_HFLOAT    13
#define CACHE_UINT      14
#define CACHE_WSTRING   15
#define CACHE_OREF      16
#define CACHE_LASTRING  17
#define CACHE_LWSTRING  18
#define CACHE_IEEE_DBL  19
#define CACHE_HSTRING   20
#define CACHE_UNDEF     21

#define CACHE_CHANGEPASSWORD  -16
#define CACHE_ACCESSDENIED    -15
#define CACHE_EXSTR_INUSE     -14
#define CACHE_NORES	         -13
#define CACHE_BADARG	         -12
#define CACHE_NOTINCACHE      -11
#define CACHE_RETTRUNC 	      -10
#define CACHE_ERUNKNOWN	      -9	
#define CACHE_RETTOOSMALL     -8	
#define CACHE_NOCON 	         -7
#define CACHE_INTERRUPT       -6
#define CACHE_CONBROKEN       -4
#define CACHE_STRTOOLONG      -3
#define CACHE_ALREADYCON      -2
#define CACHE_FAILURE	      -1
#define CACHE_SUCCESS 	      0

#define CACHE_ERMXSTR         5
#define CACHE_ERNOLINE        8
#define CACHE_ERUNDEF         9
#define CACHE_ERSYSTEM        10
#define CACHE_ERSUBSCR        16
#define CACHE_ERNOROUTINE     17
#define CACHE_ERSTRINGSTACK   20
#define CACHE_ERUNIMPLEMENTED 22
#define CACHE_ERARGSTACK      25
#define CACHE_ERPROTECT       27
#define CACHE_ERPARAMETER     40
#define CACHE_ERNAMSP         83
#define CACHE_ERWIDECHAR      89
#define CACHE_ERNOCLASS       122
#define CACHE_ERBADOREF       119
#define CACHE_ERNOMETHOD      120
#define CACHE_ERNOPROPERTY    121

#define CACHE_ETIMEOUT        -100
#define CACHE_BAD_STRING      -101
#define CACHE_BAD_NAMESPACE   -102
#define CACHE_BAD_GLOBAL      -103
#define CACHE_BAD_FUNCTION    -104

/* End of Cache/IRIS */


/* YottaDB */

#define YDB_OK       0
#define YDB_DEL_TREE 1

#define YDB_INT_MAX        ((int)0x7fffffff)
#define YDB_TP_RESTART     (YDB_INT_MAX - 1)
#define YDB_TP_ROLLBACK    (YDB_INT_MAX - 2)
#define YDB_NODE_END       (YDB_INT_MAX - 3)
#define YDB_LOCK_TIMEOUT   (YDB_INT_MAX - 4)
#define YDB_NOTOK          (YDB_INT_MAX - 5)

typedef struct {
   unsigned int   len_alloc;
   unsigned int   len_used;
   char		      *buf_addr;
} ydb_buffer_t;

typedef struct {
   unsigned long  length;
   char		      *address;
} ydb_string_t;

typedef struct {
   ydb_string_t   rtn_name;
   void		      *handle;
} ci_name_descriptor;

typedef ydb_buffer_t    DBXSTR;
typedef char            ydb_char_t;
typedef long            ydb_long_t;


/* End of YottaDB */


typedef struct tagDBXCVAL {
   void           *pstr;
   CACHE_EXSTR    zstr;
} DBXCVAL, *PDBXCVAL;


typedef struct tagDBXVAL {
   int            type;
   int            sort;
   union {
      int            int32;
      long long      int64;
      double         real;
      unsigned int   oref;
   } num;
   unsigned long  offs;
   unsigned int   csize;
   ydb_buffer_t   svalue;
   DBXCVAL        cvalue;
   struct tagDBXVAL *pnext;
} DBXVAL, *PDBXVAL;


typedef struct tagDBXGREF {
   char *         global;
   DBXVAL *       pkey;
} DBXGREF, *PDBXGREF;

typedef struct tagDBXFREF {
   char *         function;
} DBXFREF, *PDBXFREF;

typedef struct tagDBXCREF {
   short          optype;
   char *         class_name;
   int            oref;
} DBXCREF, *PDBXCREF;

#define DBX_SQL_MGSQL      1
#define DBX_SQL_MAXCOL     128

typedef struct tagDBXSQLCOL {
   short          type;
   ydb_buffer_t   name;
   char *         stype;
} DBXSQLCOL, *PDBXSQLCOL;

typedef struct tagDBXSQL {
   short          sql_type;
   int            sql_no;
   char *         sql_script;
   int            sql_script_len;
   int            sqlcode;
   char           sqlstate[8];
   unsigned long  row_no;
   int            no_cols;
   DBXSQLCOL *    cols[DBX_SQL_MAXCOL];
} DBXSQL, *PDBXSQL;

#define DBX_DBTYPE_CACHE     11
#define DBX_DBTYPE_IRIS      12
#define DBX_DBTYPE_BDB        1
#define DBX_DBTYPE_LMDB       2

#define DBX_KEYTYPE_INT       1
#define DBX_KEYTYPE_STR       2
#define DBX_KEYTYPE_M         3


typedef struct tagDBXBDBSO {
   short             loaded;
   int               no_connections;
   int               multiple_connections;
   char              libdir[256];
   char              libnam[256];
   char              funprfx[8];
   char              dbname[32];
   DBXPLIB           p_library;
   DBXZV             zv;
   DB *              pdb; /* DB structure handle */
   DB_ENV *          penv; /* DB environment handle */

   int               (* p_db_env_create)        (DB_ENV **penv, u_int32_t flags);
   int               (* p_db_create)            (DB **pdb, DB_ENV *dbenv, u_int32_t flags);
   char *            (* p_db_full_version)      (int *family, int *release, int *major, int *minor, int *patch);

} DBXBDBSO, *PDBXBDBSO;


typedef struct tagDBXLMDBSO {
   short             loaded;
   int               no_connections;
   int               multiple_connections;
   char              libdir[256];
   char              libnam[256];
   char              funprfx[8];
   char              dbname[32];
   DBXPLIB           p_library;
   DBXZV             zv;
   MDB_dbi           db; /* DB structure handle */
   MDB_dbi *         pdb; /* DB structure handle */
   MDB_env *         penv; /* DB environment handle */
   MDB_txn *         ptxn; /* DB transaction handle */
   MDB_txn *         ptxnro; /* DB transaction handle - read only */

   int               (* p_mdb_env_create)       (MDB_env **env);
   int               (* p_mdb_env_open)         (MDB_env *env, const char *path, unsigned int flags, mdb_mode_t mode);
   void              (* p_mdb_env_close)        (MDB_env *env);
   int               (* p_mdb_env_set_maxdbs)   (MDB_env *env, MDB_dbi dbs);
   int               (* p_mdb_env_set_mapsize)  (MDB_env *env, size_t size);
   int               (* p_mdb_env_stat)         (MDB_env *env, MDB_stat *stat);

   int               (* p_mdb_txn_begin)        (MDB_env *env, MDB_txn *parent, unsigned int flags, MDB_txn **txn);
   int               (* p_mdb_txn_commit)       (MDB_txn *txn);
   void              (* p_mdb_txn_abort)        (MDB_txn *txn);
   void              (* p_mdb_txn_reset)        (MDB_txn *txn);
   void              (* p_mdb_txn_renew)        (MDB_txn *txn);

   int               (* p_mdb_dbi_open)         (MDB_txn *txn, const char *name, unsigned int flags, MDB_dbi *dbi);
   void              (* p_mdb_dbi_close)        (MDB_env *env, MDB_dbi dbi);

   int               (* p_mdb_put)              (MDB_txn *txn, MDB_dbi dbi, MDB_val *key, MDB_val *data, unsigned int flags);
   int               (* p_mdb_get)              (MDB_txn *txn, MDB_dbi dbi, MDB_val *key, MDB_val *data);
   int               (* p_mdb_del)              (MDB_txn *txn, MDB_dbi dbi, MDB_val *key, MDB_val *data);

   int               (* p_mdb_cursor_open)      (MDB_txn *txn, MDB_dbi dbi, MDB_cursor **cursor);
   void              (* p_mdb_cursor_close)     (MDB_cursor *cursor);
   int               (* p_mdb_cursor_renew)     (MDB_txn *txn, MDB_cursor *cursor);
   int               (* p_mdb_cursor_get)       (MDB_cursor *cursor, MDB_val *key, MDB_val *data, MDB_cursor_op op);

   char *            (* p_mdb_strerror)         (int err);
   char *            (* p_mdb_version)          (int *major, int *minor, int *patch);

} DBXLMDBSO, *PDBXLMDBSO;


typedef struct tagDBXCON {
   short          dbtype;
   short          key_type;
   short          utf8;
   short          use_mutex;
   short          error_mode; /* v1.3.10 */
   char           type[64];
   char           db_library[256];
   char           db_file[256];
   size_t         db_size;
   char           env_dir[256];
   char           username[64];
   char           password[64];
   char           nspace[64];
   char           input_device[64];
   char           output_device[64];
   int            error_code;
   char           error[DBX_ERROR_SIZE];
   DBXMUTEX       *p_mutex;
   DBXZV          *p_zv;

   DBXBDBSO       *p_bdb_so;
   DBXLMDBSO      *p_lmdb_so;
   DBXZV          zv;

   int            tlevel;
   int            tlevelro;
   int            tstatus;
   int            tstatusro;

   int            (* p_dbxfun) (struct tagDBXMETH * pmeth);

   void           *pmeth_base;

   int            log_errors;
   int            log_functions;
   int            log_transmissions;
   char           log_file[256];
   char           log_filter[512];

} DBXCON, *PDBXCON;


typedef struct tagDBXKEY {
   DBXSTR         ibuffer;
   int            argc;
   DBXVAL         args[DBX_MAXARGS];
} DBXKEY, *PDBXKEY;


typedef struct tagDBXMETH {
   short          done;
   short          lock;
   short          increment;
   int            binary;
   int            jsargc;
   DBXKEY         key;
   DBXVAL         output_val;
   DBXVAL         output_key;
   DBXSQL         *psql;
   int            (* p_dbxfun) (struct tagDBXMETH * pmeth);
   DBXCON         *pcon;
   int            error_code;
   char           error[DBX_ERROR_SIZE];
   DBC            *pbdbcursor;
   MDB_cursor     *plmdbcursor;
} DBXMETH, *PDBXMETH;


typedef struct tagDBXQR {
   ydb_buffer_t   global_name;
   DBXKEY         key;
   DBXVAL         data;
} DBXQR, *PDBXQR;


struct dbx_pool_task {
#if !defined(_WIN32)
   pthread_t   parent_tid;
#endif
   int         task_id;
   DBXMETH     *pmeth;
   struct dbx_pool_task *next;
};


class DBX_DBNAME : public node::ObjectWrap
{
public:

   int            dbx_count;
   int            counter;
   short          open;
   short          use_mutex;
   short          handle_sigint;
   short          handle_sigterm;
   unsigned long  pid;
   DBXMUTEX *     p_mutex;
   DBXCON *       pcon;
   DBXCON         con;

   v8::Isolate             *isolate;
   v8::Local<v8::Context>  icontext;
   short                   got_icontext;

   static v8::Persistent<v8::Function> constructor;

   struct dbx_baton_t {
      DBX_DBNAME *                  c;
      void *                        gx;
      void *                        cx;
      void *                        clx;
      v8::Local<v8::String>         result_str;
      v8::Local<v8::Object>         result_obj;
      v8::Persistent<v8::Function>  cb;
      v8::Isolate *                 isolate;
      DBXCON *                      pcon;
      DBXMETH *                     pmeth;
   };

#if DBX_NODE_VERSION >= 100000
   static void                   Init                             (v8::Local<v8::Object> exports);
#else
   static void                   Init                             (v8::Handle<v8::Object> exports);
#endif

   explicit                      DBX_DBNAME                       (int value = 0);
                                 ~DBX_DBNAME();
   static void                   New                              (const v8::FunctionCallbackInfo<v8::Value>& args);

   static dbx_baton_t *          dbx_make_baton                   (DBX_DBNAME *c, DBXMETH *pmeth);
   static int                    dbx_destroy_baton                (dbx_baton_t *baton, DBXMETH *pmeth);

   static int                    dbx_queue_task                   (void * work_cb, void * after_work_cb, dbx_baton_t *baton, short context);
   static async_rtn              dbx_process_task                 (uv_work_t *req);
   static async_rtn              dbx_uv_close_callback            (uv_work_t *req);
   static async_rtn              dbx_invoke_callback              (uv_work_t *req);
   static async_rtn              dbx_invoke_callback_sql_execute  (uv_work_t *req);

   static void                   About                            (const v8::FunctionCallbackInfo<v8::Value>& args);
   static void                   Version                          (const v8::FunctionCallbackInfo<v8::Value>& args);
   static void                   SetLogLevel                      (const v8::FunctionCallbackInfo<v8::Value>& args);
   static void                   GetErrorMessage                  (const v8::FunctionCallbackInfo<v8::Value>& args);
   static void                   LogMessage                       (const v8::FunctionCallbackInfo<v8::Value>& args);
   static void                   Charset                          (const v8::FunctionCallbackInfo<v8::Value>& args);
   static void                   Open                             (const v8::FunctionCallbackInfo<v8::Value>& args);
   static void                   Close                            (const v8::FunctionCallbackInfo<v8::Value>& args);
   static int                    LogFunction                      (DBX_DBNAME *c, const v8::FunctionCallbackInfo<v8::Value>& args, void *pctx, char *name);
   static v8::Local<v8::String>  StringifyJSON                    (DBX_DBNAME *c, v8::Local<v8::Object> json);
   static int                    GlobalReference                  (DBX_DBNAME *c, const v8::FunctionCallbackInfo<v8::Value>& args, DBXMETH *pmeth, DBXGREF *pgref, short context);

   static void                   Get                              (const v8::FunctionCallbackInfo<v8::Value>& args);
   static void                   Get_bx                           (const v8::FunctionCallbackInfo<v8::Value>& args);
   static void                   GetEx                            (const v8::FunctionCallbackInfo<v8::Value>& args, int binary);
   static void                   Set                              (const v8::FunctionCallbackInfo<v8::Value>& args);
   static void                   Defined                          (const v8::FunctionCallbackInfo<v8::Value>& args);
   static void                   Delete                           (const v8::FunctionCallbackInfo<v8::Value>& args);
   static void                   Next                             (const v8::FunctionCallbackInfo<v8::Value>& args);
   static void                   Previous                         (const v8::FunctionCallbackInfo<v8::Value>& args);
   static void                   Increment                        (const v8::FunctionCallbackInfo<v8::Value>& args);
   static void                   Lock                             (const v8::FunctionCallbackInfo<v8::Value>& args);
   static void                   Unlock                           (const v8::FunctionCallbackInfo<v8::Value>& args);
   static void                   Sleep                            (const v8::FunctionCallbackInfo<v8::Value>& args);
   static void                   MGlobal                          (const v8::FunctionCallbackInfo<v8::Value>& args);
   static void                   MGlobal_Close                    (const v8::FunctionCallbackInfo<v8::Value>& args);
   static void                   MGlobalQuery                     (const v8::FunctionCallbackInfo<v8::Value>& args);
   static void                   MGlobalQuery_Close               (const v8::FunctionCallbackInfo<v8::Value>& args);
   static void                   SQL                              (const v8::FunctionCallbackInfo<v8::Value>& args);
   static void                   SQL_Close                        (const v8::FunctionCallbackInfo<v8::Value>& args);

   static void                   Dump                             (const v8::FunctionCallbackInfo<v8::Value>& args);
   static void                   Benchmark                        (const v8::FunctionCallbackInfo<v8::Value>& args);

private:

   unsigned char  csize;
};


DBXMETH *                  dbx_request_memory         (DBXCON *pcon, short context);
DBXMETH *                  dbx_request_memory_alloc   (DBXCON *pcon, short context);
int                        dbx_request_memory_free    (DBXCON *pcon, DBXMETH *pmeth, short context);
char *                     dbx_buffer_resize          (char **ppbuffer, unsigned int data_size, unsigned int req_size, unsigned int *size);

#if DBX_NODE_VERSION >= 100000
void                       dbx_set_prototype_method   (v8::Local<v8::FunctionTemplate> t, v8::FunctionCallback callback, const char* name, const char* data);
#else
void                       dbx_set_prototype_method   (v8::Handle<v8::FunctionTemplate> t, v8::FunctionCallback callback, const char* name, const char* data);
#endif

v8::Local<v8::Object>      dbx_is_object              (v8::Local<v8::Value> value, int *otype);
int                        dbx_string8_length         (v8::Isolate * isolate, v8::Local<v8::String> str, int utf8);
v8::Local<v8::String>      dbx_new_string8            (v8::Isolate * isolate, char * buffer, int utf8);
v8::Local<v8::String>      dbx_new_string8n           (v8::Isolate * isolate, char * buffer, unsigned long len, int utf8);
int                        dbx_write_char8            (v8::Isolate * isolate, v8::Local<v8::String> str, char * buffer, int utf8);

int                        dbx_ibuffer_add            (DBXMETH *pmeth, DBXKEY *pkey, v8::Isolate * isolate, int argn, v8::Local<v8::String> str, char * buffer, int buffer_len, short context);
int                        dbx_is_number              (DBXVAL *pval);
int                        dbx_set_number             (DBXVAL *pval, unsigned char *px);
int                        dbx_split_key              (DBXVAL *keys, char * key, int key_len);
int                        dbx_dump_key               (char * key, int key_len);
int                        dbx_memcpy                 (void * to, void * from, size_t size);
int                        dbx_memcpy_ex              (DBXSTR * to, void * from, size_t size);
int                        dbx_memcpy_exx             (DBXSTR * to, void * from, size_t size);
int                        dbx_cursor_init            (void *pcx);
int                        dbx_global_reset           (const v8::FunctionCallbackInfo<v8::Value>& args, v8::Isolate * isolate, DBXCON *pcon, DBXMETH *pmeth, void *pgx, int argc_offset, short context);
int                        dbx_cursor_reset           (const v8::FunctionCallbackInfo<v8::Value>& args, v8::Isolate * isolate, DBXCON *pcon, DBXMETH *pmeth, void *pcx, int argc_offset, short context);

int                        bdb_load_library           (DBXCON *pcon);
int                        bdb_open                   (DBXMETH *pmeth);
int                        bdb_parse_zv               (char *zv, DBXZV * p_bdb_sv);
int                        bdb_next                   (DBXMETH *pmeth, DBXKEY *pkey, DBXVAL *pkeyval, DBXVAL *pdataval, int context);
int                        bdb_previous               (DBXMETH *pmeth, DBXKEY *pkey, DBXVAL *pkeyval, DBXVAL *pdataval, int context);
int                        bdb_key_compare            (DBT *key1, DBT *key2, int compare_max, short keytype);
int                        bdb_get                    (DBXCON *pcon, DBT *key, DBT *data, DBXSTR *dbx_data);
int                        bdb_cursor_get             (DBC *pcursor, DBT *key, DBXSTR *dbx_key, DBT *data, DBXSTR *dbx_data, int context);
int                        bdb_resize_buffer          (DBT *key, DBXSTR *dbx_key, DBT *data, DBXSTR *dbx_data, int context);
int                        bdb_error_message          (DBXCON *pcon, int error_code);

int                        lmdb_load_library          (DBXCON *pcon);
int                        lmdb_open                  (DBXMETH *pmeth);
int                        lmdb_parse_zv              (char *zv, DBXZV * p_lmdb_sv);
int                        lmdb_start_ro_transaction  (DBXMETH *pmeth, int context);
int                        lmdb_commit_ro_transaction (DBXMETH *pmeth, int context);
int                        lmdb_start_qro_transaction (DBXMETH *pmeth, MDB_txn **ptxn, int context);
int                        lmdb_commit_qro_transaction(DBXMETH *pmeth, MDB_txn **ptxn, int context);
int                        lmdb_next                  (DBXMETH *pmeth, DBXKEY *pkey, DBXVAL *pkeyval, DBXVAL *pdataval, int context);
int                        lmdb_previous              (DBXMETH *pmeth, DBXKEY *pkey, DBXVAL *pkeyval, DBXVAL *pdataval, int context);
int                        lmdb_key_compare           (MDB_val *key1, MDB_val *key2, int compare_max, short keytype);
int                        lmdb_error_message         (DBXCON *pcon, int error_code);

int                        dbx_version                (DBXMETH *pmeth);
int                        dbx_open                   (DBXMETH *pmeth);
int                        dbx_do_nothing             (DBXMETH *pmeth);
int                        dbx_close                  (DBXMETH *pmeth);
int                        dbx_global_reference       (DBXMETH *pmeth);

int                        dbx_get                    (DBXMETH *pmeth);
int                        dbx_set                    (DBXMETH *pmeth);
int                        dbx_defined                (DBXMETH *pmeth);
int                        dbx_delete                 (DBXMETH *pmeth);
int                        dbx_next                   (DBXMETH *pmeth);
int                        dbx_previous               (DBXMETH *pmeth);
int                        dbx_increment              (DBXMETH *pmeth);
int                        dbx_lock                   (DBXMETH *pmeth);
int                        dbx_unlock                 (DBXMETH *pmeth);
int                        dbx_merge                  (DBXMETH *pmeth);
int                        dbx_sql_execute            (DBXMETH *pmeth);
int                        dbx_sql_row                (DBXMETH *pmeth, int rn, int dir);
int                        dbx_sql_cleanup            (DBXMETH *pmeth);

int                        dbx_global_directory       (DBXMETH *pmeth, DBXQR *pqr_prev, short dir, int *counter);
int                        dbx_global_order           (DBXMETH *pmeth, DBXQR *pqr_prev, short dir, short getdata, int *counter);
int                        dbx_global_query           (DBXMETH *pmeth, DBXQR *pqr_next, DBXQR *pqr_prev, short dir, short getdata, int *fixed_key_len, int *counter);
int                        dbx_display_key            (char *key, int key_size, char *buffer);

int                        dbx_launch_thread          (DBXMETH *pmeth);
#if defined(_WIN32)
LPTHREAD_START_ROUTINE     dbx_thread_main            (LPVOID pargs);
#else
void *                     dbx_thread_main            (void *pargs);
#endif

struct dbx_pool_task *     dbx_pool_add_task          (DBXMETH *pmeth);
struct dbx_pool_task *     dbx_pool_get_task          (void);
void                       dbx_pool_execute_task      (struct dbx_pool_task *task, int thread_id);
void *                     dbx_pool_requests_loop     (void *data);
int                        dbx_pool_thread_init       (DBXCON *pcon, int num_threads);
int                        dbx_pool_submit_task       (DBXMETH *pmeth);
int                        dbx_add_block_size         (unsigned char *block, unsigned long offset, unsigned long data_len, int dsort, int dtype);
unsigned long              dbx_get_block_size         (unsigned char *block, unsigned long offset, int *dsort, int *dtype);
int                        dbx_set_size               (unsigned char *str, unsigned long data_len, short big_endian);
unsigned long              dbx_get_size               (unsigned char *str, short big_endian);
void *                     dbx_realloc                (void *p, int curr_size, int new_size, short id);
void *                     dbx_malloc                 (int size, short id);
int                        dbx_free                   (void *p, short id);
DBXQR *                    dbx_alloc_dbxqr            (DBXQR *pqr, int dsize, short context);
int                        dbx_free_dbxqr             (DBXQR *pqr);
int                        dbx_ucase                  (char *string);
int                        dbx_lcase                  (char *string);

int                        dbx_create_string          (DBXSTR *pstr, void *data, short type);

int                        dbx_dump_mref              (DBXCON *pcon, DBXMETH *pmeth, char *cmnd, DBXVAL *presult, int rc);
int                        dbx_log_transmission       (DBXCON *pcon, DBXMETH *pmeth, char *name);
int                        dbx_log_response           (DBXCON *pcon, char *ibuffer, int ibuffer_len, char *name);
int                        dbx_buffer_dump            (DBXMETH *pmeth, void *buffer, unsigned int len, char *title, unsigned char csize, short mode);
int                        dbx_log_event              (DBXCON *pcon, char *message, char *title, int level);
int                        dbx_log_buffer             (DBXCON *pcon, char *buffer, int buffer_len, char *title, int level);
int                        dbx_test_file_access       (char *file, int mode);
DBXPLIB                    dbx_dso_load               (char * library);
DBXPROC                    dbx_dso_sym                (DBXPLIB p_library, char * symbol);
int                        dbx_dso_unload             (DBXPLIB p_library);
DBXTHID                    dbx_current_thread_id      (void);
unsigned long              dbx_current_process_id     (void);
int                        dbx_error_message          (DBXMETH *pmeth, int error_code, char * function);

int                        dbx_mutex_create           (DBXMUTEX *p_mutex);
int                        dbx_mutex_lock             (DBXMUTEX *p_mutex, int timeout);
int                        dbx_mutex_unlock           (DBXMUTEX *p_mutex);
int                        dbx_mutex_destroy          (DBXMUTEX *p_mutex);
int                        dbx_enter_critical_section (void *p_crit);
int                        dbx_leave_critical_section (void *p_crit);
int                        dbx_sleep                  (unsigned long msecs);

int                        dbx_fopen                  (FILE **pfp, const char *file, const char *mode);
int                        dbx_strcpy_s               (char *to, size_t size, const char *from, const char *file, const char *fun, const unsigned int line);
int                        dbx_strncpy_s              (char *to, size_t size, const char *from, size_t count, const char *file, const char *fun, const unsigned int line);
int                        dbx_strcat_s               (char *to, size_t size, const char *from, const char *file, const char *fun, const unsigned int line);
int                        dbx_strncat_s              (char *to, size_t size, const char *from, size_t count, const char *file, const char *fun, const unsigned int line);
int                        dbx_sprintf_               (char *to, size_t size, const char *format, const char *file, const char *fun, const unsigned int line, ...);

#endif

