/*
   ----------------------------------------------------------------------------
   | mg-dbx-bdb.node                                                          |
   | Author: Chris Munt cmunt@mgateway.com                                    |
   |                    chris.e.munt@gmail.com                                |
   | Copyright (c) 2016-2021 M/Gateway Developments Ltd,                      |
   | Surrey UK.                                                               |
   | All rights reserved.                                                     |
   |                                                                          |
   | http://www.mgateway.com                                                  |
   |                                                                          |
   | Licensed under the Apache License, Version 2.0 (the "License"); you may  |
   | not use this file except in compliance with the License.                 |
   | You may obtain a copy of the License at                                  |
   |                                                                          |
   | http://www.apache.org/licenses/LICENSE-2.0                               |
   |                                                                          |
   | Unless required by applicable law or agreed to in writing, software      |
   | distributed under the License is distributed on an "AS IS" BASIS,        |
   | WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. |
   | See the License for the specific language governing permissions and      |
   | limitations under the License.                                           |      
   |                                                                          |
   ----------------------------------------------------------------------------
*/

/*

Change Log:

Version 1.0.1 1 January 2021:
   First release.

Version 1.0.2 7 January 2021:
   Correct a fault in the processing of integer based keys.

Version 1.0.3 17 January 2021:
   Miscellaneous bug fixes.
   Extend the logging of request transmission data to include the corresponding response data.
   - Include 'r' in the log level.  For example: db.setloglevel("MyLog.log", "eftr", "");
   
Version 1.1.4 20 January 2021:
   Introduce the ability to specify a BDB environment for the purpose of implementing multi-process concurrent access to a database.

Version 1.1.5 23 February 2021:
   Correct a fault that resulted in a crash when loading the **mg-dbx** module in Node.js v10.

Version 1.2.6 2 March 2021:
   Add support for Lightning Memory-Mapped Database (LMDB).

Version 1.2.7 10 March 2021:
   Correct a fault that resulted in mglobal.previous() calls erroneously returning empty string - particularly in relation to records at the end of a BDB/LMDB database.

Version 1.2.8 12 April 2021:
   Correct a fault in the increment() method for LMDB.

*/


#include "mg-dbx-bdb.h"
#include "mg-global.h"
#include "mg-cursor.h"


#if defined(_WIN32)
#define DBX_LOG_FILE    "c:/temp/mg-dbx-bdb.log"
#else
#define DBX_LOG_FILE    "/tmp/mg-dbx-bdb.log"
extern int errno;
#endif

static int     dbx_counter             = 0;
static int     dbx_sql_counter         = 0;
int            dbx_total_tasks         = 0;
int            dbx_request_errors      = 0;

#if defined(_WIN32)
CRITICAL_SECTION  dbx_async_mutex;
#else
pthread_mutex_t   dbx_async_mutex        = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t   dbx_pool_mutex         = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t   dbx_result_mutex       = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t   dbx_task_queue_mutex   = PTHREAD_MUTEX_INITIALIZER;

pthread_cond_t    dbx_pool_cond           = PTHREAD_COND_INITIALIZER;
pthread_cond_t    dbx_result_cond         = PTHREAD_COND_INITIALIZER;

DBXTID            dbx_thr_id[DBX_THREADPOOL_MAX];
pthread_t         dbx_p_threads[DBX_THREADPOOL_MAX];
#endif

struct dbx_pool_task * tasks           = NULL;
struct dbx_pool_task * bottom_task     = NULL;

DBXCON * pcon_api = NULL;

DBXBDBSO *  p_bdb_so_global = NULL;
DBXLMDBSO * p_lmdb_so_global = NULL;
DBXMUTEX    mutex_global;

using namespace node;
using namespace v8;

static void dbxGoingDownSignal(int sig)
{
   /* printf("\r\nNode.js process interrupted (signal=%d)", sig); */
   exit(0);
}

#if defined(_WIN32) && defined(DBX_USE_SECURE_CRT)
/* Compile with: /Zi /MTd */
void dbxInvalidParameterHandler(const wchar_t* expression, const wchar_t* function, const wchar_t* file, unsigned int line, uintptr_t pReserved)
{
   printf("\r\nWindows Secure Function Error\r\nInvalid parameter detected in function at line %d\r\n", line);
   /* abort(); */
   return;
}
#endif


#if defined(_WIN32)
BOOL WINAPI DllMain(HINSTANCE hinstDLL, DWORD fdwReason, LPVOID lpReserved)
{
#if defined(_WIN32) && defined(DBX_USE_SECURE_CRT)
   _invalid_parameter_handler oldHandler, newHandler;
#endif

   switch (fdwReason)
   { 
      case DLL_PROCESS_ATTACH:
#if defined(_WIN32) && defined(DBX_USE_SECURE_CRT)
         newHandler = dbxInvalidParameterHandler;
         oldHandler = _set_invalid_parameter_handler(newHandler);
#endif
         InitializeCriticalSection(&dbx_async_mutex);
         break;
      case DLL_THREAD_ATTACH:
         break;
      case DLL_THREAD_DETACH:
         break;
      case DLL_PROCESS_DETACH:
         DeleteCriticalSection(&dbx_async_mutex);
         break;
   }
   return TRUE;
}
#endif

/* Start of DBX class methods */
#if DBX_NODE_VERSION >= 100000
void DBX_DBNAME::Init(Local<Object> exports)
#else
void DBX_DBNAME::Init(Handle<Object> exports)
#endif
{

#if DBX_NODE_VERSION >= 120000
   Isolate* isolate = exports->GetIsolate();
   Local<Context> icontext = isolate->GetCurrentContext();

   Local<FunctionTemplate> tpl = FunctionTemplate::New(isolate, New);
   tpl->SetClassName(String::NewFromUtf8(isolate, DBX_DBNAME_STR, NewStringType::kNormal).ToLocalChecked());
   tpl->InstanceTemplate()->SetInternalFieldCount(3);
#else
   Isolate* isolate = Isolate::GetCurrent();

   Local<FunctionTemplate> tpl = FunctionTemplate::New(isolate, New);
   tpl->InstanceTemplate()->SetInternalFieldCount(3); /* v1.1.5 */
   tpl->SetClassName(String::NewFromUtf8(isolate, DBX_DBNAME_STR));
#endif

   DBX_NODE_SET_PROTOTYPE_METHOD(tpl, "version", Version);
   DBX_NODE_SET_PROTOTYPE_METHOD(tpl, "setloglevel", SetLogLevel);
   DBX_NODE_SET_PROTOTYPE_METHOD(tpl, "logmessage", LogMessage);
   DBX_NODE_SET_PROTOTYPE_METHOD(tpl, "charset", Charset);
   DBX_NODE_SET_PROTOTYPE_METHOD(tpl, "open", Open);
   DBX_NODE_SET_PROTOTYPE_METHOD(tpl, "close", Close);
   DBX_NODE_SET_PROTOTYPE_METHOD(tpl, "get", Get);
   DBX_NODE_SET_PROTOTYPE_METHOD(tpl, "get_bx", Get_bx);
   DBX_NODE_SET_PROTOTYPE_METHOD(tpl, "set", Set);
   DBX_NODE_SET_PROTOTYPE_METHOD(tpl, "defined", Defined);
   DBX_NODE_SET_PROTOTYPE_METHOD(tpl, "delete", Delete);
   DBX_NODE_SET_PROTOTYPE_METHOD(tpl, "next", Next);
   DBX_NODE_SET_PROTOTYPE_METHOD(tpl, "previous", Previous);
   DBX_NODE_SET_PROTOTYPE_METHOD(tpl, "increment", Increment);
   DBX_NODE_SET_PROTOTYPE_METHOD(tpl, "lock", Lock);
   DBX_NODE_SET_PROTOTYPE_METHOD(tpl, "unlock", Unlock);
   DBX_NODE_SET_PROTOTYPE_METHOD(tpl, "mglobal", MGlobal);
   DBX_NODE_SET_PROTOTYPE_METHOD(tpl, "mglobal_close", MGlobal_Close);
   DBX_NODE_SET_PROTOTYPE_METHOD(tpl, "mglobalquery", MGlobalQuery);
   DBX_NODE_SET_PROTOTYPE_METHOD(tpl, "mglobalquery_close", MGlobalQuery_Close);
   DBX_NODE_SET_PROTOTYPE_METHOD(tpl, "sql", SQL);
   DBX_NODE_SET_PROTOTYPE_METHOD(tpl, "sql_close", SQL_Close);

   DBX_NODE_SET_PROTOTYPE_METHOD(tpl, "sleep", Sleep);
   DBX_NODE_SET_PROTOTYPE_METHOD(tpl, "dump", Dump);
   DBX_NODE_SET_PROTOTYPE_METHOD(tpl, "benchmark", Benchmark);

#if DBX_NODE_VERSION >= 120000
   constructor.Reset(isolate, tpl->GetFunction(icontext).ToLocalChecked());
   exports->Set(icontext, String::NewFromUtf8(isolate, DBX_DBNAME_STR, NewStringType::kNormal).ToLocalChecked(), tpl->GetFunction(icontext).ToLocalChecked()).FromJust();
#else
   constructor.Reset(isolate, tpl->GetFunction());
   exports->Set(String::NewFromUtf8(isolate, DBX_DBNAME_STR), tpl->GetFunction());
#endif

}


DBX_DBNAME::DBX_DBNAME(int value) : dbx_count(value)
{
}

DBX_DBNAME::~DBX_DBNAME()
{
}


void DBX_DBNAME::New(const FunctionCallbackInfo<Value>& args)
{
   Isolate* isolate = args.GetIsolate();
   HandleScope scope(isolate);
   DBX_DBNAME *c = new DBX_DBNAME();
   c->Wrap(args.This());

   /* V8 will set SetAlignedPointerInInternalField */
   /* args.This()->SetAlignedPointerInInternalField(0, c); */
   args.This()->SetInternalField(2, DBX_INTEGER_NEW(DBX_MAGIC_NUMBER));

   dbx_enter_critical_section((void *) &dbx_async_mutex);
   if (dbx_counter == 0) {
      mutex_global.created = 0;
      dbx_mutex_create(&(mutex_global));
   }
   c->counter = dbx_counter;
   dbx_counter ++;
   dbx_leave_critical_section((void *) &dbx_async_mutex);

   c->open = 0;

   c->csize = 8;

   c->pcon = NULL;

   c->use_mutex = 1;
   c->p_mutex = &mutex_global;
   c->handle_sigint = 0;
   c->handle_sigterm = 0;

   c->isolate = NULL;
   c->got_icontext = 0;

   c->pcon = (DBXCON *) dbx_malloc(sizeof(DBXCON), 0);
   memset((void *) c->pcon, 0, sizeof(DBXCON));

   c->pcon->p_mutex = &mutex_global;
   c->pcon->p_zv = NULL;

   c->pcon->pmeth_base = (void *) dbx_request_memory_alloc(c->pcon, 0);

   c->pcon->utf8 = 1; /* seems to be faster with UTF8 on! */
   c->pcon->db_library[0] = '\0';
   c->pcon->db_file[0] = '\0';
   c->pcon->env_dir[0] = '\0';
   c->pcon->key_type = 0;
   c->pcon->use_mutex = 1;
   c->pcon->p_bdb_so = NULL;
   c->pcon->p_lmdb_so = NULL;

   c->pcon->tlevel = 0;
   c->pcon->tlevelro = 0;
   c->pcon->tstatus = 0;
   c->pcon->tstatusro = 0;

   c->pcon->log_errors = 0;
   c->pcon->log_functions = 0;
   c->pcon->log_transmissions = 0;
   c->pcon->log_filter[0] = '\0';
   strcpy(c->pcon->log_file, DBX_LOG_FILE);

   args.GetReturnValue().Set(args.This());

   return;

}


DBX_DBNAME::dbx_baton_t * DBX_DBNAME::dbx_make_baton(DBX_DBNAME *c, DBXMETH *pmeth)
{
   dbx_baton_t *baton;

   baton = new dbx_baton_t();

   if (!baton) {
      return NULL;
   }
   baton->c = c;
   baton->gx = NULL;
   baton->cx = NULL;
   baton->clx = NULL;
   baton->pmeth = pmeth;
   return baton;
}


int DBX_DBNAME::dbx_destroy_baton(dbx_baton_t *baton, DBXMETH *pmeth)
{
   if (baton) {
      delete baton;
   }

   return 0;
}


int DBX_DBNAME::dbx_queue_task(void * work_cb, void * after_work_cb, DBX_DBNAME::dbx_baton_t *baton, short context)
{
   uv_work_t *_req = new uv_work_t;
   _req->data = baton;

#if DBX_NODE_VERSION >= 120000
   uv_queue_work(GetCurrentEventLoop(baton->isolate), _req, (uv_work_cb) work_cb, (uv_after_work_cb) after_work_cb);
#else
   uv_queue_work(uv_default_loop(), _req, (uv_work_cb) work_cb, (uv_after_work_cb) after_work_cb);
#endif

   return 0;
}

/* ASYNC THREAD */
async_rtn DBX_DBNAME::dbx_process_task(uv_work_t *req)
{
   DBX_DBNAME::dbx_baton_t *baton = static_cast<DBX_DBNAME::dbx_baton_t *>(req->data);

   dbx_launch_thread(baton->pmeth);

   baton->c->dbx_count += 1;

   return;
}


/* PRIMARY THREAD : Standard callback wrapper */
async_rtn DBX_DBNAME::dbx_uv_close_callback(uv_work_t *req)
{
/*
   printf("\r\n*** %lu cache_uv_close_callback() (%d, req=%p) ...", (unsigned long) dbx_current_thread_id(), dbx_queue_size, req);
*/
   delete req;

   return;
}


async_rtn DBX_DBNAME::dbx_invoke_callback(uv_work_t *req)
{
   Isolate* isolate = Isolate::GetCurrent();
   HandleScope scope(isolate);

   dbx_baton_t *baton = static_cast<dbx_baton_t *>(req->data);

   if (baton->gx)
      ((mglobal *) baton->gx)->async_callback((mglobal *) baton->gx);
   else if (baton->cx)
      ((mcursor *) baton->cx)->async_callback((mcursor *) baton->cx);
   else
      baton->c->Unref();

   Local<Value> argv[2];

   if (baton->pmeth->pcon->error[0])
      argv[0] = DBX_INTEGER_NEW(true);
   else
      argv[0] = DBX_INTEGER_NEW(false);

   if (baton->pmeth->binary) {
      baton->result_obj = node::Buffer::New(isolate, (char *) baton->pmeth->output_val.svalue.buf_addr, (size_t) baton->pmeth->output_val.svalue.len_used).ToLocalChecked();
      argv[1] = baton->result_obj;
   }
   else {
      baton->result_str = dbx_new_string8n(isolate, baton->pmeth->output_val.svalue.buf_addr, baton->pmeth->output_val.svalue.len_used, baton->c->pcon->utf8);
      argv[1] = baton->result_str;
   }

#if DBX_NODE_VERSION < 80000
   TryCatch try_catch;
#endif

   Local<Function> cb = Local<Function>::New(isolate, baton->cb);

#if DBX_NODE_VERSION >= 120000
   /* cb->Call(isolate->GetCurrentContext(), isolate->GetCurrentContext()->Global(), 2, argv); */
   cb->Call(isolate->GetCurrentContext(), Null(isolate), 2, argv).ToLocalChecked();
#else
   cb->Call(isolate->GetCurrentContext()->Global(), 2, argv);
#endif

#if DBX_NODE_VERSION < 80000
   if (try_catch.HasCaught()) {
      node::FatalException(isolate, try_catch);
   }
#endif

   baton->cb.Reset();

	DBX_DBFUN_END(baton->c);

   dbx_destroy_baton(baton, baton->pmeth);
   dbx_request_memory_free(baton->pmeth->pcon, baton->pmeth, 0);

   delete req;
   return;
}


async_rtn DBX_DBNAME::dbx_invoke_callback_sql_execute(uv_work_t *req)
{
   int cn;
   Isolate* isolate = Isolate::GetCurrent();
   HandleScope scope(isolate);
#if DBX_NODE_VERSION >= 100000
   Local<Context> icontext = isolate->GetCurrentContext();
#endif
   Local<String> key;
   Local<Object> obj1;

   dbx_baton_t *baton = static_cast<dbx_baton_t *>(req->data);

   if (baton->gx)
      ((mglobal *) baton->gx)->async_callback((mglobal *) baton->gx);
   else if (baton->cx)
      ((mcursor *) baton->cx)->async_callback((mcursor *) baton->cx);
   else
      baton->c->Unref();

   Local<Value> argv[2];

   if (baton->pmeth->pcon->error[0])
      argv[0] = DBX_INTEGER_NEW(true);
   else
      argv[0] = DBX_INTEGER_NEW(false);

   baton->result_obj = DBX_OBJECT_NEW();

   key = dbx_new_string8(isolate, (char *) "sqlcode", 0);
   DBX_SET(baton->result_obj, key, DBX_INTEGER_NEW(baton->pmeth->psql->sqlcode));
   key = dbx_new_string8(isolate, (char *) "sqlstate", 0);
   DBX_SET(baton->result_obj, key, dbx_new_string8(isolate, baton->pmeth->psql->sqlstate, 0));
   if (baton->pmeth->pcon->error[0]) {
      key = dbx_new_string8(isolate, (char *) "error", 0);
      DBX_SET(baton->result_obj, key, dbx_new_string8(isolate, baton->pmeth->pcon->error, 0));
   }
   else if (baton->pmeth->psql->no_cols > 0) {

      Local<Array> a = DBX_ARRAY_NEW(baton->pmeth->psql->no_cols);
      key = dbx_new_string8(isolate, (char *) "columns", 0);
      DBX_SET(baton->result_obj, key, a);

      for (cn = 0; cn < baton->pmeth->psql->no_cols; cn ++) {
         obj1 = DBX_OBJECT_NEW();
         DBX_SET(a, cn, obj1);
         key = dbx_new_string8(isolate, (char *) "name", 0);
         DBX_SET(obj1, key, dbx_new_string8(isolate, baton->pmeth->psql->cols[cn]->name.buf_addr, 0));
         if (baton->pmeth->psql->cols[cn]->stype) {
            key = dbx_new_string8(isolate, (char *) "type", 0);
            DBX_SET(obj1, key, dbx_new_string8(isolate, baton->pmeth->psql->cols[cn]->stype, 0));
         }
      }
   }

   argv[1] =  baton->result_obj;

#if DBX_NODE_VERSION < 80000
   TryCatch try_catch;
#endif

   Local<Function> cb = Local<Function>::New(isolate, baton->cb);

#if DBX_NODE_VERSION >= 120000
   /* cb->Call(isolate->GetCurrentContext(), isolate->GetCurrentContext()->Global(), 2, argv); */
   cb->Call(isolate->GetCurrentContext(), Null(isolate), 2, argv).ToLocalChecked();
#else
   cb->Call(isolate->GetCurrentContext()->Global(), 2, argv);
#endif

#if DBX_NODE_VERSION < 80000
   if (try_catch.HasCaught()) {
      node::FatalException(isolate, try_catch);
   }
#endif

   baton->cb.Reset();

	DBX_DBFUN_END(baton->c);

   dbx_destroy_baton(baton, baton->pmeth);
   dbx_request_memory_free(baton->pmeth->pcon, baton->pmeth, 0);

   delete req;
   return;
}


void DBX_DBNAME::Version(const FunctionCallbackInfo<Value>& args)
{
   int js_narg;
   DBXCON *pcon;
   DBXMETH *pmeth;
   DBX_DBNAME *c = ObjectWrap::Unwrap<DBX_DBNAME>(args.This());
   DBX_GET_ISOLATE;
   c->dbx_count ++;

   pcon = c->pcon;
   if (pcon->log_functions) {
      LogFunction(c, args, NULL, (char *) DBX_DBNAME_STR "::version");
   }
   pmeth = dbx_request_memory(pcon, 0);

   js_narg = args.Length();

   if (js_narg >= DBX_MAXARGS) {
      isolate->ThrowException(Exception::Error(dbx_new_string8(isolate, (char *) "Too many arguments on Version", 1)));
      dbx_request_memory_free(pcon, pmeth, 0);
      return;
   }

   dbx_version(pmeth);

   Local<String> result;
   result = dbx_new_string8(isolate, (char *) pmeth->output_val.svalue.buf_addr, pcon->utf8);

   dbx_request_memory_free(pcon, pmeth, 0);
   args.GetReturnValue().Set(result);

   return;
}


void DBX_DBNAME::SetLogLevel(const FunctionCallbackInfo<Value>& args)
{
   int js_narg;
   char buffer[256];
   DBXCON *pcon;
   Local<String> str;
   Local<String> result;
   DBX_DBNAME *c = ObjectWrap::Unwrap<DBX_DBNAME>(args.This());
   DBX_GET_ICONTEXT;
   c->dbx_count ++;

   pcon = c->pcon;

   pcon->log_errors = 0;
   pcon->log_functions = 0;
   pcon->log_transmissions = 0;
   pcon->log_filter[0] = '\0';

   js_narg = args.Length();

   if (js_narg > 0) {
      str = DBX_TO_STRING(args[0]);
      DBX_WRITE_UTF8(str, buffer);
      if (buffer[0]) {
         strcpy(pcon->log_file, buffer);
      }
   }
   if (js_narg > 1) {
      str = DBX_TO_STRING(args[1]);
      DBX_WRITE_UTF8(str, buffer);
      dbx_lcase(buffer);
      if (strstr(buffer, "e")) {
         pcon->log_errors = 1;
      }
      if (strstr(buffer, "f")) {
         pcon->log_functions = 1;
      }
      if (strstr(buffer, "t")) {
         if (!pcon->log_transmissions) {
            pcon->log_transmissions = 1;
         }
      }
      if (strstr(buffer, "r")) { /* v1.0.3 */
         pcon->log_transmissions = 2;
      }
   }
   if (js_narg > 2) {
      str = DBX_TO_STRING(args[2]);
      DBX_WRITE_UTF8(str, buffer);
      if (buffer[0]) {
         strcpy(pcon->log_filter, ",");
         strcpy(pcon->log_filter + 1, buffer);
         strcat(pcon->log_filter, ",");
      }
   }
   result = dbx_new_string8(isolate, (char *) pcon->log_file, pcon->utf8);
   args.GetReturnValue().Set(result);

   return;
}


void DBX_DBNAME::LogMessage(const FunctionCallbackInfo<Value>& args)
{
  int js_narg, str_len;
   char *title, *message;
   DBXCON *pcon;
   Local<String> str;
   Local<String> result;
   DBX_DBNAME *c = ObjectWrap::Unwrap<DBX_DBNAME>(args.This());
   DBX_GET_ICONTEXT;
   c->dbx_count ++;

   pcon = c->pcon;
   message = NULL;
   title = NULL;

   js_narg = args.Length();

   if (js_narg > 0) {
      str = DBX_TO_STRING(args[0]);
      str_len = dbx_string8_length(isolate, str, 0);

      message = (char *) dbx_malloc(str_len + 32, 0);
      if (message) {
         DBX_WRITE_UTF8(str, message);
      }
   }

   if (js_narg > 1) {
      str = DBX_TO_STRING(args[1]);
      str_len = dbx_string8_length(isolate, str, 0);

      title = (char *) dbx_malloc(str_len + 32, 0);
      if (title) {
         DBX_WRITE_UTF8(str, title);
      }
   }

   if (message && title) {
      dbx_log_event(pcon, message, title, 0);
   }

   result = dbx_new_string8(isolate, (char *) "", pcon->utf8);
   args.GetReturnValue().Set(result);
   return;
}



/* 1.4.11 */
void DBX_DBNAME::Charset(const FunctionCallbackInfo<Value>& args)
{
   int js_narg, str_len;
   char buffer[32];
   DBXCON *pcon;
   Local<String> str;
   Local<String> result;
   DBX_DBNAME *c = ObjectWrap::Unwrap<DBX_DBNAME>(args.This());
   DBX_GET_ICONTEXT;
   c->dbx_count ++;

   pcon = c->pcon;
   if (pcon->log_functions) {
      LogFunction(c, args, NULL, (char *) DBX_DBNAME_STR "::charset");
   }
   js_narg = args.Length();

   if (js_narg == 0) {
      if (pcon->utf8 == 0) {
         strcpy(buffer, "ascii");
      }
      else {
         strcpy(buffer, "utf-8");
      }
      result = dbx_new_string8(isolate, (char *) buffer, pcon->utf8);
      args.GetReturnValue().Set(result);
      return;
   }

   result = dbx_new_string8(isolate, (char *) buffer, pcon->utf8);
   args.GetReturnValue().Set(result);

   if (js_narg != 1) {
      isolate->ThrowException(Exception::Error(dbx_new_string8(isolate, (char *) "The Charset method takes one argument", 1)));
      return;
   }
   str = DBX_TO_STRING(args[0]);
   str_len = dbx_string8_length(isolate, str, 0);
   if (str_len > 30) {
      isolate->ThrowException(Exception::Error(dbx_new_string8(isolate, (char *) "Invalid 'character set' argument supplied to the SetCharset method", 1)));
      return;
   }

   DBX_WRITE_UTF8(str, buffer);
   dbx_lcase(buffer);

   if (strstr(buffer, "ansi") || strstr(buffer, "ascii") || strstr(buffer, "8859") || strstr(buffer, "1252")) {
      pcon->utf8 = 0;
      strcpy(buffer, "ascii");
   }
   else if (strstr(buffer, "utf8") || strstr(buffer, "utf-8")) {
      pcon->utf8 = 1;
      strcpy(buffer, "utf-8");
   }
   else {
      isolate->ThrowException(Exception::Error(dbx_new_string8(isolate, (char *) "Invalid 'character set' argument supplied to the SetCharset method", 1)));
      return;
   }

   result = dbx_new_string8(isolate, (char *) buffer, pcon->utf8);
   args.GetReturnValue().Set(result);

   return;
}


void DBX_DBNAME::Open(const FunctionCallbackInfo<Value>& args)
{
   short async;
   int rc, n, js_narg, error_code, key_len;
   char name[256], buffer[256];
   DBXCON *pcon;
   DBXMETH *pmeth;
#if !defined(_WIN32)
   struct sigaction action;
#endif
   DBX_DBNAME *c = ObjectWrap::Unwrap<DBX_DBNAME>(args.This());
   DBX_GET_ICONTEXT;
   c->dbx_count ++;

   pcon = c->pcon;
   if (pcon->log_functions) {
      LogFunction(c, args, NULL, (char *) DBX_DBNAME_STR "::open");
   }
   pmeth = dbx_request_memory(pcon, 0);

   DBX_CALLBACK_FUN(js_narg, cb, async);

   if (js_narg >= DBX_MAXARGS) {
      isolate->ThrowException(Exception::Error(dbx_new_string8(isolate, (char *) "Too many arguments on Open", 1)));
      dbx_request_memory_free(pcon, pmeth, 0);
      return;
   }

   if (c->open) {
      Local<String> result = dbx_new_string8(isolate, (char *) "", 0);
      args.GetReturnValue().Set(result);
   }

   Local<Object> obj, objn;
   Local<String> key;
   Local<String> value;

   obj = DBX_TO_OBJECT(args[0]);

#if DBX_NODE_VERSION >= 120000
   Local<Array> a = obj->GetPropertyNames(icontext).ToLocalChecked();
#else
   Local<Array> a = obj->GetPropertyNames();
#endif

   error_code = 0;
   for (n = 0; n < (int) a->Length(); n ++) {
      key = DBX_TO_STRING(DBX_GET(a, n));

      key_len = dbx_string8_length(isolate, key, 0);
      if (key_len > 60) {
         error_code = 1;
         break;
      }
      DBX_WRITE_UTF8(key, (char *) name);

      if (!strcmp(name, (char *) "type")) {
         value = DBX_TO_STRING(DBX_GET(obj ,key));
         DBX_WRITE_UTF8(value, pcon->type);
         dbx_lcase(pcon->type);

         if (!strcmp(pcon->type, "bdb"))
            pcon->dbtype = DBX_DBTYPE_BDB;
         else if (!strcmp(pcon->type, "lmdb"))
            pcon->dbtype = DBX_DBTYPE_LMDB;
      }
      else if (!strcmp(name, (char *) "db_library")) {
         value = DBX_TO_STRING(DBX_GET(obj, key));
         DBX_WRITE_UTF8(value, pcon->db_library);
      }
      else if (!strcmp(name, (char *) "db_file")) {
         value = DBX_TO_STRING(DBX_GET(obj, key));
         DBX_WRITE_UTF8(value, pcon->db_file);
      }
      else if (!strcmp(name, (char *) "env_dir")) {
         value = DBX_TO_STRING(DBX_GET(obj, key));
         DBX_WRITE_UTF8(value, pcon->env_dir);
      }
      else if (!strcmp(name, (char *) "key_type")) {
         value = DBX_TO_STRING(DBX_GET(obj ,key));
         DBX_WRITE_UTF8(value, buffer);
         dbx_lcase(buffer);

         if (!strcmp(buffer, "int"))
            pcon->key_type = DBX_KEYTYPE_INT;
         else if (!strcmp(buffer, "str"))
            pcon->key_type = DBX_KEYTYPE_STR;
         else if (!strcmp(buffer, "m") || !strcmp(buffer, "mumps"))
            pcon->key_type = DBX_KEYTYPE_M;
      }
      else if (!strcmp(name, (char *) "env_vars")) {
         char *p, *p1, *p2;
         value = DBX_TO_STRING(DBX_GET(obj, key));
         DBX_WRITE_UTF8(value, (char *) pmeth->key.ibuffer);

         p = (char *) pmeth->key.ibuffer;
         p2 = p;
         while ((p2 = strstr(p, "\n"))) {
            *p2 = '\0';
            p1 = strstr(p, "=");
            if (p1) {
               *p1 = '\0';
               p1 ++;
#if defined(_WIN32)
               SetEnvironmentVariable((LPCTSTR) p, (LPCTSTR) p1);
#else
               /* printf("\nLinux : environment variable p=%s p1=%s;", p, p1); */
               setenv(p, p1, 1);
#endif
            }
            else {
               break;
            }
            p = p2 + 1;
         }
      }
      else if (!strcmp(name, (char *) "multithreaded")) {
        if (DBX_GET(obj, key)->IsBoolean()) {
            if (DBX_TO_BOOLEAN(DBX_GET(obj, key))->IsFalse()) {
               c->use_mutex = 0;
               c->pcon->use_mutex = 0;
            }
         }
      }
      else if (!strcmp(name, (char *) "debug")) {
         ; /* TODO */
      }
      else {
         error_code = 2;
         break;
      }
   }

   if (error_code == 1) {
      isolate->ThrowException(Exception::Error(dbx_new_string8(isolate, (char *) "Oversize parameter supplied to the Open method", 1)));
      dbx_request_memory_free(pcon, pmeth, 0);
      return;
   }
   else if (error_code == 2) {
      strcat(name, " - Invalid parameter name in the Open method");
      isolate->ThrowException(Exception::Error(dbx_new_string8(isolate, (char *) name, 1)));
      dbx_request_memory_free(pcon, pmeth, 0);
      return;
   }

#if defined(_WIN32)

   if (c->handle_sigint) {
      signal(SIGINT, dbxGoingDownSignal);
      /* printf("\r\nSet Signal Handler for SIGINT"); */
   }
   if (c->handle_sigterm) {
      signal(SIGTERM, dbxGoingDownSignal);
      /* printf("\r\nSet Signal Handler for SIGTERM"); */
   }

#else

   action.sa_flags = 0;
   sigemptyset(&(action.sa_mask));
   if (c->handle_sigint) {
      action.sa_handler = dbxGoingDownSignal;
      sigaction(SIGTERM, &action, NULL);
      /* printf("\r\nSet Signal Handler for SIGINT"); */
   }
   if (c->handle_sigterm) {
      action.sa_handler = dbxGoingDownSignal;
      sigaction(SIGINT, &action, NULL);
      /* printf("\r\nSet Signal Handler for SIGTERM"); */
   }

#endif

   if (pcon->log_transmissions) {
      dbx_log_transmission(pcon, pmeth, (char *) DBX_DBNAME_STR "::open");
   }

   if (async) {
      dbx_baton_t *baton = dbx_make_baton(c, pmeth);
      baton->isolate = isolate;
      baton->pmeth->p_dbxfun = (int (*) (struct tagDBXMETH * pmeth)) dbx_do_nothing;

      rc = dbx_open(pmeth);

      if (rc == CACHE_SUCCESS) {
         c->open = 1;
      }

      Local<Function> cb = Local<Function>::Cast(args[js_narg]);

      baton->cb.Reset(isolate, cb);

      c->Ref();

      if (dbx_queue_task((void *) dbx_process_task, (void *) dbx_invoke_callback, baton, 0)) {
         char error[DBX_ERROR_SIZE];
         T_STRCPY(error, _dbxso(error), pcon->error);
         dbx_destroy_baton(baton, pmeth);
         isolate->ThrowException(Exception::Error(dbx_new_string8(isolate, error, 1)));
         dbx_request_memory_free(pcon, pmeth, 0);
         return;
      }
      return;
   }

   rc = dbx_open(pmeth);

   if (rc == CACHE_SUCCESS) {
      c->open = 1; 
   }

   if (pcon->log_transmissions == 2) {
      dbx_log_response(pcon, (char *) pcon->error, (int) strlen(pcon->error), (char *) DBX_DBNAME_STR "::open");
   }

   Local<String> result = dbx_new_string8(isolate, pcon->error, 0);
   dbx_request_memory_free(pcon, pmeth, 0);

   args.GetReturnValue().Set(result);
 }


void DBX_DBNAME::Close(const FunctionCallbackInfo<Value>& args)
{
   short async;
   int js_narg;
   DBXCON *pcon;
   DBXMETH *pmeth;
   DBX_DBNAME *c = ObjectWrap::Unwrap<DBX_DBNAME>(args.This());
   DBX_GET_ISOLATE;
   c->dbx_count ++;

   pcon = c->pcon;
   if (pcon->log_functions) {
      LogFunction(c, args, NULL, (char *) DBX_DBNAME_STR "::close");
   }
   pmeth = dbx_request_memory(pcon, 0);

   DBX_DBFUN_START(c, pcon, pmeth);

   c->open = 0;

   DBX_CALLBACK_FUN(js_narg, cb, async);

   pcon->error[0] = '\0';
   if (pcon && pcon->error[0]) {
      goto Close_Exit;
   }

   if (async) {
      dbx_baton_t *baton = dbx_make_baton(c, pmeth);
      baton->isolate = isolate;
      baton->pmeth->p_dbxfun = (int (*) (struct tagDBXMETH * pmeth)) dbx_do_nothing;

      dbx_close(baton->pmeth);

      Local<Function> cb = Local<Function>::Cast(args[js_narg]);

      baton->cb.Reset(isolate, cb);

      c->Ref();

      if (dbx_queue_task((void *) dbx_process_task, (void *) dbx_invoke_callback, baton, 0)) {
         char error[DBX_ERROR_SIZE];
         T_STRCPY(error, _dbxso(error), pcon->error);
         dbx_destroy_baton(baton, pmeth);
         isolate->ThrowException(Exception::Error(dbx_new_string8(isolate, error, 1)));
         dbx_request_memory_free(pcon, pmeth, 0);
         return;
      }

      return;
   }

   dbx_close(pmeth);

Close_Exit:

   Local<String> result = dbx_new_string8(isolate, pcon->error, 0);

   dbx_request_memory_free(pcon, pmeth, 0);

   args.GetReturnValue().Set(result);
}


int DBX_DBNAME::LogFunction(DBX_DBNAME *c, const FunctionCallbackInfo<Value>& args, void *pctx, char *name)
{
   int n, argc, max, otype;
   int len[DBX_MAXARGS];
   char * buffer;
   char namex[64];
   Local<Object> obj;
   Local<String> f;
   Local<String> str[DBX_MAXARGS];
   DBX_GET_ICONTEXT;
   argc = args.Length();

   if (c->pcon->log_filter[0]) {
      strcpy(namex, ",");
      strcat(namex, name);
      strcat(namex, ",");
      if (!strstr(c->pcon->log_filter, namex)) {
         return 0;
      }
   }

   max = 0;
   f = dbx_new_string8(isolate, (char *) "[callback]", 1);

   for (n = 0; n < argc; n ++) {
      obj = dbx_is_object(args[n], &otype);
      if (args[n]->IsFunction()) {
         str[n] = f;
      }
      else if (otype == 1) {
         str[n] = StringifyJSON(c, obj);
      }
      else {
         str[n] = DBX_TO_STRING(args[n]);
      }
      len[n] = dbx_string8_length(isolate, str[n], 1);
      max += len[n];
   }

   buffer = (char *) dbx_malloc(max + 128, 0);
   if (!buffer)
      return -1;

   strcpy(buffer, name);
   strcat(buffer, "(");
   max = (int) strlen(buffer);
   for (n = 0; n < argc; n ++) {
      if (n) {
         strcpy(buffer + max, ", ");
         max += 2;
      }
      dbx_write_char8(isolate, str[n], buffer + max, 1);
      max += len[n];
   }
   strcpy(buffer + max, ")");
   max += 1;

   dbx_log_event(c->pcon, buffer, (char *) "mg-dbx-bdb: Function", 0);

   dbx_free((void *) buffer, 0);

   return 0;
}


Local<String> DBX_DBNAME::StringifyJSON(DBX_DBNAME *c, Local<Object> json)
{
   Local<String> json_string;
   Isolate* isolate = Isolate::GetCurrent();
#if DBX_NODE_VERSION >= 100000
   Local<Context> icontext = isolate->GetCurrentContext();
#endif
   EscapableHandleScope handle_scope(isolate);

   Local<Object> global = isolate->GetCurrentContext()->Global();
   Local<Object> JSON = Local<Object>::Cast(DBX_GET(global, dbx_new_string8(isolate, (char *) "JSON", 1)));
   Local<Function> stringify = Local<Function>::Cast(DBX_GET(JSON, dbx_new_string8(isolate, (char *) "stringify", 1)));
   Local<Value> args[] = { json };
#if DBX_NODE_VERSION >= 120000
   json_string = Local<String>::Cast(stringify->Call(icontext, JSON, 1, args).ToLocalChecked());
#else
   json_string = Local<String>::Cast(stringify->Call(JSON, 1, args));
#endif
   return handle_scope.Escape(json_string);
}


int DBX_DBNAME::GlobalReference(DBX_DBNAME *c, const FunctionCallbackInfo<Value>& args, DBXMETH *pmeth, DBXGREF *pgref, short context)
{
   int n, nx, rc, otype, len;
   char *p;
   char buffer[64];
   DBXVAL *pval;
   Local<Object> obj;
   Local<String> str;
   DBX_GET_ICONTEXT;
   DBXCON *pcon = pmeth->pcon;

   pmeth->key.ibuffer_used = 0;
   pmeth->key.argc = 0;
   rc = 0;

   if (!context) {
      DBX_DB_LOCK(rc, 0);
   }

   pmeth->output_val.svalue.len_used = 0;
   nx = 0;
   n = 0;

   if (pcon->key_type == DBX_KEYTYPE_M) {
      pmeth->key.args[nx].cvalue.pstr = 0;
      if (pgref) {
         dbx_ibuffer_add(pmeth, &(pmeth->key), isolate, nx, str, pgref->global, (int) strlen(pgref->global), 0);
      }
      else {
         str = DBX_TO_STRING(args[n]);
         dbx_ibuffer_add(pmeth, &(pmeth->key), isolate, nx, str, NULL, 0, 0);
         n ++;
      }

      nx ++;

      if (pgref && (pval = pgref->pkey)) {
         while (pval) {
            pmeth->key.args[nx].cvalue.pstr = 0;
            if (pval->type == DBX_DTYPE_INT) {
               pmeth->key.args[nx].type = DBX_DTYPE_INT;
               pmeth->key.args[nx].num.int32 = (int) pval->num.int32;
               T_SPRINTF(buffer, _dbxso(buffer), "%d", pval->num.int32);
               dbx_ibuffer_add(pmeth, &(pmeth->key), isolate, nx, str, buffer, (int) strlen(buffer), 0);
            }
            else {
               pmeth->key.args[nx].type = DBX_DTYPE_STR;
               dbx_ibuffer_add(pmeth, &(pmeth->key), isolate, nx, str, pval->svalue.buf_addr, (int) pval->svalue.len_used, 0);
            }
            nx ++;
            pval = pval->pnext;
         }
      }
   }

   for (; n < pmeth->jsargc; n ++, nx ++) {

      pmeth->key.args[nx].cvalue.pstr = 0;

      if (args[n]->IsInt32()) {
         pmeth->key.args[nx].type = DBX_DTYPE_INT;
         pmeth->key.args[nx].num.int32 = (int) DBX_INT32_VALUE(args[n]);
         T_SPRINTF(buffer, _dbxso(buffer), "%d", pmeth->key.args[nx].num.int32);
         dbx_ibuffer_add(pmeth, &(pmeth->key), isolate, nx, str, buffer, (int) strlen(buffer), 0);
      }
      else {
         pmeth->key.args[nx].type = DBX_DTYPE_STR;
         obj = dbx_is_object(args[n], &otype);

         if (otype == 2) {
            p = node::Buffer::Data(obj);
            len = (int) node::Buffer::Length(obj);
            dbx_ibuffer_add(pmeth, &(pmeth->key), isolate, nx, str, p, (int) len, 0);
         }
         else {
            str = DBX_TO_STRING(args[n]);
            dbx_ibuffer_add(pmeth, &(pmeth->key), isolate, nx, str, NULL, 0, 0);
         }
      }

      if (pmeth->increment && n == (pmeth->jsargc - 1)) {
         if (pmeth->key.args[nx].svalue.len_used < 32) {
            T_STRNCPY(buffer, _dbxso(buffer), pmeth->key.args[nx].svalue.buf_addr, pmeth->key.args[nx].svalue.len_used);
         }
         else {
            buffer[0] = '1';
            buffer[1] = '\0';
         }
         pmeth->key.args[nx].type = DBX_DTYPE_DOUBLE;
         pmeth->key.args[nx].num.real = (double) strtod(buffer, NULL);
      }
   }

   pmeth->key.argc = nx;

   return rc;
}


void DBX_DBNAME::Get(const FunctionCallbackInfo<Value>& args)
{
   return GetEx(args, 0);
}


void DBX_DBNAME::Get_bx(const FunctionCallbackInfo<Value>& args)
{
   return GetEx(args, 1);
}


void DBX_DBNAME::GetEx(const FunctionCallbackInfo<Value>& args, int binary)
{
   short async;
   int rc;
   DBXCON *pcon;
   DBXMETH *pmeth;
   Local<String> result;
   DBX_DBNAME *c = ObjectWrap::Unwrap<DBX_DBNAME>(args.This());
   DBX_GET_ISOLATE;
   c->dbx_count ++;

   pcon = c->pcon;
   if (pcon->log_functions) {
      LogFunction(c, args, NULL, (char *) DBX_DBNAME_STR "::get");
   }
   pmeth = dbx_request_memory(pcon, 0);

   pmeth->binary = binary;

   DBX_CALLBACK_FUN(pmeth->jsargc, cb, async);

   if (pmeth->jsargc >= DBX_MAXARGS) {
      isolate->ThrowException(Exception::Error(dbx_new_string8(isolate, (char *) "Too many arguments on Get", 1)));
      dbx_request_memory_free(pcon, pmeth, 0);
      return;
   }
   if (pmeth->jsargc == 0) {
      isolate->ThrowException(Exception::Error(dbx_new_string8(isolate, (char *) "Missing or invalid global name on Get", 1)));
      dbx_request_memory_free(pcon, pmeth, 0);
      return;
   }
   
   DBX_DBFUN_START(c, pcon, pmeth);

   rc = GlobalReference(c, args, pmeth, NULL, async);

   if (pcon->log_transmissions) {
      dbx_log_transmission(pcon, pmeth, (char *) DBX_DBNAME_STR "::get");
   }

   if (async) {
      dbx_baton_t *baton = dbx_make_baton(c, pmeth);
      baton->isolate = isolate;
      baton->pmeth->p_dbxfun = (int (*) (struct tagDBXMETH * pmeth)) dbx_get;
      Local<Function> cb = Local<Function>::Cast(args[pmeth->jsargc]);

      baton->cb.Reset(isolate, cb);

      c->Ref();

      if (dbx_queue_task((void *) dbx_process_task, (void *) dbx_invoke_callback, baton, 0)) {
         char error[DBX_ERROR_SIZE];

         T_STRCPY(error, _dbxso(error), pcon->error);
         dbx_destroy_baton(baton, pmeth);
         isolate->ThrowException(Exception::Error(dbx_new_string8(isolate, error, 1)));
         dbx_request_memory_free(pcon, pmeth, 0);
         return;
      }
      return;
   }

   rc = dbx_get(pmeth);

   if (rc == CACHE_ERUNDEF) {
      dbx_create_string(&(pmeth->output_val.svalue), (void *) "", DBX_DTYPE_STR8);
   }
   else if (rc != CACHE_SUCCESS) {
      dbx_error_message(pmeth, rc, (char *) "dbxbdb::GetEx");
   }

   DBX_DBFUN_END(c);
   DBX_DB_UNLOCK(rc);

   if (pcon->log_transmissions == 2) {
      dbx_log_response(pcon, (char *) pmeth->output_val.svalue.buf_addr, (int) pmeth->output_val.svalue.len_used, (char *) DBX_DBNAME_STR "::get");
   }

   if (binary) {
      Local<Object> bx = node::Buffer::New(isolate, (char *) pmeth->output_val.svalue.buf_addr, (size_t) pmeth->output_val.svalue.len_used).ToLocalChecked();
      args.GetReturnValue().Set(bx);
   }
   else {
      result = dbx_new_string8n(isolate, pmeth->output_val.svalue.buf_addr, pmeth->output_val.svalue.len_used, pcon->utf8);
      args.GetReturnValue().Set(result);
   }

   dbx_request_memory_free(pcon, pmeth, 0);
   return;
}


void DBX_DBNAME::Set(const FunctionCallbackInfo<Value>& args)
{
   short async;
   int rc;
   DBXCON *pcon;
   DBXMETH *pmeth;
   Local<String> result;
   DBX_DBNAME *c = ObjectWrap::Unwrap<DBX_DBNAME>(args.This());
   DBX_GET_ISOLATE;
   c->dbx_count ++;

   pcon = c->pcon;
   if (pcon->log_functions) {
      LogFunction(c, args, NULL, (char *) DBX_DBNAME_STR "::set");
   }
   pmeth = dbx_request_memory(pcon, 0);

   DBX_CALLBACK_FUN(pmeth->jsargc, cb, async);

   if (pmeth->jsargc >= DBX_MAXARGS) {
      isolate->ThrowException(Exception::Error(dbx_new_string8(isolate, (char *) "Too many arguments on Set", 1)));
      dbx_request_memory_free(pcon, pmeth, 0);
      return;
   }
   if (pmeth->jsargc == 0) {
      isolate->ThrowException(Exception::Error(dbx_new_string8(isolate, (char *) "Missing or invalid global name on Set", 1)));
      dbx_request_memory_free(pcon, pmeth, 0);
      return;
   }

   DBX_DBFUN_START(c, pcon, pmeth);

   rc = GlobalReference(c, args, pmeth, NULL, async);

   if (pcon->log_transmissions) {
      dbx_log_transmission(pcon, pmeth, (char *) DBX_DBNAME_STR "::set");
   }

   if (async) {
      dbx_baton_t *baton = dbx_make_baton(c, pmeth);
      baton->isolate = isolate;
      baton->pmeth->p_dbxfun = (int (*) (struct tagDBXMETH * pmeth)) dbx_set;
      Local<Function> cb = Local<Function>::Cast(args[pmeth->jsargc]);
      baton->cb.Reset(isolate, cb);
      c->Ref();
      if (dbx_queue_task((void *) dbx_process_task, (void *) dbx_invoke_callback, baton, 0)) {
         char error[DBX_ERROR_SIZE];
         T_STRCPY(error, _dbxso(error), pcon->error);
         dbx_destroy_baton(baton, pmeth);
         isolate->ThrowException(Exception::Error(dbx_new_string8(isolate, error, 1)));
         dbx_request_memory_free(pcon, pmeth, 0);
         return;
      }
      return;
   }

   rc = dbx_set(pmeth);

   if (rc == CACHE_SUCCESS) {
      dbx_create_string(&(pmeth->output_val.svalue), (void *) &rc, DBX_DTYPE_INT);
   }
   else {
      dbx_error_message(pmeth, rc, (char *) "dbxbdb::Set");
   }

   DBX_DBFUN_END(c);
   DBX_DB_UNLOCK(rc);

   result = dbx_new_string8n(isolate, pmeth->output_val.svalue.buf_addr, pmeth->output_val.svalue.len_used, pcon->utf8);
   args.GetReturnValue().Set(result);

   if (pcon->log_transmissions == 2) {
      dbx_log_response(pcon, (char *) pmeth->output_val.svalue.buf_addr, (int) pmeth->output_val.svalue.len_used, (char *) DBX_DBNAME_STR "::set");
   }

   dbx_request_memory_free(pcon, pmeth, 0);
   return;
}


void DBX_DBNAME::Defined(const FunctionCallbackInfo<Value>& args)
{
   short async;
   int rc;
   DBXCON *pcon;
   DBXMETH *pmeth;
   Local<String> result;
   DBX_DBNAME *c = ObjectWrap::Unwrap<DBX_DBNAME>(args.This());
   DBX_GET_ISOLATE;
   c->dbx_count ++;

   pcon = c->pcon;
   if (pcon->log_functions) {
      LogFunction(c, args, NULL, (char *) DBX_DBNAME_STR "::defined");
   }
   pmeth = dbx_request_memory(pcon, 0);

   DBX_CALLBACK_FUN(pmeth->jsargc, cb, async);

   if (pmeth->jsargc >= DBX_MAXARGS) {
      isolate->ThrowException(Exception::Error(dbx_new_string8(isolate, (char *) "Too many arguments on Defined", 1)));
      dbx_request_memory_free(pcon, pmeth, 0);
      return;
   }
   if (pmeth->jsargc == 0) {
      isolate->ThrowException(Exception::Error(dbx_new_string8(isolate, (char *) "Missing or invalid global name on Defined", 1)));
      dbx_request_memory_free(pcon, pmeth, 0);
      return;
   }
   
   DBX_DBFUN_START(c, pcon, pmeth);

   rc = GlobalReference(c, args, pmeth, NULL, async);

   if (pcon->log_transmissions) {
      dbx_log_transmission(pcon, pmeth, (char *) DBX_DBNAME_STR "::defined");
   }

   if (async) {
      dbx_baton_t *baton = dbx_make_baton(c, pmeth);
      baton->isolate = isolate;
      baton->pmeth->p_dbxfun = (int (*) (struct tagDBXMETH * pmeth)) dbx_defined;
      Local<Function> cb = Local<Function>::Cast(args[pmeth->jsargc]);
      baton->cb.Reset(isolate, cb);
      c->Ref();
      if (dbx_queue_task((void *) dbx_process_task, (void *) dbx_invoke_callback, baton, 0)) {
         char error[DBX_ERROR_SIZE];
         T_STRCPY(error, _dbxso(error), pcon->error);
         dbx_destroy_baton(baton, pmeth);
         isolate->ThrowException(Exception::Error(dbx_new_string8(isolate, error, 1)));
         dbx_request_memory_free(pcon, pmeth, 0);
         return;
      }
      return;
   }

   rc = dbx_defined(pmeth);

   if (rc != CACHE_SUCCESS) {
      dbx_error_message(pmeth, rc, (char *) "dbxbdb::Defined");
   }

   DBX_DBFUN_END(c);
   DBX_DB_UNLOCK(rc);

   if (pcon->log_transmissions == 2) {
      dbx_log_response(pcon, (char *) pmeth->output_val.svalue.buf_addr, (int) pmeth->output_val.svalue.len_used, (char *) DBX_DBNAME_STR "::defined");
   }

   result = dbx_new_string8n(isolate, pmeth->output_val.svalue.buf_addr, pmeth->output_val.svalue.len_used, pcon->utf8);
   args.GetReturnValue().Set(result);
   dbx_request_memory_free(pcon, pmeth, 0);
   return;
}


void DBX_DBNAME::Delete(const FunctionCallbackInfo<Value>& args)
{
   short async;
   int rc, n;
   DBXCON *pcon;
   DBXMETH *pmeth;
   Local<String> result;
   DBX_DBNAME *c = ObjectWrap::Unwrap<DBX_DBNAME>(args.This());
   DBX_GET_ISOLATE;
   c->dbx_count ++;

   pcon = c->pcon;
   if (pcon->log_functions) {
      LogFunction(c, args, NULL, (char *) DBX_DBNAME_STR "::delete");
   }
   pmeth = dbx_request_memory(pcon, 0);

   DBX_CALLBACK_FUN(pmeth->jsargc, cb, async);

   if (pmeth->jsargc >= DBX_MAXARGS) {
      isolate->ThrowException(Exception::Error(dbx_new_string8(isolate, (char *) "Too many arguments on Delete", 1)));
      dbx_request_memory_free(pcon, pmeth, 0);
      return;
   }
   if (pmeth->jsargc == 0) {
      isolate->ThrowException(Exception::Error(dbx_new_string8(isolate, (char *) "Missing or invalid global name on Delete", 1)));
      dbx_request_memory_free(pcon, pmeth, 0);
      return;
   }
   
   DBX_DBFUN_START(c, pcon, pmeth);

   rc = GlobalReference(c, args, pmeth, NULL, async);

   if (pcon->log_transmissions) {
      dbx_log_transmission(pcon, pmeth, (char *) DBX_DBNAME_STR "::delete");
   }

   if (async) {
      dbx_baton_t *baton = dbx_make_baton(c, pmeth);
      baton->isolate = isolate;
      baton->pmeth->p_dbxfun = (int (*) (struct tagDBXMETH * pmeth)) dbx_delete;
      Local<Function> cb = Local<Function>::Cast(args[pmeth->jsargc]);
      baton->cb.Reset(isolate, cb);
      c->Ref();
      if (dbx_queue_task((void *) dbx_process_task, (void *) dbx_invoke_callback, baton, 0)) {
         char error[DBX_ERROR_SIZE];
         T_STRCPY(error, _dbxso(error), pcon->error);
         dbx_destroy_baton(baton, pmeth);
         isolate->ThrowException(Exception::Error(dbx_new_string8(isolate, error, 1)));
         dbx_request_memory_free(pcon, pmeth, 0);
         return;
      }
      return;
   }

   rc = dbx_delete(pmeth);

   if (rc == CACHE_SUCCESS) {
      n = 0;
      dbx_create_string(&(pmeth->output_val.svalue), (void *) &n, DBX_DTYPE_INT);
   }
   else {
      dbx_error_message(pmeth, rc, (char *) "dbxbdb::Delete");
   }

   DBX_DBFUN_END(c);
   DBX_DB_UNLOCK(rc);

   if (pcon->log_transmissions == 2) {
      dbx_log_response(pcon, (char *) pmeth->output_val.svalue.buf_addr, (int) pmeth->output_val.svalue.len_used, (char *) DBX_DBNAME_STR "::delete");
   }

   result = dbx_new_string8n(isolate, pmeth->output_val.svalue.buf_addr, pmeth->output_val.svalue.len_used, pcon->utf8);
   args.GetReturnValue().Set(result);
   dbx_request_memory_free(pcon, pmeth, 0);
   return;
}


void DBX_DBNAME::Next(const FunctionCallbackInfo<Value>& args)
{
   short async;
   int rc;
   DBXCON *pcon;
   DBXMETH *pmeth;
   Local<String> result;
   DBX_DBNAME *c = ObjectWrap::Unwrap<DBX_DBNAME>(args.This());
   DBX_GET_ISOLATE;
   c->dbx_count ++;

   pcon = c->pcon;
   if (pcon->log_functions) {
      LogFunction(c, args, NULL, (char *) DBX_DBNAME_STR "::next");
   }
   pmeth = dbx_request_memory(pcon, 0);

   DBX_CALLBACK_FUN(pmeth->jsargc, cb, async);

   if (pmeth->jsargc >= DBX_MAXARGS) {
      isolate->ThrowException(Exception::Error(dbx_new_string8(isolate, (char *) "Too many arguments on Next", 1)));
      dbx_request_memory_free(pcon, pmeth, 0);
      return;
   }
   if (pmeth->jsargc == 0) {
      isolate->ThrowException(Exception::Error(dbx_new_string8(isolate, (char *) "Missing or invalid global name on Next", 1)));
      dbx_request_memory_free(pcon, pmeth, 0);
      return;
   }
   
   DBX_DBFUN_START(c, pcon, pmeth);

   rc = GlobalReference(c, args, pmeth, NULL, async);

   if (pcon->log_transmissions) {
      dbx_log_transmission(pcon, pmeth, (char *) DBX_DBNAME_STR "::next");
   }

   if (async) {
      dbx_baton_t *baton = dbx_make_baton(c, pmeth);
      baton->isolate = isolate;
      baton->pmeth->p_dbxfun = (int (*) (struct tagDBXMETH * pmeth)) dbx_next;
      Local<Function> cb = Local<Function>::Cast(args[pmeth->jsargc]);
      baton->cb.Reset(isolate, cb);
      c->Ref();
      if (dbx_queue_task((void *) dbx_process_task, (void *) dbx_invoke_callback, baton, 0)) {
         char error[DBX_ERROR_SIZE];
         T_STRCPY(error, _dbxso(error), pcon->error);
         dbx_destroy_baton(baton, pmeth);
         isolate->ThrowException(Exception::Error(dbx_new_string8(isolate, error, 1)));
         dbx_request_memory_free(pcon, pmeth, 0);
         return;
      }
      return;
   }

   rc = dbx_next(pmeth);

   if (rc != CACHE_SUCCESS) {
      dbx_error_message(pmeth, rc, (char *) "dbxbdb::Next");
   }

   DBX_DBFUN_END(c);
   DBX_DB_UNLOCK(rc);

   if (pcon->log_transmissions == 2) {
      dbx_log_response(pcon, (char *) pmeth->output_val.svalue.buf_addr, (int) pmeth->output_val.svalue.len_used, (char *) DBX_DBNAME_STR "::next");
   }

   result = dbx_new_string8n(isolate, pmeth->output_val.svalue.buf_addr, pmeth->output_val.svalue.len_used, pcon->utf8);
   args.GetReturnValue().Set(result);
   dbx_request_memory_free(pcon, pmeth, 0);
   return;
}


void DBX_DBNAME::Previous(const FunctionCallbackInfo<Value>& args)
{
   short async;
   int rc;
   DBXCON *pcon;
   DBXMETH *pmeth;
   Local<String> result;
   DBX_DBNAME *c = ObjectWrap::Unwrap<DBX_DBNAME>(args.This());
   DBX_GET_ISOLATE;
   c->dbx_count ++;

   pcon = c->pcon;
   if (pcon->log_functions) {
      LogFunction(c, args, NULL, (char *) DBX_DBNAME_STR "::previous");
   }
   pmeth = dbx_request_memory(pcon, 0);

   DBX_CALLBACK_FUN(pmeth->jsargc, cb, async);

   if (pmeth->jsargc >= DBX_MAXARGS) {
      isolate->ThrowException(Exception::Error(dbx_new_string8(isolate, (char *) "Too many arguments on Previous", 1)));
      dbx_request_memory_free(pcon, pmeth, 0);
      return;
   }
   if (pmeth->jsargc == 0) {
      isolate->ThrowException(Exception::Error(dbx_new_string8(isolate, (char *) "Missing or invalid global name on Previous", 1)));
      dbx_request_memory_free(pcon, pmeth, 0);
      return;
   }
   
   DBX_DBFUN_START(c, pcon, pmeth);

   rc = GlobalReference(c, args, pmeth, NULL, async);

   if (pcon->log_transmissions) {
      dbx_log_transmission(pcon, pmeth, (char *) DBX_DBNAME_STR "::previous");
   }

   if (async) {
      dbx_baton_t *baton = dbx_make_baton(c, pmeth);
      baton->isolate = isolate;
      baton->pmeth->p_dbxfun = (int (*) (struct tagDBXMETH * pmeth)) dbx_previous;
      Local<Function> cb = Local<Function>::Cast(args[pmeth->jsargc]);
      baton->cb.Reset(isolate, cb);
      c->Ref();
      if (dbx_queue_task((void *) dbx_process_task, (void *) dbx_invoke_callback, baton, 0)) {
         char error[DBX_ERROR_SIZE];
         T_STRCPY(error, _dbxso(error), pcon->error);
         dbx_destroy_baton(baton, pmeth);
         isolate->ThrowException(Exception::Error(dbx_new_string8(isolate, error, 1)));
         dbx_request_memory_free(pcon, pmeth, 0);
         return;
      }
      return;
   }

   rc = dbx_previous(pmeth);

   if (rc != CACHE_SUCCESS) {
      dbx_error_message(pmeth, rc, (char *) "dbxbdb::Previous");
   }

   DBX_DBFUN_END(c);
   DBX_DB_UNLOCK(rc);

   if (pcon->log_transmissions == 2) {
      dbx_log_response(pcon, (char *) pmeth->output_val.svalue.buf_addr, (int) pmeth->output_val.svalue.len_used, (char *) DBX_DBNAME_STR "::previous");
   }

   result = dbx_new_string8n(isolate, pmeth->output_val.svalue.buf_addr, pmeth->output_val.svalue.len_used, pcon->utf8);

   args.GetReturnValue().Set(result);
   dbx_request_memory_free(pcon, pmeth, 0);
   return;
}


void DBX_DBNAME::Increment(const FunctionCallbackInfo<Value>& args)
{
   short async;
   int rc;
   DBXCON *pcon;
   DBXMETH *pmeth;
   Local<String> result;
   DBX_DBNAME *c = ObjectWrap::Unwrap<DBX_DBNAME>(args.This());
   DBX_GET_ISOLATE;
   c->dbx_count ++;

   pcon = c->pcon;
   if (pcon->log_functions) {
      LogFunction(c, args, NULL, (char *) DBX_DBNAME_STR "::increment");
   }
   pmeth = dbx_request_memory(pcon, 0);

   DBX_CALLBACK_FUN(pmeth->jsargc, cb, async);

   if (pmeth->jsargc >= DBX_MAXARGS) {
      isolate->ThrowException(Exception::Error(dbx_new_string8(isolate, (char *) "Too many arguments on Increment", 1)));
      dbx_request_memory_free(pcon, pmeth, 0);
      return;
   }
   if (pmeth->jsargc < 2) {
      isolate->ThrowException(Exception::Error(dbx_new_string8(isolate, (char *) "Missing or invalid global name on Increment", 1)));
      dbx_request_memory_free(pcon, pmeth, 0);
      return;
   }

   DBX_DBFUN_START(c, pcon, pmeth);

   pmeth->increment = 1;
   rc = GlobalReference(c, args, pmeth, NULL, async);

   if (pcon->log_transmissions) {
      dbx_log_transmission(pcon, pmeth, (char *) DBX_DBNAME_STR "::increment");
   }

   if (async) {
      dbx_baton_t *baton = dbx_make_baton(c, pmeth);
      baton->isolate = isolate;
      baton->pmeth->p_dbxfun = (int (*) (struct tagDBXMETH * pmeth)) dbx_increment;
      Local<Function> cb = Local<Function>::Cast(args[pmeth->jsargc]);
      baton->cb.Reset(isolate, cb);
      c->Ref();
      if (dbx_queue_task((void *) dbx_process_task, (void *) dbx_invoke_callback, baton, 0)) {
         char error[DBX_ERROR_SIZE];
         T_STRCPY(error, _dbxso(error), pcon->error);
         dbx_destroy_baton(baton, pmeth);
         isolate->ThrowException(Exception::Error(dbx_new_string8(isolate, error, 1)));
         dbx_request_memory_free(pcon, pmeth, 0);
         return;
      }
      return;
   }

   rc = dbx_increment(pmeth);

   if (rc != CACHE_SUCCESS) {
      dbx_error_message(pmeth, rc, (char *) "dbxbdb::Increment");
   }

   DBX_DBFUN_END(c);
   DBX_DB_UNLOCK(rc);

   if (pcon->log_transmissions == 2) {
      dbx_log_response(pcon, (char *) pmeth->output_val.svalue.buf_addr, (int) pmeth->output_val.svalue.len_used, (char *) DBX_DBNAME_STR "::increment");
   }

   result = dbx_new_string8n(isolate, pmeth->output_val.svalue.buf_addr, pmeth->output_val.svalue.len_used, pcon->utf8);
   args.GetReturnValue().Set(result);
   dbx_request_memory_free(pcon, pmeth, 0);
   return;
}


void DBX_DBNAME::Lock(const FunctionCallbackInfo<Value>& args)
{
   short async;
   int rc;
   DBXCON *pcon;
   DBXMETH *pmeth;
   Local<String> result;
   DBX_DBNAME *c = ObjectWrap::Unwrap<DBX_DBNAME>(args.This());
   DBX_GET_ISOLATE;
   c->dbx_count ++;

   pcon = c->pcon;
   if (pcon->log_functions) {
      LogFunction(c, args, NULL, (char *) DBX_DBNAME_STR "::lock");
   }
   pmeth = dbx_request_memory(pcon, 0);

   DBX_CALLBACK_FUN(pmeth->jsargc, cb, async);

   if (pmeth->jsargc >= DBX_MAXARGS) {
      isolate->ThrowException(Exception::Error(dbx_new_string8(isolate, (char *) "Too many arguments on Lock", 1)));
      dbx_request_memory_free(pcon, pmeth, 0);
      return;
   }
   if (pmeth->jsargc < 2) {
      isolate->ThrowException(Exception::Error(dbx_new_string8(isolate, (char *) "Missing or invalid global name on Lock", 1)));
      dbx_request_memory_free(pcon, pmeth, 0);
      return;
   }

   DBX_DBFUN_START(c, pcon, pmeth);

   pmeth->lock = 1;
   rc = GlobalReference(c, args, pmeth, NULL, async);

   if (pcon->log_transmissions) {
      dbx_log_transmission(pcon, pmeth, (char *) DBX_DBNAME_STR "::lock");
   }

   if (async) {
      dbx_baton_t *baton = dbx_make_baton(c, pmeth);
      baton->isolate = isolate;
      baton->pmeth->p_dbxfun = (int (*) (struct tagDBXMETH * pmeth)) dbx_lock;
      Local<Function> cb = Local<Function>::Cast(args[pmeth->jsargc]);
      baton->cb.Reset(isolate, cb);
      c->Ref();
      if (dbx_queue_task((void *) dbx_process_task, (void *) dbx_invoke_callback, baton, 0)) {
         char error[DBX_ERROR_SIZE];
         T_STRCPY(error, _dbxso(error), pcon->error);
         dbx_destroy_baton(baton, pmeth);
         isolate->ThrowException(Exception::Error(dbx_new_string8(isolate, error, 1)));
         dbx_request_memory_free(pcon, pmeth, 0);
         return;
      }
      return;
   }

   rc = dbx_lock(pmeth);

   if (rc != CACHE_SUCCESS) {
      dbx_error_message(pmeth, rc, (char *) "dbxbdb::Lock");
   }

   DBX_DBFUN_END(c);
   DBX_DB_UNLOCK(rc);

   if (pcon->log_transmissions == 2) {
      dbx_log_response(pcon, (char *) pmeth->output_val.svalue.buf_addr, (int) pmeth->output_val.svalue.len_used, (char *) DBX_DBNAME_STR "::lock");
   }

   result = dbx_new_string8n(isolate, pmeth->output_val.svalue.buf_addr, pmeth->output_val.svalue.len_used, pcon->utf8);
   args.GetReturnValue().Set(result);
   dbx_request_memory_free(pcon, pmeth, 0);
   return;
}


void DBX_DBNAME::Unlock(const FunctionCallbackInfo<Value>& args)
{
   short async;
   int rc;
   DBXCON *pcon;
   DBXMETH *pmeth;
   Local<String> result;
   DBX_DBNAME *c = ObjectWrap::Unwrap<DBX_DBNAME>(args.This());
   DBX_GET_ISOLATE;
   c->dbx_count ++;

   pcon = c->pcon;
   if (pcon->log_functions) {
      LogFunction(c, args, NULL, (char *) DBX_DBNAME_STR "::unlock");
   }
   pmeth = dbx_request_memory(pcon, 0);

   DBX_CALLBACK_FUN(pmeth->jsargc, cb, async);

   if (pmeth->jsargc >= DBX_MAXARGS) {
      isolate->ThrowException(Exception::Error(dbx_new_string8(isolate, (char *) "Too many arguments on Unlock", 1)));
      dbx_request_memory_free(pcon, pmeth, 0);
      return;
   }
   if (pmeth->jsargc < 2) {
      isolate->ThrowException(Exception::Error(dbx_new_string8(isolate, (char *) "Missing or invalid global name on Unlock", 1)));
      dbx_request_memory_free(pcon, pmeth, 0);
      return;
   }

   DBX_DBFUN_START(c, pcon, pmeth);

   pmeth->lock = 2;
   rc = GlobalReference(c, args, pmeth, NULL, async);

   if (pcon->log_transmissions) {
      dbx_log_transmission(pcon, pmeth, (char *) DBX_DBNAME_STR "::unlock");
   }

   if (async) {
      dbx_baton_t *baton = dbx_make_baton(c, pmeth);
      baton->isolate = isolate;
      baton->pmeth->p_dbxfun = (int (*) (struct tagDBXMETH * pmeth)) dbx_unlock;
      Local<Function> cb = Local<Function>::Cast(args[pmeth->jsargc]);
      baton->cb.Reset(isolate, cb);
      c->Ref();
      if (dbx_queue_task((void *) dbx_process_task, (void *) dbx_invoke_callback, baton, 0)) {
         char error[DBX_ERROR_SIZE];
         T_STRCPY(error, _dbxso(error), pcon->error);
         dbx_destroy_baton(baton, pmeth);
         isolate->ThrowException(Exception::Error(dbx_new_string8(isolate, error, 1)));
         dbx_request_memory_free(pcon, pmeth, 0);
         return;
      }
      return;
   }

   rc = dbx_unlock(pmeth);

   if (rc != CACHE_SUCCESS) {
      dbx_error_message(pmeth, rc, (char *) "dbxbdb::Unlock");
   }

   DBX_DBFUN_END(c);
   DBX_DB_UNLOCK(rc);

   if (pcon->log_transmissions == 2) {
      dbx_log_response(pcon, (char *) pmeth->output_val.svalue.buf_addr, (int) pmeth->output_val.svalue.len_used, (char *) DBX_DBNAME_STR "::unlock");
   }

   result = dbx_new_string8n(isolate, pmeth->output_val.svalue.buf_addr, pmeth->output_val.svalue.len_used, pcon->utf8);
   args.GetReturnValue().Set(result);
   dbx_request_memory_free(pcon, pmeth, 0);
   return;
}


void DBX_DBNAME::MGlobal(const FunctionCallbackInfo<Value>& args)
{
   int rc;
   DBXCON *pcon;
   DBXMETH *pmeth;
   DBX_DBNAME *c = ObjectWrap::Unwrap<DBX_DBNAME>(args.This());
   DBX_GET_ISOLATE;
   c->dbx_count ++;

   pcon = c->pcon;
   if (pcon->log_functions) {
      LogFunction(c, args, NULL, (char *) DBX_DBNAME_STR "::mglobal");
   }
   pmeth = dbx_request_memory(pcon, 1);

   pmeth->jsargc = args.Length();

   /* 1.4.10 */

   if (pmeth->jsargc < 1) {
      isolate->ThrowException(Exception::Error(dbx_new_string8(isolate, (char *) "The mglobal method takes at least one argument (the global name)", 1)));
      dbx_request_memory_free(pcon, pmeth, 0);
      return;
   }

   mglobal *gx = mglobal::NewInstance(args);

   gx->c = c;
   gx->pkey = NULL;

   /* 1.4.10 */
   rc = dbx_global_reset(args, isolate, pcon, pmeth, (void *) gx, 0, 0);
   if (rc < 0) {
      isolate->ThrowException(Exception::Error(dbx_new_string8(isolate, (char *) "The mglobal method takes at least one argument (the global name)", 1)));
      dbx_request_memory_free(pcon, pmeth, 0);
      return;
   }

/*
{
   int n;
   char buffer[256];

   printf("\r\n mglobal START");
   n = 0;
   pval = gx->pkey;
   while (pval) {
      strcpy(buffer, "");
      if (pmeth->key.args[n].svalue.len_used > 0) {
         strncpy(buffer,   pval->svalue.buf_addr,   pval->svalue.len_used);
         buffer[ pval->svalue.len_used] = '\0';
      }
      printf("\r\n >>>>>>> n=%d; type=%d; int32=%d; str=%s", ++ n, pval->type, pval->num.int32, buffer);
      pval = pval->pnext;
   }
    printf("\r\n mglobal END");
}
*/
   dbx_request_memory_free(pcon, pmeth, 0);
   return;
}


void DBX_DBNAME::MGlobal_Close(const FunctionCallbackInfo<Value>& args)
{
   int js_narg;
   DBX_DBNAME *c = ObjectWrap::Unwrap<DBX_DBNAME>(args.This());
   DBX_GET_ISOLATE;
   c->dbx_count ++;

   js_narg = args.Length();


   if (js_narg != 1) {
      isolate->ThrowException(Exception::Error(dbx_new_string8(isolate, (char *) "The MGlobal_Close method takes one argument (the MGlobal reference)", 1)));
      return;
   }

#if DBX_NODE_VERSION >= 100000
   mglobal * cx = node::ObjectWrap::Unwrap<mglobal>(args[0]->ToObject(isolate->GetCurrentContext()).ToLocalChecked());
#else
   mglobal * cx = node::ObjectWrap::Unwrap<mglobal>(args[0]->ToObject());
#endif

   cx->delete_mglobal_template(cx);

   return;
}


void DBX_DBNAME::MGlobalQuery(const FunctionCallbackInfo<Value>& args)
{
   int rc;
   DBXCON *pcon;
   DBXMETH *pmeth;
   DBX_DBNAME *c = ObjectWrap::Unwrap<DBX_DBNAME>(args.This());
   DBX_GET_ISOLATE;
   c->dbx_count ++;

   pcon = c->pcon;
   if (pcon->log_functions) {
      LogFunction(c, args, NULL, (char *) DBX_DBNAME_STR "::mglobalquery");
   }
   pmeth = dbx_request_memory(pcon, 1);

   pmeth->jsargc = args.Length();

   if (pmeth->jsargc < 1) {
      isolate->ThrowException(Exception::Error(dbx_new_string8(isolate, (char *) "The mglobalquery method takes at least one argument (the global reference to start with)", 1)));
      dbx_request_memory_free(pcon, pmeth, 0);
      return;
   }

   mcursor *cx = mcursor::NewInstance(args);

   dbx_cursor_init((void *) cx);

   cx->c = c;

   /* 1.4.10 */
   rc = dbx_cursor_reset(args, isolate, pcon, pmeth, (void *) cx, 0, 0);
   if (rc < 0) {
      isolate->ThrowException(Exception::Error(dbx_new_string8(isolate, (char *) "The mglobalquery method takes at least one argument (the global reference to start with)", 1)));
      dbx_request_memory_free(pcon, pmeth, 0);
      return;
   }

   dbx_request_memory_free(pcon, pmeth, 0);
   return;
}


void DBX_DBNAME::MGlobalQuery_Close(const FunctionCallbackInfo<Value>& args)
{
   int js_narg;
   DBX_DBNAME *c = ObjectWrap::Unwrap<DBX_DBNAME>(args.This());
   DBX_GET_ISOLATE;
   c->dbx_count ++;

   js_narg = args.Length();

   if (js_narg != 1) {
      isolate->ThrowException(Exception::Error(dbx_new_string8(isolate, (char *) "The MGlobalQuery_Close method takes one argument (the MGlobalQuery reference)", 1)));
      return;
   }

#if DBX_NODE_VERSION >= 100000
   mcursor * cx = node::ObjectWrap::Unwrap<mcursor>(args[0]->ToObject(isolate->GetCurrentContext()).ToLocalChecked());
#else
   mcursor * cx = node::ObjectWrap::Unwrap<mcursor>(args[0]->ToObject());
#endif

   cx->delete_mcursor_template(cx);

   return;
}


void DBX_DBNAME::SQL(const FunctionCallbackInfo<Value>& args)
{
   int rc;
   DBXCON *pcon;
   DBXMETH *pmeth;
   DBX_DBNAME *c = ObjectWrap::Unwrap<DBX_DBNAME>(args.This());
   DBX_GET_ISOLATE;
   c->dbx_count ++;

   pcon = c->pcon;
   if (pcon->log_functions) {
      LogFunction(c, args, NULL, (char *) DBX_DBNAME_STR "::sql");
   }
   pmeth = dbx_request_memory(pcon, 1);

   pmeth->jsargc = args.Length();

   if (pmeth->jsargc < 1) {
      isolate->ThrowException(Exception::Error(dbx_new_string8(isolate, (char *) "The sql method takes at least one argument (the sql script)", 1)));
      dbx_request_memory_free(pcon, pmeth, 0);
      return;
   }

   mcursor *cx = mcursor::NewInstance(args);

   dbx_cursor_init((void *) cx);

   cx->c = c;

   /* 1.4.10 */
   rc = dbx_cursor_reset(args, isolate, pcon, pmeth, (void *) cx, 0, 0);
   if (rc < 0) {
      isolate->ThrowException(Exception::Error(dbx_new_string8(isolate, (char *) "The sql method takes at least one argument (the sql script)", 1)));
      dbx_request_memory_free(pcon, pmeth, 0);
      return;
   }

   dbx_request_memory_free(pcon, pmeth, 0);
   return;
}


void DBX_DBNAME::SQL_Close(const FunctionCallbackInfo<Value>& args)
{
   int js_narg;
   DBX_DBNAME *c = ObjectWrap::Unwrap<DBX_DBNAME>(args.This());
   DBX_GET_ISOLATE;
   c->dbx_count ++;

   js_narg = args.Length();

   if (js_narg != 1) {
      isolate->ThrowException(Exception::Error(dbx_new_string8(isolate, (char *) "The SQL_Close method takes one argument (the SQL reference)", 1)));
      return;
   }

#if DBX_NODE_VERSION >= 100000
   mcursor * cx = node::ObjectWrap::Unwrap<mcursor>(args[0]->ToObject(isolate->GetCurrentContext()).ToLocalChecked());
#else
   mcursor * cx = node::ObjectWrap::Unwrap<mcursor>(args[0]->ToObject());
#endif

   cx->delete_mcursor_template(cx);

   return;
}


void DBX_DBNAME::Sleep(const FunctionCallbackInfo<Value>& args)
{
   int timeout;
   Local<Integer> result;
   DBX_DBNAME *c = ObjectWrap::Unwrap<DBX_DBNAME>(args.This());
   DBX_GET_ICONTEXT;
   c->dbx_count ++;


   timeout = 0;
   if (args[0]->IsInt32()) {
      timeout = (int) DBX_INT32_VALUE(args[0]);
   }

   dbx_sleep((unsigned long) timeout);
 
   result = DBX_INTEGER_NEW(0);
   args.GetReturnValue().Set(result);
}


void DBX_DBNAME::Dump(const FunctionCallbackInfo<Value>& args)
{
   int rc, n, n1, num, rkey_size, rdata_size;
   unsigned char chr, chrp;
   char *rkey, *rdata;
   DBXCON *pcon;
   DBXMETH *pmeth;
   Local<String> result;
   DBX_DBNAME *c = ObjectWrap::Unwrap<DBX_DBNAME>(args.This());
   DBX_GET_ISOLATE;
   c->dbx_count ++;
   DBT bdb_key, bdb_data;
   DBC *bdb_pcursor;
   MDB_val lmdb_key, lmdb_data;
   MDB_cursor *lmdb_pcursor;
   char buffer[4096], val[256], hex[16];

   rkey_size = 0;
   rdata_size = 0;
   rkey = NULL;
   rdata = NULL;
   bdb_pcursor = NULL;
   lmdb_pcursor = NULL;
   pcon = c->pcon;
   if (pcon->log_functions) {
      LogFunction(c, args, NULL, (char *) DBX_DBNAME_STR "::dump");
   }
   pmeth = dbx_request_memory(pcon, 0);

   DBX_CALLBACK_FUN(pmeth->jsargc, cb, rc);

   if (pmeth->jsargc >= DBX_MAXARGS) {
      isolate->ThrowException(Exception::Error(dbx_new_string8(isolate, (char *) "Too many arguments on Next", 1)));
      dbx_request_memory_free(pcon, pmeth, 0);
      return;
   }
   
   DBX_DBFUN_START(c, pcon, pmeth);

   pcon = pmeth->pcon;

   DBX_DB_LOCK(rc, 0);

   rc = dbx_global_reference(pmeth);
   if (rc != CACHE_SUCCESS) {
      dbx_error_message(pmeth, rc, (char *) "dbxbdb::Dump");
      DBX_DB_UNLOCK(rc);
      return;
   }

   if (pcon->dbtype == DBX_DBTYPE_BDB) {
      memset(&bdb_key, 0, sizeof(DBT));
      memset(&bdb_data, 0, sizeof(DBT));
      bdb_key.flags = DB_DBT_USERMEM;
      bdb_data.flags = DB_DBT_USERMEM;

      rc = pcon->p_bdb_so->pdb->cursor(pcon->p_bdb_so->pdb, NULL, &bdb_pcursor, 0);

      if (pcon->key_type == DBX_KEYTYPE_INT) {
         pmeth->output_val.num.int32 = pmeth->key.args[0].num.int32;
         bdb_key.data = &(pmeth->output_val.num.int32);
         bdb_key.size = sizeof(pmeth->output_val.num.int32);
         bdb_key.ulen = sizeof(pmeth->output_val.num.int32);
      }
      else if (pcon->key_type == DBX_KEYTYPE_STR) {
         memcpy((void *) pmeth->output_val.svalue.buf_addr, (void *) pmeth->key.args[0].svalue.buf_addr, (size_t) pmeth->key.args[0].svalue.len_used);
         pmeth->output_val.svalue.len_used = pmeth->key.args[0].svalue.len_used;
         bdb_key.data = (void *) pmeth->output_val.svalue.buf_addr;
         bdb_key.size = (u_int32_t) pmeth->output_val.svalue.len_used;
         bdb_key.ulen = (u_int32_t) pmeth->output_val.svalue.len_alloc;
      }
      else {
         bdb_key.data = (void *) pmeth->output_val.svalue.buf_addr;
         bdb_key.size = (u_int32_t) pmeth->output_val.svalue.len_used;
         bdb_key.ulen = (u_int32_t) pmeth->output_val.svalue.len_alloc;
      }

      bdb_data.data = (void *) pmeth->output_key.svalue.buf_addr;
      bdb_data.ulen = (u_int32_t)  pmeth->output_key.svalue.len_alloc;

      rc = bdb_pcursor->get(bdb_pcursor, &bdb_key, &bdb_data, DB_FIRST);
      rkey = (char *) bdb_key.data;
      rkey_size = (int) bdb_key.size;
      rdata = (char *) bdb_data.data;
      rdata_size = (int) bdb_data.size;
   }
   else if (pcon->dbtype == DBX_DBTYPE_LMDB) {
      rc = lmdb_start_ro_transaction(pmeth, 0);
      rc = pcon->p_lmdb_so->p_mdb_cursor_open(pcon->p_lmdb_so->ptxnro, pcon->p_lmdb_so->db, &lmdb_pcursor);

      if (pcon->key_type == DBX_KEYTYPE_INT) {
         pmeth->output_val.num.int32 = pmeth->key.args[0].num.int32;
         lmdb_key.mv_data = &(pmeth->output_val.num.int32);
         lmdb_key.mv_size = sizeof(pmeth->output_val.num.int32);
      }
      else if (pcon->key_type == DBX_KEYTYPE_STR) {
         memcpy((void *) pmeth->output_val.svalue.buf_addr, (void *) pmeth->key.args[0].svalue.buf_addr, (size_t) pmeth->key.args[0].svalue.len_used);
         pmeth->output_val.svalue.len_used = pmeth->key.args[0].svalue.len_used;
         lmdb_key.mv_data = (void *) pmeth->output_val.svalue.buf_addr;
         lmdb_key.mv_size = (size_t) pmeth->output_val.svalue.len_used;
      }
      else {
         lmdb_key.mv_data = (void *) pmeth->output_val.svalue.buf_addr;
         lmdb_key.mv_size = (size_t) pmeth->output_val.svalue.len_used;
      }

      lmdb_data.mv_data = (void *) pmeth->output_key.svalue.buf_addr;
      lmdb_data.mv_size = (size_t)  pmeth->output_key.svalue.len_alloc;

      rc = pcon->p_lmdb_so->p_mdb_cursor_get(lmdb_pcursor, &lmdb_key, &lmdb_data, MDB_FIRST);
      rkey = (char *) lmdb_key.mv_data;
      rkey_size = (int) lmdb_key.mv_size;
      rdata = (char *) lmdb_data.mv_data;
      rdata_size = (int) lmdb_data.mv_size;
   }

   while (rc == CACHE_SUCCESS) {
      pmeth->output_val.svalue.len_used = (unsigned int) rkey_size;
      pmeth->output_val.svalue.buf_addr[rkey_size] = '\0';

      pmeth->output_key.svalue.len_used = (unsigned int) rdata_size;
      pmeth->output_key.svalue.buf_addr[rdata_size] = '\0';

      chrp = 0;
      num = 0;
      n1 = 0;
      if (pcon->key_type == DBX_KEYTYPE_INT) {
         sprintf(hex, "%d", (int) pmeth->output_val.num.int32);
         for (n = 0; hex[n]; n ++) {
            buffer[n1 ++] = hex[n];
         }
      }
      else {
         for (n = 0; n < (int) rkey_size; n ++) {
            chr = (unsigned char) *(((char *) rkey) + n);
            if (num == 0 && n > 0 && chrp == 0 && (chr == 1 || chr == 2)) {
               num = n + 1;
            }
            if (chr < 32 || chr > 126 || (num && (n - num) < 8)) {
               sprintf(hex, "%02x", chr);
               buffer[n1 ++] = '\\';
               buffer[n1 ++] = 'x';
               buffer[n1 ++] = hex[0];
               buffer[n1 ++] = hex[1];
            }
            else {
               buffer[n1 ++] = *(((char *) rkey) + n);
            }
            if (num && (n - num) >= 7) {
               num = 0;
            }
            chrp = chr;
         }
      }
      buffer[n1 ++] = ' ';
      buffer[n1 ++] = '=';
      buffer[n1 ++] = ' ';
      for (n = 0; n < (int) rdata_size; n ++) {
         if ((int) *(((char *) rdata) + n) < 32 || (int) *(((char *) rdata) + n) > 126) {
            sprintf(hex, "%02x", (unsigned char) *(((char *) rdata) + n));
            buffer[n1 ++] = '\\';
            buffer[n1 ++] = 'x';
            buffer[n1 ++] = hex[0];
            buffer[n1 ++] = hex[1];
         }
         else {
            buffer[n1 ++] = *(((char *) rdata) + n);
         }
      }
      buffer[n1] = '\0';

      printf("\r\n%s", buffer);
      if (pcon->key_type == DBX_KEYTYPE_M) {
         int n, keyn;
         DBXVAL keys[DBX_MAXARGS];
         keyn = dbx_split_key(&keys[0], (char *) rkey, (int) rkey_size);
         for (n = 0; n < keyn; n ++) {
            num = keys[n].svalue.len_used;
            if (keys[n].svalue.len_used > 250)
               num = 250;
            strncpy(val, keys[n].svalue.buf_addr, keys[n].svalue.len_used);
            val[num] = '\0';
            if (!n)
               printf("   %d:%d:%d:%s", keys[n].csize, keys[n].type, keys[n].svalue.len_used, val);
            else
               printf(", %d:%d:%d:%s", keys[n].csize, keys[n].type, keys[n].svalue.len_used, val);
         }
      }

      if (pcon->dbtype == DBX_DBTYPE_BDB) {
         rc = bdb_pcursor->get(bdb_pcursor, &bdb_key, &bdb_data, DB_NEXT);
         rkey = (char *) bdb_key.data;
         rkey_size = (int) bdb_key.size;
         rdata = (char *) bdb_data.data;
         rdata_size = (int) bdb_data.size;
      }
      else if (pcon->dbtype == DBX_DBTYPE_LMDB) {
         rc = pcon->p_lmdb_so->p_mdb_cursor_get(lmdb_pcursor, &lmdb_key, &lmdb_data, MDB_NEXT);
         rkey = (char *) lmdb_key.mv_data;
         rkey_size = (int) lmdb_key.mv_size;
         if (pcon->key_type == DBX_KEYTYPE_INT) {
            pmeth->output_val.num.int32 = (int) dbx_get_size((unsigned char *) rkey, 0);
         }
         rdata = (char *) lmdb_data.mv_data;
         rdata_size = (int) lmdb_data.mv_size;
      }
      pmeth->output_val.svalue.len_used = 0;
   }
 
   if (pcon->dbtype == DBX_DBTYPE_BDB) {
      bdb_pcursor->close(bdb_pcursor);
   }
   else if (pcon->dbtype == DBX_DBTYPE_LMDB) {
      pcon->p_lmdb_so->p_mdb_cursor_close(lmdb_pcursor);
      lmdb_commit_ro_transaction(pmeth, 0);
   }

   if (rc != CACHE_SUCCESS) {
      dbx_error_message(pmeth, rc, (char *) "dbxbdb::Dump");
   }

   if (rc != CACHE_SUCCESS) {
      dbx_error_message(pmeth, rc, (char *) "dbxbdb::Dump");
   }

   DBX_DBFUN_END(c);
   DBX_DB_UNLOCK(rc);

   result = dbx_new_string8n(isolate, pmeth->output_val.svalue.buf_addr, pmeth->output_val.svalue.len_used, pcon->utf8);
   args.GetReturnValue().Set(result);
   dbx_request_memory_free(pcon, pmeth, 0);

   return;
}


void DBX_DBNAME::Benchmark(const FunctionCallbackInfo<Value>& args)
{
   return;
}


#if DBX_NODE_VERSION >= 120000
class dbxAddonData
{

public:

   dbxAddonData(Isolate* isolate, Local<Object> exports):
      call_count(0) {
         /* Link the existence of this object instance to the existence of exports. */
         exports_.Reset(isolate, exports);
         exports_.SetWeak(this, DeleteMe, WeakCallbackType::kParameter);
      }

   ~dbxAddonData() {
      if (!exports_.IsEmpty()) {
         /* Reset the reference to avoid leaking data. */
         exports_.ClearWeak();
         exports_.Reset();
      }
   }

   /* Per-addon data. */
   int call_count;

private:

   /* Method to call when "exports" is about to be garbage-collected. */
   static void DeleteMe(const WeakCallbackInfo<dbxAddonData>& info) {
      delete info.GetParameter();
   }

   /*
   Weak handle to the "exports" object. An instance of this class will be
   destroyed along with the exports object to which it is weakly bound.
   */
   v8::Persistent<v8::Object> exports_;
};

#endif


/* End of dbx-node class methods */

Persistent<Function> DBX_DBNAME::constructor;

extern "C" {
#if defined(_WIN32)
#if DBX_NODE_VERSION >= 100000
void __declspec(dllexport) init (Local<Object> exports)
#else
void __declspec(dllexport) init (Handle<Object> exports)
#endif
#else
#if DBX_NODE_VERSION >= 100000
static void init (Local<Object> exports)
#else
static void init (Handle<Object> exports)
#endif
#endif
{
   DBX_DBNAME::Init(exports);
   mglobal::Init(exports);
   mcursor::Init(exports);
}

#if DBX_NODE_VERSION >= 120000

/* exports, module, context */
extern "C" NODE_MODULE_EXPORT void
NODE_MODULE_INITIALIZER(Local<Object> exports,
                        Local<Value> module,
                        Local<Context> context) {
   Isolate* isolate = context->GetIsolate();

   /* Create a new instance of dbxAddonData for this instance of the addon. */
   dbxAddonData* data = new dbxAddonData(isolate, exports);
   /* Wrap the data in a v8::External so we can pass it to the method we expose. */
   /* Local<External> external = External::New(isolate, data); */
   External::New(isolate, data);

   init(exports);

   /*
   Expose the method "Method" to JavaScript, and make sure it receives the
   per-addon-instance data we created above by passing `external` as the
   third parameter to the FunctionTemplate constructor.
   exports->Set(context, String::NewFromUtf8(isolate, "method", NewStringType::kNormal).ToLocalChecked(), FunctionTemplate::New(isolate, Method, external)->GetFunction(context).ToLocalChecked()).FromJust();
   */

}

#else

  NODE_MODULE(dbx, init)

#endif
}


DBXMETH * dbx_request_memory(DBXCON *pcon, short context)
{
   int n;
   DBXMETH *pmeth;

   pmeth = NULL;
   if (context) {
      pmeth = (DBXMETH *) pcon->pmeth_base;
   }
   else {
      if (pcon->use_mutex) {
         pmeth = dbx_request_memory_alloc(pcon, 0);
      }
      else {
         pmeth = (DBXMETH *) pcon->pmeth_base;
      }
   }

   pmeth->pcon = pcon;
   pmeth->binary = 0;
   pmeth->lock = 0;
   pmeth->increment = 0;
   pmeth->done = 0;
   for (n = 0; n < DBX_MAXARGS; n ++) {
      pmeth->key.args[n].cvalue.pstr = NULL;
   }

   return pmeth;
}


DBXMETH * dbx_request_memory_alloc(DBXCON *pcon, short context)
{
   DBXMETH *pmeth;

   pmeth = (DBXMETH *) dbx_malloc(sizeof(DBXMETH), 0);
   if (!pmeth) {
      return NULL;
   }

   pmeth->output_val.svalue.buf_addr = (char *) dbx_malloc(32000, 0);
   if (!pmeth->output_val.svalue.buf_addr) {
      dbx_free((void *) pmeth, 0);
      return NULL;
   }
   memset((void *) pmeth->output_val.svalue.buf_addr, 0, 32000);
   pmeth->output_val.svalue.len_alloc = 32000;
   pmeth->output_val.svalue.len_used = 0;

   pmeth->output_key.svalue.buf_addr = (char *) dbx_malloc(32000, 0);
   if (!pmeth->output_key.svalue.buf_addr) {
      dbx_free((void *) pmeth->output_val.svalue.buf_addr, 0);
      dbx_free((void *) pmeth, 0);
      return NULL;
   }
   memset((void *) pmeth->output_key.svalue.buf_addr, 0, 32000);
   pmeth->output_key.svalue.len_alloc = 32000;
   pmeth->output_key.svalue.len_used = 0;

   pmeth->key.ibuffer = (unsigned char *) dbx_malloc(CACHE_MAXSTRLEN + DBX_IBUFFER_OFFSET, 0);
   if (!pmeth->key.ibuffer) {
      dbx_free((void *) pmeth->output_key.svalue.buf_addr, 0);
      dbx_free((void *) pmeth->output_val.svalue.buf_addr, 0);
      dbx_free((void *) pmeth, 0);
      return NULL;
   }

   memset((void *) pmeth->key.ibuffer, 0, DBX_IBUFFER_OFFSET);
   dbx_add_block_size(pmeth->key.ibuffer + 5, 0, CACHE_MAXSTRLEN, 0, 0);
   pmeth->key.ibuffer += DBX_IBUFFER_OFFSET;
   pmeth->key.ibuffer_used = 0;
   pmeth->key.ibuffer_size = CACHE_MAXSTRLEN;

   return pmeth;
}


int dbx_request_memory_free(DBXCON *pcon, DBXMETH *pmeth, short context)
{
   if (!pmeth) {
      return CACHE_SUCCESS;
   }
   if (pmeth != (DBXMETH *) pcon->pmeth_base) {
      if (pmeth->key.ibuffer) {
         pmeth->key.ibuffer -= DBX_IBUFFER_OFFSET;
         dbx_free((void *) pmeth->key.ibuffer, 0);
      }
      if (pmeth->output_val.svalue.buf_addr) {
         dbx_free((void *) pmeth->output_val.svalue.buf_addr, 0);
      }
      dbx_free((void *) pmeth, 0);
   }
   return CACHE_SUCCESS;
}


#if DBX_NODE_VERSION >= 100000
void dbx_set_prototype_method(v8::Local<v8::FunctionTemplate> t, v8::FunctionCallback callback, const char* name, const char* data)
#else
void dbx_set_prototype_method(v8::Handle<v8::FunctionTemplate> t, v8::FunctionCallback callback, const char* name, const char* data)
#endif
{
#if DBX_NODE_VERSION >= 100000

   v8::Isolate* isolate = v8::Isolate::GetCurrent();
   v8::HandleScope handle_scope(isolate);
   v8::Local<v8::Signature> s = v8::Signature::New(isolate, t);

#if DBX_NODE_VERSION >= 120000
   v8::Local<v8::String> data_str = v8::String::NewFromUtf8(isolate, data, NewStringType::kNormal).ToLocalChecked();
#else
   v8::Local<v8::String> data_str = v8::String::NewFromUtf8(isolate, data);
#endif

#if 0
   v8::Local<v8::FunctionTemplate> tx = v8::FunctionTemplate::New(isolate, callback, v8::Local<v8::Value>(), s);
#else
   v8::Local<v8::FunctionTemplate> tx = v8::FunctionTemplate::New(isolate, callback, data_str, s);
#endif

#if DBX_NODE_VERSION >= 120000
   v8::Local<v8::String> fn_name = String::NewFromUtf8(isolate, name, NewStringType::kNormal).ToLocalChecked();;

#else
   v8::Local<v8::String> fn_name = String::NewFromUtf8(isolate, name);
#endif

   tx->SetClassName(fn_name);
   t->PrototypeTemplate()->Set(fn_name, tx);
#else
   NODE_SET_PROTOTYPE_METHOD(t, name, callback);
#endif

   return;
}


v8::Local<v8::Object> dbx_is_object(v8::Local<v8::Value> value, int *otype)
{
#if DBX_NODE_VERSION >= 100000
   v8::Isolate* isolate = v8::Isolate::GetCurrent();
   v8::Local<v8::Context> icontext = isolate->GetCurrentContext();
#endif
   *otype = 0;

   if (value->IsObject()) {
      *otype = 1;
      v8::Local<v8::Object> value_obj = DBX_TO_OBJECT(value);
      if (node::Buffer::HasInstance(value_obj)) {
         *otype = 2;
      }
      return value_obj;
   }
   else {
      return v8::Local<v8::Object>::Cast(value);
   }
}


int dbx_string8_length(v8::Isolate * isolate, v8::Local<v8::String> str, int utf8)
{
   if (utf8) {
      return DBX_UTF8_LENGTH(str);
   }
   else {
      return DBX_LENGTH(str);
   }
}

v8::Local<v8::String> dbx_new_string8(v8::Isolate * isolate, char * buffer, int utf8)
{
   if (utf8) {
#if DBX_NODE_VERSION >= 120000
      return v8::String::NewFromUtf8(isolate, buffer, NewStringType::kNormal).ToLocalChecked();
#else
      return v8::String::NewFromUtf8(isolate, buffer);
#endif
   }
   else {
#if DBX_NODE_VERSION >= 100000
      return v8::String::NewFromOneByte(isolate, (uint8_t *) buffer, NewStringType::kInternalized).ToLocalChecked();
#else
      /* return String::NewFromOneByte(isolate, (uint8_t *) buffer, String::kNormalString); */
      return v8::String::NewFromOneByte(isolate, (uint8_t *) buffer, v8::NewStringType::kInternalized).ToLocalChecked();
#endif
   }
}


v8::Local<v8::String> dbx_new_string8n(v8::Isolate * isolate, char * buffer, unsigned long len, int utf8)
{
   if (utf8) {
#if DBX_NODE_VERSION >= 120000
      return v8::String::NewFromUtf8(isolate, buffer, NewStringType::kNormal, len).ToLocalChecked();
#else
      return v8::String::NewFromUtf8(isolate, buffer, String::kNormalString, len);
#endif
   }
   else {
#if DBX_NODE_VERSION >= 100000
      return v8::String::NewFromOneByte(isolate, (uint8_t *) buffer, NewStringType::kInternalized, len).ToLocalChecked();
#else
      /* return v8::String::NewFromOneByte(isolate, (uint8_t *) buffer, String::kNormalString, len); */
      return v8::String::NewFromOneByte(isolate, (uint8_t *) buffer, v8::NewStringType::kInternalized, len).ToLocalChecked();
#endif

   }
}


int dbx_write_char8(v8::Isolate * isolate, v8::Local<v8::String> str, char * buffer, int utf8)
{
   if (utf8) {
      return DBX_WRITE_UTF8(str, buffer);
   }
   else {
      return DBX_WRITE_ONE_BYTE(str, (uint8_t *) buffer);
   }
}


int dbx_ibuffer_add(DBXMETH *pmeth, DBXKEY *pkey, v8::Isolate * isolate, int argn, v8::Local<v8::String> str, char * buffer, int buffer_len, short context)
{
   int len, n;
   unsigned char *p;
   char nstr[64];
   DBXCON *pcon = pmeth->pcon;

#ifdef _WIN32
__try {
#endif

   if (buffer)
      len = buffer_len;
   else
      len = dbx_string8_length(isolate, str, pcon->utf8);

   /* 1.4.11 resize input buffer if necessary */

   if ((pkey->ibuffer_used + len + 32) >pkey->ibuffer_size) {
      p = (unsigned char *) dbx_malloc(sizeof(char) * (pkey->ibuffer_used + len + CACHE_MAXSTRLEN + DBX_IBUFFER_OFFSET), 301);
      if (p) {
         if (pkey->ibuffer && pkey->ibuffer_used > 0) { 
            memcpy((void *) p, (void *) (pkey->ibuffer - DBX_IBUFFER_OFFSET), (size_t) (pkey->ibuffer_used + DBX_IBUFFER_OFFSET));
            dbx_free((void *) (pkey->ibuffer - DBX_IBUFFER_OFFSET), 301);
         }
         pkey->ibuffer = (p + DBX_IBUFFER_OFFSET);
         pkey->ibuffer_size = (pkey->ibuffer_used + len + CACHE_MAXSTRLEN);
         pkey->args[0].svalue.buf_addr = (char *) pkey->ibuffer;
         for (n = 1; n < pkey->argc; n ++) {
            pkey->args[n].svalue.buf_addr = (char *) (pkey->ibuffer + pkey->args[n - 1].csize);
         }
      }
      else {
         return 0;
      }
   }

   p = (pkey->ibuffer + pkey->ibuffer_used);

   if (pmeth->pcon->key_type == DBX_KEYTYPE_M) {
      if (pkey->args[argn].type == DBX_DTYPE_INT) {
         dbx_set_number(&pkey->args[argn], p);
         p += 10;
         pkey->ibuffer_used += 10;
      }
      else {
         *(p ++) = 0x00;
         *(p ++) = 0x03;
         pkey->ibuffer_used += 2;
      }
   }

   if (buffer) {
      T_MEMCPY((void *) p, (void *) buffer, (size_t) len);
   }
   else {
      dbx_write_char8(isolate, str, (char *) p, pcon->utf8);
   }
   pkey->ibuffer_used += len;

   pkey->args[argn].svalue.buf_addr = (char *) p;
   pkey->args[argn].svalue.len_alloc = len;
   pkey->args[argn].svalue.len_used = len;
   pkey->args[argn].csize = pkey->ibuffer_used;
   pkey->argc = argn;

   if (pmeth->pcon->key_type == DBX_KEYTYPE_M) {
      if (pkey->args[argn].type == DBX_DTYPE_STR && dbx_is_number(&pkey->args[argn])) { /* See if we have a stringified number */

         for (n = (len - 1); n >= 0; n --) {
            p[n + 8] = p[n];
         }

         dbx_set_number(&pkey->args[argn], p - 2);
         p += 8;
         pkey->ibuffer_used += 8;
         pkey->args[argn].svalue.buf_addr = (char *) p;
         pkey->args[argn].csize = pkey->ibuffer_used;
         /* dbx_dump_key((char *) pkey->ibuffer, (int) pkey->ibuffer_used); */
      }

      if (argn == 0 && p[0] == '^') {
         for (n = 1; n < len; n ++) {
            p[n - 1] = p[n];
         }
         len --;
         p[len] = '\0';
         pkey->args[argn].svalue.len_alloc = len;
         pkey->args[argn].svalue.len_used = len;
         pkey->ibuffer_used --;
         pkey->args[argn].csize = pkey->ibuffer_used;
      }
      if (pkey->args[argn].svalue.len_used == 0) { /* null - so introducing sequence must be \x00\x00 */
         pkey->ibuffer[pkey->ibuffer_used - 1] = 0x00;
      }
   }
   else { /* v1.0.2 */
      if (pkey->args[argn].type == DBX_DTYPE_STR && dbx_is_number(&pkey->args[argn])) { /* See if we have a stringified number */
         strncpy(nstr, pkey->args[argn].svalue.buf_addr, pkey->args[argn].svalue.len_used);
         nstr[pkey->args[argn].svalue.len_used] = '\0';
         pkey->args[argn].num.int32 = (int) strtol(nstr, NULL, 10);
      }
   }

   return len;

#ifdef _WIN32
}
__except (EXCEPTION_EXECUTE_HANDLER) {

   DWORD code;
   char bufferx[256];

   __try {
      code = GetExceptionCode();
      sprintf_s(bufferx, 255, "Exception caught in f:dbx_ibuffer_add: %x", code);
      dbx_log_event(pcon, bufferx, "Error Condition", 0);
   }
   __except (EXCEPTION_EXECUTE_HANDLER) {
      ;
   }

   return 0;
}
#endif
}


int dbx_is_number(DBXVAL *pval)
{
   int neg, dp, num;
   unsigned int n;
   char buffer[64];

   if (pval->svalue.len_used == 0 || pval->svalue.len_used > 32) {
      return 0;
   }

   neg = 0;
   if (pval->svalue.buf_addr[0] == '-')
      neg = 1;

   dp = 0;
   num = 1;
   for (n = neg; n < pval->svalue.len_used; n ++) {
      if (pval->svalue.buf_addr[n] == '.' && dp == 0) {
         dp = 1;
         continue;
      }
      if ((int) pval->svalue.buf_addr[n] < 48 || (int) pval->svalue.buf_addr[n] > 57) {
         num = 0;
         break;
      }
   }

   if (num && !dp) { /* convert to integer if possible */
      strncpy(buffer, pval->svalue.buf_addr, pval->svalue.len_used);
      buffer[pval->svalue.len_used] = '\0';
      pval->num.int32 = (int) strtol(buffer, NULL, 10);
      pval->type = DBX_DTYPE_INT;
   }
   return num;
}



int dbx_set_number(DBXVAL *pval, unsigned char *px)
{
   int num, dec;
   unsigned char *p;
   char buffer[128], decstr[64];

   dec = 0;
   if (pval->type == DBX_DTYPE_INT) {
      num = pval->num.int32;
   }
   else {
      strncpy(buffer, pval->svalue.buf_addr, pval->svalue.len_used);
      buffer[pval->svalue.len_used] = '\0';
      p = (unsigned char *) strstr(buffer, ".");
      if (p) {
         *(p ++) = '\0';
         sprintf(decstr, "%s", p);
         strcat(decstr, "000000000");
         decstr[9] = '\0';
         dec = (int) strtol(decstr, NULL, 10);
      }
      num = (int) strtol(buffer, NULL, 10);
   }

   if (num < 0) {
      num *= -1;
      *(px ++) = 0x00;
      *(px ++) = 0x01;
      dbx_set_size((unsigned char *) px, 0xffffffff - (unsigned long) num, 1);
      px += 4;
      dbx_set_size((unsigned char *) px, 0xffffffff - (unsigned long) dec, 1);
      px += 4;
   }
   else {
      *(px ++) = 0x00;
      *(px ++) = 0x02;
      dbx_set_size((unsigned char *) px, (unsigned long) num, 1);
      px += 4;
      dbx_set_size((unsigned char *) px, (unsigned long) dec, 1);
      px += 4;
   }
   return 0;
}


int dbx_split_key(DBXVAL *keys, char * key, int key_len)
{
   int n, keyn, nstart, nend;
   char *dp;
   char numstr[256];

   nstart = 0;
   nend = 0;
   keyn = 0;
   n = 0;
   while (n < key_len) {
      if (key[n] == 0x00 && (key[n + 1] == 0x01 || key[n + 1] == 0x02)) { /* number */
        nend = n;
         if (keyn > 0) {
            keys[keyn - 1].svalue.len_used = (nend - nstart);
            keys[keyn - 1].svalue.len_alloc = keys[keyn - 1].svalue.len_used;
            keys[keyn - 1].csize = nend;
         }
         nstart = n + 10;
         keys[keyn].svalue.buf_addr = key + nstart;
         keys[keyn].num.int32 = (int) strtol(keys[keyn].svalue.buf_addr, NULL, 10);
         keys[keyn].type = DBX_DTYPE_INT;
         keyn ++;
         n += 10;
      }
      else if (key[n] == 0x00 && key[n + 1] == 0x03) { /* string */
         nend = n;
         if (keyn > 0) {
            keys[keyn - 1].svalue.len_used = (nend - nstart);
            keys[keyn - 1].svalue.len_alloc = keys[keyn - 1].svalue.len_used;
            keys[keyn - 1].csize = nend;
         }
         nstart = n + 2;
         keys[keyn].svalue.buf_addr = key + nstart;
         keys[keyn].type = DBX_DTYPE_STR;
         keyn ++;
         n += 2;
      }
      n ++;
   }
   if (keyn > 0) {
      keys[keyn - 1].svalue.len_used = (key_len - nstart);
      keys[keyn - 1].svalue.len_alloc = keys[keyn - 1].svalue.len_used;
      keys[keyn - 1].csize = key_len;
   }

   for (n = 0; n < keyn; n ++) {
      if (keys[keyn].type == DBX_DTYPE_INT) {
         strncpy(numstr, keys[n].svalue.buf_addr, keys[n].svalue.len_used);
         numstr[keys[n].svalue.len_used] = '\0';
         dp = strstr(numstr, ".");
         if (dp) {
            keys[n].type = DBX_DTYPE_STR;
         }
      }
   }

   return keyn;
}


int dbx_dump_key(char * key, int key_len)
{
   int n, n1;
   char buffer[4096], hex[16];

   n1 = 0;
   for (n = 0; n < key_len; n ++) {
      if ((int) *(((char *) key) + n) < 32 || (int) *(((char *) key) + n) > 126) {
         sprintf(hex, "%02x", (unsigned char) *(((char *) key) + n));
         buffer[n1 ++] = '\\';
         buffer[n1 ++] = 'x';
         buffer[n1 ++] = hex[0];
         buffer[n1 ++] = hex[1];
      }
      else {
         buffer[n1 ++] = *(((char *) key) + n);
      }
   }
   buffer[n1] = '\0';
   printf("\r\n%s", buffer);

   return 0;
}


int dbx_memcpy(void * to, void *from, size_t size)
{
   int n;
   char *p1, *p2;

   p1 = (char *) to;
   p2 = (char *) from;

   for (n = 0; n < (int) size; n ++) {
      *p1 ++ = *p2 ++;
   }
   return 0;
}


int dbx_global_reset(const v8::FunctionCallbackInfo<v8::Value>& args, v8::Isolate * isolate, DBXCON *pcon, DBXMETH *pmeth, void *pgx, int argc_offset, short context)
{
   v8::Local<v8::Context> icontext = isolate->GetCurrentContext();
   int n, len, otype;
   char global_name[256], *p;
   DBXVAL *pval, *pvalp;
   v8::Local<v8::Object> obj;
   v8::Local<v8::String> str;
   mglobal *gx = (mglobal *) pgx;

#ifdef _WIN32
__try {
#endif

   if (pcon->log_functions && gx && gx->c) {
      gx->c->LogFunction(gx->c, args, NULL, (char *) "dbx_global_reset");
   }

   dbx_write_char8(isolate, DBX_TO_STRING(args[argc_offset]), global_name, pcon->utf8);
   if (global_name[0] == '\0') {
      return -1;
   }

   pval = gx->pkey;
   while (pval) {
      pvalp = pval;
      pval = pval->pnext;
      dbx_free((void *) pvalp, 0);
   }
   gx->pkey = NULL;

   if (pcon->key_type == DBX_KEYTYPE_M) {
      if (global_name[0] == '^') {
         T_STRCPY(gx->global_name, _dbxso(gx->global_name), global_name + 1);
      }
      else {
         T_STRCPY(gx->global_name, _dbxso(gx->global_name), global_name);
      }
   }

   pvalp = NULL;
   for (n = (argc_offset + 1); n < pmeth->jsargc; n ++) {
      if (args[n]->IsInt32()) {
         pval = (DBXVAL *) dbx_malloc(sizeof(DBXVAL), 0);
         pval->type = DBX_DTYPE_INT;
         pval->num.int32 = (int) DBX_INT32_VALUE(args[n]);
      }
      else {
         obj = dbx_is_object(args[n], &otype);
         if (otype == 2) {
            p = node::Buffer::Data(obj);
            len = (int) node::Buffer::Length(obj);
            pval = (DBXVAL *) dbx_malloc(sizeof(DBXVAL) + len + 32, 0);
            pval->type = DBX_DTYPE_STR;
            pval->svalue.buf_addr = ((char *) pval) + sizeof(DBXVAL);
            memcpy((void *) pval->svalue.buf_addr, (void *) p, (size_t) len);
            pval->svalue.len_alloc = len + 32;
            pval->svalue.len_used = len;
         }
         else {
            str = DBX_TO_STRING(args[n]);
            len = (int) dbx_string8_length(isolate, str, 0);
            pval = (DBXVAL *) dbx_malloc(sizeof(DBXVAL) + len + 32, 0);
            pval->type = DBX_DTYPE_STR;
            pval->svalue.buf_addr = ((char *) pval) + sizeof(DBXVAL);
            dbx_write_char8(isolate, str, pval->svalue.buf_addr, 1);
            pval->svalue.len_alloc = len + 32;
            pval->svalue.len_used = len;
         }
      }
      if (pvalp) {
         pvalp->pnext = pval;
      }
      else {
         gx->pkey = pval;
      }
      pvalp = pval;
      pvalp->pnext = NULL;
   }

   return 0;

#ifdef _WIN32
}
__except (EXCEPTION_EXECUTE_HANDLER) {

   DWORD code;
   char bufferx[256];

   __try {
      code = GetExceptionCode();
      sprintf_s(bufferx, 255, "Exception caught in f:dbx_global_reset: %x", code);
      dbx_log_event(pcon, bufferx, "Error Condition", 0);
   }
   __except (EXCEPTION_EXECUTE_HANDLER) {
      ;
   }

   return 0;
}
#endif
}


int dbx_cursor_init(void *pcx)
{
   mcursor *cx = (mcursor *) pcx;

   cx->context = 0;
   cx->getdata = 0;
   cx->format = 0;
   cx->counter = 0;
   cx->global_name[0] = '\0';
   cx->pqr_prev = NULL;
   cx->pqr_next = NULL;
   cx->data.buf_addr = NULL;
   cx->data.len_alloc = 0;
   cx->data.len_used = 0;
   cx->psql = NULL;
   cx->c = NULL;
   cx->pcursor = NULL;

   return 0;
}


int dbx_cursor_reset(const v8::FunctionCallbackInfo<v8::Value>& args, v8::Isolate * isolate, DBXCON *pcon, DBXMETH *pmeth, void *pcx, int argc_offset, short context)
{
   v8::Local<v8::Context> icontext = isolate->GetCurrentContext();
   int n, len, alen, nx, otype;
   char *p;
   char global_name[256], buffer[256];
   DBXSQL *psql;
   DBC *pbdbcursor;
   MDB_cursor *plmdbcursor;
   MDB_txn * plmdbtxnro;
   v8::Local<v8::Object> obj;
   v8::Local<v8::String> key;
   v8::Local<v8::String> value;
   mcursor *cx = (mcursor *) pcx;

#ifdef _WIN32
__try {
#endif

   if (pcon->log_functions && cx && cx->c) {
      cx->c->LogFunction(cx->c, args, NULL, (char *) "dbx_cursor_reset");
   }

   if (pmeth->jsargc < 1) {
      return -1;
   }

   if (pcon->dbtype == DBX_DBTYPE_BDB) {
      if (cx->pcursor) {
         pbdbcursor = (DBC *) cx->pcursor;
         pbdbcursor->close(pbdbcursor);
      }
      n = pcon->p_bdb_so->pdb->cursor(pcon->p_bdb_so->pdb, NULL, &pbdbcursor, 0);
      cx->pcursor = (void *) pbdbcursor;
   }
   else if (pcon->dbtype == DBX_DBTYPE_LMDB) {
      if (cx->pcursor) {
         plmdbcursor = (MDB_cursor *) cx->pcursor;
         plmdbtxnro = (MDB_txn *) cx->ptxnro;
         pcon->p_lmdb_so->p_mdb_cursor_close(plmdbcursor);
         lmdb_commit_qro_transaction(pmeth, &(plmdbtxnro), 0);
      }
      n = lmdb_start_qro_transaction(pmeth, &(plmdbtxnro), 0);
      n = pcon->p_lmdb_so->p_mdb_cursor_open(plmdbtxnro, pcon->p_lmdb_so->db, &plmdbcursor);
      cx->ptxnro = (void *) plmdbtxnro;
      cx->pcursor = (void *) plmdbcursor;
   }

   obj = DBX_TO_OBJECT(args[argc_offset]);
   key = dbx_new_string8(isolate, (char *) "sql", 1);
   if (DBX_GET(obj, key)->IsString()) {
      value = DBX_TO_STRING(DBX_GET(obj, key));
      len = (int) dbx_string8_length(isolate, value, 0);
      psql = (DBXSQL *) dbx_malloc(sizeof(DBXSQL) + (len + 4), 0);
      for (n = 0; n < DBX_SQL_MAXCOL; n ++) {
         psql->cols[n] = NULL;
      }
      psql->no_cols = 0;
      psql->sql_script = ((char *) psql) + sizeof(DBXSQL);
      psql->sql_script_len = len;
      dbx_write_char8(isolate, value, psql->sql_script, pcon->utf8);

      psql->sql_type = DBX_SQL_MGSQL;
      key = dbx_new_string8(isolate, (char *) "type", 1);
      if (DBX_GET(obj, key)->IsString()) {
         value = DBX_TO_STRING(DBX_GET(obj, key));
         dbx_write_char8(isolate, value, buffer, pcon->utf8);
         dbx_lcase(buffer);
         psql->sql_type = DBX_SQL_MGSQL;
      }

      cx->psql = psql;
      DBX_DB_LOCK(n, 0);
      psql->sql_no = ++ dbx_sql_counter;
      DBX_DB_UNLOCK(n);
   
      cx->context = 11;
      cx->counter = 0;
      cx->getdata = 0;
      cx->format = 0;

      if (pmeth->jsargc > (argc_offset + 1)) {
         dbx_is_object(args[argc_offset + 1], &n);
         if (n) {
            obj = DBX_TO_OBJECT(args[argc_offset + 1]);
            key = dbx_new_string8(isolate, (char *) "format", 1);
            if (DBX_GET(obj, key)->IsString()) {
               char buffer[64];
               value = DBX_TO_STRING(DBX_GET(obj, key));
               dbx_write_char8(isolate, value, buffer, 1);
               dbx_lcase(buffer);
               if (!strcmp(buffer, "url")) {
                  cx->format = 1;
               }
            }
         }
      }
      return 0;
   }

   if (!cx->pqr_prev) {
      cx->pqr_prev = dbx_alloc_dbxqr(NULL, 0, 0);
   }
   if (!cx->pqr_next) {
      cx->pqr_next = dbx_alloc_dbxqr(NULL, 0, 0);
   }
   if (!cx->data.buf_addr) {
      cx->data.buf_addr = (char *) dbx_malloc(CACHE_MAXSTRLEN, 0);
      cx->data.len_alloc = CACHE_MAXSTRLEN;
      cx->data.len_used = 0;
   }

   cx->pqr_prev->key.ibuffer_used = 0;
   nx = 0;
   if (pcon->key_type == DBX_KEYTYPE_M) {
      key = dbx_new_string8(isolate, (char *) "global", 1);
      if (DBX_GET(obj, key)->IsString()) {
         dbx_write_char8(isolate, DBX_TO_STRING(DBX_GET(obj, key)), global_name, pcon->utf8);
      }
      else {
         return -1;
      }

      if (global_name[0] == '^') {
         T_STRCPY(cx->global_name, _dbxso(gx->global_name), global_name + 1);
      }
      else {
         T_STRCPY(cx->global_name, _dbxso(gx->global_name), global_name);
      }
      dbx_ibuffer_add(pmeth, &(cx->pqr_prev->key), isolate, nx, value, cx->global_name, (int) strlen(cx->global_name), 0);
      nx ++;

      strcpy(cx->pqr_next->global_name.buf_addr, cx->global_name);
      strcpy(cx->pqr_prev->global_name.buf_addr, cx->global_name);
      cx->pqr_prev->global_name.len_used = (int) strlen((char *) cx->pqr_prev->global_name.buf_addr);
      cx->pqr_next->global_name.len_used =  cx->pqr_prev->global_name.len_used;
   }
   else {
      cx->pqr_prev->global_name.len_used = 0;
      cx->pqr_next->global_name.len_used = 0;
   }

   cx->pqr_prev->key.argc = nx;

   key = dbx_new_string8(isolate, (char *) "key", 1);
   if (DBX_GET(obj, key)->IsArray()) {
      Local<Array> a = Local<Array>::Cast(DBX_GET(obj, key));
      alen = (int) a->Length();

      for (n = 0; n < alen; n ++) {
         if (DBX_GET(a, n)->IsInt32()) {
            cx->pqr_prev->key.args[nx].type = DBX_DTYPE_INT;
            cx->pqr_prev->key.args[nx].num.int32 = (int) DBX_INT32_VALUE(DBX_GET(a, n));
            T_SPRINTF(buffer, _dbxso(buffer), "%d", pmeth->key.args[nx].num.int32);
            dbx_ibuffer_add(pmeth, &(cx->pqr_prev->key), isolate, nx, value, buffer, (int) strlen(buffer), 0);
         }
         else {
            cx->pqr_prev->key.args[nx].type = DBX_DTYPE_STR;
            obj = dbx_is_object(DBX_GET(a, n), &otype);

            if (otype == 2) {
               p = node::Buffer::Data(obj);
               len = (int) node::Buffer::Length(obj);
               dbx_ibuffer_add(pmeth, &(cx->pqr_prev->key), isolate, nx, value, p, (int) len, 0);
            }
            else {
               value = DBX_TO_STRING(DBX_GET(a, n));
               dbx_ibuffer_add(pmeth, &(cx->pqr_prev->key), isolate, nx, value, NULL, 0, 0);
            }
         }
         nx ++;
      }
   }

   cx->pqr_prev->key.argc = nx;

   cx->fixed_key_len = cx->pqr_prev->key.ibuffer_used;
   cx->context = 1;
   cx->counter = 0;
   cx->getdata = 0;
   cx->format = 0;

   if (pmeth->jsargc > (argc_offset + 1)) {
      obj = DBX_TO_OBJECT(args[argc_offset + 1]);
      key = dbx_new_string8(isolate, (char *) "getdata", 1);
      if (DBX_GET(obj, key)->IsBoolean()) {
         if (DBX_TO_BOOLEAN(DBX_GET(obj, key))->IsTrue()) {
            cx->getdata = 1;
         }
      }
      key = dbx_new_string8(isolate, (char *) "multilevel", 1);
      if (DBX_GET(obj, key)->IsBoolean()) {
         if (DBX_TO_BOOLEAN(DBX_GET(obj, key))->IsTrue()) {
            cx->context = 2;
         }
      }
      key = dbx_new_string8(isolate, (char *) "globaldirectory", 1);
      if (DBX_GET(obj, key)->IsBoolean()) {
         if (DBX_TO_BOOLEAN(DBX_GET(obj, key))->IsTrue()) {
            cx->context = 9;
         }
      }
      key = dbx_new_string8(isolate, (char *) "format", 1);
      if (DBX_GET(obj, key)->IsString()) {
         char buffer[64];
         value = DBX_TO_STRING(DBX_GET(obj, key));
         dbx_write_char8(isolate, value, buffer, 1);
         dbx_lcase(buffer);
         if (!strcmp(buffer, "url")) {
            cx->format = 1;
         }
      }
   }

   if (pcon->key_type == DBX_KEYTYPE_M) {
      if (cx->context != 9 && cx->global_name[0] == '\0') { /* not a global directory so global name cannot be empty */
         return -1;
      }
   }
   else {
      cx->context = 2; /* one key so set multilevel context */
   }

   return 0;

#ifdef _WIN32
}
__except (EXCEPTION_EXECUTE_HANDLER) {

   DWORD code;
   char bufferx[256];

   __try {
      code = GetExceptionCode();
      sprintf_s(bufferx, 255, "Exception caught in f:dbx_cursor_reset: %x", code);
      dbx_log_event(pcon, bufferx, "Error Condition", 0);
   }
   __except (EXCEPTION_EXECUTE_HANDLER) {
      ;
   }

   return 0;
}
#endif
}


int bdb_load_library(DBXCON *pcon)
{
   int result;
   char primerr[DBX_ERROR_SIZE];
   char fun[64];

   strcpy(pcon->p_bdb_so->libdir, pcon->db_library);
   strcpy(pcon->p_bdb_so->funprfx, "db");
   strcpy(pcon->p_bdb_so->dbname, "BDB");

   strcpy(pcon->p_bdb_so->libnam, pcon->p_bdb_so->libdir);

   pcon->p_bdb_so->p_library = dbx_dso_load(pcon->p_bdb_so->libnam);
   if (!pcon->p_bdb_so->p_library) {
      int len1, len2;
      char *p;
#if defined(_WIN32)
      DWORD errorcode;
      LPVOID lpMsgBuf;

      lpMsgBuf = NULL;
      errorcode = GetLastError();
      sprintf(pcon->error, "Error loading %s Library: %s; Error Code : %ld",  pcon->p_bdb_so->dbname, pcon->p_bdb_so->libnam, errorcode);
      len2 = (int) strlen(pcon->error);
      len1 = FormatMessage(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
                     NULL,
                     errorcode,
                     /* MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), */
                     MAKELANGID(LANG_ENGLISH, SUBLANG_ENGLISH_US),
                     (LPTSTR) &lpMsgBuf,
                     0,
                     NULL 
                     );
      if (lpMsgBuf && len1 > 0 && (DBX_ERROR_SIZE - len2) > 30) {
         strncpy(primerr, (const char *) lpMsgBuf, DBX_ERROR_SIZE - 1);
         p = strstr(primerr, "\r\n");
         if (p)
            *p = '\0';
         len1 = (DBX_ERROR_SIZE - (len2 + 10));
         if (len1 < 1)
            len1 = 0;
         primerr[len1] = '\0';
         p = strstr(primerr, "%1");
         if (p) {
            *p = 'I';
            *(p + 1) = 't';
         }
         strcat(pcon->error, " (");
         strcat(pcon->error, primerr);
         strcat(pcon->error, ")");
      }
      if (lpMsgBuf)
         LocalFree(lpMsgBuf);
#else
      p = (char *) dlerror();
      sprintf(primerr, "Cannot load %s library: Error Code: %d", pcon->p_bdb_so->dbname, errno);
      len2 = strlen(pcon->error);
      if (p) {
         strncpy(primerr, p, DBX_ERROR_SIZE - 1);
         primerr[DBX_ERROR_SIZE - 1] = '\0';
         len1 = (DBX_ERROR_SIZE - (len2 + 10));
         if (len1 < 1)
            len1 = 0;
         primerr[len1] = '\0';
         strcat(pcon->error, " (");
         strcat(pcon->error, primerr);
         strcat(pcon->error, ")");
      }
#endif
      goto bdb_load_library_exit;
   }
/*
   int               (* p_db_create)                 (DB **pdb, DB_ENV *dbenv, u_int32_t flags);
   char *            (* p_db_full_version)           (int *family, int *release, int *major, int *minor, int *patch);
*/

   sprintf(fun, "%s_create", pcon->p_bdb_so->funprfx);
   pcon->p_bdb_so->p_db_create = (int (*) (DB **, DB_ENV *, u_int32_t)) dbx_dso_sym(pcon->p_bdb_so->p_library, (char *) fun);
   if (!pcon->p_bdb_so->p_db_create) {
      sprintf(pcon->error, "Error loading %s library: %s; Cannot locate the following function : %s", pcon->p_bdb_so->dbname, pcon->p_bdb_so->libnam, fun);
      goto bdb_load_library_exit;
   }
   sprintf(fun, "%s_env_create", pcon->p_bdb_so->funprfx);
   pcon->p_bdb_so->p_db_env_create = (int (*) (DB_ENV **, u_int32_t)) dbx_dso_sym(pcon->p_bdb_so->p_library, (char *) fun);
   if (!pcon->p_bdb_so->p_db_env_create) {
      sprintf(pcon->error, "Error loading %s library: %s; Cannot locate the following function : %s", pcon->p_bdb_so->dbname, pcon->p_bdb_so->libnam, fun);
      goto bdb_load_library_exit;
   }
   sprintf(fun, "%s_full_version", pcon->p_bdb_so->funprfx);
   pcon->p_bdb_so->p_db_full_version = (char * (*) (int *, int *, int *, int *, int *)) dbx_dso_sym(pcon->p_bdb_so->p_library, (char *) fun);
   if (!pcon->p_bdb_so->p_db_full_version) {
      sprintf(pcon->error, "Error loading %s library: %s; Cannot locate the following function : %s", pcon->p_bdb_so->dbname, pcon->p_bdb_so->libnam, fun);
      goto bdb_load_library_exit;
   }

   pcon->p_bdb_so->loaded = 1;

bdb_load_library_exit:

   if (pcon->error[0]) {
      pcon->p_bdb_so->loaded = 0;
      pcon->error_code = 1009;
      result = CACHE_NOCON;

      return result;
   }

   return CACHE_SUCCESS;
}


int bdb_open(DBXMETH *pmeth)
{
   int rc, result;
   u_int32_t db_flags; /* database open flags */
   u_int32_t env_flags; /* database environment flags */
   char *pver;
   DBXCON *pcon = pmeth->pcon;

   if (!pcon->p_bdb_so) {
      pcon->p_bdb_so = (DBXBDBSO *) dbx_malloc(sizeof(DBXBDBSO), 0);
      if (!pcon->p_bdb_so) {
         T_STRCPY(pcon->error, _dbxso(pcon->error), "No Memory");
         pcon->error_code = 1009; 
         result = CACHE_NOCON;
         return result;
      }
      memset((void *) pcon->p_bdb_so, 0, sizeof(DBXBDBSO));
      pcon->p_bdb_so->pdb = NULL;
      pcon->p_bdb_so->penv = NULL;
      pcon->p_bdb_so->loaded = 0;
      pcon->p_bdb_so->no_connections = 0;
      pcon->p_bdb_so->multiple_connections = 0;
      pcon->p_zv = &(pcon->p_bdb_so->zv);
   }

   if (pcon->p_bdb_so->loaded == 2) {
      strcpy(pcon->error, "Cannot create multiple connections to the database");
      pcon->error_code = 1009; 
      strcpy((char *) pmeth->output_val.svalue.buf_addr, "0");
      rc = CACHE_NOCON;
      goto bdb_open_exit;
   }

   if (!pcon->p_bdb_so->loaded) {
      rc = bdb_load_library(pcon);
      if (rc != CACHE_SUCCESS) {
         goto bdb_open_exit;
      }
   }

   pver = pcon->p_bdb_so->p_db_full_version(&(pcon->p_zv->family), &(pcon->p_zv->release), &(pcon->p_zv->majorversion), &(pcon->p_zv->minorversion), &(pcon->p_zv->patch));

   pcon->p_zv->dbtype = (unsigned char) pcon->dbtype;
/*
   printf("\r\n pver=%s; pcon->p_zv->release=%d; pcon->p_zv->majorversion=%d; pcon->p_zv->minorversion=%d;", pver, pcon->p_zv->release, pcon->p_zv->majorversion, pcon->p_zv->minorversion);
*/
   if (pver) {
      rc = CACHE_SUCCESS;
      strcpy(pcon->p_zv->db_version, pver);
      bdb_parse_zv(pcon->p_zv->db_version, pcon->p_zv);
      if (pcon->p_zv->dbx_build)
         sprintf(pcon->p_zv->version, "%d.%d.b%d", pcon->p_zv->majorversion, pcon->p_zv->minorversion, pcon->p_zv->dbx_build);
      else
         sprintf(pcon->p_zv->version, "%d.%d", pcon->p_zv->majorversion, pcon->p_zv->minorversion);
   }
   else {
      rc = CACHE_FAILURE;
   }

   /* v1.1.4 */
   pcon->p_bdb_so->penv = NULL;
   if (pcon->env_dir[0]) {
      rc = pcon->p_bdb_so->p_db_env_create(&(pcon->p_bdb_so->penv), 0);
      if (rc != 0) {
         /* Error handling goes here */
         strcpy(pcon->error, "Cannot create a BDB environment object");
         goto bdb_open_exit;
      }

      /* Open the environment. */
      env_flags = DB_CREATE | DB_INIT_CDB| DB_INIT_MPOOL; /* Initialize the in-memory cache. */

      rc = pcon->p_bdb_so->penv->open(pcon->p_bdb_so->penv, pcon->env_dir, env_flags, 0);
      if (rc != 0) {
         /* Error handling goes here */
         strcpy(pcon->error, "Cannot create or open a BDB environment");
         goto bdb_open_exit;
      }
   }

   rc = pcon->p_bdb_so->p_db_create(&(pcon->p_bdb_so->pdb), pcon->p_bdb_so->penv, 0);
   if (rc != 0) {
      /* Error handling goes here */
      strcpy(pcon->error, "Cannot create a BDB object");
      goto bdb_open_exit;
   }

   /* Database open flags */
   db_flags = DB_CREATE; /* If the database does not exist, create it.*/
   /* open the database */
   rc = pcon->p_bdb_so->pdb->open(pcon->p_bdb_so->pdb, /* DB structure pointer */
      NULL, /* Transaction pointer */
      pcon->db_file, /* On-disk file that holds the database. */
      NULL, /* Optional logical database name */
      DB_BTREE, /* Database access method */
      db_flags, /* Open flags */
      0); /* File mode (using defaults) */
   if (rc != 0) {
      /* Error handling goes here */
      strcpy(pcon->error, "Cannot create or open a BDB database");
      goto bdb_open_exit;
   }

bdb_open_exit:

   if (rc == CACHE_SUCCESS) {
      dbx_create_string(&(pmeth->output_val.svalue), (void *) &rc, DBX_DTYPE_INT);
   }
   else {
      dbx_create_string(&(pmeth->output_val.svalue), (void *) pcon->error, DBX_DTYPE_STR8);
   }

   return rc;
}


int bdb_parse_zv(char *zv, DBXZV * p_bdb_sv)
{
   char *p;

   p_bdb_sv->dbx_build = p_bdb_sv->patch;
   p_bdb_sv->vnumber = 0;

   if (p_bdb_sv->majorversion == 0 || p_bdb_sv->minorversion == 0) {
      p = strstr(zv, "version ");
      if (!p) {
         p = strstr(zv, "Release ");
      }
      if (p) {
         p += 8;
         p_bdb_sv->majorversion = (int) strtol(p, NULL, 10);
         p = strstr(p, ".");
         if (p) {
            p ++;
            p_bdb_sv->minorversion = (int) strtol(p, NULL, 10);
            p = strstr(p, ".");
            if (p) {
               p ++;
               p_bdb_sv->dbx_build = (int) strtol(p, NULL, 10);
            }
         }
      }
   }

   p_bdb_sv->vnumber = ((p_bdb_sv->majorversion * 100000) + (p_bdb_sv->minorversion * 10000) + p_bdb_sv->dbx_build);
   
   return CACHE_SUCCESS;
}


int bdb_next(DBXMETH *pmeth, DBXKEY *pkey, DBXVAL *pkeyval, DBXVAL *pdataval, int context)
{
   int rc, n, mkeyn, fixed_comp;
   DBXCON *pcon = pmeth->pcon;
   DBT key, key0, data;
   DBC *pcursor;
   DBXVAL mkeys[DBX_MAXARGS];

/*
   printf("\r\n ******* bdb_next ******* argn=%d; pkeyval->svalue.buf_addr=%p; pkey->ibuffer=%p;\r\n", pkey->argc, pkeyval->svalue.buf_addr, pkey->ibuffer);
*/

   memset(&key, 0, sizeof(DBT));
   memset(&key0, 0, sizeof(DBT));
   memset(&data, 0, sizeof(DBT));
   key.flags = DB_DBT_USERMEM;
   key0.flags = DB_DBT_USERMEM;
   data.flags = DB_DBT_USERMEM;

   if (context == 0) {
      rc = pcon->p_bdb_so->pdb->cursor(pcon->p_bdb_so->pdb, NULL, &pcursor, 0);
   }
   else {
     pcursor = pmeth->pbdbcursor;
   }

   if (pcon->key_type == DBX_KEYTYPE_INT) {
      key0.data = &(pkey->args[0].num.int32);
      key0.size = sizeof(pkey->args[0].num.int32);
      key0.ulen = sizeof(pkey->args[0].num.int32);

      pkeyval->num.int32 = pkey->args[0].num.int32;
      key.data = &(pkeyval->num.int32);
      key.size = sizeof(pkeyval->num.int32);
      key.ulen = sizeof(pkeyval->num.int32);
      if (pkey->args[0].svalue.len_used == 0) {
         key.size = 0;
         key0.size = 0;
      }
   }
   else if (pcon->key_type == DBX_KEYTYPE_STR) {
      key0.data = pkey->args[0].svalue.buf_addr;
      key0.size = pkey->args[0].svalue.len_used;
      key0.ulen = pkey->args[0].svalue.len_alloc;

      memcpy((void *) pkeyval->svalue.buf_addr, (void *) pkey->args[0].svalue.buf_addr, (size_t) pkey->args[0].svalue.len_used);
      pkeyval->svalue.len_used = pkey->args[0].svalue.len_used;
      key.data = (void *) pkeyval->svalue.buf_addr;
      key.size = (u_int32_t) pkeyval->svalue.len_used;
      key.ulen = (u_int32_t) pkeyval->svalue.len_alloc;
   }
   else { /* mumps */
      key0.data = (void *) pkey->ibuffer;
      key0.size = (u_int32_t) pkey->ibuffer_used;
      key0.ulen = (u_int32_t) pkey->ibuffer_size;

      memcpy((void *) pkeyval->svalue.buf_addr, (void *) pkey->ibuffer, (size_t) pkey->ibuffer_used);
      pkeyval->svalue.len_used = pkey->ibuffer_used;
      key.data = (void *) pkeyval->svalue.buf_addr;
      key.size = (u_int32_t) pkeyval->svalue.len_used;
      key.ulen = (u_int32_t) pkeyval->svalue.len_alloc;
   }

   data.data = (void *) pdataval->svalue.buf_addr;
   data.ulen = (u_int32_t) pdataval->svalue.len_alloc;

   rc = YDB_NODE_END;
   if (pcon->key_type == DBX_KEYTYPE_M) {
/*
      printf("\r\nstarting from ...");
      dbx_dump_key((char *) key0.data, (int) key0.size);
*/
      pkeyval->svalue.len_used = 0;
      fixed_comp = 0;

      for (n = 0; n < 10; n ++) {
/*
         printf("\r\nDB_SET_RANGE (seed): fixed_comp=%d; pkey->argc=%d; key.size=%d", fixed_comp, pkey->argc, key.size);
         dbx_dump_key((char *) key.data, (int) key.size);
*/
         rc = pcursor->get(pcursor, &key, &data, DB_SET_RANGE);
/*
         printf("\r\nDB_SET_RANGE: rc=%d; pkey->argc=%d; key.size=%d", rc, pkey->argc, key.size);
         dbx_dump_key((char *) key.data, (int) key.size);
*/
         if (rc != CACHE_SUCCESS) {
            rc = YDB_NODE_END;
            break;
         }
         if (*((unsigned char *) key.data) != 0x00) {
            rc = YDB_NODE_END;
            break;
         }

         /* dbx_dump_key((char *) key.data, (int) key.size); */

         if (pkey->argc < 2)
            fixed_comp = 0;
         else
            fixed_comp = bdb_key_compare(&key, &key0, pkey->args[pkey->argc - 2].csize, pcon->key_type);

         if (!fixed_comp) {
            mkeyn = dbx_split_key(&mkeys[0], (char *) key.data, (int) key.size);
/*
            printf("\r\nNext Record: pkey->argc=%d mkeyn=%d csize=%d; lens=%d:%d %s", pkey->argc, mkeyn, pkey->args[pkey->argc - 1].csize, pkey->args[pkey->argc - 1].svalue.len_used, mkeys[pkey->argc - 1].svalue.len_used, mkeys[pkey->argc - 1].svalue.buf_addr);
*/

            if (mkeyn != pkey->argc) { /* can't use data as it's under lower subscripts */
               data.size = 0;
            }

            if (mkeyn >= pkey->argc) {
               if (pkey->args[pkey->argc - 1].svalue.len_used == mkeys[pkey->argc - 1].svalue.len_used && !strncmp(pkey->args[pkey->argc - 1].svalue.buf_addr, mkeys[pkey->argc - 1].svalue.buf_addr, pkey->args[pkey->argc - 1].svalue.len_used)) {
                  /* current key returned - get next */
                  *(((unsigned char *) key.data) + pkey->args[pkey->argc - 1].csize + 0) = 0x00;
                  *(((unsigned char *) key.data) + pkey->args[pkey->argc - 1].csize + 1) = 0xff;

                  key.size = (pkey->args[pkey->argc - 1].csize + 2);
/*
                  printf("\r\nAdvance Cursor: pkey->argc=%d; key.size=%d", pkey->argc, key.size);
                  dbx_dump_key((char *) key.data, (int) key.size);
*/
                  continue;
               }
               else {
                  /* next key found */
                  if (context == 0) {
                     dbx_memcpy((void *) pkeyval->svalue.buf_addr, (void *) mkeys[pkey->argc - 1].svalue.buf_addr, (size_t) mkeys[pkey->argc - 1].svalue.len_alloc);
                     pkeyval->svalue.len_used = mkeys[pkey->argc - 1].svalue.len_alloc;
                  }
                  else {
                     dbx_memcpy((void *) pkey->ibuffer, (void *) key.data, (size_t) mkeys[pkey->argc - 1].csize);
                     pkey->ibuffer_used = mkeys[pkey->argc - 1].csize;
                     pkey->argc = dbx_split_key(&(pkey->args[0]), (char *) pkey->ibuffer, (int) pkey->ibuffer_used);
                     dbx_memcpy((void *) pkeyval->svalue.buf_addr, (void *) pkey->args[pkey->argc - 1].svalue.buf_addr, (size_t) pkey->args[pkey->argc - 1].svalue.len_alloc);
                     pkeyval->svalue.len_used = pkey->args[pkey->argc - 1].svalue.len_alloc;
                  }
                  pdataval->svalue.len_used = (unsigned int) data.size;
                  break;
               }
            }
         }
         else {
            rc = YDB_NODE_END;
            break;
         }
      }
   }
   else { /* not mumps */
/*
      printf("\r\nstarting from ...");
      dbx_dump_key((char *) key.data, (int) key.size);
*/

      if (key.size == 0) {
         rc = pcursor->get(pcursor, &key, &data, DB_FIRST);

         if (rc == CACHE_SUCCESS)
            pkeyval->svalue.len_used = key.size;
         else {
            pkeyval->svalue.len_used = 0;
            rc = YDB_NODE_END;
         }
      }
      else {
         rc = pcursor->get(pcursor, &key, &data, DB_SET_RANGE);
/*
         printf("\r\nAdvance Cursor (DB_SET_RANGE): rc=%d; key.size=%d", rc, key.size);
         dbx_dump_key((char *) key.data, (int) key.size);
*/
         if (rc == CACHE_SUCCESS) {
            pkeyval->svalue.len_used = key.size;
            if (!bdb_key_compare(&key, &key0, 0, pcon->key_type)) {
               rc = pcursor->get(pcursor, &key, &data, DB_NEXT);
/*
               printf("\r\nAdvance Cursor (DB_NEXT): rc=%d; key.size=%d", rc, key.size);
               dbx_dump_key((char *) key.data, (int) key.size);
*/
               if (rc == CACHE_SUCCESS)
                  pkeyval->svalue.len_used = key.size;
               else {
                  pkeyval->svalue.len_used = 0;
                  rc = YDB_NODE_END;
               }
            }
            else {
               pkeyval->svalue.len_used = key.size;
            }
         }
         else {
            pkeyval->svalue.len_used = 0;
            rc = YDB_NODE_END;
         }
      }
      if (rc == CACHE_SUCCESS && pcon->key_type == DBX_KEYTYPE_INT) {
         sprintf(pkeyval->svalue.buf_addr, "%d", pkeyval->num.int32);
         pkeyval->svalue.len_used = (unsigned int) strlen(pkeyval->svalue.buf_addr);
         pkeyval->type = DBX_DTYPE_INT;
      }
   }

   if (context == 0) {
      pcursor->close(pcursor); 
   }

   if (rc != CACHE_SUCCESS) {
      pkeyval->svalue.len_used = 0;
   }
   if (pkeyval->svalue.len_used == 0) {
      rc = YDB_NODE_END;
   }

   return rc;
}


int bdb_previous(DBXMETH *pmeth, DBXKEY *pkey, DBXVAL *pkeyval, DBXVAL *pdataval, int context)
{
   int rc, mkeyn, fixed_comp;
   DBXCON *pcon = pmeth->pcon;
   DBT key, key0, data;
   DBC *pcursor;
   DBXVAL mkeys[DBX_MAXARGS];
/*
   printf("\r\n ******* bdb_previous ******* argn=%d; pkeyval->svalue.buf_addr=%p; pkey->ibuffer=%p; seed_len%d\r\n", pkey->argc, pkeyval->svalue.buf_addr, pkey->ibuffer, pkey->args[pkey->argc - 1].svalue.len_used);
*/

   memset(&key, 0, sizeof(DBT));
   memset(&key0, 0, sizeof(DBT));
   memset(&data, 0, sizeof(DBT));
   key.flags = DB_DBT_USERMEM;
   key0.flags = DB_DBT_USERMEM;
   data.flags = DB_DBT_USERMEM;

   if (context == 0) {
      rc = pcon->p_bdb_so->pdb->cursor(pcon->p_bdb_so->pdb, NULL, &pcursor, 0);
   }
   else {
     pcursor = pmeth->pbdbcursor;
   }

   if (pcon->key_type == DBX_KEYTYPE_INT) {
      key0.data = &(pkey->args[0].num.int32);
      key0.size = sizeof(pkey->args[0].num.int32);
      key0.ulen = sizeof(pkey->args[0].num.int32);

      pkeyval->num.int32 = pkey->args[0].num.int32;
      key.data = &(pkeyval->num.int32);
      key.size = sizeof(pkeyval->num.int32);
      key.ulen = sizeof(pkeyval->num.int32);
      if (pkey->args[0].svalue.len_used == 0) {
         key.size = 0;
         key0.size = 0;
      }
   }
   else if (pcon->key_type == DBX_KEYTYPE_STR) {
      key0.data = pkey->args[0].svalue.buf_addr;
      key0.size = pkey->args[0].svalue.len_used;
      key0.ulen = pkey->args[0].svalue.len_alloc;

      memcpy((void *) pkeyval->svalue.buf_addr, (void *) pkey->args[0].svalue.buf_addr, (size_t) pkey->args[0].svalue.len_used);
      pkeyval->svalue.len_used = pkey->args[0].svalue.len_used;
      key.data = (void *) pkeyval->svalue.buf_addr;
      key.size = (u_int32_t) pkeyval->svalue.len_used;
      key.ulen = (u_int32_t) pkeyval->svalue.len_alloc;
   }
   else { /* mumps */
      key0.data = (void *) pkey->ibuffer;
      key0.size = (u_int32_t) pkey->ibuffer_used;
      key0.ulen = (u_int32_t) pkey->ibuffer_size;

      memcpy((void *) pkeyval->svalue.buf_addr, (void *) pkey->ibuffer, (size_t) pkey->ibuffer_used);
      pkeyval->svalue.len_used = pkey->ibuffer_used;
      key.data = (void *) pkeyval->svalue.buf_addr;
      key.size = (u_int32_t) pkeyval->svalue.len_used;
      key.ulen = (u_int32_t) pkeyval->svalue.len_alloc;
   }

   data.data = (void *) pdataval->svalue.buf_addr;
   data.ulen = (u_int32_t) pdataval->svalue.len_alloc;

   fixed_comp = 0;
   rc = YDB_NODE_END;
   if (pcon->key_type == DBX_KEYTYPE_M) {
/*
      printf("\r\n seed ... pkey->ibuffer_used=%d", pkey->ibuffer_used);
      dbx_dump_key((char *) key0.data, (int) key0.size);
*/
      pkeyval->svalue.len_used = 0;
      if (pkey->argc < 2 && pkey->args[pkey->argc - 1].svalue.len_used == 0) { /* null seed - special case */

         rc = pcursor->get(pcursor, &key, &data, DB_LAST);
/*
         printf("\r\n DB_LAST rc=%d; argc=%d; key.size=%d", rc, pkey->argc, (int) key.size);
         dbx_dump_key((char *) key.data, (int) key.size);
*/
      }
      else {

         if (pkey->args[pkey->argc - 1].svalue.len_used == 0) {
            *(((unsigned char *) key.data) + pkey->args[pkey->argc - 1].csize - 1) = 0xff;
         }

         rc = pcursor->get(pcursor, &key, &data, DB_SET_RANGE);
/*
         printf("\r\n DB_SET_RANGE rc=%d; argc=%d; key.size=%d", rc, pkey->argc, (int) key.size);
         dbx_dump_key((char *) key.data, (int) key.size);
*/
         if (rc == CACHE_SUCCESS) {
            rc = pcursor->get(pcursor, &key, &data, DB_PREV);

            if (pkey->argc < 2)
               fixed_comp = 0;
            else
               fixed_comp = bdb_key_compare(&key, &key0, pkey->args[pkey->argc - 2].csize, pcon->key_type);
/*
            printf("\r\n DB_PREV rc=%d; argc=%d; key.size=%d; fixed_comp=%d;", rc, pkey->argc, (int) key.size, fixed_comp);
            dbx_dump_key((char *) key.data, (int) key.size);
*/
         }
         else { /* v1.2.7 */
            /* printf("\r\n probably the last global in the database ..."); */
            rc = pcursor->get(pcursor, &key, &data, DB_LAST);
            if (pkey->argc < 2)
               fixed_comp = 0;
            else
               fixed_comp = bdb_key_compare(&key, &key0, pkey->args[pkey->argc - 2].csize, pcon->key_type);
/*
            printf("\r\n DB_LAST rc=%d; argc=%d; key.size=%d; fixed_comp=%d;", rc, pkey->argc, (int) key.size, fixed_comp);
            dbx_dump_key((char *) key.data, (int) key.size);
*/
         }
      }
      if (rc == CACHE_SUCCESS && !fixed_comp) {
         mkeyn = dbx_split_key(&mkeys[0], (char *) key.data, (int) key.size);

         if (context == 0) {
            dbx_memcpy((void *) pkeyval->svalue.buf_addr,  (void *) mkeys[pmeth->key.argc - 1].svalue.buf_addr,  (size_t) mkeys[pmeth->key.argc - 1].svalue.len_alloc);
            pkeyval->svalue.len_used = mkeys[pmeth->key.argc - 1].svalue.len_alloc;
         }
         else {
            dbx_memcpy((void *) pkey->ibuffer, (void *) key.data, (size_t) mkeys[pkey->argc - 1].csize);
            pkey->ibuffer_used = mkeys[pkey->argc - 1].csize;
            pkey->argc = dbx_split_key(&(pkey->args[0]), (char *) pkey->ibuffer, (int) pkey->ibuffer_used);
            dbx_memcpy((void *) pkeyval->svalue.buf_addr, (void *) pkey->args[pkey->argc - 1].svalue.buf_addr, (size_t) pkey->args[pkey->argc - 1].svalue.len_alloc);
            pkeyval->svalue.len_used = pkey->args[pkey->argc - 1].svalue.len_alloc;
         }
         pdataval->svalue.len_used = (unsigned int) data.size;

         if (mkeyn != pkey->argc) { /* can't use data as it's under lower subscripts */
            data.size = 0;
            pdataval->svalue.len_used = (unsigned int) data.size;
         }

      }
      else {
         pkeyval->svalue.len_used = 0;
         rc = YDB_NODE_END;
      }
   }
   else {
      if (key.size == 0) {
         rc = pcursor->get(pcursor, &key, &data, DB_LAST);
         if (rc == CACHE_SUCCESS)
            pkeyval->svalue.len_used = key.size;
         else {
            pkeyval->svalue.len_used = 0;
            rc = YDB_NODE_END;
         }
      }
      else {
         rc = pcursor->get(pcursor, &key, &data, DB_SET_RANGE);
         if (rc == CACHE_SUCCESS) {
            rc = pcursor->get(pcursor, &key, &data, DB_PREV);
            if (rc == CACHE_SUCCESS)
               pkeyval->svalue.len_used = key.size;
            else {
               pkeyval->svalue.len_used = 0;
               rc = YDB_NODE_END;
            }
         }
         else {
            rc = pcursor->get(pcursor, &key, &data, DB_LAST);
            if (rc == CACHE_SUCCESS)
               pkeyval->svalue.len_used = key.size;
            else {
               pkeyval->svalue.len_used = 0;
               rc = YDB_NODE_END;
            }
         }
      }
      if (rc == CACHE_SUCCESS && pcon->key_type == DBX_KEYTYPE_INT) {
         sprintf(pkeyval->svalue.buf_addr, "%d", pkeyval->num.int32);
         pkeyval->svalue.len_used = (unsigned int) strlen(pkeyval->svalue.buf_addr);
         pkeyval->type = DBX_DTYPE_INT;
      }
   }

   if (context == 0) {
      pcursor->close(pcursor); 
   }

   if (rc != CACHE_SUCCESS) {
      pkeyval->svalue.len_used = 0;
   }

   return rc;
}


int bdb_key_compare(DBT *key1, DBT *key2, int compare_max, short keytype)
{
   int n, n1, n2, rc;
   char *c1, *c2;

   c1 = (char *) key1->data;
   c2 = (char *) key2->data;
   if (keytype == DBX_KEYTYPE_M) {
      if (compare_max > 0) {
         if (key1->size < (u_int32_t) compare_max || key2->size < (u_int32_t) compare_max) {
            return -1;
         }
         if ((int) key1->size > compare_max && c1[compare_max] != '\0') {
            return -1;
         }
         if ((int) key2->size > compare_max && c2[compare_max] != '\0') {
            return -1;
         }
      }
      else {
         if (key1->size == 0) {
            return -1;
         }
         if (key1->size != key2->size) {
            return -1;
         }
         compare_max = key1->size;
      }
   }
   else {
      if (key1->size == 0) {
         return -1;
      }
      if (key1->size != key2->size) {
         return -1;
      }

      if (keytype == DBX_KEYTYPE_INT) {
         n1 = *((int *) key1->data);
         n2 = *((int *) key2->data);
         if (n1 == n2)
            return 0;
         else
            return -1;
      }
      compare_max = key1->size;
   }

   rc = 0;
   for (n = 0; n < (int) compare_max; n ++) {
      if (c1[n] != c2[n]) {
         rc = -1;
         break;
      }
   }
   return rc;
}


int bdb_error_message(DBXCON *pcon, int error_code)
{
   sprintf(pcon->error, "Berkeley DB error code: %d", error_code);
   return 0;
}


int bdb_error(DBXCON *pcon, int error_code)
{
   T_STRCPY(pcon->error, _dbxso(pcon->error), "General BDB Error");

   return 1;
}


int lmdb_load_library(DBXCON *pcon)
{
   int result;
   char primerr[DBX_ERROR_SIZE];
   char fun[64];

   strcpy(pcon->p_lmdb_so->libdir, pcon->db_library);
   strcpy(pcon->p_lmdb_so->funprfx, "mdb");
   strcpy(pcon->p_lmdb_so->dbname, "LMDB");

   strcpy(pcon->p_lmdb_so->libnam, pcon->p_lmdb_so->libdir);

   pcon->p_lmdb_so->p_library = dbx_dso_load(pcon->p_lmdb_so->libnam);
   if (!pcon->p_lmdb_so->p_library) {
      int len1, len2;
      char *p;
#if defined(_WIN32)
      DWORD errorcode;
      LPVOID lpMsgBuf;

      lpMsgBuf = NULL;
      errorcode = GetLastError();
      sprintf(pcon->error, "Error loading %s Library: %s; Error Code : %ld",  pcon->p_lmdb_so->dbname, pcon->p_lmdb_so->libnam, errorcode);
      len2 = (int) strlen(pcon->error);
      len1 = FormatMessage(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
                     NULL,
                     errorcode,
                     /* MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), */
                     MAKELANGID(LANG_ENGLISH, SUBLANG_ENGLISH_US),
                     (LPTSTR) &lpMsgBuf,
                     0,
                     NULL 
                     );
      if (lpMsgBuf && len1 > 0 && (DBX_ERROR_SIZE - len2) > 30) {
         strncpy(primerr, (const char *) lpMsgBuf, DBX_ERROR_SIZE - 1);
         p = strstr(primerr, "\r\n");
         if (p)
            *p = '\0';
         len1 = (DBX_ERROR_SIZE - (len2 + 10));
         if (len1 < 1)
            len1 = 0;
         primerr[len1] = '\0';
         p = strstr(primerr, "%1");
         if (p) {
            *p = 'I';
            *(p + 1) = 't';
         }
         strcat(pcon->error, " (");
         strcat(pcon->error, primerr);
         strcat(pcon->error, ")");
      }
      if (lpMsgBuf)
         LocalFree(lpMsgBuf);
#else
      p = (char *) dlerror();
      sprintf(primerr, "Cannot load %s library: Error Code: %d", pcon->p_lmdb_so->dbname, errno);
      len2 = strlen(pcon->error);
      if (p) {
         strncpy(primerr, p, DBX_ERROR_SIZE - 1);
         primerr[DBX_ERROR_SIZE - 1] = '\0';
         len1 = (DBX_ERROR_SIZE - (len2 + 10));
         if (len1 < 1)
            len1 = 0;
         primerr[len1] = '\0';
         strcat(pcon->error, " (");
         strcat(pcon->error, primerr);
         strcat(pcon->error, ")");
      }
#endif
      goto lmdb_load_library_exit;
   }

   sprintf(fun, "%s_env_create", pcon->p_lmdb_so->funprfx);
   pcon->p_lmdb_so->p_mdb_env_create = (int (*) (MDB_env **)) dbx_dso_sym(pcon->p_lmdb_so->p_library, (char *) fun);
   if (!pcon->p_lmdb_so->p_mdb_env_create) {
      sprintf(pcon->error, "Error loading %s library: %s; Cannot locate the following function : %s", pcon->p_lmdb_so->dbname, pcon->p_lmdb_so->libnam, fun);
      goto lmdb_load_library_exit;
   }
   sprintf(fun, "%s_env_open", pcon->p_lmdb_so->funprfx);
   pcon->p_lmdb_so->p_mdb_env_open = (int (*) (MDB_env *, const char *, unsigned int, mdb_mode_t)) dbx_dso_sym(pcon->p_lmdb_so->p_library, (char *) fun);
   if (!pcon->p_lmdb_so->p_mdb_env_open) {
      sprintf(pcon->error, "Error loading %s library: %s; Cannot locate the following function : %s", pcon->p_lmdb_so->dbname, pcon->p_lmdb_so->libnam, fun);
      goto lmdb_load_library_exit;
   }
   sprintf(fun, "%s_env_close", pcon->p_lmdb_so->funprfx);
   pcon->p_lmdb_so->p_mdb_env_close = (void (*) (MDB_env *)) dbx_dso_sym(pcon->p_lmdb_so->p_library, (char *) fun);
   if (!pcon->p_lmdb_so->p_mdb_env_close) {
      sprintf(pcon->error, "Error loading %s library: %s; Cannot locate the following function : %s", pcon->p_lmdb_so->dbname, pcon->p_lmdb_so->libnam, fun);
      goto lmdb_load_library_exit;
   }
   sprintf(fun, "%s_env_set_maxdbs", pcon->p_lmdb_so->funprfx);
   pcon->p_lmdb_so->p_mdb_env_set_maxdbs = (int (*) (MDB_env *, MDB_dbi)) dbx_dso_sym(pcon->p_lmdb_so->p_library, (char *) fun);
   if (!pcon->p_lmdb_so->p_mdb_env_set_maxdbs) {
      sprintf(pcon->error, "Error loading %s library: %s; Cannot locate the following function : %s", pcon->p_lmdb_so->dbname, pcon->p_lmdb_so->libnam, fun);
      goto lmdb_load_library_exit;
   }

   sprintf(fun, "%s_txn_begin", pcon->p_lmdb_so->funprfx);
   pcon->p_lmdb_so->p_mdb_txn_begin = (int (*) (MDB_env *, MDB_txn *, unsigned int, MDB_txn **)) dbx_dso_sym(pcon->p_lmdb_so->p_library, (char *) fun);
   if (!pcon->p_lmdb_so->p_mdb_txn_begin) {
      sprintf(pcon->error, "Error loading %s library: %s; Cannot locate the following function : %s", pcon->p_lmdb_so->dbname, pcon->p_lmdb_so->libnam, fun);
      goto lmdb_load_library_exit;
   }
   sprintf(fun, "%s_txn_commit", pcon->p_lmdb_so->funprfx);
   pcon->p_lmdb_so->p_mdb_txn_commit = (int (*) (MDB_txn *)) dbx_dso_sym(pcon->p_lmdb_so->p_library, (char *) fun);
   if (!pcon->p_lmdb_so->p_mdb_txn_commit) {
      sprintf(pcon->error, "Error loading %s library: %s; Cannot locate the following function : %s", pcon->p_lmdb_so->dbname, pcon->p_lmdb_so->libnam, fun);
      goto lmdb_load_library_exit;
   }
   sprintf(fun, "%s_txn_abort", pcon->p_lmdb_so->funprfx);
   pcon->p_lmdb_so->p_mdb_txn_abort = (void (*) (MDB_txn *)) dbx_dso_sym(pcon->p_lmdb_so->p_library, (char *) fun);
   if (!pcon->p_lmdb_so->p_mdb_txn_abort) {
      sprintf(pcon->error, "Error loading %s library: %s; Cannot locate the following function : %s", pcon->p_lmdb_so->dbname, pcon->p_lmdb_so->libnam, fun);
      goto lmdb_load_library_exit;
   }
   sprintf(fun, "%s_txn_reset", pcon->p_lmdb_so->funprfx);
   pcon->p_lmdb_so->p_mdb_txn_reset = (void (*) (MDB_txn *)) dbx_dso_sym(pcon->p_lmdb_so->p_library, (char *) fun);
   if (!pcon->p_lmdb_so->p_mdb_txn_reset) {
      sprintf(pcon->error, "Error loading %s library: %s; Cannot locate the following function : %s", pcon->p_lmdb_so->dbname, pcon->p_lmdb_so->libnam, fun);
      goto lmdb_load_library_exit;
   }
   sprintf(fun, "%s_txn_renew", pcon->p_lmdb_so->funprfx);
   pcon->p_lmdb_so->p_mdb_txn_renew = (void (*) (MDB_txn *)) dbx_dso_sym(pcon->p_lmdb_so->p_library, (char *) fun);
   if (!pcon->p_lmdb_so->p_mdb_txn_renew) {
      sprintf(pcon->error, "Error loading %s library: %s; Cannot locate the following function : %s", pcon->p_lmdb_so->dbname, pcon->p_lmdb_so->libnam, fun);
      goto lmdb_load_library_exit;
   }

   sprintf(fun, "%s_dbi_open", pcon->p_lmdb_so->funprfx);
   pcon->p_lmdb_so->p_mdb_dbi_open = (int (*) (MDB_txn *, const char *, unsigned int, MDB_dbi *)) dbx_dso_sym(pcon->p_lmdb_so->p_library, (char *) fun);
   if (!pcon->p_lmdb_so->p_mdb_dbi_open) {
      sprintf(pcon->error, "Error loading %s library: %s; Cannot locate the following function : %s", pcon->p_lmdb_so->dbname, pcon->p_lmdb_so->libnam, fun);
      goto lmdb_load_library_exit;
   }
   sprintf(fun, "%s_dbi_close", pcon->p_lmdb_so->funprfx);
   pcon->p_lmdb_so->p_mdb_dbi_close = (void (*) (MDB_env *, MDB_dbi)) dbx_dso_sym(pcon->p_lmdb_so->p_library, (char *) fun);
   if (!pcon->p_lmdb_so->p_mdb_dbi_close) {
      sprintf(pcon->error, "Error loading %s library: %s; Cannot locate the following function : %s", pcon->p_lmdb_so->dbname, pcon->p_lmdb_so->libnam, fun);
      goto lmdb_load_library_exit;
   }

   sprintf(fun, "%s_put", pcon->p_lmdb_so->funprfx);
   pcon->p_lmdb_so->p_mdb_put = (int (*) (MDB_txn *, MDB_dbi, MDB_val *, MDB_val *, unsigned int)) dbx_dso_sym(pcon->p_lmdb_so->p_library, (char *) fun);
   if (!pcon->p_lmdb_so->p_mdb_put) {
      sprintf(pcon->error, "Error loading %s library: %s; Cannot locate the following function : %s", pcon->p_lmdb_so->dbname, pcon->p_lmdb_so->libnam, fun);
      goto lmdb_load_library_exit;
   }
   sprintf(fun, "%s_get", pcon->p_lmdb_so->funprfx);
   pcon->p_lmdb_so->p_mdb_get = (int (*) (MDB_txn *, MDB_dbi, MDB_val *, MDB_val *)) dbx_dso_sym(pcon->p_lmdb_so->p_library, (char *) fun);
   if (!pcon->p_lmdb_so->p_mdb_get) {
      sprintf(pcon->error, "Error loading %s library: %s; Cannot locate the following function : %s", pcon->p_lmdb_so->dbname, pcon->p_lmdb_so->libnam, fun);
      goto lmdb_load_library_exit;
   }
   sprintf(fun, "%s_del", pcon->p_lmdb_so->funprfx);
   pcon->p_lmdb_so->p_mdb_del = (int (*) (MDB_txn *, MDB_dbi, MDB_val *, MDB_val *)) dbx_dso_sym(pcon->p_lmdb_so->p_library, (char *) fun);
   if (!pcon->p_lmdb_so->p_mdb_del) {
      sprintf(pcon->error, "Error loading %s library: %s; Cannot locate the following function : %s", pcon->p_lmdb_so->dbname, pcon->p_lmdb_so->libnam, fun);
      goto lmdb_load_library_exit;
   }

   sprintf(fun, "%s_cursor_open", pcon->p_lmdb_so->funprfx);
   pcon->p_lmdb_so->p_mdb_cursor_open = (int (*) (MDB_txn *, MDB_dbi, MDB_cursor **)) dbx_dso_sym(pcon->p_lmdb_so->p_library, (char *) fun);
   if (!pcon->p_lmdb_so->p_mdb_cursor_open) {
      sprintf(pcon->error, "Error loading %s library: %s; Cannot locate the following function : %s", pcon->p_lmdb_so->dbname, pcon->p_lmdb_so->libnam, fun);
      goto lmdb_load_library_exit;
   }
   sprintf(fun, "%s_cursor_close", pcon->p_lmdb_so->funprfx);
   pcon->p_lmdb_so->p_mdb_cursor_close = (void (*) (MDB_cursor *)) dbx_dso_sym(pcon->p_lmdb_so->p_library, (char *) fun);
   if (!pcon->p_lmdb_so->p_mdb_cursor_close) {
      sprintf(pcon->error, "Error loading %s library: %s; Cannot locate the following function : %s", pcon->p_lmdb_so->dbname, pcon->p_lmdb_so->libnam, fun);
      goto lmdb_load_library_exit;
   }
   sprintf(fun, "%s_cursor_renew", pcon->p_lmdb_so->funprfx);
   pcon->p_lmdb_so->p_mdb_cursor_renew = (int (*) (MDB_txn *, MDB_cursor *)) dbx_dso_sym(pcon->p_lmdb_so->p_library, (char *) fun);
   if (!pcon->p_lmdb_so->p_mdb_cursor_renew) {
      sprintf(pcon->error, "Error loading %s library: %s; Cannot locate the following function : %s", pcon->p_lmdb_so->dbname, pcon->p_lmdb_so->libnam, fun);
      goto lmdb_load_library_exit;
   }
   sprintf(fun, "%s_cursor_get", pcon->p_lmdb_so->funprfx);
   pcon->p_lmdb_so->p_mdb_cursor_get = (int (*) (MDB_cursor *, MDB_val *, MDB_val *, MDB_cursor_op)) dbx_dso_sym(pcon->p_lmdb_so->p_library, (char *) fun);
   if (!pcon->p_lmdb_so->p_mdb_cursor_get) {
      sprintf(pcon->error, "Error loading %s library: %s; Cannot locate the following function : %s", pcon->p_lmdb_so->dbname, pcon->p_lmdb_so->libnam, fun);
      goto lmdb_load_library_exit;
   }

   sprintf(fun, "%s_strerror", pcon->p_lmdb_so->funprfx);
   pcon->p_lmdb_so->p_mdb_strerror = (char * (*) (int)) dbx_dso_sym(pcon->p_lmdb_so->p_library, (char *) fun);
   if (!pcon->p_lmdb_so->p_mdb_strerror) {
      sprintf(pcon->error, "Error loading %s library: %s; Cannot locate the following function : %s", pcon->p_lmdb_so->dbname, pcon->p_lmdb_so->libnam, fun);
      goto lmdb_load_library_exit;
   }
   sprintf(fun, "%s_version", pcon->p_lmdb_so->funprfx);
   pcon->p_lmdb_so->p_mdb_version = (char * (*) (int *, int *, int *)) dbx_dso_sym(pcon->p_lmdb_so->p_library, (char *) fun);
   if (!pcon->p_lmdb_so->p_mdb_version) {
      sprintf(pcon->error, "Error loading %s library: %s; Cannot locate the following function : %s", pcon->p_lmdb_so->dbname, pcon->p_lmdb_so->libnam, fun);
      goto lmdb_load_library_exit;
   }

   pcon->p_lmdb_so->loaded = 1;

lmdb_load_library_exit:

   if (pcon->error[0]) {
      pcon->p_lmdb_so->loaded = 0;
      pcon->error_code = 1009;
      result = CACHE_NOCON;

      return result;
   }

   return CACHE_SUCCESS;
}


int lmdb_open(DBXMETH *pmeth)
{
   int rc, result;
   char *pver;
   DBXCON *pcon = pmeth->pcon;

   if (!pcon->p_lmdb_so) {
      pcon->p_lmdb_so = (DBXLMDBSO *) dbx_malloc(sizeof(DBXLMDBSO), 0);
      if (!pcon->p_lmdb_so) {
         T_STRCPY(pcon->error, _dbxso(pcon->error), "No Memory");
         pcon->error_code = 1009; 
         result = CACHE_NOCON;
         return result;
      }
      memset((void *) pcon->p_lmdb_so, 0, sizeof(DBXLMDBSO));
      pcon->p_lmdb_so->pdb = NULL;
      pcon->p_lmdb_so->penv = NULL;
      pcon->p_lmdb_so->loaded = 0;
      pcon->p_lmdb_so->no_connections = 0;
      pcon->p_lmdb_so->multiple_connections = 0;
      pcon->p_zv = &(pcon->p_lmdb_so->zv);
   }

   if (pcon->p_lmdb_so->loaded == 2) {
      strcpy(pcon->error, "Cannot create multiple connections to the database");
      pcon->error_code = 1009; 
      strcpy((char *) pmeth->output_val.svalue.buf_addr, "0");
      rc = CACHE_NOCON;
      goto lmdb_open_exit;
   }

   if (!pcon->p_lmdb_so->loaded) {
      rc = lmdb_load_library(pcon);
      if (rc != CACHE_SUCCESS) {
         goto lmdb_open_exit;
      }
   }

   pver = pcon->p_lmdb_so->p_mdb_version(&(pcon->p_zv->majorversion), &(pcon->p_zv->minorversion), &(pcon->p_zv->patch));

   pcon->p_zv->dbtype = (unsigned char) pcon->dbtype;
/*
   printf("\r\n pver=%s; pcon->p_zv->majorversion=%d; pcon->p_zv->minorversion=%d; pcon->p_zv->patch=%d;", pver, pcon->p_zv->majorversion, pcon->p_zv->minorversion, pcon->p_zv->patch);
*/
   if (pver) {
      rc = CACHE_SUCCESS;
      strcpy(pcon->p_zv->db_version, pver);
      lmdb_parse_zv(pcon->p_zv->db_version, pcon->p_zv);
      if (pcon->p_zv->dbx_build)
         sprintf(pcon->p_zv->version, "%d.%d.b%d", pcon->p_zv->majorversion, pcon->p_zv->minorversion, pcon->p_zv->dbx_build);
      else
         sprintf(pcon->p_zv->version, "%d.%d", pcon->p_zv->majorversion, pcon->p_zv->minorversion);
   }
   else {
      rc = CACHE_FAILURE;
   }

   /* v1.1.4 */
   pcon->p_lmdb_so->penv = NULL;
   rc = pcon->p_lmdb_so->p_mdb_env_create(&(pcon->p_lmdb_so->penv));
   if (rc != 0) {
      /* Error handling goes here */
      strcpy(pcon->error, "Cannot create a LMDB environment object");
      goto lmdb_open_exit;
   }

   if (pcon->db_file[0]) {
      rc = pcon->p_lmdb_so->p_mdb_env_set_maxdbs(pcon->p_lmdb_so->penv, (MDB_dbi) 16);
      if (rc != 0) {
         /* Error handling goes here */
         strcpy(pcon->error, "Cannot set the maximum number of databases in the LMDB environment");
         goto lmdb_open_exit;
      }
   }

   rc = pcon->p_lmdb_so->p_mdb_env_open(pcon->p_lmdb_so->penv, pcon->env_dir, MDB_NOTLS, 0664);
   if (rc != 0) {
      /* Error handling goes here */
      strcpy(pcon->error, "Cannot create or open a LMDB environment");
      goto lmdb_open_exit;
   }

   pcon->p_lmdb_so->ptxnro = NULL; /* read only transaction */
   rc = pcon->p_lmdb_so->p_mdb_txn_begin(pcon->p_lmdb_so->penv, NULL, 0, &(pcon->p_lmdb_so->ptxn));
   if (rc != 0) {
      /* Error handling goes here */
      strcpy(pcon->error, "Cannot create or open a LMDB transaction");
      goto lmdb_open_exit;
   }
   pcon->tlevel ++;

   pcon->p_lmdb_so->pdb = &(pcon->p_lmdb_so->db);
   if (pcon->db_file[0]) {
      rc = pcon->p_lmdb_so->p_mdb_dbi_open(pcon->p_lmdb_so->ptxn, pcon->db_file, MDB_CREATE, pcon->p_lmdb_so->pdb);
   }
   else {
      rc = pcon->p_lmdb_so->p_mdb_dbi_open(pcon->p_lmdb_so->ptxn, NULL, 0, pcon->p_lmdb_so->pdb);
   }
   if (rc != 0) {
      /* Error handling goes here */
      strcpy(pcon->error, "Cannot create or open a LMDB database");
      goto lmdb_open_exit;
   }
   pcon->p_lmdb_so->p_mdb_txn_commit(pcon->p_lmdb_so->ptxn);
   pcon->tlevel --;

lmdb_open_exit:

   if (rc == CACHE_SUCCESS) {
      dbx_create_string(&(pmeth->output_val.svalue), (void *) &rc, DBX_DTYPE_INT);
   }
   else {
      dbx_create_string(&(pmeth->output_val.svalue), (void *) pcon->error, DBX_DTYPE_STR8);
   }

   return rc;
}


int lmdb_parse_zv(char *zv, DBXZV * p_lmdb_sv)
{
   char *p;

   p_lmdb_sv->dbx_build = p_lmdb_sv->patch;
   p_lmdb_sv->vnumber = 0;

   if (p_lmdb_sv->majorversion == 0 || p_lmdb_sv->minorversion == 0) {
      p = strstr(zv, "LMDB ");
      if (p) {
         p += 5;
      }
      else {
         p = strstr(zv, "version ");
         if (p) {
            p += 8;
         }
      }
      if (p) {
         p_lmdb_sv->majorversion = (int) strtol(p, NULL, 10);
         p = strstr(p, ".");
         if (p) {
            p ++;
            p_lmdb_sv->minorversion = (int) strtol(p, NULL, 10);
            p = strstr(p, ".");
            if (p) {
               p ++;
               p_lmdb_sv->dbx_build = (int) strtol(p, NULL, 10);
            }
         }
      }
   }

   p_lmdb_sv->vnumber = ((p_lmdb_sv->majorversion * 100000) + (p_lmdb_sv->minorversion * 10000) + p_lmdb_sv->dbx_build);
   
   return CACHE_SUCCESS;
}


int lmdb_start_ro_transaction(DBXMETH *pmeth, int context)
{
   int rc;

   rc = 0;
   if (pmeth->pcon->tlevelro) {
      rc = 0;
   }
   else {
      if (pmeth->pcon->tstatusro == 1) {
         pmeth->pcon->p_lmdb_so->p_mdb_txn_renew(pmeth->pcon->p_lmdb_so->ptxnro);
         pmeth->pcon->tstatusro = 0;
      }
      else {
         rc = pmeth->pcon->p_lmdb_so->p_mdb_txn_begin(pmeth->pcon->p_lmdb_so->penv, NULL, MDB_RDONLY, &(pmeth->pcon->p_lmdb_so->ptxnro));
      }
   }
   pmeth->pcon->tlevelro ++;

   return rc;
}


int lmdb_commit_ro_transaction(DBXMETH *pmeth, int context)
{
   int rc;

   rc = 0;

   if (pmeth->pcon->tlevelro > 1) {
      rc = 0;
   }
   else {
      /* pmeth->pcon->p_lmdb_so->p_mdb_txn_abort(pmeth->pcon->p_lmdb_so->ptxnro); */
      pmeth->pcon->p_lmdb_so->p_mdb_txn_reset(pmeth->pcon->p_lmdb_so->ptxnro);
      pmeth->pcon->tstatusro = 1;
   }
   pmeth->pcon->tlevelro --;

   return rc;
}


int lmdb_start_qro_transaction(DBXMETH *pmeth, MDB_txn **ptxn, int context)
{
   int rc;

   rc = lmdb_start_ro_transaction(pmeth, context);
   *ptxn = pmeth->pcon->p_lmdb_so->ptxnro;

   return rc;
}


int lmdb_commit_qro_transaction(DBXMETH *pmeth, MDB_txn **ptxn, int context)
{
   int rc;

   rc = lmdb_commit_ro_transaction(pmeth, context);

   return rc;
}


int lmdb_next(DBXMETH *pmeth, DBXKEY *pkey, DBXVAL *pkeyval, DBXVAL *pdataval, int context)
{
   int rc, n, mkeyn, fixed_comp;
   DBXCON *pcon = pmeth->pcon;
   MDB_val key, key0, data;
   MDB_cursor *pcursor;
   DBXVAL mkeys[DBX_MAXARGS];

/*
   printf("\r\n ******* lmdb_next ******* argn=%d; pkeyval->svalue.buf_addr=%p; pkey->ibuffer=%p;\r\n", pkey->argc, pkeyval->svalue.buf_addr, pkey->ibuffer);
*/

   if (context == 0) {
      rc = lmdb_start_ro_transaction(pmeth, 0);
      rc = pcon->p_lmdb_so->p_mdb_cursor_open(pcon->p_lmdb_so->ptxnro, pcon->p_lmdb_so->db, &pcursor);
   }
   else {
     pcursor = pmeth->plmdbcursor;
   }

   if (pcon->key_type == DBX_KEYTYPE_INT) {
      key0.mv_data = &(pkey->args[0].num.int32);
      key0.mv_size = sizeof(pkey->args[0].num.int32);
      pkeyval->num.int32 = pkey->args[0].num.int32;
      key.mv_data = &(pkeyval->num.int32);
      key.mv_size = sizeof(pkeyval->num.int32);
      if (pkey->args[0].svalue.len_used == 0) {
         key.mv_size = 0;
         key0.mv_size = 0;
      }
   }
   else if (pcon->key_type == DBX_KEYTYPE_STR) {
      key0.mv_data = pkey->args[0].svalue.buf_addr;
      key0.mv_size = pkey->args[0].svalue.len_used;
      memcpy((void *) pkeyval->svalue.buf_addr, (void *) pkey->args[0].svalue.buf_addr, (size_t) pkey->args[0].svalue.len_used);
      pkeyval->svalue.len_used = pkey->args[0].svalue.len_used;
      key.mv_data = (void *) pkeyval->svalue.buf_addr;
      key.mv_size = (size_t) pkeyval->svalue.len_used;
   }
   else { /* mumps */
      key0.mv_data = (void *) pkey->ibuffer;
      key0.mv_size = (size_t) pkey->ibuffer_used;
      memcpy((void *) pkeyval->svalue.buf_addr, (void *) pkey->ibuffer, (size_t) pkey->ibuffer_used);
      pkeyval->svalue.len_used = pkey->ibuffer_used;
      key.mv_data = (void *) pkeyval->svalue.buf_addr;
      key.mv_size = (size_t) pkeyval->svalue.len_used;
   }

   data.mv_data = (void *) pdataval->svalue.buf_addr;
   data.mv_size = (size_t) pdataval->svalue.len_alloc;

   rc = YDB_NODE_END;
   if (pcon->key_type == DBX_KEYTYPE_M) {
/*
      printf("\r\nstarting from ...");
      dbx_dump_key((char *) key0.mv_data, (int) key0.mv_size);
*/
      pkeyval->svalue.len_used = 0;
      fixed_comp = 0;

      for (n = 0; n < 10; n ++) {
/*
         printf("\r\nDB_SET_RANGE (seed): fixed_comp=%d; pkey->argc=%d; key.mv_size=%d", fixed_comp, pkey->argc, (int) key.mv_size);
         dbx_dump_key((char *) key.mv_data, (int) key.mv_size);
*/
         rc = pcon->p_lmdb_so->p_mdb_cursor_get(pcursor, &key, &data, MDB_SET_RANGE);
/*
         printf("\r\nDB_SET_RANGE: rc=%d; pkey->argc=%d; key.mv_size=%d", rc, pkey->argc, (int) key.mv_size);
         dbx_dump_key((char *) key.mv_data, (int) key.mv_size);
*/
         if (rc == CACHE_SUCCESS) {
            /* reset key.mv_data to memory owned by us */
            pkeyval->svalue.len_used = (unsigned int) key.mv_size;
            memcpy((void *) pkeyval->svalue.buf_addr, (void *) key.mv_data, pkeyval->svalue.len_used);
            key.mv_data = (void *) pkeyval->svalue.buf_addr;
         }
         else {
            rc = YDB_NODE_END;
            break;
         }
         if (*((unsigned char *) key.mv_data) != 0x00) {
            rc = YDB_NODE_END;
            break;
         }

         /* dbx_dump_key((char *) key.mv_data, (int) key.mv_size); */

         if (pkey->argc < 2)
            fixed_comp = 0;
         else
            fixed_comp = lmdb_key_compare(&key, &key0, pkey->args[pkey->argc - 2].csize, pcon->key_type);

         if (!fixed_comp) {
            mkeyn = dbx_split_key(&mkeys[0], (char *) key.mv_data, (int) key.mv_size);
/*
            printf("\r\nNext Record: pkey->argc=%d mkeyn=%d csize=%d; lens=%d:%d %s", pkey->argc, mkeyn, pkey->args[pkey->argc - 1].csize, pkey->args[pkey->argc - 1].svalue.len_used, mkeys[pkey->argc - 1].svalue.len_used, mkeys[pkey->argc - 1].svalue.buf_addr);
*/

            if (mkeyn != pkey->argc) { /* can't use data as it's under lower subscripts */
               data.mv_size = 0;
            }

            if (mkeyn >= pkey->argc) {
               if (pkey->args[pkey->argc - 1].svalue.len_used == mkeys[pkey->argc - 1].svalue.len_used && !strncmp(pkey->args[pkey->argc - 1].svalue.buf_addr, mkeys[pkey->argc - 1].svalue.buf_addr, pkey->args[pkey->argc - 1].svalue.len_used)) {
                  /* current key returned - get next */
                  *(((unsigned char *) key.mv_data) + pkey->args[pkey->argc - 1].csize + 0) = 0x00;
                  *(((unsigned char *) key.mv_data) + pkey->args[pkey->argc - 1].csize + 1) = 0xff;
                  key.mv_size = (pkey->args[pkey->argc - 1].csize + 2);
/*
                  printf("\r\nAdvance Cursor: pkey->argc=%d; key.mv_size=%d", pkey->argc, key.mv_size);
                  dbx_dump_key((char *) key.mv_data, (int) key.mv_size);
*/
                  continue;
               }
               else {
                  /* next key found */
                  if (context == 0) {
                     dbx_memcpy((void *) pkeyval->svalue.buf_addr, (void *) mkeys[pkey->argc - 1].svalue.buf_addr, (size_t) mkeys[pkey->argc - 1].svalue.len_alloc);
                     pkeyval->svalue.len_used = mkeys[pkey->argc - 1].svalue.len_alloc;
                  }
                  else {
                     dbx_memcpy((void *) pkey->ibuffer, (void *) key.mv_data, (size_t) mkeys[pkey->argc - 1].csize);
                     pkey->ibuffer_used = mkeys[pkey->argc - 1].csize;
                     pkey->argc = dbx_split_key(&(pkey->args[0]), (char *) pkey->ibuffer, (int) pkey->ibuffer_used);
                     dbx_memcpy((void *) pkeyval->svalue.buf_addr, (void *) pkey->args[pkey->argc - 1].svalue.buf_addr, (size_t) pkey->args[pkey->argc - 1].svalue.len_alloc);
                     pkeyval->svalue.len_used = pkey->args[pkey->argc - 1].svalue.len_alloc;
                  }
                  pdataval->svalue.len_used = (unsigned int) data.mv_size;
                  memcpy((void *) pdataval->svalue.buf_addr, (void *) data.mv_data, pdataval->svalue.len_used);
                  break;
               }
            }
         }
         else {
            rc = YDB_NODE_END;
            break;
         }
      }
   }
   else { /* not mumps */
/*
      printf("\r\nstarting from ...");
      dbx_dump_key((char *) key.mv_data, (int) key.mv_size);
*/

      if (key.mv_size == 0) {
         rc = pcon->p_lmdb_so->p_mdb_cursor_get(pcursor, &key, &data, MDB_FIRST);

         if (rc == CACHE_SUCCESS) {
            pkeyval->svalue.len_used = (unsigned int) key.mv_size;
            if (pcon->key_type == DBX_KEYTYPE_INT) {
               pkeyval->num.int32 = dbx_get_size((unsigned char *) key.mv_data, 0);
            }
            else {
               memcpy((void *) pkeyval->svalue.buf_addr, (void *) key.mv_data, pkeyval->svalue.len_used);
            }
         }
         else {
            pkeyval->svalue.len_used = 0;
            rc = YDB_NODE_END;
         }
      }
      else {
         rc = pcon->p_lmdb_so->p_mdb_cursor_get(pcursor, &key, &data, MDB_SET_RANGE);

/*
         printf("\r\nAdvance Cursor (DB_SET_RANGE): rc=%d; key.mv_size=%d", rc, key.mv_size);
         dbx_dump_key((char *) key.mv_data, (int) key.mv_size);
*/
         if (rc == CACHE_SUCCESS) {
            pkeyval->svalue.len_used = (unsigned int) key.mv_size;
            if (!lmdb_key_compare(&key, &key0, 0, pcon->key_type)) {
               rc = pcon->p_lmdb_so->p_mdb_cursor_get(pcursor, &key, &data, MDB_NEXT);

/*
               printf("\r\nAdvance Cursor (DB_NEXT): rc=%d; key.mv_size=%d", rc, key.mv_size);
               dbx_dump_key((char *) key.mv_data, (int) key.mv_size);
*/
               if (rc == CACHE_SUCCESS) {
                  pkeyval->svalue.len_used = (unsigned int) key.mv_size;
                  if (pcon->key_type == DBX_KEYTYPE_INT) {
                     pkeyval->num.int32 = dbx_get_size((unsigned char *) key.mv_data, 0);
                  }
                  else {
                     memcpy((void *) pkeyval->svalue.buf_addr, (void *) key.mv_data, pkeyval->svalue.len_used);
                  }
               }
               else {
                  pkeyval->svalue.len_used = 0;
                  rc = YDB_NODE_END;
               }
            }
            else {
               pkeyval->svalue.len_used = (unsigned int) key.mv_size;
               if (pcon->key_type == DBX_KEYTYPE_INT) {
                  pkeyval->num.int32 = dbx_get_size((unsigned char *) key.mv_data, 0);
               }
               else {
                  memcpy((void *) pkeyval->svalue.buf_addr, (void *) key.mv_data, pkeyval->svalue.len_used);
               }
            }
         }
         else {
            pkeyval->svalue.len_used = 0;
            rc = YDB_NODE_END;
         }
      }
      if (rc == CACHE_SUCCESS && pcon->key_type == DBX_KEYTYPE_INT) {
         sprintf(pkeyval->svalue.buf_addr, "%d", pkeyval->num.int32);
         pkeyval->svalue.len_used = (unsigned int) strlen(pkeyval->svalue.buf_addr);
         pkeyval->type = DBX_DTYPE_INT;
      }
   }

   if (context == 0) {
      pcon->p_lmdb_so->p_mdb_cursor_close(pcursor);
      lmdb_commit_ro_transaction(pmeth, 0);
   }

   if (rc != CACHE_SUCCESS) {
      pkeyval->svalue.len_used = 0;
   }
   if (pkeyval->svalue.len_used == 0) {
      rc = YDB_NODE_END;
   }

   return rc;
}


int lmdb_previous(DBXMETH *pmeth, DBXKEY *pkey, DBXVAL *pkeyval, DBXVAL *pdataval, int context)
{
   int rc, mkeyn, fixed_comp;
   DBXCON *pcon = pmeth->pcon;
   MDB_val key, key0, data;
   MDB_cursor *pcursor;
   DBXVAL mkeys[DBX_MAXARGS];
/*
   printf("\r\n ******* lmdb_previous ******* argn=%d; pkeyval->svalue.buf_addr=%p; pkey->ibuffer=%p; seed_len%d\r\n", pkey->argc, pkeyval->svalue.buf_addr, pkey->ibuffer, pkey->args[pkey->argc - 1].svalue.len_used);
*/

   if (context == 0) {
      rc = lmdb_start_ro_transaction(pmeth, 0);
      rc = pcon->p_lmdb_so->p_mdb_cursor_open(pcon->p_lmdb_so->ptxnro, pcon->p_lmdb_so->db, &pcursor);
   }
   else {
     pcursor = pmeth->plmdbcursor;
   }

   if (pcon->key_type == DBX_KEYTYPE_INT) {
      key0.mv_data = &(pkey->args[0].num.int32);
      key0.mv_size = sizeof(pkey->args[0].num.int32);
      pkeyval->num.int32 = pkey->args[0].num.int32;
      key.mv_data = &(pkeyval->num.int32);
      key.mv_size = sizeof(pkeyval->num.int32);
      if (pkey->args[0].svalue.len_used == 0) {
         key.mv_size = 0;
         key0.mv_size = 0;
      }
   }
   else if (pcon->key_type == DBX_KEYTYPE_STR) {
      key0.mv_data = pkey->args[0].svalue.buf_addr;
      key0.mv_size = pkey->args[0].svalue.len_used;
      memcpy((void *) pkeyval->svalue.buf_addr, (void *) pkey->args[0].svalue.buf_addr, (size_t) pkey->args[0].svalue.len_used);
      pkeyval->svalue.len_used = pkey->args[0].svalue.len_used;
      key.mv_data = (void *) pkeyval->svalue.buf_addr;
      key.mv_size = (size_t) pkeyval->svalue.len_used;
   }
   else { /* mumps */
      key0.mv_data = (void *) pkey->ibuffer;
      key0.mv_size = (size_t) pkey->ibuffer_used;
      memcpy((void *) pkeyval->svalue.buf_addr, (void *) pkey->ibuffer, (size_t) pkey->ibuffer_used);
      pkeyval->svalue.len_used = pkey->ibuffer_used;
      key.mv_data = (void *) pkeyval->svalue.buf_addr;
      key.mv_size = (size_t) pkeyval->svalue.len_used;
   }

   data.mv_data = (void *) pdataval->svalue.buf_addr;
   data.mv_size = (size_t) pdataval->svalue.len_alloc;

   fixed_comp = 0;
   rc = YDB_NODE_END;
   if (pcon->key_type == DBX_KEYTYPE_M) {
/*
      printf("\r\n seed ... pkey->ibuffer_used=%d", pkey->ibuffer_used);
      dbx_dump_key((char *) key0.mv_data, (int) key0.mv_size);
*/
      pkeyval->svalue.len_used = 0;
      if (pkey->argc < 2 && pkey->args[pkey->argc - 1].svalue.len_used == 0) { /* null seed - special case */

         rc = pcon->p_lmdb_so->p_mdb_cursor_get(pcursor, &key, &data, MDB_LAST);

/*
         printf("\r\n DB_LAST rc=%d; argc=%d; key.mv_size=%d", rc, pkey->argc, (int) key.mv_size);
         dbx_dump_key((char *) key.mv_data, (int) key.mv_size);
*/
      }
      else {

         if (pkey->args[pkey->argc - 1].svalue.len_used == 0) {
            *(((unsigned char *) key.mv_data) + pkey->args[pkey->argc - 1].csize - 1) = 0xff;
         }

         rc = pcon->p_lmdb_so->p_mdb_cursor_get(pcursor, &key, &data, MDB_SET_RANGE);
/*
         printf("\r\n DB_SET_RANGE rc=%d; argc=%d; key.mv_size=%d", rc, pkey->argc, (int) key.mv_size);
         dbx_dump_key((char *) key.mv_data, (int) key.mv_size);
*/
         if (rc == CACHE_SUCCESS) {
            rc = pcon->p_lmdb_so->p_mdb_cursor_get(pcursor, &key, &data, MDB_PREV);

            if (pkey->argc < 2)
               fixed_comp = 0;
            else
               fixed_comp = lmdb_key_compare(&key, &key0, pkey->args[pkey->argc - 2].csize, pcon->key_type);
/*
            printf("\r\n DB_PREV rc=%d; argc=%d; key.mv_size=%d; fixed_comp=%d;", rc, pkey->argc, (int) key.mv_size, fixed_comp);
            dbx_dump_key((char *) key.mv_data, (int) key.mv_size);
*/
         }
      }
      if (rc == CACHE_SUCCESS && !fixed_comp) {
         mkeyn = dbx_split_key(&mkeys[0], (char *) key.mv_data, (int) key.mv_size);

         if (context == 0) {
            dbx_memcpy((void *) pkeyval->svalue.buf_addr,  (void *) mkeys[pmeth->key.argc - 1].svalue.buf_addr,  (size_t) mkeys[pmeth->key.argc - 1].svalue.len_alloc);
            pkeyval->svalue.len_used = mkeys[pmeth->key.argc - 1].svalue.len_alloc;
         }
         else {
            dbx_memcpy((void *) pkey->ibuffer, (void *) key.mv_data, (size_t) mkeys[pkey->argc - 1].csize);
            pkey->ibuffer_used = mkeys[pkey->argc - 1].csize;
            pkey->argc = dbx_split_key(&(pkey->args[0]), (char *) pkey->ibuffer, (int) pkey->ibuffer_used);
            dbx_memcpy((void *) pkeyval->svalue.buf_addr, (void *) pkey->args[pkey->argc - 1].svalue.buf_addr, (size_t) pkey->args[pkey->argc - 1].svalue.len_alloc);
            pkeyval->svalue.len_used = pkey->args[pkey->argc - 1].svalue.len_alloc;
         }
         pdataval->svalue.len_used = (unsigned int) data.mv_size;
         memcpy((void *) pdataval->svalue.buf_addr, (void *) data.mv_data, pdataval->svalue.len_used);

         if (mkeyn != pkey->argc) { /* can't use data as it's under lower subscripts */
            data.mv_size = 0;
            pdataval->svalue.len_used = (unsigned int) data.mv_size;
         }

      }
      else {
         pkeyval->svalue.len_used = 0;
         rc = YDB_NODE_END;
      }
   }
   else { /* not mumps */
      if (key.mv_size == 0) {
         rc = pcon->p_lmdb_so->p_mdb_cursor_get(pcursor, &key, &data, MDB_LAST);
         if (rc == CACHE_SUCCESS) {
            pkeyval->svalue.len_used = (unsigned int) key.mv_size;
            if (pcon->key_type == DBX_KEYTYPE_INT) {
               pkeyval->num.int32 = dbx_get_size((unsigned char *) key.mv_data, 0);
            }
            else {
               memcpy((void *) pkeyval->svalue.buf_addr, (void *) key.mv_data, pkeyval->svalue.len_used);
            }
         }
         else {
            pkeyval->svalue.len_used = 0;
            rc = YDB_NODE_END;
         }
      }
      else {
         rc = pcon->p_lmdb_so->p_mdb_cursor_get(pcursor, &key, &data, MDB_SET_RANGE);

         if (rc == CACHE_SUCCESS) {
            rc = pcon->p_lmdb_so->p_mdb_cursor_get(pcursor, &key, &data, MDB_PREV);

            if (rc == CACHE_SUCCESS) {
               pkeyval->svalue.len_used = (unsigned int) key.mv_size;
               if (pcon->key_type == DBX_KEYTYPE_INT) {
                  pkeyval->num.int32 = dbx_get_size((unsigned char *) key.mv_data, 0);
               }
               else {
                  memcpy((void *) pkeyval->svalue.buf_addr, (void *) key.mv_data, pkeyval->svalue.len_used);
               }
            }
            else {
               pkeyval->svalue.len_used = 0;
               rc = YDB_NODE_END;
            }
         }
         else {
            rc = pcon->p_lmdb_so->p_mdb_cursor_get(pcursor, &key, &data, MDB_LAST);

            if (rc == CACHE_SUCCESS) {
               pkeyval->svalue.len_used = (unsigned int) key.mv_size;
               if (pcon->key_type == DBX_KEYTYPE_INT) {
                  pkeyval->num.int32 = dbx_get_size((unsigned char *) key.mv_data, 0);
               }
               else {
                  memcpy((void *) pkeyval->svalue.buf_addr, (void *) key.mv_data, pkeyval->svalue.len_used);
               }
            }
            else {
               pkeyval->svalue.len_used = 0;
               rc = YDB_NODE_END;
            }
         }
      }
      if (rc == CACHE_SUCCESS && pcon->key_type == DBX_KEYTYPE_INT) {
         sprintf(pkeyval->svalue.buf_addr, "%d", pkeyval->num.int32);
         pkeyval->svalue.len_used = (unsigned int) strlen(pkeyval->svalue.buf_addr);
         pkeyval->type = DBX_DTYPE_INT;
      }
   }

   if (context == 0) {
      pcon->p_lmdb_so->p_mdb_cursor_close(pcursor);
      lmdb_commit_ro_transaction(pmeth, 0);
   }

   if (rc != CACHE_SUCCESS) {
      pkeyval->svalue.len_used = 0;
   }

   return rc;
}


int lmdb_key_compare(MDB_val *key1, MDB_val *key2, int compare_max, short keytype)
{
   int n, n1, n2, rc;
   char *c1, *c2;

   c1 = (char *) key1->mv_data;
   c2 = (char *) key2->mv_data;
   if (keytype == DBX_KEYTYPE_M) {
      if (compare_max > 0) {
         if (key1->mv_size < (u_int32_t) compare_max || key2->mv_size < (u_int32_t) compare_max) {
            return -1;
         }
         if ((int) key1->mv_size > compare_max && c1[compare_max] != '\0') {
            return -1;
         }
         if ((int) key2->mv_size > compare_max && c2[compare_max] != '\0') {
            return -1;
         }
      }
      else {
         if (key1->mv_size == 0) {
            return -1;
         }
         if (key1->mv_size != key2->mv_size) {
            return -1;
         }
         compare_max = (int) key1->mv_size;
      }
   }
   else {
      if (key1->mv_size == 0) {
         return -1;
      }
      if (key1->mv_size != key2->mv_size) {
         return -1;
      }

      if (keytype == DBX_KEYTYPE_INT) {
         n1 = *((int *) key1->mv_data);
         n2 = *((int *) key2->mv_data);
         if (n1 == n2)
            return 0;
         else
            return -1;
      }
      compare_max = (int) key1->mv_size;
   }

   rc = 0;
   for (n = 0; n < (int) compare_max; n ++) {
      if (c1[n] != c2[n]) {
         rc = -1;
         break;
      }
   }
   return rc;
}


int lmdb_error_message(DBXCON *pcon, int error_code)
{
   sprintf(pcon->error, "LMDB error code: %d", error_code);
   return 0;
}


int lmdb_error(DBXCON *pcon, int error_code)
{
   T_STRCPY(pcon->error, _dbxso(pcon->error), "General LMDB Error");

   return 1;
}


int dbx_version(DBXMETH *pmeth)
{
   char buffer[256];
   DBXCON *pcon = pmeth->pcon;

#ifdef _WIN32
__try {
#endif

   T_SPRINTF((char *) buffer, _dbxso(buffer), "mg-dbx-bdb: version: %s; ABI: %d", DBX_VERSION, NODE_MODULE_VERSION);

   if (pcon->p_zv && pcon->p_zv->version[0]) {
      if (pcon->p_zv->dbtype == DBX_DBTYPE_BDB)
         T_STRCAT((char *) buffer, _dbxso(buffer), "; BerkeleyDB version: ");
      else if (pcon->p_zv->dbtype == DBX_DBTYPE_LMDB)
         T_STRCAT((char *) buffer, _dbxso(buffer), "; LMDB version: ");
      T_STRCAT((char *) buffer, _dbxso(buffer), pcon->p_zv->version);
   }

   dbx_create_string(&(pmeth->output_val.svalue), (void *) buffer, DBX_DTYPE_STR8);

   return 0;

#ifdef _WIN32
}
__except (EXCEPTION_EXECUTE_HANDLER) {

   DWORD code;
   char bufferx[256];

   __try {
      code = GetExceptionCode();
      sprintf_s(bufferx, 255, "Exception caught in f:dbx_version: %x", code);
      dbx_log_event(pcon, bufferx, "Error Condition", 0);
   }
   __except (EXCEPTION_EXECUTE_HANDLER) {
      ;
   }

   return 0;
}
#endif
}

int dbx_open(DBXMETH *pmeth)
{
   int rc;
   DBXCON *pcon = pmeth->pcon;

#ifdef _WIN32
__try {
#endif

   rc = 0;
   if (!pcon->dbtype) {
      strcpy(pcon->error, "Unable to determine the database type");
      rc = CACHE_NOCON;
      goto dbx_open_exit;
   }

   if (!pcon->db_library[0]) {
      strcpy(pcon->error, "Unable to determine the path to the database installation");
      rc = CACHE_NOCON;
      goto dbx_open_exit;
   }

   dbx_enter_critical_section((void *) &dbx_async_mutex);
   if (pcon->dbtype == DBX_DBTYPE_BDB && p_bdb_so_global) {
      rc = CACHE_SUCCESS;
      pcon->p_bdb_so = p_bdb_so_global;
      pcon->p_bdb_so->no_connections ++;
      pcon->p_bdb_so->multiple_connections ++;
      pcon->p_zv = &(p_bdb_so_global->zv);
      dbx_leave_critical_section((void *) &dbx_async_mutex);
      return rc;
   }
   else if (pcon->dbtype == DBX_DBTYPE_LMDB && p_lmdb_so_global) {
      rc = CACHE_SUCCESS;
      pcon->p_lmdb_so = p_lmdb_so_global;
      pcon->p_lmdb_so->no_connections ++;
      pcon->p_lmdb_so->multiple_connections ++;
      pcon->p_zv = &(p_lmdb_so_global->zv);
      dbx_leave_critical_section((void *) &dbx_async_mutex);
      return rc;
   }

   if (pcon->dbtype == DBX_DBTYPE_BDB) {
      rc = bdb_open(pmeth);
      pcon->p_bdb_so->no_connections ++;
      p_bdb_so_global = pcon->p_bdb_so;
   }
   else if (pcon->dbtype == DBX_DBTYPE_LMDB) {
      rc = lmdb_open(pmeth);
      pcon->p_lmdb_so->no_connections ++;
      p_lmdb_so_global = pcon->p_lmdb_so;
   }

   dbx_pool_thread_init(pcon, 1);

   dbx_leave_critical_section((void *) &dbx_async_mutex);

dbx_open_exit:

   return rc;

#ifdef _WIN32
}
__except (EXCEPTION_EXECUTE_HANDLER) {

   DWORD code;
   char bufferx[256];

   __try {
      code = GetExceptionCode();
      sprintf_s(bufferx, 255, "Exception caught in f:dbx_open: %x", code);
      dbx_log_event(pcon, bufferx, "Error Condition", 0);
   }
   __except (EXCEPTION_EXECUTE_HANDLER) {
      ;
   }

   return 0;
}
#endif
}


int dbx_do_nothing(DBXMETH *pmeth)
{

   /* Function called asynchronously */

   return 0;
}


int dbx_close(DBXMETH *pmeth)
{
   int no_connections;
   DBXCON *pcon = pmeth->pcon;

#ifdef _WIN32
__try {
#endif

   no_connections = 0;

   dbx_enter_critical_section((void *) &dbx_async_mutex);
   if (pcon->dbtype == DBX_DBTYPE_BDB) {
      if (pcon->p_bdb_so) {
         pcon->p_bdb_so->no_connections --;
         no_connections =  pcon->p_bdb_so->no_connections;
      }
   }
   else if (pcon->dbtype == DBX_DBTYPE_LMDB) {
      if (pcon->p_lmdb_so) {
         pcon->p_lmdb_so->no_connections --;
         no_connections =  pcon->p_lmdb_so->no_connections;
      }
   }
   dbx_leave_critical_section((void *) &dbx_async_mutex);

   /* printf("\r\ndbx_close: no_connections=%d\r\n", no_connections); */

   if (pcon->dbtype == DBX_DBTYPE_BDB) {

      /* printf("\r\ndbx_close: no_connections=%d; pcon->p_bdb_so->multiple_connections=%d;\r\n", no_connections, pcon->p_bdb_so->multiple_connections); */

      if (pcon->p_bdb_so && no_connections == 0 && pcon->p_bdb_so->multiple_connections == 0) {
         if (pcon->p_bdb_so->loaded) {

            if (pcon->p_bdb_so->pdb != NULL) {
               pcon->p_bdb_so->pdb->close(pcon->p_bdb_so->pdb, 0);
            }

/*
            printf("\r\np_bdb_exit=%d\r\n", rc);
            dbx_dso_unload(pcon->p_bdb_so->p_library); 
*/
            pcon->p_bdb_so->p_library = NULL;
            pcon->p_bdb_so->loaded = 0;
         }

         strcpy(pcon->error, "");
         dbx_create_string(&(pmeth->output_val.svalue), (void *) "1", DBX_DTYPE_STR);

         strcpy(pcon->p_bdb_so->libdir, "");
         strcpy(pcon->p_bdb_so->libnam, "");
      }
   }
   else if (pcon->dbtype == DBX_DBTYPE_LMDB) {

      /* printf("\r\ndbx_close: no_connections=%d; pcon->p_lmdb_so->multiple_connections=%d;\r\n", no_connections, pcon->p_lmdb_so->multiple_connections); */

      if (pcon->p_lmdb_so) {
         if (pcon->tlevel > 0) {
            pcon->p_lmdb_so->p_mdb_txn_commit(pcon->p_lmdb_so->ptxn);
            pcon->tlevel = 0;
         }
         if (pcon->tlevelro > 0) {
            pcon->p_lmdb_so->p_mdb_txn_abort(pcon->p_lmdb_so->ptxnro);
            pcon->tlevelro = 0;
         }
      }

      if (pcon->p_lmdb_so && no_connections == 0 && pcon->p_lmdb_so->multiple_connections == 0) {
         if (pcon->p_lmdb_so->loaded) {

            if (pcon->p_lmdb_so->pdb != NULL) {
               pcon->p_lmdb_so->p_mdb_dbi_close(pcon->p_lmdb_so->penv, pcon->p_lmdb_so->db);
            }
            if (pcon->p_lmdb_so->penv != NULL) {
               pcon->p_lmdb_so->p_mdb_env_close(pcon->p_lmdb_so->penv);
            }


/*
            printf("\r\np_lmdb_exit=%d\r\n", rc);
            dbx_dso_unload(pcon->p_lmdb_so->p_library); 
*/
            pcon->p_lmdb_so->p_library = NULL;
            pcon->p_lmdb_so->loaded = 0;
         }

         strcpy(pcon->error, "");
         dbx_create_string(&(pmeth->output_val.svalue), (void *) "1", DBX_DTYPE_STR);

         strcpy(pcon->p_lmdb_so->libdir, "");
         strcpy(pcon->p_lmdb_so->libnam, "");
      }
   }


   T_STRCPY(pcon->p_zv->version, _dbxso(pcon->p_zv->version), "");

   T_STRCPY(pcon->db_library, _dbxso(pcon->db_library), "");
   T_STRCPY(pcon->db_file, _dbxso(pcon->db_file), "");

   return 0;

#ifdef _WIN32
}
__except (EXCEPTION_EXECUTE_HANDLER) {

   DWORD code;
   char bufferx[256];

   __try {
      code = GetExceptionCode();
      sprintf_s(bufferx, 255, "Exception caught in f:dbx_close: %x", code);
      dbx_log_event(pcon, bufferx, "Error Condition", 0);
   }
   __except (EXCEPTION_EXECUTE_HANDLER) {
      ;
   }

   return 0;
}
#endif
}


int dbx_global_reference(DBXMETH *pmeth)
{
   int rc;
   DBXCON *pcon = pmeth->pcon;

#ifdef _WIN32
__try {
#endif

   rc = CACHE_SUCCESS;
   if (pcon->dbtype == DBX_DBTYPE_BDB) {
      rc = CACHE_SUCCESS;
      return rc;
   }
   else if (pcon->dbtype == DBX_DBTYPE_LMDB) {
      rc = CACHE_SUCCESS;
      return rc;
   }

   return rc;

#ifdef _WIN32
}
__except (EXCEPTION_EXECUTE_HANDLER) {

   DWORD code;
   char bufferx[256];

   __try {
      code = GetExceptionCode();
      sprintf_s(bufferx, 255, "Exception caught in f:dbx_global_reference: %x", code);
      dbx_log_event(pcon, bufferx, "Error Condition", 0);
   }
   __except (EXCEPTION_EXECUTE_HANDLER) {
      ;
   }

   return 0;
}
#endif
}


int dbx_get(DBXMETH *pmeth)
{
   int rc;
   DBXCON *pcon = pmeth->pcon;

#ifdef _WIN32
__try {
#endif

   DBX_DB_LOCK(rc, 0);

   rc = dbx_global_reference(pmeth);

   if (rc != CACHE_SUCCESS) {
      dbx_error_message(pmeth, rc, (char *) "dbx_get");
      goto dbx_get_exit;
   }

   if (pcon->dbtype == DBX_DBTYPE_BDB) {
      DBT key, data;

      memset(&key, 0, sizeof(DBT));
      memset(&data, 0, sizeof(DBT));
      key.flags = DB_DBT_USERMEM;
      data.flags = DB_DBT_USERMEM;

      if (pcon->key_type == DBX_KEYTYPE_INT) {
         key.data = &(pmeth->key.args[0].num.int32);
         key.size = sizeof(pmeth->key.args[0].num.int32);
      }
      else if (pcon->key_type == DBX_KEYTYPE_STR) {
         key.data = (void *) pmeth->key.args[0].svalue.buf_addr;
         key.size = (u_int32_t) pmeth->key.args[0].svalue.len_used;
      }
      else { /* mumps */
         key.data = (void *) pmeth->key.ibuffer;
         key.size = (u_int32_t) pmeth->key.args[pmeth->key.argc - 1].csize;
      }

      data.data = (void *) pmeth->output_val.svalue.buf_addr;
      data.ulen = (u_int32_t)  pmeth->output_val.svalue.len_alloc;

/* v1.1.4
      {
         int rc1;
         rc1 = pcon->p_bdb_so->pdb->sync(pcon->p_bdb_so->pdb, 0);
      }
*/
      rc = pcon->p_bdb_so->pdb->get(pcon->p_bdb_so->pdb, NULL, &key, &data, 0);
      pmeth->output_val.svalue.len_used = data.size;

      if (rc == DB_NOTFOUND) {
         rc = CACHE_ERUNDEF;
      }
   }
   else if (pcon->dbtype == DBX_DBTYPE_LMDB) {
      MDB_val key, data;

      if (pcon->key_type == DBX_KEYTYPE_INT) {
         key.mv_data = (void *) &(pmeth->key.args[0].num.int32);
         key.mv_size = (size_t) sizeof(pmeth->key.args[0].num.int32);
      }
      else if (pcon->key_type == DBX_KEYTYPE_STR) {
         key.mv_data = (void *) pmeth->key.args[0].svalue.buf_addr;
         key.mv_size = (size_t) pmeth->key.args[0].svalue.len_used;
      }
      else { /* mumps */
         key.mv_data = (void *) pmeth->key.ibuffer;
         key.mv_size = (size_t) pmeth->key.args[pmeth->key.argc - 1].csize;;
      }

      data.mv_data = (void *) pmeth->output_val.svalue.buf_addr;
      data.mv_size = (size_t)  pmeth->output_val.svalue.len_alloc;

      rc = lmdb_start_ro_transaction(pmeth, 0);
      rc = pcon->p_lmdb_so->p_mdb_get(pcon->p_lmdb_so->ptxnro, pcon->p_lmdb_so->db, &key, &data);
      lmdb_commit_ro_transaction(pmeth, 0);

      if (rc == CACHE_SUCCESS) {
         pmeth->output_val.svalue.len_used = (unsigned int) data.mv_size;
         memcpy((void *) pmeth->output_val.svalue.buf_addr, (void *) data.mv_data, data.mv_size);
      }
      else {
         pmeth->output_val.svalue.len_used = 0;
         if (rc == MDB_NOTFOUND) {
            rc = CACHE_ERUNDEF;
         }
      }
   }

   if (rc == CACHE_ERUNDEF) {
      dbx_create_string(&(pmeth->output_val.svalue), (void *) "", DBX_DTYPE_STR8);
   }
   else if (rc != CACHE_SUCCESS) {
      dbx_error_message(pmeth, rc, (char *) "dbx_get");
   }

dbx_get_exit:

   DBX_DB_UNLOCK(rc);

   return 0;

#ifdef _WIN32
}
__except (EXCEPTION_EXECUTE_HANDLER) {

   DWORD code;
   char bufferx[256];

   __try {
      code = GetExceptionCode();
      sprintf_s(bufferx, 255, "Exception caught in f:dbx_get: %x", code);
      dbx_log_event(pcon, bufferx, "Error Condition", 0);
   }
   __except (EXCEPTION_EXECUTE_HANDLER) {
      ;
   }

   return 0;
}
#endif
}


int dbx_set(DBXMETH *pmeth)
{
   int rc, ndata;
   DBXCON *pcon = pmeth->pcon;

#ifdef _WIN32
__try {
#endif

   DBX_DB_LOCK(rc, 0);

   rc = dbx_global_reference(pmeth);

   if (rc != CACHE_SUCCESS) {
      dbx_error_message(pmeth, rc, (char *) "dbx_set");
      goto dbx_set_exit;
   }

   if (pcon->dbtype == DBX_DBTYPE_BDB) {
      DBT key, data;

      memset(&key, 0, sizeof(DBT));
      memset(&data, 0, sizeof(DBT));
      key.flags = DB_DBT_USERMEM;
      data.flags = DB_DBT_USERMEM;
      ndata = 1;

      if (pcon->key_type == DBX_KEYTYPE_INT) {
         key.data = &(pmeth->key.args[0].num.int32);
         key.size = sizeof(pmeth->key.args[0].num.int32);
      }
      else if (pcon->key_type == DBX_KEYTYPE_STR) {
         key.data = (void *) pmeth->key.args[0].svalue.buf_addr;
         key.size = (u_int32_t) pmeth->key.args[0].svalue.len_used;
      }
      else { /* mumps */
         key.data = (void *) pmeth->key.ibuffer;
         key.size = (u_int32_t) pmeth->key.args[pmeth->key.argc - 2].csize;
         ndata = pmeth->key.argc - 1;
      }

      data.data = (void *) pmeth->key.args[ndata].svalue.buf_addr;
      data.size = (u_int32_t) pmeth->key.args[ndata].svalue.len_used;

      rc = pcon->p_bdb_so->pdb->put(pcon->p_bdb_so->pdb, NULL, &key, &data, 0);

/* v1.1.4
      {
         int rc1;
         rc1 = pcon->p_bdb_so->pdb->sync(pcon->p_bdb_so->pdb, 0);
      }
*/

/*
      if (rc == DB_KEYEXIST) {
         printf("\r\nPut failed because key %d already exists", rc);
      }
*/

   }
   else if (pcon->dbtype == DBX_DBTYPE_LMDB) {
      MDB_val key, data;

      ndata = 1;
      if (pcon->key_type == DBX_KEYTYPE_INT) {
         key.mv_data = (void *) &(pmeth->key.args[0].num.int32);
         key.mv_size = (size_t) sizeof(pmeth->key.args[0].num.int32);
      }
      else if (pcon->key_type == DBX_KEYTYPE_STR) {
         key.mv_data = (void *) pmeth->key.args[0].svalue.buf_addr;
         key.mv_size = (size_t) pmeth->key.args[0].svalue.len_used;
      }
      else { /* mumps */
         key.mv_data = (void *) pmeth->key.ibuffer;
         key.mv_size = (size_t) pmeth->key.args[pmeth->key.argc - 2].csize;
         ndata = pmeth->key.argc - 1;
      }

      data.mv_data = (void *) pmeth->key.args[ndata].svalue.buf_addr;
      data.mv_size = (size_t) pmeth->key.args[ndata].svalue.len_used;

      rc = pcon->p_lmdb_so->p_mdb_txn_begin(pcon->p_lmdb_so->penv, NULL, 0, &(pcon->p_lmdb_so->ptxn));
      if (rc != 0) {
         strcpy(pcon->error, "Cannot create or open a LMDB transaction for an update operation");
         dbx_error_message(pmeth, rc, (char *) "dbx_set");
         goto dbx_set_exit;
      }

      rc = pcon->p_lmdb_so->p_mdb_put(pcon->p_lmdb_so->ptxn, pcon->p_lmdb_so->db, &key, &data, 0);
      pcon->tlevel ++;

      pcon->p_lmdb_so->p_mdb_txn_commit(pcon->p_lmdb_so->ptxn);
      pcon->tlevel --;
   }

   if (rc == CACHE_SUCCESS) {
      dbx_create_string(&(pmeth->output_val.svalue), (void *) &rc, DBX_DTYPE_INT);
   }
   else {
      dbx_error_message(pmeth, rc, (char *) "dbx_set");
   }

dbx_set_exit:

   DBX_DB_UNLOCK(rc);

   return 0;

#ifdef _WIN32
}
__except (EXCEPTION_EXECUTE_HANDLER) {

   DWORD code;
   char bufferx[256];

   __try {
      code = GetExceptionCode();
      sprintf_s(bufferx, 255, "Exception caught in f:dbx_set: %x", code);
      dbx_log_event(pcon, bufferx, "Error Condition", 0);
   }
   __except (EXCEPTION_EXECUTE_HANDLER) {
      ;
   }

   return 0;
}
#endif
}


int dbx_defined(DBXMETH *pmeth)
{
   int rc, n;
   DBXCON *pcon = pmeth->pcon;

#ifdef _WIN32
__try {
#endif

   DBX_DB_LOCK(rc, 0);

   rc = dbx_global_reference(pmeth);
   if (rc != CACHE_SUCCESS) {
      dbx_error_message(pmeth, rc, (char *) "dbx_defined");
      goto dbx_defined_exit;
   }

   n = 0;
   if (pcon->dbtype == DBX_DBTYPE_BDB) {
      DBT key, key0, data;
      DBC *pcursor;

      memset(&key, 0, sizeof(DBT));
      memset(&key0, 0, sizeof(DBT));
      memset(&data, 0, sizeof(DBT));
      key.flags = DB_DBT_USERMEM;
      data.flags = DB_DBT_USERMEM;

      if (pcon->key_type == DBX_KEYTYPE_INT) {
         key.data = &(pmeth->key.args[0].num.int32);
         key.size = sizeof(pmeth->key.args[0].num.int32);
      }
      else if (pcon->key_type == DBX_KEYTYPE_STR) {
         key.data = (void *) pmeth->key.args[0].svalue.buf_addr;
         key.size = (u_int32_t) pmeth->key.args[0].svalue.len_used;
      }
      else { /* mumps */
         key.data = (void *) pmeth->key.ibuffer;
         key.size = (u_int32_t) pmeth->key.args[pmeth->key.argc - 1].csize;
         key.ulen = (u_int32_t) pmeth->key.ibuffer_size;

         memcpy((void *) pmeth->output_key.svalue.buf_addr, (void *) pmeth->key.ibuffer, (size_t) pmeth->key.args[pmeth->key.argc - 1].csize);
         key0.data = (void *) pmeth->output_key.svalue.buf_addr;
         key0.size = (u_int32_t) pmeth->key.args[pmeth->key.argc - 1].csize;
         key0.ulen = (u_int32_t) pmeth->output_key.svalue.len_alloc;
      }

      data.data = (void *) pmeth->output_val.svalue.buf_addr;
      data.ulen = (u_int32_t)  pmeth->output_val.svalue.len_alloc;

      rc = pcon->p_bdb_so->pdb->get(pcon->p_bdb_so->pdb, NULL, &key, &data, 0);
      pmeth->output_val.svalue.len_used = data.size;

      if (rc == DB_NOTFOUND) {
         rc = CACHE_ERUNDEF;
         n = 0;
      }
      else {
         n = 1;
      }

      if (pcon->key_type == DBX_KEYTYPE_M) {
         rc = pcon->p_bdb_so->pdb->cursor(pcon->p_bdb_so->pdb, NULL, &pcursor, 0);
         if (rc == CACHE_SUCCESS) {
/*
            printf("\r\ndbx_defined: n=%d; key.ulen=%d; key.size=%d; pmeth->key.argc=%d; pmeth->key.args[pmeth->key.argc - 1].csize=%d", n, (int) key.ulen, (int) key.size, pmeth->key.argc, pmeth->key.args[pmeth->key.argc - 1].csize);
*/
            rc = pcursor->get(pcursor, &key, &data, DB_SET_RANGE);
/*
            printf("\r\ndbx_defined: DB_SET_RANGE: n=%d; rc=%d key.size=%d; pmeth->key.argc=%d; pmeth->key.args[pmeth->key.argc - 1].csize=%d", n, rc, (int) key.size, pmeth->key.argc, pmeth->key.args[pmeth->key.argc - 1].csize);
            dbx_dump_key((char *) key.data, (int) key.size);
*/
            if (rc == CACHE_SUCCESS) {
               if (n && key.size == pmeth->key.args[pmeth->key.argc - 1].csize && !bdb_key_compare(&key, &key0, (int) pmeth->key.args[pmeth->key.argc - 1].csize, pcon->key_type)) { /* current record defined, get next */
                  rc = pcursor->get(pcursor, &key, &data, DB_NEXT);
/*
                  printf("\r\ndbx_defined: DB_NEXT: n=%d; rc=%d key.size=%d; pmeth->key.argc=%d; pmeth->key.args[pmeth->key.argc - 1].csize=%d", n, rc, (int) key.size, pmeth->key.argc, pmeth->key.args[pmeth->key.argc - 1].csize);
                  dbx_dump_key((char *) key.data, (int) key.size);
*/
               }
               if (rc == CACHE_SUCCESS && key.size > pmeth->key.args[pmeth->key.argc - 1].csize && !bdb_key_compare(&key, &key0, (int) pmeth->key.args[pmeth->key.argc - 1].csize, pcon->key_type)) {
                  n += 10;
               }
            }
            pcursor->close(pcursor);
         }
      }
      rc = CACHE_SUCCESS;
   }
/*
      MDB_val key, data;

      if (pcon->key_type == DBX_KEYTYPE_INT) {
         key.mv_data = (void *) &(pmeth->key.args[0].num.int32);
         key.mv_size = (size_t) sizeof(pmeth->key.args[0].num.int32);
      }
      else if (pcon->key_type == DBX_KEYTYPE_STR) {
         key.mv_data = (void *) pmeth->key.args[0].svalue.buf_addr;
         key.mv_size = (size_t) pmeth->key.args[0].svalue.len_used;
      }
      else {
         key.mv_data = (void *) pmeth->key.ibuffer;
         key.mv_size = (size_t) pmeth->key.args[pmeth->key.argc - 1].csize;;
      }

      data.mv_data = (void *) pmeth->output_val.svalue.buf_addr;
      data.mv_size = (size_t)  pmeth->output_val.svalue.len_alloc;

      rc = pcon->p_lmdb_so->p_mdb_get(pcon->p_lmdb_so->ptxn, pcon->p_lmdb_so->db, &key, &data);

      if (rc == CACHE_SUCCESS) {
         pmeth->output_val.svalue.len_used = (unsigned int) data.mv_size;
         memcpy((void *) pmeth->output_val.svalue.buf_addr, (void *) data.mv_data, data.mv_size);
      }
      else {
         pmeth->output_val.svalue.len_used = 0;
         if (rc == MDB_NOTFOUND) {
            rc = CACHE_ERUNDEF;
         }
      }

*/
   else if (pcon->dbtype == DBX_DBTYPE_LMDB) {
      MDB_val key, key0, data;
      MDB_cursor *pcursor;

      if (pcon->key_type == DBX_KEYTYPE_INT) {
         key.mv_data = &(pmeth->key.args[0].num.int32);
         key.mv_size = sizeof(pmeth->key.args[0].num.int32);
      }
      else if (pcon->key_type == DBX_KEYTYPE_STR) {
         key.mv_data = (void *) pmeth->key.args[0].svalue.buf_addr;
         key.mv_size = (size_t) pmeth->key.args[0].svalue.len_used;
      }
      else { /* mumps */
         key.mv_data = (void *) pmeth->key.ibuffer;
         key.mv_size = (size_t) pmeth->key.args[pmeth->key.argc - 1].csize;

         memcpy((void *) pmeth->output_key.svalue.buf_addr, (void *) pmeth->key.ibuffer, (size_t) pmeth->key.args[pmeth->key.argc - 1].csize);
         key0.mv_data = (void *) pmeth->output_key.svalue.buf_addr;
         key0.mv_size = (size_t) pmeth->key.args[pmeth->key.argc - 1].csize;
      }

      data.mv_data = (void *) pmeth->output_val.svalue.buf_addr;

      rc = lmdb_start_ro_transaction(pmeth, 0);
      rc = pcon->p_lmdb_so->p_mdb_get(pcon->p_lmdb_so->ptxnro, pcon->p_lmdb_so->db, &key, &data);
      pmeth->output_val.svalue.len_used = (unsigned int) data.mv_size;

      if (rc == MDB_NOTFOUND) {
         rc = CACHE_ERUNDEF;
         n = 0;
      }
      else {
         n = 1;
      }

      if (pcon->key_type == DBX_KEYTYPE_M) {
         rc = pcon->p_lmdb_so->p_mdb_cursor_open(pcon->p_lmdb_so->ptxnro, pcon->p_lmdb_so->db, &pcursor);
         if (rc == CACHE_SUCCESS) {
/*
            printf("\r\ndbx_defined: n=%d; key.mv_size=%d; pmeth->key.argc=%d; pmeth->key.args[pmeth->key.argc - 1].csize=%d", n, (int) key.mv_size, pmeth->key.argc, pmeth->key.args[pmeth->key.argc - 1].csize);
*/
            rc =  pcon->p_lmdb_so->p_mdb_cursor_get(pcursor, &key, &data, MDB_SET_RANGE);

/*
            printf("\r\ndbx_defined: DB_SET_RANGE: n=%d; rc=%d key.mv_size=%d; pmeth->key.argc=%d; pmeth->key.args[pmeth->key.argc - 1].csize=%d", n, rc, (int) key.mv_size, pmeth->key.argc, pmeth->key.args[pmeth->key.argc - 1].csize);
            dbx_dump_key((char *) key.mv_data, (int) key.mv_size);
*/
            if (rc == CACHE_SUCCESS) {
               if (n && key.mv_size == pmeth->key.args[pmeth->key.argc - 1].csize && !lmdb_key_compare(&key, &key0, (int) pmeth->key.args[pmeth->key.argc - 1].csize, pcon->key_type)) { /* current record defined, get next */
                  rc = pcon->p_lmdb_so->p_mdb_cursor_get(pcursor, &key, &data, MDB_NEXT);
/*
                  printf("\r\ndbx_defined: DB_NEXT: n=%d; rc=%d key.mv_size=%d; pmeth->key.argc=%d; pmeth->key.args[pmeth->key.argc - 1].csize=%d", n, rc, (int) key.mv_size, pmeth->key.argc, pmeth->key.args[pmeth->key.argc - 1].csize);
                  dbx_dump_key((char *) key.mv_data, (int) key.mv_size);
*/
               }
               if (rc == CACHE_SUCCESS && key.mv_size > pmeth->key.args[pmeth->key.argc - 1].csize && !lmdb_key_compare(&key, &key0, (int) pmeth->key.args[pmeth->key.argc - 1].csize, pcon->key_type)) {
                  n += 10;
               }
            }
            pcon->p_lmdb_so->p_mdb_cursor_close(pcursor);
         }
      }
      rc = lmdb_commit_ro_transaction(pmeth, 0);
      rc = CACHE_SUCCESS;
   }


   if (rc == CACHE_SUCCESS || rc == CACHE_ERUNDEF) {
      dbx_create_string(&(pmeth->output_val.svalue), (void *) &n, DBX_DTYPE_INT);
   }
   else {
      dbx_error_message(pmeth, rc, (char *) "dbx_defined");
   }

dbx_defined_exit:

   DBX_DB_UNLOCK(rc);

   return 0;

#ifdef _WIN32
}
__except (EXCEPTION_EXECUTE_HANDLER) {

   DWORD code;
   char bufferx[256];

   __try {
      code = GetExceptionCode();
      sprintf_s(bufferx, 255, "Exception caught in f:dbx_defined: %x", code);
      dbx_log_event(pcon, bufferx, "Error Condition", 0);
   }
   __except (EXCEPTION_EXECUTE_HANDLER) {
      ;
   }

   return 0;
}
#endif
}



int dbx_delete(DBXMETH *pmeth)
{
   int rc, n;
   DBXCON *pcon = pmeth->pcon;
   DBT key, key0, data;
   DBC *pcursor;

#ifdef _WIN32
__try {
#endif

   DBX_DB_LOCK(rc, 0);

   rc = dbx_global_reference(pmeth);
   if (rc != CACHE_SUCCESS) {
      dbx_error_message(pmeth, rc, (char *) "dbx_delete");
      goto dbx_delete_exit;
   }

   n = 0;
   if (pcon->dbtype == DBX_DBTYPE_BDB) {
      memset(&key, 0, sizeof(DBT));
      memset(&key0, 0, sizeof(DBT));
      memset(&data, 0, sizeof(DBT));
      key.flags = DB_DBT_USERMEM;
      key0.flags = DB_DBT_USERMEM;
      data.flags = DB_DBT_USERMEM;

      if (pcon->key_type == DBX_KEYTYPE_INT) {
         key.data = &(pmeth->key.args[0].num.int32);
         key.size = sizeof(pmeth->key.args[0].num.int32);
      }
      else if (pcon->key_type == DBX_KEYTYPE_STR) {
         key.data = (void *) pmeth->key.args[0].svalue.buf_addr;
         key.size = (u_int32_t) pmeth->key.args[0].svalue.len_used;
      }
      else { /* mumps */
         key.data = (void *) pmeth->key.ibuffer;
         key.size = (u_int32_t) pmeth->key.args[pmeth->key.argc - 1].csize;
         key.ulen = (u_int32_t) pmeth->key.ibuffer_size;

         memcpy((void *) pmeth->output_key.svalue.buf_addr, (void *) pmeth->key.ibuffer, (size_t) pmeth->key.args[pmeth->key.argc - 1].csize);
         key0.data = (void *) pmeth->output_key.svalue.buf_addr;
         key0.size = (u_int32_t) pmeth->key.args[pmeth->key.argc - 1].csize;
         key0.ulen = (u_int32_t) pmeth->output_key.svalue.len_alloc;
      }

      data.data = (void *) pmeth->output_val.svalue.buf_addr;
      data.ulen = (u_int32_t)  pmeth->output_val.svalue.len_alloc;

      rc = pcon->p_bdb_so->pdb->del(pcon->p_bdb_so->pdb, NULL, &key, 0);
      n = rc;

      if (pcon->key_type == DBX_KEYTYPE_M) {
         rc = pcon->p_bdb_so->pdb->cursor(pcon->p_bdb_so->pdb, NULL, &pcursor, 0);
         if (rc == CACHE_SUCCESS) {
/*
            printf("\r\nkey.ulen=%d; pmeth->key.argc=%d; pmeth->key.args[pmeth->key.argc - 1].csize=%d", (int) key.ulen, pmeth->key.argc, pmeth->key.args[pmeth->key.argc - 1].csize);
*/
            rc = pcursor->get(pcursor, &key, &data, DB_SET_RANGE);
/*
            printf("\r\nrc=%d key.size=%d; pmeth->key.argc=%d; pmeth->key.args[pmeth->key.argc - 1].csize=%d", rc, (int) key.size, pmeth->key.argc, pmeth->key.args[pmeth->key.argc - 1].csize);
            dbx_dump_key((char *) key.data, (int) key.size);
*/
            if (rc == CACHE_SUCCESS && !bdb_key_compare(&key, &key0, (int) pmeth->key.args[pmeth->key.argc - 1].csize, pcon->key_type)) {
               for (;;) {
                  /* dbx_dump_key((char *) key.data, (int) key.size); */
                  rc = pcon->p_bdb_so->pdb->del(pcon->p_bdb_so->pdb, NULL, &key, 0);
                  rc = pcursor->get(pcursor, &key, &data, DB_NEXT);
                  if (rc != CACHE_SUCCESS || bdb_key_compare(&key, &key0, (int) pmeth->key.args[pmeth->key.argc - 1].csize, pcon->key_type)) {
                     break;
                  }
               }
            }
            pcursor->close(pcursor);
         }
         rc = CACHE_SUCCESS;
      }
   }
   else if (pcon->dbtype == DBX_DBTYPE_LMDB) {
      MDB_val key, key0, data;
      MDB_cursor *pcursor;

      if (pcon->key_type == DBX_KEYTYPE_INT) {
         key.mv_data = &(pmeth->key.args[0].num.int32);
         key.mv_size = sizeof(pmeth->key.args[0].num.int32);
      }
      else if (pcon->key_type == DBX_KEYTYPE_STR) {
         key.mv_data = (void *) pmeth->key.args[0].svalue.buf_addr;
         key.mv_size = (size_t) pmeth->key.args[0].svalue.len_used;
      }
      else { /* mumps */
         key.mv_data = (void *) pmeth->key.ibuffer;
         key.mv_size = (size_t) pmeth->key.args[pmeth->key.argc - 1].csize;

         memcpy((void *) pmeth->output_key.svalue.buf_addr, (void *) pmeth->key.ibuffer, (size_t) pmeth->key.args[pmeth->key.argc - 1].csize);
         key0.mv_data = (void *) pmeth->output_key.svalue.buf_addr;
         key0.mv_size = (size_t) pmeth->key.args[pmeth->key.argc - 1].csize;
      }

      data.mv_data = (void *) pmeth->output_val.svalue.buf_addr;

      rc = pcon->p_lmdb_so->p_mdb_txn_begin(pcon->p_lmdb_so->penv, NULL, 0, &(pcon->p_lmdb_so->ptxn));
      if (rc != 0) {
         strcpy(pcon->error, "Cannot create or open a LMDB transaction for a delete operation");
         dbx_error_message(pmeth, rc, (char *) "dbx_delete");
         goto dbx_delete_exit;
      }
      pcon->tlevel ++;

      rc = pcon->p_lmdb_so->p_mdb_del(pcon->p_lmdb_so->ptxn, pcon->p_lmdb_so->db, &key, NULL);
      n = rc;

      if (pcon->key_type == DBX_KEYTYPE_M) {
         rc = pcon->p_lmdb_so->p_mdb_cursor_open(pcon->p_lmdb_so->ptxn, pcon->p_lmdb_so->db, &pcursor);

         if (rc == CACHE_SUCCESS) {
/*
            printf("\r\npmeth->key.argc=%d; pmeth->key.args[pmeth->key.argc - 1].csize=%d", pmeth->key.argc, pmeth->key.args[pmeth->key.argc - 1].csize);
*/
            rc =  pcon->p_lmdb_so->p_mdb_cursor_get(pcursor, &key, &data, MDB_SET_RANGE);

/*
            printf("\r\nrc=%d key.mv_size=%d; pmeth->key.argc=%d; pmeth->key.args[pmeth->key.argc - 1].csize=%d", rc, (int) key.mv_size, pmeth->key.argc, pmeth->key.args[pmeth->key.argc - 1].csize);
            dbx_dump_key((char *) key.mv_data, (int) key.mv_size);
*/
            if (rc == CACHE_SUCCESS && !lmdb_key_compare(&key, &key0, (int) pmeth->key.args[pmeth->key.argc - 1].csize, pcon->key_type)) {
               for (;;) {
                  /* dbx_dump_key((char *) key.data, (int) key.size); */
                  rc = pcon->p_lmdb_so->p_mdb_del(pcon->p_lmdb_so->ptxn, pcon->p_lmdb_so->db, &key, NULL);

                  rc =  pcon->p_lmdb_so->p_mdb_cursor_get(pcursor, &key, &data, MDB_NEXT);

                  if (rc != CACHE_SUCCESS || lmdb_key_compare(&key, &key0, (int) pmeth->key.args[pmeth->key.argc - 1].csize, pcon->key_type)) {
                     break;
                  }
               }
            }
            pcon->p_lmdb_so->p_mdb_cursor_close(pcursor);
         }
         rc = CACHE_SUCCESS;
      }
      rc = pcon->p_lmdb_so->p_mdb_txn_commit(pcon->p_lmdb_so->ptxn);
      pcon->tlevel --;
   }

   if (rc == CACHE_SUCCESS) {
      dbx_create_string(&(pmeth->output_val.svalue), (void *) &n, DBX_DTYPE_INT);
   }
   else {
      dbx_error_message(pmeth, rc, (char *) "dbx_delete");
   }

dbx_delete_exit:

   DBX_DB_UNLOCK(rc);

   return 0;

#ifdef _WIN32
}
__except (EXCEPTION_EXECUTE_HANDLER) {

   DWORD code;
   char bufferx[256];

   __try {
      code = GetExceptionCode();
      sprintf_s(bufferx, 255, "Exception caught in f:dbx_delete: %x", code);
      dbx_log_event(pcon, bufferx, "Error Condition", 0);
   }
   __except (EXCEPTION_EXECUTE_HANDLER) {
      ;
   }

   return 0;
}
#endif
}


int dbx_next(DBXMETH *pmeth)
{
   int rc;
   DBXCON *pcon = pmeth->pcon;

#ifdef _WIN32
__try {
#endif

   DBX_DB_LOCK(rc, 0);

   rc = dbx_global_reference(pmeth);
   if (rc != CACHE_SUCCESS) {
      dbx_error_message(pmeth, rc, (char *) "dbx_next");
      goto dbx_next_exit;
   }

   if (pcon->dbtype == DBX_DBTYPE_BDB) {
      rc = bdb_next(pmeth, &(pmeth->key), &(pmeth->output_val), &(pmeth->output_key), 0);
   }
   else if (pcon->dbtype == DBX_DBTYPE_LMDB) {
      rc = lmdb_next(pmeth, &(pmeth->key), &(pmeth->output_val), &(pmeth->output_key), 0);
   }

   if (rc != CACHE_SUCCESS) {
      dbx_error_message(pmeth, rc, (char *) "dbx_next");
   }

dbx_next_exit:

   DBX_DB_UNLOCK(rc);

   return 0;

#ifdef _WIN32
}
__except (EXCEPTION_EXECUTE_HANDLER) {

   DWORD code;
   char bufferx[256];

   __try {
      code = GetExceptionCode();
      sprintf_s(bufferx, 255, "Exception caught in f:dbx_next: %x", code);
      dbx_log_event(pcon, bufferx, "Error Condition", 0);
   }
   __except (EXCEPTION_EXECUTE_HANDLER) {
      ;
   }

   return 0;
}
#endif
}


int dbx_previous(DBXMETH *pmeth)
{
   int rc;
   DBXCON *pcon = pmeth->pcon;

#ifdef _WIN32
__try {
#endif

   DBX_DB_LOCK(rc, 0);

   rc = dbx_global_reference(pmeth);
   if (rc != CACHE_SUCCESS) {
      dbx_error_message(pmeth, rc, (char *) "dbx_previous");
      goto dbx_previous_exit;
   }

   if (pcon->dbtype == DBX_DBTYPE_BDB) {
      rc = bdb_previous(pmeth, &(pmeth->key), &(pmeth->output_val), &(pmeth->output_key), 0);
   }
   else if (pcon->dbtype == DBX_DBTYPE_LMDB) {
      rc = lmdb_previous(pmeth, &(pmeth->key), &(pmeth->output_val), &(pmeth->output_key), 0);
   }

   if (rc != CACHE_SUCCESS) {
      dbx_error_message(pmeth, rc, (char *) "dbx_previous");
   }

dbx_previous_exit:

   DBX_DB_UNLOCK(rc);

   return 0;

#ifdef _WIN32
}
__except (EXCEPTION_EXECUTE_HANDLER) {

   DWORD code;
   char bufferx[256];

   __try {
      code = GetExceptionCode();
      sprintf_s(bufferx, 255, "Exception caught in f:dbx_previous: %x", code);
      dbx_log_event(pcon, bufferx, "Error Condition", 0);
   }
   __except (EXCEPTION_EXECUTE_HANDLER) {
      ;
   }

   return 0;
}
#endif
}


int dbx_increment(DBXMETH *pmeth)
{
   int rc;
   double value;
   DBXCON *pcon = pmeth->pcon;

#ifdef _WIN32
__try {
#endif

   DBX_DB_LOCK(rc, 0);

   pmeth->increment = 1;
   rc = dbx_global_reference(pmeth);
   if (rc != CACHE_SUCCESS) {
      dbx_error_message(pmeth, rc, (char *) "dbx_increment");
      goto dbx_increment_exit;
   }

   if (pcon->dbtype == DBX_DBTYPE_BDB) {
      DBT key, data;

      memset(&key, 0, sizeof(DBT));
      memset(&data, 0, sizeof(DBT));
      key.flags = DB_DBT_USERMEM;
      data.flags = DB_DBT_USERMEM;

      if (pcon->key_type == DBX_KEYTYPE_INT) {
         key.data = &(pmeth->key.args[0].num.int32);
         key.size = sizeof(pmeth->key.args[0].num.int32);
      }
      else if (pcon->key_type == DBX_KEYTYPE_STR) {
         key.data = (void *) pmeth->key.args[0].svalue.buf_addr;
         key.size = (u_int32_t) pmeth->key.args[0].svalue.len_used;
      }
      else { /* mumps */
         key.data = (void *) pmeth->key.ibuffer;
         key.size = (u_int32_t) pmeth->key.args[pmeth->key.argc - 2].csize;
      }

      data.data = (void *) pmeth->output_val.svalue.buf_addr;
      data.ulen = (u_int32_t)  pmeth->output_val.svalue.len_alloc;
      data.size = 0;

      rc = pcon->p_bdb_so->pdb->get(pcon->p_bdb_so->pdb, NULL, &key, &data, 0);
      pmeth->output_val.svalue.len_used = data.size;
      pmeth->output_val.svalue.buf_addr[data.size] = '\0';

      value = (double) strtod(pmeth->output_val.svalue.buf_addr, NULL);
      value += pmeth->key.args[pmeth->key.argc - 1].num.real;
      sprintf(pmeth->output_val.svalue.buf_addr, "%g", value);
      pmeth->output_val.svalue.len_used = (unsigned int) strlen(pmeth->output_val.svalue.buf_addr);

      data.data = (void *) pmeth->output_val.svalue.buf_addr;
      data.size = (u_int32_t) pmeth->output_val.svalue.len_used;
      data.ulen = (u_int32_t) pmeth->output_val.svalue.len_alloc;
      rc = pcon->p_bdb_so->pdb->put(pcon->p_bdb_so->pdb, NULL, &key, &data, 0);
   }
   else if (pcon->dbtype == DBX_DBTYPE_LMDB) {
      MDB_val key, data;

      if (pcon->key_type == DBX_KEYTYPE_INT) {
         key.mv_data = &(pmeth->key.args[0].num.int32);
         key.mv_size = sizeof(pmeth->key.args[0].num.int32);
      }
      else if (pcon->key_type == DBX_KEYTYPE_STR) {
         key.mv_data = (void *) pmeth->key.args[0].svalue.buf_addr;
         key.mv_size = (size_t) pmeth->key.args[0].svalue.len_used;
      }
      else { /* mumps */
         key.mv_data = (void *) pmeth->key.ibuffer;
         key.mv_size = (size_t) pmeth->key.args[pmeth->key.argc - 2].csize;
      }

      data.mv_data = (void *) pmeth->output_val.svalue.buf_addr;
      data.mv_size = (size_t)  pmeth->output_val.svalue.len_alloc;

      rc = lmdb_start_ro_transaction(pmeth, 0); /* v1.2.8 */
      rc = pcon->p_lmdb_so->p_mdb_get(pcon->p_lmdb_so->ptxnro, pcon->p_lmdb_so->db, &key, &data);
      lmdb_commit_ro_transaction(pmeth, 0); /* v1.2.8 */

      if (rc == CACHE_SUCCESS) { /* v1.2.8 */
         memcpy((void *) pmeth->output_val.svalue.buf_addr, (void *) data.mv_data, data.mv_size);
      }
      pmeth->output_val.svalue.len_used = (unsigned int) data.mv_size;
      pmeth->output_val.svalue.buf_addr[data.mv_size] = '\0';

      value = (double) strtod(pmeth->output_val.svalue.buf_addr, NULL);
      value += pmeth->key.args[pmeth->key.argc - 1].num.real;
      sprintf(pmeth->output_val.svalue.buf_addr, "%g", value);
      pmeth->output_val.svalue.len_used = (unsigned int) strlen(pmeth->output_val.svalue.buf_addr);

      data.mv_data = (void *) pmeth->output_val.svalue.buf_addr;
      data.mv_size = (size_t) pmeth->output_val.svalue.len_used;

      /* v1.2.8 */
      rc = pcon->p_lmdb_so->p_mdb_txn_begin(pcon->p_lmdb_so->penv, NULL, 0, &(pcon->p_lmdb_so->ptxn));
      if (rc != 0) {
         strcpy(pcon->error, "Cannot create or open a LMDB transaction for an update operation");
         dbx_error_message(pmeth, rc, (char *) "dbx_increment");
         goto dbx_increment_exit;
      }
      rc = pcon->p_lmdb_so->p_mdb_put(pcon->p_lmdb_so->ptxn, pcon->p_lmdb_so->db, &key, &data, 0);
      pcon->tlevel ++;

      pcon->p_lmdb_so->p_mdb_txn_commit(pcon->p_lmdb_so->ptxn);
      pcon->tlevel --;
   }

   if (rc != CACHE_SUCCESS) {
      dbx_error_message(pmeth, rc, (char *) "dbx_increment");
   }

dbx_increment_exit:

   DBX_DB_UNLOCK(rc);

   return 0;

#ifdef _WIN32
}
__except (EXCEPTION_EXECUTE_HANDLER) {

   DWORD code;
   char bufferx[256];

   __try {
      code = GetExceptionCode();
      sprintf_s(bufferx, 255, "Exception caught in f:dbx_increment: %x", code);
      dbx_log_event(pcon, bufferx, "Error Condition", 0);
   }
   __except (EXCEPTION_EXECUTE_HANDLER) {
      ;
   }

   return 0;
}
#endif
}


int dbx_lock(DBXMETH *pmeth)
{
   int rc, retval, timeout;
   unsigned long long timeout_nsec;
   char buffer[32];
   DBXCON *pcon = pmeth->pcon;

#ifdef _WIN32
__try {
#endif

   DBX_DB_LOCK(rc, 0);

   pmeth->lock = 1;
   rc = dbx_global_reference(pmeth);
   if (rc != CACHE_SUCCESS) {
      dbx_error_message(pmeth, rc, (char *) "dbx_lock");
      goto dbx_lock_exit;
   }

   if (pcon->dbtype == DBX_DBTYPE_BDB) {
      timeout = -1;
      if (pmeth->key.args[pmeth->key.argc - 1].svalue.len_used < 16) {
         strncpy(buffer, pmeth->key.args[pmeth->key.argc - 1].svalue.buf_addr, pmeth->key.args[pmeth->key.argc - 1].svalue.len_used);
         buffer[pmeth->key.args[pmeth->key.argc - 1].svalue.len_used] = '\0';
         timeout = (int) strtol(buffer, NULL, 10);
      }
      timeout_nsec = 1000000000;

      if (timeout < 0)
         timeout_nsec *= 3600;
      else
         timeout_nsec *= timeout;
      rc = YDB_OK;
      if (rc == YDB_OK) {
         retval = 1;
         rc = YDB_OK;
      }
      else if (rc == YDB_LOCK_TIMEOUT) {
         retval = 0;
         rc = YDB_OK;
      }
      else {
         retval = 0;
         rc = CACHE_FAILURE;
      }
   }
   else if (pcon->dbtype == DBX_DBTYPE_LMDB) {
      timeout = -1;
      if (pmeth->key.args[pmeth->key.argc - 1].svalue.len_used < 16) {
         strncpy(buffer, pmeth->key.args[pmeth->key.argc - 1].svalue.buf_addr, pmeth->key.args[pmeth->key.argc - 1].svalue.len_used);
         buffer[pmeth->key.args[pmeth->key.argc - 1].svalue.len_used] = '\0';
         timeout = (int) strtol(buffer, NULL, 10);
      }
      timeout_nsec = 1000000000;

      if (timeout < 0)
         timeout_nsec *= 3600;
      else
         timeout_nsec *= timeout;
      rc = YDB_OK;
      if (rc == YDB_OK) {
         retval = 1;
         rc = YDB_OK;
      }
      else if (rc == YDB_LOCK_TIMEOUT) {
         retval = 0;
         rc = YDB_OK;
      }
      else {
         retval = 0;
         rc = CACHE_FAILURE;
      }
   }


   if (rc == CACHE_SUCCESS) {
      dbx_create_string(&(pmeth->output_val.svalue), (void *) &retval, DBX_DTYPE_INT);
   }
   else {
      dbx_error_message(pmeth, rc, (char *) "dbx_lock");
   }

dbx_lock_exit:

   DBX_DB_UNLOCK(rc);

   return 0;

#ifdef _WIN32
}
__except (EXCEPTION_EXECUTE_HANDLER) {

   DWORD code;
   char bufferx[256];

   __try {
      code = GetExceptionCode();
      sprintf_s(bufferx, 255, "Exception caught in f:dbx_lock: %x", code);
      dbx_log_event(pcon, bufferx, "Error Condition", 0);
   }
   __except (EXCEPTION_EXECUTE_HANDLER) {
      ;
   }

   return 0;
}
#endif
}


int dbx_unlock(DBXMETH *pmeth)
{
   int rc, retval;
   DBXCON *pcon = pmeth->pcon;

#ifdef _WIN32
__try {
#endif

   DBX_DB_LOCK(rc, 0);

   pmeth->lock = 2;
   rc = dbx_global_reference(pmeth);
   if (rc != CACHE_SUCCESS) {
      dbx_error_message(pmeth, rc, (char *) "dbx_unlock");
      goto dbx_unlock_exit;
   }

   if (pcon->dbtype == DBX_DBTYPE_BDB) {
      rc = YDB_OK;
      if (rc == YDB_OK)
         retval = 1;
      else
         retval = 0;
   }
   else if (pcon->dbtype == DBX_DBTYPE_LMDB) {
      rc = YDB_OK;
      if (rc == YDB_OK)
         retval = 1;
      else
         retval = 0;
   }


   if (rc == CACHE_SUCCESS) {
      dbx_create_string(&(pmeth->output_val.svalue), (void *) &retval, DBX_DTYPE_INT);
   }
   else {
      dbx_error_message(pmeth, rc, (char *) "dbx_unlock");
   }

dbx_unlock_exit:

   DBX_DB_UNLOCK(rc);

   return 0;

#ifdef _WIN32
}
__except (EXCEPTION_EXECUTE_HANDLER) {

   DWORD code;
   char bufferx[256];

   __try {
      code = GetExceptionCode();
      sprintf_s(bufferx, 255, "Exception caught in f:dbx_unlock: %x", code);
      dbx_log_event(pcon, bufferx, "Error Condition", 0);
   }
   __except (EXCEPTION_EXECUTE_HANDLER) {
      ;
   }

   return 0;
}
#endif
}


int dbx_merge(DBXMETH *pmeth)
{
   int rc, n, ref1, ref1_csize, ref2_csize;
   unsigned char ref2_fixed[1024];
   DBXCON *pcon = pmeth->pcon;

#ifdef _WIN32
__try {
#endif
/*
   printf("\r\nlen=%d\r\n", pmeth->key.ibuffer_used);
   dbx_dump_key((char *) pmeth->key.ibuffer, (int) pmeth->key.ibuffer_used);
*/
   DBX_DB_LOCK(rc, 0);

   ref1_csize = 0;
   ref2_csize = 0;

   rc = dbx_global_reference(pmeth);
   if (rc != CACHE_SUCCESS) {
      dbx_error_message(pmeth, rc, (char *) "dbx_merge");
      goto dbx_merge_exit;
   }

   ref1 = 0;
   n = 0;
   if (pcon->dbtype == DBX_DBTYPE_BDB) {
      DBT key, key0, key2, data;
      DBC *pcursor;

      memset(&key, 0, sizeof(DBT));
      memset(&key0, 0, sizeof(DBT));
      memset(&key2, 0, sizeof(DBT));
      memset(&data, 0, sizeof(DBT));
      key.flags = DB_DBT_USERMEM;
      key0.flags = DB_DBT_USERMEM;
      key2.flags = DB_DBT_USERMEM;
      data.flags = DB_DBT_USERMEM;

      if (pcon->key_type == DBX_KEYTYPE_INT) {
         key.data = &(pmeth->key.args[0].num.int32);
         key.size = sizeof(pmeth->key.args[0].num.int32);
      }
      else if (pcon->key_type == DBX_KEYTYPE_STR) {
         key.data = (void *) pmeth->key.args[0].svalue.buf_addr;
         key.size = (u_int32_t) pmeth->key.args[0].svalue.len_used;
      }
      else { /* mumps */
         for (n = 1; n < pmeth->jsargc; n ++) {
            if (pmeth->key.args[n].sort == DBX_DSORT_GLOBAL) {
               ref1 = n;
               break;
            }
         }
         ref1_csize = pmeth->key.args[pmeth->jsargc - 1].csize - pmeth->key.args[ref1 - 1].csize;
         ref2_csize = pmeth->key.args[ref1 - 1].csize;

         key.data = (void *) (pmeth->key.ibuffer + ref2_csize);
         key.size = (u_int32_t) ref1_csize;
         key.ulen = (u_int32_t) pmeth->key.ibuffer_size;
/*
         printf("\r\nref2=%d; ref1=%d; key.size=%d; ref1_csize=%d; ref2_csize=%d", ref2, ref1, (int) key.size, ref1_csize, ref2_csize);
         dbx_dump_key((char *) key.data, (int) key.size);
*/
         memcpy((void *) pmeth->output_key.svalue.buf_addr, (void *) (pmeth->key.ibuffer + ref2_csize), (size_t) ref1_csize);
         key0.data = (void *) pmeth->output_key.svalue.buf_addr;
         key0.size = (u_int32_t) ref1_csize;
         key0.ulen = (u_int32_t) pmeth->output_key.svalue.len_alloc;

         memcpy((void *) ref2_fixed, (void *) pmeth->key.ibuffer, (size_t) ref2_csize);
      }

      data.data = (void *) pmeth->output_val.svalue.buf_addr;
      data.ulen = (u_int32_t)  pmeth->output_val.svalue.len_alloc;

      if (pcon->key_type == DBX_KEYTYPE_M) {
         rc = pcon->p_bdb_so->pdb->cursor(pcon->p_bdb_so->pdb, NULL, &pcursor, 0);
         if (rc == CACHE_SUCCESS) {
/*
            printf("\r\nkey.ulen=%d; pmeth->key.argc=%d; pmeth->key.args[pmeth->key.argc - 1].csize=%d", (int) key.ulen, pmeth->key.argc, pmeth->key.args[pmeth->key.argc - 1].csize);
*/
            rc = pcursor->get(pcursor, &key, &data, DB_SET_RANGE);
/*
            printf("\r\nrc=%d key.size=%d; pmeth->key.argc=%d; pmeth->key.args[pmeth->key.argc - 1].csize=%d; key0.size=%d;", rc, (int) key.size, pmeth->key.argc, pmeth->key.args[pmeth->key.argc - 1].csize, (int) key0.size);
            dbx_dump_key((char *) key.data, (int) key.size);
*/
            if (rc == CACHE_SUCCESS && !bdb_key_compare(&key, &key0, (int) key0.size, pcon->key_type)) {
/*
            printf("\r\nrc=%d; key0.size=%d; comp=%d", rc, key0.size, bdb_key_compare(&key, &key0, (int) key0.size, pcon->key_type));
*/
               for (;;) {
/*
                  dbx_dump_key((char *) key.data, (int) key.size);
*/
                  memcpy((void *) (ref2_fixed + ref2_csize), (void *) ((char *) key.data + ref1_csize), (size_t) (key.size - ref1_csize));
/*
                  dbx_dump_key((char *) ref2_fixed, (int) (ref2_csize + (key.size - ref1_csize)));
*/
                  key2.data = (void *) ref2_fixed;
                  key2.size = (u_int32_t) (ref2_csize + (key.size - ref1_csize));
                  key2.ulen = 1024;
                  rc = pcon->p_bdb_so->pdb->put(pcon->p_bdb_so->pdb, NULL, &key2, &data, 0);

                  rc = pcursor->get(pcursor, &key, &data, DB_NEXT);

                  if (rc != CACHE_SUCCESS || bdb_key_compare(&key, &key0, (int) key0.size, pcon->key_type)) {
                     break;
                  }
               }
            }
            pcursor->close(pcursor);
         }
         rc = CACHE_SUCCESS;
      }
   }
   else if (pcon->dbtype == DBX_DBTYPE_LMDB) {
      MDB_val key, key0, key2, data;
      MDB_cursor *pcursor;

      if (pcon->key_type == DBX_KEYTYPE_INT) {
         key.mv_data = &(pmeth->key.args[0].num.int32);
         key.mv_size = sizeof(pmeth->key.args[0].num.int32);
      }
      else if (pcon->key_type == DBX_KEYTYPE_STR) {
         key.mv_data = (void *) pmeth->key.args[0].svalue.buf_addr;
         key.mv_size = (size_t) pmeth->key.args[0].svalue.len_used;
      }
      else { /* mumps */
         for (n = 1; n < pmeth->jsargc; n ++) {
            if (pmeth->key.args[n].sort == DBX_DSORT_GLOBAL) {
               ref1 = n;
               break;
            }
         }
         ref1_csize = pmeth->key.args[pmeth->jsargc - 1].csize - pmeth->key.args[ref1 - 1].csize;
         ref2_csize = pmeth->key.args[ref1 - 1].csize;

         key.mv_data = (void *) (pmeth->key.ibuffer + ref2_csize);
         key.mv_size = (size_t) ref1_csize;
/*
         printf("\r\nref2=%d; ref1=%d; key.mv_size=%d; ref1_csize=%d; ref2_csize=%d", ref2, ref1, (int) key.mv_size, ref1_csize, ref2_csize);
         dbx_dump_key((char *) key.mv_data, (int) key.mv_size);
*/
         memcpy((void *) pmeth->output_key.svalue.buf_addr, (void *) (pmeth->key.ibuffer + ref2_csize), (size_t) ref1_csize);
         key0.mv_data = (void *) pmeth->output_key.svalue.buf_addr;
         key0.mv_size = (size_t) ref1_csize;

         memcpy((void *) ref2_fixed, (void *) pmeth->key.ibuffer, (size_t) ref2_csize);
      }

      data.mv_data = (void *) pmeth->output_val.svalue.buf_addr;
      data.mv_size = (size_t)  pmeth->output_val.svalue.len_alloc;
/*
     rc = pcon->p_lmdb_so->p_mdb_cursor_open(pcon->p_lmdb_so->ptxn, pcon->p_lmdb_so->db, &pcursor);
         rc = pcon->p_lmdb_so->p_mdb_cursor_get(pcursor, &key, &data, MDB_SET_RANGE);
      rc = pcon->p_lmdb_so->p_mdb_put(pcon->p_lmdb_so->ptxn, pcon->p_lmdb_so->db, &key, &data, 0);
*/

      if (pcon->key_type == DBX_KEYTYPE_M) {
         rc = pcon->p_lmdb_so->p_mdb_txn_begin(pcon->p_lmdb_so->penv, NULL, 0, &(pcon->p_lmdb_so->ptxn));
         pcon->tlevel ++;
         rc = pcon->p_lmdb_so->p_mdb_cursor_open(pcon->p_lmdb_so->ptxn, pcon->p_lmdb_so->db, &pcursor);
         if (rc == CACHE_SUCCESS) {
/*
            printf("\r\npmeth->key.argc=%d; pmeth->key.args[pmeth->key.argc - 1].csize=%d", pmeth->key.argc, pmeth->key.args[pmeth->key.argc - 1].csize);
*/
            rc = pcon->p_lmdb_so->p_mdb_cursor_get(pcursor, &key, &data, MDB_SET_RANGE);
/*
            printf("\r\nrc=%d key.mv_size=%d; pmeth->key.argc=%d; pmeth->key.args[pmeth->key.argc - 1].csize=%d; key0.size=%d;", rc, (int) key.mv_size, pmeth->key.argc, pmeth->key.args[pmeth->key.argc - 1].csize, (int) key0.mv_size);
            dbx_dump_key((char *) key.mv_data, (int) key.mv_size);
*/
            if (rc == CACHE_SUCCESS && !lmdb_key_compare(&key, &key0, (int) key0.mv_size, pcon->key_type)) {
/*
            printf("\r\nrc=%d; key0.mv_size=%d; comp=%d", rc, key0.mv_size, lmdb_key_compare(&key, &key0, (int) key0.mv_size, pcon->key_type));
*/
               for (;;) {
/*
                  dbx_dump_key((char *) key.mv_data, (int) key.mv_size);
*/
                  memcpy((void *) (ref2_fixed + ref2_csize), (void *) ((char *) key.mv_data + ref1_csize), (size_t) (key.mv_size - ref1_csize));
/*
                  dbx_dump_key((char *) ref2_fixed, (int) (ref2_csize + (key.mv_size - ref1_csize)));
*/
                  key2.mv_data = (void *) ref2_fixed;
                  key2.mv_size = (size_t) (ref2_csize + (key.mv_size - ref1_csize));
                  rc = pcon->p_lmdb_so->p_mdb_put(pcon->p_lmdb_so->ptxn, pcon->p_lmdb_so->db, &key2, &data, 0);

                  rc = pcon->p_lmdb_so->p_mdb_cursor_get(pcursor, &key, &data, MDB_NEXT);

                  if (rc != CACHE_SUCCESS || lmdb_key_compare(&key, &key0, (int) key0.mv_size, pcon->key_type)) {
                     break;
                  }
               }
            }
            pcon->p_lmdb_so->p_mdb_cursor_close(pcursor);
            pcon->p_lmdb_so->p_mdb_txn_commit(pcon->p_lmdb_so->ptxn);
            pcon->tlevel --;
         }
         rc = CACHE_SUCCESS;
      }
   }

dbx_merge_exit:

   return 0;

#ifdef _WIN32
}
__except (EXCEPTION_EXECUTE_HANDLER) {

   DWORD code;
   char bufferx[256];

   __try {
      code = GetExceptionCode();
      sprintf_s(bufferx, 255, "Exception caught in f:dbx_merge: %x", code);
      dbx_log_event(pcon, bufferx, "Error Condition", 0);
   }
   __except (EXCEPTION_EXECUTE_HANDLER) {
      ;
   }

   return 0;
}
#endif

}


int dbx_sql_execute(DBXMETH *pmeth)
{
   return 0;
}


int dbx_sql_row(DBXMETH *pmeth, int rn, int dir)
{
   return 0;
}


int dbx_sql_cleanup(DBXMETH *pmeth)
{
   return 0;
}


int dbx_global_directory(DBXMETH *pmeth, DBXQR *pqr_prev, short dir, int *counter)
{
   int rc, eod;
   char buffer[256];
   DBXCON *pcon = pmeth->pcon;

#ifdef _WIN32
__try {
#endif

   if (pcon->log_transmissions) {
      int nx;
      v8::Local<v8::String> str;

      pmeth->key.ibuffer_used = 0;
      nx = 0;
      dbx_ibuffer_add(pmeth, &(pmeth->key), NULL, nx ++, str, pqr_prev->global_name.buf_addr, pqr_prev->global_name.len_used, 0);
      dbx_log_transmission(pcon, pmeth, (char *) (dir == 1 ? "mcursor::next (global directory)" : "mcursor::previous (global directory)"));
      pmeth->key.ibuffer_used = 0;
   }

   rc = 0;
   eod = 0;
   if (pqr_prev->global_name.buf_addr[0] != '^') {
      strcpy(buffer, "^");
      if (pqr_prev->global_name.len_used > 0) {
         strncpy(buffer + 1, pqr_prev->global_name.buf_addr, pqr_prev->global_name.len_used);
         pqr_prev->global_name.len_used ++;
         strncpy(pqr_prev->global_name.buf_addr, buffer, pqr_prev->global_name.len_used);
      }
      else {
         strcpy(pqr_prev->global_name.buf_addr, "^");
         pqr_prev->global_name.len_used = 1;
      }
   }

   if (pcon->dbtype == DBX_DBTYPE_BDB) {
     if (dir == 1) {
         rc = bdb_next(pmeth, &(pqr_prev->key), &(pmeth->output_val), &(pqr_prev->data), 1);
      }
      else {
         rc = bdb_previous(pmeth, &(pqr_prev->key), &(pmeth->output_val), &(pqr_prev->data), 1);
      }
   }
   else if (pcon->dbtype == DBX_DBTYPE_LMDB) {
     if (dir == 1) {
         rc = lmdb_next(pmeth, &(pqr_prev->key), &(pmeth->output_val), &(pqr_prev->data), 1);
      }
      else {
         rc = lmdb_previous(pmeth, &(pqr_prev->key), &(pmeth->output_val), &(pqr_prev->data), 1);
      }
   }

   if (rc == CACHE_SUCCESS) {
      strncpy(pqr_prev->global_name.buf_addr, pmeth->output_val.svalue.buf_addr, pmeth->output_val.svalue.len_used);
      pqr_prev->global_name.len_used = pmeth->output_val.svalue.len_used;
      pqr_prev->global_name.buf_addr[pqr_prev->global_name.len_used] = '\0';
   }
   else {
      eod = 1;
      /* dbx_error_message(pmeth, rc, (char *) "dbx_global_directory"); */
   }

   if (pcon->log_transmissions == 2) {
      if (eod)
         dbx_log_response(pcon, (char *) "[END]", (int) 5, (char *) (dir == 1 ? "mcursor::next (global directory)" : "mcursor::previous (global directory)"));
      else
         dbx_log_response(pcon, (char *) pqr_prev->global_name.buf_addr, (int) pqr_prev->global_name.len_used, (char *) (dir == 1 ? "mcursor::next (global directory)" : "mcursor::previous (global directory)"));
   }

   return eod;

#ifdef _WIN32
}
__except (EXCEPTION_EXECUTE_HANDLER) {

   DWORD code;
   char bufferx[256];

   __try {
      code = GetExceptionCode();
      sprintf_s(bufferx, 255, "Exception caught in f:dbx_global_directory: %x", code);
      dbx_log_event(pcon, bufferx, "Error Condition", 0);
   }
   __except (EXCEPTION_EXECUTE_HANDLER) {
      ;
   }

   return 0;
}
#endif
}


int dbx_global_order(DBXMETH *pmeth, DBXQR *pqr_prev, short dir, short getdata, int *counter)
{
   int rc, n, eod;
   DBXCON *pcon = pmeth->pcon;

#ifdef _WIN32
__try {
#endif

   rc = 0;
   eod = 0;
   if (pcon->log_transmissions) {
      int nx;
      v8::Local<v8::String> str;

      pmeth->key.ibuffer_used = 0;
      nx = 0;
      dbx_ibuffer_add(pmeth, &(pmeth->key), NULL, nx ++, str, pqr_prev->global_name.buf_addr, pqr_prev->global_name.len_used, 0);
      for (n = 0; n < pqr_prev->key.argc; n ++) {
         dbx_ibuffer_add(pmeth, &(pmeth->key), NULL, nx ++, str, pqr_prev->key.args[n].svalue.buf_addr, pqr_prev->key.args[n].svalue.len_used, 0);
      }
      dbx_log_transmission(pcon, pmeth, (char *) (dir == 1 ? "mcursor::next (order)" : "mcursor::previous (order)"));
      pmeth->key.ibuffer_used = 0;
   }

   if (pcon->dbtype == DBX_DBTYPE_BDB) {
      if (dir == 1) {
         rc = bdb_next(pmeth, &(pqr_prev->key), &(pmeth->output_val), &(pqr_prev->data), 1);
      }
      else {
         rc = bdb_previous(pmeth, &(pqr_prev->key), &(pmeth->output_val), &(pqr_prev->data), 1);
      }
   }
   else if (pcon->dbtype == DBX_DBTYPE_LMDB) {
      if (dir == 1) {
         rc = lmdb_next(pmeth, &(pqr_prev->key), &(pmeth->output_val), &(pqr_prev->data), 1);
      }
      else {
         rc = lmdb_previous(pmeth, &(pqr_prev->key), &(pmeth->output_val), &(pqr_prev->data), 1);
      }
   }

   if (rc != CACHE_SUCCESS && rc != YDB_NODE_END) {
      eod = 1;
      dbx_error_message(pmeth, rc, (char *) "dbx_global_order");
   }
   if (rc == YDB_NODE_END || pmeth->output_val.svalue.len_used == 0) {
      eod = 1;
   }

   if (pcon->log_transmissions == 2) {
      if (pmeth->output_val.svalue.len_used == 0)
         dbx_log_response(pcon, (char *) "[END]", (int) 5, (char *) (dir == 1 ? "mcursor::next (order)" : "mcursor::previous (order)"));
      else
         dbx_log_response(pcon, (char *) pmeth->output_val.svalue.buf_addr, (int) pmeth->output_val.svalue.len_used, (char *) (dir == 1 ? "mcursor::next (order)" : "mcursor::previous (order)"));
   }

   return eod;

#ifdef _WIN32
}
__except (EXCEPTION_EXECUTE_HANDLER) {

   DWORD code;
   char bufferx[256];

   __try {
      code = GetExceptionCode();
      sprintf_s(bufferx, 255, "Exception caught in f:dbx_global_order: %x", code);
      dbx_log_event(pcon, bufferx, "Error Condition", 0);
   }
   __except (EXCEPTION_EXECUTE_HANDLER) {
      ;
   }

   return 0;
}
#endif
}


int dbx_global_query(DBXMETH *pmeth, DBXQR *pqr_next, DBXQR *pqr_prev, short dir, short getdata, int *fixed_key_len, int *counter)
{
   int rc, n, eod;
   DBXCON *pcon = pmeth->pcon;

#ifdef _WIN32
__try {
#endif

   eod = 0;
   if (pcon->log_transmissions) {
      int nx;
      v8::Local<v8::String> str;

      pmeth->key.ibuffer_used = 0;
      nx = 0;
      dbx_ibuffer_add(pmeth, &(pmeth->key), NULL, nx ++, str, pqr_prev->global_name.buf_addr, pqr_prev->global_name.len_used, 0);
      for (n = 0; n < pqr_prev->key.argc; n ++) {
         dbx_ibuffer_add(pmeth, &(pmeth->key), NULL, nx ++, str, pqr_prev->key.args[n].svalue.buf_addr, pqr_prev->key.args[n].svalue.len_used, 0);
      }
      dbx_log_transmission(pcon, pmeth, (char *) (dir == 1 ? "mcursor::next (query)" : "mcursor::previous (query)"));
      pmeth->key.ibuffer_used = 0;
   }

   if (pcon->dbtype == DBX_DBTYPE_BDB) {
      DBT key, key0, data;

      memset(&key, 0, sizeof(DBT));
      memset(&key0, 0, sizeof(DBT));
      memset(&data, 0, sizeof(DBT));
      key.flags = DB_DBT_USERMEM;
      key0.flags = DB_DBT_USERMEM;
      data.flags = DB_DBT_USERMEM;

      rc = CACHE_SUCCESS;
      if (pcon->key_type == DBX_KEYTYPE_INT) {
         key0.data = &(pqr_prev->key.args[0].num.int32);
         key0.size = sizeof(pqr_prev->key.args[0].num.int32);
         key0.ulen = sizeof(pqr_prev->key.args[0].num.int32);

         pqr_next->key.args[0].num.int32 = pqr_prev->key.args[0].num.int32;
         key.data = &(pqr_next->key.args[0].num.int32);
         key.size = sizeof(pqr_next->key.args[0].num.int32);
         key.ulen = sizeof(pqr_next->key.args[0].num.int32);
      }
      else if (pcon->key_type == DBX_KEYTYPE_STR) {
         if ((*counter) == 0) {
            key0.data = (void *) pqr_prev->key.ibuffer;
            key0.size = (u_int32_t) pqr_prev->key.ibuffer_used;
            key0.ulen = (u_int32_t) pqr_prev->key.ibuffer_size;

            memcpy((void *) pqr_next->key.ibuffer, (void *) pqr_prev->key.ibuffer, (size_t) pqr_prev->key.ibuffer_used);
            pqr_next->key.ibuffer_used = pqr_prev->key.ibuffer_used;
         }
         key.data = (void *) pqr_next->key.ibuffer;
         key.size = (u_int32_t) pqr_next->key.ibuffer_used;
         key.ulen = (u_int32_t) pqr_next->key.ibuffer_size;
      }
      else { /* mumps */
         key0.data = (void *) pqr_prev->key.ibuffer;
         key0.size = (u_int32_t) pqr_prev->key.ibuffer_used;
         key0.ulen = (u_int32_t) pqr_prev->key.ibuffer_size;

         memcpy((void *) pqr_next->key.ibuffer, (void *) pqr_prev->key.ibuffer, (size_t) pqr_prev->key.ibuffer_used);
         pqr_next->key.ibuffer_used = pqr_prev->key.ibuffer_used;
         key.data = (void *) pqr_next->key.ibuffer;
         key.size = (u_int32_t) pqr_next->key.ibuffer_used;
         key.ulen = (u_int32_t) pqr_next->key.ibuffer_size;
      }

      data.data = (void *) pqr_next->data.svalue.buf_addr;
      data.ulen = (u_int32_t) pqr_next->data.svalue.len_alloc;

      if (dir == 1) { /* get next */

         if (pcon->key_type == DBX_KEYTYPE_M) {
            /* dbx_dump_key((char *) key0.data, (int) key0.size); */
            pqr_next->data.svalue.len_used = 0;
            for (n = (*counter); n < ((*counter) + 5); n ++) {
               if (n == 0)
                  rc = pmeth->pbdbcursor->get(pmeth->pbdbcursor, &key, &data, DB_SET_RANGE);
               else
                  rc = pmeth->pbdbcursor->get(pmeth->pbdbcursor, &key, &data, DB_NEXT);

               if (rc != CACHE_SUCCESS) {
                  pqr_next->key.args[0].svalue.len_used = 0;
                  break;
               }
/*
               printf("\r\ndbx_global_query (forward): n=%d; counter=%d; key.size=%d; pqr_prev->key.argc=%d", n, *counter, (int) key.size, pqr_prev->key.argc);
               dbx_dump_key((char *) key.data, (int) key.size);
*/
               if (!bdb_key_compare(&key, &key0, *(fixed_key_len), pcon->key_type)) {
                  if (!bdb_key_compare(&key, &key0, 0, pcon->key_type)) { /* current key returned - get next */
                     continue;
                  }
                  pqr_next->key.argc = dbx_split_key(&(pqr_next->key.args[0]), (char *) key.data, (int) key.size);
                  pqr_next->key.ibuffer_used = key.size;
                  pqr_next->data.svalue.len_used = data.size;
                  break;
               }
               else {
                  pqr_next->key.args[0].svalue.len_used = 0;
                  break;
               }
            }
         }
         else { /* not mumps */
/*
            printf("\r\ndbx_global_query (forward): counter=%d; key.size=%d; pqr_prev->key.argc=%d", *counter, (int) key.size, pqr_prev->key.argc);
            dbx_dump_key((char *) key.data, (int) key.size);
*/
            if ((*counter) == 0) {
               if (key.size == 0) {
                  rc = pmeth->pbdbcursor->get(pmeth->pbdbcursor, &key, &data, DB_FIRST);
               }
               else {
                  rc = pmeth->pbdbcursor->get(pmeth->pbdbcursor, &key, &data, DB_SET_RANGE);
                  if (rc == CACHE_SUCCESS && !bdb_key_compare(&key, &key0, 0, pcon->key_type)) {
                     rc = pmeth->pbdbcursor->get(pmeth->pbdbcursor, &key, &data, DB_NEXT);
                  }
               }
            }
            else {
               rc = pmeth->pbdbcursor->get(pmeth->pbdbcursor, &key, &data, DB_NEXT);
            }

            if (rc == CACHE_SUCCESS) {
               if (pcon->key_type == DBX_KEYTYPE_STR) {
                  pqr_next->key.ibuffer_used = (unsigned int) key.size;
                  pqr_next->key.args[0].svalue.len_used = (unsigned int) key.size;
                  pqr_next->key.args[0].svalue.buf_addr = (char *) pqr_next->key.ibuffer;
               }
               else {
                  sprintf((char *) pqr_next->key.ibuffer, "%d", pqr_next->key.args[0].num.int32);
                  pqr_next->key.args[0].svalue.len_used = (unsigned int) strlen((char *) pqr_next->key.ibuffer);
                  pqr_next->key.args[0].svalue.buf_addr = (char *) pqr_next->key.ibuffer;
               }
               pqr_next->key.argc = 1;
               pqr_next->data.svalue.len_used = (unsigned int) data.size;
            }
            else {
               pqr_next->key.args[0].svalue.len_used = 0;
            }
         }
      }
      else { /* get previous */
         if (pcon->key_type == DBX_KEYTYPE_M) {
/*
            dbx_dump_key((char *) key0.data, (int) key0.size);
*/
            pqr_next->data.svalue.len_used = 0;
            for (n = (*counter); n < ((*counter) + 5); n ++) {
               if (n == 0) {
                  *(((unsigned char *) key.data) + (key.size + 0)) = 0x00;
                  *(((unsigned char *) key.data) + (key.size + 1)) = 0xff;
                  key.size += 2;
                  rc = pmeth->pbdbcursor->get(pmeth->pbdbcursor, &key, &data, DB_SET_RANGE);
/*
                  printf("\r\ndbx_global_query (reverse) DB_SET_RANGE: n=%d; counter=%d; fixed_key_len=%d; rc=%d; key.size=%d; pqr_prev->key.argc=%d", n, *counter, *fixed_key_len, rc, (int) key.size, pqr_prev->key.argc);
                  dbx_dump_key((char *) key.data, (int) key.size);
*/
                  if (rc != CACHE_SUCCESS) {
                     rc = pmeth->pbdbcursor->get(pmeth->pbdbcursor, &key, &data, DB_LAST);
/*
                     printf("\r\ndbx_global_query (reverse) DB_LAST: n=%d; counter=%d; fixed_key_len=%d; rc=%d; key.size=%d; pqr_prev->key.argc=%d", n, *counter, *fixed_key_len, rc, (int) key.size, pqr_prev->key.argc);
                     dbx_dump_key((char *) key.data, (int) key.size);
*/
                  }
               }
               else {
                  rc = pmeth->pbdbcursor->get(pmeth->pbdbcursor, &key, &data, DB_PREV);
/*
                  printf("\r\ndbx_global_query (reverse) DB_PREV: n=%d; counter=%d; fixed_key_len=%d; rc=%d; key.size=%d; pqr_prev->key.argc=%d", n, *counter, *fixed_key_len, rc, (int) key.size, pqr_prev->key.argc);
                  dbx_dump_key((char *) key.data, (int) key.size);
*/
               }

               if (rc != CACHE_SUCCESS) {
                  pqr_next->key.args[0].svalue.len_used = 0;
                  break;
               }

               /* dbx_dump_key((char *) key.data, (int) key.size); */

               if (!bdb_key_compare(&key, &key0, *(fixed_key_len), pcon->key_type)) {
                  if (!bdb_key_compare(&key, &key0, 0, pcon->key_type)) { /* current key returned - get previous */
                     continue;
                  }
                  pqr_next->key.argc = dbx_split_key(&(pqr_next->key.args[0]), (char *) key.data, (int) key.size);
                  pqr_next->key.ibuffer_used = key.size;
                  pqr_next->data.svalue.len_used = data.size;
                  break;
               }
               else {
                  /* printf("\r\nPossibly next global/subscript in the chain"); */
                  continue;
               }
            }
         }
         else {
/*
            printf("\r\ndbx_global_query (reverse): counter=%d; key.size=%d; pqr_prev->key.argc=%d", *counter, (int) key.size, pqr_prev->key.argc);
            dbx_dump_key((char *) key.data, (int) key.size);
*/
            if ((*counter) == 0) {
               if (key.size == 0) {
                  rc = pmeth->pbdbcursor->get(pmeth->pbdbcursor, &key, &data, DB_LAST);
               }
               else {
                  rc = pmeth->pbdbcursor->get(pmeth->pbdbcursor, &key, &data, DB_SET_RANGE);
                  if (rc == CACHE_SUCCESS) {
                     rc = pmeth->pbdbcursor->get(pmeth->pbdbcursor, &key, &data, DB_PREV);
                  }
                  else {
                     rc = pmeth->pbdbcursor->get(pmeth->pbdbcursor, &key, &data, DB_LAST);
                  }
               }
            }
            else {
               rc = pmeth->pbdbcursor->get(pmeth->pbdbcursor, &key, &data, DB_PREV);
            }
            if (rc == CACHE_SUCCESS) {
              if (pcon->key_type == DBX_KEYTYPE_STR) {
                  pqr_next->key.ibuffer_used = (unsigned int) key.size;
                  pqr_next->key.args[0].svalue.len_used = (unsigned int) key.size;
                  pqr_next->key.args[0].svalue.buf_addr = (char *) pqr_next->key.ibuffer;
               }
               else {
                  sprintf((char *) pqr_next->key.ibuffer, "%d", pqr_next->key.args[0].num.int32);
                  pqr_next->key.args[0].svalue.len_used = (unsigned int) strlen((char *) pqr_next->key.ibuffer);
                  pqr_next->key.args[0].svalue.buf_addr = (char *) pqr_next->key.ibuffer;
               }
               pqr_next->key.argc = 1;
               pqr_next->data.svalue.len_used = (unsigned int) data.size;
            }
            else {
               pqr_next->key.args[0].svalue.len_used = 0;
            }
         }
      }

      if (pqr_next->key.args[0].svalue.len_used == 0) {
         rc = YDB_NODE_END;
      }
      else {
         (*counter) ++;
      }

      if (rc == YDB_NODE_END || rc != YDB_OK) {
         eod = 1;
         pqr_next->data.svalue.buf_addr[0] = '\0';
         pqr_next->data.svalue.len_used = 0;
         pqr_next->key.argc = 0;
      }
   }
   else if (pcon->dbtype == DBX_DBTYPE_LMDB) {
      MDB_val key, key0, data;

      rc = CACHE_SUCCESS;
      if (pcon->key_type == DBX_KEYTYPE_INT) {
         key0.mv_data = &(pqr_prev->key.args[0].num.int32);
         key0.mv_size = sizeof(pqr_prev->key.args[0].num.int32);
         pqr_next->key.args[0].num.int32 = pqr_prev->key.args[0].num.int32;
         key.mv_data = &(pqr_next->key.args[0].num.int32);
         key.mv_size = sizeof(pqr_next->key.args[0].num.int32);
      }
      else if (pcon->key_type == DBX_KEYTYPE_STR) {
         if ((*counter) == 0) {
            key0.mv_data = (void *) pqr_prev->key.ibuffer;
            key0.mv_size = (size_t) pqr_prev->key.ibuffer_used;
            memcpy((void *) pqr_next->key.ibuffer, (void *) pqr_prev->key.ibuffer, (size_t) pqr_prev->key.ibuffer_used);
            pqr_next->key.ibuffer_used = pqr_prev->key.ibuffer_used;
         }
         key.mv_data = (void *) pqr_next->key.ibuffer;
         key.mv_size = (size_t) pqr_next->key.ibuffer_used;
      }
      else { /* mumps */
         key0.mv_data = (void *) pqr_prev->key.ibuffer;
         key0.mv_size = (size_t) pqr_prev->key.ibuffer_used;
         memcpy((void *) pqr_next->key.ibuffer, (void *) pqr_prev->key.ibuffer, (size_t) pqr_prev->key.ibuffer_used);
         pqr_next->key.ibuffer_used = pqr_prev->key.ibuffer_used;
         key.mv_data = (void *) pqr_next->key.ibuffer;
         key.mv_size = (size_t) pqr_next->key.ibuffer_used;
      }

      data.mv_data = (void *) pqr_next->data.svalue.buf_addr;
      data.mv_size = (size_t) pqr_next->data.svalue.len_alloc;

      if (dir == 1) { /* get next */

         if (pcon->key_type == DBX_KEYTYPE_M) {
            /* dbx_dump_key((char *) key0.mv_data, (int) key0.mv_size); */
            pqr_next->data.svalue.len_used = 0;
            for (n = (*counter); n < ((*counter) + 5); n ++) {
               if (n == 0)
                  rc = pcon->p_lmdb_so->p_mdb_cursor_get(pmeth->plmdbcursor, &key, &data, MDB_SET_RANGE);
               else
                  rc = pcon->p_lmdb_so->p_mdb_cursor_get(pmeth->plmdbcursor, &key, &data, MDB_NEXT);

               if (rc != CACHE_SUCCESS) {
                  pqr_next->key.args[0].svalue.len_used = 0;
                  break;
               }

               if (!lmdb_key_compare(&key, &key0, *(fixed_key_len), pcon->key_type)) {
                  if (!lmdb_key_compare(&key, &key0, 0, pcon->key_type)) { /* current key returned - get next */
                     continue;
                  }
                  pqr_next->key.argc = dbx_split_key(&(pqr_next->key.args[0]), (char *) key.mv_data, (int) key.mv_size);
                  pqr_next->key.ibuffer_used = (unsigned int) key.mv_size;
                  pqr_next->data.svalue.len_used = (unsigned int) data.mv_size;
                  memcpy((void *) pqr_next->data.svalue.buf_addr, (void *) data.mv_data, (size_t) pqr_next->data.svalue.len_used);
                  break;
               }
               else {
                  pqr_next->key.args[0].svalue.len_used = 0;
                  break;
               }
            }
         }
         else { /* not mumps */
/*
            printf("\r\ndbx_global_query (forward): counter=%d; key.mv_size=%d; pqr_prev->key.argc=%d", *counter, (int) key.mv_size, pqr_prev->key.argc);
            dbx_dump_key((char *) key.mv_data, (int) key.mv_size);
*/
            if ((*counter) == 0) {
               if (key.mv_size == 0) {
                  rc = pcon->p_lmdb_so->p_mdb_cursor_get(pmeth->plmdbcursor, &key, &data, MDB_FIRST);
               }
               else {
                  rc = pcon->p_lmdb_so->p_mdb_cursor_get(pmeth->plmdbcursor, &key, &data, MDB_SET_RANGE);
                  if (rc == CACHE_SUCCESS && !lmdb_key_compare(&key, &key0, 0, pcon->key_type)) {
                     rc = pcon->p_lmdb_so->p_mdb_cursor_get(pmeth->plmdbcursor, &key, &data, MDB_NEXT);
                  }
               }
            }
            else {
               rc = pcon->p_lmdb_so->p_mdb_cursor_get(pmeth->plmdbcursor, &key, &data, MDB_NEXT);
            }

            if (rc == CACHE_SUCCESS) {
               if (pcon->key_type == DBX_KEYTYPE_STR) {
                  pqr_next->key.ibuffer_used = (unsigned int) key.mv_size;
                  pqr_next->key.args[0].svalue.len_used = (unsigned int) key.mv_size;
                  memcpy((void *) pqr_next->key.ibuffer, (void *) key.mv_data, (size_t) key.mv_size);
                  pqr_next->key.args[0].svalue.buf_addr = (char *) pqr_next->key.ibuffer;
               }
               else {
                  pqr_next->key.args[0].num.int32 = (int) dbx_get_size((unsigned char *) key.mv_data, 0);
                  sprintf((char *) pqr_next->key.ibuffer, "%d", pqr_next->key.args[0].num.int32);
                  pqr_next->key.args[0].svalue.len_used = (unsigned int) strlen((char *) pqr_next->key.ibuffer);
                  pqr_next->key.args[0].svalue.buf_addr = (char *) pqr_next->key.ibuffer;
               }
               pqr_next->key.argc = 1;
               pqr_next->data.svalue.len_used = (unsigned int) data.mv_size;
               memcpy((void *) pqr_next->data.svalue.buf_addr, (void *) data.mv_data, (size_t) pqr_next->data.svalue.len_used);
            }
            else {
               pqr_next->key.args[0].svalue.len_used = 0;
            }
         }
      }
      else { /* get previous */
         if (pcon->key_type == DBX_KEYTYPE_M) {
/*
            dbx_dump_key((char *) key0.data, (int) key0.size);
*/
            pqr_next->data.svalue.len_used = 0;
            for (n = (*counter); n < ((*counter) + 5); n ++) {
               if (n == 0) {
                  *(((unsigned char *) key.mv_data) + (key.mv_size + 0)) = 0x00;
                  *(((unsigned char *) key.mv_data) + (key.mv_size + 1)) = 0xff;
                  key.mv_size += 2;
                  rc = pcon->p_lmdb_so->p_mdb_cursor_get(pmeth->plmdbcursor, &key, &data, MDB_SET_RANGE);

/*
                  printf("\r\ndbx_global_query (reverse) DB_SET_RANGE: n=%d; counter=%d; fixed_key_len=%d; rc=%d; key.mv_size=%d; pqr_prev->key.argc=%d", n, *counter, *fixed_key_len, rc, (int) key.mv_size, pqr_prev->key.argc);
                  dbx_dump_key((char *) key.mv_data, (int) key.mv_size);
*/
                  if (rc != CACHE_SUCCESS) {
                     rc = pcon->p_lmdb_so->p_mdb_cursor_get(pmeth->plmdbcursor, &key, &data, MDB_LAST);
/*
                     printf("\r\ndbx_global_query (reverse) DB_LAST: n=%d; counter=%d; fixed_key_len=%d; rc=%d; key.mv_size=%d; pqr_prev->key.argc=%d", n, *counter, *fixed_key_len, rc, (int) key.mv_size, pqr_prev->key.argc);
                     dbx_dump_key((char *) key.mv_data, (int) key.mv_size);
*/
                  }
               }
               else {
                  rc = pcon->p_lmdb_so->p_mdb_cursor_get(pmeth->plmdbcursor, &key, &data, MDB_PREV);
/*
                  printf("\r\ndbx_global_query (reverse) DB_PREV: n=%d; counter=%d; fixed_key_len=%d; rc=%d; key.mv_size=%d; pqr_prev->key.argc=%d", n, *counter, *fixed_key_len, rc, (int) key.mv_size, pqr_prev->key.argc);
                  dbx_dump_key((char *) key.mv_data, (int) key.mv_size);
*/
               }

               if (rc != CACHE_SUCCESS) {
                  pqr_next->key.args[0].svalue.len_used = 0;
                  break;
               }

               /* dbx_dump_key((char *) key.data, (int) key.size); */

               if (!lmdb_key_compare(&key, &key0, *(fixed_key_len), pcon->key_type)) {
                  if (!lmdb_key_compare(&key, &key0, 0, pcon->key_type)) { /* current key returned - get previous */
                     continue;
                  }
                  pqr_next->key.argc = dbx_split_key(&(pqr_next->key.args[0]), (char *) key.mv_data, (int) key.mv_size);
                  pqr_next->key.ibuffer_used = (unsigned int) key.mv_size;
                  pqr_next->data.svalue.len_used = (unsigned int) data.mv_size;
                  memcpy((void *) pqr_next->data.svalue.buf_addr, (void *) data.mv_data, (size_t) pqr_next->data.svalue.len_used);
                  break;
               }
               else {
                  /* printf("\r\nPossibly next global/subscript in the chain"); */
                  continue;
               }
            }
         }
         else { /* not mumps */
/*
            printf("\r\ndbx_global_query (reverse): counter=%d; key.mv_size=%d; pqr_prev->key.argc=%d", *counter, (int) key.mv_size, pqr_prev->key.argc);
            dbx_dump_key((char *) key.mv_data, (int) key.mv_size);
*/
            if ((*counter) == 0) {
               if (key.mv_size == 0) {
                  rc = pcon->p_lmdb_so->p_mdb_cursor_get(pmeth->plmdbcursor, &key, &data, MDB_LAST);
               }
               else {
                  rc = pcon->p_lmdb_so->p_mdb_cursor_get(pmeth->plmdbcursor, &key, &data, MDB_SET_RANGE);
                  if (rc == CACHE_SUCCESS) {
                     rc = pcon->p_lmdb_so->p_mdb_cursor_get(pmeth->plmdbcursor, &key, &data, MDB_PREV);
                  }
                  else {
                     rc = pcon->p_lmdb_so->p_mdb_cursor_get(pmeth->plmdbcursor, &key, &data, MDB_LAST);
                  }
               }
            }
            else {
               rc = pcon->p_lmdb_so->p_mdb_cursor_get(pmeth->plmdbcursor, &key, &data, MDB_PREV);
            }
            if (rc == CACHE_SUCCESS) {
              if (pcon->key_type == DBX_KEYTYPE_STR) {
                  pqr_next->key.ibuffer_used = (unsigned int) key.mv_size;
                  pqr_next->key.args[0].svalue.len_used = (unsigned int) key.mv_size;
                  memcpy((void *) pqr_next->key.ibuffer, (void *) key.mv_data, (size_t) key.mv_size);
                  pqr_next->key.args[0].svalue.buf_addr = (char *) pqr_next->key.ibuffer;
               }
               else {
                  pqr_next->key.args[0].num.int32 = (int) dbx_get_size((unsigned char *) key.mv_data, 0);
                  sprintf((char *) pqr_next->key.ibuffer, "%d", pqr_next->key.args[0].num.int32);
                  pqr_next->key.args[0].svalue.len_used = (unsigned int) strlen((char *) pqr_next->key.ibuffer);
                  pqr_next->key.args[0].svalue.buf_addr = (char *) pqr_next->key.ibuffer;
               }
               pqr_next->key.argc = 1;
               pqr_next->data.svalue.len_used = (unsigned int) data.mv_size;
               memcpy((void *) pqr_next->data.svalue.buf_addr, (void *) data.mv_data, (size_t) pqr_next->data.svalue.len_used);
            }
            else {
               pqr_next->key.args[0].svalue.len_used = 0;
            }
         }
      }

      if (pqr_next->key.args[0].svalue.len_used == 0) {
         rc = YDB_NODE_END;
      }
      else {
         (*counter) ++;
      }

      if (rc == YDB_NODE_END || rc != YDB_OK) {
         eod = 1;
         pqr_next->data.svalue.buf_addr[0] = '\0';
         pqr_next->data.svalue.len_used = 0;
         pqr_next->key.argc = 0;
      }
   }
   else {
      rc = YDB_NODE_END;
   }

   if (rc != CACHE_SUCCESS && rc != YDB_NODE_END) {
      dbx_error_message(pmeth, rc, (char *) "dbx_global_query");
   }

   if (pcon->log_transmissions == 2) {
      if (eod)
         dbx_log_response(pcon, (char *) "[END]", (int) 5, (char *) (dir == 1 ? "mcursor::next (query)" : "mcursor::previous (query)"));
      else {
         int nx;
         v8::Local<v8::String> str;

         pmeth->key.ibuffer_used = 0;
         nx = 0;
         dbx_ibuffer_add(pmeth, &(pmeth->key), NULL, nx ++, str, pqr_next->global_name.buf_addr, pqr_next->global_name.len_used, 0);
         for (n = 0; n < pqr_next->key.argc; n ++) {
            dbx_ibuffer_add(pmeth, &(pmeth->key), NULL, nx ++, str, (char *) pqr_next->key.args[n].svalue.buf_addr, (int) pqr_next->key.args[n].svalue.len_used, 0);
         }
         dbx_log_response(pcon, (char *) pmeth->key.ibuffer, (int) pmeth->key.ibuffer_used, (char *) (dir == 1 ? "mcursor::next (query)" : "mcursor::previous (query)"));
         pmeth->key.ibuffer_used = 0;
      }
   }

   return eod;

#ifdef _WIN32
}
__except (EXCEPTION_EXECUTE_HANDLER) {

   DWORD code;
   char bufferx[256];

   __try {
      code = GetExceptionCode();
      sprintf_s(bufferx, 255, "Exception caught in f:dbx_global_query: %x", code);
      dbx_log_event(pcon, bufferx, "Error Condition", 0);
   }
   __except (EXCEPTION_EXECUTE_HANDLER) {
      ;
   }

   return 0;
}
#endif
}


int dbx_display_key(char *key, int key_size, char *buffer)
{
   int n, n1;
   char hex[16];

   n1 = 0;
   for (n = 0; n < key_size; n ++) {
      if ((int) *(((char *) key) + n) < 32 || (int) *(((char *) key) + n) > 126) {
         sprintf(hex, "%02x", (unsigned char) *(((char *) key) + n));
         buffer[n1 ++] = '\\';
         buffer[n1 ++] = 'x';
         buffer[n1 ++] = hex[0];
         buffer[n1 ++] = hex[1];
      }
      else {
         buffer[n1 ++] = *(((char *) key) + n);
      }
   }
   buffer[n1] = '\0';

   return 0;
}


int cache_report_failure(DBXCON *pcon)
{

   if (pcon->error_code == 0) {
      pcon->error_code = 10001;
      T_STRCPY(pcon->error, _dbxso(pcon->error), DBX_TEXT_E_ASYNC);
   }

   return 0;
}


/* ASYNC THREAD : Do the Cache task */
int dbx_launch_thread(DBXMETH *pmeth)
{
#if !defined(_WIN32)

   dbx_pool_submit_task(pmeth);

   return 1;

#else

   {
      int rc = 0;
#if defined(_WIN32)
      HANDLE hthread;
      SIZE_T stack_size;
      DWORD thread_id, cre_flags, result;
      LPSECURITY_ATTRIBUTES pattr;

      stack_size = 0;
      cre_flags = 0;
      pattr = NULL;

      hthread = CreateThread(pattr, stack_size, (LPTHREAD_START_ROUTINE) dbx_thread_main, (LPVOID) pmeth, cre_flags, &thread_id);
/*
      printf("\r\n*** %lu Primary Thread : CreateThread (%d) ...", (unsigned long) dbx_current_thread_id(), dbx_queue_size);
*/
      if (!hthread) {
         printf("failed to create thread, errno = %d\n",errno);
      }
      else {
         result = WaitForSingleObject(hthread, INFINITE);
      }
#else
      pthread_attr_t attr;
      pthread_t child_thread;

      size_t stacksize, newstacksize;

      pthread_attr_init(&attr);

      stacksize = 0;
      pthread_attr_getstacksize(&attr, &stacksize);

      newstacksize = DBX_THREAD_STACK_SIZE;

      pthread_attr_setstacksize(&attr, newstacksize);
/*
      printf("Thread: default stack=%lu; new stack=%lu;\n", (unsigned long) stacksize, (unsigned long) newstacksize);
*/
      rc = pthread_create(&child_thread, &attr, dbx_thread_main, (void *) pmeth);
      if (rc) {
         printf("failed to create thread, errno = %d\n",errno);
      }
      else {
         rc = pthread_join(child_thread,NULL);
      }
#endif
   }

#endif

   return 1;
}


#if defined(_WIN32)
LPTHREAD_START_ROUTINE dbx_thread_main(LPVOID pargs)
#else
void * dbx_thread_main(void *pargs)
#endif
{
   DBXMETH *pmeth;
/*
   printf("\r\n*** %lu Worker Thread : Thread (%d) ...", (unsigned long) dbx_current_thread_id(), dbx_queue_size);
*/
   pmeth = (DBXMETH *) pargs;

   pmeth->p_dbxfun(pmeth);

#if defined(_WIN32)
   return 0;
#else
   return NULL;
#endif
}


struct dbx_pool_task* dbx_pool_add_task(DBXMETH *pmeth)
{
   struct dbx_pool_task* enqueue_task;

   enqueue_task = (struct dbx_pool_task*) dbx_malloc(sizeof(struct dbx_pool_task), 601);

   if (!enqueue_task) {
      dbx_request_errors ++;
      return NULL;
   }

   enqueue_task->task_id = 6;
   enqueue_task->pmeth = pmeth;
   enqueue_task->next = NULL;

#if !defined(_WIN32)
   enqueue_task->parent_tid = pthread_self();

   pthread_mutex_lock(&dbx_task_queue_mutex);

   if (dbx_total_tasks == 0) {
      tasks = enqueue_task;
      bottom_task = enqueue_task;
   }
   else {
      bottom_task->next = enqueue_task;
      bottom_task = enqueue_task;
   }

   dbx_total_tasks ++;

   pthread_mutex_unlock(&dbx_task_queue_mutex);
   pthread_cond_signal(&dbx_pool_cond);
#endif

   return enqueue_task;
}


struct dbx_pool_task * dbx_pool_get_task(void)
{
   struct dbx_pool_task* task;

#if !defined(_WIN32)
   pthread_mutex_lock(&dbx_task_queue_mutex);
#endif

   if (dbx_total_tasks > 0) {
      task = tasks;
      tasks = task->next;

      if (tasks == NULL) {
         bottom_task = NULL;
      }
      dbx_total_tasks --;
   }
   else {
      task = NULL;
   }

#if !defined(_WIN32)
   pthread_mutex_unlock(&dbx_task_queue_mutex);
#endif

   return task;
}


void dbx_pool_execute_task(struct dbx_pool_task *task, int thread_id)
{
   if (task) {

      task->pmeth->p_dbxfun(task->pmeth);

      task->pmeth->done = 1;

#if !defined(_WIN32)
      pthread_cond_broadcast(&dbx_result_cond);
#endif
   }
}


void * dbx_pool_requests_loop(void *data)
{
   int thread_id;
   DBXTID *ptid;
   struct dbx_pool_task *task = NULL;

   ptid = (DBXTID *) data;
   if (!ptid) {
      return NULL;
   }

   thread_id = ptid->thread_id;

/*
#if !defined(_WIN32)
{
   int ret;
   pthread_attr_t tattr;
   size_t size;
   ret = pthread_getattr_np(pthread_self(), &tattr);
   ret = pthread_attr_getstacksize(&tattr, &size);
   printf("\r\n*** pthread_attr_getstacksize (pool) %x *** ret=%d; size=%u\r\n", (unsigned long) pthread_self(), ret, size);
}
#endif
*/

#if !defined(_WIN32)
   pthread_mutex_lock(&dbx_pool_mutex);

   while (1) {
      if (dbx_total_tasks > 0) {
         task = dbx_pool_get_task();
         if (task) {
            pthread_mutex_unlock(&dbx_pool_mutex);
            dbx_pool_execute_task(task, thread_id);
            dbx_free(task, 3001);
            pthread_mutex_lock(&dbx_pool_mutex);
         }
      }
      else {
         pthread_cond_wait(&dbx_pool_cond, &dbx_pool_mutex);
      }
   }
#endif

   return NULL;
}


int dbx_pool_thread_init(DBXCON *pcon, int num_threads)
{
#if !defined(_WIN32)
   int iterator;
   pthread_attr_t attr[DBX_THREADPOOL_MAX];
   size_t stacksize, newstacksize;

   for (iterator = 0; iterator < num_threads; iterator ++) {

      pthread_attr_init(&attr[iterator]);

      stacksize = 0;
      pthread_attr_getstacksize(&attr[iterator], &stacksize);

      newstacksize = DBX_THREAD_STACK_SIZE;
/*
      printf("Thread Pool: default stack=%lu; new stack=%lu;\n", (unsigned long) stacksize, (unsigned long) newstacksize);
*/
      pthread_attr_setstacksize(&attr[iterator], newstacksize);

      dbx_thr_id[iterator].thread_id = iterator;
      dbx_thr_id[iterator].p_mutex = pcon->p_mutex;
      dbx_thr_id[iterator].p_zv = pcon->p_zv;
      
      pthread_create(&dbx_p_threads[iterator], &attr[iterator], dbx_pool_requests_loop, (void *) &dbx_thr_id[iterator]);

   }
#endif
   return 0;
}


int dbx_pool_submit_task(DBXMETH *pmeth)
{
#if !defined(_WIN32)
   struct timespec   ts;
   struct timeval    tp;

   pmeth->done = 0;

   dbx_pool_add_task(pmeth);

   pthread_mutex_lock(&dbx_result_mutex);

   while (!pmeth->done) {

      gettimeofday(&tp, NULL);
      ts.tv_sec  = tp.tv_sec;
      ts.tv_nsec = tp.tv_usec * 1000;
      ts.tv_sec += 3;

      pthread_cond_timedwait(&dbx_result_cond, &dbx_result_mutex, &ts);
   }

   pthread_mutex_unlock(&dbx_result_mutex);
#endif
   return 1;
}


int dbx_add_block_size(unsigned char *block, unsigned long offset, unsigned long data_len, int dsort, int dtype)
{
   dbx_set_size((unsigned char *) block + offset, data_len, 0);
   block[offset + 4] = (unsigned char) ((dsort * 20) + dtype);

   return 1;
}


unsigned long dbx_get_block_size(unsigned char *block, unsigned long offset, int *dsort, int *dtype)
{
   unsigned long data_len;
   unsigned char uc;

   data_len = 0;
   uc = (unsigned char) block[offset + 4];

   *dtype = uc % 20;
   *dsort = uc / 20;

   if (*dsort != DBX_DSORT_STATUS) {
      data_len = dbx_get_size((unsigned char *) block + offset, 0);
   }

   if (!DBX_DSORT_ISVALID(*dsort)) {
      *dsort = DBX_DSORT_INVALID;
   }

   return data_len;
}


int dbx_set_size(unsigned char *str, unsigned long data_len, short big_endian)
{
   if (big_endian) {
      str[3] = (unsigned char) (data_len >> 0);
      str[2] = (unsigned char) (data_len >> 8);
      str[1] = (unsigned char) (data_len >> 16);
      str[0] = (unsigned char) (data_len >> 24);
   }
   else {
      str[0] = (unsigned char) (data_len >> 0);
      str[1] = (unsigned char) (data_len >> 8);
      str[2] = (unsigned char) (data_len >> 16);
      str[3] = (unsigned char) (data_len >> 24);
   }

   return 0;
}


unsigned long dbx_get_size(unsigned char *str, short big_endian)
{
   unsigned long size;

   if (big_endian) {
      size = ((unsigned char) str[3]) | (((unsigned char) str[2]) << 8) | (((unsigned char) str[1]) << 16) | (((unsigned char) str[0]) << 24);
   }
   else {
      size = ((unsigned char) str[0]) | (((unsigned char) str[1]) << 8) | (((unsigned char) str[2]) << 16) | (((unsigned char) str[3]) << 24);
   }

   return size;
}


void * dbx_realloc(void *p, int curr_size, int new_size, short id)
{
   if (new_size >= curr_size) {
      if (p) {
         dbx_free((void *) p, 0);
      }
      p = (void *) dbx_malloc(new_size, id);
      if (!p) {
         return NULL;
      }
   }
   return p;
}


void * dbx_malloc(int size, short id)
{
   void *p;

#if defined(_WIN32)
      p = (void *) HeapAlloc(GetProcessHeap(), HEAP_ZERO_MEMORY, size + 32);
#else
   p = (void *) malloc(size);
#endif

   return p;
}


int dbx_free(void *p, short id)
{
   /* printf("\ndbx_free: id=%d; p=%p;", id, p); */

#if defined(_WIN32)
      HeapFree(GetProcessHeap(), 0, p);
#else
   free((void *) p);
#endif

   return 0;
}

DBXQR * dbx_alloc_dbxqr(DBXQR *pqr, int dsize, short context)
{
   int n;
   char *p;

   pqr = (DBXQR *) dbx_malloc(sizeof(DBXQR) + 128, 0);
   if (!pqr) {
      return pqr;
   }
   pqr->key.ibuffer_size = 0;
   p = (char *) ((char *) pqr) + sizeof(DBXQR);
   pqr->global_name.buf_addr = p;
   pqr->global_name.len_alloc = 128;
   pqr->global_name.len_used = 0;

   pqr->key.ibuffer_size = 0;
   pqr->key.ibuffer_used = 0;
   pqr->key.ibuffer = (unsigned char *) dbx_malloc(CACHE_MAXSTRLEN, 0);
   if (pqr->key.ibuffer) {
      pqr->key.ibuffer_size = CACHE_MAXSTRLEN;
      pqr->key.ibuffer_used = 0;
   }

   for (n = 0; n < DBX_MAXARGS; n ++) {
      pqr->key.args[n].svalue.buf_addr = NULL;
      pqr->key.args[n].svalue.len_alloc = 0;
      pqr->key.args[n].svalue.len_used = 0;
   }
   pqr->key.args[0].svalue.buf_addr = (char *) pqr->key.ibuffer;
   pqr->key.args[0].svalue.len_alloc = pqr->key.ibuffer_size;
   pqr->key.args[0].svalue.len_used = 0;

   pqr->data.svalue.len_alloc = 0;
   pqr->data.svalue.len_used = 0;
   pqr->data.svalue.buf_addr = (char *) dbx_malloc(CACHE_MAXSTRLEN, 0);
   if (pqr->data.svalue.buf_addr) {
      pqr->data.svalue.len_alloc = CACHE_MAXSTRLEN;
      pqr->data.svalue.len_used = 0;
   }
   return pqr;
}


int dbx_free_dbxqr(DBXQR *pqr)
{
   if (pqr->data.svalue.buf_addr) {
      dbx_free((void *) pqr->data.svalue.buf_addr, 0); 
   }
   dbx_free((void *) pqr, 0); 
   return 0;
}


int dbx_ucase(char *string)
{
#ifdef _UNICODE

   CharUpperA(string);
   return 1;

#else

   int n, chr;

   n = 0;
   while (string[n] != '\0') {
      chr = (int) string[n];
      if (chr >= 97 && chr <= 122)
         string[n] = (char) (chr - 32);
      n ++;
   }
   return 1;

#endif
}


int dbx_lcase(char *string)
{
#ifdef _UNICODE

   CharLowerA(string);
   return 1;

#else

   int n, chr;

   n = 0;
   while (string[n] != '\0') {
      chr = (int) string[n];
      if (chr >= 65 && chr <= 90)
         string[n] = (char) (chr + 32);
      n ++;
   }
   return 1;

#endif
}


int dbx_create_string(DBXSTR *pstr, void *data, short type)
{
   DBXSTR *pstrobj_in;

   if (!data) {
      return -1;
   }

   pstr->len_used = 0;

   if (type == DBX_DTYPE_STROBJ) {
      pstrobj_in = (DBXSTR *) data;

      type = DBX_DTYPE_STR8;
      data = (void *) pstrobj_in->buf_addr;
   }

   if (type == DBX_DTYPE_STR8) {
      T_STRCPY((char *) pstr->buf_addr, pstr->len_alloc, (char *) data);
   }
   else if (type == DBX_DTYPE_INT) {
      T_SPRINTF((char *) pstr->buf_addr, pstr->len_alloc, "%d", (int) *((int *) data));
   }
   else {
      pstr->buf_addr[0] = '\0';
   }
   pstr->len_used = (unsigned long) strlen((char *) pstr->buf_addr);

   return (int) pstr->len_used;
}



int dbx_log_transmission(DBXCON *pcon, DBXMETH *pmeth, char *name)
{
   char namex[64];
   char buffer[256];

   if (pcon->log_filter[0]) {
      strcpy(namex, ",");
      strcat(namex, name);
      strcat(namex, ",");
      if (!strstr(pcon->log_filter, namex)) {
         return 0;
      }
   }
   sprintf(buffer, (char *) "mg-dbx-bdb: transmission: %s", name);

   dbx_log_buffer(pcon, (char *) pmeth->key.ibuffer, pmeth->key.ibuffer_used, buffer, 0);

   return 0;
}


int dbx_log_response(DBXCON *pcon, char *ibuffer, int ibuffer_len, char *name)
{
   char namex[64];
   char buffer[256];

   if (pcon->log_filter[0]) {
      strcpy(namex, ",");
      strcat(namex, name);
      strcat(namex, ",");
      if (!strstr(pcon->log_filter, namex)) {
         return 0;
      }
   }
   sprintf(buffer, (char *) "mg-dbx-bdb: response: %s", name);

   dbx_log_buffer(pcon, (char *) ibuffer, ibuffer_len, buffer, 0);

   return 0;
}


int dbx_buffer_dump(DBXCON *pcon, void *buffer, unsigned int len, char *title, unsigned char csize, short mode)
{
   unsigned int n;
   unsigned char *p8;
   unsigned short *p16;
   unsigned short c;

   p8 = NULL;
   p16 = NULL;

   if (csize == 16) {
      p16 = (unsigned short *) buffer;
   }
   else {
      p8 = (unsigned char *) buffer;
   }
   printf("\nbuffer dump (title=%s; size=%d; charsize=%u; mode=%d)...\n", title, len, csize, mode);

   for (n = 0; n < len; n ++) {
      if (csize == 16) {
         c = p16[n];
      }
      else {
         c = p8[n];
      }

      if (mode == 1) {
         printf("\\x%04x ", c);
         if (!((n + 1) % 8)) {
            printf("\r\n");
         }
      }
      else {
         if ((c < 32) || (c > 126)) {
            if (csize == 16) {
               printf("\\x%04x", c);
            }
            else {
               printf("\\x%02x", c);
            }
         }
         else {
            printf("%c", (char) c);
         }
      }
   }

   return 0;
}


int dbx_log_event(DBXCON *pcon, char *message, char *title, int level)
{
   int len, n;
   char timestr[64], heading[256], buffer[2048];
   char *p_buffer;
   time_t now = 0;
#if defined(_WIN32)
   HANDLE hLogfile = 0;
   DWORD dwPos = 0, dwBytesWritten = 0;
#else
   FILE *fp = NULL;
   struct flock lock;
#endif

#ifdef _WIN32
__try {
#endif

   now = time(NULL);
   sprintf(timestr, "%s", ctime(&now));
   for (n = 0; timestr[n] != '\0'; n ++) {
      if ((unsigned int) timestr[n] < 32) {
         timestr[n] = '\0';
         break;
      }
   }
/*
   sprintf(heading, ">>> Time: %s; Build: %s pid=%lu;tid=%lu;req_no=%lu;fun_no=%lu", timestr, (char *) DBX_VERSION, (unsigned long) dbx_current_process_id(), (unsigned long) dbx_current_thread_id(), p_log->req_no, p_log->fun_no);
*/

   sprintf(heading, ">>> Time: %s; Build: %s pid=%lu;tid=%lu;", timestr, (char *) DBX_VERSION, (unsigned long) dbx_current_process_id(), (unsigned long) dbx_current_thread_id());

   len = (int) strlen(heading) + (int) strlen(title) + (int) strlen(message) + 20;

   if (len < 2000)
      p_buffer = buffer;
   else
      p_buffer = (char *) malloc(sizeof(char) * len);

   if (p_buffer == NULL)
      return 0;

   p_buffer[0] = '\0';
   strcpy(p_buffer, heading);
   strcat(p_buffer, "\r\n    ");
   strcat(p_buffer, title);
   strcat(p_buffer, "\r\n    ");
   strcat(p_buffer, message);
   len = (int) strlen(p_buffer) * sizeof(char);

#if defined(_WIN32)

   strcat(p_buffer, "\r\n");
   len = len + (2 * sizeof(char));
   hLogfile = CreateFileA(pcon->log_file, GENERIC_WRITE, FILE_SHARE_WRITE,
                         (LPSECURITY_ATTRIBUTES) NULL, OPEN_ALWAYS,
                         FILE_ATTRIBUTE_NORMAL, (HANDLE) NULL);
   dwPos = SetFilePointer(hLogfile, 0, (LPLONG) NULL, FILE_END);
   LockFile(hLogfile, dwPos, 0, dwPos + len, 0);
   WriteFile(hLogfile, (LPTSTR) p_buffer, len, &dwBytesWritten, NULL);
   UnlockFile(hLogfile, dwPos, 0, dwPos + len, 0);
   CloseHandle(hLogfile);

#else /* UNIX or VMS */

   strcat(p_buffer, "\n");
   fp = fopen(pcon->log_file, "a");
   if (fp) {

      lock.l_type = F_WRLCK;
      lock.l_start = 0;
      lock.l_whence = SEEK_SET;
      lock.l_len = 0;
      n = fcntl(fileno(fp), F_SETLKW, &lock);

      fputs(p_buffer, fp);
      fclose(fp);

      lock.l_type = F_UNLCK;
      lock.l_start = 0;
      lock.l_whence = SEEK_SET;
      lock.l_len = 0;
      n = fcntl(fileno(fp), F_SETLK, &lock);
   }

#endif

   if (p_buffer != buffer)
      free((void *) p_buffer);

   return 1;

#ifdef _WIN32
}
__except (EXCEPTION_EXECUTE_HANDLER) {
      return 0;
}

#endif

}


int dbx_log_buffer(DBXCON *pcon, char *buffer, int buffer_len, char *title, int level)
{
   unsigned int c, len, strt;
   int n, n1, nc, size;
   char tmp[16];
   char *p;

#ifdef _WIN32
__try {
#endif

   for (n = 0, nc = 0; n < buffer_len; n ++) {
      c = (unsigned int) buffer[n];
      if (c < 32 || c > 126)
         nc ++;
   }

   size = buffer_len + (nc * 4) + 32;
   p = (char *) malloc(sizeof(char) * size);
   if (!p)
      return 0;

   if (nc) {

      for (n = 0, nc = 0; n < buffer_len; n ++) {
         c = (unsigned int) buffer[n];
         if (c < 32 || c > 126) {
            sprintf((char *) tmp, "%02x", c);
            len = (int) strlen(tmp);
            if (len > 2)
               strt = len - 2;
            else
               strt = 0;
            p[nc ++] = '\\';
            p[nc ++] = 'x';
            for (n1 = strt; tmp[n1]; n1 ++)
               p[nc ++] = tmp[n1];
         }
         else
            p[nc ++] = buffer[n];
      }
      p[nc] = '\0';
   }
   else {
      strncpy(p, buffer, buffer_len);
      p[buffer_len] = '\0';
   }

   dbx_log_event(pcon, (char *) p, title, level);

   free((void *) p);

   return 1;

#ifdef _WIN32
}
__except (EXCEPTION_EXECUTE_HANDLER) {
      return 0;
}

#endif

}



int dbx_test_file_access(char *file, int mode)
{
   int result;
   char *p;
   char buffer[256];
   FILE *fp;

   result = 0;
   *buffer = '\0';
   dbx_fopen(&fp, file, "r");
   if (fp) {
      p = fgets(buffer, 250, fp);
      if (p && strlen(buffer)) {
/*
         dbx_log_event(NULL, buffer, (char *) "dbx_test_file_access", 0);
*/
         result = 1;
      }
      fclose(fp);
   }

   return result;


}


DBXPLIB dbx_dso_load(char * library)
{
   DBXPLIB p_library;

#if defined(_WIN32)
   p_library = LoadLibraryA(library);
#else
#if defined(RTLD_DEEPBIND)
   p_library = dlopen(library, RTLD_NOW | RTLD_DEEPBIND);
#else
   p_library = dlopen(library, RTLD_NOW);
#endif
#endif

   return p_library;
}



DBXPROC dbx_dso_sym(DBXPLIB p_library, char * symbol)
{
   DBXPROC p_proc;

#if defined(_WIN32)
   p_proc = GetProcAddress(p_library, symbol);
#else
   p_proc  = (void *) dlsym(p_library, symbol);
#endif

   return p_proc;
}



int dbx_dso_unload(DBXPLIB p_library)
{

#if defined(_WIN32)
   FreeLibrary(p_library);
#else
   dlclose(p_library); 
#endif

   return 1;
}


DBXTHID dbx_current_thread_id(void)
{
#if defined(_WIN32)
   return (DBXTHID) GetCurrentThreadId();
#else
   return (DBXTHID) pthread_self();
#endif
}


unsigned long dbx_current_process_id(void)
{
#if defined(_WIN32)
   return (unsigned long) GetCurrentProcessId();
#else
   return ((unsigned long) getpid());
#endif
}


int dbx_error_message(DBXMETH *pmeth, int error_code, char *function)
{
   int rc;
   char title[128];
   DBXCON *pcon = pmeth->pcon;

   rc = 0;
   if (pcon->dbtype == DBX_DBTYPE_BDB) {
      rc = bdb_error_message(pcon, error_code);
   }
   else if (pcon->dbtype == DBX_DBTYPE_LMDB) {
      rc = lmdb_error_message(pcon, error_code);
   }

   if (pcon->log_errors) {
      sprintf(title, (char *) "mg-dbx-bdb: error in function: %s", function);
      dbx_log_event(pcon, pcon->error, (char *) title, 0);
   }

   return rc;
}


int dbx_mutex_create(DBXMUTEX *p_mutex)
{
   int result;

   result = 0;
   if (p_mutex->created) {
      return result;
   }

#if defined(_WIN32)
   p_mutex->h_mutex = CreateMutex(NULL, FALSE, NULL);
   result = 0;
#else
   result = pthread_mutex_init(&(p_mutex->h_mutex), NULL);
#endif

   p_mutex->created = 1;
   p_mutex->stack = 0;
   p_mutex->thid = 0;

   return result;
}



int dbx_mutex_lock(DBXMUTEX *p_mutex, int timeout)
{
   int result;
   DBXTHID tid;
#ifdef _WIN32
   DWORD result_wait;
#endif

   result = 0;

   if (!p_mutex->created) {
      return -1;
   }

   tid = dbx_current_thread_id();
   if (p_mutex->thid == tid) {
      p_mutex->stack ++;
      /* printf("\r\n thread already owns lock : thid=%lu; stack=%d;\r\n", (unsigned long) tid, p_mutex->stack); */
      return 0; /* success - thread already owns lock */
   }

#if defined(_WIN32)
   if (timeout == 0) {
      result_wait = WaitForSingleObject(p_mutex->h_mutex, INFINITE);
   }
   else {
      result_wait = WaitForSingleObject(p_mutex->h_mutex, (timeout * 1000));
   }

   if (result_wait == WAIT_OBJECT_0) { /* success */
      result = 0;
   }
   else if (result_wait == WAIT_ABANDONED) {
      printf("\r\ndbx_mutex_lock: Returned WAIT_ABANDONED state");
      result = -1;
   }
   else if (result_wait == WAIT_TIMEOUT) {
      printf("\r\ndbx_mutex_lock: Returned WAIT_TIMEOUT state");
      result = -1;
   }
   else if (result_wait == WAIT_FAILED) {
      printf("\r\ndbx_mutex_lock: Returned WAIT_FAILED state: Error Code: %d", GetLastError());
      result = -1;
   }
   else {
      printf("\r\ndbx_mutex_lock: Returned Unrecognized state: %d", result_wait);
      result = -1;
   }
#else
   result = pthread_mutex_lock(&(p_mutex->h_mutex));
#endif

   p_mutex->thid = tid;
   p_mutex->stack = 0;

   return result;
}


int dbx_mutex_unlock(DBXMUTEX *p_mutex)
{
   int result;
   DBXTHID tid;

   result = 0;

   if (!p_mutex->created) {
      return -1;
   }

   tid = dbx_current_thread_id();
   if (p_mutex->thid == tid && p_mutex->stack) {
      /* printf("\r\n thread has stacked locks : thid=%lu; stack=%d;\r\n", (unsigned long) tid, p_mutex->stack); */
      p_mutex->stack --;
      return 0;
   }
   p_mutex->thid = 0;
   p_mutex->stack = 0;

#if defined(_WIN32)
   ReleaseMutex(p_mutex->h_mutex);
   result = 0;
#else
   result = pthread_mutex_unlock(&(p_mutex->h_mutex));
#endif /* #if defined(_WIN32) */

   return result;
}


int dbx_mutex_destroy(DBXMUTEX *p_mutex)
{
   int result;

   if (!p_mutex->created) {
      return -1;
   }

#if defined(_WIN32)
   CloseHandle(p_mutex->h_mutex);
   result = 0;
#else
   result = pthread_mutex_destroy(&(p_mutex->h_mutex));
#endif

   p_mutex->created = 0;

   return result;
}


int dbx_enter_critical_section(void *p_crit)
{
   int result;

#if defined(_WIN32)
   EnterCriticalSection((LPCRITICAL_SECTION) p_crit);
   result = 0;
#else
   result = pthread_mutex_lock((pthread_mutex_t *) p_crit);
#endif
   return result;
}


int dbx_leave_critical_section(void *p_crit)
{
   int result;

#if defined(_WIN32)
   LeaveCriticalSection((LPCRITICAL_SECTION) p_crit);
   result = 0;
#else
   result = pthread_mutex_unlock((pthread_mutex_t *) p_crit);
#endif
   return result;
}


int dbx_sleep(unsigned long msecs)
{
#if defined(_WIN32)

   Sleep((DWORD) msecs);

#else

#if 1
   unsigned int secs, msecs_rem;

   secs = (unsigned int) (msecs / 1000);
   msecs_rem = (unsigned int) (msecs % 1000);

   /* printf("\n   ===> msecs=%ld; secs=%ld; msecs_rem=%ld", msecs, secs, msecs_rem); */

   if (secs > 0) {
      sleep(secs);
   }
   if (msecs_rem > 0) {
      usleep((useconds_t) (msecs_rem * 1000));
   }

#else
   unsigned int secs;

   secs = (unsigned int) (msecs / 1000);
   if (secs == 0)
      secs = 1;
   sleep(secs);

#endif

#endif

   return 0;
}



int dbx_fopen(FILE **pfp, const char *file, const char *mode)
{
   int n;

   n = 0;
#if defined(DBX_USE_SECURE_CRT)
   n = (int) fopen_s(pfp, file, mode);
#else
   *pfp = fopen(file, mode);
#endif
   return n;
}


int dbx_strcpy_s(char *to, size_t size, const char *from, const char *file, const char *fun, const unsigned int line)
{
   size_t len_to;

   len_to = strlen(from);

   if (len_to > (size - 1)) {
      char buffer[256];
#if defined(DBX_USE_SECURE_CRT)
      sprintf_s(buffer, _dbxso(buffer), "Buffer Size Error: Function: strcpy_s([%lu], [%lu])    file=%s; fun=%s; line=%u;", (unsigned long) size, (unsigned long) len_to, file, fun, line);
#else
      sprintf(buffer, "Buffer Size Error: Function: strcpy([%lu], [%lu])    file=%s; fun=%s; line=%u;", (unsigned long) size, (unsigned long) len_to, file, fun, line);
#endif
      printf("\r\n%s", buffer);
   }

#if defined(DBX_USE_SECURE_CRT)
#if defined(_WIN32)
   strcpy_s(to, size, from);
#else
   strcpy_s(to, size, from);
#endif
#else
   strcpy(to, from);
#endif
   return 0;
}


int dbx_strncpy_s(char *to, size_t size, const char *from, size_t count, const char *file, const char *fun, const unsigned int line)
{
   if (count > size) {
      char buffer[256];
#if defined(DBX_USE_SECURE_CRT)
      sprintf_s(buffer, _dbxso(buffer), "Buffer Size Error: Function: strncpy_s([%lu], [%lu])    file=%s; fun=%s; line=%u;", (unsigned long) size, (unsigned long) count, file, fun, line);
#else
      sprintf(buffer, "Buffer Size Error: Function: strncpy([%lu], [%lu])    file=%s; fun=%s; line=%u;", (unsigned long) size, (unsigned long) count, file, fun, line);
#endif
      printf("\r\n%s", buffer);
   }

#if defined(DBX_USE_SECURE_CRT)
#if defined(_WIN32)
   /* strncpy_s(to, size, from, count); */
   memcpy_s(to, size, from, count);
#else
   strncpy_s(to, size, from, count);
#endif
#else
   strncpy(to, from, count);
#endif
   return 0;
}

int dbx_strcat_s(char *to, size_t size, const char *from, const char *file, const char *fun, const unsigned int line)
{
   size_t len_to, len_from;

   len_to = strlen(to);
   len_from = strlen(from);

   if (len_from > ((size - len_to) - 1)) {
      char buffer[256];
#if defined(DBX_USE_SECURE_CRT)
      sprintf_s(buffer, _dbxso(buffer), "Buffer Size Error: Function: strcat_s([%lu <= (%lu-%lu)], [%lu])    file=%s; fun=%s; line=%u;", (unsigned long) (size - len_to), (unsigned long) size, (unsigned long) len_to, (unsigned long) len_from , file, fun, line);
#else
      sprintf(buffer, "Buffer Size Error: Function: strcat([%lu <= (%lu-%lu)], [%lu])    file=%s; fun=%s; line=%u;", (unsigned long) (size - len_to), (unsigned long) size, (unsigned long) len_to, (unsigned long) len_from, file, fun, line);
#endif
      printf("\r\n%s", buffer);

   }
#if defined(DBX_USE_SECURE_CRT)
#if defined(_WIN32)
   strcat_s(to, size, from);
#else
   strcat_s(to, size, from);
#endif
#else
   strcat(to, from);
#endif

   return 0;
}

int dbx_strncat_s(char *to, size_t size, const char *from, size_t count, const char *file, const char *fun, const unsigned int line)
{
   size_t len_to;

   len_to = strlen(to);

   if (count > (size - len_to)) {
      char buffer[256];
#if defined(DBX_USE_SECURE_CRT)
      sprintf_s(buffer, _dbxso(buffer), "Buffer Size Error: Function: strncat_s([%lu <= (%lu-%lu)], [%lu])    file=%s; fun=%s; line=%u;", (unsigned long) (size - len_to), (unsigned long) size, (unsigned long) len_to, (unsigned long) count , file, fun, line);
#else
      sprintf(buffer, "Buffer Size Error: Function: strncat([%lu <= (%lu-%lu)], [%lu])    file=%s; fun=%s; line=%u;", (unsigned long) (size - len_to), (unsigned long) size, (unsigned long) len_to, (unsigned long) count , file, fun, line);
#endif
      printf("\r\n%s", buffer);
   }

#if defined(DBX_USE_SECURE_CRT)
#if defined(_WIN32)
   strncat_s(to, size, from, count);
#else
   strncat_s(to, size, from, count);
#endif
#else
   strncat(to, from, count);
#endif

   return 0;
}


int dbx_sprintf_s(char *to, size_t size, const char *format, const char *file, const char *fun, const unsigned int line, ...)
{
   va_list arg;
   size_t len_to;
   int ret, i;
   long l;
   char *p, *s;
   void *pv;
   char buffer[32];

   va_start(arg, line);

   /* Estimate size required based on types commonly used in the Gateway */
   len_to = strlen(format);
   p = (char *) format;
   while ((p = strstr(p, "%"))) {
      p ++;
      len_to -= 2;
      if (*p == 's') {
         s = va_arg(arg, char *);
         if (s)
            len_to += strlen(s);
      }
      else if (*p == 'c') {
         len_to += 1;
      }
      else if (*p == 'd' || *p == 'u') {
         i = va_arg(arg, int);
#if defined(DBX_USE_SECURE_CRT)
         sprintf_s(buffer, 32, "%d", i);
#else
         sprintf(buffer, "%d", i);
#endif
         len_to += strlen(buffer);
      }
      else if (*p == 'x') {
         i = va_arg(arg, int);
#if defined(DBX_USE_SECURE_CRT)
         sprintf_s(buffer, 32, "%x", i);
#else
         sprintf(buffer, "%x", i);
#endif
         len_to += strlen(buffer);
      }
      else if (*p == 'l') {
         len_to --;
         l = va_arg(arg, long);
#if defined(DBX_USE_SECURE_CRT)
         sprintf_s(buffer, 32, "%ld", l);
#else
         sprintf(buffer, "%ld", l);
#endif
         len_to += strlen(buffer);
      }
      else if (*p == 'p') {
         pv = va_arg(arg, void *);
#if defined(DBX_USE_SECURE_CRT)
         sprintf_s(buffer, 32, "%p", pv);
#else
         sprintf(buffer, "%p", pv);
#endif
         len_to += strlen(buffer);
      }
      else {
         pv = va_arg(arg, void *);
         len_to += 10;
      }
   }

   if (len_to > (size - 1)) {
      char buffer[256];
#if defined(DBX_USE_SECURE_CRT)
      sprintf_s(buffer, _dbxso(buffer), "Buffer Size Error: Function: sprintf_s([%lu], [%lu], ...)    file=%s; fun=%s; line=%u;", (unsigned long) size, (unsigned long) len_to, file, fun, line);
#else
      sprintf(buffer, "Buffer Size Error: Function: sprintf([%lu], [%lu], ...)    file=%s; fun=%s; line=%u;", (unsigned long) size, (unsigned long) len_to, file, fun, line);
#endif
      printf("\r\n%s", buffer);
   }

   va_start(arg, line);

#if defined(DBX_USE_SECURE_CRT)
#if defined(_WIN32)
   ret = sprintf_s(to, size, format, arg);
#else
   ret = vsprintf_s(to, size, format, arg);
#endif
#else
   ret = vsprintf(to, format, arg);
#endif
   va_end(arg);

   return ret;
}

