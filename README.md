# mg-dbx-bdb

High speed Synchronous and Asynchronous access to Berkeley DB and LMDB from Node.js.

Chris Munt <cmunt@mgateway.com>  
12 April 2021, M/Gateway Developments Ltd [http://www.mgateway.com](http://www.mgateway.com)

* The M database emulation mode is an experimental project at this stage.
* The Berkeley DB and LMDB mode should be stable.
* Verified to work with Node.js v14 to v15.
* [Release Notes](#RelNotes) can be found at the end of this document.

Contents

* [Introduction](#Intro") 
* [Pre-requisites](#PreReq") 
* [Installing mg-dbx-bdb](#Install)
* [Connecting to the database](#Connect)
* [Invocation of database functions (Berkeley DB and LMDB mode)](#DBFunctionsBDB)
* [Invocation of database functions (M emulation mode)](#DBFunctionsM)
* [Working with binary data](#Binary)
* [Using Node.js/V8 worker threads](#Threads)
* [The Event Log](#EventLog)
* [License](#License)

## <a name="Intro"></a> Introduction

More will be written about the aims and rationale for this project in due course.  In the meantime, what follows in this section is a brief description of the functionality provided by this Node.js add-on module.

There are two parts to this project.  Firstly, simple synchronous and asynchronous access to Berkeley DB and/or LMDB B-Tree storage using either integer or string based keys.  Secondly, an emulation of the M database storage model is layered on top of these two B-Tree storage solutions.

### Berkeley DB and LMDB Mode

In this mode, a Berkeley DB (or LMDB) database is created/opened and key-value pairs are added to it.

       database[{keys}]=value

Examples:

       db = person_database.db
       db[1]="John Smith"
       db[2]="Jane Jones"


### M emulation Mode

The M database is broadly B-Tree based but includes extra structural elements.  The database is divided up into discrete sections known, in M, as **global variables** or **"globals"** for short.  You can think of each M **global** as being conceptually similar to a table in a relational database.  Records in each **global** are identified by a key.  In the M model, each key can consist of one or many individual key items, giving the dataset the multidimensional characteristics of the M storage model.  In M, keys are known as **subscripts** and are conceptually similar to a (primary) key in a relational database.

       database[{global variables}, {keys ...}]=value

Examples:

       db = database.db
       person = new db.global("person")
       hospital_admission = new db.global("admission")

       person[1]="John Smith"
       person[2]="Jane Jones"

       hospital_admission[1, 20201203]="Admission Record for 3 December 2020"
       hospital_admission[1, 20210101]="Admission Record for 1 January 2021"

The above scheme shows a one-to-many relationship between a person and that person's hospital admission records.  All records are held in a single Berkeley DB (or LMDB) database.

## <a name="PreReq"></a> Pre-requisites 

**mg-dbx-bdb** is a Node.js addon written in C++.  It is distributed as C++ source code and the NPM installation procedure will expect a C++ compiler to be present on the target system.

Linux systems can use the freely available GNU C++ compiler (g++) which can be installed as follows.

Ubuntu, Debian and Raspberry Pi OS:

       apt-get install g++

Red Hat and CentOS:

       yum install gcc-c++

Apple OS X can use the freely available **Xcode** development environment.

Microsoft Windows can use the freely available Microsoft Visual Studio Community:

* Microsoft Visual Studio Community: [https://www.visualstudio.com/vs/community/](https://www.visualstudio.com/vs/community/)


If the Windows machine is not set up for systems development, building native Addon modules for this platform from C++ source can be quite arduous.  There is some helpful advice available at:

* [Compiling native Addon modules for Windows](https://github.com/Microsoft/nodejs-guidelines/blob/master/windows-environment.md#compiling-native-addon-modules)

Alternatively there are built Windows x64 binaries available from:

* [https://github.com/chrisemunt/mg-dbx-bdb/blob/master/bin/winx64](https://github.com/chrisemunt/mg-dbx-bdb/blob/master/bin/winx64)


## <a name="Install"></a> Installing mg-dbx-bdb

Assuming that Node.js is already installed and a C++ compiler is available to the installation process:

       npm install mg-dbx-bdb

This command will create the **mg-dbx-bdb** addon (*mg-dbx-bdb.node*).


## <a name="Connect"></a> Connecting to the database

Most **mg-dbx-bdb** methods are capable of operating either synchronously or asynchronously. For an operation to complete asynchronously, simply supply a suitable callback as the last argument in the call.

The first step is to add **mg-dbx-bdb** to your Node.js script

       var dbxbdb = require('mg-dbx-bdb').dbxbdb;

And optionally (as required):

       var mglobal = require('mg-dbx-bdb').mglobal;
       var mcursor = require('mg-dbx-bdb').mcursor;

### Create a Database Object

       var db = new dbxbdb();


### Open a connection to the database (Berkeley DB)

To open a BDB database, the database file is specified in **open()** property **db\_file**.  This will allow single-process access to the database.  In order to open a BDB database for concurrent access through multiple processes the BDB environment directory should be specified in **open()** property **env\_dir**.  In summary:

* Single process access: specify **db\_file**.
* Multi-process concurrent access: specify **db\_file** and **env\_dir**.

In the following examples, modify all paths to match those of your own installation.

#### UNIX

Assuming BDB is installed under **/usr/local/BerkeleyDB.18.1/lib/**

           var open = db.open({
               type: "BDB",
               db_library: "/usr/local/BerkeleyDB.18.1/lib/libdb.so",
               db_file: "/opt/bdb/my_bdb_database.db",
               env_dir: "/opt/bdb",
               key_type: "int"});
             });

* **key\_type** can be **int** (integer), **str** (string) or **m** (multidimensional M database emulation).

#### Windows

Assuming BDB is installed under **c:/bdb/**

           var open = db.open({
               type: "BDB",
               db_library: "c:/bdb/libdb181.dll",
               db_file: "c:/bdb/my_bdb_database.db",
               env_dir: "c:/bdb",
               key_type: "int"});
             });

* **key\_type** can be **int** (integer), **str** (string) or **m** (multidimensional M database emulation).


### Open a connection to the database (LMDB)

To open a LMDB database for concurrent access, the database environment is specified in **open()** property **env\_dir**.  The database (and associated files) will be created in this directory.

In the following examples, modify all paths to match those of your own installation.

#### UNIX

Assuming LMDB is installed in the usual location: **/usr/lib/liblmdb.so**

           var open = db.open({
               type: "LMDB",
               db_library: "liblmdb.so",
               env_dir: "/opt/lmdb",
               key_type: "int"});
             });

* **key\_type** can be **int** (integer), **str** (string) or **m** (multidimensional M database emulation).

#### Windows

Assuming the LMDB DLL is installed as: **c:/LMDBWindows/lib/LMDBWindowsDll64.dll**

           var open = db.open({
               type: "LMDB",
               db_library: "c:/LMDBWindows/lib/LMDBWindowsDll64.dll",
               env_dir: "c:/lmdb",
               key_type: "int"});
             });

* **key\_type** can be **int** (integer), **str** (string) or **m** (multidimensional M database emulation).


### Additional (optional) properties for the open() method

* **multithreaded**: A boolean value to be set to 'true' or 'false' (default **multithreaded: false**).  Set this property to 'true' if the application uses multithreaded techniques in JavaScript (e.g. V8 worker threads).

### Return the version of mg-dbx-bdb

       var result = db.version();

Example:

       console.log("\nmg-dbx-bdb Version: " + db.version());


### Returning (and optionally changing) the current character set

UTF-8 is the default character encoding for **mg-dbx-bdb**.  The other option is the 8-bit ASCII character set (characters of the range ASCII 0 to ASCII 255).  The ASCII character set is a better option when exchanging single-byte binary data with the database.

       current_charset = db.charset([<new_charset>]);

Example 1 (Get the current character set): 

       var charset = db.charset();

Example 2 (Change the current character set): 

       var new_charset = db.charset('ascii');

* If the operation is successful this method will echo back the new character set name.  If not successful, the method will return the name of the current (unchanged) character set.
* Currently supported character sets and encoding schemes: 'ascii' and 'utf-8'.


### Close database connection

       db.close();
 

## <a name="DBFunctionsBDB"></a> Invocation of database functions (Berkeley DB and LMDB mode)

This mode of operation applies to BDB (or LMDB) databases opened with a **key\_type** of **int** or **str**. 

### Set a record

Synchronous:

       var result = db.set(<key>, <data>);

Asynchronous:

       db.set(<key>, <data>, callback(<error>, <result>));
      
Example:

       db.set(1, "John Smith");

### Get a record

Synchronous:

       var result = db.get(<key>);

Asynchronous:

       db.get(<key>, callback(<error>, <result>));
      
Example:

       var name = db.get(1);

* Note: use **get\_bx** to receive the result as a Node.js Buffer.

### Delete a record

Synchronous:

       var result = db.delete(<key>);

Asynchronous:

       db.delete(<key>, callback(<error>, <result>));
      
Example:

       var name = db.delete(1);


### Check whether a record is defined

Synchronous:

       var result = db.defined(<key>);

Asynchronous:

       db.defined(<key>, callback(<error>, <result>));
      
Example:

       var name = db.defined(1);


### Parse a set of records (in order)

Synchronous:

       var result = db.next(<key>);

Asynchronous:

       db.next(<key>, callback(<error>, <result>));
      
Example:

       var key = "";
       while ((key = db.next(key)) != "") {
          console.log("\nPerson: " + key + ' : ' + db.get(key));
       }


### Parse a set of records (in reverse order)

Synchronous:

       var result = db.previous(<key>);

Asynchronous:

       db.previous(<key>, callback(<error>, <result>));
      
Example:

       var key = "";
       while ((key = db.previous(key)) != "") {
          console.log("\nPerson: " + key + ' : ' + db.get(key));
       }

 
### <a name="CursorsBDB"></a> Cursor based data retrieval

This facility provides high-performance techniques for traversing records held in the database. 

#### Specifying the query

The first task is to specify the 'query' for the database traverse.

       query = new mcursor(db, {key: [<seed_key>]}[, <options>]);
Or:

       query = db.mglobalquery({key: [<seed_key>]}[, <options>]);

The 'options' object can contain the following properties:

* **getdata**: A boolean value (default: **getdata: false**). Set to 'true' to return data values associated with each database record returned.

* **format**: Format for output (default: not specified). If the output consists of multiple data elements, the return value (by default) is a JavaScript object made up of a 'key' array and an associated 'data' value.  Set to "url" to return such data as a single URL escaped string including the key value ('key1') and associated 'data' value.

Example (return all keys and names from the 'Person' global):

       query = db.mglobalquery({key: [""]}, {multilevel: false, getdata: true});

#### Traversing the dataset

In key order:

       result = query.next();

In reverse key order:

       result = query.previous();

In all cases these methods will return 'null' when the end of the dataset is reached.

Example 1 (return all key values from the database - returns a simple variable):

       query = db.mglobalquery({key: [""]});
       while ((result = query.next()) !== null) {
          console.log("result: " + result);
       }

Example 2 (return all key and data values from the database - returns an object):

       query = db.mglobalquery({key: [""]}, getdata: true);
       while ((result = query.next()) !== null) {
          console.log("result: " + JSON.stringify(result, null, '\t'));
       }


## <a name="DBFunctionsM"></a> Invocation of database functions (M emulation mode)

This mode of operation applies to BDB (or LMDB) databases opened with a **key\_type** of **m**. 

### Register a global name (and fixed key)


       global = new mglobal(db, <global_name>[, <fixed_key>]);
Or:

       global = db.mglobal(<global_name>[, <fixed_key>]);

Example (using a global named "Person"):

       var person = db.mglobal("Person");

### Set a record

Synchronous:

       var result = <global>.set(<key>, <data>);

Asynchronous:

       <global>.set(<key>, <data>, callback(<error>, <result>));
      
Example:

       person.set(1, "John Smith");

### Get a record

Synchronous:

       var result = <global>.get(<key>);

Asynchronous:

       <global>.get(<key>, callback(<error>, <result>));
      
Example:

       var name = person.get(1);

* Note: use **get\_bx** to receive the result as a Node.js Buffer.

### Delete a record

Synchronous:

       var result = <global>.delete(<key>);

Asynchronous:

       <global>.delete(<key>, callback(<error>, <result>));
      
Example:

       var name = person.delete(1);


### Check whether a record is defined

Synchronous:

       var result = <global>.defined(<key>);

Asynchronous:

       <global>.defined(<key>, callback(<error>, <result>));
      
Example:

       var name = person.defined(1);


### Parse a set of records (in order)

Synchronous:

       var result = <global>.next(<key>);

Asynchronous:

       <global>.next(<key>, callback(<error>, <result>));
      
Example:

       var key = "";
       while ((key = person.next(key)) != "") {
          console.log("\nPerson: " + key + ' : ' + person.get(key));
       }


### Parse a set of records (in reverse order)

Synchronous:

       var result = <global>.previous(<key>);

Asynchronous:

       <global>.previous(<key>, callback(<error>, <result>));
      
Example:

       var key = "";
       while ((key = person.previous(key)) != "") {
          console.log("\nPerson: " + key + ' : ' + person.get(key));
       }


### Increment the value of a global node

Synchronous:

       var result = <global>.increment(<key>, <increment_value>);

Asynchronous:

       <global>.increment(<key>, <increment_value>, callback(<error>, <result>));
      
Example (increment the value of the "counter" node by 1.5 and return the new value):

       var result = person.increment("counter", 1.5);


### Lock a global node

Synchronous:

       var result = <global>.lock(<key>, <timeout>);

Asynchronous:

       <global>.lock(<key>, <timeout>, callback(<error>, <result>));
      
Example (lock global node '1' with a timeout of 30 seconds):

       var result = person.lock(1, 30);

* Note: Specify the timeout value as '-1' for no timeout (i.e. wait until the global node becomes available to lock).


### Unlock a (previously locked) global node

Synchronous:

       var result = <global>.unlock(<key>);

Asynchronous:

       <global>.unlock(<key>, callback(<error>, <result>));
      
Example (unlock global node '1'):

       var result = person.unlock(1);


### Merge (or copy) part of one global to another


Synchronous (merge from global2 to global1):

       var result = <global1>.merge([<key1>,] <global2> [, <key2>]);

Asynchronous (merge from global2 to global1):

       <global1>.defined([<key1>,] <global2> [, <key2>], callback(<error>, <result>));
      
Example 1 (merge ^MyGlobal2 to ^MyGlobal1):

       global1 = new mglobal(db, 'MyGlobal1');
       global2 = new mglobal(db, 'MyGlobal2');
       global1.merge(global2);

Example 2 (merge ^MyGlobal2(0) to ^MyGlobal1(1)):

       global1 = new mglobal(db, 'MyGlobal1', 1);
       global2 = new mglobal(db, 'MyGlobal2', 0);
       global1.merge(global2);

Alternatively:

       global1 = new mglobal(db, 'MyGlobal1');
       global2 = new mglobal(db, 'MyGlobal2');
       global1.merge(1, global2, 0);

### Reset a global name (and fixed key)

       <global>.reset(<global_name>[, <fixed_key>]);

Example:

       // Process orders for customer #1
       customer_orders = db.mglobal("Customer", 1, "orders")
       do_work ...

       // Process orders for customer #2
       customer_orders.reset("Customer", 2, "orders");
       do_work ...

 
## <a name="Cursors"></a> Cursor based data retrieval

This facility provides high-performance techniques for traversing records held in database globals. 

### Specifying the query

The first task is to specify the 'query' for the global traverse.

       query = new mcursor(db, {global: <global_name>, key: [<seed_key>]}[, <options>]);
Or:

       query = db.mglobalquery({global: <global_name>, key: [<seed_key>]}[, <options>]);

The 'options' object can contain the following properties:

* **multilevel**: A boolean value (default: **multilevel: false**). Set to 'true' to return all descendant nodes from the specified 'seed_key'.

* **getdata**: A boolean value (default: **getdata: false**). Set to 'true' to return any data values associated with each global node returned.

* **format**: Format for output (default: not specified). If the output consists of multiple data elements, the return value (by default) is a JavaScript object made up of a 'key' array and an associated 'data' value.  Set to "url" to return such data as a single URL escaped string including all key values ('key[1->n]') and any associated 'data' value.

Example (return all keys and names from the 'Person' global):

       query = db.mglobalquery({global: "Person", key: [""]}, {multilevel: false, getdata: true});

### Traversing the dataset

In key order:

       result = query.next();

In reverse key order:

       result = query.previous();

In all cases these methods will return 'null' when the end of the dataset is reached.

Example 1 (return all key values from the 'Person' global - returns a simple variable):

       query = db.mglobalquery({global: "Person", key: [""]});
       while ((result = query.next()) !== null) {
          console.log("result: " + result);
       }

Example 2 (return all key values and names from the 'Person' global - returns an object):

       query = db.mglobalquery({global: "Person", key: [""]}, multilevel: false, getdata: true);
       while ((result = query.next()) !== null) {
          console.log("result: " + JSON.stringify(result, null, '\t'));
       }


Example 3 (return all key values and names from the 'Person' global - returns a string):

       query = db.mglobalquery({global: "Person", key: [""]}, multilevel: false, getdata: true, format: "url"});
       while ((result = query.next()) !== null) {
          console.log("result: " + result);
       }

Example 4 (return all key values and names from the 'Person' global, including any descendant nodes):

       query = db.mglobalquery({global: "Person", key: [""]}, {{multilevel: true, getdata: true});
       while ((result = query.next()) !== null) {
          console.log("result: " + JSON.stringify(result, null, '\t'));
       }

* M programmers will recognise this last example as the M **$Query()** command.
 

### Traversing the global directory (return a list of global names)

       query = db.mglobalquery({global: <seed_global_name>}, {globaldirectory: true});

Example (return all global names held in the current directory)

       query = db.mglobalquery({global: ""}, {globaldirectory: true});
       while ((result = query.next()) !== null) {
          console.log("result: " + result);
       }


## <a name="Binary"></a> Working with binary data

In **mg-dbx-bdb** the default character encoding scheme is UTF-8.  When transmitting binary data between the database and Node.js there are two options.

* Switch to using the 8-bit ASCII character set.
* Receive the incoming data into Node.js Buffers.

On the input (to the database) side all **mg-dbx-bdb** function arguments can be presented as Node.js Buffers and **mg-dbx-bdb** will automatically detect that an argument is a Buffer and process it accordingly.

On the retrieval side, the following functions can be used to return the output as a Node.js Buffer.

* db::get\_bx
* mglobal::get\_bx


These functions work the same way as their non '_bx' suffixed counterparts.  The only difference is that they will return data as a Node.js Buffer as opposed to a type of String.

The following two examples illustrate the two schemes for receiving binary data from the database.

Example 1: Receive binary data from a DB function as a Node.js 8-bit character stream

       <db>.charset('ascii');
       var stream_str8 = <db>.get(<key>);
       <db>.charset('utf-8'); // reset character encoding

Example 2: Receive binary data from a DB record as a Node.js Buffer

       var stream_buffer = <db>.get_bx(<key>);


## <a name="Threads"></a> Using Node.js/V8 worker threads

**mg-dbx-bdb** functionality can be used with Node.js/V8 worker threads.  This enhancement is available with Node.js v12 (and later).

* Note: be sure to include the property **multithreaded: true** in the **open** method when opening database  connections to be used in multi-threaded applications.

Use the following constructs for instantiating **mg-dbx-bdb** objects in multi-threaded applications:

        // Use:
        var <global> = new mglobal(<db>, <global>);
        // Instead of:
	    var <global> = <db>.mglobal(<global>);

        // Use:
        var <cursor> = new mcursor(<db>, <global_query>);
        // Instead of:
        var <cursor> = <db>.mglobalquery(<global_query>)


The following scheme illustrates how **mg-dbx-bdb** should be used in threaded Node.js applications.

       const { Worker, isMainThread, parentPort, threadId } = require('worker_threads');

       if (isMainThread) {
          // start the threads
          const worker1 = new Worker(__filename);
          const worker2 = new Worker(__filename);

          // process messages received from threads
          worker1.on('message', (message) => {
             console.log(message);
          });
          worker2.on('message', (message) => {
             console.log(message);
          });
       } else {
          var dbxbdb = require('mg-dbx-bdb').dbxbdb;
          // And as required ...
          var mglobal = require('mg-dbx-bdb').mglobal;
          var mcursor = require('mg-dbx-bdb').mcursor;

          var db = new dbxbdb();
          db.open(<parameters>);

          var global = new mglobal(db, <global>);

          // do some work

          var result = db.close();
          // tell the parent that we're done
          parentPort.postMessage("threadId=" + threadId + " Done");
       }

## <a name="EventLog"></a> The Event Log

**mg\-dbx\-bdb** provides an Event Log facility for recording errors in a physical file and, as an aid to debugging, recording the **mg\-dbx\-bdb** functions called by the application.  This Log facility can also be used by Node.js applications.

To use this facility, the Event Log file must be specified using the following function:


       db.setloglevel(<log_file>, <Log_level>, <log_filter>);

Where:

* **log\_file**: The name (and path to) the log file you wish to use. The default is c:/temp/mg-dbx-bdb.log (or /tmp/mg-dbx-bdb.log under UNIX).
* **log\_level**: A set of characters to include one or more of the following:
	* **e** - Log error conditions.
	* **f** - Log all **mg\-dbx\-bdb** function calls (function name and arguments).
	* **t** - Log the request data buffers to be transmitted from **mg\-dbx\-bdb** to the DB Server.
	* **r** - Log the request data buffers to be transmitted from **mg\-dbx\-bdb** to the DB Server and the corresponding response data.
* **log\_filter**: A comma-separated list of functions that you wish the log directive to be active for. This should be left empty to activate the log for all functions.

Examples:

      db.setloglevel("c:/temp/mg-dbx-bdb.log", "e", "");
      db.setloglevel("/tmp/mg-dbx-bdb.log", "ft", "dbx::set,mglobal::set,mcursor::execute");

Node.js applications can write their own messages to the Event Log using the following function:

      db.logmessage(<message>, <title>);

Logging can be switched off by calling the **setloglevel** function without specifying a log level.  For example:

      db.setloglevel("c:/temp/mg-dbx-bdb.log");

## <a name="License"></a> License

Copyright (c) 2018-2021 M/Gateway Developments Ltd,
Surrey UK.                                                      
All rights reserved.
 
http://www.mgateway.com                                                  
Email: cmunt@mgateway.com
 
 
Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.      

## <a name="RelNotes"></a>Release Notes

### v1.0.1 (1 January 2021)

* Initial Release

### v1.0.2 (7 January 2021)

* Correct a fault in the processing of integer based keys.

### v1.0.3 (17 January 2021)

* Miscellaneous bug fixes.
* Extend the logging of request transmission data to include the corresponding response data.
	* Include 'r' in the log level.  For example: db.setloglevel("MyLog.log", "eftr", "");

### v1.1.4 (20 January 2021)

* Introduce the ability to specify a BDB environment for the purpose of implementing multi-process concurrent access to a database.

### v1.1.5 (23 February 2021)

* Correct a fault that resulted in a crash when loading the **mg-dbx-bdb** module in Node.js v10.
	* This change only affects **mg-dbx-bdb** for Node.js v10.

### v1.2.6 (2 March 2021)

* Add support for Lightning Memory-Mapped Database (LMDB).

### v1.2.7 (10 March 2021)

* Correct a fault that resulted in **mglobal.previous()** calls erroneously returning empty string - particularly in relation to records at the end of a BDB/LMDB database.

### v1.2.8 (12 April 2021)

* Correct a fault in the **increment()** method for LMDB.
