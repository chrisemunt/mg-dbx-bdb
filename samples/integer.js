//
// mg-dbx-bdb.node: A simple demo using integer based keys
//

var bdb = require('mg-dbx-bdb').dbxbdb;
var mcursor = require('mg-dbx-bdb').mcursor;
var db = new bdb();

var lmdb = process.argv[2];

// Modify the parameters in the open() method to suit your installation

if (process.platform == 'win32') {
   if (lmdb == 1)
      var open = db.open({type: "LMDB", db_library: "c:/LMDBWindows/lib/LMDBWindowsDll64.dll", env_dir: "c:/bdb/integer", key_type: "int"});
   else
      var open = db.open({type: "BDB", db_library: "c:/c/bdb/libdb181.dll", db_file: "c:/bdb/integer.db", key_type: "int"});
}
else {
   if (lmdb == 1)
      var open = db.open({type: "LMDB", db_library: "liblmdb.so", env_dir: "/opt/bdb/integer", key_type: "int"});
   else
      var open = db.open({type: "BDB", db_library: "/usr/local/BerkeleyDB.18.1/lib/libdb.so", db_file: "/opt/bdb/integer.db", key_type: "int"});
}

console.log("Version: " + db.version());

console.log("Setting up some records ...");
for (n = 0; n < 20; n += 2) {
  db.set(n, "Record #" + n);
  console.log(n + " = " + "Record #" + n);
}

console.log("\nGet Record #2: " + db.get(2));
console.log("Is Record #2 Defined?: " + db.defined(2));
console.log("Delete Record #2: " + db.delete(2));
console.log("Is Record #2 Defined?: " + db.defined(2));

console.log("\nParse the set of records ...");
var key = "";
while ((key = db.next(key)) != "") {
   console.log("Next Record: " + key + ': ' + db.get(key));
}

console.log("\nParse the set of records using a cursor, starting after record #7 ...");
var query = new mcursor(db, {key: [7]}, {getdata: true});
while ((result = query.next()) !== null) {
   console.log("Record: " + JSON.stringify(result, null, 2));
}

console.log("\nClosing the database");
db.close();


