//
// mg-dbx-bdb.node: A simple demo using string based keys
//

var bdb = require('mg-dbx-bdb').dbxbdb;
var mcursor = require('mg-dbx-bdb').mcursor;
var db = new bdb();

// Modify the parameters in the open() method to suit your installation

if (process.platform == 'win32') {
   var open = db.open({type: "BDB", db_library: "c:/c/bdb/libdb181.dll", db_file: "c:/bdb/string.db", key_type: "str"});
}
else {
   var open = db.open({type: "BDB", db_library: "/usr/local/BerkeleyDB.18.1/lib/libdb.so", db_file: "/opt/bdb/string.db", key_type: "str"});
}

console.log("Version: " + db.version());

console.log("Setting up some records ...");
for (n = 0; n < 20; n += 2) {
  db.set("key#" + n, "Record #" + n);
  console.log("key#" + n + " = " + "Record #" + n);
}

console.log("\nGet Record 'key#2': " + db.get("key#2"));
console.log("Is Record 'key#2' Defined?: " + db.defined("key#2"));
console.log("Delete Record 'key#2': " + db.delete("key#2"));
console.log("Is Record 'key#2' Defined?: " + db.defined("key#2"));

console.log("\nParse the set of records ...");
var key = "";
while ((key = db.next(key)) != "") {
   console.log("Next Record: " + key + ': ' + db.get(key));
}

console.log("\nParse the set of records using a cursor, starting after record 'key#7' ...");
var query = new mcursor(db, {key: ["key#7"]}, {getdata: true});
while ((result = query.next()) !== null) {
   console.log("Record: " + JSON.stringify(result, null, 2));
}

console.log("\nClosing the database");
db.close();


