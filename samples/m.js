//
// mg-dbx-bdb.node: A simple demo using the M database emulation
//

var bdb = require('mg-dbx-bdb').dbxbdb;
var mglobal = require('mg-dbx-bdb').mglobal;
var mcursor = require('mg-dbx-bdb').mcursor;
var db = new bdb();

if (process.platform == 'win32') {
   var open = db.open({type: "BDB", db_library: "c:/c/bdb/libdb181.dll", db_file: "c:/bdb/m.db", key_type: "m"});
}
else {
   var open = db.open({type: "BDB", db_library: "/usr/local/BerkeleyDB.18.1/lib/libdb.so", db_file: "/opt/bdb/m.db", key_type: "m"});
}

console.log("Version: " + db.version());

console.log("Setting up some records ...");

patient = new mglobal(db, "patient");
admission = new mglobal(db, "admission");

patient.set(1, "John Smith");
patient.set(2, "Jane Jones");

admission.set(1, "2020-06-07", "Ward #1");
admission.set(1, "2020-11-12", "Ward #2");
admission.set(1, "2021-01-03", "Ward #1a");

console.log("\nList all registered patients ...");
var id = "";
while ((id = patient.next(id)) != "") {
   console.log("Next Patient Record: " + id + ': ' + patient.get(id));
}

console.log("\nList all admission records for patient #1 in ascending order ...");
var id = 1;
var date = '';
while ((date = admission.next(id, date)) != "") {
   console.log("Next Admission Record: id=" + id + '; date=' + date + '; ward=' + admission.get(id, date));
}

console.log("\nList all 2020 admission records for patient #1 in descending order ...");
var id = 1;
var date = '2021-01-01';
while ((date = admission.previous(id, date)) != "") {
   console.log("Previous Admission Record: id=" + id + '; date=' + date + '; ward=' + admission.get(id, date));
}

console.log("\nList all admission records in ascending order ...");
var query = new mcursor(db, {global: "admission"}, {multilevel: true, getdata: true});
while ((result = query.next()) !== null) {
   console.log("Record: " + JSON.stringify(result, null, 2));
}

console.log("\nClosing the database");
db.close();

