// ================================================================
// processWork.js
//
// ================================================================

var readline = require('readline');
var mongo = require('mongodb');
var axios = require('axios');

const appConnectionStr = 'mongodb://admin:power_low12@ec2-3-87-195-226.compute-1.amazonaws.com:27017';
const mongoOptions = { useNewUrlParser: true, useUnifiedTopology: true };

var MongoClient = mongo.MongoClient;
var numProcessing = 0;
var count = 0;
var fileComplete = false;
var debug = false;
const numAppServers = 4;
const startPort = 3334;

const testNum = Math.floor(Date.now() / 1000);
const perfURL = "http://ec2-3-87-195-226.compute-1.amazonaws.com:____/test_run/" + getTestId();
var perfURLs = generatePerfUrls(perfURL, startPort, numAppServers);

console.log("App server URLs: ", perfURLs);
console.log("A random URL: ", getPerfUrl());
console.log("A random URL: ", getPerfUrl());
console.log("A random URL: ", getPerfUrl()); 
console.log("A random URL: ", getPerfUrl());
console.log("A random URL: ", getPerfUrl());
console.log("A random URL: ", getPerfUrl()); 

function generatePerfUrls (url, sPort, num) {
    list = [];
    for (i=0; i<num; i++) {
	port = sPort + i;
	list.push(url.replace('____', port));
    }
    return list;
}

function getPerfUrl() {
    return perfURLs[Math.floor(Math.random() * numAppServers)];
}



function getTestId () {
    return "test_" + testNum;
}

MongoClient.connect(appConnectionStr, mongoOptions, function(err, client) {

    const runTest = async client => {
	
	//Create test in logging system
	const response = await axios.post(getPerfUrl());
	const testInfo = response.data;
	console.log("Starting test: ", testInfo);
    
	var db = client.db("Messages");
	var messagesCol = db.collection("Messages");

	console.log("Starting to read");

	const recordTestStart = axios.patch(getPerfUrl(), {
	    measurement_name: "startTest",
	    measurements: [{timeStamp: Date.now()}]
	})
    
	var rl = readline.createInterface({
	    input: process.stdin,
	    //  output: process.stdout,
	    terminal: false
	});

	async function processDocLine(docLine) {

	    try {

		var doc = JSON.parse(docLine);
		if (debug) {console.log("startInsert");}
		const startInsert = axios.post(getPerfUrl() + '/log', {
		    //		    type: "measurements",
		    eventType: "startInsert",
		    measurements: {
			timeStamp: Date.now(),
			docId: doc.id
		    }
		});

		if (debug) {console.log("numProcessing: ", numProcessing, "> Insert MongoDB");}
		let iResult = await messagesCol.insertOne(doc);
		numProcessing--;
		if (fileComplete && numProcessing == 0) {
	    	    client.close();

		    const recordTestEnd = axios.patch(getPerfUrl(), {
			measurement_name: "endTest",
			measurements: [{timeStamp: Date.now()}]
		    })
		}
		if (debug) {console.log("completeInsert");}
		const completeInsert = axios.post(getPerfUrl() + '/log', {
		    eventType: "completeInsert",
		    measurements: {
			timeStamp: Date.now(),
			docId: doc.id
		    }
		});

		const startInsertResponse = await startInsert;
		const completeInsertResponse = await completeInsert;
	    }
	    catch(err) {
		console.log(err);
	    }
	}
	
	rl.on('line', function(line){
//	    console.log(line);
	    numProcessing++;
	    count++;

	    if (count%100 == 0) {
		console.log(count);
	    }



	    processDocLine(line);
	
	    // messagesCol.insertOne(doc).then(result => {
	    // 	numProcessing--;
	    // 	if (fileComplete && numProcessing == 0) {
	    // 	    client.close();
	    // 	}
	    // });
	})

	rl.on('close', () => {
	    console.log("File Processing complete.");
	    fileComplete = true;
	})

    }

    runTest(client);
});
