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


const testNum = Math.floor(Date.now() / 1000);
const perfURL = "http://localhost:3334/test_run/test_" + testNum;



MongoClient.connect(appConnectionStr, mongoOptions, function(err, client) {

    const runTest = async client => {
	
	//Create test in logging system
	const response = await axios.post(perfURL);
	const testInfo = response.data;
	console.log("Starting test: ", testInfo);
    
	var db = client.db("Messages");
	var messagesCol = db.collection("Messages");

	console.log("Starting to read");
    
	var rl = readline.createInterface({
	    input: process.stdin,
	    //  output: process.stdout,
	    terminal: false
	});

	async function processDocLine(docLine) {

	    try {

		var doc = JSON.parse(docLine);
		console.log("startInsert");
		const startInsert = axios.patch(perfURL, {
		    measurement_name: "startInsert",
		    measurements: [{
			timeStamp: Date.now(),
			docId: doc.id}]
		});

		console.log("numProcessing: ", numProcessing, "> Insert MongoDB");
		let iResult = await messagesCol.insertOne(doc);
		numProcessing--;
		if (fileComplete && numProcessing == 0) {
	    	    client.close();
		}
		console.log("completeInsert");
		const completeInsert = axios.patch(perfURL, {
		    measurement_name: "completeInsert",
		    measurements: [{
			timeStamp: Date.now(),
			docId: doc.id
		    }]
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
