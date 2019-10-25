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




const testNum = Math.floor(Date.now() / 1000);
const perfURL = "http://ec2-3-87-195-226.compute-1.amazonaws.com:3334/test_run/" + getTestId();

function getTestId () {
    return "test_" + testNum;
}

MongoClient.connect(appConnectionStr, mongoOptions, function(err, client) {

    const runTest = async client => {
	
	//Create test in logging system
	const response = await axios.post(perfURL);
	const testInfo = response.data;
	console.log("Starting test: ", testInfo);
    
	var db = client.db("Messages");
	var messagesCol = db.collection("Messages");

	console.log("Starting to read");

	const recordTestStart = axios.patch(perfURL, {
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
		const startInsert = axios.post(perfURL + '/log', {
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

		    const recordTestEnd = axios.patch(perfURL, {
			measurement_name: "endTest",
			measurements: [{timeStamp: Date.now()}]
		    })
		}
		if (debug) {console.log("completeInsert");}
		const completeInsert = axios.post(perfURL + '/log', {
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
