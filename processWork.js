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
var numProcessing = 0;           //number of lines where processing is in flight
var count = 0;                   //number of lines read
var numComplete=0;                 //numvwe of lines completely processed
var fileComplete = false;
var debug = false;
const numAppServers = 4;
const startPort = 3334;

const testNum = Math.floor(Date.now() / 1000);
const perfURL = "http://ec2-3-87-195-226.compute-1.amazonaws.com:____/test_run/" + getTestId();
var perfURLs = generatePerfUrls(perfURL, startPort, numAppServers);

const aggQuery = [
    {
	$sample: {
	    size: 100
	}
    },
    {
	$group: {
	    _id: null,
	    avgAge: {
		$avg: "$age"
	    },
	    emails : {
		$push : "$emails"
	    }
	}
    },
    {
	$project: {
	    _id : 0,
	    avgAge: 1,
	    emails : {
		$reduce : {
		    input : "$emails",
		    initialValue : [],
			in: {$setUnion : ["$$value", "$$this"]}
		}
	    }
	}
    }
];

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

function logProgress() {
    console.log("Read: ", count, " inFlight: ", numProcessing, " Completed: ", numComplete);
}

MongoClient.connect(appConnectionStr, mongoOptions, function(err, client) {

    const runTest = async client => {
	
	//Create test in logging system
	const response = await axios.post(getPerfUrl());
	const testInfo = response.data;
	console.log("Starting test: ", testInfo);
    
	var db = client.db("Messages");
	const messagesCol = db.collection("Messages");
	const aggMessagesCol = db.collection("aggMessages");

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
		let iResult = messagesCol.insertOne(doc);
		let aggResult = await messagesCol.aggregate(aggQuery).toArray();
		let aggIResult = aggMessagesCol.insertOne(aggResult[0]).catch(function(err) {
		    console.log("Unable to insert agg document. numProcessing: ", numProcessing);
		    console.log(err);
		});

		const messInsertResponse = await iResult;
		const aggInsertResponse = await aggIResult;

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
		numProcessing--;
		numComplete++;

		if (numComplete%100 == 0) {
		    logProgress();
		}

		if (fileComplete && numProcessing == 0) {

		    const recordTestEnd = await axios.patch(getPerfUrl(), {
			measurement_name: "endTest",
			measurements: [{timeStamp: Date.now()}]
		    });
		    client.close();

		}
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
		logProgress();
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
	    logProgress();
	    fileComplete = true;
	})

    }

    runTest(client);
});



/* More complex aggregation for testing
[{$sample: {
  size: 100
}}, {$group: {
  _id: null,
  avgAge: {
    $avg: "$age"
  },
  emails : {
    $push : "$emails"
  }
}}, {$project: {
  _id : 0,
  avgAge: 1,
  emails : {$reduce : {
    input : "$emails",
    initialValue : [],
    in: {$setUnion : ["$$value", "$$this"]}
  }
  }
}}]
*/
