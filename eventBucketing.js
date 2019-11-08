
var db = db.getSiblingDB("results");

var latestTest = db.test_run.find().sort({last_modified : -1}).limit(1).next();

var testStartTime=latestTest.startTest[0].timeStamp;
var testEndTime=latestTest.endTest[0].timeStamp;

var opsMgrCollStartTime=NumberLong(new Date(db.metrics_hosts.findOne().measurements[0].dataPoints[0].timestamp));

//var dividerStart=Math.floor(testStartTime / 1000)*1000;
var dividerStart=opsMgrCollStartTime;
var dividerEnd=Math.round(testEndTime / 1000)*1000;

//console.log("[", dividerStart, ", ", dividerEnd,"]");

var dividers = [];
for (d=dividerStart; d<dividerEnd; d=d+10000) {
    dividers.push(d);
}
//console.log(dividers);

//printjson(dividers);

var aggPipeline = [
    {
	$match: {
	    test_id : latestTest._id,
	    "data.eventType" : "completeInsert"
	}
    },
    {
	$bucket: {
	    groupBy: "$data.measurements.timeStamp",
	    boundaries: dividers,
	    default: "errors",
	    output: {
		count: { $sum : 1 }
	    }
	}
    },
    {
	$addFields : {
	    date: {
		"$convert" : {
		    input: "$_id",
		    to: "date",
		    onError: 0
		}
	    },
	    dateStr: {
		$convert : {
		    input : {
			"$convert" : {
			    input: "$_id",
			    to: "date",
			    onError: 0
			}
		    },
		    to: "string",
		    onError: 0
		}
	    },
	    perSec: { $multiply : ["$count", 0.1]}
	}
    },
    {
	$out : "complete10SBucket"
    }
];

var results = db.event_data.aggregate(aggPipeline).toArray();

printjson(results);

//db.createView("complete10SBucket", "event_data", aggPipeline);

var opCounterInsertViewPipe = [
    {
	$unwind: {
	    path: "$measurements"
	}
    },
    {
	$match: {
	    "measurements.name" : "OPCOUNTER_INSERT"
	}
    },
    {
	$unwind: {
	    path: "$measurements.dataPoints",
	    includeArrayIndex: 'index'
	}
    },
    {
	$project : {
	    _id : 0,
	}
    },
    {
	$addFields : {
	    measurementTimeStr : {
		$convert : {
		    input: "$measurements.dataPoints.timestamp",
		    to: "string",
		    onError: 0
		}
	    },
	    measurementTime : {
		$convert : {
		    input: "$measurements.dataPoints.timestamp",
		    to: "date",
		    onError: 0
		}
	    }
	}
    },
    {
	$out: "opcounter_insert"
    }
];

const metricHostTimeBucketPipe = [
    {
	$unwind: {
	    path: '$measurements'
	}
    },
    {
	$unwind: {
	    path: '$measurements.dataPoints',
	    includeArrayIndex: 'index'
	}
    },
    {
	$group: {
	    _id: "$measurements.dataPoints.timestamp",
	    metrics: {
		$push: {
		    k: "$measurements.name", 
		    v: "$measurements.dataPoints.value"
		}
	    }
	}
    },
    {
	$project: {
	    _id: 0,
	    measurementTimeStr: "$_id",
	    measurementTime: {
		$convert: {
		    input: '$_id',
		    to: 'date',
		    onError: 0
		}
	    },
	    metrics: {$arrayToObject : "$metrics"}
	}
    },
    {
	$out: 'metrics_host_timeBuckets'
    }
];


db.metrics_hosts.aggregate(opCounterInsertViewPipe);



var joinFromOMgrToAppData = [
    {
	$lookup: {
	    from: 'complete10SBucket',
	    localField: 'measurementTime',
	    foreignField: 'date',
	    as: 'appInserts'
	}
    }
];
