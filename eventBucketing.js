const startTime=1572295309324;
const endTime=1572295708278;

const dividerStart=Math.floor(startTime / 1000)*1000;
const dividerEnd=Math.round(endTime / 1000)*1000;

//console.log("[", dividerStart, ", ", dividerEnd,"]");

var dividers = [];
for (d=dividerStart; d<dividerEnd; d=d+10000) {
    dividers.push(d);
}
//console.log(dividers);

printjson(dividers);


db.event_data.aggregate([
    {
	$match: {
	    test_id : "test_1572295309",
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
    }
])


