
batchSize=$1
iterations=$2
sleepSec=$3

for((i=0;i<$iterations;i++))
do
    mgeneratejs '{"id" : {"$inc" : {"start": 1}}, "name": "$name", "age": "$age", "emails": {"$array": {"of": "$email", "number": 3}}}' -n $batchSize 
  sleep ${sleepSec}s
done
