# pocPerfToolkit

Run like:
>mgeneratejs '{"id" : {"$inc" : {"start": 1}}, "name": "$name", "age": "$age", "emails": {"$array": {"of": "$email", "number": 3}}}' -n 2000 | node processWork.js
