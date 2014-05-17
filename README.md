# minwq
Minimalistic Node.JS Work Queue, backed by Redis. (>=2.6.* redis required with Lua support)

    npm install minwq

##Usage
	var minwq = require("minwq")

	// Create a new store
	var store= new minwq([options]);

	// Optional
	// You can provide a queue suffix for all jobs for namespacing purposes.
	var store = new minwq({
		prefix: "myapp",
	});

	// If you need to provide custom redis clients (due to authentication or because you just want to), you can override createClient function of store;
	minwq.prototype.createClient = function() {
		return redis.createClient(..whatever);
	}

#### Pushing - Push a new job to a queue

* `queue` [required] - Name of the queue
* `data` [required] - Job payload
* `delay` [optional] - Delay job execution (in milliseconds)
* `unique` [optional] - If provided, only a single living job can have the unqiue token, additional push requests will fail.
* `replace` [optional] - Boolean. If a unique key is provided and a duplicate job is pushed, it is ignored by default. Provide replace: true to replace the older job with new one.
* `callback(err, jobid)` - Gets called when the job is created

***

```
store.push({
	queue: "email",
	data: {
		to: "foo@bar.com",
		subject: "Foo",
		text: "Bar"
	},
	delay: 100,
	unique: "unqiue token",
}, callback);
```

#### Poping - Pop a job from queue

* `ttl` [optional] - Job retry delay (in milliseconds).

If a job takes more than `ttl` milliseconds to complete, it will be requeued.
No TTL means the hob will be removed from queue immediately when the pop function gets called.

***

```
store.pop({
	queue: "email",
	ttl: 10000
}, function(err, job) {
	// job.data contains the job payload
	console.log(job.data);

	// after finishing the job, you need to remove it explicitly from the queue:
	job.remove(callback);
});
```

##license
MIT
