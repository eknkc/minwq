# minwq
Minimalistic Node.JS Work Queue, backed by Redis.

	npm install minwq
	
##usage
	var minwq = require("minwq")
	
	// Create a new store
	var store= minwq();
	
	// Optional
	// You can provide a queue suffix for all jobs for namespacing purposes.
	// Also, you can provide a custom redis client in case you need to connect to a redis server with authentication or such
	var store = minwq({
		prefix: "myapp",
		client: redis.createClient(...)
	});	
	
	// Push a new job to a queue
	// queue [required] - Name of the queue
	// data [required] - Job payload
	// ttr [optional] - Job retry delay.
	// If a job takes more than ttr seconds to complete, it will be requeued.
	// No TTR means the hob will be removed from queue immediately when the pop function gets called.
	// callback(err, jobid) - Gets called when the job is created
	store.push({
		queue: "email",
		data: {
			to: "foo@bar.com",
			subject: "Foo",
			text: "Bar"
		},
		ttr: 180
	}, callback);
	
	
	// Pop a job from queue
	store.pop({
		queue: "email"
	}, function(err, job) {
		// job.data contains the job payload
		console.log(job.data);
		
		// after finishing the job, you need to remove it explicitly from the queue:
		job.remove(callback);
		
		// or, you can push it back to the queue
		job.pushback(callback)
	});
	
##license
MIT