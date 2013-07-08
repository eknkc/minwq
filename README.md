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
	store.createClient = function() {
		return redis.createClient(..whatever);
	}

#### Pushing - Push a new job to a queue

* `queue` [required] - Name of the queue
* `data` [required] - Job payload
* `delay` [optional] - Delay job execution (in seconds)
* `unique` [optional] - If provided, only a single living job can have the unqiue token, additional push requests will fail.
* `priority` [optional] - Integer, denoting the job priority. Higher gets executed first.
* `ttr` [optional] - Job retry delay (in seconds).
* `callback(err, jobid)` - Gets called when the job is created
* **

If a job takes more than `ttr` seconds to complete, it will be requeued.
No TTR means the hob will be removed from queue immediately when the pop function gets called.

	store.push({
		queue: "email",
		data: {
			to: "foo@bar.com",
			subject: "Foo",
			text: "Bar"
		},
		delay: 100,
		unique: "unqiue token",
		priority: 1,
		ttr: 180
	}, callback);


#### Poping - Pop a job from queue

	store.pop({
		queue: "email"
	}, function(err, job) {
		// job.data contains the job payload
		console.log(job.data);

		// after finishing the job, you need to remove it explicitly from the queue:
		job.remove(callback);
	});

##Polling
#### Job Consumer and empty queue callback

	// Called for every poll iteration
	var consumer = function (payload, [args,] next) {
		// `payload` is the job message
		// `this` keyword is determined by the `bind` option
		// `args` determined by the `args`option
		// `next(err)` is the acknowledgment to queue
	}

	// Called when the queue is emptied
	var done = function () {}

#### Making the Subscription

* `channel` [required] - Name of the queue to be subscribed
* `type` [required] - Polling algorithm, TYPE.LINEAR, TYPE.ADAPTIVE
* `value` [required] - Poll iteration interval (in seconds)
* `handler` [required] - Job consumer function
* `bind` [optional] - `handler` bind
* `done` [optional] - Queue emptied callback
* `args` [optional] - Additional arguments to the `handler`
* **

	var subscriptionId = store.poll.subscribe({
		channel: 'email',
		type: store.poll.TYPE.INTERVAL,
		value: 5,
		handler: consumer,
		bind: { test: 'a' },
		done: done,
		args: [10, 20]
	});

#### Manipulating the Subscription

	// Start the polling for the given `subscriptionId`
	store.poll.start(subscriptionId);

	// Stop the polling for the given `subscriptionId`
	// Do not delete the subscription
	store.poll.stop(subscriptionId);

	// Stop the polling for the given `subscriptionId`
	// Delete the subscription
	store.poll.unsubscribe(subscriptionId);

##license
MIT
