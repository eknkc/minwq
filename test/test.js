var mwq = require("../")
  , should = require('should')
  , redis = require('redis')
  , async = require('async');

describe("minwq", function() {
  var redisSettings = {
    port: '6379',
    host: '127.0.0.1'
  };
  var redisClient = redis.createClient(redisSettings.port, redisSettings.host);
  var q = new mwq({
    prefix: "myapp",
    client: redisClient
  });

  var testQueues = ['test1', 'test2', 'test3', 'test4', 'test5', 'test6', 'prioritytest'];

  after(function (next) {
    async.map(testQueues, function (queue, next) {
      q.clear(queue, next);
    }, next);
  });

  it('should load the config correctly', function (next) {
    this.timeout(0);
    q.options.prefix.should.be.equal('myapp');
    next();
  });

  it('should close the redis connection safely', function (next) {
    this.timeout(0);
    q.close();
    should.not.exist(q.options.client);
    redisClient = redis.createClient(redisSettings.port, redisSettings.host);
    q.options.client = redisClient;
    next();
  });

  it('should not push without redis connection', function (next) {
    this.timeout(0);
    q.close();
    q.push({ queue: "test1", data: { x: 1, y: 2 } }, function (err, job) {
      should.exist(err);
      should.not.exist(job);
      should.equal(err.message, 'Redis not specified.');
      redisClient = redis.createClient(redisSettings.port, redisSettings.host);
      q.options.client = redisClient;
      next();
    });
  });

  it('should not pop without redis connection', function (next) {
    this.timeout(0);
    q.push({ queue: "test1", data: { x: 1, y: 2 } }, function (err, job) {
      should.not.exist(err);

      q.close();
      q.pop({ queue: "test1"}, function (err, job) {
        should.exist(err);
        should.equal(err.message, 'Redis not specified.');
        redisClient = redis.createClient(redisSettings.port, redisSettings.host);
        q.options.client = redisClient;
        next();
      });
    });
  });

  it("should push a job correctly", function (next) {
    this.timeout(0);
    q.push({ queue: "test1", data: { x: 1, y: 2 } }, function (err, job) {
      should.not.exist(err);

      q.pop({ queue: "test1" }, function (err, job) {
        if (err) return next(err);
        job.data.x.should.equal(1);
        job.data.y.should.equal(2);
        next();
      });
    });
  });

  it("should pushback a job correctly", function (next) {
    this.timeout(0);
    q.push({ queue: "test2", data: { x: 1, y: 2 }, ttr: 1000 }, function (err) {
      should.not.exist(err);

      q.pop({ queue: "test2" }, function (err, job) {
        should.not.exist(err);

        job.pushback(function (err, data) {
          should.not.exist(err);

          q.pop({ queue: "test2" }, function (err, job) {
            should.not.exist(err);
            job.data.x.should.be.equal(1);
            job.data.y.should.be.equal(2);
            next();
          });
        })
      });
    });
  });

  it("should check for unique tokens", function (next) {
    this.timeout(0);
    q.push({ queue: "test3", data: { x: 1, y: 2 }, unique: "x" }, function (err, job) {
      should.not.exist(err);
      should.exist(job);

      q.push({ queue: "test3", data: { x: 2, y: 3 }, unique: "x" }, function (err, job) {
        should.not.exist(err);
        should.not.exist(job);

        q.pop({ queue: "test3" }, function (err, job) {
          should.not.exist(err);

          job.remove(function (err) {
            should.not.exist(err);

            q.push({ queue: "test3", data: { x: 1, y: 2 }, unique: "x" }, function (err, job) {
              should.not.exist(err);
              should.exist(job);

              q.pop({ queue: "test3" }, function (err, job) {
                should.not.exist(err);
                job.remove(next);
              });
            });
          })
        });
      });
    });
  });

  it('should handle high priority option', function (next) {
    this.timeout(0);
    q.push({ queue: "prioritytest", data: { x: 1, y: 2 }, priority: 1 }, function (err, job) {
      should.not.exist(err);
      should.exist(job);

      q.push({ queue: "prioritytest", data: { x: 3, y: 4 }, priority: 1 }, function (err, job) {
        should.not.exist(err);


        q.pop({ queue: "prioritytest" }, function (err, job) {
          should.not.exist(err);
          job.data.x.should.equal(3);
          job.data.y.should.equal(4);

          q.pop({ queue: "prioritytest" }, function (err, job) {
            should.not.exist(err);
            job.data.x.should.equal(1);
            job.data.y.should.equal(2);
            next();
          });
        });
      });
    });
  });

  it('should handle low priority option', function (next) {
    this.timeout(0);
    q.push({ queue: "prioritytest", data: { x: 1, y: 2 }, priority: 0 }, function (err, job) {
      should.not.exist(err);
      should.exist(job);

      q.push({ queue: "prioritytest", data: { x: 3, y: 4 }, priority: 0 }, function (err, job) {
        should.not.exist(err);

        q.pop({ queue: "prioritytest" }, function (err, job) {
          should.not.exist(err);
          job.data.x.should.equal(1);
          job.data.y.should.equal(2);

          q.pop({ queue: "prioritytest" }, function (err, job) {
            should.not.exist(err);
            job.data.x.should.equal(3);
            job.data.y.should.equal(4);
            next();
          });
        });
      });
    });
  });

  it('should delay queue items', function (next) {
    this.timeout(0);
    q.push({ queue: "test4", data: { x: 1, y: 2 }, delay: 12 }, function (err, job) {
      should.not.exist(err);
      should.exist(job);

      q.push({ queue: "test4", data: { x: 3, y: 4 } }, function (err, job) {
        should.not.exist(err);
        should.exist(job);

        q.pop({ queue: "test4" }, function (err, job) {
          should.not.exist(err);
          should.exist(job);
          job.data.x.should.be.equal(3);
          job.data.y.should.be.equal(4);

          setTimeout(function () {
            q.pop({ queue: "test4" }, function (err, job) {
              should.not.exist(err);
              should.exist(job);
              job.data.x.should.be.equal(1);
              job.data.y.should.be.equal(2);
              next();
            });
          }, 13000);
        });
      });
    });
  });

  it('should clear the given queue', function (next) {
    this.timeout(0);
    q.push({ queue: "test4", data: { x: 1, y: 2 } }, function (err, job) {
      should.not.exist(err);
      should.exist(job);

      q.clear('test4', function (err) {
        should.not.exist(err);

        q.pop({ queue: "test4" }, function (err, job) {
          should.not.exist(err);
          should.not.exist(job);
          next();
        });
      });
    });
  });

  it('should clear all queues', function (next) {
    this.timeout(0);
    q.push({ queue: "test4", data: { x: 1, y: 2 } }, function (err, job) {
      should.not.exist(err);
      should.exist(job);

      q.push({ queue: "test5", data: { x: 1, y: 2 } }, function (err, job) {
        should.not.exist(err);
        should.exist(job);

        q.clear(function (err) {
          should.not.exist(err);

          q.pop({ queue: "test4" }, function (err, job) {
            should.not.exist(err);
            should.not.exist(job);

            q.pop({ queue: "test5" }, function (err, job) {
              should.not.exist(err);
              should.not.exist(job);
              next();
            });
          });
        });
      });
    });
  });

  it('should subscribe/unsubscribe to the given queue', function (next) {
    this.timeout(0);
    var sub1Id = q.poll.subscribe({
      channel: 'test4',
      type: q.poll.TYPE.INTERVAL,
      value: 3,
      handler: function (payload, next) { next(); },
      args: [should]
    });

    var sub2Id = q.poll.subscribe({
      channel: 'test5',
      type: q.poll.TYPE.LINEAR,
      value: 5,
      handler: function (payload, next) { next(); },
      args: [should]
    });

    var subscription = q.poll.get(sub1Id);
    should.exist(subscription);
    subscription.channel.should.be.equal('test4');
    subscription.type.should.be.equal(q.poll.TYPE.INTERVAL);
    subscription.value.should.be.equal(3);

    subscription = q.poll.get(sub2Id);
    should.exist(subscription);
    subscription.channel.should.be.equal('test5');
    subscription.type.should.be.equal(q.poll.TYPE.LINEAR);
    subscription.value.should.be.equal(5);

    // Unsubscription
    q.poll.unsubscribe(sub1Id);
    subscription = q.poll.get('test4', sub1Id);
    should.not.exist(subscription);

    q.poll.unsubscribe(sub2Id);
    subscription = q.poll.get('test5', sub2Id);
    should.not.exist(subscription);

    next();
  });

  it('should start the subscription to the given queue with interval algorithm', function (next) {
    this.timeout(0);
    var callCount = 0;
    var subscriptionId = null;

    var consumer = function (data, should, next) {
      callCount++;

      if (callCount === 1) {
        data.x.should.be.equal(1);
        data.y.should.be.equal(2);
      }

      if (callCount === 2) {
        data.x.should.be.equal(3);
        data.y.should.be.equal(4);
      }

      next();
    }

    var done = function () {
      q.poll.unsubscribe(subscriptionId);
      next();
    }

    subscriptionId = q.poll.subscribe({
      channel: 'test4',
      type: q.poll.TYPE.INTERVAL,
      value: 3,
      handler: consumer,
      done: done,
      args: [should]
    });

    q.push({ queue: "test4", data: { x: 1, y: 2 } }, function (err, job) {
      should.not.exist(err);
      should.exist(job);

      q.push({ queue: "test4", data: { x: 3, y: 4 } }, function (err, job) {
        should.not.exist(err);
        should.exist(job);

        q.poll.start(subscriptionId, function (err) {
          should.not.exist(err);
        });
      });
    });
  });

  it('should stop subscription to the given queue with interval algorithm', function (next) {
    this.timeout(0);
    var callCount = 0;
    var queueName = 'test5';

    var consumer = function (data, should, q, next) {
      callCount++;

      if (callCount === 1) {
        data.x.should.be.equal(1);
        data.y.should.be.equal(2);
        q.poll.stop(queueName);
      }

      if (callCount === 2) {
        data.x.should.be.equal(3);
        data.y.should.be.equal(4);
      }

      next();
    }

    var sId = q.poll.subscribe({
      channel: queueName,
      type: q.poll.TYPE.INTERVAL,
      value: 3,
      handler: consumer,
      args: [should, q]
    });

    q.push({ queue: queueName, data: { x: 1, y: 2 } }, function (err, job) {
      should.not.exist(err);
      should.exist(job);

      q.push({ queue: queueName, data: { x: 3, y: 4 } }, function (err, job) {
        should.not.exist(err);
        should.exist(job);

        q.poll.start(sId, function (err) {
          should.not.exist(err);
        });

        setTimeout(function () {
          callCount.should.be.equal(1);
          next();
        }, 5000);
      });
    });
  });

  it('should start the subscription to the given queue with linear algorithm', function (next) {
    this.timeout(0);
    var callCount = 0;
    var subscriptionId = null;

    var consumer = function (data, should, next) {
      callCount++;

      if (callCount === 1) {
        data.x.should.be.equal(1);
        data.y.should.be.equal(2);
      }

      if (callCount === 2) {
        data.x.should.be.equal(3);
        data.y.should.be.equal(4);
      }

      next();
    }

    var done = function () {
      q.poll.unsubscribe(subscriptionId);
      next();
    }

    subscriptionId = q.poll.subscribe({
      channel: 'test4',
      type: q.poll.TYPE.LINEAR,
      value: 3,
      handler: consumer,
      done: done,
      args: [should]
    });

    q.push({ queue: "test4", data: { x: 1, y: 2 } }, function (err, job) {
      should.not.exist(err);
      should.exist(job);

      q.push({ queue: "test4", data: { x: 3, y: 4 } }, function (err, job) {
        should.not.exist(err);
        should.exist(job);

        q.poll.start(subscriptionId, function (err) {
          should.not.exist(err);
        });
      });
    });
  });

  it('should stop subscription to the given queue with linear algorithm', function (next) {
    this.timeout(0);
    var callCount = 0;
    var queueName = 'test6';

    var consumer = function (data, should, q, next) {
      callCount++;

      if (callCount === 1) {
        data.x.should.be.equal(1);
        data.y.should.be.equal(2);
        q.poll.stop(queueName);
      }

      if (callCount === 2) {
        data.x.should.be.equal(3);
        data.y.should.be.equal(4);
      }

      next();
    }

    var sId = q.poll.subscribe({
      channel: queueName,
      type: q.poll.TYPE.LINEAR,
      value: 3,
      handler: consumer,
      args: [should, q]
    });

    q.push({ queue: queueName, data: { x: 1, y: 2 } }, function (err, job) {
      should.not.exist(err);
      should.exist(job);

      q.push({ queue: queueName, data: { x: 3, y: 4 } }, function (err, job) {
        should.not.exist(err);
        should.exist(job);

        q.poll.start(sId, function (err) {
          should.not.exist(err);
        });

        setTimeout(function () {
          callCount.should.be.equal(1);
          next();
        }, 5000);
      });
    });
  });

  it('should make subscription to the given queue with multiple consumer', function (next) {
    this.timeout(0);
    var queueName = 'test2';
    var sId1 = null;
    var sId2 = null;

    var consumer1 = function (data, should, q, next) {
      data.x.should.be.equal(1);
      data.y.should.be.equal(2);
      next();
    }

    var consumer2 = function (data, should, q, next) {
      data.x.should.be.equal(3);
      data.y.should.be.equal(4);
      next();
    }

    var done = function () {
      q.poll.unsubscribe(sId1);
      q.poll.unsubscribe(sId2);
      next();
    }

    sId1 = q.poll.subscribe({
      channel: queueName,
      type: q.poll.TYPE.INTERVAL,
      value: 3,
      handler: consumer1,
      done: done,
      args: [should, q]
    });

    sId2 = q.poll.subscribe({
      channel: queueName,
      type: q.poll.TYPE.INTERVAL,
      value: 4,
      handler: consumer2,
      done: done,
      args: [should, q]
    });

    q.push({ queue: queueName, data: { x: 1, y: 2 } }, function (err, job) {
      should.not.exist(err);
      should.exist(job);

      q.push({ queue: queueName, data: { x: 3, y: 4 } }, function (err, job) {
        should.not.exist(err);
        should.exist(job);

        q.poll.start(sId1, function (err) {
          should.not.exist(err);
        });

        q.poll.start(sId2, function (err) {
          should.not.exist(err);
        });
      });
    });
  });
});
