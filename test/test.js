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
  var q = mwq({
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
    q.options('prefix').should.be.equal('myapp');
    next();
  });

  it('should close the redis connection safely', function (next) {
    q.close();
    should.not.exist(q.options('client'));
    redisClient = redis.createClient(redisSettings.port, redisSettings.host);
    q.options('client', redisClient);
    next();
  });

  it('should not push without redis connection', function (next) {
    q.close();
    q.push({ queue: "test1", data: { x: 1, y: 2 } }, function (err, job) {
      should.exist(err);
      should.not.exist(job);
      should.equal(err.message, 'Redis not specified.');
      redisClient = redis.createClient(redisSettings.port, redisSettings.host);
      q.options('client', redisClient);
      next();
    });
  });

  it('should not pop without redis connection', function (next) {
    q.push({ queue: "test1", data: { x: 1, y: 2 } }, function (err, job) {
      should.not.exist(err);

      q.close();
      q.pop({ queue: "test1"}, function (err, job) {
        should.exist(err);
        should.equal(err.message, 'Redis not specified.');
        redisClient = redis.createClient(redisSettings.port, redisSettings.host);
        q.options('client', redisClient);
        next();
      });
    });
  });

  it("should push a job correctly", function (next) {
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
});
