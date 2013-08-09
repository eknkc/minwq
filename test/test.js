var mwq = require("../")
  , should = require('should')
  , redis = require('redis')
  , async = require('async')
  , stream = require("stream")

describe("minwq", function() {
  var q = new mwq({
    prefix: "myapp"
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
    q.push({ queue: "prioritytest", data: { x: 1, y: 2 }, priority: 0 }, function (err, job) {
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
    q.push({ queue: "prioritytest", data: { x: 1, y: 2 }, priority: 1 }, function (err, job) {
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
    q.push({ queue: "test4", data: { x: 1, y: 2 }, delay: 200 }, function (err, job) {
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
              should.not.exist(job);
            });
          }, 0);

          setTimeout(function () {
            q.pop({ queue: "test4" }, function (err, job) {
              should.not.exist(err);
              should.exist(job);
              job.data.x.should.be.equal(1);
              job.data.y.should.be.equal(2);
              next();
            });
          }, 250);
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

  it("should open a stream", function(next) {
    var st = q.stream({ queue: "test-stream" });
    var j = 0;

    var dest = new stream.Writable({ objectMode: true });

    dest._write = function(job, _, cont) {
      job.remove(function (err, data) {
        if (err) return next(err);
        job.data.should.be.equal(j);
        j++;
        cont();

        if (j == 3)
          next();
      });
    }

    st.pipe(dest);

    q.push({queue: 'test-stream', data: 0}, function (err, data) {
      should.not.exist(err);
      q.push({queue: 'test-stream', data: 1}, function (err, data) {
        should.not.exist(err);
        q.push({queue: 'test-stream', data: 2}, function (err, data) {
          should.not.exist(err);
        })
      })
    })
  });

  it("should create a handler", function(next) {
    var st = q.stream({ queue: "test-stream-handlers" });
    var j = 0;

    st.pipe(q.handler(function(job, cont) {
      job.remove(function (err, data) {
        if (err) return next(err);
        job.data.should.be.equal(j);
        j++;
        cont();

        if (j == 3)
          next();
      });
    }));

    q.push({queue: 'test-stream-handlers', data: 0}, function (err, data) {
      should.not.exist(err);
      q.push({queue: 'test-stream-handlers', data: 1}, function (err, data) {
        should.not.exist(err);
        q.push({queue: 'test-stream-handlers', data: 2}, function (err, data) {
          should.not.exist(err);
        })
      })
    })
  });
});
