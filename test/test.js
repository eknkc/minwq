var mwq = require("../")
  , should = require('should')
  , redis = require('redis')
  , stream = require("stream")

describe("minwq", function() {
  var q = new mwq({
    prefix: "myapp"
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
      if (err) throw err;

      q.push({ queue: "test3", data: { x: 2, y: 3 }, unique: "x" }, function (err, job) {
        if (err) throw err;

        q.pop({ queue: "test3" }, function (err, job) {
          if (err) throw err;

          job.remove(function (err) {
            if (err) throw err;

            q.pop({ queue: "test3" }, function (err, job) {
              if (err) throw err;
              should.not.exist(job);
              next();
            });
          })
        });
      });
    });
  });

  it('should delay queue items', function (next) {
    this.timeout(0);

    q.push({ queue: "test4", data: { x: 1, y: 2 }, delay: 200 }, function (err, job) {
      if (err) throw err;
      should.exist(job);

      q.push({ queue: "test4", data: { x: 3, y: 4 } }, function (err, job) {
        if (err) throw err;
        should.exist(job);

        q.pop({ queue: "test4" }, function (err, job) {
          if (err) throw err;
          should.exist(job);
          job.data.x.should.be.equal(3);
          job.data.y.should.be.equal(4);

          q.pop({ queue: "test4" }, function (err, job) {
            if (err) throw err;
            should.not.exist(job);
          });

          setTimeout(function () {
            q.pop({ queue: "test4" }, function (err, job) {
              if (err) throw err;
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

  it("should not replace unique items", function (next) {
    q.push({ queue: 'testunique1', data: { x: 1 }, unique: 'xx' }, function (err, data) {
      if (err) throw err;

      q.push({ queue: 'testunique1', data: { x: 2 }, unique: 'xx' }, function (err, data) {
        if (err) throw err;

        q.pop({ queue: "testunique1" }, function (err, job) {
          if (err) throw err;
          job.data.x.should.be.equal(1);

          q.pop({ queue: "testunique1" }, function (err, job) {
            if (err) throw err;
            should.not.exist(job);
            next();
          });
        });
      });
    });
  });

  it("should replace unique items when preferred", function (next) {
    q.push({ queue: 'testunique2', data: { x: 1 }, unique: 'xx' }, function (err, data) {
      if (err) throw err;

      q.push({ queue: 'testunique2', data: { x: 2 }, unique: 'xx', replace: true }, function (err, data) {
        if (err) throw err;

        q.pop({ queue: "testunique2" }, function (err, job) {
          if (err) throw err;
          job.data.x.should.be.equal(2);

          q.pop({ queue: "testunique2" }, function (err, job) {
            if (err) throw err;
            should.not.exist(job);
            next();
          });
        });
      });
    });
  });
});
