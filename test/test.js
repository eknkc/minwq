var mwq = require("../")
  , assert = require("assert")

describe("minwq", function() {
  var q = mwq();

  it("should push a job correctly", function (next) {
    q.push({ queue: "test1", data: { x: 1, y: 2 } }, function (err) {
      if (err) return next(err);

      q.pop({ queue: "test1" }, function (err, job) {
        if (err) return next(err);
        assert.equal(job.data.x, 1);
        assert.equal(job.data.y, 2);
        next();
      });
    });
  });

  it("should pushback a job correctly", function (next) {
    q.push({ queue: "test2", data: { x: 1, y: 2 }, ttr: 1000 }, function (err) {
      if (err) return next(err);

      q.pop({ queue: "test2" }, function (err, job) {
        if (err) return next(err);

        job.pushback(function (err, data) {
          if (err) return next(err);

          q.pop({ queue: "test2" }, function (err, job) {
            if (err) return next(err);
            assert.equal(job.data.x, 1);
            assert.equal(job.data.y, 2);
            next();
          });
        })
      });
    });
  });

  it("should check for unique tokens", function (next) {
    q.push({ queue: "test3", data: { x: 1, y: 2 }, unique: "x" }, function (err, job) {
      if (err) return next(err);
      assert.notEqual(job, null);

      q.push({ queue: "test3", data: { x: 2, y: 3 }, unique: "x" }, function (err, job) {
        if (err) return next(err);
        assert.equal(job, null);

        q.pop({ queue: "test3" }, function (err, job) {
          if (err) return next(err);

          job.remove(function (err) {
            if (err) return next(err);

            q.push({ queue: "test3", data: { x: 1, y: 2 }, unique: "x" }, function (err, job) {
              if (err) return next(err);
              assert.notEqual(job, null);

              q.pop({ queue: "test3" }, function (err, job) {
                if (err) return next(err);
                job.remove(next);
              });
            });
          })
        });
      });
    });
  });
});
