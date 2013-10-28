module.exports = require("./lib/minwq.js");

var r = new (module.exports)();

r.pop({
  queue: "anan",
  data: { a: 1 },
  ttl: 10000
}, function (err, job) {
  if (err) return next(err);
  if (job) {
    r.remove({
      queue: "anan",
      id: job.id
    }, console.log)
  }
})
