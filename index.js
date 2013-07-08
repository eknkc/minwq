module.exports = require("./lib/minwq.js");

var q = module.exports();

q.push({ queue: "deneme", data: 13 }, function (err, data) {
  if (err) return next(err);
  q.pop({ queue: "deneme" }, function (err, data) {
    if (err) return next(err);
  });
});
