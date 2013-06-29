var redis = require("redis")
  , fs = require("fs")
  , path = require("path")
  , crypto = require("crypto")

module.exports = function(options) {
  options = options || {};
  options.prefix = options.prefix || '';

  var client = options.client || redis.createClient()
    , scripts = {}

  function execScript(name, data, next) {
    next = next || function(){};

    if (!scripts[name]) {
      return fs.readFile(path.resolve(__dirname, "../redis/" + name + ".lua"), "utf8", function (err, contents) {
        if (err) return next(err);

        scripts[name] = {
          data: contents,
          hash: crypto.createHash('sha1').update(contents, "utf8").digest('hex')
        };

        execScript(name, data, next);
      });
    }

    client.evalsha([scripts[name].hash, data.length].concat(data), function (err, result) {
      if (err && /^NOSCRIPT/.test(err.message)) {
        return client.script('load', scripts[name].data, function (err) {
          if (err) return next(err);
          execScript(name, data, next);
        });
      }

      next(err, result);
    });
  }

  return {
    push: function(opt, next) {
      if (!opt.queue)
        return process.nextTick(function() { next(new Error("Queue not specified.")); });

      if (!opt.ttr)
        opt.ttr = 0;

      execScript('push', [options.prefix + opt.queue, JSON.stringify(opt.data), opt.ttr], next);
    },
    pop: function(opt, next) {
      if (!opt.queue)
        return process.nextTick(function() { next(new Error("Queue not specified.")); });

      var queue = options.prefix + opt.queue;

      execScript('pop', [queue], function (err, data) {
        if (err) return next(err);
        if (!data) return next();

        next(null, {
          data: JSON.parse(data[1]),
          remove: function(next) {
            if (!data[2]) return process.nextTick(next || function() { });
            execScript('rem', [queue, data[0]], next);
          },
          pushback: function(next) {
            if (!data[2]) return process.nextTick(next || function() { });
            execScript('pushback', [queue, data[0]], next);
          }
        });
      });
    }
  };
};
