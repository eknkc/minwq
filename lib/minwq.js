var redis = require("redis")
  , fs = require("fs")
  , path = require("path")
  , crypto = require("crypto")

module.exports = function(options) {
  options = options || {};
  options.prefix = options.prefix || 'def';
  options.wqPrefix = 'mwq';
  options.client = options.client || redis.createClient();
  var scripts = {}

  function execScript(name, keys, args, next) {
    next = next || function(){};

    if (!scripts[name]) {
      return fs.readFile(path.resolve(__dirname, "../redis/" + name + ".lua"), "utf8", function (err, contents) {
        if (err) return next(err);

        scripts[name] = {
          data: contents,
          hash: crypto.createHash('sha1').update(contents, "utf8").digest('hex')
        };

        execScript(name, keys, args, next);
      });
    }

    options.client.evalsha([scripts[name].hash, keys.length].concat(keys).concat(args), function (err, result) {
      if (err && /^NOSCRIPT/.test(err.message)) {
        return options.client.script('load', scripts[name].data, function (err) {
          if (err) return next(err);
          execScript(name, keys, args, next);
        });
      }

      next(err, result);
    });
  }

  return {
    options: function (key, value) {
      if (arguments.length === 0)
        return options;

      if (arguments.length === 1)
        return options[key];

      options[key] = value;
    },
    push: function(opt, next) {
      if (!opt.queue)
        return process.nextTick(function() { next(new Error("Queue not specified.")); });
      if (options.client === null)
        return process.nextTick(function() { next(new Error("Redis not specified.")); });

      opt.ttr = opt.ttr || 0;
      opt.delay = opt.delay || 0;
      opt.unique = opt.unique || "";
      opt.priority = opt.priority || 0;

      execScript('push', [[options.wqPrefix, options.prefix, opt.queue].join(':')], [JSON.stringify(opt.data), opt.unique, opt.priority, opt.delay, opt.ttr], next);
    },
    pop: function(opt, next) {
      if (!opt.queue)
        return process.nextTick(function() { next(new Error("Queue not specified.")); });
      if (options.client === null)
        return process.nextTick(function() { next(new Error("Redis not specified.")); });

      var queue = [options.wqPrefix, options.prefix, opt.queue].join(':');

      execScript('pop', [queue], [], function (err, data) {
        if (err) return next(err);
        if (!data) return next();

        next(null, {
          data: JSON.parse(data[1]),
          remove: function(next) {
            if (!data[2]) return process.nextTick(next || function() { });
            execScript('rem', [queue, data[0]], [], next);
          },
          pushback: function(next) {
            if (!data[2]) return process.nextTick(next || function() { });
            execScript('pushback', [queue, data[0]], [], next);
          }
        });
      });
    },
    close: function () {
      if (options.client === null)
        return process.nextTick(function() { next(new Error("Redis not specified.")); });

      options.client.quit();
      options.client = null;
    },
    clear: function (queue, next) {
      if (options.client === null)
        return process.nextTick(function() { next(new Error("Redis not specified.")); });

      var keys = [options.wqPrefix, options.prefix];
      if (next)
        keys.push(queue);

      execScript('clear', keys, [], next || queue);
    }
  };
};
