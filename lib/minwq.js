var redis = require("redis")
  , fs = require("fs")
  , path = require("path")
  , crypto = require("crypto")
  , Poll = require('./poll');

var Queue = function (options) {
  this.options = options || {};
  this.options.prefix = options.prefix || 'def';
  this.options.wqPrefix = 'mwq';
  this.options.client = options.client || redis.createClient();
  this.scripts = {}
  this.poll = new Poll(this);
}

Queue.prototype.execScript = function (name, keys, args, next) {
  next = next || function(){};
  var self = this;

  if (! this.scripts[name]) {
    return fs.readFile(path.resolve(__dirname, "../redis/" + name + ".lua"), "utf8", function (err, contents) {
      if (err) return next(err);

      self.scripts[name] = {
        data: contents,
        hash: crypto.createHash('sha1').update(contents, "utf8").digest('hex')
      };

      self.execScript(name, keys, args, next);
    });
  }

  this.options.client.evalsha([this.scripts[name].hash, keys.length].concat(keys).concat(args), function (err, result) {
    if (err && /^NOSCRIPT/.test(err.message)) {
      return this.options.client.script('load', self.scripts[name].data, function (err) {
        if (err) return next(err);
        self.execScript(name, keys, args, next);
      });
    }

    next(err, result);
  });
}

Queue.prototype.push = function(opt, next) {
  if (! opt.queue)
    return process.nextTick(function() { next(new Error("Queue not specified.")); });
  if (this.options.client === null)
    return process.nextTick(function() { next(new Error("Redis not specified.")); });

  opt.ttr = opt.ttr || 0;
  opt.delay = opt.delay || 0;
  opt.unique = opt.unique || "";
  opt.priority = opt.priority || 0;

  this.execScript('push', [[this.options.wqPrefix, this.options.prefix, opt.queue].join(':')], [JSON.stringify(opt.data), opt.unique, opt.priority, opt.delay, opt.ttr], next);
}

Queue.prototype.pop = function(opt, next) {
  if (!opt.queue)
    return process.nextTick(function() { next(new Error("Queue not specified.")); });
  if (this.options.client === null)
    return process.nextTick(function() { next(new Error("Redis not specified.")); });

  var queue = [this.options.wqPrefix, this.options.prefix, opt.queue].join(':');
  var self = this;

  this.execScript('pop', [queue], [], function (err, data) {
    if (err) return next(err);
    if (!data) return next();

    next(null, {
      data: JSON.parse(data[1]),
      remove: function(next) {
        if (!data[2]) return process.nextTick(next || function() { });
        self.execScript('rem', [queue, data[0]], [], next);
      },
      pushback: function(next) {
        if (!data[2]) return process.nextTick(next || function() { });
        self.execScript('pushback', [queue, data[0]], [], next);
      }
    });
  });
}

Queue.prototype.close = function () {
  if (this.options.client === null)
    return process.nextTick(function() { next(new Error("Redis not specified.")); });

  this.options.client.quit();
  this.options.client = null;
}

Queue.prototype.clear = function (queue, next) {
  if (this.options.client === null)
    return process.nextTick(function() { next(new Error("Redis not specified.")); });

  var keys = [this.options.wqPrefix, this.options.prefix];
  if (next)
    keys.push(queue);

  this.execScript('clear', keys, [], next || queue);
}

module.exports = Queue;
