var redis = require("redis")
  , fs = require("fs")
  , path = require("path")
  , crypto = require("crypto")
  , async = require("async")
  , msgpack = require("msgpack-js")

var Queue = function (options) {
  if (!(this instanceof Queue))
    return new Queue(options);

  var self = this;

  self.options = options || {};
  self.options.prefix = self.options.prefix || 'minwq';

  self.scripts = {}
  self.client = self.createClient();
}

Queue.prototype.createClient = function() {
  return redis.createClient();
}

Queue.prototype.getKey = function() {
  return [this.options.prefix].concat([].slice.call(arguments)).join(':');
}

Queue.prototype.execScript = function (name, keys, args, next) {
  next = next || function(){};
  var self = this;

  if (!this.scripts[name]) {
    return fs.readFile(path.resolve(__dirname, "../redis/" + name + ".lua"), "utf8", function (err, contents) {
      if (err) return next(err);

      self.scripts[name] = {
        data: contents,
        hash: crypto.createHash('sha1').update(contents, "utf8").digest('hex')
      };

      self.execScript(name, keys, args, next);
    });
  }

  var meta = {
    date: Date.now()
  };

  this.client.evalsha([this.scripts[name].hash, keys.length].concat(keys).concat(args).concat([JSON.stringify(meta)]), function (err, result) {
    if (err && /^NOSCRIPT/.test(err.message)) {
      return self.client.script('load', self.scripts[name].data, function (err) {
        if (err) return next(err);
        self.execScript(name, keys, args, next);
      });
    }

    next(err, result);
  });
}

Queue.prototype.push = function(opt, next) {
  if (!opt.queue)
    return process.nextTick(function() { next(new Error("Queue not specified.")); });

  if (!this.client)
    return process.nextTick(function() { next(new Error("Redis not specified.")); });

  var job = {
    data: msgpack.encode(opt.data).toString('base64')
  };

  if (opt.unique) {
    job.id = crypto.createHash('sha1').update(opt.unique, "utf8").digest('hex');

    if (!!opt.replace)
      job.replace = true;
  } else {
    job.id = crypto.randomBytes(24).toString('hex');
  }

  if (opt.delay)
    job.delay = opt.delay;

  this.execScript('push', [this.getKey(opt.queue, "set"), this.getKey(opt.queue, "hash")], [JSON.stringify(job)], next);
}

Queue.prototype.pop = function(opt, next) {
  if (!opt.queue)
    return process.nextTick(function() { next(new Error("Queue not specified.")); });

  if (!this.client)
    return process.nextTick(function() { next(new Error("Redis not specified.")); });

  var settings = {};

  if (opt.ttl)
    settings.ttl = opt.ttl;

  var self = this;

  self.execScript('pop', [this.getKey(opt.queue, "set"), this.getKey(opt.queue, "hash")], [JSON.stringify(settings)], function (err, job) {
    if (err) return next(err);
    if (!job) return next();

    next(null, {
      id: job[0],
      data: msgpack.decode(new Buffer(job[1], 'base64')),
      remove: function(next) { self.remove({ queue: opt.queue, id: job[0] }, next) }
    });
  });
}

Queue.prototype.remove = function(opt, next) {
  if (!opt.queue)
    return process.nextTick(function() { next(new Error("Queue not specified.")); });

  if (this.client === null)
    return process.nextTick(function() { next(new Error("Redis not specified.")); });

  if (!opt.id)
    return process.nextTick(function() { next(new Error("Provide an id.")); });

  this.execScript('rem', [this.getKey(opt.queue, "set"), this.getKey(opt.queue, "hash")], [JSON.stringify({ id: opt.id })], next);
}

Queue.prototype.close = function () {
  if (this.client === null)
    return;

  this.client.quit();
  this.client = null;
}

module.exports = Queue;
