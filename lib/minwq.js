var redis = require("redis")
  , fs = require("fs")
  , path = require("path")
  , crypto = require("crypto")
  , async = require("async")
  , QueueStream = require("./stream.js")
  , Writable = require("stream").Writable
  , EventEmitter = require('events').EventEmitter

var Queue = function (options) {
  if (!(this instanceof Queue))
    return new Queue(options);

  EventEmitter.call(this);

  var self = this;

  self.options = options || {};
  self.options.prefix = self.options.prefix || 'def';
  self.options.globalPrefix = 'mwq';

  self.scripts = {}
  self.client = self.createClient();

  self.client.on('error', function () { self.emit('disconnect'); });
  self.client.on('connect', function () { self.emit('connect'); });
}

Queue.prototype = Object.create(EventEmitter.prototype, { constructor: { value: Queue }});

Queue.prototype.createClient = function() {
  return redis.createClient();
}

Queue.prototype.getQueue = function() {
  return [this.options.globalPrefix, this.options.prefix].concat([].slice.call(arguments)).join(':');
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

  if (this.client === null)
    return process.nextTick(function() { next(new Error("Redis not specified.")); });

  if (opt.unique)
    opt.unique = crypto.createHash('sha1').update(opt.unique, "utf8").digest('hex');

  opt.id = crypto.randomBytes(12).toString('hex');
  opt.data = JSON.stringify(opt.data);

  this.execScript('push', [this.getQueue(opt.queue)], [JSON.stringify(opt)], next);
}

Queue.prototype.pop = function(opt, next) {
  if (!opt.queue)
    return process.nextTick(function() { next(new Error("Queue not specified.")); });

  if (this.client === null)
    return process.nextTick(function() { next(new Error("Redis not specified.")); });

  var self = this;

  self.execScript('pop', [self.getQueue(opt.queue)], [], function (err, job) {
    if (err) return next(err);
    if (!job) return next();

    job = JSON.parse(job);

    next(null, {
      data: JSON.parse(job.data),
      remove: function(next) {
        self.remove(job, next);
      }
    });
  });
}

Queue.prototype.remove = function(opt, next) {
  if (!opt.queue)
    return process.nextTick(function() { next(new Error("Queue not specified.")); });

  if (this.client === null)
    return process.nextTick(function() { next(new Error("Redis not specified.")); });

  if (!opt.id && !opt.unique)
    return process.nextTick(function() { next(new Error("Provide an id or unique token.")); });

  if (opt.unique)
    opt.unique = crypto.createHash('sha1').update(opt.unique, "utf8").digest('hex');

  this.execScript('rem', [this.getQueue(opt.queue)], [JSON.stringify(opt)], next);
}

Queue.prototype.stream = function(opt) {
  return new QueueStream({
    parent: this,
    queue: opt.queue
  });
};

Queue.prototype.handler = function(handler) {
  var target = new Writable({ objectMode: true, highWaterMark: 0 });

  target._write = function(job, _, next) {
    handler(job, next);
  };

  return target;
}

Queue.prototype.close = function () {
  if (this.client === null)
    return;

  this.client.quit();
  this.client = null;
}

Queue.prototype.clear = function (queue, next) {
  if (this.client === null)
    return process.nextTick(function() { next(new Error("Redis not specified.")); });

  if (typeof queue == 'function') {
    next = queue;
    queue = null;
  }

  var client = this.client;

  client.keys(this.getQueue('*'), function (err, data) {
    if (err) return next(err);
    async.each(data, function (entry, next) {
      client.del(entry, next);
    }, next);
  });
}

module.exports = Queue;
