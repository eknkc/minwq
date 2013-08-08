var redis = require("redis")
  , fs = require("fs")
  , path = require("path")
  , crypto = require("crypto")
  , async = require("async")
  , QueueStream = require("./stream.js")
  , EventEmitter = require('events').EventEmitter

var Queue = function (options) {
  if (!(this instanceof Queue))
    return new Queue(options);

  EventEmitter.call(this);

  var self = this;

  self.options = options || {};
  self.options.prefix = self.options.prefix || 'def';
  self.options.wqPrefix = 'mwq';
  self.options.client = self.createClient();
  self.scripts = {}

  self.options.client.on('error', function () { self.emit('disconnect'); });
  self.options.client.on('connect', function () { self.emit('connect'); });
}

Queue.prototype = Object.create(EventEmitter.prototype, { constructor: { value: Queue }});

Queue.prototype.createClient = function() {
  return redis.createClient();
}

Queue.prototype.getQueue = function() {
  return [this.options.wqPrefix, this.options.prefix].concat([].slice.call(arguments)).join(':');
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

  this.options.client.evalsha([this.scripts[name].hash, keys.length].concat(keys).concat(args).concat([JSON.stringify(meta)]), function (err, result) {
    if (err && /^NOSCRIPT/.test(err.message)) {
      return self.options.client.script('load', self.scripts[name].data, function (err) {
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

  if (this.options.client === null)
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

  if (this.options.client === null)
    return process.nextTick(function() { next(new Error("Redis not specified.")); });

  var self = this;

  self.execScript('pop', [self.getQueue(opt.queue)], [], function (err, data) {
    if (err) return next(err);
    if (!data) return next();

    data = JSON.parse(data);

    next(null, {
      data: JSON.parse(data.data),
      remove: function(next) {
        self.execScript('rem', [self.getQueue(opt.queue)], [JSON.stringify({ id: data.id })], next);
      }
    });
  });
}

Queue.prototype.stream = function(opt) {
  return new QueueStream({
    parent: this,
    queue: opt.queue
  });
};

Queue.prototype.close = function () {
  if (this.options.client === null)
    return;

  this.options.client.quit();
  this.options.client = null;
}

Queue.prototype.clear = function (queue, next) {
  if (this.options.client === null)
    return process.nextTick(function() { next(new Error("Redis not specified.")); });

  if (typeof queue == 'function') {
    next = queue;
    queue = null;
  }

  var keys = [this.options.wqPrefix, this.options.prefix];
  if (queue) keys.push(queue);
  keys = keys.join(':') + "*";

  var client = this.options.client;

  client.keys(keys, function (err, data) {
    if (err) return next(err);
    async.each(data, function (entry, next) {
      client.del(entry, next);
    }, next);
  });
}

module.exports = Queue;
