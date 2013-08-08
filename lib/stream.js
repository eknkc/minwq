var Readable = require("stream").Readable
  , util = require("util")

function QueueStream(options) {
  Readable.call(this, { objectMode: true, highWaterMark: 0 });

  var self = this;

  self._parent = options.parent;
  self._queue = options.queue;
  self._client = self._parent.createClient();
  self._subscribed = false;
  self._reading = false;
  self._pollDelay = options.pollDelay || 2000;
  self._lastPoll = Date.now();

  self._client.on("message", function (chan, job) {
    self._pop();
  });

  self._client.on('error', function () {
    self._reading = false;
  });

  self._timer = setInterval(function () {
    if (self._lastPoll + self._pollDelay > Date.now())
      return;

    self._pop();
  }, 500);
}

QueueStream.prototype = Object.create(Readable.prototype, { constructor: { value: QueueStream }});

QueueStream.prototype._read = function(n) {
  var self = this;

  if (!self._client)
    return;

  if (!self._reading) {
    self._reading = true;
    self._pop();
  }

  if (!self._subscribed) {
    self._subscribed = true;

    self._client.subscribe(self._parent.getQueue(self._queue), function (err) {
      if (err) {
        self._subscribed = false;
        return self.emit("error", err);
      }
    });
  }
};

QueueStream.prototype._pop = function() {
  var self = this;

  if (!self._client || !self._reading)
    return;

  self._lastPoll = Date.now();

  self._parent.pop({
    queue: self._queue
  }, function (err, data) {
    if (err)  {
      self._reading = false;
      return self.emit("error", err);
    }

    if (!data)
      return;

    if (!self.push(data))
      self._reading = false;
    else
      self._pop();
  });
}

QueueStream.prototype.close = function() { console.log("CLOSE");
  var self = this;

  if (!self._client)
    return;

  if (self._subscribed)
    self._client.unsubscribe(self._parent.getQueue(self._queue));

  self._client.quit();
  self._client = null;

  if (self._timer)
    clearInterval(self._timer);

  self.push(null);
}

module.exports = QueueStream;
