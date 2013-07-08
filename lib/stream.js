var Readable = require("stream").Readable
  , util = require("util")

function QueueStream(options) {
  Readable.call(this, { objectMode: true, highWaterMark: 0 });

  var self = this;

  self._parent = options.parent;
  self._queue = options.queue;
  self._client = self._parent.createClient();
  self._reading = false;
  self._pollDelay = options.pollDelay || 5000;

  self._client.on("message", function (chan, job) {
    self._pop();
  });
}

QueueStream.prototype = Object.create(Readable.prototype, { constructor: { value: QueueStream }});

QueueStream.prototype._read = function(n) {
  if (!this._client) return;

  if (!this._reading) {
    this._reading = true;
    this._client.subscribe(this._parent.getQueue(this._queue));
  }

  this._pop();
};

QueueStream.prototype._pop = function() {
  var self = this;
  if (!self._client || !self._reading) return;

  if (self._timer)
    clearTimeout(self._timer);

  self._parent.pop({
    queue: self._queue
  }, function (err, data) {
    if (err) return self.close();
    if (!data) return;

    if (!self.push(data)) {
      self._reading = false;
      self._client.unsubscribe(self._parent.getQueue(self._queue));
    } else {
      self._timer = setTimeout(function (data) {
        self._timer = null;
        self._pop();
      }, self._pollDelay);
    }
  });
}

QueueStream.prototype.close = function() {
  var self = this;
  if (!self._client) return;

  self._client.unsubscribe(self._parent.getQueue(self._queue));
  self._client.quit();
  self._client = null;

  if (self._timer)
    clearTimeout(self._timer);

  self.push(null);
}

module.exports = QueueStream;
