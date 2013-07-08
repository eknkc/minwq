var Readable = require("stream").Readable
  , util = require("util")

function QueueStream(options) {
  Readable.call(this, { objectMode: true });

  var self = this;

  self._parent = options.parent;
  self._queue = options.queue;
  self._client = self._parent.createClient();
  self._timer = setInterval(function() { self._pop() }, 5000);
  self._reading = false;

  self._client.on("message", function (chan, job) {
    self._pop();
  });
}

QueueStream.prototype = Object.create(Readable.prototype, { constructor: { value: QueueStream }});

QueueStream.prototype._read = function(n) {
  var self = this;
  if (!self._client) return;

  if (!self._reading) {
    self._reading = true;
    self._client.subscribe(self._parent.getQueue(self._queue));
  }

  self._pop();
};

QueueStream.prototype._pop = function() {
  var self = this;
  if (!self._client) return;

  self._parent.pop({
    queue: self._queue
  }, function (err, data) {
    if (err) return self.close();
    if (!data) return;

    if (!self.push(data)) {
      self._reading = false;
      self._client.unsubscribe(self._parent.getQueue(self._queue));
    }
  });
}

QueueStream.prototype.close = function() {
  var self = this;
  if (!self._client) return;

  self._client.unsubscribe(self._parent.getQueue(self._queue));
  self._client.quit();
  self._client = null;
  clearInterval(self._timer);

  self.push(null);
}

module.exports = QueueStream;
