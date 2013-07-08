var uuid = require('node-uuid');

var Poll = function (queue) {
  this.queue = queue;
  this.subscriptions = {};
  this.TYPE = {
    INTERVAL: 1,
    LINEAR: 2,
    ADAPTIVE: 3
  }
}

Poll.prototype.get = function (subscriptionId) {
  return this.subscriptions[subscriptionId];
}

Poll.prototype.channelSubscriptions = function (channel) {
  return Object.keys(this.subscriptions).filter(function (id) {
    return (this.subscriptions[id].channel === channel);
  });
}

Poll.prototype.subscribe = function (options) {
  options.id = uuid.v4();
  this.subscriptions[options.id] = options;
  return options.id;
}

Poll.prototype.unsubscribe = function (subscriptionId) {
  if (! this.subscriptions[subscriptionId])
    return new Error('Invalid Subscription');

  var subscription = this.subscriptions[subscriptionId];
  this.stop(subscriptionId);
  delete this.subscriptions[subscriptionId];
}

Poll.prototype.start = function (subscriptionId, next) {
  if (! this.subscriptions[subscriptionId])
    return new Error('Invalid Subscription');

  var self = this;
  var subscription = this.subscriptions[subscriptionId];
  subscription.handler = subscription.handler || function () {
    arguments.slice(-1).apply();
  };
  subscription.done = subscription.done || function () {};

  if (subscription.type === self.TYPE.INTERVAL) {
    subscription.timer = setInterval(function () {
      self.queue.pop({ queue: subscription.channel }, function (err, job) {
        if (err) return next(err);
        if (! job) return subscription.done.apply(subscription.bind || null);

        subscription.handler.apply(subscription.bind || null, [job.data].concat(subscription.args || []).concat([function (err) {
          job.remove(function (err) {});
        }]));
      });
    }, subscription.value * 1000);
  }

  if (subscription.type === self.TYPE.LINEAR) {
    var runner = function () {
      subscription.timer = setTimeout(function () {
        self.queue.pop({ queue: subscription.channel }, function (err, job) {
          if (err) return next(err);
          if (! job) return subscription.done.apply(subscription.bind || null);

          subscription.handler.apply(subscription.bind || null, [job.data].concat(subscription.args || []).concat([function (err) {
            job.remove(function (err) {});
            runner();
          }]));
        });
      }, subscription.value * 1000);
    }

    runner();
  }

  if (subscription.type === self.TYPE.ADAPTIVE) {
    //@TODO: Not Implemented
  }
};

Poll.prototype.stop = function (subscriptionId) {
  if (! this.subscriptions[subscriptionId])
    return new Error('Invalid Subscription');

  var subscription = this.subscriptions[subscriptionId];
  if (! subscription.timer) return;
  clearInterval(subscription.timer);
};

module.exports = Poll;
