var q = require('q')
  , mongo = require('mongodb')
  , Channel = require('./channel')
  , debug = require('debug')('vubsub')

function Client(namespace, id, db) {
  this.namespace = namespace;
  this.id = id;
  this.db = db;
  this.channels = {};
}
Client.prototype.ping = function(cb) {
  return q.ninvoke(this.db.collection('clients'), 'update', { _id: this.id }, { $set: { ping: new Date() }}).nodeify(cb);
}
Client.prototype.channel = Client.prototype.join = function(name, lastMessage) {
  if (this.channels[name]) {
    return this.channels[name];
  } else {
    var channel = new Channel(name, this, lastMessage);
    this.channels[name] = channel;
    return channel;
  }
}
Client.prototype.leave = function(channel, cb) {
  delete this.channels[channel.name];
  return channel._close(cb);
}
Client.prototype.send = function(channel, type, data) {
  return exports.send(this.db, this.namespace, this.id, channel, type, data);
}
Client.prototype.sendns = function(namespace, channel, type, data) {
  return exports.send(this.db, namespace, this.id, channel, type, data);
}

var clients = {};
exports.create = function(db, meta, cb) {
  meta = meta || {};
  meta.ns = meta.ns || 'vubsub';
  var now = new Date();
  var data = { ts: now, ping: now, meta: meta };
  var deferred = q.defer();
  
  db.collection('clients').insertOne(data, {w:1}, function(err, result) {
    if (err) {
      debug('Did not get client!', err)
      deferred.reject(err);
    } {
      if (!result.insertedCount) {
        debug('Did not get client!', data)
        deferred.reject('no-data');
      } else {
        debug('Got client!')
        var client = new Client(meta.ns, result.insertedId, db);
        clients[client.id] = client;
        Channel.collection(db, meta.ns).then(function() {
          deferred.resolve(client);
        }, function(err) {
          deferred.reject(err);
        });
      }
      }
  });
  return deferred.promise.nodeify(cb);
}
exports.get = function(id, cb) {
  return q.fcall(function() {
    if (clients[id]) {
      return clients[id];
    } else {
      throw new Error('Unknown client');
    }   
  }).nodeify(cb);
}
exports.send = Channel.send;
exports.find = Channel.find;
exports.count = Channel.count;

