var q = require('q')
  , mongo = require('mongodb')
  , Channel = require('./channel')

function Client(namespace, id, db) {
  this.namespace = namespace;
  this.id = id;
  this.db = db;
  this.channels = {};
}
Client.prototype.ping = function(cb) {
  this.db.collection('clients').update({ _id: this.id }, { $set: { ping: new Date() }}, cb);
}
Client.prototype.channel = Client.prototype.join = function(name, lastMessage) {
  if (this.channels[name]) {
    return this.channels[name];
  } else {
    var channel = new Channel(name, this, lastMessage);
    this.channels[name] = channel;
    this.db.collection('clients').update({ _id: this.id }, { $set: { "channels." + channel.name: new Date() }});
    return channel;
  }
}
Client.prototype.leave = function(channel) {
  this.db.collection('clients').update({ _id: this.id }, { $unset: { "channels." + channel.name: true }});
  channel._close();
  delete this.channels[name];
}
Client.prototype.send = function(channel, type, data) {
  return exports.send(this.db, this.namespace, this.id, channel, type, data);
}
Client.prototype.sendns = function(namespace, channel, type, data) {
  return exports.send(this.db, namespace, this.id, channel, type, data);
}

var clients = {};
exports.create = function(db, meta) {
  var meta = meta || {};
  meta.ns = meta.ns || 'vubsub';
  var now = new Date();
  var data = { ts: now, ping: now, meta: meta };
  var deferred = q.defer();
  
  db.collection('clients').insert(data, function(err, data) {
    if (err || !data || !data[0] || !data[0]._id) {
      deferred.reject(err);
    } else {
      var client = new Client(meta.ns, data[0]._id, db);
      clients[client.id] = client;
      deferred.resolve(client);
    }
  });
  return deferred.promise;
}
exports.get = function(id) {
  return q.fcall(function() {
    if (clients[id]) {
      return clients[id];
    } else {
      throw new Error('Unknown client');
    }   
  });
}
exports.send = function(db, namespace, client, channel, type, data) {
  return Channel.send(db.collection('ns_' + namespace), client, channel, type, data);
}

