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
Client.prototype.channel = function(name, lastMessage) {
  if (this.channels[name]) {
    return this.channels[name];
  } else {
    var channel = new Channel(name, this, lastMessage);
    this.channels[name] = channel;
    return channel;
  }
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
