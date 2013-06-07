var util = require('util')
  , EventEmitter = require('events').EventEmitter
  , q = require('q')
  , mongo = require('mongodb')

function Channel(name, client, lastMessage) {
  var self = this;
  self.name = name;
  self.client = client;
  self.db = client.db;
  
  self.collection = q.ninvoke(self.db, 'createCollection', 'ns_' + client.namespace, { size: Channel.COLLECTION_SIZE, capped: true, autoIndexId: true }).then(function(collection) {
    var cursor = collection.find().sort({ $natural: -1 }).limit(1);
    return q.ninvoke(cursor, 'nextObject').then(function(doc) {
      if (doc) {
        return doc;
      } else {
        var deferred = q.defer();
        collection.insert({ dummy: true }, { safe: true }, function(err, docs) {  
          if (err || !docs || !docs.length) {
            deferred.reject('failed to insert the first doc');
          } else {
            deferred.resolve(docs[0]);
          }
        });
        return deferred.promise;
      }
    }).then(function(doc) {
      // if we've come here, then:
      // - the collection exists
      // - it had at least a document
      // - and we know the _id to the most recent one
      var query = {
        _id: { $gt: (lastMessage ? mongo.ObjectID(lastMessage) : doc._id) },
        channel: name,
        from: { $ne: client.id }
      };
      var options = {
        tailable: true,
        awaitdata: true,
        numberOfRetries: -1,
        tailableRetryInterval: Channel.TAILABLE_RETRY_INTERVAL
      }

      self.cursor = collection.find(query, options).sort({ $natural: 1 });
      self.cursor.nextObject(function(err, data) { 
        self._handleNext(err, data);
      });
      self.emit('ready', collection);
      return collection;
    });
  });   
}
util.inherits(Channel, EventEmitter);
Channel.COLLECTION_SIZE = 10 * 1024 * 1024;
Channel.TAILABLE_RETRY_INTERVAL = 200;

Channel.prototype._handleNext = function(err, data) {
  var self = this;
  if (err || !data) {
    self.cursor.close();
    self.emit('error', err ? err : new Error('no data'));
  } else {
    if (data.type) self.emit(data.type, data);

    self.emit('data', data);

    self.cursor.nextObject(function(err, data) { 
      self._handleNext(err, data);
    });
  }
}

Channel.prototype.send = function(type, data) {
  var self = this;
  return self.collection.then(function(collection) {
    var data = {
      ts: new Date(),
      channel: self.name,
      from: self.client.id,
      type: type,
      data: data
    };
    return q.ninvoke(collection, 'insert', data);
  });
}

module.exports = Channel;