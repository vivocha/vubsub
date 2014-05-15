var util = require('util')
  , EventEmitter = require('events').EventEmitter
  , q = require('q')
  , mongo = require('mongodb')

function Channel(name, client, lastMessage) {
  var self = this;
  self.name = name;
  self.client = client;
  self.db = client.db;
  if (lastMessage) {
    self.lastMessage = mongo.ObjectID(lastMessage.toString());
  }
  self._init();
  self.db.collection('channels').insert({ client: self.client.id, channel: self.name, ns: self.client.namespace, ping: new Date() }, { w: 0 });
}
util.inherits(Channel, EventEmitter);
Channel.COLLECTION_SIZE = 10 * 1024 * 1024;
Channel.TAILABLE_RETRY_INTERVAL = 200;

Channel.prototype.send = function(type, data, cb) {
  var self = this;
  self.ping();
  return self.collection.then(function(collection) {
    return q.ninvoke(collection, 'insert', {
      ts: new Date(),
      channel: self.name,
      from: self.client.id,
      type: type,
      data: data
    });
  }).nodeify(cb);
}
Channel.prototype.leave = function(cb) {
  return this.client.leave(this, cb);
}
Channel.prototype.ping = function(cb) {
  var self = this;
  return self.client.ping().then(function() {
    return q.ninvoke(self.db.collection('channels'), 'update', { client: self.client.id, channel: self.name }, { $set: { ping: new Date() }});
  }).nodeify(cb);
}
Channel.prototype.count = function(cb) {
  return Channel.count(this.db, this.client.namespace, this.name, cb);
}
Channel.prototype._init = function() {
  var self = this;
  self.collection = Channel.collection(self.db, self.client.namespace).then(function(collection) {
    var cursor = collection.find().sort({ $natural: -1 }).limit(1);
    return q.ninvoke(cursor, 'nextObject').then(function(doc) {
      if (doc) {
        return doc;
      } else {
        var deferred = q.defer();
        collection.insert({ init: true }, { w: 1 }, function(err, docs) {  
          if (err || !docs || !docs.length) {
            console.error('failed to insert the first doc', err, docs, collection);
            deferred.reject(err || 'first_doc');
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
      self.lastMessage = self.lastMessage || doc._id;
      self._query(function(err, data) { 
        self._handleNext(err, data);
      });
      self.emit('ready', collection);
      return collection;
    });
  }).fail(function(err) {
    console.error('failed to initialize channel', self.name, self.client.namespace, err);
    self.leave();
    self.emit('error', err);
  });
}
Channel.prototype._query = function(cb) {
  var self = this;
  var query = { 
    _id: { $gt: self.lastMessage },
    channel: self.name,
    from: { $ne: self.client.id }
  };
  var options = {
    tailable: true,
    numberOfRetries: -1,
    tailableRetryInterval: Channel.TAILABLE_RETRY_INTERVAL
  }
  self.collection.then(function(collection) {
    self.cursor = collection.find(query, options).sort({ $natural: 1 });
    self.cursor.nextObject(cb);
  });
}
Channel.prototype._handleNext = function(err, data) {
  var self = this;
  if (err) {
    console.error('Channel._handleNext', self.name, err);
    console.warn('Channel._handleNext', self.name, 'reinitializing');
    self._init();
  } else if (!data) {
    if (self.cursor) {
      console.warn('Channel._handleNext', self.name, 'no data');
      self._query(function(err, data) { 
        self._handleNext(err, data);
      });
    } else {
      console.warn('Channel._handleNext', self.name, 'closed');
      self.emit('close', { reason: 'closed' });
    }
  } else {
    if (data._id) self.lastMessage = data._id;
    if (data.type) self.emit(data.type, data);
    self.emit('data', data);
    if (self.cursor) {
      self.cursor.nextObject(function(err, data) { 
        self._handleNext(err, data);
      });
    }
  }
}
Channel.prototype._close = function(cb) {
  if (this.cursor) {
    this.cursor.close();
    this.cursor = null;
  }
  return q.ninvoke(this.db.collection('channels'), 'remove', { client: this.client.id, channel: this.name }).nodeify(cb);
}

Channel.collection = function(db, namespace, cb) {
  var d = q.defer();
  var name = 'ns_' + namespace;

  function getOrCreateNamespace(cb) {
    db.collection(name, { strict: true }, function(err, collection) {
      if (err || !collection) {
        console.warn('Channel.collection', 'trying to create namespace collection', name);
        db.createCollection(name, {
          size: Channel.COLLECTION_SIZE,
          capped: true,
          autoIndexId: true
        }, cb);
      } else {
        cb(null, collection);
      }
    });
  }
  
  getOrCreateNamespace(function(err, collection) {
    if (err || !collection) {
      // Take two...
      getOrCreateNamespace(function(err, collection) {
        if (err) {
          console.error('Channel.collection', 'failed to create namespace collection', name);
          d.reject(err);
        } else if (!collection) {
          console.error('Channel.collection', 'failed to get namespace collection', name);
          d.reject(new Error('failed to create a new namespace collection'));
        } else {
          d.resolve(collection);
        }
      });
    } else {
      d.resolve(collection);
    }   
  });
  return d.promise.nodeify(cb);
}
Channel.send = function(db, namespace, message, cb) {
  return Channel.collection(db, namespace).then(function(collection) {
    return q.ninvoke(collection, 'insert', message);    
  }).nodeify(cb);
}
Channel.find = function(db, namespace, channel, cb) {
  return q.ninvoke(db.collection('channels').find({ channel: channel, ns: namespace }), 'toArray').nodeify(cb);
}
Channel.count = function(db, namespace, channel, cb) {
  return q.ninvoke(db.collection('channels').find({ channel: channel, ns: namespace }), 'count').nodeify(cb);
}

module.exports = Channel;