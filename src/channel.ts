import { EventEmitter } from 'events';
import * as mongo from 'mongodb';
import { logger } from './common';
import { Client } from './client';

export class Channel extends EventEmitter {
  static COLLECTION_SIZE: number = 10 * 1024 * 1024;
  static TAILABLE_RETRY_INTERVAL: number = 200;

  private lastMessage: mongo.ObjectID;
  private collection: Promise<mongo.Collection>;
  private cursor: mongo.Cursor<any>;

  constructor(public name: string, private client: Client, lastMessage?: string) {
    super();
    if (lastMessage) {
      this.lastMessage = new mongo.ObjectID(lastMessage);
    }
    this.init();
    this.client.db.collection('channels').insertOne({ client: this.client.id, channel: this.name, ns: this.client.namespace, ping: new Date() }, { w: 0 });
  }
  private async init() {
    let resolve, reject;
    this.collection = new Promise((_resolve, _reject) => {
      resolve = _resolve;
      reject = _reject;
    });

    try {
      let collection = await Channel.collection(this.client.db, this.client.namespace);
      let doc = await collection.findOne({},{sort: { $natural: -1 }, limit: 1});
      if (!doc) {
        let result = await collection.insertOne({ init: true }, { w: 1 });
        if (result.insertedCount !== 1 || !result.ops[0]) {
          logger('failed to insert the first doc', result.ops, collection);
          throw new Error('first_doc');
        } else {
          doc = result.ops[0];
        }
      }
      // if we've come here, then:
      // - the collection exists
      // - it had at least a document
      // - and we know the _id to the most recent one
      this.lastMessage = this.lastMessage || doc._id;
      this.query();
      this.emit('ready', collection);
      resolve(collection);
    } catch (err) {
      logger('failed to initialize channel', this.name, this.client.namespace, err);
      this.leave();
      this.emit('error', err);
      reject(err);
    }
  }
  async send(type: string, data?: any): Promise<boolean> {
    this.ping();
    let coll = await this.collection;
    await coll.insertOne({
      ts: new Date(),
      channel: this.name,
      from: this.client.id,
      type: type,
      data: data
    }, { w: 1 });
    return true;
  }
  leave(): Promise<boolean> {
    return this.client.leave(this);
  }
  async ping(): Promise<boolean> {
    await this.client.ping();
    await this.client.db.collection('channels').updateOne({
      client: this.client.id,
      channel: this.name
    }, { $set: { ping: new Date() }}, { w: 1 });
    return true;
  }
  count(): Promise<number> {
    return Channel.count(this.client.db, this.client.namespace, this.name);
  }
  query() {
    let query = {
      _id: { $gt: this.lastMessage },
      channel: this.name,
      from: { $ne: this.client.id }
    };
    let options = {
      tailable: true,
      numberOfRetries: Number.MAX_VALUE,
      awaitData: true,
      tailableRetryInterval: Channel.TAILABLE_RETRY_INTERVAL,
      sort: { $natural: 1 }
    };

    this.collection.then(
      collection => {
        this.cursor = collection.find(query, options);
        this.cursor.next(this.handleNext.bind(this));
      },
      err => {
        this.emit('error',err)
      }
    );
  }
  handleNext(err, data) {
    if (!this.cursor && (err || !data)) {
      logger('Channel.handleNext', this.name, 'closed');
      this.emit('close', { reason: 'closed' });
    } else if (err) {
      logger('Channel.handleNext', this.name, 'reinitializing', err);
      this.init();
    } else if (!data) {
      logger('Channel.handleNext', this.name, 'no data');
      this.query();
    } else {
      if (data._id) this.lastMessage = data._id;
      if (data.type) this.emit(data.type, data);
      this.emit('data', data);
      if (this.cursor) {
        this.cursor.next(this.handleNext.bind(this));
      }
    }
  }
  async close(): Promise<boolean> {
    if (this.cursor) {
      this.cursor.close();
      this.cursor = null;
    }
    await this.client.db.collection('channels').deleteOne({ client: this.client.id, channel: this.name });
    return true;
  }

  static async collection(db: mongo.Db, namespace: string): Promise<mongo.Collection> {
    let name = 'ns_' + namespace;

    function getOrCreateNamespace(): Promise<mongo.Collection> {
      return new Promise(resolve => {
        db.collection(name, { strict: true }, function(err, collection) {
          if (err || !collection) {
            logger('Channel.collection', 'trying to create namespace collection', name);
            resolve(db.createCollection(name, {
              size: Channel.COLLECTION_SIZE,
              capped: true,
              autoIndexId: true
            }).then(collection => {
              if (!collection) {
                logger('Channel.collection', 'failed to get namespace collection', name);
                throw new Error('failed to create a new namespace collection');
              } else {
                return collection;
              }
            }));
          } else {
            resolve(collection);
          }
        });
      });
    }

    let collection;
    try {
      collection = await getOrCreateNamespace();
    } catch (err) {
      // Ignore the first err
    }
    if (!collection) {
      // Take two...
      try {
        collection = await getOrCreateNamespace();
      } catch (err) {
        logger('Channel.collection', 'failed to create namespace collection', name, err);
        return Promise.reject(err);
      }
    }
    return collection;
  }
  static async send(db: mongo.Db, namespace: string, message: any): Promise<any> {
    let collection = await Channel.collection(db, namespace);
    let result = await collection.insertOne(message, { w: 1 });
    return result.ops;
  }
  static find(db: mongo.Db, namespace: string, channel: string): Promise<any> {
    return db.collection('channels').find({ channel: channel, ns: namespace }).toArray();
  }
  static count(db: mongo.Db, namespace: string, channel: string): Promise<number> {
    return db.collection('channels').find({ channel: channel, ns: namespace }).count(true);
  }
}
