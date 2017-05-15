import * as mongo from 'mongodb';
import { logger } from './common';
import { Channel } from './channel';

export class Client {
  private channels: {
    [name:string]: Channel;
  } = {};

  constructor(public namespace: string, public id: string, public db: mongo.Db) {
  }

  async ping(): Promise<boolean> {
    await this.db.collection('clients').updateOne({ _id: this.id }, { $set: { ping: new Date() }});
    return true;
  }
  join(name: string, lastMessage?: string): Channel {
    if (this.channels[name]) {
      return this.channels[name];
    } else {
      let channel = new Channel(name, this, lastMessage);
      this.channels[name] = channel;
      return channel;
    }
  }
  channel(name: string, lastMessage?: string): Channel {
    return this.join(name, lastMessage);
  }
  leave(channel: Channel): Promise<boolean> {
    delete this.channels[channel.name];
    return channel.close();
  }
}

const clients: {
  [id:string]: Client;
} = {};
export async function create(db: mongo.Db, meta: any = {}): Promise<Client> {
  meta.ns = meta.ns || 'vubsub';
  let now = new Date();
  let data = { ts: now, ping: now, meta: meta };

  let result = await db.collection('clients').insertOne(data, {w:1});
  if (!result.insertedCount) {
    logger('Did not get client!', data);
    throw new Error('no-data');
  } else {
    logger('Got client!');
    let client = new Client(meta.ns, result.insertedId.toString(), db);
    clients[client.id] = client;
    await Channel.collection(db, meta.ns);
    return client;
  }
}
export async function get(id): Promise<Client> {
  if (clients[id]) {
    return clients[id];
  } else {
    throw new Error('Unknown client');
  }
}

