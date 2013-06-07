vubsub
======

Pub/Sub for Node.js and MongoDB

vubsub is a Pub/Sub implementation for Node.js on top of MongoDB tailable
cursors. vubsub was inspired by mubsub.

The main differences between vubsub and mubsub are:
- Active clients are tracked via a dedicated *clients* collection
- You can specify a namespace for each client (or group of clients): a namespace
is a mapped to a collection (ns_*namespace*) where all the messages for that
namespace are stored
- When connecting a client to a channel, you can specify the _id of the last
received message and restart receiving message from that point.

vubsub uses Q, Kris Kowal's implementation of Promises.

## How to Install

```bash
npm install vubsub
```

## How to use

To create a new client, simply call the create function passing a *db* instance and,
optionally, a metadata object. If the metadata contains the *ns* key, it'll be used as
the namespace for the client (i.e. the client will be linked to a collection named
ns_*namespace*). The metadata is stored, together with the client id in the *clients*
collection (you can you this to keep track of your clients).

```js
var vubsub = require('vubsub')
  , MongoClient = require('mongodb').MongoClient

MongoClient.connect('mongodb://localhost/test', { auto_reconnect: true }, function(err, db) {
  if (err || !db) throw new Error('failed to connect to the db');
  
  vubsub.create(db, { ns: 'myNamespace' }).then(function(client) {
    console.log('Client connected: ' + client.id);
  });
});
```

To create a channel and listen to events on the channel:

```js
vubsub.create(db, { ns: 'myNamespace' }).then(function(client) {
  return client.channel('myChannel' /*, you can pass here the id of the last received message */);
}).then(function(channel) {
  console.log('Created channel ' + channel.name);
  channel.on('myEvent', function(event) {
    console.log('Received event', event);
  });
});
```

To send a message to a channel:

```js
channel.send('myEvent', { a: 1, b: 2}).then(function() {
  console.log('Message sent');
});
```