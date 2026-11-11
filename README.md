[![npm](https://img.shields.io/npm/v/gcn-kafka)](https://www.npmjs.com/package/gcn-kafka)

# GCN Kafka Client for Node.js

This is the official Node.js client for the [General Coordinates Network (GCN)](https://gcn.nasa.gov). It is a very lightweight wrapper around [@platformatic/kafka](https://www.npmjs.com/package/@platformatic/kafka).

## To Install

Run this command to install with [npm](https://www.npmjs.com):

```
npm install gcn-kafka
```

## To Use

### Consumer Client

```mjs
import { Consumer } from 'gcn-kafka'

const consumer = new Consumer({
  client_id: 'fill-me-in',
  client_secret: 'fill-me-in',
})

// Create a consumer stream
const stream = await consumer.consume({
  topics: ['gcn.heartbeat'],
})

for await (const message of stream) {
  console.log(`Received: ${message.value}`)
}
```

### Producer Client

```mjs
import { Producer } from 'gcn-kafka'

const producer = new Producer({
  client_id: 'fill-me-in',
  client_secret: 'fill-me-in',
})

const encodedData = Buffer.from(
  JSON.stringify({
    key1: 'value',
    //...
  }),
  'utf-8',
)

async function main() {
  await producer.send({
    messages: [
      {
        topic: 'gcn.example.topic',
        value: encodedData,
      },
    ],
  })

  await producer.close()
}

await main()
```

### Admin Client

```mjs
import {
  Admin,
  AclOperations,
  AclPermissionTypes,
  ResourceTypes,
  ResourcePatternTypes,
} from 'gcn-kafka'

const admin = new Admin({
  client_id: 'fill-me-in',
  client_secret: 'fill-me-in',
})

async function main() {
  try {
    const acls = await admin.describeAcls({
      filter: {
        resourceType: ResourceTypes.ANY,
        host: '*',
        permissionType: AclPermissionTypes.ANY,
        operation: AclOperations.ANY,
        resourcePatternType: ResourcePatternTypes.ANY,
      },
    })
    for (const item of acls) {
      console.log(item.acls)
    }
    await admin.close()
  } catch (error) {
    console.log(error)
  }
}

await main()
```

## Testing and Development Kafka Clusters

GCN has three Kafka clusters: production, testing, and an internal development deployment. Use the optional `domain` parameter to select which broker to connect to.

```mjs
// Production (default)
const consumer = new Consumer({
  client_id: 'fill me in',
  client_secret: 'fill me in',
  domain: 'gcn.nasa.gov',
})

// Testing
const consumer = new Consumer({
  client_id: 'fill me in',
  client_secret: 'fill me in',
  domain: 'test.gcn.nasa.gov',
})

// Development (internal)
const consumer = new Consumer({
  client_id: 'fill me in',
  client_secret: 'fill me in',
  domain: 'dev.gcn.nasa.gov',
})
```
