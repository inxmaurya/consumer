import { Kafka } from 'kafkajs';
import { Redis } from 'ioredis';

const kafka = new Kafka({
  clientId: 'my-consumer',
  brokers: [
    'b-1.smc.4nooo6.c2.kafka.ap-south-1.amazonaws.com:9094',
    'b-2.smc.4nooo6.c2.kafka.ap-south-1.amazonaws.com:9094',
    'b-3.smc.4nooo6.c2.kafka.ap-south-1.amazonaws.com:9094',
  ],
  ssl: true,
  sasl: undefined, // Set this if SASL is required.
});

const consumer = kafka.consumer({
  groupId: 'test-group',
});

// Redis publisher configuration
const redis = new Redis({
  host: process.env.REDIS_HOST || 'localhost',
  port: parseInt(process.env.REDIS_PORT || '6379'),
});

async function consumeMessages() {
  await consumer.connect();
  console.log('Consumer connected');

  const topic = 'test-topic';

  // Subscribe to the topic
  await consumer.subscribe({ topic, fromBeginning: true });

  // Consume messages
  await consumer.run({
    autoCommit: false, // Disable auto-commit
    eachMessage: async ({ topic, partition, message }) => {
      console.log({topic,partition,key: message.key?.toString(),value: message.value?.toString(),offset: message.offset,});

      // Commit the offset after processing
      await consumer.commitOffsets([
        { topic, partition, offset: (parseInt(message.offset) + 1).toString() },
      ]);

      const key =  message.key?.toString();
      const value =  message.value?.toString();
      console.log('START-----Consumed message')
      console.log(`MESSAGE: Key: ${key} - Value: ${value}`);
      console.log('END-----Consumed message');
      // Publish to Redis
      redis.publish('test-channel', JSON.stringify({ channel: 'test-channel', datum: value, action: 'message' }));
      console.log('Published message to Redis channel "test-channel"');
    },
  });
}

consumeMessages().catch((err) => {
  console.error('Error in consumer:', err);
  consumer.disconnect();
  redis.disconnect();
});
