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
  // Disable auto-commit
  autoCommit: false
});

async function consumeMessages() {
  await consumer.connect();
  console.log('Consumer connected');

  const topic = 'test-topic';

  // Subscribe to the topic
  await consumer.subscribe({ topic, fromBeginning: true });

  // Redis publisher configuration
  const redis = new Redis();

  // Consume messages
  await consumer.run({
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
