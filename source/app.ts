import { Kafka } from 'kafkajs';
import { Redis } from 'ioredis';
import dotenv from 'dotenv';

dotenv.config({ path: `.env.${process.env.NODE_ENV}` });

const BROCKER_LISTS = [
  process.env.FIRST_BROKER!,
  process.env.SECOND_BROKER!,
  process.env.THIRD_BROKER!,
  process.env.FOURTH_BROKER!,
  process.env.FIFTH_BROKER!,
  process.env.SIXTH_BROKER!,
];

if (!BROCKER_LISTS.every(Boolean)) {
  throw new Error('One or more Kafka broker addresses are missing.');
}

const CONFIGURATIONS = {
  SSL: process.env.SSL === 'true',
  SASL: undefined, // Add SASL config here if required
};

console.log('Kafka Configuration:');
console.log(BROCKER_LISTS, CONFIGURATIONS);

const kafka = new Kafka({
  clientId: 'my-consumer',
  brokers: BROCKER_LISTS,
  ssl: CONFIGURATIONS.SSL,
  sasl: undefined,
});

const consumer = kafka.consumer({
  groupId: 'test-group',
});

// Redis client setup
const redis = new Redis({
  host: process.env.REDIS_HOST || 'localhost',
  port: parseInt(process.env.REDIS_PORT || '6379'),
});

redis.on('error', (err) => {
  console.error('Redis connection error:', err);
});

async function publishToRedis(channel: string, message: string, retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      await redis.publish(channel, message);
      console.log(`Published message to Redis channel "${channel}"`);
      return;
    } catch (error) {
      console.error('Redis publish error:', error);
      if (i === retries - 1) {
        console.error('Max retries reached. Message not published.');
      } else {
        console.log('Retrying...');
      }
    }
  }
}

async function consumeMessages() {
  try {
    await consumer.connect();
    console.log('Consumer connected');

    const topic = 'test-topic';
    await consumer.subscribe({ topic, fromBeginning: true });

    await consumer.run({
      autoCommit: false,
      eachMessage: async ({ topic, partition, message }) => {
        const key = message.key?.toString();
        const value = message.value?.toString();

        console.log('Received message:', { topic, partition, key, value, offset: message.offset });

        // Process and commit offset
        try {
          console.log('Processing message...');
          await consumer.commitOffsets([{ topic, partition, offset: (parseInt(message.offset) + 1).toString() }]);
          console.log('Offset committed');
        } catch (error) {
          console.error('Error committing offset:', error);
        }

        // Publish to Redis
        await publishToRedis('test-channel', JSON.stringify({ channel: 'test-channel', datum: value, action: 'message' }));
      },
    });
  } catch (err) {
    console.error('Error in consumer:', err);
  }
}

consumeMessages().catch((err) => {
  console.error('Fatal error in consumer:', err);
  consumer.disconnect();
  redis.disconnect();
});

process.on('SIGINT', async () => {
  console.log('Interrupt signal received. Closing connections...');
  await consumer.disconnect();
  redis.disconnect();
  process.exit(0);
});
