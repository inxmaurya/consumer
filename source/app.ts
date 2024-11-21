import { Kafka } from 'kafkajs';

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

const consumer = kafka.consumer({ groupId: 'test-group' });

async function consumeMessages() {
  await consumer.connect();
  console.log('Consumer connected');

  const topic = 'test-topic';

  // Subscribe to the topic
  await consumer.subscribe({ topic, fromBeginning: true });

  // Consume messages
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        topic,
        partition,
        key: message.key?.toString(),
        value: message.value?.toString(),
      });
    },
  });
}

consumeMessages().catch((err) => {
  console.error('Error in consumer:', err);
  consumer.disconnect();
});
