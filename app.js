require('dotenv').config();
const express = require('express');
const { KafkaClient, Consumer } = require('kafka-node');
const axios = require('axios');

const app = express();
app.use(express.json());
const port = process.env.SERVICE_PORT

// Kafka configuration
const kafkaClient = new KafkaClient({ kafkaHost: process.env.KAFKA_HOST});
const topic = process.env.KAFKA_TOPIC;
const groupId = process.env.KAFKA_GROUP_ID;
const apiUrl = process.env.KAFKA_EVENT_REDIRECT_API;

function runConsumer() {
  return new Promise((resolve, reject) => {
    const consumer = new Consumer(
      kafkaClient,
      [{ topic, partition: 0 }],
      {
        groupId,
      }
    );

    console.log('Connected to Kafka');
    console.log(`Subscribed to topic: ${topic}`);

    consumer.on('message', async (message) => {
      console.log(`Received message: ${message.value}`);
      try {
        const response = await axios.post(apiUrl, { event: message.value });
        console.log('API call success:', response.status, response.statusText);
      } catch (error) {
        console.error('API call failed:', error.message);
      }
    });

    consumer.on('error', (err) => {
      console.error('Consumer error:', err.message);
      reject(err);
    });

    consumer.on('offsetOutOfRange', (err) => {
      console.error('Offset out of range:', err.message);
    });

    resolve();
  });
}

// Health check endpoint
app.get('/health', (req, res) => {
  res.send({ status: 'Service is running' });
});


// Start the Kafka consumer and Express server
app.listen(port, async () => {
  console.log(`Server running on port ${port}`);
  try {
    await runConsumer();
  } catch (err) {
    console.error('Error starting Kafka consumer:', err.message);
  }
});
