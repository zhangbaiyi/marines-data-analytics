import amqp from "amqplib";
import assert from "assert";

import LOGGER from "./logger";

// Default Timeout for RabbitMQ Operations
const RABBITMQ_TIMEOUT_TIME_SEC = 30;
export const RABBITMQ_TIMEOUT_TIME_MS = RABBITMQ_TIMEOUT_TIME_SEC * 1000;

type RabbitMQQueueStrings = {
  REQUEST_QUEUE_STR: string;
  RESPONSE_QUEUE_STR: string;
};

export function generateReqAndResQueueStrings(
  channelPrefix: string
): RabbitMQQueueStrings {
  return {
    REQUEST_QUEUE_STR: `${channelPrefix}_request_queue` as const,
    RESPONSE_QUEUE_STR: `${channelPrefix}_response_queue` as const
  };
}

function generateRandomInt(min = 0, max = 1_000_000): number {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

export function generateCorrelationIDRabbitMQ(counter: number) {
  return `${Date.now()}_${generateRandomInt()}_${counter}`;
}

export async function startRabbitMQConnection(
  rabbitmq_url: string
): Promise<amqp.Connection> {
  assert(rabbitmq_url.length > 0, "RabbitMQ URL should not be empty");
  return new Promise((resolve, reject) => {
    try {
      // Setup RabbitMQ connection
      const connection = amqp.connect(rabbitmq_url);

      LOGGER.info("RabbitMQ Connection created successfully.");
      resolve(connection);
    } catch (error) {
      const err = error as Error;
      LOGGER.error(
        `Error: Failed to start RabbitMQ connection properly. Full Error Message - ${err}`
      );
      reject(err);
    }
  });
}

export async function createRabbitMQReqResChannel(
  connection: amqp.Connection,
  channelUsagePrefix: string
): Promise<amqp.Channel> {
  assert(
    channelUsagePrefix.length > 0,
    "Channel-Usage Prefix should not be empty"
  );
  try {
    // Setup a new RabbitMQ channel for transactions
    const channel = await connection.createChannel();

    // Declare Request and Response Queues
    const activeQueues = [
      `${channelUsagePrefix}_REQUEST_QUEUE`,
      `${channelUsagePrefix}_RESPONSE_QUEUE`
    ] as const;
    for (const queue of activeQueues) {
      await channel.assertQueue(queue, { durable: true });
    }

    return channel;
  } catch (err) {
    LOGGER.error(
      `Error: Failed to initiate RabbitMQ channels properly. Full Error Message - ${err}`
    );
    process.exit(1);
  }
}

export async function checkChannelQueuesStatus(
  channel: amqp.Channel,
  channelUsagePrefix: string
): Promise<void> {
  assert(
    channelUsagePrefix.length > 0,
    "Channel-Usage Prefix should not be empty"
  );

  const activeQueues = [
    `${channelUsagePrefix}_REQUEST_QUEUE`,
    `${channelUsagePrefix}_RESPONSE_QUEUE`
  ] as const;
  for (const queue of activeQueues) {
    const queueInfo = await channel.checkQueue(queue);
    LOGGER.debug(`Queue: ${queue}`);
    LOGGER.debug(`Details: ${JSON.stringify(queueInfo)}`);
  }
}
