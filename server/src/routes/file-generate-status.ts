import express from "express";

import { RABBITMQ_CONNECTION } from "../main";
import LOGGER from "../utils/logger";
import {
  checkChannelQueuesStatus,
  createRabbitMQReqResChannel,
  generateCorrelationIDRabbitMQ,
  generateReqAndResQueueStrings
  // RABBITMQ_TIMEOUT_TIME_MS
} from "../utils/rabbitmq";
import { HTTP_STATUS_CODE, onErrorMsg, onSuccessMsg } from "./utils";

const CHANNEL_PREFIX = "file_generate_status" as const;
const { REQUEST_QUEUE_STR, RESPONSE_QUEUE_STR } =
  generateReqAndResQueueStrings(CHANNEL_PREFIX);

const router = express.Router();

let counter = 0;
router.post("/api/file-generate-status", async (req, res) => {
  LOGGER.trace(req.body);
  const { filePrefix } = req.body as { filePrefix: string };

  LOGGER.trace(`Previous: ${counter}`);
  // Generate a unique correlation ID for this request
  const correlationId = generateCorrelationIDRabbitMQ(counter);
  LOGGER.debug(`File Request ID: ${correlationId}`);
  counter++;
  LOGGER.trace(`Current: ${counter}`);

  try {
    const fileGenerateStatusChannel = await createRabbitMQReqResChannel(
      RABBITMQ_CONNECTION!,
      CHANNEL_PREFIX
    );
    await checkChannelQueuesStatus(fileGenerateStatusChannel, CHANNEL_PREFIX);

    LOGGER.trace(`ID (Before Send): ${correlationId}`);
    // Send the request to the Request Queue
    fileGenerateStatusChannel.sendToQueue(
      REQUEST_QUEUE_STR,
      Buffer.from(JSON.stringify(filePrefix)),
      {
        correlationId,
        replyTo: RESPONSE_QUEUE_STR
      }
    );

    // Listen for the response from the Response Queue
    const response = await new Promise<string>((resolve, reject) => {
      // Fallback for too long of a response time
      // const timeout = setTimeout(() => {
      //   LOGGER.error(
      //     `Timeout: No response received for correlationId: ${correlationId}`
      //   );
      //   fileGenerateStatusChannel.close();
      //   throw new Error("Timeout: No response received from Python");
      // }, RABBITMQ_TIMEOUT_TIME_MS);

      fileGenerateStatusChannel.consume(
        RESPONSE_QUEUE_STR,
        (message) => {
          if (message == null) {
            // clearTimeout(timeout);
            fileGenerateStatusChannel.close();
            reject(new Error("Invalid Response from Python"));
            return;
          }
          const messageCorrelationId = message.properties
            .correlationId as string;
          LOGGER.trace({
            messageCorrelationId,
            correlationId
          });

          if (message.content.length === 0) {
            // clearTimeout(timeout);
            fileGenerateStatusChannel.close();
            reject(new Error("Empty Response from Python"));
            return;
          }

          if (messageCorrelationId === correlationId) {
            const responseData = message.content.toString();
            LOGGER.trace(
              `Python Response Message Content: ${message.content.toString()}`
            );

            // clearTimeout(timeout);
            // Acknowledge the message from RabbitMQ
            fileGenerateStatusChannel.ack(message);

            resolve(responseData);
          }
        },
        { noAck: false } // Ensure manual acknowledgment
      );
    });

    const parsedResponseData = JSON.parse(response) as {
      prediction: string;
    };
    const prediction = parsedResponseData.prediction;
    LOGGER.debug(`Parsed Response: ${prediction}`);

    // Close the channel to avoid leak in abstraction (avoid listening for the different/"wrong" response messages)
    fileGenerateStatusChannel.close();

    return res
      .status(HTTP_STATUS_CODE.OK)
      .json(onSuccessMsg(parsedResponseData));
  } catch (error: unknown) {
    const err = error as Error;
    LOGGER.error(`Error Handling Python Request: ${err.message}`);
    return res
      .status(HTTP_STATUS_CODE.INTERNAL_SERVER_ERROR)
      .json(onErrorMsg(err.message));
  }
});

export { router as FILE_GENERATE_STATUS_ROUTER };
