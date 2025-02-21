import express from "express";
import fs from "fs";
import multer from "multer";
import path from "path";

import { RABBITMQ_CONNECTION } from "../main";
import LOGGER from "../utils/logger";
import {
  checkChannelQueuesStatus,
  createRabbitMQReqResChannel,
  generateCorrelationIDRabbitMQ,
  generateReqAndResQueueStrings
  // RABBITMQ_TIMEOUT_TIME_MS
} from "../utils/rabbitmq";
import {
  HTTP_STATUS_CODE,
  MULTER_FILE_UPLOAD_LIMIT,
  onErrorMsg,
  onSuccessMsg
} from "./utils";

type UploadedFileInfo = {
  filename: string;
  path: string;
};

type PythonResponseData = {
  file_name: string;
};

const CHANNEL_PREFIX = "file_generate_status" as const;
const { REQUEST_QUEUE_STR, RESPONSE_QUEUE_STR } =
  generateReqAndResQueueStrings(CHANNEL_PREFIX);

const router = express.Router();

// Ensure the 'data-lake' directory exists
const dataLakePath = path.resolve(process.cwd(), "../data-lake");
LOGGER.debug(dataLakePath);
if (!fs.existsSync(dataLakePath)) {
  fs.mkdirSync(dataLakePath);
} else {
  LOGGER.warn("Directory 'data-lake' already exists.");
}

// Ensure the 'final-submission' directory exists
const finalSubmissionPath = path.resolve(process.cwd(), "../final-submission");
if (!fs.existsSync(finalSubmissionPath)) {
  fs.mkdirSync(finalSubmissionPath);
} else {
  LOGGER.warn("Directory 'final-submission' already exists.");
}

// Multer Configuration
const multerStorageConfig = multer.diskStorage({
  destination: (_, __, callback) => {
    callback(null, dataLakePath);
  },
  filename: (_, file, callback) => {
    callback(null, file.originalname);
  }
});
const MULTER_UPLOAD = multer({ storage: multerStorageConfig });

// Uploading files from FE-Client to the server
router.post(
  "/api/upload-files",
  MULTER_UPLOAD.array("files", MULTER_FILE_UPLOAD_LIMIT),
  (req, res) => {
    if (!req.files || req.files.length === 0) {
      LOGGER.error("No files uploaded");
      return res.status(400).json({ error: "No files uploaded" });
    }

    const uploadedFiles: UploadedFileInfo[] = (
      req.files! as Express.Multer.File[]
    ).map((file) => ({
      filename: file.filename,
      path: file.path
    }));

    LOGGER.debug(uploadedFiles);
    return res.status(HTTP_STATUS_CODE.OK).json(onSuccessMsg("Files uploaded"));
  }
);

// Serving and returning static files (for Download and Preview on FE-Client)
router.use("/api/datalake/files", express.static(dataLakePath));
router.use("/api/final/files", express.static(finalSubmissionPath));

let counter = 0;
router.post("/api/file-generate-status", async (req, res) => {
  LOGGER.trace(req.query);
  LOGGER.trace(req.body);
  const { message } = req.body as { message: string };

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
      Buffer.from(JSON.stringify({ value: message, query_params: req.query })),
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

    const parsedResponseData = JSON.parse(response) as PythonResponseData;
    const fileName = parsedResponseData.file_name;
    LOGGER.debug(`Parsed Response: ${fileName}`);

    // Close the channel to avoid leak in abstraction (avoid listening for the different/"wrong" response messages)
    fileGenerateStatusChannel.close();

    // Needed for static file access on the "/api/final/files" Express middleware endpoint
    const finalSubmissionLocalFilePath = path.join(
      "api/final/files/",
      fileName
    );

    return res
      .status(HTTP_STATUS_CODE.OK)
      .json(onSuccessMsg(finalSubmissionLocalFilePath));
  } catch (error: unknown) {
    const err = error as Error;
    LOGGER.error(`Error Handling Python Request: ${err.message}`);
    return res
      .status(HTTP_STATUS_CODE.INTERNAL_SERVER_ERROR)
      .json(onErrorMsg(err.message));
  }
});

export { router as FILE_GENERATE_STATUS_ROUTER };
