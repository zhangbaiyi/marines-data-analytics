import "reflect-metadata";

import amqp from "amqplib";
import bodyParser from "body-parser";
import config from "config";
import cors from "cors";
import express, { NextFunction } from "express";
import http from "http";

import { AppConfig } from "../config/types";
import APP_ROUTER from "./routes";
import LOGGER from "./utils/logger";
import { startRabbitMQConnection } from "./utils/rabbitmq";
import { Optional } from "./utils/types";

const appConfig: AppConfig = config.util.toObject(config);
LOGGER.trace(appConfig);

const BACKEND_PORT = appConfig.backendServerPort;
const BACKEND_SERVER_URL = appConfig.backendServerUrl;

const app = express();
app.use(bodyParser.json({ limit: "1mb" }));
app.use(bodyParser.urlencoded({ limit: "1mb", extended: true }));
app.use(cors({ credentials: true }));
app.use(APP_ROUTER);
app.use((_, res, next: NextFunction) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Request-Method", "*");
  res.setHeader(
    "Access-Control-Allow-Methods",
    "GET, POST, PUT, PATCH, DELETE, HEAD, TRACE, OPTIONS"
  );
  res.setHeader(
    "Access-Control-Allow-Headers",
    "Origin, X-Requested-With, Content-Type, Accept"
  );
  next();
});

const RABBITMQ_URL = "amqp://guest:guest@localhost:5672";
let rabbitmqConnection: Optional<amqp.Connection> = null;

const server = http.createServer(app);
server.listen(BACKEND_PORT, async () => {
  LOGGER.info(`Server has started on port ${BACKEND_PORT}`);
  LOGGER.info(`Server is running on "${BACKEND_SERVER_URL}"`);
  await initDependencies();
});
server.on("error", shutdown);
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

async function initDependencies(): Promise<void> {
  const createdRabbitMQConnection = await startRabbitMQConnection(RABBITMQ_URL);
  rabbitmqConnection = createdRabbitMQConnection;
  // TODO: Initialize TypeORM Connection (later)
  LOGGER.debug("All App Dependencies initialized successfully.");
}

function shutdown() {
  LOGGER.warn("Shutting down server...");
  rabbitmqConnection?.close();
  server.close(() => {
    LOGGER.warn("Server closed. Exiting process...");
    process.exit(0);
  });
}

export { rabbitmqConnection as RABBITMQ_CONNECTION };
