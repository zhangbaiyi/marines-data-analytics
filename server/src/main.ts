import config from "config";
import express from "express";

import { AppConfig } from "../config/types";
import LOGGER from "./utils/logger";

const appConfig: AppConfig = config.util.toObject(config);
LOGGER.info(appConfig);

const backendServerPort = appConfig.backendServerPort;
const backendServerUrl = appConfig.backendServerUrl;

const app = express();

app.get("/", (_, res) => {
  res.send({ message: "Hello API" });
});

app.listen(backendServerPort, () => {
  LOGGER.info(`Server is running on ${backendServerUrl}`);
});
