import { AppConfig } from "./types";

// Necessary Back-End Configuration Properties (using NPM "config")
const config: AppConfig = {
  backendServerPort: parseInt(`${process.env.BACKEND_PORT}`),
  backendServerUrl: `http://localhost:${process.env.BACKEND_PORT}`,
  frontendClientPort: parseInt(`${process.env.FRONTEND_PORT}`),
  frontendClientUrl: `http://localhost:${process.env.FRONTEND_PORT}`,
  logLevel: "trace"
};

export default config;
