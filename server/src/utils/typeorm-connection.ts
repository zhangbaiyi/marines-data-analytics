import { DataSource } from "typeorm";

import LOGGER from "./logger";
import { AppDataSourceTypeORM } from "./typeorm-data-source";
import { Optional } from "./types";

async function startDBConnection(): Promise<DataSource> {
  return new Promise((resolve, reject) => {
    LOGGER.debug("Establishing SQLite3 connection (with TypeORM)...");
    AppDataSourceTypeORM.initialize()
      .then((dataSource) => {
        LOGGER.debug("SQLite3 Database connection established successfully.");
        resolve(dataSource);
      })
      .catch((error) => {
        LOGGER.error("Error connecting to the database:", error);
        reject(error);
      });
  });
}

async function closeDBConnection(
  connection: Optional<DataSource>
): Promise<void> {
  LOGGER.trace(
    `Shutting down the SQLite3 connection (with TypeORM)...${
      connection
        ? "CONNECTION CURRENTLY ACTIVE!"
        : " No connection found currently."
    }`
  );
  await connection?.destroy();
  LOGGER.debug("SQLite3 Database connection closed.");
}

export { closeDBConnection, startDBConnection };
