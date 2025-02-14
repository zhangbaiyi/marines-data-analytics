import { DataSource } from "typeorm";

import { ENTITIES } from "../db-entity";
import { MIGRATIONS } from "../db-migration";
import LOGGER from "./logger";

let sqliteDBPath = "./src/db/database.sqlite";
if (process.env["NODE_ENV"] === "production") {
  sqliteDBPath = `./dist/server/${sqliteDBPath.replace("./", "")}`;
}
LOGGER.debug(`sqliteDBPath: ${sqliteDBPath}`);

/**
 * DataSource for TypeORM.
 *
 * @tutorial [TypeORM-DataSource](https://orkhan.gitbook.io/typeorm/docs/data-source-options)
 * @tutorial [TypeORM-DB-Entities](https://orkhan.gitbook.io/typeorm/docs/entities)
 * @tutorial [TypeORM-DB-Migrations](https://orkhan.gitbook.io/typeorm/docs/migrations)
 */
export const AppDataSourceTypeORM = new DataSource({
  type: "sqlite",
  database: sqliteDBPath,
  synchronize: false,
  logging: true,
  entities: ENTITIES,
  migrations: MIGRATIONS,
  subscribers: []
});
