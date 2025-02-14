import LOGGER from "./src/utils/logger";
import { z } from "zod";
import { fromZodError } from "zod-validation-error";

// Include secret enviornment variables here
const processEnvSchema = z
  .object({
    FRONTEND_PORT: z.number().gte(1000),
    BACKEND_PORT: z.number().gte(1000)
  })
  .strict();
const envParseResult = processEnvSchema.safeParse(process.env);
// Checking if enviornment variables (in ".env" file) were setup properly
if (!envParseResult.success) {
  LOGGER.error(fromZodError(envParseResult.error));
  process.exit(1);
}

// Allows for global TypeScript intellisense of 'process.env' variables in the Backend
declare global {
  namespace NodeJS {
    // Extending ProcessEnv to include the validated environment variables
    /* eslint-disable-next-line @typescript-eslint/no-empty-object-type, @typescript-eslint/no-empty-interface */
    interface ProcessEnv extends z.infer<typeof processEnvSchema> {}
  }
}

// If this file has no import/export statements (i.e. is a script)
// convert it into a module by adding an empty export statement.
export {};
