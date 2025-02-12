import dayjs from "dayjs";
import fs from "fs";
import path from "path";
import { Logger, pino } from "pino";

const level = "trace"; // Or through "config" # TODO: Later

const CURRENT_TIMESTAMP = dayjs().format("YYYY-MM-DD_HH-mm-ss");
const LOG_DIR = `logs`;
const CURR_LOG_FILE = path.join(
  process.cwd(),
  `${LOG_DIR}/server-${CURRENT_TIMESTAMP}.log`
);

// Define the log directory and ensure it exists
const LOG_DIRECTORY = path.join(process.cwd(), LOG_DIR);
if (!fs.existsSync(LOG_DIRECTORY)) {
  fs.mkdirSync(LOG_DIRECTORY);
}

/*
From Chat-GPT: 
For the logging library for Node.js called Pino, it has different log levels that 
represent the severity or importance of log messages. The log levels in Pino, in 
increasing order of severity, are:

- trace: Used for very detailed and fine-grained debugging information.
- debug: More detailed information than trace, typically used for debugging.
- info: General information about the application's state.
- warn: Indicates potential issues or situations that may need attention.
- error: Indicates errors that the application can recover from.
- fatal: Represents critical errors that lead to the termination of the application.
*/
const LEVEL_STRINGS = {
  10: "TRACE",
  20: "DEBUG",
  30: "INFO",
  40: "WARN",
  50: "ERROR",
  60: "FATAL"
} as const;
type LevelKey = keyof typeof LEVEL_STRINGS;

const LOGGER: Logger = pino({
  level: level,
  mixin(_context, level) {
    return { severity: LEVEL_STRINGS[level as LevelKey] };
  },
  transport: {
    targets: [
      {
        level: level,
        target: "pino/file",
        options: {
          destination: CURR_LOG_FILE,
          translateTime: "yyyy-mm-dd HH:MM:ss",
          ignore: "pid,hostname"
        }
      },
      {
        level: level,
        target: "pino-pretty",
        options: {
          colorize: true,
          translateTime: "yyyy-mm-dd HH:MM:ss",
          ignore: "pid,hostname,severity"
        }
      }
    ]
  },
  base: null,
  timestamp: () => `, "time":"${dayjs().format()}"`
});

export default LOGGER;
