import express from "express";
import { FILE_GENERATE_STATUS_ROUTER } from "./file-generate-status";
import { HTTP_STATUS_CODE, onErrorMsg, onSuccessMsg } from "./utils";
import { TEST_ROUTER } from "./test";

const APP_ROUTER = express.Router();
APP_ROUTER.use(FILE_GENERATE_STATUS_ROUTER);
APP_ROUTER.use(TEST_ROUTER);

// Route for Testing (Generated by Nx by Default)
APP_ROUTER.get("/", (_, res) => {
  return res.status(HTTP_STATUS_CODE.OK).json(onSuccessMsg("Hello World!!!"));
});

APP_ROUTER.get("/api", (_, res) => {
  return res.status(HTTP_STATUS_CODE.OK).json(onSuccessMsg("Hello API!!!"));
});

// Fallback Route (If Route Passed is Unspecified)
APP_ROUTER.get("*", (_, res) => {
  return res
    .status(HTTP_STATUS_CODE.BAD_REQUEST)
    .json(onErrorMsg("Invalid API endpoint specified."));
});

export default APP_ROUTER;
