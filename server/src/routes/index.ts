import express from "express";

import { HTTP_STATUS_CODE, onErrorMsg, onSuccessMsg } from "./utils";

const APP_ROUTER = express.Router();

// Add more routes here below: (e.g. APP_ROUTER.use(...));


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
