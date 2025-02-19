import express from "express";
import fs from "fs";
import multer from "multer";
import path from "path";

import LOGGER from "../utils/logger";
import {
  HTTP_STATUS_CODE,
  MULTER_FILE_UPLOAD_LIMIT,
  onSuccessMsg
} from "./utils";

// Ensure the 'data-lake' directory exists
const dataLakePath = path.resolve(process.cwd(), "../data-lake");
LOGGER.debug(dataLakePath);
if (!fs.existsSync(dataLakePath)) {
  fs.mkdirSync(dataLakePath);
} else {
  LOGGER.warn("Directory already exists.");
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

const router = express.Router();

router.post(
  "/api/upload-files",
  MULTER_UPLOAD.array("files", MULTER_FILE_UPLOAD_LIMIT),
  (req, res) => {
    if (!req.files || req.files.length === 0) {
      LOGGER.error("No files uploaded");
      return res.status(400).json({ error: "No files uploaded" });
    }

    const uploadedFiles = (req.files! as Express.Multer.File[]).map((file) => ({
      filename: file.filename,
      path: file.path
    }));

    LOGGER.debug(uploadedFiles);
    return res.status(HTTP_STATUS_CODE.OK).json(onSuccessMsg("Files uploaded"));
  }
);

export { router as FILE_OPERATIONS_ROUTER };
