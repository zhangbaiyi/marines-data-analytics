import express from "express";
import fs from "fs";
import multer from "multer";
import path from "path";

import LOGGER from "../utils/logger";
import {
  HTTP_STATUS_CODE,
  MULTER_FILE_UPLOAD_LIMIT,
  onErrorMsg,
  onSuccessMsg
} from "./utils";

type UploadedFileInfo = {
  filename: string;
  path: string;
};

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

// Uploading files from FE-Client to the server
router.post(
  "/api/upload-files",
  MULTER_UPLOAD.array("files", MULTER_FILE_UPLOAD_LIMIT),
  (req, res) => {
    if (!req.files || req.files.length === 0) {
      LOGGER.error("No files uploaded");
      return res.status(400).json({ error: "No files uploaded" });
    }

    const uploadedFiles: UploadedFileInfo[] = (
      req.files! as Express.Multer.File[]
    ).map((file) => ({
      filename: file.filename,
      path: file.path
    }));

    LOGGER.debug(uploadedFiles);
    return res.status(HTTP_STATUS_CODE.OK).json(onSuccessMsg("Files uploaded"));
  }
);

// Serving and returning static files (for Download and Preview on FE-Client)
router.use("/api/datalake/files", express.static(dataLakePath));

const finalSubmissionPath = path.resolve(process.cwd(), "../final-submission");
router.use("/api/final/files", express.static(finalSubmissionPath));

router.get("/api/final-result", (_, res) => {
  if (!fs.existsSync(finalSubmissionPath)) {
    return res
      .status(HTTP_STATUS_CODE.BAD_REQUEST)
      .json(onErrorMsg("No files uploaded"));
  }

  // Actual logic to derive the path of final submission would go here
  const finalFileName = "PDF_Test.pdf";
  const finalSubmissionLocalFilePath = path.join(
    "api/final/files/",
    finalFileName
  );
  LOGGER.debug(
    `Location of Final-Submission PDF: ${finalSubmissionLocalFilePath}`
  );

  return res
    .status(HTTP_STATUS_CODE.OK)
    .json(onSuccessMsg(finalSubmissionLocalFilePath));
});

export { router as FILE_OPERATIONS_ROUTER };
