import express from "express";
import { jsPDF } from "jspdf";
import path from "path";

import LOGGER from "../utils/logger";
import { HTTP_STATUS_CODE, onSuccessMsg } from "./utils";

const router = express.Router();

const finalSubmissionPath = path.resolve(process.cwd(), "../final-submission");
router.use("api/final/files", express.static(finalSubmissionPath));

router.post("/api/test", (req, res) => {
  LOGGER.warn(req.body);
  const doc = new jsPDF();
  doc.setFontSize(16);
  doc.text("Hello World", 10, 10);

  const body = req.body as { arguments: string };
  LOGGER.warn(body.arguments);
  const args = JSON.parse(body.arguments) as Record<string, string>;
  LOGGER.warn("Got here");
  LOGGER.debug(args);
  doc.text("Entries from Front-End:", 10, 15);
  let yCoor = 20;
  for (const [key, value] of Object.entries(args)) {
    LOGGER.warn(`Key: ${key}, Value: ${value}`);
    doc.text(`${key}: ${value}`, 10, yCoor);
    yCoor += 10;
  }

  const finalFileName = "PDF_Test.pdf";
  const finalSubmissionLocalFilePath = path.join(
    "api/final/files/",
    finalFileName
  );
  LOGGER.debug(
    `Location of Final-Submission PDF: ${finalSubmissionLocalFilePath}`
  );
  LOGGER.debug(
    `Final-Submission Path: ${path.join(finalSubmissionPath, finalFileName)}`
  );

  doc.save(path.join(finalSubmissionPath, finalFileName));

  return res
    .status(HTTP_STATUS_CODE.OK)
    .json(onSuccessMsg(finalSubmissionLocalFilePath));
});

export { router as TEST_ROUTER };
