import express from "express";
import { jsPDF } from "jspdf";
import path from "path";
import LOGGER from "src/utils/logger";
import { HTTP_STATUS_CODE, onSuccessMsg } from "./utils";

const router = express.Router();

const finalSubmissionPath = path.resolve(process.cwd(), "../final-submission");
router.use("api/final/files", express.static(finalSubmissionPath));

router.post("/api/test", (_req, res) => {
  const doc = new jsPDF();
  doc.setFontSize(16);
  doc.text("Hello World", 10,10);

  const finalFileName = "PDF_Test.pdf";
  const finalSubmissionLocalFilePath = path.join(
    "api/final/files/",
    finalFileName
  );
  LOGGER.debug(
    `Location of Final-Submission PDF: ${finalSubmissionLocalFilePath}`
  );

  doc.save(path.join(finalSubmissionPath, finalFileName));

  return res
    .status(HTTP_STATUS_CODE.OK)
    .json(onSuccessMsg(finalSubmissionLocalFilePath));
});

export { router as TEST_ROUTER };
