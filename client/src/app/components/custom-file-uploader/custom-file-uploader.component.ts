import { CommonModule } from "@angular/common";
import { Component, model, OnInit, ViewChild } from "@angular/core";
import { MatButtonModule } from "@angular/material/button";
import { FilePickerComponent, FilePickerModule, FilePreviewModel, ValidationError } from "ngx-awesome-uploader";

import { CustomAdapter } from "./custom-adapter";

/**
 * REFERENCES: Ngx-Awesome-Uploader (File Uploader)
 * - https://www.npmjs.com/package/ngx-awesome-uploader?activeTab=readme
 * - https://stackblitz.com/edit/ngx-awesome-uploader?file=README.md
 */
@Component({
  selector: "custom-file-uploader-ngx-awesome-uploader",
  imports: [CommonModule, MatButtonModule, FilePickerModule],
  templateUrl: "./custom-file-uploader.component.html",
  styleUrl: "./custom-file-uploader.component.css"
})
export class CustomFileUploaderComponent implements OnInit {
  @ViewChild("uploader", { static: true }) private readonly filePickerRef!: FilePickerComponent;
  customAdapter!: CustomAdapter;
  readonly MAX_NUMBER_FILES = 10;
  readonly ALLOWED_FILE_EXTENSIONS = [".csv", ".docx", ".xlsx", ".parquet"];
  readonly NGX_AWESOME_UPLOADER_ALLOWED_FILE_EXTENSIONS = this.ALLOWED_FILE_EXTENSIONS.join(", ");
  readonly formData = model<FormData>(new FormData());

  ngOnInit() {
    this.customAdapter = new CustomAdapter(this.formData, this.ALLOWED_FILE_EXTENSIONS, this.filePickerRef);
  }

  onUploadSuccess(event: FilePreviewModel) {
    console.log(`Successfully Uploaded: "${(event.file as File).name}"`);
  }

  onValidationError(error: ValidationError) {
    alert(`Validation Error [${error.error}]: ${error.file.name}`);
    this.filePickerRef.removeFileFromList({ file: error.file, fileName: error.file.name });
  }
}
