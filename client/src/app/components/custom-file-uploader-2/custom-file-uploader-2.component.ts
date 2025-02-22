import { CommonModule } from "@angular/common";
import { Component } from "@angular/core";
import { FormControl, FormsModule, ReactiveFormsModule } from "@angular/forms";
import { MatFormFieldModule } from "@angular/material/form-field";
import { MatSelectModule } from "@angular/material/select";
import { FileUploadControl, FileUploadModule, FileUploadValidators, ValidatorFn } from "@iplab/ngx-file-upload";
import { IFileUploadControlConfiguration } from "@iplab/ngx-file-upload/lib/helpers/control.interface";

/**
 * REFERENCES: Ngx-File-Upload (File Uploader)
 * - https://www.npmjs.com/package/@iplab/ngx-file-upload?activeTab=readme
 * - https://pivan.github.io/file-upload/
 */
@Component({
  selector: "custom-file-uploader-2-ngx-file-upload-iplab",
  imports: [CommonModule, FormsModule, ReactiveFormsModule, MatSelectModule, MatFormFieldModule, FileUploadModule],
  templateUrl: "./custom-file-uploader-2.component.html",
  styleUrl: "./custom-file-uploader-2.component.css"
})
export class CustomFileUploader2Component {
  readonly fileUploadControlConfig: IFileUploadControlConfiguration = {
    listVisible: true,
    discardInvalid: true
  } as const;
  readonly MAX_NUMBER_FILES = 10;
  readonly ALLOWED_FILE_EXTENSIONS = [".csv", ".xlsx", ".parquet"];
  readonly fileUploadValidators: ValidatorFn | ValidatorFn[] = [
    FileUploadValidators.accept(this.ALLOWED_FILE_EXTENSIONS),
    FileUploadValidators.filesLimit(this.MAX_NUMBER_FILES)
  ];
  readonly fileUploadControl = new FileUploadControl(this.fileUploadControlConfig, this.fileUploadValidators);
  readonly toppings = new FormControl([]);
  readonly toppingList = ["Extra cheese", "Mushroom", "Onion", "Pepperoni", "Sausage", "Tomato"];

  constructor() {
    console.log("CustomFileUploader2Component constructor");
  }
}
