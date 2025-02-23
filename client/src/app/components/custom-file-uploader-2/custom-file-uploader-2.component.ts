import { CommonModule } from "@angular/common";
import { Component, signal } from "@angular/core";
import { FormControl, FormsModule, ReactiveFormsModule } from "@angular/forms";
import { MatButtonModule } from "@angular/material/button";
import { MatFormFieldModule } from "@angular/material/form-field";
import { MatIconModule } from "@angular/material/icon";
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
  imports: [
    CommonModule,
    FileUploadModule,
    FormsModule,
    ReactiveFormsModule,
    MatFormFieldModule,
    MatSelectModule,
    MatButtonModule,
    MatIconModule
  ],
  templateUrl: "./custom-file-uploader-2.component.html",
  styleUrl: "./custom-file-uploader-2.component.css"
})
export class CustomFileUploader2Component {
  readonly MAX_NUMBER_FILES = 10;
  readonly ALLOWED_FILE_EXTENSIONS = [".csv", ".xlsx", ".docx", ".parquet"];
  readonly fileUploadValidators: ValidatorFn | ValidatorFn[] = [
    FileUploadValidators.accept(this.ALLOWED_FILE_EXTENSIONS),
    FileUploadValidators.filesLimit(this.MAX_NUMBER_FILES)
  ];
  readonly fileUploadControlConfig: IFileUploadControlConfiguration = {
    multiple: true,
    listVisible: true,
    discardInvalid: true
  } as const;
  readonly uploadedFiles = signal<File[]>([]);
  readonly fileUploadControl = new FileUploadControl(this.fileUploadControlConfig, this.fileUploadValidators);
  readonly toppings = new FormControl<string[]>([]);
  readonly toppingList = ["Extra Cheese", "Mushroom", "Onion", "Pepperoni", "Sausage", "Tomato"] as const;

  getAdditionalSelectMessage(): string {
    if (this.toppings.value == null || this.toppings.value.length <= 1) {
      return "";
    }

    const additionalSelections = this.toppings.value.length - 1;
    let additionalSelectMsg = `+${additionalSelections} other`;
    if (additionalSelections > 1) {
      additionalSelectMsg = `${additionalSelectMsg}s`;
    }
    additionalSelectMsg = `(${additionalSelectMsg})`;
    return additionalSelectMsg;
  }

  checkFileUploadIsDisabled() {
    const hasExceededFileLimit = this.fileUploadControl.value.length >= this.MAX_NUMBER_FILES;
    if (hasExceededFileLimit) {
      this.fileUploadControl.disable();
    }
    return hasExceededFileLimit;
  }

  handleRemoveFile(file: File) {
    if (this.checkFileUploadIsDisabled()) {
      this.fileUploadControl.enable();
    }
    this.fileUploadControl.removeFile(file);
  }

  getFileUploadControl() {
    console.log(this.fileUploadControl);
    console.log(this.fileUploadControl.value);
    console.log({ isDisabled: this.fileUploadControl.disabled });
  }
}
