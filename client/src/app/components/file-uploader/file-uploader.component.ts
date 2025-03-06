import { CommonModule } from "@angular/common";
import { Component, computed, effect, model, OnDestroy, signal } from "@angular/core";
import { FormArray, FormControl, FormsModule, ReactiveFormsModule } from "@angular/forms";
import { MatButtonModule } from "@angular/material/button";
import { MatFormFieldModule } from "@angular/material/form-field";
import { MatIconModule } from "@angular/material/icon";
import { MatSelectModule } from "@angular/material/select";
import { FileUploadControl, FileUploadModule, FileUploadValidators, ValidatorFn } from "@iplab/ngx-file-upload";
import { IFileUploadControlConfiguration } from "@iplab/ngx-file-upload/lib/helpers/control.interface";
import { Subscription } from "rxjs";

import { DemoService } from "../../shared/services/demo.service";

export type MappedFileOptions = { fileName: string; selectedOptions: FormControl<string[]> };

/**
 * REFERENCES: Ngx-File-Upload (File Uploader)
 * - https://www.npmjs.com/package/@iplab/ngx-file-upload?activeTab=readme
 * - https://pivan.github.io/file-upload/
 */
@Component({
  selector: "custom-file-uploader",
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
  templateUrl: "./file-uploader.component.html",
  styleUrl: "./file-uploader.component.css"
})
export class FileUploaderComponent implements OnDestroy {
  private readonly subscriptions: Subscription[] = [];
  private readonly backgroundOptionStateMap = new Map<string, string[]>();
  readonly foodItems = ["Pizza", "Burgers", "Fries", "Cookies", "Ice Cream"] as const;
  readonly MAX_NUMBER_FILES = 10;
  readonly ALLOWED_FILE_EXTENSIONS = [".csv", ".xlsx", ".parquet"];
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
  readonly optionsPerFile = computed(
    () =>
      new FormArray<FormControl<string[]>>(
        this.uploadedFiles().map(
          (file) => new FormControl<string[]>(this.backgroundOptionStateMap.get(file.name) ?? [], { nonNullable: true })
        )
      )
  );
  readonly optionEntries = model.required<MappedFileOptions[]>({
    alias: "optionEntries"
  });

  constructor(private readonly demoService: DemoService) {
    effect(() => {
      this.optionEntries.update(() => {
        const newOptionEntries: MappedFileOptions[] = [];
        for (const [idx, file] of this.uploadedFiles().entries()) {
          newOptionEntries.push({ fileName: file.name, selectedOptions: this.optionsPerFile().at(idx) });
        }
        return newOptionEntries;
      });
    });
  }

  ngOnDestroy() {
    for (const subscription of this.subscriptions) {
      subscription.unsubscribe();
    }
  }

  checkFileUploadIsDisabled() {
    const hasExceededFileLimit = this.fileUploadControl.value.length >= this.MAX_NUMBER_FILES;
    if (hasExceededFileLimit) {
      this.fileUploadControl.disable();
    }
    return hasExceededFileLimit;
  }

  getAdditionalSelectMessage(file: File, fileArrayIdx: number): string {
    const currFoodOptionValue = this.optionsPerFile().at(fileArrayIdx).value;
    let additionalSelectMsg = "";
    if (currFoodOptionValue.length > 1) {
      const additionalSelections = currFoodOptionValue.length - 1;
      additionalSelectMsg = `+${additionalSelections} other`;
      if (additionalSelections > 1) {
        additionalSelectMsg = `${additionalSelectMsg}s`;
      }
      additionalSelectMsg = `(${additionalSelectMsg})`;
    }
    this.addToBackgroundState(file.name, currFoodOptionValue);
    return additionalSelectMsg;
  }

  private addToBackgroundState(fileName: string, foodOptions: string[]) {
    this.backgroundOptionStateMap.set(fileName, foodOptions);
  }

  handleRemoveFile(file: File) {
    if (this.checkFileUploadIsDisabled()) {
      this.fileUploadControl.enable();
    }
    this.removeFromBackgroundState(file.name);
    this.fileUploadControl.removeFile(file);
  }

  private removeFromBackgroundState(fileName: string) {
    this.backgroundOptionStateMap.delete(fileName);
  }

  handleFileUpload() {
    if (this.uploadedFiles().length === 0) {
      alert("No files uploaded!");
      return;
    }

    const formData = new FormData();
    for (const file of this.uploadedFiles()) {
      formData.append("files", file);
    }

    const sub = this.demoService.uploadFilesToBE(formData).subscribe((fileUploadResult) => {
      console.log({ fileUploadResult });
    });
    this.subscriptions.push(sub);
  }

  getFileUploadControl() {
    console.log({ files: this.uploadedFiles() });
    console.log(this.fileUploadControl);
    console.log(this.fileUploadControl.value);
    console.log({ isDisabled: this.fileUploadControl.disabled });
  }
}
