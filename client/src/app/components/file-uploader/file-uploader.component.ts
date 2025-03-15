import { CommonModule } from "@angular/common";
import { Component, computed, effect, model, OnDestroy, output, signal } from "@angular/core";
import { FormArray, FormControl, FormsModule, ReactiveFormsModule } from "@angular/forms";
import { MatButtonModule } from "@angular/material/button";
import { MatFormFieldModule } from "@angular/material/form-field";
import { MatIconModule } from "@angular/material/icon";
import { MatSelectModule } from "@angular/material/select";
import { FileUploadControl, FileUploadModule, FileUploadValidators, ValidatorFn } from "@iplab/ngx-file-upload";
import { IFileUploadControlConfiguration } from "@iplab/ngx-file-upload/lib/helpers/control.interface";
import { NgxMatSelectSearchModule } from "ngx-mat-select-search";
import { map, startWith, Subscription } from "rxjs";

import { DemoService } from "../../shared/services/demo.service";

export type MappedFileOptions = { fileName: string; selectedOptions: FormControl<string[]> };
export type FileUploaderOutputResult = { optionEntries: MappedFileOptions[]; optionEntries2: MappedFileOptions[] };

/**
 * REFERENCES: Ngx-File-Upload (File Uploader)
 * - https://www.npmjs.com/package/@iplab/ngx-file-upload?activeTab=readme
 * - https://pivan.github.io/file-upload/
 */
/**
 * REFERENCES: Ngx-Mat-Select-Search
 * - https://www.npmjs.com/package/ngx-mat-select-search
 * - https://stackblitz.com/github/bithost-gmbh/ngx-mat-select-search-example
 */
@Component({
  selector: "custom-file-uploader",
  imports: [
    CommonModule,
    FileUploadModule,
    FormsModule,
    ReactiveFormsModule,
    MatFormFieldModule,
    NgxMatSelectSearchModule,
    MatSelectModule,
    MatButtonModule,
    MatIconModule
  ],
  templateUrl: "./file-uploader.component.html",
  styleUrl: "./file-uploader.component.css"
})
export class FileUploaderComponent implements OnDestroy {
  private readonly subscriptions: Subscription[] = [];
  private readonly backgroundOptionStateMapSingle = new Map<string, string[]>();
  readonly optionsList = ["Pizza", "Burgers", "Fries", "Cookies", "Ice Cream"];
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
          (file) =>
            new FormControl<string[]>(this.backgroundOptionStateMapSingle.get(file.name) ?? [], { nonNullable: true })
        )
      )
  );
  readonly optionEntries = model.required<MappedFileOptions[]>({
    alias: "optionEntries"
  });

  /**
   * Multi-Option Settings
   */
  readonly optionEntriesNgxMatSelectSearch = model.required<MappedFileOptions[]>({
    alias: "optionEntries2"
  });
  readonly optionChange = output<FileUploaderOutputResult>();

  private readonly backgroundOptionStateMapMultiple = new Map<string, string[]>();
  readonly optionSearchTooltipMessage = "Select All / Unselect All" as const;
  readonly optionPerFileMultiselect = computed(
    () =>
      new FormArray<FormControl<string[]>>(
        this.uploadedFiles().map(
          (file) =>
            new FormControl<string[]>(this.backgroundOptionStateMapMultiple.get(file.name) ?? [], { nonNullable: true })
        )
      )
  );

  readonly optionSearchMultiselectFilterControl = computed(() =>
    this.uploadedFiles().map(() => new FormControl<string>("", { nonNullable: true }))
  );

  readonly filteredOptionsMulti = computed(() =>
    this.optionSearchMultiselectFilterControl().map((control) =>
      control.valueChanges.pipe(
        startWith(""),
        map((search) => {
          if (search.length === 0) {
            return this.optionsList.slice();
          }
          const lowerCaseSearch = search.toLowerCase();
          return this.optionsList.filter((option) => option.toLowerCase().startsWith(lowerCaseSearch));
        })
      )
    )
  );

  toggleAllOptions(options: { hasSelectedAll: boolean; index: number }) {
    const { hasSelectedAll, index } = options;
    if (hasSelectedAll) {
      this.optionPerFileMultiselect().controls[index].setValue(this.optionsList);
    } else {
      this.optionPerFileMultiselect().controls[index].setValue([]);
    }
  }

  getAdditionalSelectMessage2(file: File, index: number): string {
    const currentOptionValue = this.optionPerFileMultiselect().controls[index].value;
    let additionalSelectMsg = "";
    if (currentOptionValue.length > 1) {
      const additionalSelections = currentOptionValue.length - 1;
      additionalSelectMsg = `+${additionalSelections} other`;
      if (additionalSelections > 1) {
        additionalSelectMsg = `${additionalSelectMsg}s`;
      }
      additionalSelectMsg = `(${additionalSelectMsg})`;
    }
    this.addToBackgroundState2(file.name, currentOptionValue);
    return additionalSelectMsg;
  }

  private addToBackgroundState2(fileName: string, options: string[]) {
    this.backgroundOptionStateMapMultiple.set(fileName, options);
  }

  private removeFromBackgroundState2(fileName: string) {
    this.backgroundOptionStateMapMultiple.delete(fileName);
  }

  // END OF MULTI-OPTION SETTINGS

  constructor(private readonly demoService: DemoService) {
    // effect(() => {
    //   this.optionEntries.update(() => {
    //     const newOptionEntries: MappedFileOptions[] = [];
    //     for (const [idx, file] of this.uploadedFiles().entries()) {
    //       newOptionEntries.push({ fileName: file.name, selectedOptions: this.optionsPerFile().at(idx) });
    //     }
    //     return newOptionEntries;
    //   });
    // });

    // // FOR MULTI-OPTION SETTINGS

    // effect(() => {
    //   this.optionEntriesNgxMatSelectSearch.update(() => {
    //     const newOptionEntries: MappedFileOptions[] = [];
    //     for (const [idx, file] of this.uploadedFiles().entries()) {
    //       newOptionEntries.push({
    //         fileName: file.name,
    //         selectedOptions: this.optionPerFileMultiselect().controls[idx]
    //       });
    //     }
    //     return newOptionEntries;
    //   });
    // });

    // // END OF MULTI-OPTION SETTINGS

    effect(() => {
      const optionEntries: MappedFileOptions[] = [];
      const optionEntries2: MappedFileOptions[] = [];

      for (const [idx, file] of this.uploadedFiles().entries()) {
        optionEntries.push({ fileName: file.name, selectedOptions: this.optionsPerFile().at(idx) });
        optionEntries2.push({ fileName: file.name, selectedOptions: this.optionPerFileMultiselect().controls[idx] });
      }

      this.optionChange.emit({ optionEntries, optionEntries2 });
    });
  }

  ngOnDestroy() {
    for (const subscription of this.subscriptions) {
      subscription.unsubscribe();
    }
  }

  checkFileUploadIsDisabled(): boolean {
    const hasExceededFileLimit = this.fileUploadControl.value.length >= this.MAX_NUMBER_FILES;
    if (hasExceededFileLimit) {
      this.fileUploadControl.disable();
    }
    return hasExceededFileLimit;
  }

  getAdditionalSelectMessage(file: File, fileArrayIdx: number): string {
    const currOptionValue = this.optionsPerFile().at(fileArrayIdx).value;
    let additionalSelectMsg = "";
    if (currOptionValue.length > 1) {
      const additionalSelections = currOptionValue.length - 1;
      additionalSelectMsg = `+${additionalSelections} other`;
      if (additionalSelections > 1) {
        additionalSelectMsg = `${additionalSelectMsg}s`;
      }
      additionalSelectMsg = `(${additionalSelectMsg})`;
    }
    this.addToBackgroundState(file.name, currOptionValue);
    return additionalSelectMsg;
  }

  private addToBackgroundState(fileName: string, options: string[]) {
    this.backgroundOptionStateMapSingle.set(fileName, options);
  }

  handleRemoveFile(file: File) {
    if (this.checkFileUploadIsDisabled()) {
      this.fileUploadControl.enable();
    }
    this.removeFromBackgroundState(file.name);
    this.removeFromBackgroundState2(file.name);
    this.fileUploadControl.removeFile(file);
  }

  private removeFromBackgroundState(fileName: string) {
    this.backgroundOptionStateMapSingle.delete(fileName);
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
    console.log({ mapSingle: this.backgroundOptionStateMapSingle, mapMultiple: this.backgroundOptionStateMapMultiple });
    console.log({ optionsPerFile: this.optionsPerFile(), optionsPerFileMultiselect: this.optionPerFileMultiselect() });
  }
}
