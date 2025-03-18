import { CommonModule } from "@angular/common";
import { ChangeDetectionStrategy, Component, computed, effect, model, OnDestroy, output, signal } from "@angular/core";
import { FormArray, FormControl, FormsModule, ReactiveFormsModule } from "@angular/forms";
import { MatButtonModule } from "@angular/material/button";
import { DateAdapter, MAT_DATE_FORMATS, MAT_DATE_LOCALE, provideNativeDateAdapter } from "@angular/material/core";
import { MatDatepicker, MatDatepickerModule } from "@angular/material/datepicker";
import { MatFormFieldModule } from "@angular/material/form-field";
import { MatIconModule } from "@angular/material/icon";
import { MatInputModule } from "@angular/material/input";
import { MatSelectModule } from "@angular/material/select";
import { MAT_MOMENT_DATE_ADAPTER_OPTIONS, MomentDateAdapter } from "@angular/material-moment-adapter";
import { FileUploadControl, FileUploadModule, FileUploadValidators, ValidatorFn } from "@iplab/ngx-file-upload";
import { IFileUploadControlConfiguration } from "@iplab/ngx-file-upload/lib/helpers/control.interface";
import moment, { Moment } from "moment";
import { NgxMatSelectSearchModule } from "ngx-mat-select-search";
import { map, startWith, Subscription, tap } from "rxjs";

import { DEFAULT_FILE_OPTIONS } from "../../shared/models/demo.model";
import { DemoService } from "../../shared/services/demo.service";

export type MappedFileOptions = {
  fileName: string;
  selectedOptions: FormControl<string[]>;
  dateSelected: FormControl<Moment>;
};
export type FileUploaderOutputResult = { optionEntries: MappedFileOptions[]; optionEntries2: MappedFileOptions[] };

/**
 * REFERENCES: Angular Mat-Datepicker (Month and Year Only)
 * - https://stackblitz.com/angular/ekjvrbglvnb?file=src%2Fapp%2Fdatepicker-views-selection-example.ts
 */
const DATEPICKER_MONTH_YEAR_FORMAT = "MM/YYYY";
const FILE_UPLOAD_MAT_DATE_FORMATS = {
  parse: {
    dateInput: DATEPICKER_MONTH_YEAR_FORMAT
  },
  display: {
    dateInput: DATEPICKER_MONTH_YEAR_FORMAT,
    monthYearLabel: "MMM YYYY",
    dateA11yLabel: "LL",
    monthYearA11yLabel: "MMMM YYYY"
  }
};

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
    MatIconModule,
    MatInputModule,
    MatDatepickerModule
  ],
  providers: [
    provideNativeDateAdapter(),
    {
      provide: DateAdapter,
      useClass: MomentDateAdapter,
      deps: [MAT_DATE_LOCALE, MAT_MOMENT_DATE_ADAPTER_OPTIONS]
    },
    {
      provide: MAT_DATE_FORMATS,
      useValue: FILE_UPLOAD_MAT_DATE_FORMATS
    }
  ],
  templateUrl: "./file-uploader.component.html",
  styleUrl: "./file-uploader.component.css",
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class FileUploaderComponent implements OnDestroy {
  private readonly subscriptions: Subscription[] = [];
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
            new FormControl<string[]>(
              this.demoService.backgroundOptionStateMapSingle.get(file.name)?.dataAssocations ?? [],
              {
                nonNullable: true
              }
            )
        )
      )
  );
  readonly optionsDatePerFileMonthSingle = computed(
    () =>
      new FormArray<FormControl<Moment>>(
        this.uploadedFiles().map(
          (file) =>
            new FormControl<Moment>(
              this.demoService.backgroundOptionStateMapSingle.get(file.name)?.dateSelected ?? moment(),
              {
                nonNullable: true
              }
            )
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

  readonly optionSearchTooltipMessage = "Select All / Unselect All" as const;
  readonly optionPerFileMultiselect = computed(
    () =>
      new FormArray<FormControl<string[]>>(
        this.uploadedFiles().map(
          (file) =>
            new FormControl<string[]>(
              this.demoService.backgroundOptionStateMapMultiple.get(file.name)?.dataAssocations ?? [],
              {
                nonNullable: true
              }
            )
        )
      )
  );

  readonly optionsDatePerFileMonthMultiple = computed(
    () =>
      new FormArray<FormControl<Moment>>(
        this.uploadedFiles().map(
          (file) =>
            new FormControl<Moment>(
              this.demoService.backgroundOptionStateMapMultiple.get(file.name)?.dateSelected ?? moment(),
              {
                nonNullable: true
              }
            )
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
        tap((search) => console.log("New Search Value:", search)),
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

  toggleAllOptions(options: { hasSelectedAll: boolean }, index: number) {
    const { hasSelectedAll } = options;
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

  addToBackgroundState2(fileName: string, fileAssociations: string[]) {
    this.demoService.backgroundOptionStateMapSingle.set(fileName, {
      ...(this.demoService.backgroundOptionStateMapSingle.get(fileName) ?? Object.assign({}, DEFAULT_FILE_OPTIONS)),
      dataAssocations: fileAssociations
    });
  }

  addDateToBackgroundState2(fileName: string, date: Moment) {
    this.demoService.backgroundOptionStateMapMultiple.set(fileName, {
      ...(this.demoService.backgroundOptionStateMapMultiple.get(fileName) ?? Object.assign({}, DEFAULT_FILE_OPTIONS)),
      dateSelected: date
    });
  }

  removeFromBackgroundState2(fileName: string) {
    this.demoService.backgroundOptionStateMapMultiple.delete(fileName);
  }

  handleDateChosenYearMultiple(normalizedYear: Moment, dateFormControlIdx: number) {
    const ctrlValue = this.optionsDatePerFileMonthMultiple().at(dateFormControlIdx).value;
    ctrlValue.year(normalizedYear.year());
    this.optionsDatePerFileMonthMultiple().at(dateFormControlIdx).setValue(ctrlValue);
  }

  handleDateChosenMonthMultiple(
    normalizedMonth: Moment,
    dateFormControlIdx: number,
    fileName: string,
    datepicker: MatDatepicker<Moment>
  ) {
    const ctrlValue = this.optionsDatePerFileMonthMultiple().at(dateFormControlIdx).value;
    ctrlValue.month(normalizedMonth.month());
    this.optionsDatePerFileMonthMultiple().at(dateFormControlIdx).setValue(ctrlValue);
    console.log({ dateMultiple: ctrlValue.format("YYYYMM") });
    this.addDateToBackgroundState2(fileName, ctrlValue);
    datepicker.close();
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
        optionEntries.push({
          fileName: file.name,
          selectedOptions: this.optionsPerFile().at(idx),
          dateSelected: this.optionsDatePerFileMonthSingle().at(idx)
        });
        optionEntries2.push({
          fileName: file.name,
          selectedOptions: this.optionPerFileMultiselect().at(idx),
          dateSelected: this.optionsDatePerFileMonthMultiple().at(idx)
        });
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

  addToBackgroundState(fileName: string, fileAssociations: string[]) {
    this.demoService.backgroundOptionStateMapSingle.set(fileName, {
      ...(this.demoService.backgroundOptionStateMapSingle.get(fileName) ?? Object.assign({}, DEFAULT_FILE_OPTIONS)),
      dataAssocations: fileAssociations
    });
  }

  addDateToBackgroundState(fileName: string, date: Moment) {
    this.demoService.backgroundOptionStateMapSingle.set(fileName, {
      ...(this.demoService.backgroundOptionStateMapSingle.get(fileName) ?? Object.assign({}, DEFAULT_FILE_OPTIONS)),
      dateSelected: date
    });
  }

  removeFromBackgroundState(fileName: string) {
    this.demoService.backgroundOptionStateMapSingle.delete(fileName);
  }

  handleRemoveFile(file: File) {
    if (this.checkFileUploadIsDisabled()) {
      this.fileUploadControl.enable();
    }
    this.removeFromBackgroundState(file.name);
    this.removeFromBackgroundState2(file.name);
    this.fileUploadControl.removeFile(file);
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

  handleDateChosenYearSingle(normalizedYear: Moment, dateFormControlIdx: number) {
    const ctrlValue = this.optionsDatePerFileMonthSingle().at(dateFormControlIdx).value;
    ctrlValue.year(normalizedYear.year());
    this.optionsDatePerFileMonthSingle().at(dateFormControlIdx).setValue(ctrlValue);
  }

  handleDateChosenMonthSingle(
    normalizedMonth: Moment,
    dateFormControlIdx: number,
    fileName: string,
    datepicker: MatDatepicker<Moment>
  ) {
    const ctrlValue = this.optionsDatePerFileMonthSingle().at(dateFormControlIdx).value;
    ctrlValue.month(normalizedMonth.month());
    this.optionsDatePerFileMonthSingle().at(dateFormControlIdx).setValue(ctrlValue);
    console.log({ dateSingle: ctrlValue.format("YYYYMM") });
    this.addDateToBackgroundState(fileName, ctrlValue);
    datepicker.close();
  }

  getFileUploadControl() {
    console.log({ files: this.uploadedFiles() });
    console.log(this.fileUploadControl);
    console.log(this.fileUploadControl.value);
    console.log({ isDisabled: this.fileUploadControl.disabled });
    console.log({
      mapSingle: this.demoService.backgroundOptionStateMapSingle,
      mapMultiple: this.demoService.backgroundOptionStateMapMultiple
    });
    console.log({ optionsPerFile: this.optionsPerFile(), optionsPerFileMultiselect: this.optionPerFileMultiselect() });
    console.log({
      singleFileMonthControls: this.optionsDatePerFileMonthSingle().controls.map((ctrl) =>
        // eslint-disable-next-line @typescript-eslint/no-unsafe-return
        ctrl.value.format(DATEPICKER_MONTH_YEAR_FORMAT)
      )
    });
    console.log({
      multipleFileMonthControls: this.optionsDatePerFileMonthMultiple().controls.map((ctrl) =>
        // eslint-disable-next-line @typescript-eslint/no-unsafe-return
        ctrl.value.format(DATEPICKER_MONTH_YEAR_FORMAT)
      )
    });
  }
}
