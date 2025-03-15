import { CommonModule } from "@angular/common";
import { Component, computed, effect, input, OnDestroy, output } from "@angular/core";
import { FormArray, FormControl, FormsModule, ReactiveFormsModule } from "@angular/forms";
import { MatButtonModule } from "@angular/material/button";
import { MatFormFieldModule } from "@angular/material/form-field";
import { MatIconModule } from "@angular/material/icon";
import { MatSelectModule } from "@angular/material/select";
import { NgxMatSelectSearchModule } from "ngx-mat-select-search";
import { map, startWith, Subscription } from "rxjs";

export type MappedFileOptions = { fileName: string; selectedOptions: FormControl<string[]> };
export type FileUploaderOutputResult = { optionEntries: MappedFileOptions[] };

/**
 * REFERENCES: Ngx-Mat-Select-Search
 * - https://www.npmjs.com/package/ngx-mat-select-search
 * - https://stackblitz.com/github/bithost-gmbh/ngx-mat-select-search-example
 */
@Component({
  selector: "custom-search-with-mat-select",
  imports: [
    CommonModule,
    FormsModule,
    ReactiveFormsModule,
    MatFormFieldModule,
    MatSelectModule,
    NgxMatSelectSearchModule,
    MatButtonModule,
    MatIconModule
  ],
  templateUrl: "./custom-search-with-mat-select.component.html",
  styleUrl: "./custom-search-with-mat-select.component.css"
})
export class CustomSearchWithMatSelectComponent implements OnDestroy {
  private readonly subscriptions: Subscription[] = [];
  readonly filesInputArr = input<File[]>(
    Array.from<File>({ length: 5 }).map((_, index) => new File([], `File ${index}`))
  ); // Or can be any other signal input (as reference)
  private readonly backgroundOptionStateMap = new Map<string, string[]>();
  readonly optionSearchTooltipMessage = "Select All / Unselect All" as const;
  readonly optionsList = ["Extra Cheese", "Mushroom", "Onion", "Pepperoni", "Sausage", "Tomato"];
  readonly out = output<FileUploaderOutputResult>();

  readonly optionPerFileMultiselect = computed(
    () =>
      new FormArray<FormControl<string[]>>(
        this.filesInputArr().map(
          (file) => new FormControl<string[]>(this.backgroundOptionStateMap.get(file.name) ?? [], { nonNullable: true })
        )
      )
  );

  readonly optionSearchMultiselectFilterControl = computed(() =>
    this.filesInputArr().map(() => new FormControl<string>("", { nonNullable: true }))
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

  constructor() {
    effect(() => {
      const optionEntries: MappedFileOptions[] = [];

      for (const [idx, file] of this.filesInputArr().entries()) {
        optionEntries.push({ fileName: file.name, selectedOptions: this.optionPerFileMultiselect().controls[idx] });
      }

      this.out.emit({ optionEntries });
    });
  }

  ngOnDestroy() {
    for (const subscription of this.subscriptions) {
      subscription.unsubscribe();
    }
  }

  getAdditionalSelectMessage(index: number): string {
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
    return additionalSelectMsg;
  }

  toggleAllOptions(options: { hasSelectedAll: boolean; index: number }) {
    const { hasSelectedAll, index } = options;
    if (hasSelectedAll) {
      this.optionPerFileMultiselect().controls[index].setValue(this.optionsList);
    } else {
      this.optionPerFileMultiselect().controls[index].setValue([]);
    }
  }

  printStateToConsole() {
    console.log({ filesInputArr: this.filesInputArr });
    console.log({ optionMultiselectControl: this.optionPerFileMultiselect().controls.map((control) => control.value) });
    console.log({
      optionSearchMultiselectFilterControl: this.optionSearchMultiselectFilterControl().map((control) => control.value)
    });
  }
}
