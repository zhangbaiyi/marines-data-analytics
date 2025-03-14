import { CommonModule } from "@angular/common";
import { Component, computed, input, OnDestroy, output } from "@angular/core";
import { toSignal } from "@angular/core/rxjs-interop";
import { FormArray, FormControl, FormsModule, ReactiveFormsModule } from "@angular/forms";
import { MatButtonModule } from "@angular/material/button";
import { MatFormFieldModule } from "@angular/material/form-field";
import { MatIconModule } from "@angular/material/icon";
import { MatSelectModule } from "@angular/material/select";
import { NgxMatSelectSearchModule } from "ngx-mat-select-search";
import { Subscription } from "rxjs";

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
  readonly filesInputArr = input<File[]>(Array.from<File>({ length: 5 }));
  readonly optionSearchTooltipMessage = "Select All / Unselect All" as const;
  readonly optionList = ["Extra Cheese", "Mushroom", "Onion", "Pepperoni", "Sausage", "Tomato"];
  readonly out = output<string[]>();

  readonly optionMultiselectControl = computed(() =>
    this.filesInputArr().map(() => new FormControl<string[]>([], { nonNullable: true }))
  );

  readonly optionSearchMultiselectFilterControl = computed(() =>
    this.filesInputArr().map(() => new FormControl<string>("", { nonNullable: true }))
  );

  private readonly backgroundOptionStateMap = new Map<string, string[]>();
  readonly optionSearchMulti = computed(
    () =>
      new FormArray<FormControl<string[]>>(
        this.filesInputArr().map(
          (file) => new FormControl<string[]>(this.backgroundOptionStateMap.get(file.name) ?? [], { nonNullable: true })
        )
      )
  );

  private readonly searchTermFilterKeyword = this.optionSearchMultiselectFilterControl().map((filterControl) =>
    toSignal(filterControl.valueChanges, {
      initialValue: filterControl.value
    })
  );

  readonly filteredOptionsMulti = computed(() =>
    this.searchTermFilterKeyword.map((searchSignal) => {
      const search = searchSignal(); // Extracting value from the signal
      console.log({ search });
      if (search.length === 0) {
        return this.optionList.slice();
      }

      const lowerCaseSearch = search.toLowerCase();
      console.log({ res: this.optionList.filter((option) => option.toLowerCase().includes(lowerCaseSearch)) });
      return this.optionList.filter((option) => option.toLowerCase().includes(lowerCaseSearch));
    })
  );

  ngOnDestroy() {
    for (const subscription of this.subscriptions) {
      subscription.unsubscribe();
    }
  }

  getAdditionalSelectMessage(index: number): string {
    if (this.optionMultiselectControl()[index].value.length <= 1) {
      return "";
    }

    const additionalSelections = this.optionMultiselectControl()[index].value.length - 1;
    let additionalSelectMsg = `+${additionalSelections} other`;
    if (additionalSelections > 1) {
      additionalSelectMsg = `${additionalSelectMsg}s`;
    }
    return `(${additionalSelectMsg})`;
  }

  toggleAllOptions(options: { hasSelectedAll: boolean }, index: number) {
    console.log(index);
    if (options.hasSelectedAll) {
      this.optionMultiselectControl()[index].setValue(this.optionList);
    } else {
      this.optionMultiselectControl()[index].setValue([]);
    }
  }

  printStateToConsole() {
    console.log({ filesInputArr: this.filesInputArr });
    console.log({ optionMultiselectControl: this.optionMultiselectControl().map((control) => control.value) });
    console.log({
      optionSearchMultiselectFilterControl: this.optionSearchMultiselectFilterControl().map((control) => control.value)
    });
  }
}
