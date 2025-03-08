import { CommonModule } from "@angular/common";
import { Component, computed, OnDestroy } from "@angular/core";
import { toSignal } from "@angular/core/rxjs-interop";
import { FormControl, FormsModule, ReactiveFormsModule } from "@angular/forms";
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
  readonly optionSearchTooltipMessage = "Select All / Unselect All" as const;
  readonly optionList = ["Extra Cheese", "Mushroom", "Onion", "Pepperoni", "Sausage", "Tomato"];

  readonly optionMultiselectControl: FormControl<string[]> = new FormControl<string[]>([], { nonNullable: true });
  readonly optionSearchMultiselectFilterControl: FormControl<string> = new FormControl<string>("", {
    nonNullable: true
  });

  private readonly searchTermFilterKeyword = toSignal(this.optionSearchMultiselectFilterControl.valueChanges, {
    initialValue: this.optionSearchMultiselectFilterControl.value
  });
  readonly filteredOptionsMulti = computed(() => {
    const search = this.searchTermFilterKeyword();
    if (search.length === 0) {
      return this.optionList.slice();
    }

    const lowerCaseSearch = search.toLowerCase();
    return this.optionList.filter((option) => option.toLowerCase().includes(lowerCaseSearch));
  });

  ngOnDestroy() {
    for (const subscription of this.subscriptions) {
      subscription.unsubscribe();
    }
  }

  getAdditionalSelectMessage(): string {
    if (this.optionMultiselectControl.value.length <= 1) {
      return "";
    }

    const additionalSelections = this.optionMultiselectControl.value.length - 1;
    let additionalSelectMsg = `+${additionalSelections} other`;
    if (additionalSelections > 1) {
      additionalSelectMsg = `${additionalSelectMsg}s`;
    }
    additionalSelectMsg = `(${additionalSelectMsg})`;
    return additionalSelectMsg;
  }

  toggleAllOptions(options: { hasSelectedAll: boolean }) {
    if (options.hasSelectedAll) {
      this.optionMultiselectControl.setValue(this.optionList);
    } else {
      this.optionMultiselectControl.setValue([]);
    }
  }

  printStateToConsole() {
    console.log({ optionMultiselectControl: this.optionMultiselectControl.value });
    console.log({ optionSearchMultiselectFilterControl: this.optionSearchMultiselectFilterControl.value });
  }
}
