import { CommonModule } from "@angular/common";
import { Component, inject, OnDestroy, signal } from "@angular/core";
import { FormsModule } from "@angular/forms";
import { MatButtonModule } from "@angular/material/button";
import { MatCardModule } from "@angular/material/card";
import { MatSelectModule } from "@angular/material/select";
import { Subscription } from "rxjs";
import { DemoService } from "src/app/shared/services/demo.service";

import { APP_CONFIG_TOKEN } from "../../../environments/app-config-env.token";
import { EnvironmentModel } from "../../../environments/environment.model";
import { FileUploaderComponent, MappedFileOptions } from "../file-uploader/file-uploader.component";
import { PdfPreviewerComponent } from "../pdf-previewer/pdf-previewer.component";

export type MappedFileOptionsFlattened = { fileName: string; selectedOptions: string };

@Component({
  selector: "app-demo",
  imports: [
    CommonModule,
    FormsModule,
    MatButtonModule,
    MatCardModule,
    MatSelectModule,
    PdfPreviewerComponent,
    FileUploaderComponent
  ],
  templateUrl: "./demo.component.html",
  styleUrl: "./demo.component.css"
})
export class DemoComponent implements OnDestroy {
  private readonly environment = inject<EnvironmentModel>(APP_CONFIG_TOKEN);
  private readonly subscriptions: Subscription[] = [];
  readonly optionEntries = signal<MappedFileOptions[]>([]);
  readonly optionEntriesNgxMatSelectSearch = signal<MappedFileOptions[]>([]);
  readonly pdfSrcPathLink = signal("");

  constructor(private readonly demoService: DemoService) {}

  ngOnDestroy() {
    for (const subscription of this.subscriptions) {
      subscription.unsubscribe();
    }
  }

  private convertOptionsToJSONObject(options: MappedFileOptions[]): MappedFileOptionsFlattened[] {
    if (options.length === 0) {
      alert("Error: No selected file options found.");
      return [];
    }

    const newOptionsArr: MappedFileOptionsFlattened[] = [];
    for (const { fileName, selectedOptions } of options) {
      newOptionsArr.push({ fileName, selectedOptions: selectedOptions.value.toString() });
    }
    return newOptionsArr;
  }

  retrievePdfFileLink() {
    console.log({ optionEntries: this.optionEntries() });
    const optionEntriesFlattened = this.convertOptionsToJSONObject(this.optionEntries());
    console.log({ optionEntriesFlattened });

    console.log({ optionEntriesNgxMatSelectSearch: this.optionEntriesNgxMatSelectSearch() });
    const optionEntriesFlattened2 = this.convertOptionsToJSONObject(this.optionEntriesNgxMatSelectSearch());
    console.log({ optionEntriesFlattened2 });

    const sub = this.demoService.testAPI(optionEntriesFlattened).subscribe((pdfPath) => {
      const pdfPathResolvedToServer = `${this.environment.API_URL}/${pdfPath}`;
      this.pdfSrcPathLink.set(pdfPathResolvedToServer);
    });
    this.subscriptions.push(sub);
  }

  closePdf() {
    this.pdfSrcPathLink.set("");
  }
}
