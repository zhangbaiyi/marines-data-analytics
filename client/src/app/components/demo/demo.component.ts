import { CommonModule } from "@angular/common";
import { Component, inject, OnDestroy, signal } from "@angular/core";
import { FormArray, FormControl, FormsModule } from "@angular/forms";
import { MatButtonModule } from "@angular/material/button";
import { MatCardModule } from "@angular/material/card";
import { MatSelectModule } from "@angular/material/select";
import { Subscription } from "rxjs";
import { DemoService } from "src/app/shared/services/demo.service";

import { APP_CONFIG_TOKEN } from "../../../environments/app-config-env.token";
import { EnvironmentModel } from "../../../environments/environment.model";
import { FileUploaderComponent } from "../file-uploader/file-uploader.component";
import { PdfPreviewerComponent } from "../pdf-previewer/pdf-previewer.component";

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
  optionKeys = signal<File[]>([]);
  optionValues = signal<FormArray<FormControl<string[]>>>(new FormArray<FormControl<string[]>>([]));
  pdfSrcPathLink = signal("");

  private readonly subscriptions: Subscription[] = [];

  constructor(private readonly demoService: DemoService) {}

  ngOnDestroy() {
    for (const subscription of this.subscriptions) {
      subscription.unsubscribe();
    }
  }

  private convertOptionsToObject(): Record<string, string> {
    if (this.optionKeys().length !== this.optionValues().length) {
      alert("Error: Mismatch in PDF files and their respective selected options.");
      return {};
    }

    const optionsObj: Record<string, string> = {};
    for (const [idx, entry] of this.optionKeys().entries()) {
      optionsObj[entry.name] = this.optionValues().value[idx].toString();
    }

    return optionsObj;
  }

  retrievePdfFileLink() {
    console.log({ parentObjKeys: this.optionKeys(), parentObjValues: this.optionValues().value });
    const optionsObj = this.convertOptionsToObject();
    console.log({ optionsObj });
    const sub = this.demoService.testAPI(optionsObj).subscribe((pdfPath) => {
      const pdfPathResolvedToServer = `${this.environment.API_URL}/${pdfPath}`;
      this.pdfSrcPathLink.set(pdfPathResolvedToServer);
    });
    this.subscriptions.push(sub);
  }

  closePdf() {
    this.pdfSrcPathLink.set("");
  }
}
