import { CommonModule } from "@angular/common";
import { Component, inject, OnDestroy, signal } from "@angular/core";
import { MatButtonModule } from "@angular/material/button";
import { MatCardModule } from "@angular/material/card";
import { MatTabsModule } from "@angular/material/tabs";
import { PdfViewerModule } from "ng2-pdf-viewer";
import { Subscription } from "rxjs";

import { APP_CONFIG_TOKEN } from "../../../environments/app-config-env.token";
import { EnvironmentModel } from "../../../environments/environment.model";
import { DEFAULT_DEMO_CONTENT, DemoContent } from "../../shared/models/demo.model";
import { DemoService } from "../../shared/services/demo.service";
import { PdfPreviewerComponent } from "../pdf-previewer/pdf-previewer.component";

@Component({
  selector: "app-demo",
  imports: [CommonModule, MatButtonModule, MatCardModule, MatTabsModule, PdfViewerModule, PdfPreviewerComponent],
  templateUrl: "./demo.component.html",
  styleUrl: "./demo.component.css"
})
export class DemoComponent implements OnDestroy {
  private readonly environment = inject<EnvironmentModel>(APP_CONFIG_TOKEN);
  private readonly subscriptions: Subscription[] = [];
  demoContent = signal<DemoContent>(DEFAULT_DEMO_CONTENT);
  selectedFiles = signal<File[]>([]);
  fileUploadState = signal<string>("");
  pdfSrcPathLink = signal<string>("");

  constructor(private readonly demoService: DemoService) {}

  ngOnDestroy() {
    for (const subscription of this.subscriptions) {
      subscription.unsubscribe();
    }
  }

  retrieveContent() {
    const sub = this.demoService.getFilledCard().subscribe((content) => {
      this.demoContent.set(content);
    });
    this.subscriptions.push(sub);
  }

  clearContent() {
    const sub = this.demoService.clearContent().subscribe((content) => {
      this.demoContent.set(content);
    });
    this.subscriptions.push(sub);
  }

  onFileSelected(event: Event) {
    const input = event.target as HTMLInputElement;
    if (input.files == null || input.files.length === 0) {
      return;
    }

    const prevFilesCheckLength = input.files.length;
    const files = Array.from(input.files).filter(
      (file) => file.name.endsWith(".csv") || file.name.endsWith(".xlsx") || file.name.endsWith(".parquet")
    );
    const afterFilesCheckLength = files.length;

    if (prevFilesCheckLength !== afterFilesCheckLength) {
      alert("Only CSV and XLSX files are allowed.");
    }
    this.selectedFiles.set(files);
  }

  onFileUpload() {
    if (this.selectedFiles().length === 0) {
      alert("No files selected.");
      return;
    }

    const formData = new FormData();
    for (const file of this.selectedFiles()) {
      formData.append("files", file);
    }

    const sub = this.demoService.testFileUpload(formData).subscribe((fileUploadResult) => {
      this.fileUploadState.set(fileUploadResult);
    });
    this.subscriptions.push(sub);
  }

  retrievePdfFileLink() {
    const sub = this.demoService.getPdfFileLinkFromServer().subscribe((pdfPathObj) => {
      console.log({ pdfPathObj });
      const pdfPath = pdfPathObj.file_name;
      const pdfPathResolvedToServer = `${this.environment.API_URL}/${pdfPath}`;
      this.pdfSrcPathLink.set(pdfPathResolvedToServer);
    });
    this.subscriptions.push(sub);
  }

  closePdf() {
    this.pdfSrcPathLink.set("");
  }
}
