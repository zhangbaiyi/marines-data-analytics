import { CommonModule } from "@angular/common";
import { Component, inject, OnDestroy, signal } from "@angular/core";
import { MatButtonModule } from "@angular/material/button";
import { MatCardModule } from "@angular/material/card";
import { Subscription } from "rxjs";

import { APP_CONFIG_TOKEN } from "../../../environments/app-config-env.token";
import { EnvironmentModel } from "../../../environments/environment.model";
import { DEFAULT_DEMO_CONTENT, DemoContent } from "../../shared/models/demo.model";
import { DemoService } from "../../shared/services/demo.service";
import { PdfPreviewerComponent } from "../pdf-previewer/pdf-previewer.component";

@Component({
  selector: "app-demo",
  imports: [CommonModule, MatButtonModule, MatCardModule, PdfPreviewerComponent],
  templateUrl: "./demo.component.html",
  styleUrl: "./demo.component.css"
})
export class DemoComponent implements OnDestroy {
  private readonly environment = inject<EnvironmentModel>(APP_CONFIG_TOKEN);
  private readonly subscriptions: Subscription[] = [];
  private readonly acceptedFileExtensionTypes = [".csv", ".xlsx", ".docx", ".parquet"];
  readonly acceptedFileExtensionTypesString = this.acceptedFileExtensionTypes.join(", ");
  readonly demoContent = signal<DemoContent>(DEFAULT_DEMO_CONTENT);
  readonly selectedFiles = signal<File[]>([]);
  readonly fileUploadState = signal<string>("");
  readonly pdfSrcPathLink = signal<string>("");

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
    const files = Array.from(input.files).filter((file) =>
      this.acceptedFileExtensionTypes.some((fileExtension) => file.name.endsWith(fileExtension))
    );
    const afterFilesCheckLength = files.length;

    if (prevFilesCheckLength !== afterFilesCheckLength) {
      alert("Only CSV, XLSX, DOCX, and PARQUET files are allowed.");
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
      const pdfPathResolvedToServer = `${this.environment.API_URL}/${pdfPathObj}`;
      this.pdfSrcPathLink.set(pdfPathResolvedToServer);
    });
    this.subscriptions.push(sub);
  }

  closePdf() {
    this.pdfSrcPathLink.set("");
  }
}
