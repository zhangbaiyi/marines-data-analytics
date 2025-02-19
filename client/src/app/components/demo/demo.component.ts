import { CommonModule } from "@angular/common";
import { Component, OnDestroy, signal } from "@angular/core";
import { MatButtonModule } from "@angular/material/button";
import { MatCardModule } from "@angular/material/card";
import { Subscription } from "rxjs";

import { DEFAULT_DEMO_CONTENT, DemoContent } from "../../shared/models/demo.model";
import { DemoService } from "../../shared/services/demo.service";

@Component({
  selector: "app-demo",
  imports: [CommonModule, MatButtonModule, MatCardModule],
  templateUrl: "./demo.component.html",
  styleUrl: "./demo.component.css"
})
export class DemoComponent implements OnDestroy {
  private readonly subscriptions: Subscription[] = [];
  demoContent = signal<DemoContent>(DEFAULT_DEMO_CONTENT);
  selectedFiles = signal<File[]>([]);
  fileUploadState = signal<string>("");

  constructor(private readonly demoService: DemoService) {}

  ngOnDestroy() {
    for (const subscription of this.subscriptions) {
      subscription.unsubscribe();
    }
  }

  retriveContent() {
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
    if (!input.files || input.files.length === 0) {
      return;
    }

    for (let i = 0; i < input.files.length; i++) {
      const file = input.files.item(i);
      console.log({ file });
    }

    const files = Array.from(input.files).filter((file) => file.name.endsWith(".csv") || file.name.endsWith(".xlsx"));
    console.log("Files Length:", files.length);
    for (const file of files) {
      console.log({ file });
    }
    this.selectedFiles.set(files);
    console.log("Selected Files Length:", this.selectedFiles().length);

    if (files.length !== this.selectedFiles().length) {
      alert("Only CSV and XLSX files are allowed.");
    }
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

    const sub = this.demoService.testFileUpload(formData).subscribe((result) => {
      console.log({ result });
      this.fileUploadState.set(result);
    });
    this.subscriptions.push(sub);
  }
}
