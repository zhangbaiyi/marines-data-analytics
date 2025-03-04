import { CommonModule } from "@angular/common";
import { Component, Input, Output, EventEmitter } from "@angular/core";
import { MatButtonModule } from "@angular/material/button";
import { MatIconModule } from "@angular/material/icon";
import { PDFDocumentProxy, PdfViewerModule } from "ng2-pdf-viewer";

@Component({
  selector: "pdf-previewer",
  standalone: true,
  imports: [CommonModule, MatButtonModule, MatIconModule, PdfViewerModule],
  templateUrl: "./pdf-previewer.component.html",
  styleUrl: "./pdf-previewer.component.css",
})
export class PdfPreviewerComponent {
  @Input() pdfSrcPathLink: string = ""; // Regular string instead of signal<string>
  @Output() pdfSrcPathLinkChange = new EventEmitter<string>();

  private readonly ZOOM_CHANGE = 0.1;
  pdfPage: number = 1;
  pdfTotalPages: number = 0; // Now writable
  pdfZoom: number = 1;

  afterLoadComplete(pdf: PDFDocumentProxy) {
    this.pdfTotalPages = pdf.numPages;
  }

  zoomIn() {
    this.pdfZoom += this.ZOOM_CHANGE;
  }

  zoomOut() {
    if (this.pdfZoom > 0.5) {
      this.pdfZoom -= this.ZOOM_CHANGE;
    }
  }

  closePdf() {
    this.pdfSrcPathLink = "";
    this.pdfSrcPathLinkChange.emit(this.pdfSrcPathLink);
  }
}
