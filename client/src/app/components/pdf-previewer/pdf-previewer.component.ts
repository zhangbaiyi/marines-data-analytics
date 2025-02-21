import { CommonModule } from "@angular/common";
import { Component, computed, model, signal, ViewChild } from "@angular/core";
import { MatButtonModule } from "@angular/material/button";
import { MatCardModule } from "@angular/material/card";
import { MatIconModule } from "@angular/material/icon";
import { PDFDocumentProxy, PdfViewerComponent, PdfViewerModule } from "ng2-pdf-viewer";

@Component({
  selector: "pdf-previewer",
  imports: [CommonModule, MatButtonModule, MatCardModule, MatIconModule, PdfViewerModule],
  templateUrl: "./pdf-previewer.component.html",
  styleUrl: "./pdf-previewer.component.css"
})
export class PdfPreviewerComponent {
  @ViewChild(PdfViewerComponent)
  private readonly pdfComponent!: PdfViewerComponent;
  private readonly ZOOM_CHANGE = 0.1;
  private readonly PAGE_CHANGE = 1;
  // readonly pdfSrcPathLink = "http://localhost:3000/api/final/files/PDF_Test.pdf";
  readonly pdfSrcPathLink = model.required<string>();
  isPreviewerDisabled = computed(() => this.pdfSrcPathLink().length === 0);
  pdfPage = signal<number>(1);
  pdfTotalPages = signal<number>(0);
  pdfZoom = signal<number>(1);
  hasLoadedInInitally = signal<boolean>(false);

  afterLoadComplete(pdf: PDFDocumentProxy) {
    this.pdfTotalPages.set(pdf.numPages);
  }

  search(stringToSearch: string) {
    this.pdfComponent.eventBus.dispatch("find", {
      query: stringToSearch,
      type: "again",
      caseSensitive: false,
      highlightAll: true,
      phraseSearch: true
    });
  }

  nextPage() {
    if (this.pdfPage() >= this.pdfTotalPages()) {
      return;
    }
    this.pdfPage.update((prevPDFPage) => prevPDFPage + this.PAGE_CHANGE);
  }

  previousPage() {
    if (this.pdfPage() <= 1) {
      return;
    }
    this.pdfPage.update((prevPDFPage) => prevPDFPage - this.PAGE_CHANGE);
  }

  zoomIn() {
    this.pdfZoom.update((prevZoom) => prevZoom + this.ZOOM_CHANGE);
  }

  zoomOut() {
    if (this.pdfZoom() <= 0.5) {
      return;
    }
    this.pdfZoom.update((prevZoom) => prevZoom - this.ZOOM_CHANGE);
  }

  closePdf() {
    this.pdfSrcPathLink.set("");
  }
}
