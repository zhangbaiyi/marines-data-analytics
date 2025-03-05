import { CommonModule } from "@angular/common";
import { Component, signal } from "@angular/core";
import { FormsModule } from "@angular/forms";
import { MatButtonModule } from "@angular/material/button";
import { MatCardModule } from "@angular/material/card";
import { MatSelectModule } from "@angular/material/select";
import { FileUploaderComponent } from "../file-uploader/file-uploader.component";
import * as XLSX from "xlsx";
import jsPDF from "jspdf";
import { PdfPreviewerComponent } from "../pdf-previewer/pdf-previewer.component";
import { DemoService } from "src/app/shared/services/demo.service";


@Component({
  selector: "app-demo",
  standalone: true,
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
export class DemoComponent {
  readonly acceptedFileExtensionTypes = [".xlsx"];
  selectedFiles = signal<File[]>([]);
  pdfSrcPathLink = signal<string>("");
  foodItems = signal<string[]>(["Pizza", "Burgers", "Fries", "Cookies", "Ice Cream"]);
  selectedFood = signal<string>("");
  foodData = signal<any[]>([]);
  filteredPrices = signal<any[]>([]);
  constructor(private readonly demoService: DemoService) {}
  onFileSelected(event: Event) {
    const input = event.target as HTMLInputElement;
    if (!input.files || input.files.length === 0) return;

    const file = input.files[0];
    if (!this.acceptedFileExtensionTypes.some((ext) => file.name.endsWith(ext))) {
      alert("Only XLSX files are allowed.");
      return;
    }

    this.readExcelFile(file);
  }

  readExcelFile(file: File) {
    const reader = new FileReader();
    reader.onload = (e) => {
      const data = new Uint8Array((e.target as FileReader).result as ArrayBuffer);
      const workbook = XLSX.read(data, { type: "array" });
      const sheetName = workbook.SheetNames[0];
      const sheet = workbook.Sheets[sheetName];
      const jsonData = XLSX.utils.sheet_to_json(sheet);

      this.foodData.set(
        jsonData.map((item: any) => ({
          foodItem: item["Food Item"],
          price: item["Price"]
        }))
      );
    };
    reader.readAsArrayBuffer(file);
  }

  filterPrices() {
    this.filteredPrices.set(this.foodData().filter((item) => item.foodItem === this.selectedFood()));
  }

  generatePDF() {
    if (!this.selectedFood() || this.filteredPrices().length === 0) {
      alert("Please select a food item and ensure prices are loaded.");
      return;
    }

    const doc = new jsPDF();
    doc.setFontSize(16);
    doc.text(`Price List for ${this.selectedFood()}`, 10, 10);

    let y = 20;
    this.filteredPrices().forEach((item) => {
      doc.text(`${item.foodItem}: $${item.price}`, 10, y);
      y += 10;
    });

    const pdfBlob = doc.output("blob");
    const pdfUrl = URL.createObjectURL(pdfBlob);
    this.pdfSrcPathLink.set(pdfUrl);
  }

  retrievePdfFileLink() {
      this.demoService.testAPI("hi").subscribe((pdfPath) => {
      const pdfPathResolvedToServer = `http://localhost:3000/${pdfPath}`;
      this.pdfSrcPathLink.set(pdfPathResolvedToServer);
    });
  }
}
