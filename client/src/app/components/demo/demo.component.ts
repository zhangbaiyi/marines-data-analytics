import { CommonModule } from "@angular/common";
import { Component } from "@angular/core";
import { FormsModule } from "@angular/forms"; // Import FormsModule
import { MatButtonModule } from "@angular/material/button";
import { MatCardModule } from "@angular/material/card";
import { MatSelectModule } from "@angular/material/select";
import * as XLSX from "xlsx";
import jsPDF from "jspdf";
import { PdfPreviewerComponent } from "../pdf-previewer/pdf-previewer.component";

@Component({
  selector: "app-demo",
  standalone: true,
  imports: [CommonModule, FormsModule, MatButtonModule, MatCardModule, MatSelectModule, PdfPreviewerComponent], // Add FormsModule here
  templateUrl: "./demo.component.html",
  styleUrl: "./demo.component.css",
})
export class DemoComponent {
  readonly acceptedFileExtensionTypes = [".xlsx"];
  readonly selectedFiles: File[] = [];
  pdfSrcPathLink: string = ""; 
  foodItems: string[] = ["Pizza", "Burgers", "Fries", "Cookies", "Ice Cream"]; // Ensure this is populated
  selectedFood: string = "";
  foodData: any[] = [];
  filteredPrices: any[] = [];

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

      this.foodData = jsonData.map((item: any) => ({
        foodItem: item["Food Item"],
        price: item["Price"]
      }));
    };
    reader.readAsArrayBuffer(file);
  }

  filterPrices() {
    this.filteredPrices = this.foodData.filter(
      (item) => item.foodItem === this.selectedFood
    );
  }

  generatePDF() {
    if (!this.selectedFood || this.filteredPrices.length === 0) {
      alert("Please select a food item and ensure prices are loaded.");
      return;
    }

    const doc = new jsPDF();
    doc.setFontSize(16);
    doc.text(`Price List for ${this.selectedFood}`, 10, 10);

    let y = 20;
    this.filteredPrices.forEach((item) => {
      doc.text(`${item.foodItem}: $${item.price}`, 10, y);
      y += 10;
    });

    const pdfBlob = doc.output("blob");
    const pdfUrl = URL.createObjectURL(pdfBlob);
    this.pdfSrcPathLink = pdfUrl; 
  }
}
