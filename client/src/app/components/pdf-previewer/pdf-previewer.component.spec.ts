import { ComponentFixture, TestBed } from "@angular/core/testing";

import { PdfPreviewerComponent } from "./pdf-previewer.component";

describe("PdfPreviewerComponent", () => {
  let component: PdfPreviewerComponent;
  let fixture: ComponentFixture<PdfPreviewerComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [PdfPreviewerComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(PdfPreviewerComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
