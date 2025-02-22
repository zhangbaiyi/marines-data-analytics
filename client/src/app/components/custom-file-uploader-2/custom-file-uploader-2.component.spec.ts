import { ComponentFixture, TestBed } from "@angular/core/testing";

import { CustomFileUploader2Component } from "./custom-file-uploader-2.component";

describe("CustomFileUploader2Component", () => {
  let component: CustomFileUploader2Component;
  let fixture: ComponentFixture<CustomFileUploader2Component>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [CustomFileUploader2Component]
    }).compileComponents();

    fixture = TestBed.createComponent(CustomFileUploader2Component);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
