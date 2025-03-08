import { ComponentFixture, TestBed } from "@angular/core/testing";

import { CustomSearchWithMatSelectComponent } from "./custom-search-with-mat-select.component";

describe("CustomSearchWithMatSelectComponent", () => {
  let component: CustomSearchWithMatSelectComponent;
  let fixture: ComponentFixture<CustomSearchWithMatSelectComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [CustomSearchWithMatSelectComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(CustomSearchWithMatSelectComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
