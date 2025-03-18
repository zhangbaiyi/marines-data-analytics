import { Injectable } from "@angular/core";
import { Moment } from "moment";
import { map, Observable, of } from "rxjs";

import { MappedFileOptionsFlattened } from "../../components/demo/demo.component";
import { DEFAULT_DEMO_CONTENT, DEFAULT_FILE_OPTIONS, DemoContent, FileOptions } from "../models/demo.model";
import { APIService } from "./api.service";

@Injectable({
  providedIn: "root"
})
export class DemoService {
  readonly #backgroundOptionStateMapSingle = new Map<string, FileOptions>();
  readonly #backgroundOptionStateMapMultiple = new Map<string, FileOptions>();

  constructor(private readonly apiService: APIService) {}

  get backgroundOptionStateMapSingle(): Map<string, FileOptions> {
    return this.#backgroundOptionStateMapSingle;
  }

  get backgroundOptionStateMapMultiple(): Map<string, FileOptions> {
    return this.#backgroundOptionStateMapMultiple;
  }

  getFilledCard(): Observable<DemoContent> {
    const c: DemoContent = Object.assign({}, DEFAULT_DEMO_CONTENT);
    c.text = "New Demo Content Text";
    c.id = 2;
    return of(c);
  }

  clearContent(): Observable<DemoContent> {
    return of(DEFAULT_DEMO_CONTENT);
  }

  uploadFilesToBE(formData: FormData): Observable<string> {
    return this.apiService.post<string>("api/upload-files", formData).pipe(map((res) => res.response));
  }

  getPdfFileLinkFromServer(): Observable<string> {
    const queryString = "?category=sales,email&month=202412&group=all";
    return this.apiService
      .post<string>(`api/file-generate-status${queryString}`, { message: "Hello World!" })
      .pipe(map((res) => res.response));
  }

  testAPI(test: MappedFileOptionsFlattened[]): Observable<string> {
    return this.apiService
      .post<string>("api/test", { arguments: JSON.stringify(test) })
      .pipe(map((res) => res.response));
  }

  addToBackgroundState(fileName: string, fileAssociations: string[]) {
    this.#backgroundOptionStateMapSingle.set(fileName, {
      ...(this.#backgroundOptionStateMapSingle.get(fileName) ?? Object.assign({}, DEFAULT_FILE_OPTIONS)),
      dataAssocations: fileAssociations
    });
  }

  addDateToBackgroundState(fileName: string, date: Moment) {
    this.#backgroundOptionStateMapSingle.set(fileName, {
      ...(this.#backgroundOptionStateMapSingle.get(fileName) ?? Object.assign({}, DEFAULT_FILE_OPTIONS)),
      dateSelected: date
    });
  }

  removeFromBackgroundState(fileName: string) {
    this.#backgroundOptionStateMapSingle.delete(fileName);
  }

  addToBackgroundState2(fileName: string, fileAssociations: string[]) {
    this.#backgroundOptionStateMapSingle.set(fileName, {
      ...(this.#backgroundOptionStateMapSingle.get(fileName) ?? Object.assign({}, DEFAULT_FILE_OPTIONS)),
      dataAssocations: fileAssociations
    });
  }

  addDateToBackgroundState2(fileName: string, date: Moment) {
    this.#backgroundOptionStateMapMultiple.set(fileName, {
      ...(this.#backgroundOptionStateMapMultiple.get(fileName) ?? Object.assign({}, DEFAULT_FILE_OPTIONS)),
      dateSelected: date
    });
  }

  removeFromBackgroundState2(fileName: string) {
    this.#backgroundOptionStateMapMultiple.delete(fileName);
  }
}
