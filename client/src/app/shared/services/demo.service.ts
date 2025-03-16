import { Injectable } from "@angular/core";
import { map, Observable, of } from "rxjs";

import { MappedFileOptionsFlattened } from "../../components/demo/demo.component";
import { DEFAULT_DEMO_CONTENT, DemoContent } from "../models/demo.model";
import { APIService } from "./api.service";

@Injectable({
  providedIn: "root"
})
export class DemoService {
  readonly #backgroundOptionStateMapSingle = new Map<string, string[]>();
  readonly #backgroundOptionStateMapMultiple = new Map<string, string[]>();

  constructor(private readonly apiService: APIService) {}

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
    const queryString = "?type=python,java,c,typescript,html,css&version=2";
    return this.apiService
      .post<string>(`api/file-generate-status${queryString}`, { message: "Hello World!" })
      .pipe(map((res) => res.response));
  }

  testAPI(test: MappedFileOptionsFlattened[]): Observable<string> {
    return this.apiService
      .post<string>("api/test", { arguments: JSON.stringify(test) })
      .pipe(map((res) => res.response));
  }

  addToBackgroundState(fileName: string, options: string[]) {
    this.#backgroundOptionStateMapSingle.set(fileName, options);
  }

  removeFromBackgroundState(fileName: string) {
    this.#backgroundOptionStateMapSingle.delete(fileName);
  }

  get backgroundOptionStateMapSingle(): Map<string, string[]> {
    return this.#backgroundOptionStateMapSingle;
  }

  addToBackgroundState2(fileName: string, options: string[]) {
    this.#backgroundOptionStateMapMultiple.set(fileName, options);
  }

  removeFromBackgroundState2(fileName: string) {
    this.#backgroundOptionStateMapMultiple.delete(fileName);
  }

  get backgroundOptionStateMapMultiple(): Map<string, string[]> {
    return this.#backgroundOptionStateMapMultiple;
  }
}
