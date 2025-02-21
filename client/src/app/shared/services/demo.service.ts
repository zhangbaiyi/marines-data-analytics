import { Injectable } from "@angular/core";
import { map, Observable, of } from "rxjs";

import { DEFAULT_DEMO_CONTENT, DemoContent } from "../models/demo.model";
import { APIService } from "./api.service";

@Injectable({
  providedIn: "root"
})
export class DemoService {
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

  testFileUpload(formData: FormData): Observable<string> {
    return this.apiService.post<string>("api/upload-files", formData).pipe(map((res) => res.response));
  }

  getPdfFileLinkFromServer(): Observable<string> {
    const queryString = "?type=python,java,c,typescript,html,css&version=2";
    return this.apiService
      .post<string>(`api/file-generate-status${queryString}`, { message: "Hello World!" })
      .pipe(map((res) => res.response));
  }
}
