import { Injectable } from "@angular/core";
import { map, Observable, of } from "rxjs";

import { DEFAULT_DEMO_CONTENT, DemoContent, PythonPrediction } from "../models/demo.model";
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

  retrivePythonPrediction(): Observable<PythonPrediction> {
    // [1] How Angular services are used in the component
    // [2] Query parameters vs. Passing content in the body
    // [3] Usage of RxJS here
    return this.apiService
      .post<PythonPrediction>("api/python?type=python,java,c,typescript,html,css&version=2", {
        message: "Hello World!"
      })
      .pipe(
        map((res) => {
          // eslint-disable-next-line no-console
          console.log(res.response);
          return res.response;
        })
      );
  }
}
