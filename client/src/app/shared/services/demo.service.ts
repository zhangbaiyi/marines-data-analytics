import { Injectable } from "@angular/core";
import { Observable, of } from "rxjs";

import { DEFAULT_DEMO_CONTENT, DemoContent } from "../models/demo.model";

@Injectable({
  providedIn: "root"
})
export class DemoService {
  getFilledCard(): Observable<DemoContent> {
    const c: DemoContent = Object.assign({}, DEFAULT_DEMO_CONTENT);
    c.text = "New Demo Content Text";
    c.id = 2;
    return of(c);
  }

  clearContent(): Observable<DemoContent> {
    return of(DEFAULT_DEMO_CONTENT);
  }
}
