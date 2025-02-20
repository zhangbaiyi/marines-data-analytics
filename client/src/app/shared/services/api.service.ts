import { HttpClient, HttpHeaders } from "@angular/common/http";
import { inject, Injectable } from "@angular/core";
import { Observable } from "rxjs";

import { APP_CONFIG_TOKEN } from "../../../environments/app-config-env.token";

type APIResponse<T> = T extends (infer U)[] ? { response: U[] } : { response: T };

@Injectable({
  providedIn: "root"
})
export class APIService {
  private readonly environment = inject(APP_CONFIG_TOKEN);
  private readonly BASE_URL = this.environment.API_URL;
  constructor(private readonly http: HttpClient) {
    // eslint-disable-next-line no-console
    console.log({ environment: this.environment });
    // eslint-disable-next-line no-console
    console.log({ BASE_URL: this.BASE_URL });
  }

  get<T>(endpoint: string): Observable<APIResponse<T>> {
    return this.http.get<APIResponse<T>>(`${this.BASE_URL}/${endpoint}`);
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  post<T>(endpoint: string, body: any, options?: { headers?: HttpHeaders }): Observable<APIResponse<T>> {
    return this.http.post<APIResponse<T>>(`${this.BASE_URL}/${endpoint}`, body, options);
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  put<T>(endpoint: string, body: any): Observable<APIResponse<T>> {
    return this.http.put<APIResponse<T>>(`${this.BASE_URL}/${endpoint}`, body);
  }

  delete<T>(endpoint: string): Observable<APIResponse<T>> {
    return this.http.delete<APIResponse<T>>(`${this.BASE_URL}/${endpoint}`);
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  patch<T>(endpoint: string, body: any): Observable<APIResponse<T>> {
    return this.http.patch<APIResponse<T>>(`${this.BASE_URL}/${endpoint}`, body);
  }
}
