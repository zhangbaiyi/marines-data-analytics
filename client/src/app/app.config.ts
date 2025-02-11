import { provideHttpClient, withFetch } from "@angular/common/http";
import { ApplicationConfig, provideZoneChangeDetection } from "@angular/core";
import { provideAnimationsAsync } from "@angular/platform-browser/animations/async";
import { provideRouter } from "@angular/router";

import { APP_CONFIG_TOKEN } from "../environments/app-config-env.token";
import { ENVIRONMENT } from "../environments/environment";
import { appRoutes } from "./app.routes";

export const appConfig: ApplicationConfig = {
  providers: [
    provideZoneChangeDetection({ eventCoalescing: true }),
    provideHttpClient(withFetch()),
    provideAnimationsAsync(),
    provideRouter(appRoutes),
    { provide: APP_CONFIG_TOKEN, useValue: ENVIRONMENT }
  ]
};
