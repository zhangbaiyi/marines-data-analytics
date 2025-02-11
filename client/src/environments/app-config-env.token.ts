/* Reference: https://ekrem-kocak.medium.com/using-environment-variables-with-nx-19-and-angular-18-058f2e989fc9 */
import { InjectionToken } from "@angular/core";

import { EnvironmentModel } from "./environment.model";

export const APP_CONFIG_TOKEN = new InjectionToken<EnvironmentModel>("APP_CONFIG_TOKEN");
