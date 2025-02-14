import { Route } from "@angular/router";

export const appRoutes: Route[] = [
  {
    path: "",
    loadChildren: () => import("./components/components.routing").then((r) => r.componentsRoutes)
  }
];
