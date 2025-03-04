import { Route } from "@angular/router";

export const componentsRoutes: Route[] = [
  {
    path: "",
    loadComponent: () => import("./demo/demo.component").then((m) => m.DemoComponent)
  }
];
