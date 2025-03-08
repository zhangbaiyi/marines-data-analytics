import { Route } from "@angular/router";

export const componentsRoutes: Route[] = [
  {
    path: "",
    loadComponent: () => import("./nx-welcome.component").then((m) => m.NxWelcomeComponent)
  },
  {
    path: "demo",
    loadComponent: () => import("./demo/demo.component").then((m) => m.DemoComponent)
  },
  {
    path: "custom_select",
    loadComponent: () =>
      import("./custom-search-with-mat-select/custom-search-with-mat-select.component").then(
        (m) => m.CustomSearchWithMatSelectComponent
      )
  }
];
