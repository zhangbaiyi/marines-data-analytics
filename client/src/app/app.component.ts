import { Component } from "@angular/core";
import { RouterModule } from "@angular/router";

// import { NxWelcomeComponent } from "./nx-welcome.component";

@Component({
  imports: [RouterModule],
  // imports: [NxWelcomeComponent, RouterModule],
  selector: "app-root",
  templateUrl: "./app.component.html",
  styleUrl: "./app.component.css"
})
export class AppComponent {
  title = "client";
}
