import { CommonModule } from "@angular/common";
import { Component, OnDestroy, signal } from "@angular/core";
import { MatButtonModule } from "@angular/material/button";
import { MatCardModule } from "@angular/material/card";
import { Subscription } from "rxjs";

import { DEFAULT_DEMO_CONTENT, DemoContent } from "../../shared/models/demo.model";
import { DemoService } from "../../shared/services/demo.service";

@Component({
  selector: "app-demo",
  imports: [CommonModule, MatButtonModule, MatCardModule],
  templateUrl: "./demo.component.html",
  styleUrl: "./demo.component.css"
})
export class DemoComponent implements OnDestroy {
  private readonly subscriptions: Subscription[] = [];
  demoContent = signal<DemoContent>(DEFAULT_DEMO_CONTENT);

  constructor(private readonly demoService: DemoService) {}

  ngOnDestroy() {
    for (const subscription of this.subscriptions) {
      subscription.unsubscribe();
    }
  }

  retrieveContent() {
    const sub = this.demoService.getFilledCard().subscribe((content) => {
      this.demoContent.set(content);
    });
    this.subscriptions.push(sub);
  }

  clearContent() {
    const sub = this.demoService.clearContent().subscribe((content) => {
      this.demoContent.set(content);
    });
    this.subscriptions.push(sub);
  }
}
