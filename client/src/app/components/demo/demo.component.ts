import { CommonModule } from "@angular/common";
import { Component, OnDestroy, signal } from "@angular/core";
import { MatButtonModule } from "@angular/material/button";
import { MatCardModule } from "@angular/material/card";
import { MatProgressSpinnerModule } from "@angular/material/progress-spinner";
import { Subscription } from "rxjs";

import {
  DEFAULT_DEMO_CONTENT,
  DEFAULT_PREDICTION,
  DemoContent,
  PythonPrediction
} from "../../shared/models/demo.model";
import { DemoService } from "../../shared/services/demo.service";

@Component({
  selector: "app-demo",
  imports: [CommonModule, MatButtonModule, MatCardModule, MatProgressSpinnerModule],
  templateUrl: "./demo.component.html",
  styleUrl: "./demo.component.css"
})
export class DemoComponent implements OnDestroy {
  private readonly subscriptions: Subscription[] = [];
  demoContent = signal<DemoContent>(DEFAULT_DEMO_CONTENT);
  predictionContent = signal<PythonPrediction>(DEFAULT_PREDICTION);
  isLoading = signal<boolean>(false);

  constructor(private readonly demoService: DemoService) {}

  ngOnDestroy() {
    for (const subscription of this.subscriptions) {
      subscription.unsubscribe();
    }
  }

  retriveContent() {
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

  testRabbitMQ() {
    this.isLoading.set(true);
    const sub = this.demoService.retrivePythonPrediction().subscribe((prediction) => {
      this.predictionContent.set(prediction);
      // eslint-disable-next-line no-console
      console.log("Prediction:", prediction);
      this.isLoading.set(false);
    });
    this.subscriptions.push(sub);
  }

  clearPrediction() {
    this.predictionContent.set(DEFAULT_PREDICTION);
  }
}
