export type DemoContent = {
  id: number;
  text: string;
};

export const DEFAULT_DEMO_CONTENT: DemoContent = {
  id: 0,
  text: ""
};

export type PythonPrediction = {
  prediction: string;
};

export const DEFAULT_PREDICTION: PythonPrediction = {
  prediction: ""
};
