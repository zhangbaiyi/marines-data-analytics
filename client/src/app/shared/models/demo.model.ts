export type DemoContent = {
  id: number;
  text: string;
};

export const DEFAULT_DEMO_CONTENT: DemoContent = {
  id: 0,
  text: ""
};

export type PythonFileNamePrediction = {
  file_name: string;
};

export const DEFAULT_PREDICTION: PythonFileNamePrediction = {
  file_name: ""
};
