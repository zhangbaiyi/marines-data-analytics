import moment, { Moment } from "moment";

export type DemoContent = {
  id: number;
  text: string;
};

export const DEFAULT_DEMO_CONTENT: DemoContent = {
  id: 0,
  text: ""
};

export type FileOptions = {
  dataAssocations: string[];
  dateSelected: Moment;
};

export const DEFAULT_FILE_OPTIONS: FileOptions = {
  dataAssocations: [],
  dateSelected: moment()
};
