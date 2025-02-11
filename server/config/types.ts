export type AppConfig = {
  backendServerPort: number;
  backendServerUrl: string;
  frontendClientPort: number;
  frontendClientUrl: string;
  loglevel: string;
};

export function parseBool(value: string): boolean {
  return value === "true";
}
