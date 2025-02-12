export type AppConfig = {
  backendServerPort: number;
  backendServerUrl: string;
  frontendClientPort: number;
  frontendClientUrl: string;
  logLevel: string;
};

export function parseBool(value: string): boolean {
  return value === "true";
}
