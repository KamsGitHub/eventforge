export interface JobHandler {
  execute(payload: unknown): Promise<unknown>;
}
