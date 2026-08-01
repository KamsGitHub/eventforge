export interface JobExecutionContext {
  /**
   * Whether the job this handler is running for has had cancellation
   * requested (Milestone 12's cooperative RUNNING cancellation). A handler
   * doing real long-running work should poll this between "ticks" and stop
   * itself if it becomes true — a handler whose work is effectively
   * instantaneous (like GenerateReportHandler) has no meaningful tick
   * boundary to check it at, which is a known, accepted limitation of
   * simulated work, not a bug.
   */
  isCancelled(): Promise<boolean>;
}

export interface JobHandler {
  execute(payload: unknown, context: JobExecutionContext): Promise<unknown>;
}
