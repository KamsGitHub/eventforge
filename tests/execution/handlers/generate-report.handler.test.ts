import { GenerateReportHandler } from '@/modules/execution/domain/handlers/generate-report.handler';

describe('GenerateReportHandler', () => {
  const handler = new GenerateReportHandler();

  it('succeeds with a valid payload, returning a generated report', async () => {
    const result = await handler.execute({ reportName: 'Q1 Revenue' });

    expect(result).toMatchObject({ reportName: 'Q1 Revenue' });
    expect((result as { reportId: string }).reportId).toEqual(expect.any(String));
    expect((result as { generatedAt: string }).generatedAt).toEqual(expect.any(String));
  });

  it('fails when reportName is missing', async () => {
    await expect(handler.execute({})).rejects.toThrow();
  });

  it('fails when reportName is not a string', async () => {
    await expect(handler.execute({ reportName: 42 })).rejects.toThrow();
  });
});
