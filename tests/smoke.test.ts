import { err, ok, type Result } from '@/shared/result';

describe('toolchain smoke test', () => {
  it('resolves the @/ path alias and type-checks under strict mode', () => {
    const success: Result<number> = ok(42);
    const failure: Result<number> = err(new Error('boom'));

    expect(success.ok).toBe(true);
    expect(success.ok && success.value).toBe(42);
    expect(failure.ok).toBe(false);
  });
});
