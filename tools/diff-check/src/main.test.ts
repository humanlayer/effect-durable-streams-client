import { describe, expect, it } from "vitest";
import { runLint } from "./main.ts";
import { collectViolations } from "./diff-check.ts";

describe("error-severity lint process results", () => {
  it("reads successful lint output", () => {
    expect(runLint(process.execPath, ["-e", 'process.stdout.write("clean")'], process.cwd())).toBe(
      "clean",
    );
  });

  it("retains error diagnostics for added-line filtering on exit 1", () => {
    const diagnostic = {
      message: "Do not use let",
      code: "diff-check(no-let)",
      severity: "error" as const,
      filename: "source.ts",
      labels: [{ span: { line: 3, column: 1 } }],
    };
    const output = JSON.stringify({ diagnostics: [diagnostic] });
    expect(
      runLint(
        process.execPath,
        ["-e", `process.stdout.write(${JSON.stringify(output)}); process.exitCode = 1`],
        process.cwd(),
      ),
    ).toBe(output);
    expect(
      collectViolations({
        addedLines: new Map([["source.ts", [{ start: 3, end: 3 }]]]),
        diagnostics: [diagnostic],
      }),
    ).toHaveLength(1);
    expect(
      collectViolations({
        addedLines: new Map([["source.ts", [{ start: 4, end: 4 }]]]),
        diagnostics: [diagnostic],
      }),
    ).toHaveLength(0);
  });

  it("rejects execution failures instead of treating them as lint results", () => {
    expect(() =>
      runLint(
        process.execPath,
        ["-e", 'process.stderr.write("broken config"); process.exitCode = 2'],
        process.cwd(),
      ),
    ).toThrow("broken config");
    expect(() => runLint("/nonexistent/diff-check-oxlint", [], process.cwd())).toThrow();
  });
});
