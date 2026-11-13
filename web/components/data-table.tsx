import type { ReactNode } from "react";

/**
 * The table recipe from bsvblockchain.org (/network/rules): bordered, surface
 * head, hairline rows. A minimum width, as there, so a phone scrolls the table
 * sideways instead of crushing the path column into one character per line.
 * Prose cells are capped at a reading measure.
 */
export function DataTable({
  head,
  rows,
}: {
  head: string[];
  rows: ReactNode[][];
}): ReactNode {
  return (
    <div className="overflow-x-auto rounded-lg border border-border">
      <table className="w-full min-w-[44rem] text-start">
        <thead className="bg-surface">
          <tr>
            {head.map((h) => (
              <th key={h} className="label-sm px-5 py-3 text-start text-ink-muted uppercase">
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((cells, r) => (
            <tr key={r} className="border-t border-border bg-surface-raised align-top">
              {cells.map((cell, c) => (
                <td
                  key={c}
                  className={
                    c === 0
                      ? "px-5 py-4 font-mono text-sm whitespace-nowrap text-foreground"
                      : c === cells.length - 1
                        ? "max-w-xl px-5 py-4 text-sm leading-relaxed text-ink-muted"
                        : "px-5 py-4 text-sm text-ink-muted"
                  }
                >
                  {cell}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
