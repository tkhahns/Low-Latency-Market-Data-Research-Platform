import type { ConnectionStatus } from "../types";

interface Props {
  status: ConnectionStatus;
}

const labels: Record<ConnectionStatus, string> = {
  connecting: "Connecting…",
  live: "Live",
  polling: "Live (polling)",
  disconnected: "Disconnected",
};

const colors: Record<ConnectionStatus, string> = {
  connecting: "bg-neutral-bg text-neutral-text",
  live: "bg-fresh-bg text-fresh-text",
  polling: "bg-fresh-bg text-fresh-text",
  disconnected: "bg-stale-bg text-stale-text",
};

export function ConnectionBadge({ status }: Props) {
  return (
    <span className={`rounded-full px-3 py-1 text-xs font-medium ${colors[status]}`}>
      {labels[status]}
    </span>
  );
}
