import { html } from "../html.js";

const svg = (body) =>
  html`<svg width="14" height="14" viewBox="0 0 14 14" fill="none" aria-hidden="true">${body}</svg>`;
const line = (d) => html`<path d=${d} stroke="currentColor" stroke-width="1.5" />`;

export const icons = {
  logo: html`<svg width="20" height="20" viewBox="0 0 20 20" fill="none" aria-hidden="true">
    <circle cx="4" cy="4" r="2.5" fill="currentColor" />
    <circle cx="10" cy="10" r="2.5" fill="currentColor" />
    <circle cx="16" cy="16" r="2.5" fill="currentColor" />
    <path d="M5.5 5.5l3 3M11.5 11.5l3 3" stroke="currentColor" stroke-width="1.6" />
  </svg>`,
  close: svg(line("M3 3l8 8M11 3l-8 8")),
  up: svg(line("M3 9l4-4 4 4")),
  down: svg(line("M3 5l4 4 4-4")),
  copy: svg(html`<rect x="4.5" y="4.5" width="7" height="7" rx="1.5" stroke="currentColor"
      stroke-width="1.4" /><path d="M9.5 2.5h-6a1 1 0 0 0-1 1v6" stroke="currentColor"
      stroke-width="1.4" />`),
  trash: svg(line("M2.5 4h9M5.5 4V2.5h3V4M4 4l.6 7.5h4.8L10 4")),
  plus: svg(line("M7 2.5v9M2.5 7h9")),
  error: svg(html`<circle cx="7" cy="7" r="5.5" stroke="currentColor" stroke-width="1.4" />
    <path d="M7 4v3.5M7 9.5v.5" stroke="currentColor" stroke-width="1.6" />`),
  ok: svg(line("M3 7.5l2.5 2.5L11 4.5")),
};
