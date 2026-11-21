// htm bound to Preact: components are written as tagged template strings, so no JSX compiler or
// build step is needed.
import { h } from "preact";
import htm from "htm";

export const html = htm.bind(h);
