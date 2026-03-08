export function resolveRenderFlag(render?: boolean, renderMarkdown?: boolean): boolean {
  return Boolean(renderMarkdown || render);
}

export function resolveRenderPlain(
  renderPlain?: boolean,
  render?: boolean,
  renderMarkdown?: boolean,
): boolean {
  // Explicit plain render wins when any render flag is set; otherwise false.
  if (!renderPlain) return false;
  return Boolean(renderMarkdown || render || renderPlain);
}
