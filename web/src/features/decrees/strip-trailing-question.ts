/**
 * Normalize a string for question-prompt comparison.
 *
 * Strips wrapping markdown emphasis (`**`, `*`, `_`), trailing punctuation
 * (`? . ! : ;`), leading/trailing whitespace, and lowercases the result.
 *
 * This is intentionally conservative — it handles the common formatting
 * differences an LLM produces between prose and structured_question prompt,
 * without crossing into fuzzy or semantic matching.
 */
export function normalizeForPromptMatch(s: string): string {
  let n = s.trim();
  // Strip one level of wrapping bold / italic markdown
  if (n.length >= 4 && n.startsWith("**") && n.endsWith("**")) {
    n = n.slice(2, -2);
  } else if (n.length >= 2 && n.startsWith("*") && n.endsWith("*")) {
    n = n.slice(1, -1);
  } else if (n.length >= 2 && n.startsWith("_") && n.endsWith("_")) {
    n = n.slice(1, -1);
  }
  // Strip trailing punctuation
  n = n.replace(/[?.!:;]+$/, "");
  return n.trim().toLowerCase();
}

/**
 * Strip a trailing question prompt from prose content.
 *
 * When an LLM writes the question text in both the conversational prose and
 * the structured_question block, this function removes the duplicate from
 * the prose so that `content_markdown` contains only non-question context.
 *
 * It only removes the **terminal segment** (last paragraph or last line)
 * when it matches the prompt after normalization.  It never mutates
 * mid-prose content.
 *
 * @returns The prose with the trailing prompt removed, or the original
 *          prose unchanged if no match is found.
 */
export function stripTrailingQuestion(prose: string, prompt: string | undefined): string {
  if (!prompt) return prose;
  const normPrompt = normalizeForPromptMatch(prompt);
  if (!normPrompt) return prose;

  const trimmed = prose.trimEnd();

  // Entire prose is the question
  if (normalizeForPromptMatch(trimmed) === normPrompt) {
    return "";
  }

  // Last paragraph (after double newline)
  const lastPara = trimmed.lastIndexOf("\n\n");
  if (lastPara >= 0) {
    const tail = trimmed.slice(lastPara + 2).trim();
    if (normalizeForPromptMatch(tail) === normPrompt) {
      return trimmed.slice(0, lastPara).trimEnd();
    }
  }

  // Last line (after single newline)
  const lastLine = trimmed.lastIndexOf("\n");
  if (lastLine >= 0 && lastLine !== lastPara) {
    const tail = trimmed.slice(lastLine + 1).trim();
    if (normalizeForPromptMatch(tail) === normPrompt) {
      return trimmed.slice(0, lastLine).trimEnd();
    }
  }

  return prose;
}
