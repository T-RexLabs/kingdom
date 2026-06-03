import type { QuestionPayload } from "./planner-operation-controller";
import { stripTrailingQuestion } from "./strip-trailing-question";

const BLOCK_OPEN = "```structured_question";
const BLOCK_CLOSE = "```";

function readNonEmptyString(value: unknown): string | null {
  if (typeof value !== "string") {
    return null;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function parseQuestionType(value: unknown): QuestionPayload["type"] | null {
  if (typeof value !== "string") {
    return null;
  }
  switch (value) {
    case "yes_no":
    case "single_select":
    case "multi_select":
    case "short_text":
    case "long_text":
      return value;
    default:
      return null;
  }
}

function parseQuestionOption(value: unknown): NonNullable<QuestionPayload["options"]>[number] | null {
  const stringOption = readNonEmptyString(value);
  if (stringOption) {
    return { label: stringOption, value: stringOption };
  }
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }

  const candidate = value as Record<string, unknown>;
  const label = readNonEmptyString(candidate.label);
  const optionValue = readNonEmptyString(candidate.value);
  if (!label && !optionValue) {
    return null;
  }

  return {
    label: label ?? optionValue ?? "",
    value: optionValue ?? label ?? "",
  };
}

function parseQuestionOptions(value: unknown): QuestionPayload["options"] | undefined {
  if (!Array.isArray(value)) {
    return undefined;
  }

  const options = value
    .map((option) => parseQuestionOption(option))
    .filter((option): option is NonNullable<typeof option> => option !== null);
  return options.length > 0 ? options : undefined;
}

function parseQuestionPayload(value: unknown): QuestionPayload | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }

  const candidate = value as Record<string, unknown>;
  const prompt = readNonEmptyString(candidate.prompt);
  const type = parseQuestionType(candidate.type);
  if (!prompt || !type) {
    return null;
  }

  const options = parseQuestionOptions(candidate.options);
  if (options) {
    return { prompt, type, options };
  }
  return { prompt, type };
}

/**
 * Parse a structured_question fenced block from raw streaming text.
 *
 * Returns the prose content (text outside the block) and the parsed question
 * (null while the block is still streaming or absent). This is the frontend
 * equivalent of the backend's parseStructuredQuestion — same contract, runs
 * on the live accumulating stream.
 *
 * When a valid question is parsed, the trailing question prompt is stripped
 * from the prose via `stripTrailingQuestion` so that `content` contains only
 * non-question context. The question text lives exclusively in `question.prompt`.
 */
export function parseStreamingQuestion(raw: string): {
  content: string;
  question: QuestionPayload | null;
} {
  const blockStart = raw.indexOf(BLOCK_OPEN);
  if (blockStart < 0) {
    return { content: raw, question: null };
  }

  const before = raw.slice(0, blockStart);
  const afterTag = raw.slice(blockStart + BLOCK_OPEN.length);
  const blockEnd = afterTag.indexOf(BLOCK_CLOSE);

  if (blockEnd < 0) {
    // Block started but hasn't closed yet — show text before the block only.
    return { content: before.trimEnd(), question: null };
  }

  const jsonBody = afterTag.slice(0, blockEnd).trim();
  const after = afterTag.slice(blockEnd + BLOCK_CLOSE.length);
  const content = (before + after).trim();

  try {
    const parsed = parseQuestionPayload(JSON.parse(jsonBody));
    if (parsed) {
      const normalized = stripTrailingQuestion(content, parsed.prompt);
      return { content: normalized, question: parsed };
    }
  } catch {
    // Invalid JSON — treat as no question.
  }

  return { content, question: null };
}
