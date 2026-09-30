import { readFileSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const schemaPath = new URL("../contracts/theme/theme.schema.json", import.meta.url);
const apiPath = new URL("../contracts/openapi/openapi.yaml", import.meta.url);
const source = JSON.parse(readFileSync(schemaPath, "utf8"));
const name = (value) => `Theme${value[0].toUpperCase()}${value.slice(1)}`;

// OpenAPI 3.0 describes the shape for generated clients. The canonical Draft-07
// resolver additionally enforces conditional completeness and allowed map keys.
function project(schema) {
  if (Array.isArray(schema)) return schema.map(project);
  if (schema === null || typeof schema !== "object") return schema;
  const result = {};
  for (const [key, value] of Object.entries(schema)) {
    if (["$schema", "definitions", "if", "then", "else", "propertyNames", "anyOf"].includes(key)) continue;
    if (key === "properties") {
      result.properties = Object.fromEntries(Object.entries(value).map(([field, definition]) => [field, project(definition)]));
    } else if (key === "$ref") {
      if (!value.startsWith("#/definitions/")) throw new Error(`Unsupported theme reference: ${value}`);
      result.$ref = `#/components/schemas/${name(value.slice("#/definitions/".length))}`;
    } else if (key === "const") {
      result.enum = [value];
      result.type = Number.isInteger(value) ? "integer" : typeof value;
    } else if (key === "allOf") {
      const structural = value.filter((entry) => !entry.if).map(project);
      if (structural.length) result.allOf = structural;
    } else {
      result[key] = project(value);
    }
  }
  if (result.enum && !result.type) result.type = Number.isInteger(result.enum[0]) ? "integer" : typeof result.enum[0];
  return result;
}

const components = { ThemeManifest: project(source) };
for (const [key, value] of Object.entries(source.definitions ?? {})) components[name(key)] = project(value);
const begin = "    # BEGIN GENERATED THEME CONTRACT\n";
const end = "    # END GENERATED THEME CONTRACT\n";
const generated = begin + Object.entries(components).map(([key, value]) => {
  const json = JSON.stringify(value, null, 2).split("\n").map((line) => `      ${line}`).join("\n");
  return `    ${key}:\n${json}\n`;
}).join("") + end;
const original = readFileSync(apiPath, "utf8");
const start = original.indexOf(begin);
const finish = original.indexOf(end);
if ((start === -1) !== (finish === -1)) throw new Error("Incomplete generated theme contract markers");
const output = start < 0 ? `${original.trimEnd()}\n${generated}` : original.slice(0, start) + generated + original.slice(finish + end.length);
if (process.argv.includes("--check")) {
  if (output !== original) throw new Error(`Theme contract is stale. Run node ${fileURLToPath(import.meta.url)}`);
} else if (output !== original) {
  writeFileSync(apiPath, output);
}
