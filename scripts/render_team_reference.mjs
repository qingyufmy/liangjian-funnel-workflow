// Local, fixed-input visual-reference converter. All financial values are illustrative.
// No business runtime, network, dependency installation, or source-file writes.
import { readFile, mkdir, writeFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import vm from 'node:vm';

const project = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const sourceDirectory = path.resolve(project, '../liangjian_team_v2_20261016/artifacts/frontend-role-design-20261019/claude-reference-20261021');
const sourcePath = path.join(sourceDirectory, 'HOME_REFERENCE.dc.html');
const readmePath = path.join(sourceDirectory, 'README_REFERENCE.md');
const outputPath = path.join(project, '.impeccable/review/claude-reference-20261021/reference.html');
const sha256 = (value) => createHash('sha256').update(value).digest('hex');
const [sourceBytes, readmeBytes] = await Promise.all([readFile(sourcePath), readFile(readmePath)]);
const source = sourceBytes.toString('utf8');
const readme = readmeBytes.toString('utf8');
if (!readme.includes('renderVals()') || !source.includes('示意数据，仅用于视觉参考。')) {
  throw new Error('The expected fixed visual reference and its illustrative-data notice are required.');
}

const logicPattern = /<script\b(?=[^>]*\btype="text\/x-dc")[^>]*>([\s\S]*?)<\/script>/g;
const logicMatches = [...source.matchAll(logicPattern)];
if (logicMatches.length !== 1) throw new Error('Expected exactly one DC logic script.');
// This is a convenience evaluator for this trusted, fixed source, not a sandbox for
// arbitrary JavaScript. Node APIs are not provided; dynamic code generation is disabled.
const context = vm.createContext(Object.create(null), {
  codeGeneration: { strings: false, wasm: false },
});
const valuesJson = vm.runInContext(
  `"use strict"; class DCLogic {}\n${logicMatches[0][1]}\nJSON.stringify(new Component().renderVals())`,
  context,
  { timeout: 1000, filename: 'HOME_REFERENCE.renderVals.local.js' },
);
const values = JSON.parse(valuesJson);
const expectedLengths = { stations: 6, plans: 7, feed: 5 };
for (const [key, length] of Object.entries(expectedLengths)) {
  if (!Array.isArray(values[key]) || values[key].length !== length) {
    throw new Error(`Unexpected fixed reference list: ${key}`);
  }
}

function evaluate(expression, scope) {
  const text = expression.trim();
  if (text === 'true') return true;
  if (text === 'false') return false;
  if (!/^[A-Za-z_$][\w$]*(?:\.[A-Za-z_$][\w$]*)*$/.test(text)) {
    throw new Error(`Unsupported binding expression: ${text}`);
  }
  return text.split('.').reduce((value, key) => {
    if (['__proto__', 'prototype', 'constructor'].includes(key)) throw new Error('Unsafe binding key.');
    return value != null && Object.hasOwn(value, key) ? value[key] : undefined;
  }, scope);
}

function attribute(attributes, name) {
  const match = attributes.match(new RegExp(`(?:^|\\s)${name}="([^"]*)"`));
  if (!match) throw new Error(`Missing ${name} attribute.`);
  return match[1];
}

function expressionAttribute(attributes, name) {
  const value = attribute(attributes, name).match(/^\{\{([\s\S]*?)\}\}$/);
  if (!value) throw new Error(`Expected a binding for ${name}.`);
  return value[1];
}

function parseTemplate(template) {
  const root = { children: [] };
  const stack = [root];
  const controls = /<(\/?)sc-(for|if)\b([^>]*)>/g;
  let end = 0;
  for (const token of template.matchAll(controls)) {
    stack.at(-1).children.push(template.slice(end, token.index));
    if (token[1]) {
      if (stack.length === 1 || stack.at(-1).kind !== token[2]) throw new Error('Unbalanced template control.');
      stack.pop();
    } else {
      const node = { kind: token[2], attributes: token[3], children: [] };
      stack.at(-1).children.push(node);
      stack.push(node);
    }
    end = token.index + token[0].length;
  }
  stack.at(-1).children.push(template.slice(end));
  if (stack.length !== 1) throw new Error('Unclosed template control.');
  return root.children;
}

const escapeHtml = (value) => String(value).replace(/[&<>"']/g, (character) => ({
  '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
})[character]);

let loopsExpanded = 0;
let conditionsEvaluated = 0;
function render(nodes, scope) {
  return nodes.map((node) => {
    if (typeof node === 'string') {
      return node.replace(/\{\{([\s\S]*?)\}\}/g, (_match, expression) => {
        const value = evaluate(expression, scope);
        if (!['string', 'number', 'boolean'].includes(typeof value)) {
          throw new Error(`Missing or non-scalar binding: ${expression}`);
        }
        return escapeHtml(value);
      });
    }
    if (node.kind === 'if') {
      conditionsEvaluated += 1;
      return evaluate(expressionAttribute(node.attributes, 'value'), scope) ? render(node.children, scope) : '';
    }
    loopsExpanded += 1;
    const items = evaluate(expressionAttribute(node.attributes, 'list'), scope);
    const alias = attribute(node.attributes, 'as');
    if (!Array.isArray(items) || !/^[A-Za-z_$][\w$]*$/.test(alias)) throw new Error('Unsupported loop.');
    return items.map((item) => render(node.children, { ...scope, [alias]: item })).join('');
  }).join('');
}

let html = source.replace(logicPattern, '')
  .replace(/<script\b[^>]*\bsrc="\.\/support\.js"[^>]*>\s*<\/script>/g, '')
  .replace(/<\/?(?:x-dc|helmet)\b[^>]*>/g, '');
html = render(parseTemplate(html), values);
// A non-visible comment identifies provenance without changing layout or sample copy.
html = html.replace(/<!doctype html>/i, '<!doctype html>\n<!-- Static Claude visual reference; illustrative data only, not live business evidence. -->');
if (loopsExpanded !== 3 || conditionsEvaluated !== 48) throw new Error('Unexpected template control count.');
if (/\{\{|\}\}|<\/?sc-|<script\b|<\/?(?:helmet|x-dc)\b|support\.js/i.test(html)) {
  throw new Error('Unresolved template directive or executable reference dependency.');
}
if (!html.includes('示意数据，仅用于视觉参考。')) throw new Error('Illustrative-data notice was lost.');
await mkdir(path.dirname(outputPath), { recursive: true });
await writeFile(outputPath, html, 'utf8');
if (sha256(await readFile(sourcePath)) !== sha256(sourceBytes)) throw new Error('Reference source changed during conversion.');
console.log(JSON.stringify({
  sourcePath, sourceSha256: sha256(sourceBytes), readmeSha256: sha256(readmeBytes),
  outputPath, artifactSha256: sha256(await readFile(outputPath)),
  method: 'fixed local renderVals in vm (1000ms); 3 loops and 48 conditions; escaped scalar bindings; unwrap canvas and remove scripts; retain original styles and font links',
  sampleCounts: expectedLengths,
  limitations: 'Static illustrative state only; font links are preserved and browser loading must respect the caller network policy; original layout and preview dimensions are not restyled.',
}, null, 2));
