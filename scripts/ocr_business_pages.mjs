// Offline OCR of explicitly rendered filing pages. Does not promote evidence.
// Args: absolute tesseract.js module directory, page directory, output JSON.
import { createRequire } from 'node:module';
import { readdir, writeFile, rename, mkdir } from 'node:fs/promises';
import path from 'node:path';
const [moduleDir, pageDir, output] = process.argv.slice(2);
if (!moduleDir || !pageDir || !output) throw new Error('Expected module directory, pages directory, output JSON');
const { createWorker } = createRequire(import.meta.url)(path.resolve(moduleDir));
const files = (await readdir(pageDir)).filter(x => /^page-\d+\.png$/.test(x)).sort();
if (!files.length || files.length > 40) throw new Error('Expected 1..40 explicitly rendered pages');
const cachePath = path.join(path.dirname(output), 'ocr-language-cache');
await mkdir(cachePath, { recursive: true });
const worker = await createWorker('chi_sim+eng', 1, { cachePath });
const rows = [];
try {
  for (const file of files) {
    const { data } = await worker.recognize(path.join(pageDir, file));
    rows.push({ page_number: Number(file.match(/\d+/)[0]), confidence: data.confidence, text: data.text });
    await writeFile(output+'.tmp', JSON.stringify({ parser:'tesseract.js/chi_sim+eng', requires_page_review:true, rows }, null, 2));
    await rename(output+'.tmp', output);
    console.log(JSON.stringify({ page:rows.at(-1).page_number, confidence:data.confidence, chars:data.text.length }));
  }
} finally { await worker.terminate(); }
