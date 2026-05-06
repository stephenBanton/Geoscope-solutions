#!/usr/bin/env node
const fs = require('fs');

function parseCSVLine(line) {
  const out = [];
  let cur = '';
  let q = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (q && line[i + 1] === '"') {
        cur += '"';
        i++;
      } else {
        q = !q;
      }
    } else if (ch === ',' && !q) {
      out.push(cur);
      cur = '';
    } else {
      cur += ch;
    }
  }
  out.push(cur);
  return out;
}

function check(filePath, stateColName = 'state') {
  if (!fs.existsSync(filePath)) {
    console.log(filePath + ' => MISSING');
    return;
  }
  const text = fs.readFileSync(filePath, 'utf8');
  const lines = text.split(/\r?\n/).filter(Boolean);
  if (lines.length < 2) {
    console.log(filePath + ' => EMPTY');
    return;
  }
  const header = parseCSVLine(lines[0]).map((h) => h.trim().toLowerCase());
  const idx = header.indexOf(stateColName.toLowerCase());
  const states = new Set();
  for (let i = 1; i < lines.length; i++) {
    const cols = parseCSVLine(lines[i]);
    const st = String(cols[idx] || '').trim().toUpperCase();
    if (/^[A-Z]{2}$/.test(st)) states.add(st);
  }
  console.log(filePath + ' => rows=' + (lines.length - 1) + ' states=' + states.size + ' ' + Array.from(states).sort().join(','));
}

check('downloads/missing/HIFLD_CHILD_CARE/child_care.csv');
check('downloads/missing/UST/ust_from_hdrive.csv');
check('downloads/missing/LUST/lust_from_hdrive.csv');
check('downloads/missing/NURSING_HOMES/cms_nh_facilities.csv');
check('downloads/missing/DERIVED/schools_public_derived.csv');
check('downloads/missing/DERIVED/schools_private_derived.csv');
check('downloads/missing/SCHOOLS_PUBLIC/schools_public_austin.csv');
