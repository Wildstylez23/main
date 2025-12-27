const fs = require('fs');
const path = require('path');

const cfgPath = path.resolve(__dirname, '../package/firebase.json');
if (!fs.existsSync(cfgPath)) {
  console.error('package/firebase.json not found');
  process.exit(2);
}

const cfg = JSON.parse(fs.readFileSync(cfgPath, 'utf8'));

const targetName = 'vissenmarktplaats-www-redirect';

const hosting = cfg.hosting || [];
const target = hosting.find(h => h.target === targetName);
if (!target) {
  console.error('Redirect hosting target not found in firebase.json:', targetName);
  process.exit(2);
}

const redirects = target.redirects || [];
if (!redirects.length) {
  console.error('No redirects defined for target', targetName);
  process.exit(2);
}

console.log('Found', redirects.length, 'redirect rule(s) for', targetName);
for (const r of redirects) {
  console.log(`source: ${r.source} -> destination: ${r.destination} (type=${r.type})`);
}

// Basic assertion: there should be a wildcard redirect to the apex
const match = redirects.find(r => r.source === '/**' && r.type === 301 && r.destination && r.destination.startsWith('https://'));
if (match) {
  console.log('\n✔ Redirect rule looks correct.');
  process.exit(0);
} else {
  console.error('\n✖ No suitable wildcard 301 redirect found for www target.');
  process.exit(1);
}
