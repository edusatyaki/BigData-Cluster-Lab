/* ============================================================
   UI
   ============================================================ */
const $ = id => document.getElementById(id);
const out = $('out'), cmd = $('cmd');
const CH = CHAPTERS.find(c=>c.id === document.body.dataset.chapter) || CHAPTERS[0], TASKS = CH.tasks;
HOST = CH.host;
let idx = 0, done = new Set(), revealed = false, status = null, cmdHist = [], hPos = 0, finished = false;
let dataView = null;
const KEY = CH.key;
function save(){ try { localStorage.setItem(KEY, JSON.stringify({ idx, done:[...done], finished })); } catch(_){} }
function load(){ try { const s = JSON.parse(localStorage.getItem(KEY) || 'null'); if (s){ idx = Math.min(Math.max(0, s.idx|0), TASKS.length-1); done = new Set(s.done || []); finished = !!s.finished; } } catch(_){} }

function setPrompt(){ $('prompt').textContent = HOST; }
function line(html, cls){ const d = document.createElement('div'); d.className = 'l' + (cls ? ' '+cls : ''); d.innerHTML = html; out.appendChild(d); out.scrollTop = out.scrollHeight; }
function banner(){
  line(CH.id === 'ch03' ? 'Spark master: yarn, Application Id: application_1761552000_0042\nSpark Web UI available at http://shopeasy-driver:4040' : 'Linux gateway.shopeasy.in 6.8.0 x86_64 · Hadoop 3.4.0 · Spark 3.5.1 · Kafka 3.8', 'dim');
  line('\nType your answer at the prompt. Enter runs it, Shift+Enter adds a line.\nType <span class="pr">help</span> for commands, <span class="pr">cls</span> to clear.\n', 'dim');
}
function replayTo(i){
  S = CH.init();
  for (let j=0;j<i;j++) execute(TASKS[j].answer);
  flashIds.clear();
}

function doneOf(c){
  if (c === CH) return done;
  try { const st = JSON.parse(localStorage.getItem(c.key) || 'null'); return new Set(st && st.done || []); } catch(_){ return new Set(); }
}
function topicsOf(c){
  const groups = [];
  c.tasks.forEach((t,i)=>{ const last = groups[groups.length-1]; if (last && last.name === t.group) last.items.push(i); else groups.push({ name:t.group, items:[i] }); });
  return groups;
}
/* outline collapse state: closed chapter/topic keys + hidden sidebar, remembered per browser */
const OKEY = 'bigdata-lab-outline';
let closed = new Set(), sideOff = false;
try { const o = JSON.parse(localStorage.getItem(OKEY) || 'null'); if (o){ closed = new Set(o.closed || []); sideOff = !!o.sideOff; } } catch(_){}
function saveOutline(){ try { localStorage.setItem(OKEY, JSON.stringify({ closed:[...closed], sideOff })); } catch(_){} }
const topicKey = (c,g) => `${c.id}:t${g.items[0]}`;
const CHEV = '<svg class="chev" viewBox="0 0 16 16" aria-hidden="true"><path d="M4 6l4 4 4-4" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/></svg>';
function openCurrent(){ closed.delete(CH.id); const g = topicsOf(CH).find(g=>g.items.includes(idx)); if (g) closed.delete(topicKey(CH,g)); }
function renderOutline(){
  const html = CHAPTERS.map(c=>{
    const on = c === CH, d = doneOf(c), shut = closed.has(c.id);
    const head = `<span class="chap-num">${c.num}</span><span class="chap-title">${esc(c.title)}<small>${esc(c.story)}</small></span><span class="chap-count">${d.size}/${c.tasks.length}</span>`;
    const topics = topicsOf(c).map(g=>{
      const n = g.items.filter(i=>d.has(i)).length, k = topicKey(c,g), tshut = closed.has(k);
      const items = g.items.map(i=>{
        const cls = `qi${d.has(i)?' done':''}${on && i===idx && !finished?' cur':''}${c.tasks[i].kind === 'quiz' ? ' cp' : ''}`;
        const inner = `<span class="st">${d.has(i)?'✓':i+1}</span><span>${esc(c.tasks[i].title)}</span>`;
        return on ? `<button type="button" class="${cls}" data-i="${i}"${i===idx&&!finished?' aria-current="step"':''}>${inner}</button>`
                  : `<a class="${cls}" href="${c.href}#q${i+1}">${inner}</a>`;
      }).join('');
      return `<button type="button" class="topic${tshut?' shut':''}" data-tog="${k}" aria-expanded="${!tshut}">${CHEV}<span class="tn">${esc(g.name)}</span><span class="tc">${n}/${g.items.length}</span></button><div class="items"${tshut?' hidden':''}>${items}</div>`;
    }).join('');
    const headHtml = on
      ? `<button type="button" class="chap-head" data-tog="${c.id}" aria-expanded="${!shut}">${head}${CHEV}</button>`
      : `<div class="chap-head"><a class="chap-link" href="${c.href}" title="Open chapter ${c.num}">${head}</a><button type="button" class="chap-tog" data-tog="${c.id}" aria-expanded="${!shut}" aria-label="Show or hide chapter ${c.num} topics">${CHEV}</button></div>`;
    const dsRow = c.dataset ? `<button type="button" class="qi ds" data-ds="${c.id}"><span class="st">{ }</span><span>Dataset · setup SQL</span></button>` : '';
    return `<section class="chap${on?' on':''}${shut?' shut':''}">${headHtml}<div class="topics"${shut?' hidden':''}>${dsRow}${topics}</div></section>`;
  }).join('');
  $('outline').innerHTML = `<div class="ol-bar"><span class="ol-label">Course outline</span><button type="button" class="ol-act" data-all="open" title="Expand all chapters and topics">Expand</button><button type="button" class="ol-act" data-all="close" title="Collapse all chapters and topics">Collapse</button><button type="button" class="ol-hide" id="olHide" aria-label="Hide course outline" title="Hide outline">«</button></div>${html}`;
  $('progText').textContent = `Chapter ${CH.num} · ${done.size} of ${TASKS.length} complete`;
  $('progBar').style.width = (100*done.size/TASKS.length) + '%';
}
function renderQ(){
  const q = $('q');
  if (finished){
    q.innerHTML = `<div class="fin"><div class="tag" style="display:inline-block">${CH.finish.tag}</div><h1>${CH.finish.title}</h1><p class="ask">${CH.finish.text}</p><p class="note">${CH.finish.note}</p><div class="btnrow" style="justify-content:center;margin-top:18px"><button class="btn primary" id="restart" type="button">Start again</button>${CHAPTERS[CHAPTERS.indexOf(CH)+1] ? `<a class="btn" href="${CHAPTERS[CHAPTERS.indexOf(CH)+1].href}">Next chapter →</a>` : ''}</div></div>`;
    $('restart').onclick = ()=>{ done.clear(); finished = false; goTo(0, true); };
    return;
  }
  const t = TASKS[idx];
  let st;
  if (done.has(idx) && (!status || status.kind === 'ok' || status.kind === 'wait')) st = `<div class="status ok"><strong>✓ Correct.</strong> ${t.note}</div>`;
  else if (status) st = `<div class="status ${status.kind}">${status.html}</div>`;
  else st = `<div class="status wait">${t.kind === 'quiz' ? 'Pick an option, or type <code>answer B</code> in the shell.' : 'Type your answer in the shell and press Enter.'}</div>`;
  const opts = t.kind === 'quiz' ? `<div class="opts">${t.options.map((o,i)=>{ const L = 'ABCDEFGH'[i]; const pick = status && status.pick === L; const win = done.has(idx) && L === t.correct; return `<button type="button" class="opt${win ? ' right' : pick ? ' wrong' : ''}" data-opt="${L}"><b>${L}</b><span>${o}</span></button>`; }).join('')}</div>` : '';
  q.innerHTML = `
    <div class="qhead"><span class="tag${t.kind === 'quiz' ? ' cp' : ''}">${esc(t.group)}${t.kind === 'quiz' ? ' · checkpoint' : ''}</span><span class="qn">Chapter ${CH.num} · Question ${idx+1} of ${TASKS.length}</span></div>
    <h1>${t.title}</h1>
    ${t.story ? `<p class="story">${t.story}</p>` : ''}
    <p class="ask">${t.ask}</p>
    ${opts}
    ${st}
    <div class="btnrow">
      <button class="btn${done.has(idx)?'':' primary'}" id="rev" type="button">${revealed ? 'Hide answer' : 'Show answer'}</button>
      ${idx>0 ? '<button class="btn" id="rst" type="button" title="Rebuild the cluster as if every earlier question was answered correctly">↺ Reset to this question</button>' : ''}
    </div>
    ${revealed ? `<div class="reveal">${t.kind === 'quiz' ? `<p class="qa"><b>${t.correct}</b> · ${t.options['ABCDEFGH'.indexOf(t.correct)]}</p>` : `<div class="codebox"><pre>${esc(t.answer)}</pre><button type="button" class="copy" data-copy="answer">Copy</button></div>`}<p>${t.explain}</p>${t.real ? `<div class="sql"><span>${esc(t.realLabel || 'On a real cluster')}</span><div class="codebox"><pre>${esc(t.real)}</pre><button type="button" class="copy" data-copy="real">Copy</button></div></div>` : ''}</div>` : ''}`;
  $('rev').onclick = ()=>{ revealed = !revealed; renderQ(); };
  q.querySelectorAll('.copy').forEach(b=> b.onclick = ()=> copyText(t[b.dataset.copy], b));
  q.querySelectorAll('.opt').forEach(b=> b.onclick = ()=>{ run('answer ' + b.dataset.opt); });
  if ($('rst')) $('rst').onclick = ()=>{ goTo(idx, true); };
}
function renderNav(){
  $('prev').disabled = idx === 0 && !finished;
  const nx = $('next');
  nx.textContent = idx === TASKS.length-1 ? 'Finish ✓' : 'Next →';
  nx.disabled = finished;
  nx.classList.toggle('go', done.has(idx) && !finished);
  $('crumb').textContent = finished ? `Chapter ${CH.num} complete` : `${CH.title} › ${TASKS[idx].group} › Q${idx+1}`;
}

/* ---------- data panel: cluster, HDFS, shards, streams, CAP, lake, tables ---------- */
function views(){
  const v = [];
  if (S.nodes.length && CH.id !== 'ch03') v.push({ id:'cluster', label: S.nodes.length === 1 ? 'Server' : `Cluster · ${S.nodes.length}` });
  if (Object.keys(S.files).length) v.push({ id:'hdfs', label:`HDFS · ${Object.keys(S.files).length}` });
  if (S.parts) v.push({ id:'shards', label:`Shards · ${S.parts.shards.length}` });
  if (S.kafka.topics.length) v.push({ id:'stream', label:'Kafka' });
  if (S.cap) v.push({ id:'cap', label:'CAP' });
  if (Object.keys(S.lake).length) v.push({ id:'lake', label:`Lake · ${Object.keys(S.lake).length}` });
  Object.keys(S.tables).sort().forEach(n=>v.push({ id:'t:' + n, label:`${n} · ${S.tables[n].rows.length}` }));
  return v;
}
const fl = id => flashIds.has(id) ? ' flash' : '';
function viewHtml(id){
  if (id === 'cluster'){
    const blk = {}; const big = {};
    Object.entries(S.files).forEach(([p, f])=>{
      if (f.blocks) f.blocks.forEach(b=>b.nodes.forEach(n=>(blk[n] = blk[n] || []).push(b.id)));
      else Object.entries(f.perNode).forEach(([n, c])=>(big[n] = big[n] || []).push(`${p.split('/').pop()} ×${c.toLocaleString('en-IN')}`));
    });
    const shard = {}; if (S.parts) S.parts.shards.forEach(s=>s.nodes.forEach((n,i)=>(shard[n] = shard[n] || []).push(s.id + (i ? '′' : ''))));
    return `<div class="nodes${S.nodes.length === 1 ? ' one' : ''}">${S.nodes.map(n=>{
      const pct = Math.min(100, 100*n.usedTB/n.diskTB);
      return `<div class="node${n.up ? '' : ' down'}${n.big ? ' big' : ''}${fl(n.id)}">
        <div class="nh"><b>${esc(n.id)}</b><span class="dot">${n.up ? 'up' : 'down'}</span></div>
        <div class="spec">${n.cores} cores · ${n.ram >= 1024 ? n.ram/1024 + ' TB' : n.ram + ' GB'} RAM</div>
        <div class="dbar${pct >= 99 ? ' full' : ''}"><i style="width:${pct}%"></i></div>
        <div class="spec">${fmtTB(n.usedTB)} / ${fmtTB(n.diskTB)} disk</div>
        ${(blk[n.id] || shard[n.id] || big[n.id]) ? `<div class="blks">${(shard[n.id]||[]).map(s=>`<span class="shd">${s}</span>`).join('')}${(blk[n.id]||[]).map(b=>`<span class="blk">${b}</span>`).join('')}${(big[n.id]||[]).map(b=>`<span class="blk wide">${esc(b)}</span>`).join('')}</div>` : ''}
      </div>`; }).join('')}</div>`;
  }
  if (id === 'hdfs'){
    return `<table><tr><th>path</th><th>size</th><th>blocks</th><th>replication</th><th title="n3 = node-3">blocks → nodes</th></tr>${Object.entries(S.files).map(([p, f])=>{
      const under = f.blocks ? f.blocks.filter(b=>b.nodes.length < f.repl).length : 0;
      const map = f.blocks ? f.blocks.map(b=>`<span class="bm"><span class="blk">${b.id}</span>${b.nodes.map(n=>n.replace('node-', 'n')).join('·')}</span>`).join('') : `${Object.keys(f.perNode).length} nodes, ~${Math.round(f.nblocks*f.repl/Object.keys(f.perNode).length).toLocaleString('en-IN')} replicas each`;
      return `<tr><td class="mono">${esc(p)}</td><td>${fmtMB(f.mb)}</td><td>${f.nblocks.toLocaleString('en-IN')}</td><td>${f.repl}× ${under ? '<span class="bad">under</span>' : '<span class="good">healthy</span>'}</td><td class="mono small">${map}</td></tr>`;
    }).join('')}</table>`;
  }
  if (id === 'shards'){
    return `<table><tr><th>shard</th><th>range</th><th>copies on</th><th>users</th></tr>${S.parts.shards.map(s=>`<tr><td><span class="shd">${s.id}</span></td><td>${s.range}</td><td class="mono">${s.nodes.map(n=>nodeOf(n).up ? n : `<s class="bad">${n}</s>`).join(', ')}</td><td>${s.users.join(', ')}</td></tr>`).join('')}</table>`;
  }
  if (id === 'stream'){
    return `<table><tr><th>topic</th><th>partitions</th><th>replication</th><th>consumer</th><th>events processed</th></tr>${S.kafka.topics.map(t=>`<tr><td class="mono">${esc(t)}</td><td>3</td><td>3</td><td>${S.kafka.events ? 'fraud-check (Spark Streaming)' : '—'}</td><td>${S.kafka.events || 0}</td></tr>`).join('')}</table>`;
  }
  if (id === 'cap'){
    const c = S.cap;
    return `<div class="cap"><div class="dc"><small>DC · Mumbai</small><b>₹${c.dc.mumbai.toLocaleString('en-IN')}</b></div><div class="link${c.link ? '' : ' cut'}"><span>${c.link ? '⇆ in sync' : '✕ partition'}</span><small>mode ${c.mode}</small></div><div class="dc${c.dc.delhi !== c.dc.mumbai ? ' stale' : ''}"><small>DC · Delhi</small><b>₹${c.dc.delhi.toLocaleString('en-IN')}</b></div></div>`;
  }
  if (id === 'lake'){
    return `<table><tr><th>s3://shopeasy-lake/</th><th>size</th><th>format</th></tr>${Object.keys(S.lake).sort().map(p=>`<tr class="${fl('lake:' + p).trim()}"><td class="mono">${esc(p)}</td><td>${fmtMB(S.lake[p].mb)}</td><td>${esc(S.lake[p].kind)}</td></tr>`).join('')}</table>`;
  }
  if (id.startsWith('t:')){
    const t = S.tables[id.slice(2)];
    const isN = i => t.rows.every(r=>typeof r[i] === 'number' || r[i] === null);
    return `<table class="${fl(id).trim()}"><tr>${t.cols.map(c=>`<th>${esc(c)}</th>`).join('')}</tr>${t.rows.map(r=>`<tr>${r.map((v,i)=>`<td class="${isN(i) ? 'num' : ''}">${v === null ? '<i class="nul">NULL</i>' : esc(fmtVal(v))}</td>`).join('')}</tr>`).join('')}</table>`;
  }
  return '';
}
function renderData(){
  const v = views();
  if (S.view && v.some(x=>x.id === S.view)){ dataView = S.view; S.view = null; }
  if (!v.some(x=>x.id === dataView)) dataView = v[0] ? v[0].id : null;
  const cur = v.find(x=>x.id === dataView);
  const t = dataView && dataView.startsWith('t:') ? S.tables[dataView.slice(2)] : null;
  $('dcrumb').textContent = cur ? (t ? `warehouse › ${dataView.slice(2)} · ${t.format}${t.rowsReal ? ' · sample of ' + t.rowsReal + ' rows' : ''}` : cur.label.replace(/ · \d+$/, '')) : '';
  $('dchips').innerHTML = v.map(x=>`<button type="button" class="chip${x.id===dataView?' on':''}" data-c="${esc(x.id)}">${esc(x.label)}</button>`).join('');
  $('dbody').innerHTML = cur ? viewHtml(dataView) : '<div class="empty">Nothing here yet.</div>';
}
function renderAll(){ renderOutline(); renderQ(); renderNav(); renderData(); setPrompt(); }

function goTo(i, rebuild){
  const advancing = i === idx + 1 && done.has(idx) && !rebuild;
  idx = i; finished = false; revealed = false; status = null;
  openCurrent(); saveOutline();
  if (!advancing){
    replayTo(i);
    out.innerHTML = '';
    banner();
    line(i ? `── Question ${i+1}: cluster rebuilt as if Q1–Q${i} were answered correctly ──` : `── Question 1: ${CH.startNote} ──`, 'sep');
  } else {
    line(`── Question ${i+1} ──`, 'sep');
  }
  flashIds.clear();
  try { history.replaceState(null, '', '#q' + (i+1)); } catch(_){}
  save(); renderAll(); scrollOutline(); cmd.focus();
}

function run(src){
  const code = src.trim();
  line(`<span class="pr">${esc(HOST)}</span> ${esc(src)}`);
  if (!code) return;
  cmdHist.push(src); hPos = cmdHist.length;
  if (/^(cls|clear)\s*;?$/.test(code)){ out.innerHTML = ''; return; }
  flashIds.clear();
  const before = clone(S);
  const r = execute(src);
  if (r.html) line(r.html);
  if (!finished && !done.has(idx)){
    let verdict = false;
    try { verdict = autoCheck(TASKS[idx], r, before); } catch(e){ console.error(e); verdict = false; }
    if (verdict === true){ done.add(idx); status = { kind:'ok' }; save(); }
    else if (verdict && typeof verdict === 'object') status = verdict;
    else if (typeof verdict === 'string') status = { kind:'hint', html:verdict };
    else if (r.err){
      let h = 'The shell returned an error. Read the red message, fix the command and run it again.';
      if (/command not found/.test(r.err.message)) h = 'Unknown command. Check the spelling, or type <code>help</code> to list this chapter’s commands.';
      else if (/PARSE_SYNTAX_ERROR/.test(r.err.message)) h = 'SQL syntax error: check commas between columns, quotes around strings (<code>\'North\'</code>) and the clause order SELECT → FROM → JOIN → WHERE → GROUP BY → HAVING → ORDER BY → LIMIT.';
      else if (/UNRESOLVED_COLUMN/.test(r.err.message)) h = 'A column name isn’t in that table. Click the table chip in the Data panel to see its columns.';
      else if (/TABLE_OR_VIEW_NOT_FOUND/.test(r.err.message)) h = 'No table with that name. Run <code>SHOW TABLES;</code> to list them.';
      status = { kind:'err', html:h };
    } else if (r.calls.length && !r.calls.some(c=>c.op === 'answer')) status = { kind:'hint', html:'That ran, but it isn’t what this question asks. Re-read the task above.' };
  } else if (finished || done.has(idx)){
    // keep the green status; if they answered a quiz again, show the right option
  }
  renderAll();
}

/* ---------- checking ---------- */
function expectedFrom(before, answer){
  const keep = [S, flashIds];
  S = clone(before); flashIds = new Set();
  const r = execute(answer); const st = S;
  [S, flashIds] = keep;
  return { r, st };
}
const normRow = r => JSON.stringify(r.map(v=>typeof v === 'number' ? Math.round(v*100)/100 : v));
function compareRes(got, exp, ordered){
  const g = got.rows.map(normRow), e = exp.rows.map(normRow);
  if (got.cols.length !== exp.cols.length) return `Your result has <b>${got.cols.length}</b> column${got.cols.length === 1 ? '' : 's'} (${got.cols.map(c=>'<code>'+esc(c)+'</code>').join(', ')}); the answer has <b>${exp.cols.length}</b> (${exp.cols.map(c=>'<code>'+esc(c)+'</code>').join(', ')}).`;
  if (g.length !== e.length) return `Your query returned <b>${g.length}</b> row${g.length === 1 ? '' : 's'}; the answer returns <b>${e.length}</b>. Check your <code>WHERE</code>, <code>GROUP BY</code>, <code>HAVING</code> or <code>LIMIT</code>.`;
  if ([...g].sort().join('|') !== [...e].sort().join('|')) return 'Right number of rows, but some values differ. Compare your output with the question: the filter value, the aggregate, the column summed.';
  if (ordered && g.join('|') !== e.join('|')) return 'Right rows, wrong order. Check <code>ORDER BY</code>: <code>DESC</code> means biggest first.';
  return true;
}
function autoCheck(t, r, before){
  if (t.kind === 'quiz'){
    const a = r.calls.filter(c=>c.op === 'answer').pop();
    if (!a) return t.check ? t.check(r) : false;
    if (a.v === t.correct){ line(`<span class="s">✓ ${a.v} is correct.</span>`); return true; }
    const n = 'ABCDEFGH'.indexOf(a.v);
    if (n < 0 || n >= t.options.length){ return { kind:'hint', html:`There’s no option ${a.v}. Choose ${'ABCDEFGH'.slice(0, t.options.length).split('').join(', ')}.` }; }
    line(`<span class="er">✗ ${a.v} isn’t it.</span>`);
    return { kind:'hint', pick:a.v, html:`<b>${a.v}</b> isn’t right. Read the options again, or click <em>Show answer</em> for the explanation.` };
  }
  if (r.calls.some(c=>c.op === 'answer')) return 'This question is answered in the shell with a command, not <code>answer X</code>.';
  if (t.check) return t.check(r, before);
  if (t.kind === 'sql'){
    const op = t.table ? 'sql.ctas' : 'sql.select';
    const call = r.calls.filter(c=>c.op === op).pop();
    if (!call){
      if (t.table && did(r, 'sql.select')) return `That’s the right idea as a query. Now save it: <code>CREATE TABLE ${t.table} AS SELECT …</code>`;
      return false;
    }
    if (t.table && call.name !== t.table) return `Name the table <code>${t.table}</code>. Run <code>DROP TABLE ${esc(call.name)};</code> and try again.`;
    const exp = expectedFrom(before, t.answer).r.calls.filter(c=>c.op === op).pop();
    if (t.from && !call.tables.some(x=>x.toLowerCase() === t.from.toLowerCase())) return `Run it on <code>${t.from}</code> so you can compare the scanned bytes.`;
    const v = compareRes(call.res, exp.res, t.ordered);
    if (v !== true) return v + (t.table ? ` Then <code>DROP TABLE ${t.table};</code> and run it again.` : '');
    if (t.need){ const miss = t.need.find(re=>!re.test(call.src)); if (miss) return `Right rows! Now write it the way this question practises: use <code>${esc(String(miss).replace(/^\/\\b|\\b\/i?$/g, ''))}</code>.`; }
    return true;
  }
  return false;
}

/* ---------- dataset setup SQL ---------- */
function sqlLit(v){ return v === null ? 'NULL' : typeof v === 'number' ? String(v) : `'${String(v).replace(/'/g, "''")}'`; }
function datasetCode(c){
  const tabs = c.dataset.tables;
  const blocks = Object.keys(tabs).map(n=>{
    const t = tabs[n];
    const type = i => t.rows.every(r=>typeof r[i] === 'number') ? (t.rows.every(r=>Number.isInteger(r[i])) ? 'INT' : 'DECIMAL(12,2)') : /date/.test(t.cols[i]) ? 'DATE' : 'VARCHAR(40)';
    return `DROP TABLE IF EXISTS ${n};\nCREATE TABLE ${n} (\n${t.cols.map((col,i)=>`  ${col} ${type(i)}`).join(',\n')}\n);\nINSERT INTO ${n} (${t.cols.join(', ')}) VALUES\n${t.rows.map(r=>'  (' + r.map(sqlLit).join(', ') + ')').join(',\n')};`;
  });
  return `-- Chapter ${c.num} · ${c.title} — ${c.story}\n-- Paste into PostgreSQL / MySQL / SQLite / spark-sql. Re-running it starts clean.\n\n${blocks.join('\n\n')}\n\n` + Object.keys(tabs).map(n=>`SELECT COUNT(*) FROM ${n};   -- ${tabs[n].rows.length}`).join('\n');
}
function openDataset(c){
  let m = $('dsModal');
  if (!m){
    m = document.createElement('div'); m.id = 'dsModal'; m.className = 'modal'; m.hidden = true;
    m.innerHTML = `<div class="modal-card" role="dialog" aria-modal="true" aria-labelledby="dsTitle"><div class="modal-head"><div><span class="tag" id="dsTag"></span><h2 id="dsTitle"></h2></div><button type="button" class="modal-x" id="dsClose" aria-label="Close">×</button></div><p class="modal-note" id="dsNote"></p><div class="codebox"><pre id="dsCode"></pre><button type="button" class="copy" id="dsCopy">Copy</button></div><p class="modal-foot">The lake and clickstream parts of this chapter are simulated; these three tables are real SQL you can run anywhere.</p></div>`;
    document.body.appendChild(m);
    m.addEventListener('click', e=>{ if (e.target === m || e.target.id === 'dsClose') closeDataset(); });
  }
  const code = datasetCode(c);
  $('dsTag').textContent = `Chapter ${c.num} · Dataset`;
  $('dsTitle').textContent = c.dataset.title;
  $('dsNote').textContent = c.dataset.note;
  $('dsCode').textContent = code;
  $('dsCopy').onclick = ()=> copyText(code, $('dsCopy'));
  m.hidden = false; drawer(false); $('dsCopy').focus();
}
function closeDataset(){ const m = $('dsModal'); if (m && !m.hidden){ m.hidden = true; cmd.focus(); } }

/* ---------- input handling ---------- */
function balance(s){
  let n = 0, q = null;
  for (let i=0;i<s.length;i++){
    const ch = s[i];
    if (q){ if (ch === '\\'){ i++; continue; } if (ch === q) q = null; continue; }
    if (ch === '"' || ch === "'" || ch === '`'){ q = ch; continue; }
    if ('({['.includes(ch)) n++; else if (')}]'.includes(ch)) n--;
  }
  return n;
}
function grow(){ cmd.style.height = 'auto'; cmd.style.height = cmd.scrollHeight + 'px'; }
cmd.addEventListener('input', grow);
cmd.addEventListener('keydown', e=>{
  if (e.key === 'Enter' && !e.shiftKey){
    if (balance(cmd.value) > 0) return;      // unfinished bracket: keep typing on a new line
    e.preventDefault();
    const v = cmd.value; cmd.value = ''; grow(); run(v);
  } else if (e.key === 'Tab'){
    e.preventDefault(); cmd.setRangeText('  ', cmd.selectionStart, cmd.selectionEnd, 'end');
  } else if (e.key === 'ArrowUp' && !cmd.value.slice(0, cmd.selectionStart).includes('\n')){
    if (hPos > 0){ e.preventDefault(); hPos--; cmd.value = cmdHist[hPos]; grow(); }
  } else if (e.key === 'ArrowDown' && !cmd.value.slice(cmd.selectionEnd).includes('\n')){
    if (hPos < cmdHist.length){ e.preventDefault(); hPos++; cmd.value = cmdHist[hPos] || ''; grow(); }
  } else if (e.key === 'l' && e.ctrlKey){ e.preventDefault(); out.innerHTML = ''; }
});
$('shell').addEventListener('click', e=>{ if (!window.getSelection().toString() && e.target.tagName !== 'BUTTON') cmd.focus(); });
$('clr').onclick = ()=>{ out.innerHTML = ''; cmd.focus(); };
function scrollOutline(){ const b = $('outline').querySelector('.qi.cur'); if (b) b.scrollIntoView({ block:'nearest' }); }
function drawer(open){ $('outline').classList.toggle('open', open); $('scrim').classList.toggle('on', open); $('menu').setAttribute('aria-expanded', open); }
const narrow = () => innerWidth <= 1240;
function setSide(off){ sideOff = off; $('app').classList.toggle('side-off', off); $('menu').setAttribute('aria-expanded', !off); saveOutline(); }
$('outline').addEventListener('click', e=>{
  const dsb = e.target.closest('[data-ds]');
  if (dsb){ openDataset(CHAPTERS.find(c=>c.id === dsb.dataset.ds)); return; }
  const q = e.target.closest('button.qi');
  if (q){ drawer(false); goTo(+q.dataset.i, true); return; }
  const tg = e.target.closest('[data-tog]');
  if (tg){ const k = tg.dataset.tog; closed.has(k) ? closed.delete(k) : closed.add(k); saveOutline(); renderOutline(); return; }
  const all = e.target.closest('[data-all]');
  if (all){
    closed.clear();
    if (all.dataset.all === 'close') CHAPTERS.forEach(c=>{ closed.add(c.id); topicsOf(c).forEach(g=>closed.add(topicKey(c,g))); });
    saveOutline(); renderOutline(); return;
  }
  if (e.target.closest('#olHide')) narrow() ? drawer(false) : setSide(true);
});
$('menu').onclick = ()=> narrow() ? drawer(!$('outline').classList.contains('open')) : setSide(!sideOff);
setSide(sideOff);
function copyText(text, btn){
  const ok2 = ok => { btn.textContent = ok ? 'Copied ✓' : 'Press ⌘C'; btn.classList.toggle('ok', ok); setTimeout(()=>{ btn.textContent = 'Copy'; btn.classList.remove('ok'); }, 1600); };
  const fallback = () => { const ta = document.createElement('textarea'); ta.value = text; ta.style.position = 'fixed'; ta.style.opacity = '0'; document.body.appendChild(ta); ta.select(); let ok = false; try { ok = document.execCommand('copy'); } catch(_){} ta.remove(); ok2(ok); };
  if (navigator.clipboard && window.isSecureContext) navigator.clipboard.writeText(text).then(()=>ok2(true), fallback); else fallback();
}
$('scrim').onclick = ()=> drawer(false);
document.addEventListener('keydown', e=>{ if (e.key === 'Escape'){ drawer(false); closeDataset(); } });
if (CH.dataset) $('dsBtn').onclick = ()=> openDataset(CH); else $('dsBtn').hidden = true;
window.addEventListener('resize', ()=>{ out.scrollTop = out.scrollHeight; if (innerWidth > 1240) drawer(false); });
$('dchips').addEventListener('click', e=>{ const b = e.target.closest('.chip'); if (b){ dataView = b.dataset.c; renderData(); } });
$('dtog').onclick = ()=>{ const d = $('data'); d.classList.toggle('min'); $('dtog').textContent = d.classList.contains('min') ? 'Show' : 'Hide'; };
$('prev').onclick = ()=>{ if (finished){ finished = false; goTo(TASKS.length-1, true); } else if (idx > 0) goTo(idx-1, true); };
$('conn').textContent = CH.conn;
$('next').onclick = ()=>{
  if (idx === TASKS.length-1){ finished = true; save(); renderAll(); return; }
  goTo(idx+1, !done.has(idx));
};

/* full screen */
const root = document.documentElement;
const fsOn = () => !!(document.fullscreenElement || document.webkitFullscreenElement);
function fsToggle(){
  if (fsOn()) (document.exitFullscreen || document.webkitExitFullscreen).call(document);
  else { const r = (root.requestFullscreen || root.webkitRequestFullscreen).call(root); if (r && r.catch) r.catch(()=>{}); }
}
if (!(root.requestFullscreen || root.webkitRequestFullscreen) || document.fullscreenEnabled === false) $('fs').style.display = 'none';
$('fs').onclick = fsToggle;
const fsSync = () => { $('fs').textContent = fsOn() ? '⛶ Exit full screen' : '⛶ Full screen'; };
document.addEventListener('fullscreenchange', fsSync);
document.addEventListener('webkitfullscreenchange', fsSync);
document.addEventListener('keydown', e=>{ if ((e.key === 'f' || e.key === 'F') && document.activeElement !== cmd && !e.metaKey && !e.ctrlKey) fsToggle(); });

/* boot */
load();
const hq = location.hash.match(/^#q(\d+)$/);
const startAt = hq ? Math.min(Math.max(+hq[1]-1, 0), TASKS.length-1) : idx;
idx = -1;
goTo(startAt, true);
