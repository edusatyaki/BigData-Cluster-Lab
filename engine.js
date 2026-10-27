/* ============================================================
   Big Data Cluster Lab — engine
   A simulated cluster gateway: one server → a cluster of nodes,
   HDFS (blocks + replicas + self-healing), Spark jobs (map →
   shuffle → reduce, data locality, task retry), Kafka streams,
   a two-data-centre CAP playground, a data lake / warehouse,
   and a small Spark SQL engine (SELECT · JOIN · GROUP BY ·
   ORDER BY · LIMIT · CTAS · EXPLAIN).
   ============================================================ */
let HOST = 'shopeasy@gateway:~$';
let S;                       // the whole simulated world (plain JSON — cloneable)
let flashIds = new Set();    // node ids / table names to flash in the data panel

const esc = s => String(s).replace(/[&<>"]/g, c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const span = (c, s) => `<span class="${c}">${esc(s)}</span>`;
const K = s => span('k', s), NU = s => span('n', s), ST = s => span('s', s), D = s => span('dim', s), ER = s => span('er', s), B = s => span('key', s);
const clone = v => JSON.parse(JSON.stringify(v));
const isObj = v => v && typeof v === 'object' && !Array.isArray(v);
class ShellError extends Error { constructor(m){ super(m); this.name = 'ShellError'; } }
const fail = m => { throw new ShellError(m); };

/* ---------- formatting helpers ---------- */
function fmtMB(mb){
  if (mb >= 1024*1024) return +(mb/1024/1024).toFixed(mb >= 10*1024*1024 ? 0 : 1) + ' TB';
  if (mb >= 1024) return +(mb/1024).toFixed(mb >= 10*1024 ? 0 : 1) + ' GB';
  if (mb >= 1) return +mb.toFixed(mb >= 10 ? 0 : 1) + ' MB';
  return Math.max(1, Math.round(mb*1024)) + ' KB';
}
const fmtTB = tb => tb >= 10 ? tb.toFixed(0) + ' TB' : tb.toFixed(1) + ' TB';
const bar = (frac, w=20) => { const f = Math.max(0, Math.min(w, Math.round(frac*w))); return '▓'.repeat(f) + '░'.repeat(w-f); };
const pad = (s, n) => { s = String(s); return s.length >= n ? s : s + ' '.repeat(n - s.length); };
const lpad = (s, n) => { s = String(s); return s.length >= n ? s : ' '.repeat(n - s.length) + s; };
const num = n => Number(n).toLocaleString('en-IN');
const cnt = n => Number(n).toLocaleString('en-US');

/* ---------- world builders (used by chapter init) ---------- */
function mkNode(id, o){ return Object.assign({ id, up:true, diskTB:4, usedTB:0, cores:8, ram:64, big:false }, o || {}); }
function freshWorld(o){
  return Object.assign({
    help: 'all', single:false, level:1, nodes:[], site:{ up:true },
    local:{}, files:{}, parts:null, reviews:null, kafka:{ topics:[] }, cap:null,
    tables:{}, lake:{}, seen:{}, view:null, nextBlk:1073741825
  }, o || {});
}
const nodeOf = id => S.nodes.find(n=>n.id === id);
const aliveNodes = () => S.nodes.filter(n=>n.up);
function needNode(id){
  if (!id) fail('Which node? e.g. node kill node-5   (see: cluster status)');
  const n = nodeOf(id);
  if (!n) fail(`No such node: ${id}. Nodes: ${S.nodes.map(x=>x.id).join(', ')}`);
  return n;
}

/* ---------- argument parsing ---------- */
function words(line){
  const out = []; let cur = '', q = null, has = false;
  for (const ch of line){
    if (q){ if (ch === q) q = null; else cur += ch; continue; }
    if (ch === '"' || ch === "'"){ q = ch; has = true; continue; }
    if (/\s/.test(ch)){ if (cur || has) out.push(cur); cur = ''; has = false; continue; }
    cur += ch;
  }
  if (cur || has) out.push(cur);
  return out;
}
function flags(a){
  const f = {}, pos = [];
  for (let i=0;i<a.length;i++){
    const w = a[i];
    if (/^--?[A-Za-z]/.test(w) && !/^-(put|ls|du|cat|rm|mkdir|h|files|blocks|locations|report|setrep)$/.test(w)){
      const k = w.replace(/^--?/, '');
      if (k.includes('=')){ const [kk, vv] = k.split('='); f[kk.toLowerCase()] = vv; }
      else if (i+1 < a.length && !/^--?[A-Za-z]/.test(a[i+1])) f[k.toLowerCase()] = a[++i];
      else f[k.toLowerCase()] = true;
    } else pos.push(w);
  }
  return { f, pos };
}

/* ============================================================
   COMMANDS
   ============================================================ */
const HELP = {
  ch01: [
    ['site load festival', 'send festival-sale traffic at ShopEasy'],
    ['server status', 'inspect the single server'],
    ['server upgrade', 'scale UP: buy a bigger box'],
    ['ingest logs --days <n>', 'write application logs (5 TB/day)'],
    ['node kill <id> · node revive <id>', 'crash or restore a machine'],
    ['cluster add-node <n>', 'scale OUT: add n commodity nodes'],
    ['cluster status', 'list every node'],
    ['events sample <2005|today>', 'what one visit records'],
    ['stream txns <n>', 'watch card transactions arrive'],
    ['profile show <user>', 'one user profile, every shape'],
    ['reviews avg · reviews clean', 'rating, before/after cleaning'],
    ['funnel analyze checkout', 'turn raw logs into an insight'],
    ['rdbms add-column <table> <col>', 'change a live relational schema'],
    ['hdfs dfs -put <file> <dir>', 'store a file in HDFS'],
    ['spark-submit wordcount.py <path> [--executors n]', 'run a parallel job'],
    ['answer <A|B|C|D>', 'answer a checkpoint question']
  ],
  ch02: [
    ['cluster status', 'nodes, pooled storage & compute'],
    ['bench read 128MB', 'local disk vs network'],
    ['partition users --by name --shards <n>', 'split a dataset across nodes'],
    ['users get <name>', 'look a user up'],
    ['replicate users --factor <n>', 'copy each shard to n nodes'],
    ['node kill <id> · node revive <id>', 'crash or restore a node'],
    ['hdfs dfs -put <file> <dir> · -ls <dir>', 'store / list files'],
    ['hdfs fsck <path> -files -blocks -locations', 'where every block lives'],
    ['hdfs dfsadmin -report', 'cluster storage report'],
    ['spark-submit wordcount.py <path> [--executors n] [--locality off] [--fail node-x]', 'parallel job'],
    ['batch run eod-report', 'scheduled overnight job'],
    ['kafka-topics --create --topic <t>', 'create a Kafka topic'],
    ['stream start fraud-check --topic <t>', 'process events as they arrive'],
    ['cap status · cap partition · cap heal', 'two data centres, one link'],
    ['cap mode <CP|AP> · cap read balance --dc <dc>', 'choose during a partition'],
    ['cap write balance <amount> --dc <dc>', 'write at one data centre'],
    ['answer <A|B|C|D>', 'answer a checkpoint question']
  ],
  ch03: [
    ['lake ls', 'list the data lake (S3)'],
    ['lake put <file>', 'land a file in the lake as-is'],
    ['warehouse load <file> --table <t>', 'load into the warehouse (schema-on-write)'],
    ['SHOW TABLES; · DESCRIBE <t>;', 'what the warehouse holds'],
    ['SELECT … FROM … [JOIN] [WHERE] [GROUP BY] [ORDER BY] [LIMIT];', 'Spark SQL'],
    ['SELECT … FROM json.`raw/<file>`;', 'schema-on-read straight from the lake'],
    ['CREATE TABLE t AS SELECT …;', 'transform inside the warehouse'],
    ['EXPLAIN SELECT …;', 'how the engine splits the work'],
    ['ingest mysql.orders --into raw_orders', 'extract + load from the app DB'],
    ['pipeline run <etl|elt>', 'see where the transform happens'],
    ['answer <A|B|C|D>', 'answer a checkpoint question']
  ]
};

let calls = [];        // ops recorded by the command being executed
const rec = o => { calls.push(o); return o; };

function cmdHelp(){
  const list = HELP[S.help] || [].concat(HELP.ch01, HELP.ch02, HELP.ch03);
  const w = Math.min(58, Math.max(...list.map(x=>x[0].length)) + 2);
  return list.map(([c, d])=>`  ${K(pad(c, w))}  ${D(d)}`).join('\n') + '\n' + D('  Shift+Enter for a new line · ↑↓ history · cls clears the screen');
}

/* ---------- Chapter 1 world: one server, then a cluster ---------- */
function cmdSite(a){
  if (a[0] !== 'load') fail('Usage: site load festival');
  rec({ op:'site.load' });
  const up = aliveNodes();
  if (!up.length){ rec({ op:'site.down' }); return ER('✗ every machine is down — nothing can serve the sale.'); }
  if (up.length === 1){
    const s = up[0], cpu = 100;
    rec({ op:'site.crash' });
    S.site.up = true;
    return [
      D('festival sale opens · 10:00:00 IST'),
      `  traffic   ${bar(1)}  ${NU('48,000')} req/s   ${D('(normal day: 1,200)')}`,
      `  CPU       ${bar(1)}  ${ER('100%')}  ${D(`${s.cores} cores maxed`)}`,
      `  disk      ${bar(Math.min(1, (s.usedTB+3.2)/s.diskTB))}  ${ER('FULL')}  ${D('order + log writes rejected')}`,
      `  memory    ${bar(0.97)}  ${ER('97%')}  ${D('swapping')}`,
      ER('✗ 10:00:41  HTTP 503 Service Unavailable — shopeasy.in is DOWN'),
      D('  The code didn’t get worse. One machine hit every ceiling at once.'),
      D('  (traffic subsided at 10:47 — the site came back after a 47-minute outage)')
    ].join('\n');
  }
  const share = Math.round(48000 / up.length);
  const pct = Math.min(99, Math.round(100 * (48000 / up.length) / 12500));
  rec({ op:'site.ok', nodes:up.length });
  return [
    D('festival sale opens · 10:00:00 IST'),
    `  traffic   ${NU('48,000')} req/s spread over ${NU(up.length)} machines → ~${NU(num(share))} req/s each`,
    ...up.map(n=>`  ${pad(n.id, 10)}${bar(pct/100, 14)}  ${NU(pct + '%')} CPU`),
    ST('✓ 10:00:41  shopeasy.in is UP — p99 latency 180 ms'),
    D('  No machine is near its ceiling. Need more? add nodes.')
  ].join('\n');
}

function cmdServer(a){
  const s = S.nodes.find(n=>n.big) || S.nodes[0];
  if (a[0] === 'status'){
    rec({ op:'server.status' });
    if (!s) fail('No server.');
    const lv = ['','16 cores · 128 GB RAM','32 cores · 512 GB RAM','64 cores · 2 TB RAM'][S.level];
    return [
      `${B(s.id)}  ${s.up ? ST('● UP') : ER('● DOWN')}   ${D('the only machine ShopEasy owns')}`,
      `  model    ${lv}   ${D(`(tier ${S.level} of 3)`)}`,
      `  disk     ${bar(s.usedTB/s.diskTB)}  ${NU(fmtTB(s.usedTB))} / ${NU(fmtTB(s.diskTB))}`,
      `  replicas ${ER('none')}   ${D('— if this box dies, everything dies with it')}`,
      `  cost     ₹${NU(num([0,4,12,40][S.level]))} lakh / year`
    ].join('\n');
  }
  if (a[0] === 'upgrade'){
    if (!s) fail('No server to upgrade.');
    if (S.level >= 3){
      rec({ op:'server.ceiling' });
      return ER('✗ No bigger machine exists. Tier 3 (64 cores · 2 TB RAM) is the largest box on the market.') + '\n' +
        D('  Scale-up has a hard ceiling — and it’s still ONE point of failure.\n  Way forward: ') + K('cluster add-node <n>');
    }
    S.level++;
    s.cores = [0,16,32,64][S.level]; s.ram = [0,128,512,2048][S.level];
    rec({ op:'server.upgrade', level:S.level });
    const rows = [[1,1,4],[2,2,12],[3,4,40]];
    return [
      ST(`✓ upgraded ${s.id} to tier ${S.level}: ${s.cores} cores · ${s.ram >= 1024 ? s.ram/1024 + ' TB' : s.ram + ' GB'} RAM`),
      D('  tier   speed   cost / year'),
      ...rows.map(([t, sp, c])=>`  ${t === S.level ? K('▶ ' + t) : '  ' + t}     ${pad(sp + '×', 6)}  ₹${pad(c + ' lakh', 9)} ${D(bar(c/40, 20))}`),
      D(`  Speed went ${rows[S.level-1][1]}×, cost went ${rows[S.level-1][2]/4}× — cost climbs faster than speed.`)
    ].join('\n');
  }
  fail('Usage: server status | server upgrade');
}

function cmdIngest(a){
  const { f, pos } = flags(a);
  if (pos[0] === 'logs'){
    const days = +(f.days || 1);
    if (!(days > 0)) fail('Usage: ingest logs --days <n>');
    const up = aliveNodes();
    if (!up.length) fail('No machine is up to receive the logs.');
    const free = up.reduce((t,n)=>t + (n.diskTB - n.usedTB), 0);
    const want = 5 * days;
    rec({ op:'ingest.logs', days, nodes:up.length, full: want > free });
    const out = [D(`writing application logs at 5 TB/day to ${up.length === 1 ? up[0].id : up.length + ' nodes'} · free space ${fmtTB(free)}`)];
    if (want > free){
      const hours = free / 5 * 24;
      const steps = 4;
      for (let i=1;i<=steps;i++){
        const h = Math.round(hours*i/steps), used = up.reduce((t,n)=>t+n.usedTB,0) + free*i/steps, cap = up.reduce((t,n)=>t+n.diskTB,0);
        out.push(`  hour ${lpad(h, 3)}  ${bar(used/cap)}  ${NU(fmtTB(used))} / ${fmtTB(cap)}`);
      }
      up.forEach(n=>n.usedTB = n.diskTB);
      out.push(ER(`✗ hour ${Math.round(hours)}: DISK FULL — log writes rejected, new orders failing`));
      out.push(D(up.length === 1 ? '  One box holds only so many terabytes. You can’t add disks to it forever.' : '  Even a cluster fills up — but here the fix is just: add more nodes.'));
    } else {
      let left = want;
      up.forEach(n=>{ const take = Math.min(left, (n.diskTB - n.usedTB) * want / free); n.usedTB += take; });
      const cap = up.reduce((t,n)=>t+n.diskTB,0), used = up.reduce((t,n)=>t+n.usedTB,0);
      out.push(`  day ${days}  ${bar(used/cap)}  ${NU(fmtTB(used))} / ${fmtTB(cap)}`);
      out.push(ST(`✓ ${fmtTB(want)} written across ${up.length} nodes · ${fmtTB(cap-used)} still free`));
    }
    return out.join('\n');
  }
  if (pos[0] === 'mysql.orders' || pos[0] === 'orders'){
    const into = f.into || 'raw_orders';
    S.tables[into] = clone(S.src.raw_orders);
    flashIds.add('t:' + into); S.view = 't:' + into;
    rec({ op:'ingest.orders', into });
    return [
      D('connector  mysql://shopeasy-app-db:3306/shop.orders  →  warehouse'),
      `  extract   ${NU(S.src.raw_orders.rows.length)} rows   ${D('SELECT * FROM orders WHERE updated_at > :last_run')}`,
      `  load      ${ST(into)}  ${D('as-is: duplicates, cancelled orders and all')}`,
      ST(`✓ E + L done. ${S.src.raw_orders.rows.length} raw rows landed in ${into}.`) + D('  The T happens next — inside the warehouse, in SQL.')
    ].join('\n');
  }
  fail('Usage: ingest logs --days <n>   |   ingest mysql.orders --into raw_orders');
}

function cmdNode(a){
  const [act, id] = a;
  if (act === 'kill' || act === 'stop'){
    const n = needNode(id);
    if (!n.up) return D(`${id} is already down.`);
    n.up = false; flashIds.add(id);
    rec({ op:'node.kill', id, alone: S.nodes.length === 1 });
    if (S.nodes.length === 1){
      return [
        ER(`✗ ${id}: disk controller failure — machine DOWN`),
        ER('✗ shopeasy.in OFFLINE for every customer'),
        `  replicas  ${ER('none')}   backups running  ${ER('none')}`,
        ER('  potential permanent data loss: orders, carts, payments since last backup'),
        D('  A single node is a single point of failure.')
      ].join('\n');
    }
    const out = [ER(`✗ ${id} DOWN — heartbeat lost`)];
    // shards
    if (S.parts){
      S.parts.shards.forEach(sh=>{
        if (!sh.nodes.includes(id)) return;
        const live = sh.nodes.filter(x=>nodeOf(x).up);
        out.push(live.length ? `  shard ${sh.id} (${sh.range})  ${ST('still served')} from ${live.join(', ')}` : ER(`  shard ${sh.id} (${sh.range}) OFFLINE — no other copy exists`));
      });
    }
    // HDFS self-healing
    const healed = heal(id);
    if (healed.under){
      out.push(`  NameNode: ${NU(healed.under)} block replica${healed.under>1?'s':''} lost on ${id} → reads served from surviving replicas`);
      healed.moves.slice(0, 6).forEach(m=>out.push(`  ${ST('↻ auto-repair')} ${m.blk}: ${m.from} → ${K(m.to)}  ${D('(replication restored to ' + m.repl + ')')}`));
      if (healed.moves.length > 6) out.push(D(`  … ${healed.moves.length - 6} more blocks re-replicated`));
      if (healed.summary) out.push(`  ${ST('↻ auto-repair')} ${healed.summary}`);
      out.push(healed.moves.length || healed.summary ? ST('✓ self-healed — no data lost') : K(`⚠ no spare node to copy to — blocks stay under-replicated until a node joins (still readable)`));
    }
    if (S.site && S.nodes.length > 1 && !S.parts && !healed.under && S.help === 'ch01'){
      const up = aliveNodes().length;
      out.push(ST(`✓ shopeasy.in still UP — ${up} healthy nodes carry the load; replicas of ${id}'s data live elsewhere`));
    }
    return out.join('\n');
  }
  if (act === 'revive' || act === 'start'){
    const n = needNode(id);
    if (n.up) return D(`${id} is already up.`);
    n.up = true; flashIds.add(id);
    rec({ op:'node.revive', id });
    return ST(`✓ ${id} back online`) + (S.nodes.length === 1 ? ST(' — shopeasy.in is serving again') : D(' — rejoined the cluster'));
  }
  fail('Usage: node kill <id> | node revive <id>');
}

/* HDFS: re-create every replica that lived on a dead node */
function heal(dead){
  const res = { under:0, moves:[], summary:'' };
  const load = {}; S.nodes.forEach(n=>load[n.id] = 0);
  Object.values(S.files).forEach(f=>{ if (f.blocks) f.blocks.forEach(b=>b.nodes.forEach(x=>load[x]++)); });
  Object.entries(S.files).forEach(([p, f])=>{
    if (f.blocks){
      f.blocks.forEach(b=>{
        if (!b.nodes.includes(dead)) return;
        res.under++;
        const src = b.nodes.find(x=>x !== dead && nodeOf(x).up);
        const cand = aliveNodes().map(n=>n.id).filter(x=>!b.nodes.includes(x)).sort((x,y)=>load[x]-load[y] || S.nodes.findIndex(n=>n.id===y) - S.nodes.findIndex(n=>n.id===x));
        b.nodes = b.nodes.filter(x=>x !== dead);
        if (cand.length && src){ b.nodes.push(cand[0]); load[cand[0]]++; flashIds.add(cand[0]); res.moves.push({ blk:b.id, from:src, to:cand[0], repl:b.nodes.length }); }
      });
    } else if (f.perNode && f.perNode[dead]){
      const lost = f.perNode[dead]; res.under += lost; delete f.perNode[dead];
      const up = aliveNodes();
      up.forEach((n,i)=>{ f.perNode[n.id] = (f.perNode[n.id]||0) + Math.floor(lost/up.length) + (i < lost % up.length ? 1 : 0); });
      res.summary = `${num(lost)} replicas of ${p} copied to the ${up.length} healthy nodes`;
    }
  });
  return res;
}

function cmdCluster(a){
  const { pos } = flags(a);
  if (pos[0] === 'add-node' || pos[0] === 'add-nodes'){
    const k = +(pos[1] || 1);
    if (!(k >= 1 && k <= 40)) fail('Usage: cluster add-node <n>   (1–40)');
    const start = S.nodes.filter(n=>!n.big).length;
    const added = [];
    for (let i=1;i<=k;i++){ const id = 'node-' + (start + i + (S.nodes.some(n=>n.big) ? 1 : 0)); S.nodes.push(mkNode(id)); added.push(id); flashIds.add(id); }
    S.single = false; S.view = 'cluster';
    rec({ op:'cluster.add', k, total:S.nodes.length });
    const up = aliveNodes();
    const disk = up.reduce((t,n)=>t+n.diskTB,0), cores = up.reduce((t,n)=>t+n.cores,0);
    return [
      ST(`✓ added ${k} commodity node${k>1?'s':''}: ${added.length > 4 ? added.slice(0,2).join(', ') + ' … ' + added[added.length-1] : added.join(', ')}`) + D('  (8 cores · 64 GB · 4 TB each, ₹1.2 lakh/yr)'),
      `  cluster  ${NU(S.nodes.length)} machines · ${NU(fmtTB(disk))} pooled disk · ${NU(cores)} cores`,
      D('  Scale OUT: capacity and reliability grow just by adding ordinary machines.')
    ].join('\n');
  }
  if (pos[0] === 'status' || pos[0] === 'nodes' || !pos.length){
    rec({ op:'cluster.status' });
    S.view = 'cluster';
    const up = aliveNodes();
    const disk = up.reduce((t,n)=>t+n.diskTB,0), used = up.reduce((t,n)=>t+n.usedTB,0), cores = up.reduce((t,n)=>t+n.cores,0), ram = up.reduce((t,n)=>t+n.ram,0);
    const out = [
      `${B('cluster shopeasy-prod')}  ${NU(S.nodes.length)} node${S.nodes.length>1?'s':''} · ${ST(up.length + ' up')}${S.nodes.length - up.length ? ' · ' + ER((S.nodes.length - up.length) + ' down') : ''}`,
      `  pooled   ${NU(fmtTB(disk))} disk (${fmtTB(used)} used) · ${NU(cores)} cores · ${NU(ram >= 1024 ? (ram/1024).toFixed(ram % 1024 ? 1 : 0) + ' TB' : ram + ' GB')} RAM`
    ];
    S.nodes.forEach(n=>out.push(`  ${pad(n.id, 10)}${n.up ? ST('● up  ') : ER('● down')}  ${D(`${n.cores}c · ${n.ram >= 1024 ? n.ram/1024 + 'TB' : n.ram + 'GB'} · disk ${fmtTB(n.usedTB)}/${fmtTB(n.diskTB)}`)}`));
    if (S.nodes.length > 1) out.push(D('  To you it’s one system. Underneath: independent machines talking over a network.'));
    return out.join('\n');
  }
  fail('Usage: cluster status | cluster add-node <n>');
}

/* ---------- data explosion & the Vs ---------- */
function cmdEvents(a){
  const yr = a[1] || a[0];
  if (a[0] !== 'sample' || !yr) fail('Usage: events sample 2005 | events sample today');
  if (yr === '2005'){
    rec({ op:'events.2005' }); S.seen.e2005 = true;
    return [D('one visit to ShopEasy · 2005'), '  ' + ST('order_placed') + '   ' + ST('payment_done'), `  ${NU(2)} events / visit   ${D('— only what a human typed')}`].join('\n');
  }
  if (yr === 'today' || yr === '2026'){
    rec({ op:'events.today' }); S.seen.eToday = true;
    const ev = ['page_view','scroll_25%','hover_card','scroll_50%','search','page_view','add_to_cart','app_ping','video_play','rage_click','scroll_75%','hover_card','coupon_view','app_ping','page_view'];
    return [D('one visit to ShopEasy · today (first 15 events)'),
      ev.map((e,i)=>`  ${D('10:02:' + String(11 + i*2).padStart(2,'0') + '.' + String(i*137 % 1000).padStart(3,'0'))}  ${ST(e)}`).join('\n'),
      D('  … 1,985 more'),
      `  ${NU('~2,000')} events / visit   ${D('— 1000× more, emitted by machines: app logs, clickstream, device pings')}`].join('\n');
  }
  fail('Usage: events sample 2005 | events sample today');
}

function cmdStream(a){
  const { f, pos } = flags(a);
  if (pos[0] === 'txns'){
    const n = Math.min(12, Math.max(1, +(pos[1] || 5)));
    rec({ op:'stream.txns', n });
    const out = [D('card transactions arriving · decision budget < 1000 ms')];
    for (let i=0;i<n;i++){
      const ms = String(1 + i*11 + (i*7 % 5)).padStart(3,'0');
      const flag = i === 2;
      out.push(`  12:00:00.${ms}  txn #${88231 + i}  ₹${lpad(num([1240, 560, 84999, 315, 2100, 99, 4500, 760, 12000, 330, 640, 1999][i]), 7)}  ${flag ? ER('⚑ flagged — new device + 40× usual amount') : ST('✓ approved')}  ${D(`${[23,18,41,15,27,12,31,19,22,16,14,25][i]} ms`)}`);
    }
    out.push(D(`  ${n} events in ${(n*11)} ms. At 2,000,000 txns/s this never stops — velocity.`));
    return out.join('\n');
  }
  if (pos[0] === 'start'){
    const job = pos[1] || 'fraud-check', topic = f.topic;
    if (!topic) fail('Which topic? e.g. stream start fraud-check --topic payments');
    if (!S.kafka.topics.includes(topic)) fail(`Topic '${topic}' does not exist. Create it first: kafka-topics --create --topic ${topic}`);
    rec({ op:'stream.start', job, topic });
    S.kafka.events = (S.kafka.events || 0) + 6; S.view = 'stream';
    const ev = [['₹1,240','Mumbai','approve'],['₹560','Pune','approve'],['₹84,999','Lagos','FLAG'],['₹315','Delhi','approve'],['₹2,100','Delhi','approve'],['₹49,000','unknown device','FLAG']];
    return [
      D(`Spark Structured Streaming · job ${job} · reading Kafka topic ${topic}`),
      D('  card swipe → Kafka (the pipe) → Spark Streaming (the processor) → decision'),
      ...ev.map((e,i)=>`  event ${lpad(i+1,2)}  ${pad(e[0], 8)} ${pad(e[1], 15)} ${e[2] === 'FLAG' ? ER('⚑ blocked') : ST('✓ approved')}  ${D((180 + i*37) + ' ms after the swipe')}`),
      ST('✓ streaming — each event handled in < 1 s, the moment it arrives (data in motion)'),
      D('  The input never ends; the job keeps running.')
    ].join('\n');
  }
  fail('Usage: stream txns <n> | stream start fraud-check --topic <t>');
}

function cmdProfile(a){
  if (a[0] !== 'show') fail('Usage: profile show u42');
  rec({ op:'profile.show' });
  return [
    D('user u42 — one entity, four shapes'),
    `  ${K('structured')}       table row   ${D('│')} id=42 │ name=Ananya │ city=Pune │ joined=2023-04-11`,
    `  ${K('semi-structured')}  JSON log    ${ST('{"event":"add_to_cart","sku":"HP-221","ts":1761552011,"device":{"os":"android"}}')}`,
    `  ${K('unstructured')}     image       ${D('avatar.jpg · 2.4 MB · 3024×4032 pixels')}`,
    `  ${K('unstructured')}     free text   ${ST('"Love trekking & filter coffee. Returns things I don’t like 🙃"')}`,
    D('  Rows and columns only fit the first line — variety.')
  ].join('\n');
}

function cmdReviews(a){
  if (!S.reviews) S.reviews = { total:1000, genuine:610, cleaned:false };
  const r = S.reviews;
  if (a[0] === 'avg' || a[0] === 'average'){
    const avg = r.cleaned ? 3.2 : 4.8, n = r.cleaned ? r.genuine : r.total;
    rec({ op:'reviews.avg', cleaned:r.cleaned, avg });
    return `${B('Wireless Earbuds X2')}  ${r.cleaned ? ST('★ ' + avg) : K('★ ' + avg)}  ${D(`average of ${num(n)} reviews${r.cleaned ? ' (cleaned)' : ''}`)}`;
  }
  if (a[0] === 'clean'){
    if (r.cleaned) return D('Already cleaned.');
    r.cleaned = true;
    rec({ op:'reviews.clean' });
    return [
      D('checking 1,000 ingested reviews…'),
      `  duplicates (same text, same hour)   ${ER('−214')}`,
      `  bot accounts (age &lt; 1 day, 5★ only) ${ER('−176')}`,
      `  genuine                              ${ST('610')}`,
      ST('✓ cleaned. Run reviews avg again.')
    ].join('\n');
  }
  fail('Usage: reviews avg | reviews clean');
}

function cmdFunnel(a){
  if (a[0] !== 'analyze') fail('Usage: funnel analyze checkout');
  rec({ op:'funnel' });
  const st = [['view cart', 100], ['address', 81], ['shipping cost shown', 74], ['payment', 24], ['order placed', 22]];
  return [
    D('scanning 5 PB of clickstream logs (12 months, 340 M sessions)…'),
    ...st.map(([s, p])=>`  ${pad(s, 20)} ${bar(p/100, 25)} ${NU(p + '%')}`),
    K('  insight: 68% of users who see the shipping cost quit right there.'),
    D('  5 PB of logs → one sentence that recovers millions in checkouts. That’s value.')
  ].join('\n');
}

function cmdRdbms(a){
  if (a[0] !== 'add-column') fail('Usage: rdbms add-column orders coupon_code');
  const t = a[1] || 'orders', c = a[2] || 'new_col';
  rec({ op:'rdbms.alter', t, c });
  return [
    `postgres=# ${K(`ALTER TABLE ${t} ADD COLUMN ${c} TEXT DEFAULT 'none';`)}`,
    D(`  ${t}: 1,200,000,000 rows · 2.1 TB · schema-on-write`),
    `  rewriting table…  ${bar(0.07)}  7%   ${D('est. 6 h 40 m')}`,
    ER(`  ACCESS EXCLUSIVE lock held on ${t} — checkout queries are queueing (2,318 waiting)`),
    ER('✗ cancelled by on-call engineer after 9 minutes. Changing a rigid schema on a live billion-row table is slow and risky.'),
    D('  A data lake would just accept new fields and apply structure when it’s read (schema-on-read).')
  ].join('\n');
}

function cmdBench(a){
  rec({ op:'bench' });
  return [
    D('reading one 128 MB block'),
    `  from local disk           ${bar(0.05, 30)}  ${NU('0.3 s')}`,
    `  over the network (1 Gbps,  ${bar(0.37, 30)}  ${NU('1.2 s')}   ${D('quiet')}`,
    `    shared with 40 jobs)     ${bar(1, 30)}  ${ER('3.4 s')}   ${D('busy — and packets can drop')}`,
    K('  The network is the bottleneck → move as little data over it as possible.'),
    D('  Nodes coordinate by passing messages over this network — they share no memory.')
  ].join('\n');
}

/* ---------- partitioning & replication ---------- */
const USERS = ['Aarav','Bhavna','Chirag','Divya','Farhan','Kavya','Meera','Nikhil','Priya','Rohan','Sana','Tanvi','Vikram','Zoya'];
function cmdPartition(a){
  const { f, pos } = flags(a);
  if (pos[0] !== 'users') fail('Usage: partition users --by name --shards 2');
  const k = +(f.shards || 2);
  if (![2,3,4].includes(k)) fail('Use 2, 3 or 4 shards.');
  if (f.by && f.by !== 'name') fail('This dataset is partitioned by name: --by name');
  const ranges = { 2:[['A','M'],['N','Z']], 3:[['A','H'],['I','Q'],['R','Z']], 4:[['A','F'],['G','M'],['N','S'],['T','Z']] }[k];
  const up = aliveNodes();
  S.parts = { table:'users', by:'name', factor:1, shards: ranges.map(([lo,hi], i)=>({ id:'S'+(i+1), range:`${lo}–${hi}`, lo, hi, nodes:[up[i].id], users: USERS.filter(u=>u[0] >= lo && u[0] <= hi) })) };
  S.parts.shards.forEach(s=>flashIds.add(s.nodes[0]));
  S.view = 'shards';
  rec({ op:'partition', k });
  return [
    ST(`✓ users (14 rows) split by name into ${k} shards — horizontal: by rows, not columns`),
    ...S.parts.shards.map(s=>`  ${K(s.id)}  ${pad(s.range, 4)} → ${pad(s.nodes[0], 8)} ${D(s.users.join(', '))}`),
    D('  Each node holds a subset and works its own shard in parallel.')
  ].join('\n');
}
function cmdReplicate(a){
  const { f, pos } = flags(a);
  if (pos[0] !== 'users') fail('Usage: replicate users --factor 3');
  if (!S.parts) fail('Nothing to replicate yet — partition users first.');
  const k = +(f.factor || 3);
  if (!(k >= 1 && k <= 5)) fail('Replication factor must be 1–5.');
  const up = aliveNodes().map(n=>n.id);
  S.parts.factor = k;
  S.parts.shards.forEach((s, i)=>{
    const prim = up.includes(s.nodes[0]) ? s.nodes[0] : up[i];
    const offs = [0, 4, 8, 2, 6];
    const list = [prim], pi = up.indexOf(prim);
    for (let slot=1; slot < k; slot++){
      for (let g=0; g < up.length; g++){
        const cand = up[(pi + offs[slot] + g) % up.length];
        if (!list.includes(cand)){ list.push(cand); break; }
      }
    }
    s.nodes = list; list.forEach(x=>flashIds.add(x));
  });
  S.view = 'shards';
  rec({ op:'replicate', k });
  return [
    ST(`✓ every shard now copied ${k}× on different nodes`),
    ...S.parts.shards.map(s=>`  ${K(s.id)}  ${pad(s.range, 4)} → ${s.nodes.join(', ')}`),
    D('  Copies must be kept in sync on every write — that cost is the seed of the CAP theorem.')
  ].join('\n');
}
function cmdUsers(a){
  if (a[0] !== 'get' || !a[1]) fail('Usage: users get <name>');
  if (!S.parts) fail('users is not partitioned yet.');
  const name = a[1][0].toUpperCase() + a[1].slice(1).toLowerCase();
  const sh = S.parts.shards.find(s=>name[0] >= s.lo && name[0] <= s.hi);
  const live = sh.nodes.filter(x=>nodeOf(x).up);
  if (!live.length){
    rec({ op:'users.get', name, ok:false });
    return ER(`✗ ${name}: shard ${sh.id} (${sh.range}) is unavailable — its only copy was on ${sh.nodes.join(', ')}, which is down.`) + '\n' + D('  Partitioning alone is fragile: lose a node, lose that piece.');
  }
  rec({ op:'users.get', name, ok:true, node:live[0], failover: live[0] !== sh.nodes[0] });
  const u = USERS.indexOf(name);
  return (u < 0 ? D(`no user named ${name} in shard ${sh.id}`) : `{ id: ${NU(101 + u)}, name: ${ST("'" + name + "'")}, city: ${ST("'" + ['Pune','Delhi','Surat','Chennai','Lucknow','Hyderabad','Mumbai','Indore','Mumbai','Jaipur','Kochi','Nagpur','Patna','Bhopal'][u] + "'")} }`) +
    '\n' + D(`  routed by name → shard ${sh.id} (${sh.range}) → answered by ${live[0]}${live[0] !== sh.nodes[0] ? ` (replica — ${sh.nodes[0]} is down)` : ''}`);
}

/* ---------- HDFS ---------- */
function placeBlock(k, repl){
  const up = aliveNodes().map(n=>n.id);
  const L = up.length, list = [];
  const base = (k*5 + 1) % L;
  [0, 3, 7].slice(0, repl).forEach(o=>{
    let j = (base + o) % L, g = 0;
    while (list.includes(up[j]) && g++ < L) j = (j + 1) % L;
    if (!list.includes(up[j])) list.push(up[j]);
  });
  return list;
}
function hdfsPath(p){ if (!p) return null; p = p.replace(/^hdfs:\/\/[^/]*/, ''); return p.startsWith('/') ? p : '/user/shopeasy/' + p; }
function cmdHdfs(a){
  if (S.nodes.length < 2) fail('HDFS needs a cluster of machines. Right now ShopEasy has one server — scale out first.');
  if (a[0] === 'dfs' || a[0] === 'fs') a = a.slice(1);
  const op = a[0];
  if (op === '-put' || op === '-copyFromLocal'){
    const src = a[1], dst = a[2] || '/';
    if (!src) fail('Usage: hdfs dfs -put <localfile> <hdfs-dir>');
    const lf = S.local[src];
    if (!lf) fail(`put: \`${src}': No such file or directory   (local files: ${Object.keys(S.local).join(', ')})`);
    let path = hdfsPath(dst);
    if (path.endsWith('/') || !/\.[a-z0-9]+$/i.test(path)) path = path.replace(/\/?$/, '/') + src;
    if (S.files[path]) fail(`put: \`${path}': File exists`);
    const repl = Math.min(3, aliveNodes().length), nb = Math.ceil(lf.mb / 128);
    const f = { mb:lf.mb, repl, nblocks:nb, mtime:'2026-10-27 10:' + String(10 + Object.keys(S.files).length*3).padStart(2,'0') };
    if (nb <= 24){
      f.blocks = [];
      for (let k=0;k<nb;k++) f.blocks.push({ id:'B' + (k+1), blk:'blk_' + (S.nextBlk++), mb: k < nb-1 ? 128 : lf.mb - 128*(nb-1), nodes: placeBlock(k, repl), text: lf.text ? lf.text[k] : null });
      f.blocks.forEach(b=>b.nodes.forEach(x=>flashIds.add(x)));
    } else {
      f.perNode = {};
      const up = aliveNodes();
      up.forEach((n,i)=>{ f.perNode[n.id] = Math.floor(nb*repl/up.length) + (i < (nb*repl) % up.length ? 1 : 0); flashIds.add(n.id); });
    }
    const perTB = lf.mb * repl / 1024 / 1024 / aliveNodes().length;
    aliveNodes().forEach(n=>n.usedTB = Math.min(n.diskTB, n.usedTB + perTB));
    S.files[path] = f; S.view = 'hdfs';
    rec({ op:'hdfs.put', path, src });
    const out = [D(`${src} (${fmtMB(lf.mb)}) → hdfs://namenode:8020${path}`), `  split into ${NU(num(nb))} block${nb>1?'s':''} of ${f.blocks && nb === 1 ? fmtMB(lf.mb) : '128 MB'} · each copied ${NU(repl + '×')} on different nodes`];
    if (f.blocks) f.blocks.forEach(b=>out.push(`  ${K(pad(b.id, 3))} ${D(pad(fmtMB(b.mb), 7))} → ${b.nodes.join(', ')}`));
    else out.push(`  ${NU(num(nb*repl))} block replicas spread over ${aliveNodes().length} nodes (~${num(Math.round(nb*repl/aliveNodes().length))} each)`);
    out.push(ST('✓ stored') + D(' — bigger than any one disk? add nodes. A node dies? its blocks live on elsewhere.'));
    return out.join('\n');
  }
  if (op === '-ls'){
    const dir = (hdfsPath(a[1] || '/') || '/').replace(/\/?$/, '/');
    rec({ op:'hdfs.ls', dir });
    const list = Object.keys(S.files).filter(p=>p.startsWith(dir === '//' ? '/' : dir)).sort();
    if (!list.length) return Object.keys(S.files).length ? D(`ls: \`${dir}': empty`) : D('(HDFS is empty — hdfs dfs -put a file first)');
    return `Found ${list.length} item${list.length>1?'s':''}\n` + list.map(p=>{ const f = S.files[p]; return `-rw-r--r--   ${NU(f.repl)} shopeasy supergroup ${lpad(Math.round(f.mb*1048576), 14)} ${f.mtime} ${ST(p)}`; }).join('\n');
  }
  if (op === '-du'){
    rec({ op:'hdfs.du' });
    const list = Object.keys(S.files).sort();
    return list.map(p=>`${pad(fmtMB(S.files[p].mb), 9)} ${pad(fmtMB(S.files[p].mb*S.files[p].repl), 9)} ${ST(p)}`).join('\n') || D('(empty)');
  }
  if (op === '-cat'){
    const p = hdfsPath(a[1]); const f = S.files[p];
    if (!f) fail(`cat: \`${a[1]}': No such file or directory`);
    rec({ op:'hdfs.cat', p });
    return f.blocks && f.blocks[0].text ? f.blocks.map(b=>b.text).join('\n') : D('(binary / very large file — first block:) ') + 'GET /cart 200 14ms\nGET /checkout 500 2011ms\nPOST /pay 200 87ms\n' + D('…');
  }
  if (op === 'fsck'){
    const p = hdfsPath(a[1] || '/');
    const paths = p === '/' ? Object.keys(S.files) : [p];
    if (p !== '/' && !S.files[p]) fail(`fsck: ${a[1]} does not exist`);
    rec({ op:'hdfs.fsck', p });
    S.view = 'hdfs';
    const up = aliveNodes().length;
    const out = [D('Connecting to namenode via http://namenode:9870'), D(`FSCK started by shopeasy for path ${p} at Tue Oct 27 10:31:07 IST 2026`)];
    let tb = 0, under = 0;
    paths.forEach(pp=>{
      const f = S.files[pp];
      tb += f.nblocks;
      out.push(`${ST(pp)} ${Math.round(f.mb*1048576)} bytes, replicated: replication=${f.repl}, ${num(f.nblocks)} block(s):  ${ST('OK')}`);
      if (f.blocks) f.blocks.forEach((b,i)=>{
        if (b.nodes.length < f.repl) under++;
        out.push(`${i}. BP-929597290:${b.blk} len=${Math.round(b.mb*1048576)} ${b.nodes.length < f.repl ? ER('Live_repl=' + b.nodes.length) : 'Live_repl=' + b.nodes.length}  [${b.nodes.map(x=>K(x)).join(', ')}]`);
      });
      else out.push(D(`   (${num(f.nblocks)} blocks — locations summarised: ${Object.entries(f.perNode).map(([n,c])=>n + ':' + num(c)).join(' ')})`));
    });
    out.push('', `Status: ${under ? ER('UNDER-REPLICATED') : ST('HEALTHY')}`,
      ` Number of data-nodes:  ${up}${S.nodes.length - up ? ER('   (dead: ' + S.nodes.filter(n=>!n.up).map(n=>n.id).join(', ') + ')') : ''}`,
      ` Total blocks (validated): ${num(tb)}`,
      ` Under-replicated blocks:  ${under}`,
      ` Default replication factor: 3`,
      ` Missing blocks: 0`);
    return out.join('\n');
  }
  if (op === 'dfsadmin'){
    rec({ op:'hdfs.report' });
    const up = aliveNodes(), cap = up.reduce((t,n)=>t+n.diskTB,0), used = up.reduce((t,n)=>t+n.usedTB,0);
    return [`Configured Capacity: ${fmtTB(cap)}`, `DFS Used: ${fmtTB(used)} (${(100*used/cap).toFixed(2)}%)`, `DFS Remaining: ${fmtTB(cap-used)}`, '-------------------------------------------------',
      `Live datanodes (${up.length}):  ${up.map(n=>n.id).join(' ')}`, `Dead datanodes (${S.nodes.length - up.length}):  ${S.nodes.filter(n=>!n.up).map(n=>ER(n.id)).join(' ') || '—'}`].join('\n');
  }
  fail('Usage: hdfs dfs -put <file> <dir> | hdfs dfs -ls <dir> | hdfs fsck <path> -files -blocks -locations | hdfs dfsadmin -report');
}

/* ---------- Spark ---------- */
function wordCounts(f, b, k){
  if (b && b.text){ const c = {}; b.text.split(/\s+/).filter(Boolean).forEach(w=>c[w] = (c[w]||0) + 1); return c; }
  const w = ['page_view','add_to_cart','search','checkout'];
  const c = {}; w.forEach((x,i)=>c[x] = 410000 - i*92000 + ((k*7919 + i*104729) % 37000)); return c;
}
function cmdSpark(a){
  const { f, pos } = flags(a);
  const app = pos[0];
  if (!app) fail('Usage: spark-submit wordcount.py <hdfs-path> [--executors n]');
  if (!/wordcount/.test(app)) fail(`${app}: not found. Available: wordcount.py`);
  if (S.nodes.length < 2) fail('Spark needs a cluster — ShopEasy has a single server.');
  const p = hdfsPath(pos[1]);
  if (!p) fail('Which input? e.g. spark-submit wordcount.py /data/events-2026.log');
  const file = S.files[p];
  if (!file) fail(`AnalysisException: Path does not exist: hdfs://namenode:8020${p}`);
  if (!file.blocks) fail('This demo counts words in files up to 24 blocks. Try a smaller file.');
  const ex = Math.max(1, Math.min(64, +(f.executors || f['num-executors'] || 1)));
  const loc = !(f.locality === 'off' || f.locality === 'false');
  const failNode = typeof f.fail === 'string' ? f.fail : null;
  if (failNode) needNode(failNode);
  const up = aliveNodes().map(n=>n.id);
  const tasks = file.blocks.map((b, k)=>{
    const local = b.nodes.find(x=>up.includes(x));
    let host = loc ? local : up.filter(x=>!b.nodes.includes(x))[k % Math.max(1, up.filter(x=>!b.nodes.includes(x)).length)] || local;
    return { k, b, host, counts: wordCounts(file, b, k) };
  });
  const out = [D(`INFO SparkContext: Running Spark version 3.5.1 · app wordcount.py · ${ex} executor${ex>1?'s':''}`)];
  if (ex === 1) out.push(D('  (1 executor — tasks run one after another. Pass --executors n to run them in parallel.)'));
  const showText = !!file.blocks[0].text;
  out.push(K(`Stage 0 · MAP`) + D(` — ${tasks.length} tasks, one per block${loc ? ', each sent to a node that already holds the block' : ', locality OFF: blocks pulled across the network'}`));
  let retried = null;
  tasks.forEach(t=>{
    let line = `  task ${t.k}  ${pad(t.b.id, 3)} on ${pad(t.host, 9)} ${loc ? ST('NODE_LOCAL') : ER('ANY       ')}  `;
    if (failNode && t.host === failNode && !retried){
      const alt = t.b.nodes.find(x=>x !== failNode && up.includes(x)) || up.find(x=>x !== failNode);
      line += ER(`✗ lost (${failNode} died)`) + `  ${ST('↻ retried on ' + alt)}`;
      retried = { task:t.k, from:failNode, to:alt }; t.host = alt;
    } else {
      if (retried && t.host === failNode){ t.host = t.b.nodes.find(x=>x !== failNode && up.includes(x)) || up.find(x=>x !== failNode); line = `  task ${t.k}  ${pad(t.b.id, 3)} on ${pad(t.host, 9)} ${ST('NODE_LOCAL')}  `; }
      line += D(Object.entries(t.counts).slice(0, 3).map(([w,c])=>`${w}:${cnt(c)}`).join(' '));
    }
    out.push(line);
  });
  if (failNode && !retried){
    const t = tasks[0]; const alt = t.b.nodes.find(x=>x !== failNode && up.includes(x)) || up[0];
    out.push(`  ${ER(`✗ task 0 attempt 1 lost on ${failNode}`)}  ${ST('↻ retried on ' + alt)}`);
    retried = { task:0, from:failNode, to:alt };
  }
  const tot = {};
  tasks.forEach(t=>Object.entries(t.counts).forEach(([w,c])=>{ (tot[w] = tot[w] || []).push(c); }));
  out.push(K('Stage 1 · SHUFFLE') + D(' — group the partial counts by word'));
  Object.entries(tot).slice(0,4).forEach(([w, cs])=>out.push(`  ${pad(w, 12)} → ${D(cs.slice(0, 4).map(c=>cnt(c)).join(', ') + (cs.length > 4 ? `, … (${cs.length} partials)` : ''))}`));
  out.push(K('Stage 1 · REDUCE') + D(' — combine'));
  out.push('  ' + Object.entries(tot).slice(0,4).map(([w, cs])=>`${ST(w)} → ${NU(cnt(cs.reduce((x,y)=>x+y,0)))}`).join('   '));
  const per = showText ? 20 : 40, waves = Math.ceil(tasks.length / ex);
  const netPer = loc ? 0 : Math.round(Math.min(ex, tasks.length) * 128 / 25);
  const secs = waves * (per + netPer) + (retried ? per : 0) + 4;
  const moved = loc ? `${tasks.length * 4} KB of code shipped to the data` : `${fmtMB(file.mb)} of data dragged across the network`;
  out.push(ST(`✓ Job 0 finished in ${secs} s`) + `  ${D('·')} ${ex} executor${ex>1?'s':''} ${D('·')} ${loc ? ST(moved) : ER(moved)}${retried ? D(' · 1 task retried') : ''}`);
  rec({ op:'spark', path:p, ex, loc, secs, failNode, retried: !!retried });
  S.lastJob = { ex, loc, secs };
  return out.join('\n');
}

/* ---------- batch & Kafka ---------- */
function cmdBatch(a){
  if (a[0] !== 'run') fail('Usage: batch run eod-report');
  rec({ op:'batch' });
  return [
    D('job eod-report · scheduled daily at 02:00 (Spark on YARN)'),
    `  00:00 → 23:59  collect   ${bar(1, 18)}  ${NU('41,233,904')} transactions at rest in HDFS`,
    `  02:00          start     ${D('read the whole day in one pass · 240 tasks')}`,
    `  02:47          done      ${ST('end-of-day report written to /reports/2026-10-27/')}`,
    `  08:00          read      ${D('the branch manager opens it with her coffee')}`,
    ST('✓ high throughput, cost-effective — but the answer is hours old (latency: ~6–30 h)')
  ].join('\n');
}
function cmdKafka(a){
  const { f } = flags(a);
  if (f.create && !f.topic && typeof f.create === 'string') f.topic = f.create;
  if (f.list){ rec({ op:'kafka.list' }); return S.kafka.topics.join('\n') || D('(no topics)'); }
  if (!f.create) fail('Usage: kafka-topics --create --topic payments');
  const t = f.topic;
  if (!t || t === true) fail('Name the topic: kafka-topics --create --topic payments');
  if (S.kafka.topics.includes(t)) fail(`Topic '${t}' already exists.`);
  S.kafka.topics.push(t); S.view = 'stream';
  rec({ op:'kafka.create', topic:t });
  return ST(`Created topic ${t}.`) + D(`  (3 partitions, replication-factor 3 — Kafka is the pipe: it ingests and transports events)`);
}

/* ---------- CAP ---------- */
function capFresh(){ return { link:true, mode:'CP', dc:{ mumbai:10000, delhi:10000 } }; }
function cmdCap(a){
  if (!S.cap) S.cap = capFresh();
  const c = S.cap; S.view = 'cap';
  const { f, pos } = flags(a);
  const dcName = d => { d = String(d || '').toLowerCase(); if (!(d in c.dc)) fail(`Unknown data centre '${d}'. Use --dc mumbai or --dc delhi`); return d; };
  const head = () => `  mumbai ₹${NU(num(c.dc.mumbai))}   ${c.link ? ST('⇆ link healthy') : ER('✕ link broken')}   delhi ₹${NU(num(c.dc.delhi))}   ${D('mode ' + c.mode)}`;
  if (pos[0] === 'status' || !pos.length){
    rec({ op:'cap.status' });
    return [B('account #4471 · balance replicated in two data centres'), head(), D(c.link ? '  In sync: a write in one is visible in the other before the next read.' : '  The data centres can’t talk. Every request must now choose: correct, or available?')].join('\n');
  }
  if (pos[0] === 'partition'){
    if (!c.link) return D('Already partitioned.');
    c.link = false; rec({ op:'cap.partition' });
    return ER('✕ fibre cut between mumbai and delhi — messages dropped') + '\n' + head() + '\n' + D('  Both data centres are up; they just can’t reach each other. That’s a network partition (P).');
  }
  if (pos[0] === 'heal'){
    if (c.link) return D('The link is already healthy.');
    c.link = true;
    const v = c.dc.mumbai; const was = c.dc.delhi; c.dc.delhi = v;
    rec({ op:'cap.heal' });
    return ST('✓ link restored') + (was !== v ? `\n  ${ST('↻ reconcile')} delhi ₹${num(was)} → ₹${NU(num(v))}  ${D('(copies brought back in sync)')}` : '') + '\n' + head();
  }
  if (pos[0] === 'mode'){
    const m = String(pos[1] || '').toUpperCase();
    if (m !== 'CP' && m !== 'AP') fail('Usage: cap mode CP | cap mode AP');
    c.mode = m; rec({ op:'cap.mode', mode:m });
    return ST(`✓ mode ${m}`) + D(m === 'CP' ? '  — during a partition, refuse rather than serve stale data' : '  — during a partition, always answer, even with possibly stale data');
  }
  if (pos[0] === 'write'){
    const amt = +String(pos[2] || '').replace(/[₹,]/g, '');
    if (pos[1] !== 'balance' || !(amt >= 0)) fail('Usage: cap write balance 7000 --dc mumbai');
    const d = dcName(f.dc || 'mumbai');
    c.dc[d] = amt;
    const o = d === 'mumbai' ? 'delhi' : 'mumbai';
    if (c.link) c.dc[o] = amt;
    rec({ op:'cap.write', dc:d, amt, link:c.link });
    return ST(`✓ ${d}: balance = ₹${num(amt)}`) + (c.link ? D(`  · replicated to ${o} instantly`) : ER(`  · could NOT reach ${o} — it still says ₹${num(c.dc[o])}`)) + '\n' + head();
  }
  if (pos[0] === 'read'){
    const d = dcName(f.dc || 'delhi');
    if (c.link){
      rec({ op:'cap.read', dc:d, mode:c.mode, result:'fresh', link:true });
      return `${d} → balance ₹${NU(num(c.dc[d]))}  ${ST('✓ consistent AND available')}` + D(`  (network healthy — mode ${c.mode} costs nothing right now)`);
    }
    const stale = c.dc[d] !== Math.max(...Object.values(c.dc)) || c.dc.mumbai !== c.dc.delhi;
    if (c.mode === 'CP'){
      rec({ op:'cap.read', dc:d, mode:'CP', result:'refused', link:false });
      return ER(`✗ ${d}: 503 — cannot confirm the latest balance with mumbai. Please retry.`) + '\n' + D('  CP: sacrificed availability to stay consistent. The user waits, but never sees a wrong number.');
    }
    rec({ op:'cap.read', dc:d, mode:'AP', result: stale ? 'stale' : 'fresh', link:false });
    return `${d} → balance ₹${NU(num(c.dc[d]))}  ${stale ? K('⚠ possibly stale') : ''}` + '\n' + D(`  AP: always answers${stale ? ` — but mumbai already says ₹${num(c.dc.mumbai)}` : ''}. Copies reconcile when the link heals.`);
  }
  fail('Usage: cap status | cap partition | cap mode CP|AP | cap write balance <n> --dc <dc> | cap read balance --dc <dc> | cap heal');
}

/* ---------- lake & warehouse ---------- */
function cmdLake(a){
  if (a[0] === 'ls'){
    rec({ op:'lake.ls' }); S.view = 'lake';
    const list = Object.keys(S.lake).sort();
    return D('s3://shopeasy-lake/') + '\n' + list.map(p=>`  ${pad(fmtMB(S.lake[p].mb), 8)} ${ST(p)}  ${D(S.lake[p].kind)}`).join('\n');
  }
  if (a[0] === 'put'){
    const src = a[1];
    const lf = src && S.local[src];
    if (!lf) fail(`lake put: no local file '${src || ''}'. Local files: ${Object.keys(S.local).join(', ')}`);
    const p = 'raw/' + src;
    S.lake[p] = { mb:lf.mb, kind:lf.kind, rows:lf.rows || null };
    flashIds.add('lake:' + p); S.view = 'lake';
    rec({ op:'lake.put', p });
    return ST(`✓ ${src} → s3://shopeasy-lake/${p}`) + D(`  (${fmtMB(lf.mb)}, stored as-is — no schema checked, nothing rejected)`) + '\n' + D('  Structure gets applied later, when someone reads it: schema-on-read.');
  }
  fail('Usage: lake ls | lake put <file>');
}
function cmdWarehouse(a){
  const { f, pos } = flags(a);
  if (pos[0] !== 'load' || !pos[1]) fail('Usage: warehouse load <file> --table <table>');
  const src = pos[1].replace(/^raw\//, '');
  const lf = S.local[src] || S.lake['raw/' + src];
  if (!lf) fail(`no file '${pos[1]}'. Local files: ${Object.keys(S.local).join(', ')}`);
  const t = f.table || 'events';
  rec({ op:'warehouse.load', src, t, ok:false });
  if (lf.kind === 'image'){
    return ER(`✗ COPY ${t} FROM '${src}': not a delimited/JSON file — 0 rows loaded.`) + '\n' + D('  A warehouse only takes structured rows. Images, audio and video don’t fit.');
  }
  return [
    D(`COPY INTO ${t} FROM @landing/${src}  ·  target schema: ${t}(user_id INT, event STRING, ts TIMESTAMP)`),
    ER(`✗ row 1: column 'user' is STRING "u_8812", expected INT user_id`),
    ER(`✗ row 1: unexpected field 'device' (object) — not in schema`),
    ER(`✗ row 3: missing required field 'ts'`),
    ER(`✗ load aborted — 0 of ${num(lf.rows ? lf.rows.length * 50000 : 250000)} rows loaded`),
    D('  Schema-on-write: if it doesn’t fit the catalogued shelf, it doesn’t get in.')
  ].join('\n');
}
function cmdPipeline(a){
  const m = String(a[1] || a[0] || '').toLowerCase();
  if (a[0] !== 'run' || !/^(etl|elt)$/.test(m)) fail('Usage: pipeline run etl | pipeline run elt');
  rec({ op:'pipeline', m });
  if (m === 'etl') return [
    B('ETL') + D(' — the classic approach (Informatica / Talend → Teradata)'),
    `  ${K('E')}xtract    mysql.orders, crm.customers      ${D('→ staging server')}`,
    `  ${K('T')}ransform  on the staging server:           ${D('mask PAN & phone · validate · dedupe · fix dates')}`,
    `  ${K('L')}oad       only clean rows → warehouse        ${D('raw data is never kept in the warehouse')}`,
    ST('✓ 3.1 M clean rows loaded') + D('  · regulated data is cleaned and masked BEFORE it lands — why banks & healthcare prefer it')
  ].join('\n');
  return [
    B('ELT') + D(' — the cloud approach (Fivetran → Snowflake / BigQuery, transformed with dbt)'),
    `  ${K('E')}xtract    mysql.orders, crm.customers, app events`,
    `  ${K('L')}oad       everything raw → warehouse          ${D('storage is cheap now')}`,
    `  ${K('T')}ransform  inside the warehouse, in SQL       ${D('its own elastic compute does the heavy lifting')}`,
    ST('✓ raw kept — re-transform any time the question changes') + D('  · why Netflix, Airbnb & modern e-commerce prefer it')
  ].join('\n');
}

/* ---------- checkpoint answers ---------- */
function cmdAnswer(a){
  const v = String(a[0] || '').toUpperCase().replace(/[^A-Z]/g, '');
  if (!/^[A-H]$/.test(v)) fail('Usage: answer A   (or B, C, D)');
  rec({ op:'answer', v });
  return D(`answer → ${v}`);
}

/* ============================================================
   SPARK SQL — tokenizer · parser · evaluator
   ============================================================ */
const KW = new Set('SELECT DISTINCT FROM WHERE GROUP BY HAVING ORDER ASC DESC LIMIT JOIN INNER LEFT ON AS AND OR NOT IN LIKE IS NULL CREATE TABLE SHOW TABLES DESCRIBE DESC EXPLAIN DROP BETWEEN TRUE FALSE CASE WHEN THEN ELSE END'.split(' '));
function sqlTokens(src){
  const t = []; let i = 0;
  while (i < src.length){
    const ch = src[i];
    if (/\s/.test(ch)){ i++; continue; }
    if (src.startsWith('--', i)){ while (i < src.length && src[i] !== '\n') i++; continue; }
    if (ch === "'" || ch === '"'){
      let j = i + 1, s = '';
      while (j < src.length && !(src[j] === ch && src[j+1] !== ch)){ if (src[j] === ch) j++; s += src[j++]; }
      if (j >= src.length) fail('[PARSE_SYNTAX_ERROR] unclosed string literal');
      t.push({ k:'str', v:s }); i = j + 1; continue;
    }
    if (ch === '`'){ const j = src.indexOf('`', i+1); if (j < 0) fail('[PARSE_SYNTAX_ERROR] unclosed `'); t.push({ k:'id', v:src.slice(i+1, j), q:true }); i = j + 1; continue; }
    if (/\d/.test(ch) || (ch === '.' && /\d/.test(src[i+1]||''))){ const m = src.slice(i).match(/^\d*\.?\d+(e[+-]?\d+)?/i); t.push({ k:'num', v:+m[0] }); i += m[0].length; continue; }
    if (/[A-Za-z_]/.test(ch)){
      const m = src.slice(i).match(/^[A-Za-z_][A-Za-z0-9_]*/)[0];
      const U = m.toUpperCase();
      t.push(KW.has(U) ? { k:'kw', v:U } : { k:'id', v:m });
      i += m.length; continue;
    }
    const two = src.slice(i, i+2);
    if (['<=','>=','<>','!=','=='].includes(two)){ t.push({ k:'op', v: two === '==' ? '=' : two === '<>' ? '!=' : two }); i += 2; continue; }
    if ('=<>+-*/(),.;%'.includes(ch)){ t.push({ k:'op', v:ch }); i++; continue; }
    fail(`[PARSE_SYNTAX_ERROR] unexpected character '${ch}'`);
  }
  return t;
}
function sqlParse(src){
  const T = sqlTokens(src); let p = 0;
  const peek = (o=0) => T[p+o], at = (k, v) => T[p] && T[p].k === k && (v === undefined || T[p].v === v);
  const kw = v => at('kw', v), op = v => at('op', v);
  const eat = (k, v) => { if (!at(k, v)) fail(`[PARSE_SYNTAX_ERROR] Syntax error at or near ${T[p] ? "'" + T[p].v + "'" : 'end of input'}${v ? ` (expected ${v})` : ''}`); return T[p++]; };
  const ident = () => { if (at('id')) return T[p++].v; if (at('kw') && !['FROM','WHERE','GROUP','ORDER','LIMIT','JOIN','ON','HAVING','AS','AND','OR','INNER','LEFT'].includes(T[p].v)) return T[p++].v.toLowerCase(); fail(`[PARSE_SYNTAX_ERROR] expected a name near ${T[p] ? "'" + T[p].v + "'" : 'end of input'}`); };

  function expr(){ return orE(); }
  function orE(){ let l = andE(); while (kw('OR')){ p++; l = { t:'bin', o:'OR', l, r:andE() }; } return l; }
  function andE(){ let l = notE(); while (kw('AND')){ p++; l = { t:'bin', o:'AND', l, r:notE() }; } return l; }
  function notE(){ if (kw('NOT')){ p++; return { t:'not', e:notE() }; } return cmpE(); }
  function cmpE(){
    let l = addE();
    for (;;){
      if (T[p] && T[p].k === 'op' && ['=','!=','<','>','<=','>='].includes(T[p].v)){ const o = T[p++].v; l = { t:'bin', o, l, r:addE() }; continue; }
      let neg = false;
      if (kw('NOT') && peek(1) && peek(1).k === 'kw' && ['IN','LIKE','BETWEEN'].includes(peek(1).v)){ p++; neg = true; }
      if (kw('IN')){ p++; eat('op','('); const list = [expr()]; while (op(',')){ p++; list.push(expr()); } eat('op',')'); l = { t:'in', e:l, list, neg }; continue; }
      if (kw('LIKE')){ p++; l = { t:'like', e:l, pat:addE(), neg }; continue; }
      if (kw('BETWEEN')){ p++; const lo = addE(); eat('kw','AND'); const hi = addE(); l = { t:'between', e:l, lo, hi, neg }; continue; }
      if (kw('IS')){ p++; let n = false; if (kw('NOT')){ p++; n = true; } eat('kw','NULL'); l = { t:'isnull', e:l, neg:n }; continue; }
      return l;
    }
  }
  function addE(){ let l = mulE(); while (op('+') || op('-')){ const o = T[p++].v; l = { t:'bin', o, l, r:mulE() }; } return l; }
  function mulE(){ let l = unE(); while (op('*') || op('/') || op('%')){ const o = T[p++].v; l = { t:'bin', o, l, r:unE() }; } return l; }
  function unE(){ if (op('-')){ p++; return { t:'neg', e:unE() }; } return prim(); }
  function prim(){
    if (at('num')) return { t:'lit', v:T[p++].v };
    if (at('str')) return { t:'lit', v:T[p++].v };
    if (kw('NULL')){ p++; return { t:'lit', v:null }; }
    if (kw('TRUE') || kw('FALSE')){ return { t:'lit', v:T[p++].v === 'TRUE' }; }
    if (op('(')){ p++; const e = expr(); eat('op',')'); return e; }
    if (kw('CASE')){
      p++; const whens = [];
      while (kw('WHEN')){ p++; const c = expr(); eat('kw','THEN'); whens.push([c, expr()]); }
      let el = null; if (kw('ELSE')){ p++; el = expr(); } eat('kw','END');
      return { t:'case', whens, el };
    }
    if (op('*')){ p++; return { t:'star' }; }
    const n = ident();
    if (op('(')){
      p++; const fn = n.toUpperCase();
      if (op('*')){ p++; eat('op',')'); return { t:'fn', fn, star:true, args:[] }; }
      let distinct = false; if (kw('DISTINCT')){ p++; distinct = true; }
      const args = []; if (!op(')')){ args.push(expr()); while (op(',')){ p++; args.push(expr()); } }
      eat('op',')');
      return { t:'fn', fn, args, distinct };
    }
    if (op('.')){ p++; if (op('*')){ p++; return { t:'star', q:n }; } return { t:'col', q:n, c:ident() }; }
    return { t:'col', c:n };
  }
  function tref(){
    let name, path = null, fmt = null;
    const first = eat('id').v;
    if (op('.') && peek(1) && peek(1).k === 'id' && peek(1).q){ p++; path = T[p++].v; fmt = first.toLowerCase(); name = path; }
    else name = first;
    let alias = null;
    if (kw('AS')){ p++; alias = ident(); } else if (at('id')) alias = T[p++].v;
    return { name, path, fmt, alias: alias || (path ? null : name) };
  }
  function select(){
    eat('kw','SELECT');
    let distinct = false; if (kw('DISTINCT')){ p++; distinct = true; }
    const items = [];
    do {
      if (items.length) p++;
      const e = expr(); let as = null;
      if (kw('AS')){ p++; as = ident(); } else if (at('id')) as = T[p++].v;
      items.push({ e, as });
    } while (op(','));
    eat('kw','FROM');
    const from = tref(), joins = [];
    while (kw('JOIN') || kw('INNER') || kw('LEFT')){
      let left = false; if (kw('LEFT')){ p++; left = true; } else if (kw('INNER')) p++;
      eat('kw','JOIN'); const t = tref(); eat('kw','ON'); joins.push({ t, on:expr(), left });
    }
    let where = null, group = [], having = null, order = [], limit = null;
    if (kw('WHERE')){ p++; where = expr(); }
    if (kw('GROUP')){ p++; eat('kw','BY'); group.push(expr()); while (op(',')){ p++; group.push(expr()); } }
    if (kw('HAVING')){ p++; having = expr(); }
    if (kw('ORDER')){ p++; eat('kw','BY'); do { if (order.length) p++; const e = expr(); let desc = false; if (kw('ASC')) p++; else if (kw('DESC')){ p++; desc = true; } order.push({ e, desc }); } while (op(',')); }
    if (kw('LIMIT')){ p++; limit = eat('num').v; }
    return { t:'select', distinct, items, from, joins, where, group, having, order, limit };
  }
  let st;
  if (kw('SELECT')) st = select();
  else if (kw('EXPLAIN')){ p++; st = { t:'explain', q:select() }; }
  else if (kw('CREATE')){ p++; eat('kw','TABLE'); const name = ident(); eat('kw','AS'); st = { t:'ctas', name, q:select() }; }
  else if (kw('SHOW')){ p++; eat('kw','TABLES'); st = { t:'show' }; }
  else if (kw('DESCRIBE') || kw('DESC')){ p++; if (kw('TABLE')) p++; st = { t:'describe', name:ident() }; }
  else if (kw('DROP')){ p++; eat('kw','TABLE'); st = { t:'drop', name:ident() }; }
  else fail('[PARSE_SYNTAX_ERROR] Statements start with SELECT, CREATE TABLE … AS SELECT, EXPLAIN, SHOW TABLES or DESCRIBE.');
  while (op(';')) p++;
  if (p < T.length) fail(`[PARSE_SYNTAX_ERROR] Syntax error at or near '${T[p].v}'`);
  return st;
}

const AGG = new Set(['COUNT','SUM','AVG','MIN','MAX']);
const hasAgg = e => !!e && (e.t === 'fn' && AGG.has(e.fn) || ['l','r','e','pat','lo','hi','el'].some(k=>e[k] && typeof e[k] === 'object' && hasAgg(e[k])) || (e.args || []).some(hasAgg) || (e.list || []).some(hasAgg) || (e.whens || []).some(w=>hasAgg(w[0]) || hasAgg(w[1])));
function nameOf(e){
  switch (e.t){
    case 'col': return e.c;
    case 'lit': return e.v === null ? 'NULL' : String(e.v);
    case 'fn': return `${e.fn.toLowerCase()}(${e.star ? '1' : (e.distinct ? 'DISTINCT ' : '') + e.args.map(nameOf).join(', ')})`;
    case 'bin': return `(${nameOf(e.l)} ${e.o} ${nameOf(e.r)})`;
    case 'neg': return `(- ${nameOf(e.e)})`;
    default: return 'expr';
  }
}
function lookupCol(row, e){
  if (e.q){
    const k = e.q + '.' + e.c;
    const hit = Object.keys(row).find(x=>x.toLowerCase() === k.toLowerCase());
    if (hit === undefined) fail(`[UNRESOLVED_COLUMN] A column or function parameter with name \`${e.q}\`.\`${e.c}\` cannot be resolved.`);
    return row[hit];
  }
  const hit = Object.keys(row).find(x=>x.toLowerCase() === e.c.toLowerCase());
  if (hit === undefined) fail(`[UNRESOLVED_COLUMN] A column or function parameter with name \`${e.c}\` cannot be resolved. Did you mean one of: ${Object.keys(row).filter(k=>!k.includes('.')).map(k=>'`'+k+'`').join(', ')}?`);
  return row[hit];
}
function likeRe(p){ return new RegExp('^' + String(p).replace(/[.*+?^${}()|[\]\\]/g, '\\$&').replace(/%/g, '.*').replace(/_/g, '.') + '$', 'i'); }
function ev(e, row, grp){
  switch (e.t){
    case 'lit': return e.v;
    case 'col': return lookupCol(row, e);
    case 'neg': return -ev(e.e, row, grp);
    case 'not': return !truthy(ev(e.e, row, grp));
    case 'star': fail('[INVALID_USAGE_OF_STAR] * is only allowed in the SELECT list or COUNT(*).');
    case 'in': { const v = ev(e.e, row, grp); const r = e.list.some(x=>eq(ev(x, row, grp), v)); return e.neg ? !r : r; }
    case 'like': { const v = ev(e.e, row, grp); const r = v !== null && likeRe(ev(e.pat, row, grp)).test(String(v)); return e.neg ? !r : r; }
    case 'between': { const v = ev(e.e, row, grp); const r = v >= ev(e.lo, row, grp) && v <= ev(e.hi, row, grp); return e.neg ? !r : r; }
    case 'isnull': { const v = ev(e.e, row, grp); return e.neg ? v !== null && v !== undefined : v === null || v === undefined; }
    case 'case': { for (const [c, v] of e.whens) if (truthy(ev(c, row, grp))) return ev(v, row, grp); return e.el ? ev(e.el, row, grp) : null; }
    case 'bin': {
      if (e.o === 'AND') return truthy(ev(e.l, row, grp)) && truthy(ev(e.r, row, grp));
      if (e.o === 'OR') return truthy(ev(e.l, row, grp)) || truthy(ev(e.r, row, grp));
      const l = ev(e.l, row, grp), r = ev(e.r, row, grp);
      if (l === null || r === null) return null;
      switch (e.o){
        case '=': return eq(l, r); case '!=': return !eq(l, r);
        case '<': return l < r; case '>': return l > r; case '<=': return l <= r; case '>=': return l >= r;
        case '+': return l + r; case '-': return l - r; case '*': return l * r; case '/': return r === 0 ? null : l / r; case '%': return l % r;
      }
      return null;
    }
    case 'fn': {
      if (AGG.has(e.fn)){
        if (!grp) fail(`[MISSING_GROUP_BY] ${e.fn.toLowerCase()}() needs GROUP BY or a whole-table aggregate.`);
        if (e.star) return grp.length;
        let vals = grp.map(r=>ev(e.args[0], r, null)).filter(v=>v !== null && v !== undefined);
        if (e.distinct) vals = [...new Map(vals.map(v=>[JSON.stringify(v), v])).values()];
        if (e.fn === 'COUNT') return vals.length;
        if (!vals.length) return null;
        if (e.fn === 'SUM') return vals.reduce((a,b)=>a+b, 0);
        if (e.fn === 'AVG') return vals.reduce((a,b)=>a+b, 0) / vals.length;
        if (e.fn === 'MIN') return vals.reduce((a,b)=>b < a ? b : a);
        if (e.fn === 'MAX') return vals.reduce((a,b)=>b > a ? b : a);
      }
      const args = e.args.map(x=>ev(x, row, grp));
      switch (e.fn){
        case 'ROUND': return args[0] === null ? null : +Number(args[0]).toFixed(args[1] || 0);
        case 'UPPER': return args[0] === null ? null : String(args[0]).toUpperCase();
        case 'LOWER': return args[0] === null ? null : String(args[0]).toLowerCase();
        case 'COALESCE': return args.find(v=>v !== null && v !== undefined) ?? null;
        case 'LENGTH': return args[0] === null ? null : String(args[0]).length;
      }
      fail(`[UNRESOLVED_ROUTINE] Cannot resolve function \`${e.fn.toLowerCase()}\`.`);
    }
  }
  fail('unsupported expression');
}
const truthy = v => v === true || (v !== null && v !== undefined && v !== false && v !== 0);
const eq = (a, b) => (typeof a === 'string' && typeof b === 'string') ? a === b : a == b;

function tableFor(tr){
  if (tr.path){
    const p = tr.path.replace(/^s3:\/\/shopeasy-lake\//, '');
    const f = S.lake[p];
    if (!f) fail(`[PATH_NOT_FOUND] Path does not exist: s3://shopeasy-lake/${p}` + (Object.keys(S.local).some(x=>p.endsWith(x)) ? `   (it’s still on your laptop — lake put ${p.replace(/^raw\//,'')} first)` : ''));
    if (!f.rows) fail(`[UNSUPPORTED_FILE] ${p} is not a ${tr.fmt} file.`);
    const cols = []; f.rows.forEach(r=>Object.keys(r).forEach(k=>{ if (!cols.includes(k)) cols.push(k); }));
    return { name:p, cols, rows: f.rows.map(r=>cols.map(c=>r[c] === undefined ? null : (isObj(r[c]) ? JSON.stringify(r[c]) : r[c]))), format:'json', totalCols:cols.length, colMB:f.mb / cols.length, lake:true };
  }
  const t = S.tables[tr.name] || S.tables[Object.keys(S.tables).find(k=>k.toLowerCase() === tr.name.toLowerCase())];
  if (!t) fail(`[TABLE_OR_VIEW_NOT_FOUND] The table or view \`${tr.name}\` cannot be found. Try SHOW TABLES;`);
  return t;
}
function colsUsed(q){
  const used = new Set(); let star = false;
  const walk = e => { if (!e || typeof e !== 'object') return; if (e.t === 'col') used.add(e.c.toLowerCase()); if (e.t === 'star') star = true; Object.values(e).forEach(v=>{ if (Array.isArray(v)) v.forEach(x=>Array.isArray(x) ? x.forEach(walk) : walk(x)); else if (v && typeof v === 'object') walk(v); }); };
  q.items.forEach(i=>walk(i.e)); walk(q.where); q.group.forEach(walk); walk(q.having); q.order.forEach(o=>walk(o.e)); q.joins.forEach(j=>walk(j.on));
  return { used, star };
}
function runSelect(q){
  const srcs = [{ tr:q.from, t:tableFor(q.from) }].concat(q.joins.map(j=>({ tr:j.t, t:tableFor(j.t), j })));
  const mk = (tr, t, vals) => { const o = {}; const al = tr.alias || tr.name; t.cols.forEach((c,i)=>{ const v = vals ? vals[i] : null; if (!(c in o)) o[c] = v; o[al + '.' + c] = v; }); return o; };
  let rows = srcs[0].t.rows.map(r=>mk(srcs[0].tr, srcs[0].t, r));
  for (const s of srcs.slice(1)){
    const next = [];
    rows.forEach(l=>{
      let hit = false;
      s.t.rows.forEach(r=>{
        const rr = mk(s.tr, s.t, r);
        const m = Object.assign({}, rr, l);       // left columns win unqualified names
        Object.keys(rr).forEach(k=>{ if (k.includes('.')) m[k] = rr[k]; });
        if (truthy(ev(s.j.on, m, null))){ next.push(m); hit = true; }
      });
      if (!hit && s.j.left){ const rr = mk(s.tr, s.t, null); const m = Object.assign({}, rr, l); Object.keys(rr).forEach(k=>{ if (k.includes('.')) m[k] = null; }); next.push(m); }
    });
    rows = next;
  }
  if (q.where) rows = rows.filter(r=>truthy(ev(q.where, r, null)));
  // expand *
  let items = [];
  q.items.forEach(it=>{
    if (it.e.t === 'star'){
      srcs.forEach(s=>{ if (!it.e.q || it.e.q.toLowerCase() === String(s.tr.alias || s.tr.name).toLowerCase()) s.t.cols.forEach(c=>items.push({ e:{ t:'col', q:s.tr.alias || s.tr.name, c }, as:c })); });
    } else items.push(it);
  });
  const agg = q.group.length || items.some(i=>hasAgg(i.e)) || hasAgg(q.having);
  let ctx;
  if (agg){
    const groups = new Map();
    rows.forEach(r=>{ const k = JSON.stringify(q.group.map(g=>ev(g, r, null))); if (!groups.has(k)) groups.set(k, []); groups.get(k).push(r); });
    if (!q.group.length && !groups.size) groups.set('[]', []);
    // non-aggregated select items must be grouped
    const gnames = q.group.map(g=>nameOf(g).toLowerCase());
    items.forEach(i=>{ if (!hasAgg(i.e) && i.e.t === 'col' && !gnames.includes(i.e.c.toLowerCase()) && !gnames.includes((i.e.q ? i.e.q + '.' : '') + i.e.c.toLowerCase()) && !q.group.some(g=>g.t === 'col' && g.c.toLowerCase() === i.e.c.toLowerCase())) fail(`[MISSING_AGGREGATION] The non-aggregating expression \`${i.e.c}\` is based on columns which are not participating in the GROUP BY clause. Add it to GROUP BY, or wrap it in an aggregate like first() / max().`); });
    ctx = [...groups.values()].map(g=>({ row: g[0] || {}, grp: g }));
    if (q.having) ctx = ctx.filter(c=>truthy(ev(q.having, c.row, c.grp)));
  } else ctx = rows.map(r=>({ row:r, grp:null }));
  const cols = items.map(i=>i.as || nameOf(i.e));
  let out = ctx.map(c=>({ c, vals: items.map(i=>{ const v = ev(i.e, c.row, c.grp); return typeof v === 'number' && !Number.isInteger(v) ? +v.toFixed(4) : v; }) }));
  if (q.distinct){ const seen = new Set(); out = out.filter(o=>{ const k = JSON.stringify(o.vals); if (seen.has(k)) return false; seen.add(k); return true; }); }
  if (q.order.length){
    const key = (o, oe) => {
      if (oe.e.t === 'col' && !oe.e.q){ const i = cols.findIndex(c=>c.toLowerCase() === oe.e.c.toLowerCase()); if (i >= 0) return o.vals[i]; }
      if (oe.e.t === 'lit' && typeof oe.e.v === 'number') return o.vals[oe.e.v - 1];
      const i = cols.findIndex(c=>c.toLowerCase() === nameOf(oe.e).toLowerCase()); if (i >= 0) return o.vals[i];
      return ev(oe.e, o.c.row, o.c.grp);
    };
    out.sort((a, b)=>{ for (const oe of q.order){ const x = key(a, oe), y = key(b, oe); if (x === y) continue; if (x === null) return 1; if (y === null) return -1; const r = x < y ? -1 : 1; return oe.desc ? -r : r; } return 0; });
  }
  if (q.limit !== null) out = out.slice(0, q.limit);
  // scan statistics
  const { used, star } = colsUsed(q);
  const scans = srcs.map(s=>{
    const t = s.t, mine = star ? t.cols.length : t.cols.filter(c=>used.has(c.toLowerCase())).length;
    const read = t.format === 'parquet' ? Math.max(1, mine) : t.totalCols;
    return { name:t.lake ? 'json.`' + t.name + '`' : s.tr.name, format:t.format, read, total:t.totalCols, mb: read * t.colMB, rowsReal:t.rowsReal };
  });
  return { cols, rows: out.map(o=>o.vals), scans };
}
function fmtVal(v){ if (v === null || v === undefined) return 'NULL'; if (typeof v === 'number') return Number.isInteger(v) ? String(v) : String(+v.toFixed(2)); return String(v); }
function renderTable(res){
  if (!res.cols.length) return '';
  const cells = res.rows.map(r=>r.map(fmtVal));
  const w = res.cols.map((c,i)=>Math.max(c.length, ...cells.map(r=>r[i].length)));
  const line = '+' + w.map(x=>'-'.repeat(x+2)).join('+') + '+';
  const isNum = i => res.rows.length && res.rows.every(r=>r[i] === null || typeof r[i] === 'number');
  const row = (r, h) => '|' + r.map((v,i)=>' ' + (h ? K(pad(v, w[i])) : isNum(i) ? NU(lpad(v, w[i])) : esc(pad(v, w[i]))) + ' ').join('|') + '|';
  return [D(line), row(res.cols, true), D(line), ...cells.map(r=>row(r)), D(line)].join('\n');
}
function scanLine(res, secs){
  const s = res.scans;
  const parts = s.map(x=>`${x.name} (${x.format}${x.format === 'parquet' ? `, ${x.read} of ${x.total} columns` : x.total > 1 ? `, all ${x.total} columns` : ''}) ${fmtMB(x.mb)}`);
  return D(`Time taken: ${secs} seconds, Fetched ${res.rows.length} row(s)`) + '\n' + D('scanned: ') + parts.map(p=>/parquet/.test(p) ? ST(p) : K(p)).join(D(' + '));
}
function explainPlan(q){
  const { used, star } = colsUsed(q);
  const agg = q.group.length || q.items.some(i=>hasAgg(i.e));
  const keys = q.group.map(nameOf).join(', ');
  const fns = q.items.filter(i=>hasAgg(i.e)).map(i=>nameOf(i.e)).join(', ');
  const scan = tr => { const t = tableFor(tr); const cols = star ? t.cols : t.cols.filter(c=>used.has(c.toLowerCase())); return `FileScan ${t.format} ${tr.path ? 'json.`' + tr.path + '`' : tr.name}[${(t.format === 'parquet' ? cols : t.cols).join(',')}]` + (q.where && tr === q.from ? ` PushedFilters: [${nameOf(q.where).replace(/[()]/g,'')}]` : ''); };
  const steps = [];
  if (q.limit !== null && q.order.length) steps.push([`TakeOrderedAndProject(limit=${q.limit}, orderBy=[${q.order.map(o=>nameOf(o.e) + (o.desc ? ' DESC' : ' ASC')).join(', ')}])`, 'top-N on each node, then merged']);
  else if (q.order.length) steps.push([`*(3) Sort [${q.order.map(o=>nameOf(o.e) + (o.desc ? ' DESC' : ' ASC')).join(', ')}]`, ''], ['Exchange rangepartitioning', 'shuffle to sort globally']);
  else if (q.limit !== null) steps.push([`CollectLimit ${q.limit}`, '']);
  if (agg){
    steps.push([`*(2) HashAggregate(keys=[${keys}], functions=[${fns}])`, 'final: combine the partial results']);
    steps.push([`Exchange hashpartitioning(${keys || 'single'}, 200)`, 'SHUFFLE across the network']);
    steps.push([`*(1) HashAggregate(keys=[${keys}], functions=[${fns.replace(/(\w+)\(/g, 'partial_$1(')}])`, 'runs on every node, in parallel']);
  }
  if (q.joins.length) steps.push([`BroadcastHashJoin [${nameOf(q.joins[0].on).replace(/[()]/g,'')}], Inner`, `small table ${q.joins[0].t.name} copied to every node`]);
  if (q.where) steps.push([`*(1) Filter ${nameOf(q.where)}`, '']);
  const t0 = tableFor(q.from);
  steps.push([scan(q.from), t0.format === 'parquet' ? 'each executor reads only its own blocks, only these columns' : 'row format: every column is read']);
  const out = [K('== Physical Plan ==')];
  steps.forEach((st, i)=>out.push((i ? '   '.repeat(i-1) + '+- ' : '') + st[0] + (st[1] ? '   ' + D('← ' + st[1]) : '')));
  q.joins.forEach(j=>{ const d = steps.length - 2; out.push('   '.repeat(Math.max(0, d)) + '+- BroadcastExchange'); out.push('   '.repeat(Math.max(0, d) + 1) + '+- ' + scan(j.t)); });
  return out.join('\n');
}
function runSql(src){
  const st = sqlParse(src);
  if (st.t === 'show'){
    rec({ op:'sql.show' });
    const names = Object.keys(S.tables).sort();
    return renderTable({ cols:['namespace','tableName','format'], rows: names.map(n=>['shopeasy', n, S.tables[n].format]) }) + '\n' + D(`Fetched ${names.length} row(s)`);
  }
  if (st.t === 'describe'){
    const t = tableFor({ name:st.name });
    rec({ op:'sql.describe', name:st.name });
    S.view = 't:' + Object.keys(S.tables).find(k=>S.tables[k] === t);
    const types = t.cols.map((c,i)=>{ const v = t.rows.map(r=>r[i]).find(x=>x !== null); return typeof v === 'number' ? (Number.isInteger(v) ? 'bigint' : 'double') : 'string'; });
    return renderTable({ cols:['col_name','data_type'], rows: t.cols.map((c,i)=>[c, types[i]]) }) + '\n' + D(`# ${t.format}${t.totalCols > t.cols.length ? ` · ${t.totalCols} columns on disk (${t.totalCols - t.cols.length} more not shown)` : ''}${t.rowsReal ? ' · ' + t.rowsReal + ' rows in production' : ''}`);
  }
  if (st.t === 'drop'){
    if (!S.tables[st.name]) fail(`[TABLE_OR_VIEW_NOT_FOUND] ${st.name}`);
    delete S.tables[st.name]; rec({ op:'sql.drop', name:st.name });
    return D('Time taken: 0.09 seconds');
  }
  if (st.t === 'explain'){
    rec({ op:'sql.explain', q:st.q, src });
    return explainPlan(st.q);
  }
  const q = st.t === 'ctas' ? st.q : st;
  const res = runSelect(q);
  const nodes = Math.max(1, aliveNodes().length);
  const mb = res.scans.reduce((t,x)=>t+x.mb, 0);
  const secs = +(0.3 + mb / 2000 / Math.min(nodes, 8) + (q.joins.length ? 0.4 : 0)).toFixed(2);
  if (st.t === 'ctas'){
    if (S.tables[st.name]) fail(`[TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table \`${st.name}\` because it already exists. DROP TABLE ${st.name}; first.`);
    S.tables[st.name] = { cols:res.cols, rows:res.rows, format:'parquet', totalCols:res.cols.length, colMB:2, created:true };
    flashIds.add('t:' + st.name); S.view = 't:' + st.name;
    rec({ op:'sql.ctas', name:st.name, res, q, src });
    return ST(`✓ created table ${st.name} (${res.rows.length} rows, parquet)`) + '\n' + D(`Time taken: ${secs} seconds`) + '\n' + D('scanned: ') + res.scans.map(x=>`${x.name} ${fmtMB(x.mb)}`).join(' + ');
  }
  rec({ op:'sql.select', res, q, src, tables: res.scans.map(x=>x.name) });
  const tname = q.from.path ? null : Object.keys(S.tables).find(k=>k.toLowerCase() === q.from.name.toLowerCase());
  if (tname) S.view = 't:' + tname;
  return renderTable(res) + '\n' + scanLine(res, secs);
}

/* ============================================================
   execute — the single entry point used by the UI
   ============================================================ */
const SQL_START = /^\s*(SELECT|CREATE|EXPLAIN|SHOW|DESCRIBE|DESC|DROP|WITH)\b/i;
function execute(src){
  calls = [];
  const html = [];
  let err = null;
  const text = String(src).trim();
  try {
    if (!text) return { html:'', calls };
    if (SQL_START.test(text)){
      const stmts = text.split(/;\s*(?=\S)/).map(s=>s.trim()).filter(Boolean);
      stmts.forEach(s=>html.push(runSql(s)));
    } else {
      const lines = text.split('\n').map(l=>l.trim()).filter(l=>l && !l.startsWith('#'));
      for (const l of lines){
        if (SQL_START.test(l)){ html.push(runSql(l)); continue; }
        const r = runLine(l);
        if (r !== undefined && r !== '') html.push(r);
      }
    }
  } catch (e){
    err = e;
    html.push(e instanceof ShellError ? ER(e.message) : ER('Error: ' + e.message));
    if (!(e instanceof ShellError)) console.error(e);
  }
  return { html: html.join('\n'), calls, err };
}
function runLine(l){
  const a = words(l.replace(/;\s*$/, ''));
  let c = a.shift();
  if (c === 'sudo') c = a.shift();
  c = (c || '').toLowerCase();
  switch (c){
    case 'help': case '?': return cmdHelp();
    case 'site': return cmdSite(a);
    case 'server': return cmdServer(a);
    case 'ingest': return cmdIngest(a);
    case 'node': return cmdNode(a);
    case 'cluster': return cmdCluster(a);
    case 'events': return cmdEvents(a);
    case 'stream': return cmdStream(a);
    case 'profile': return cmdProfile(a);
    case 'reviews': return cmdReviews(a);
    case 'funnel': return cmdFunnel(a);
    case 'rdbms': return cmdRdbms(a);
    case 'bench': return cmdBench(a);
    case 'partition': return cmdPartition(a);
    case 'replicate': return cmdReplicate(a);
    case 'users': return cmdUsers(a);
    case 'hdfs': case 'hadoop': return cmdHdfs(a);
    case 'spark-submit': return cmdSpark(a);
    case 'batch': return cmdBatch(a);
    case 'kafka-topics': case 'kafka-topics.sh': return cmdKafka(a);
    case 'cap': return cmdCap(a);
    case 'lake': return cmdLake(a);
    case 'warehouse': return cmdWarehouse(a);
    case 'pipeline': return cmdPipeline(a);
    case 'answer': case 'ans': return cmdAnswer(a);
    case 'ls': return D('Local files on the gateway: ') + Object.keys(S.local).map(ST).join('  ');
    case 'whoami': return 'shopeasy';
    case 'db.': case 'use': fail('This is a cluster shell, not mongosh. Type help for commands.');
  }
  if (/^[A-D]$/i.test(c)) return cmdAnswer([c]);
  fail(`${c}: command not found. Type help for the commands this chapter uses.`);
}

/* ---------- helpers shared by the chapter files ---------- */
const did = (r, op, f) => r.calls.some(c=>c.op === op && (!f || f(c)));
function quiz(group, title, ask, options, correct, explain, note, extra){
  return Object.assign({ group, kind:'quiz', title, ask, options, correct, answer:'answer ' + correct, explain, note }, extra || {});
}

