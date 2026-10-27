/* ============================================================
   Chapter 03 · The Data Ecosystem & SQL — Sources → Storage →
   Processing → Insight
   Built from Lecture 3: data warehouse (schema-on-write) vs data
   lake (schema-on-read) and the lakehouse; row vs columnar storage
   (CSV/Avro vs Parquet/ORC); pipelines, ETL vs ELT; the three-layer
   map; and why declarative SQL scales from 1 GB to 1 PB.
   ============================================================ */
const SHOP = {
  customers: {
    cols:['id','name','city','segment'],
    rows:[
      [1,'Ananya','Pune','Premium'], [2,'Rohan','Delhi','Regular'], [3,'Meera','Mumbai','Premium'], [4,'Kabir','Kolkata','Regular'],
      [5,'Ishita','Chennai','Regular'], [6,'Arjun','Bengaluru','Premium'], [7,'Zoya','Lucknow','Regular'], [8,'Dev','Jaipur','Student']
    ]
  },
  orders: {
    cols:['order_id','customer_id','region','product','amount','order_date'],
    rows:[
      [1001,1,'West','Laptop',55000,'2026-10-25'], [1002,2,'North','Earbuds',2500,'2026-10-25'], [1003,3,'West','Phone',22000,'2026-10-25'],
      [1004,4,'East','Monitor',15000,'2026-10-25'], [1005,5,'South','Phone',21000,'2026-10-26'], [1006,6,'South','Laptop',52000,'2026-10-26'],
      [1007,2,'North','Keyboard',2000,'2026-10-26'], [1008,7,'North','Phone',20000,'2026-10-26'], [1009,1,'West','Earbuds',3000,'2026-10-27'],
      [1010,8,'North','Tablet',30000,'2026-10-27'], [1011,3,'West','Monitor',16000,'2026-10-27'], [1012,6,'South','Earbuds',3500,'2026-10-27']
    ]
  },
  raw_orders: {
    cols:['order_id','region','amount','status','order_date'],
    rows:[
      [1001,'West',55000,'delivered','2026-10-25'], [1002,'North',2500,'delivered','2026-10-25'], [1002,'North',2500,'delivered','2026-10-25'],
      [1003,'West',22000,'delivered','2026-10-25'], [1004,'East',15000,'cancelled','2026-10-25'], [1005,'South',21000,'delivered','2026-10-26'],
      [1006,'South',52000,'delivered','2026-10-26'], [1006,'South',52000,'delivered','2026-10-26'], [1007,'North',2000,'delivered','2026-10-26'],
      [1008,'North',20000,'delivered','2026-10-26'], [1009,'West',3000,'cancelled','2026-10-27'], [1010,'North',30000,'delivered','2026-10-27'],
      [1011,'West',16000,'delivered','2026-10-27'], [1012,'South',3500,'delivered','2026-10-27']
    ]
  }
};
const CLICKS = [
  { user:'u_8812', event:'page_view',   device:{ os:'android' }, ts:'2026-10-27T10:02:11' },
  { user:'u_8812', event:'add_to_cart', device:{ os:'android' }, ts:'2026-10-27T10:02:40', sku:'LP-1001' },
  { user:'u_1207', event:'search',      device:{ os:'ios' },     query:'earbuds' },
  { user:'u_1207', event:'add_to_cart', device:{ os:'ios' },     ts:'2026-10-27T10:03:05', sku:'EB-2210' },
  { user:'u_5530', event:'page_view',   device:{ os:'web' },     ts:'2026-10-27T10:03:17' },
  { user:'u_5530', event:'rage_click',  device:{ os:'web' },     ts:'2026-10-27T10:03:19' },
  { user:'u_9001', event:'add_to_cart', device:{ os:'android' }, ts:'2026-10-27T10:04:02', sku:'PH-3003' },
  { user:'u_9001', event:'checkout',    device:{ os:'android' }, ts:'2026-10-27T10:05:44' }
];

const CH03_TASKS = [
  /* ---------- Warehouse vs lake ---------- */
  { group:'Warehouse vs lake', kind:'sql', title:'What the warehouse holds',
    story:'One pipeline, four boxes: <b>Sources → Storage → Processing → Insight</b>. You’re in <b>Spark SQL</b> on ShopEasy’s platform. Type SQL directly (end with <code>;</code>) or use shell commands like <code>lake ls</code>.',
    ask:'List the warehouse tables: <code>SHOW TABLES;</code>',
    answer:'SHOW TABLES;',
    check: r => did(r, 'sql.show'),
    explain:'A <strong>data warehouse</strong> is a central repository of <em>structured, cleaned, integrated</em> data from many sources, optimised for analytical queries and reporting rather than day-to-day transactions. Examples: Redshift, BigQuery, Snowflake.',
    note:'<code>customers</code>, <code>orders</code> (Parquet) and <code>orders_csv</code>, a CSV export of the same orders.' },
  { group:'Warehouse vs lake', title:'Schema-on-write says no',
    ask:'The mobile app dumped today’s raw clickstream. Try loading it straight into the warehouse: <code>warehouse load clickstream-2026-10-27.json --table events</code>',
    answer:'warehouse load clickstream-2026-10-27.json --table events',
    check: r => did(r, 'warehouse.load'),
    explain:'<strong>Schema-on-write</strong>: the schema is enforced <em>before</em> data is written, so anything off-schema is rejected. It’s like a library that refuses a book until it has a catalogued shelf. Clean and query-ready, but rigid and slow to load.',
    note:'0 rows loaded. A string user id, a nested <code>device</code> object and a missing timestamp.' },
  { group:'Warehouse vs lake', title:'The lake says yes',
    ask:'Land the same file in the data lake instead: <code>lake put clickstream-2026-10-27.json</code>',
    answer:'lake put clickstream-2026-10-27.json',
    check: r => did(r, 'lake.put', c=>/clickstream/.test(c.p)) ? true : did(r, 'lake.put') ? 'Put the clickstream file.' : false,
    explain:'A <strong>data lake</strong> (S3, GCS, Azure Data Lake, HDFS) holds vast raw data in its native format until it’s needed: logs, JSON, images, CSV, Parquet. It’s like a garage: toss everything in, and sort a box the day you open it.',
    note:'Stored as-is in <code>s3://shopeasy-lake/raw/</code>. Check the Lake chip in the Data panel.' },
  { group:'Warehouse vs lake', kind:'sql', title:'Schema-on-read',
    ask:'Query the raw file directly. Get the <code>user</code> and <code>event</code> of every <code>add_to_cart</code>:<br><code>SELECT user, event FROM json.`raw/clickstream-2026-10-27.json` WHERE event = \'add_to_cart\';</code>',
    answer:"SELECT user, event\nFROM json.`raw/clickstream-2026-10-27.json`\nWHERE event = 'add_to_cart';",
    explain:'<strong>Schema-on-read</strong>: structure is applied when you query, not when you store. The data scientist reads only the fields they care about, and the extra <code>device</code>, <code>sku</code> and <code>query</code> fields simply come along unused.',
    note:'u_8812, u_1207 and u_9001 added something to their carts.' },
  quiz('Warehouse vs lake', 'Trade-offs, not winners',
    'Which description fits a <strong>data lake</strong>?',
    ['Structured · schema-on-write · fast structured queries · higher storage cost · analysts/BI',
     'Raw, any format · schema-on-read · flexible · lower storage cost · data scientists / ML / engineers',
     'Only images and video', 'A faster kind of warehouse'], 'B',
    'Two tension axes: structure vs flexibility, and performance vs storage cost. The warehouse is best for reliable reporting; the lake for exploration, ML and unknown future use.',
    'Raw, schema-on-read, cheap, flexible.'),
  quiz('Warehouse vs lake', 'The lakehouse',
    'What does a <strong>lakehouse</strong> (Databricks Delta Lake, Snowflake, Apache Iceberg) add?',
    ['A second copy of the lake inside the warehouse', 'ACID transactions, schema enforcement and fast SQL directly on cheap data-lake storage', 'Streaming only', 'A rule that only structured files can be stored'], 'B',
    'Running a lake and a warehouse side by side means duplicated data, extra pipelines and governance headaches. A lakehouse gives lake-cost storage with warehouse-grade querying.',
    'Warehouse features on lake storage, so you stop choosing.'),

  /* ---------- Row vs columnar ---------- */
  { group:'Row vs columnar', kind:'sql', from:'orders_csv', title:'Row storage: CSV',
    story:'The file format inside your lake quietly decides your <b>speed and your cloud bill</b>. Both tables below hold the same 120 M orders with <b>50 columns</b> on disk. Your query needs <b>2</b> of them.',
    ask:'Total <code>amount</code> per <code>region</code> from the CSV copy: <code>SELECT region, SUM(amount) AS sales FROM orders_csv GROUP BY region;</code> Note the <em>scanned</em> line.',
    answer:'SELECT region, SUM(amount) AS sales\nFROM orders_csv\nGROUP BY region;',
    explain:'<strong>Row storage</strong> (CSV, JSON, Avro) keeps each whole record together. That’s great when you want entire records, as in transactional reads and writes. But to sum one column the engine still reads all 50.',
    note:'All 50 columns read: about 5.9 GB scanned.' },
  { group:'Row vs columnar', kind:'sql', from:'orders', title:'Columnar storage: Parquet',
    ask:'Same query on the Parquet table: <code>SELECT region, SUM(amount) AS sales FROM orders GROUP BY region;</code>',
    answer:'SELECT region, SUM(amount) AS sales\nFROM orders\nGROUP BY region;',
    explain:'<strong>Columnar storage</strong> (Parquet, ORC) keeps each column together, so the engine reads only <code>region</code> and <code>amount</code> and skips the other 48 (<em>column pruning</em>). Predicate pushdown also skips irrelevant chunks, and similar values side by side compress far tighter.',
    note:'2 of 50 columns, 240 MB: 25× less data for the same answer.' },
  quiz('Row vs columnar', 'Why does Parquet compress better?',
    'Why does columnar data compress far better than row data?',
    ['Parquet uses a secret algorithm', 'Similar values sit next to each other (all regions, then all amounts), so they pack tightly', 'It deletes duplicate rows', 'It stores fewer rows'], 'B',
    'A column of “West, West, North, West…” compresses beautifully. A row mixing ids, names and amounts doesn’t.',
    'Like values side by side.'),
  quiz('Row vs columnar', 'Pick the format',
    'The customer-support screen fetches <em>one entire order record</em> at a time, thousands of times a second. Which layout suits it?',
    ['Row-oriented (e.g. Avro or an OLTP database)', 'Columnar (Parquet)'], 'A',
    'Whole-record reads and writes favour row layout. Columnar wins for analytics that touch a few columns across many rows.',
    'Row storage for whole records, columnar for analytics.'),

  /* ---------- Pipelines · ETL vs ELT ---------- */
  { group:'Pipelines · ETL vs ELT', title:'ETL: transform on the way in',
    story:'A <b>pipeline</b> moves data from source to destination and reshapes it so it’s usable: <b>ingestion</b>, then <b>transformation</b>. The data engineer’s day job.',
    ask:'See the classic approach: <code>pipeline run etl</code>',
    answer:'pipeline run etl',
    check: r => did(r, 'pipeline', c=>c.m === 'etl'),
    explain:'<strong>ETL</strong> transforms on a separate staging server and loads only clean results. It was historically necessary because warehouse compute and storage were expensive. Still preferred where regulated data (PII) must be cleaned and masked <em>before</em> it lands: banks, healthcare, insurance.',
    note:'Extract → Transform (staging) → Load.' },
  { group:'Pipelines · ETL vs ELT', title:'ELT: transform inside',
    ask:'Now the cloud approach: <code>pipeline run elt</code>',
    answer:'pipeline run elt',
    check: r => did(r, 'pipeline', c=>c.m === 'elt'),
    explain:'<strong>ELT</strong> loads raw data first, then transforms it in place using the warehouse’s own compute. You keep the raw data to re-transform any time. Fivetran → Snowflake/BigQuery, transformed with dbt.',
    note:'Extract → Load raw → Transform in SQL. You’ll do exactly that next.' },
  { group:'Pipelines · ETL vs ELT', title:'E + L: ingest raw orders',
    ask:'Pull the orders from the app’s MySQL database into the warehouse, raw: <code>ingest mysql.orders --into raw_orders</code>',
    answer:'ingest mysql.orders --into raw_orders',
    check: r => did(r, 'ingest.orders', c=>c.into === 'raw_orders') ? true : did(r, 'ingest.orders') ? 'Load it into a table called <code>raw_orders</code>.' : false,
    explain:'<strong>Ingestion</strong> pulls data from its origins into storage, on a schedule (batch) or continuously (streaming), through connectors or APIs.',
    note:'14 raw rows landed. Something is off; let’s look.' },
  { group:'Pipelines · ETL vs ELT', kind:'sql', title:'Spot the duplicates',
    ask:'Compare total rows with distinct orders: <code>SELECT COUNT(*) AS total_rows, COUNT(DISTINCT order_id) AS unique_orders FROM raw_orders;</code>',
    answer:'SELECT COUNT(*) AS total_rows,\n       COUNT(DISTINCT order_id) AS unique_orders\nFROM raw_orders;',
    explain:'The connector replayed two orders. Raw data is messy, and that’s fine in ELT, because cleaning is the next step.',
    note:'14 rows, 12 unique orders: two duplicates.' },
  { group:'Pipelines · ETL vs ELT', kind:'sql', table:'clean_orders', title:'T: clean it in SQL',
    ask:'Create <code>clean_orders</code> with one row per order and only <code>delivered</code> ones:<br><code>CREATE TABLE clean_orders AS SELECT DISTINCT order_id, region, amount, order_date FROM raw_orders WHERE status = \'delivered\';</code>',
    answer:"CREATE TABLE clean_orders AS\nSELECT DISTINCT order_id, region, amount, order_date\nFROM raw_orders\nWHERE status = 'delivered';",
    explain:'<strong>Transformation</strong> means cleaning, standardising, deduplicating, joining and aggregating raw data into a query-ready shape, often in SQL (dbt) or Spark. Here: deduplicate and drop cancelled orders.',
    note:'10 clean orders. The raw table is still there if the rules ever change.' },
  { group:'Pipelines · ETL vs ELT', kind:'sql', table:'daily_sales', title:'T: roll up to daily totals',
    ask:'Build the dashboard table: <code>CREATE TABLE daily_sales AS SELECT order_date, SUM(amount) AS total FROM clean_orders GROUP BY order_date;</code>',
    answer:'CREATE TABLE daily_sales AS\nSELECT order_date, SUM(amount) AS total\nFROM clean_orders\nGROUP BY order_date;',
    explain:'Roll-ups like this feed the <em>Insight</em> box: BI dashboards and KPI tracking.',
    note:'25 Oct ₹79,500 · 26 Oct ₹95,000 · 27 Oct ₹49,500.' },
  quiz('Pipelines · ETL vs ELT', 'Bank or streaming service?',
    'A bank must mask card numbers and validate records <em>before</em> they ever land in the warehouse. ETL or ELT?',
    ['ETL', 'ELT'], 'A',
    'Regulated data (PII, compliance) is cleaned and masked first, so it’s ETL. Netflix, Airbnb and modern e-commerce, with huge, varied event data and fast-changing questions, prefer ELT.',
    'ETL.'),
  quiz('Pipelines · ETL vs ELT', 'Why the shift?',
    'The industry shift from ETL to ELT is really the story of…',
    ['SQL being replaced', 'The cloud making storage and compute cheap and elastic (Snowflake, BigQuery, Databricks)', 'Data getting smaller', 'Banks adopting streaming'], 'B',
    'Once loading junk stopped being expensive, keeping raw data and transforming it later became the simpler choice.',
    'Cheap, elastic cloud storage and compute.'),
  quiz('Pipelines · ETL vs ELT', 'Three layers, not a hundred tools',
    'Where do <strong>Kafka</strong>, <strong>Flink</strong> and <strong>Kinesis</strong> belong on the organising map?',
    ['Storage (the durable base layer)', 'Processing (compute at scale)', 'Streaming (data in motion)'], 'C',
    'Storage: HDFS, S3, GCS. Processing: Spark, MapReduce, Hive. Streaming: Kafka, Flink, Kinesis. Modern platforms like BigQuery, Snowflake and Databricks blur the layers into one managed service.',
    'Streaming.'),

  /* ---------- SQL at any scale ---------- */
  { group:'SQL at any scale', kind:'sql', ordered:true, title:'One statement, N machines',
    story:'You write <b>one declarative statement</b>; the engine splits the work across many machines. It’s the same query at 1 GB or 1 PB.',
    ask:'Sales per region, biggest first: <code>SELECT region, SUM(amount) AS sales FROM orders GROUP BY region ORDER BY sales DESC;</code>',
    answer:'SELECT region, SUM(amount) AS sales\nFROM orders\nGROUP BY region\nORDER BY sales DESC;',
    explain:'You state <em>what</em> you want, not <em>how</em> to compute it. The engine plans the how, which is exactly what lets it scale transparently.',
    note:'West 96,000 · South 76,500 · North 54,500 · East 15,000.' },
  { group:'SQL at any scale', title:'How the engine splits it',
    ask:'Ask Spark for its plan: <code>EXPLAIN SELECT region, SUM(amount) FROM orders GROUP BY region;</code>',
    answer:'EXPLAIN SELECT region, SUM(amount)\nFROM orders\nGROUP BY region;',
    check: r => did(r, 'sql.explain'),
    explain:'Read it bottom-up: a <strong>FileScan</strong> of only 2 Parquet columns on every node, a <strong>partial aggregate</strong> per node in parallel, an <strong>Exchange</strong> (the shuffle) over the network, then the <strong>final aggregate</strong>. That’s Chapter 02’s map → shuffle → reduce, generated from one line of SQL.',
    note:'partial_sum on every node → shuffle → sum.' },
  { group:'SQL at any scale', kind:'sql', ordered:true, title:'Filter, sort, limit',
    ask:'The 3 biggest orders from the <code>North</code> region: return <code>order_id</code>, <code>product</code>, <code>amount</code>, sorted by <code>amount</code> descending.',
    answer:"SELECT order_id, product, amount\nFROM orders\nWHERE region = 'North'\nORDER BY amount DESC\nLIMIT 3;",
    explain:'<code>WHERE</code> is pushed down into the scan, so whole Parquet chunks without North rows are skipped. <code>ORDER BY … LIMIT</code> becomes a top-N on each node before anything is shuffled.',
    note:'1010 Tablet 30,000 · 1008 Phone 20,000 · 1002 Earbuds 2,500.' },
  { group:'SQL at any scale', kind:'sql', need:[/\bJOIN\b/i], title:'Relational logic still applies',
    ask:'Revenue per customer <code>segment</code>: join <code>orders o</code> to <code>customers c</code> on <code>o.customer_id = c.id</code>, then <code>SUM(o.amount) AS revenue</code> grouped by <code>c.segment</code>.',
    answer:'SELECT c.segment, SUM(o.amount) AS revenue\nFROM orders o\nJOIN customers c ON o.customer_id = c.id\nGROUP BY c.segment;',
    explain:'Joins, GROUP BY and aggregations work the same way on a cluster. The engine broadcasts the small <code>customers</code> table to every node so the big <code>orders</code> table never moves.',
    note:'Premium 151,500 · Regular 60,500 · Student 30,000.' },
  { group:'SQL at any scale', kind:'sql', title:'HAVING',
    ask:'Customers with more than one order: <code>customer_id</code> and <code>COUNT(*) AS orders</code>, keeping only groups where the count is greater than 1.',
    answer:'SELECT customer_id, COUNT(*) AS orders\nFROM orders\nGROUP BY customer_id\nHAVING COUNT(*) > 1;',
    explain:'<code>WHERE</code> filters rows before grouping; <code>HAVING</code> filters groups after. It’s the SQL you already know.',
    note:'Customers 1, 2, 3 and 6, with 2 orders each.' },
  quiz('SQL at any scale', 'Why SQL refuses to die',
    'What makes SQL scale transparently from 1 GB to 1 PB?',
    ['It’s faster to type', 'It’s declarative: you state what you want and the engine plans how to parallelise it', 'It only works on small data', 'Every query is hand-written distributed code'], 'B',
    'Add decades of tools, talent and training that already speak SQL, and every new engine speaks it too: BigQuery, Snowflake, Spark SQL, Presto/Trino, Athena, Hive.',
    'Declarative.'),
  quiz('SQL at any scale', 'Old meets new',
    'Moving from the DBMS you know to Big Data, which of these <strong>changes</strong>?',
    ['The SQL interface', 'Tabular thinking (tables, rows, columns)', 'Relational logic (joins, GROUP BY)', 'The cost model: driven by data scanned, not a fixed server'], 'D',
    'What stays: SQL, tables, relational logic. What changes: scale (TB/PB), storage separated from compute, schema-on-read, and cost by bytes scanned. That’s why Parquet saved money two sections ago.',
    'The cost model. Where data lives, in what shape, how it moves, and SQL is how you ask it questions at any scale.')
];

(window.CHAPTERS = window.CHAPTERS || []).push({
  id: 'ch03', num: '03', title: 'Data Ecosystem & SQL', href: 'chapter03.html', story: 'Lakes, warehouses & Spark SQL',
  key: 'bigdata-lab-ch03-v1',
  host: 'spark-sql>',
  conn: 'spark-sql · thrift://shopeasy-warehouse:10000 · s3://shopeasy-lake',
  startNote: 'Spark SQL ready · warehouse + data lake mounted',
  dataset: {
    title: 'ShopEasy warehouse — 3 tables',
    note: 'customers, orders and raw_orders as plain SQL. Runs as-is in PostgreSQL, MySQL, SQLite or Spark SQL. In Spark you can then save orders as Parquet with CREATE TABLE … USING parquet AS SELECT.',
    tables: SHOP
  },
  init(){
    const nodes = [];
    for (let i=1;i<=8;i++) nodes.push(mkNode('node-' + i));
    const t = (src, format, extra) => Object.assign({ cols:src.cols.slice(), rows:clone(src.rows), format, totalCols:50, colMB:120, rowsReal:'120 M' }, extra || {});
    return freshWorld({
      help:'ch03', view:'t:orders', nodes,
      tables:{
        customers: t(SHOP.customers, 'parquet', { totalCols:4, colMB:4, rowsReal:'9 M' }),
        orders: t(SHOP.orders, 'parquet'),
        orders_csv: t(SHOP.orders, 'csv')
      },
      src:{ raw_orders: t(SHOP.raw_orders, 'parquet', { totalCols:5, colMB:30, rowsReal:'14 M' }) },
      lake:{
        'raw/server-2026-10-26.log': { mb: 820*1024, kind:'log' },
        'raw/orders-export.csv': { mb: 6*1024, kind:'csv' },
        'raw/product-photos/': { mb: 2.1*1024*1024, kind:'image' },
        'raw/sensor-readings.parquet': { mb: 48*1024, kind:'parquet' }
      },
      local:{
        'clickstream-2026-10-27.json': { mb: 1200, kind:'json', rows: CLICKS },
        'product-photo.jpg': { mb: 2.4, kind:'image' }
      }
    });
  },
  tasks: CH03_TASKS,
  finish: {
    tag: 'Chapter 03 complete', title: 'SQL at any scale.',
    text: `You landed raw data, chose formats, built an ELT pipeline and queried the warehouse, in ${CH03_TASKS.length} steps.`,
    note: 'Storage → format → movement → ecosystem → query. The SQL you learned is the SQL you’ll use.'
  }
});
