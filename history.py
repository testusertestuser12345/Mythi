#!/usr/bin/env python3
"""
live_history_en.py - Local web dashboard for monitoring Mythic command history
in real time with multi-callback tabs, sticky callback metadata, sort toggle,
Card / Text (terminal-like) view toggle, and 3 copy buttons (cmd / out / all).
"""
import asyncio
import threading
import json
import base64
import binascii
import traceback
from urllib.parse import urlparse, parse_qs
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from mythic import mythic

# ====== Update for your environment ======
USERNAME    = "user_name"
PASSWORD    = "password"
SERVER_IP   = "127.0.0.1"
SERVER_PORT = 7443
WEB_PORT    = 8600
LIMIT       = 300
# =========================================

CB_ALL_QUERY = """
query AllCBs {
  callback(order_by: {display_id: asc}) {
    id
    display_id
    ip
    host
    user
    os
    architecture
    pid
    active
    last_checkin
  }
}
"""

CB_FILTER_QUERY = """
query FilteredCBs($cbs: [Int!]) {
  callback(where: {display_id: {_in: $cbs}}, order_by: {display_id: asc}) {
    id
    display_id
    ip
    host
    user
    os
    architecture
    pid
    active
    last_checkin
  }
}
"""

SEQ_ALL_QUERY = """
query SeqAll {
  task(order_by: {id: asc}) {
    id
    callback { display_id }
  }
}
"""

SEQ_FILTER_QUERY = """
query SeqFilter($cbs: [Int!]) {
  task(where: {callback: {display_id: {_in: $cbs}}}, order_by: {id: asc}) {
    id
    callback { display_id }
  }
}
"""

LIVE_ALL_QUERY = """
query LiveAll($limit: Int!) {
  task(order_by: {id: desc}, limit: $limit) {
    id
    display_id
    command_name
    display_params
    status
    timestamp
    operator { username }
    callback { display_id }
    responses(order_by: {id: asc}) { response_text }
  }
}
"""

LIVE_FILTER_QUERY = """
query LiveFilter($cbs: [Int!], $limit: Int!) {
  task(
    where: {callback: {display_id: {_in: $cbs}}},
    order_by: {id: desc},
    limit: $limit
  ) {
    id
    display_id
    command_name
    display_params
    status
    timestamp
    operator { username }
    callback { display_id }
    responses(order_by: {id: asc}) { response_text }
  }
}
"""


def decode_chunk(s):
    if not s:
        return ""
    t = "".join(s.split())
    t += "=" * (-len(t) % 4)
    try:
        return base64.b64decode(t).decode("utf-8", errors="replace")
    except (binascii.Error, ValueError):
        return s


def unwrap_gql(res):
    if isinstance(res, dict) and "data" in res and isinstance(res["data"], dict):
        return res["data"]
    return res or {}


loop = asyncio.new_event_loop()


def _run_loop():
    asyncio.set_event_loop(loop)
    loop.run_forever()


threading.Thread(target=_run_loop, daemon=True).start()
mythic_instance = None


async def _login():
    global mythic_instance
    mythic_instance = await mythic.login(
        username=USERNAME,
        password=PASSWORD,
        server_ip=SERVER_IP,
        server_port=SERVER_PORT,
        ssl=True,
        timeout=-1,
    )


async def _fetch(cbs):
    has_filter = bool(cbs)
    cb_error = ""
    callbacks_info = []

    try:
        if has_filter:
            raw_cb = await asyncio.wait_for(
                mythic.execute_custom_query(
                    mythic_instance, query=CB_FILTER_QUERY, variables={"cbs": cbs}
                ),
                timeout=15,
            )
        else:
            raw_cb = await asyncio.wait_for(
                mythic.execute_custom_query(
                    mythic_instance, query=CB_ALL_QUERY, variables={}
                ),
                timeout=15,
            )
        cb_data = unwrap_gql(raw_cb)
        for c in (cb_data.get("callback") or []):
            callbacks_info.append({
                "cb": c.get("display_id", "?"),
                "ip": c.get("ip") or "-",
                "host": c.get("host") or "-",
                "user": c.get("user") or "-",
                "domain": "",
                "os": c.get("os") or "-",
                "arch": c.get("architecture") or "-",
                "pid": c.get("pid") if c.get("pid") is not None else "-",
                "active": bool(c.get("active", True)),
                "last_checkin": c.get("last_checkin") or "-",
            })
    except Exception as e:
        cb_error = f"callback query error: {e}"

    if has_filter:
        raw_seq = await asyncio.wait_for(
            mythic.execute_custom_query(
                mythic_instance, query=SEQ_FILTER_QUERY, variables={"cbs": cbs}
            ),
            timeout=20,
        )
    else:
        raw_seq = await asyncio.wait_for(
            mythic.execute_custom_query(
                mythic_instance, query=SEQ_ALL_QUERY, variables={}
            ),
            timeout=20,
        )
    seq_data = unwrap_gql(raw_seq)

    seq_map, counter = {}, {}
    for row in (seq_data.get("task") or []):
        cb = (row.get("callback") or {}).get("display_id")
        counter[cb] = counter.get(cb, 0) + 1
        seq_map[row["id"]] = counter[cb]

    if has_filter:
        raw_live = await asyncio.wait_for(
            mythic.execute_custom_query(
                mythic_instance, query=LIVE_FILTER_QUERY, variables={"cbs": cbs, "limit": LIMIT}
            ),
            timeout=20,
        )
    else:
        raw_live = await asyncio.wait_for(
            mythic.execute_custom_query(
                mythic_instance, query=LIVE_ALL_QUERY, variables={"limit": LIMIT}
            ),
            timeout=20,
        )
    live = unwrap_gql(raw_live)

    tasks_out = []
    for t in (live.get("task") or []):
        chunks = [decode_chunk(r.get("response_text")) for r in (t.get("responses") or [])]
        result = "".join(chunks).rstrip() or "(no output)"
        cmd = f"{t.get('command_name','')} {t.get('display_params','') or ''}".strip()
        cb_id = (t.get("callback") or {}).get("display_id", "?")
        seq_num = seq_map.get(t["id"], "?")
        task_id = t.get("display_id", "?")
        ts = t.get("timestamp", "")
        status = t.get("status", "")
        op = (t.get("operator") or {}).get("username", "")

        all_text = (
            "Callback: CB " + str(cb_id) + " | #" + str(seq_num) + " (task " + str(task_id) + ")\n"
            + "Timestamp: " + str(ts) + " | Status: " + str(status) + " | Operator: " + str(op) + "\n"
            + "Command: " + str(cmd) + "\n"
            + "--- Output ---\n" + str(result)
        )

        tasks_out.append({
            "id": t["id"],
            "cb": cb_id,
            "seq": seq_num,
            "task": task_id,
            "time": ts,
            "status": status,
            "operator": op,
            "cmd": cmd,
            "result": result,
            "all": all_text,
        })

    return {"callbacks": callbacks_info, "tasks": tasks_out, "cb_error": cb_error}


def fetch(cbs):
    return asyncio.run_coroutine_threadsafe(_fetch(cbs), loop).result(timeout=45)


PAGE = r"""<!doctype html><html><head><meta charset="utf-8">
<title>Mythic Live History</title>
<style>
 *{box-sizing:border-box}
 body{background:#1b1b1b;color:#e4e4e4;font-family:monospace;margin:0;height:100vh;display:flex;flex-direction:column;overflow:hidden}
 #top-sticky{flex:none;background:#141414;border-bottom:2px solid #333;z-index:10}
 #bar{padding:8px 12px;background:#111;border-bottom:1px solid #2b2b2b;display:flex;flex-wrap:wrap;align-items:center;gap:8px}
 input,button{font-family:monospace;font-size:13px;padding:4px 8px;background:#2a2a2a;
   color:#e4e4e4;border:1px solid #444;border-radius:4px;cursor:pointer}
 input{cursor:text}
 button:hover{background:#363636}
 #tabs{padding:6px 12px 0;background:#161616;border-bottom:1px solid #333;overflow-x:auto;white-space:nowrap}
 .tab{display:inline-block;padding:5px 14px;margin-right:4px;cursor:pointer;
   border:1px solid #444;border-bottom:none;border-radius:6px 6px 0 0;background:#222;font-size:13px}
 .tab.active{background:#26456e;border-color:#3a68a6;font-weight:bold;color:#fff}
 #cb-meta{padding:8px 12px;background:#1c222b;border-bottom:1px solid #2e3b4e;font-size:12px;line-height:1.5}
 .meta-grid{display:flex;flex-wrap:wrap;gap:6px 18px;align-items:center}
 .meta-item b{color:#9ecbff}
 .pill-on{background:#1e4d2b;color:#8ef5a8;padding:1px 7px;border-radius:10px;font-size:11px}
 .pill-off{background:#4d2424;color:#ff9c9c;padding:1px 7px;border-radius:10px;font-size:11px}
 #scroll-area{flex:1;overflow-y:auto;padding:10px 12px 28px}
 .task{border:1px solid #333;border-radius:6px;margin:8px 0;overflow:hidden;background:#181818}
 .head{padding:6px 10px;background:#222;font-size:12px;display:flex;flex-wrap:wrap;justify-content:space-between;align-items:center;gap:8px}
 .head-left{display:flex;flex-wrap:wrap;align-items:center;gap:6px}
 .head-actions{display:flex;flex-wrap:wrap;gap:4px}
 .badge{display:inline-block;background:#26456e;color:#fff;border-radius:4px;
   padding:1px 7px;font-weight:bold}
 .dim{color:#9aa}
 .cmd{padding:7px 10px;color:#8fd0ff;white-space:pre-wrap;word-break:break-all;background:#151a20}
 .result{padding:8px 10px;white-space:pre-wrap;background:#161616;
   max-height:280px;overflow:auto;border-top:1px solid #2c2c2c}
 .cp{font-size:11px;padding:2px 8px;background:#323232;border:1px solid #4a4a4a}
 .cp:hover{background:#444}
 .err{color:#ff6b6b;white-space:pre-wrap}
 .term-view{
   margin:0;padding:12px 14px;background:#101010;color:#e6e6e6;
   font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,"Liberation Mono",monospace;
   font-size:13px;line-height:1.45;white-space:pre-wrap;word-break:break-word;
   user-select:text;-webkit-user-select:text;
 }
 .term-sep{color:#555}
 .term-head{color:#a8b8c8}
 .term-cmd{color:#7cc7ff;font-weight:bold}
 .term-out{color:#e6e6e6}
 #toast{position:fixed;bottom:16px;left:50%;transform:translateX(-50%);
   background:#26456e;color:#fff;padding:8px 16px;border-radius:6px;opacity:0;
   transition:opacity .2s;pointer-events:none;z-index:99}
 #toast.show{opacity:1}
</style></head><body>
<div id="top-sticky">
  <div id="bar">
    <span>Callbacks:</span>
    <input id="cbs" value="" size="22" placeholder="blank = all (e.g. 20,21)">
    <span>Every</span>
    <input id="iv" value="2" size="2">
    <span>sec</span>
    <button id="order-btn">↓ Descending (Newest)</button>
    <button id="view-btn">View: Card</button>
    <button id="toggle">⏸ Pause</button>
    <span id="status">(Starting...)</span>
  </div>
  <div id="tabs"></div>
  <div id="cb-meta">Loading callback metadata...</div>
</div>
<div id="scroll-area">
  <div id="log">Loading...</div>
</div>
<div id="toast"></div>
<script>
window.addEventListener('error', function(ev){
  const st = document.getElementById('status');
  const lg = document.getElementById('log');
  if(st) st.innerHTML = '<span class="err">JS Error</span>';
  if(lg) lg.innerHTML = '<pre class="err">' + String(ev.message || ev) + '</pre>';
});

let running = true;
let sortDesc = true;
let textMode = false;
let lastCallbacks = [];
let lastTasks = [];
let lastCbError = "";
let currentShown = [];
let selectedTab = 'all';

function esc(s){
  return String(s == null ? "" : s).replace(/[&<>]/g, function(c){
    return {'&':'&amp;','<':'&lt;','>':'&gt;'}[c];
  });
}
function parseCbs(){
  return document.getElementById('cbs').value
    .split(',')
    .map(function(x){ return x.trim(); })
    .filter(function(x){ return /^\d+$/.test(x); });
}
function toast(msg){
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.classList.add('show');
  clearTimeout(window._tt);
  window._tt = setTimeout(function(){ t.classList.remove('show'); }, 1000);
}
function copyText(text, label){
  if(navigator.clipboard && navigator.clipboard.writeText){
    navigator.clipboard.writeText(text || '').catch(function(){});
  }
  toast('Copied ' + label);
}
function copyCmd(i){ copyText(currentShown[i].cmd, 'command'); }
function copyOut(i){ copyText(currentShown[i].result, 'output'); }
function copyAll(i){ copyText(currentShown[i].all, 'full entry with timestamp'); }
function selectTab(t){ selectedTab = String(t); render(); }

function getTabIds(cbsInputList){
  const fetchedIds = lastCallbacks.map(function(c){ return String(c.cb); });
  if(cbsInputList.length > 0){
    const merged = cbsInputList.slice();
    for(const id of fetchedIds){
      if(merged.indexOf(id) === -1) merged.push(id);
    }
    return merged;
  }
  const taskIds = [];
  for(const t of lastTasks){
    const s = String(t.cb);
    if(s !== '?' && taskIds.indexOf(s) === -1) taskIds.push(s);
  }
  const allIds = fetchedIds.slice();
  for(const id of taskIds){
    if(allIds.indexOf(id) === -1) allIds.push(id);
  }
  allIds.sort(function(a, b){ return Number(a) - Number(b); });
  return allIds;
}

function renderTabs(tabIds){
  const validTabs = ['all'].concat(tabIds);
  if(validTabs.indexOf(selectedTab) === -1) selectedTab = 'all';
  document.getElementById('tabs').innerHTML = validTabs.map(function(t){
    const active = (String(t) === String(selectedTab)) ? ' active' : '';
    const label = (t === 'all') ? 'All' : ('CB ' + t);
    return '<span class="tab' + active + '" data-tab="' + esc(t) + '">' + esc(label) + '</span>';
  }).join('');
  Array.prototype.forEach.call(document.querySelectorAll('#tabs .tab'), function(el){
    el.onclick = function(){ selectTab(el.getAttribute('data-tab')); };
  });
}

function renderCbMeta(tabIds, shownTasks){
  const box = document.getElementById('cb-meta');
  if(selectedTab === 'all'){
    const totalCb = tabIds.length;
    const activeCount = lastCallbacks.filter(function(c){ return c.active; }).length;
    const cbsArr = parseCbs();
    const scopeLabel = cbsArr.length > 0 ? ('Filtered (' + cbsArr.join(',') + ')') : 'All (Unfiltered)';
    box.innerHTML =
      '<div class="meta-grid">' +
        '<span class="meta-item"><b>Scope:</b> ' + esc(scopeLabel) + '</span>' +
        '<span class="meta-item"><b>Callbacks:</b> ' + totalCb + ' (' + activeCount + ' Active)</span>' +
        '<span class="meta-item"><b>Shown Tasks:</b> ' + shownTasks.length + '</span>' +
        '<span class="meta-item"><b>Sort:</b> ' + (sortDesc ? 'Descending (Newest)' : 'Ascending (Oldest)') + '</span>' +
        (lastCbError ? '<span class="err">(' + esc(lastCbError) + ')</span>' : '') +
      '</div>';
    return;
  }

  const info = lastCallbacks.find(function(c){ return String(c.cb) === String(selectedTab); });
  if(!info){
    box.innerHTML =
      '<div class="meta-grid">' +
        '<span class="meta-item"><b>CB ' + esc(selectedTab) + '</b></span>' +
        '<span class="dim">No callback metadata available (Shown tasks: ' + shownTasks.length + ')</span>' +
      '</div>';
    return;
  }

  const statusPill = info.active
    ? '<span class="pill-on">Active</span>'
    : '<span class="pill-off">Inactive</span>';

  box.innerHTML =
    '<div class="meta-grid">' +
      '<span class="meta-item"><b>CB ' + esc(info.cb) + '</b> ' + statusPill + '</span>' +
      '<span class="meta-item"><b>IP:</b> ' + esc(info.ip) + '</span>' +
      '<span class="meta-item"><b>Host:</b> ' + esc(info.host) + '</span>' +
      '<span class="meta-item"><b>User:</b> ' + esc(info.user) + '</span>' +
      '<span class="meta-item"><b>OS/Arch:</b> ' + esc(info.os + ' / ' + info.arch) + '</span>' +
      '<span class="meta-item"><b>PID:</b> ' + esc(info.pid) + '</span>' +
      '<span class="meta-item"><b>Last Checkin:</b> ' + esc(info.last_checkin) + '</span>' +
      '<span class="meta-item"><b>Tasks:</b> ' + shownTasks.length + '</span>' +
    '</div>';
}

function block(t, i){
  return '<div class="task">' +
    '<div class="head">' +
      '<div class="head-left">' +
        '<span class="badge">CB ' + esc(t.cb) + '</span>' +
        '<b>#' + esc(t.seq) + '</b>' +
        '<span class="dim">(task ' + esc(t.task) + ')</span>' +
        '<span class="dim">|</span><span>' + esc(t.time) + '</span>' +
        '<span class="dim">|</span><span>' + esc(t.status) + '</span>' +
        (t.operator ? '<span class="dim">|</span><span>' + esc(t.operator) + '</span>' : '') +
      '</div>' +
      '<div class="head-actions">' +
        '<button class="cp" onclick="copyCmd(' + i + ')">copy cmd</button>' +
        '<button class="cp" onclick="copyOut(' + i + ')">copy out</button>' +
        '<button class="cp" onclick="copyAll(' + i + ')">copy all</button>' +
      '</div>' +
    '</div>' +
    '<div class="cmd">' + esc(t.cmd) + '</div>' +
    '<div class="result">' + esc(t.result) + '</div>' +
  '</div>';
}

function renderTerminalView(shown){
  if(!shown.length) return '<pre class="term-view">(No tasks to display)</pre>';
  const parts = shown.map(function(t){
    const meta = '[CB ' + t.cb + ' | #' + t.seq + ' (task ' + t.task + ') | ' + t.time + ' | ' + t.status + (t.operator ? (' | ' + t.operator) : '') + ']';
    return '<span class="term-sep">------------------------------------------------------------</span>\n' +
      '<span class="term-head">' + esc(meta) + '</span>\n' +
      '<span class="term-cmd">$ ' + esc(t.cmd) + '</span>\n' +
      '<span class="term-out">' + esc(t.result) + '</span>';
  });
  return '<pre class="term-view">' + parts.join('\n') + '\n<span class="term-sep">------------------------------------------------------------</span></pre>';
}

function render(){
  const cbsInputList = parseCbs();
  const tabIds = getTabIds(cbsInputList);
  renderTabs(tabIds);

  let shown = lastTasks.slice();
  if(selectedTab !== 'all'){
    shown = shown.filter(function(t){ return String(t.cb) === String(selectedTab); });
  }
  shown.sort(function(a, b){
    return sortDesc ? (Number(b.id) - Number(a.id)) : (Number(a.id) - Number(b.id));
  });
  currentShown = shown;

  renderCbMeta(tabIds, shown);
  document.getElementById('log').innerHTML = textMode
    ? renderTerminalView(shown)
    : (shown.map(function(t, i){ return block(t, i); }).join('') || '(No tasks to display)');
}

async function tick(){
  if(!running) return;
  const cbs = parseCbs().join(',');
  document.getElementById('status').textContent = '(Refreshing...)';
  try{
    const url = '/api/history' + (cbs ? ('?cbs=' + encodeURIComponent(cbs)) : '');
    const r = await fetch(url, { cache: 'no-store' });
    const text = await r.text();
    let data;
    try { data = JSON.parse(text); } catch(_){ throw new Error('Invalid JSON: ' + text.slice(0, 300)); }
    if(!r.ok || data.error) throw new Error(data.error || ('HTTP ' + r.status));
    lastCallbacks = data.callbacks || [];
    lastTasks = data.tasks || [];
    lastCbError = data.cb_error || "";
    const scopeText = cbs ? ('CB ' + cbs) : 'All CBs';
    document.getElementById('status').textContent =
      '(' + scopeText + ' / ' + lastTasks.length + ' tasks / ' + new Date().toLocaleTimeString() + ')';
    render();
  }catch(e){
    const msg = String((e && e.message) || e);
    document.getElementById('status').innerHTML = '<span class="err">Error</span>';
    document.getElementById('cb-meta').innerHTML = '<span class="err">Fetch Error: ' + esc(msg) + '</span>';
    document.getElementById('log').innerHTML = '<pre class="err">' + esc(msg) + '</pre>';
  }
}

document.getElementById('order-btn').onclick = function(){
  sortDesc = !sortDesc;
  this.textContent = sortDesc ? '↓ Descending (Newest)' : '↑ Ascending (Oldest)';
  render();
};
document.getElementById('view-btn').onclick = function(){
  textMode = !textMode;
  this.textContent = textMode ? 'View: Text' : 'View: Card';
  render();
};
document.getElementById('toggle').onclick = function(){
  running = !running;
  this.textContent = running ? '⏸ Pause' : '▶ Resume';
};
document.getElementById('cbs').addEventListener('change', function(){ tick(); });

function loopTick(){
  tick().finally(function(){
    const iv = Math.max(1, parseInt(document.getElementById('iv').value, 10) || 2);
    setTimeout(loopTick, iv * 1000);
  });
}
loopTick();
</script></body></html>"""


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_GET(self):
        u = urlparse(self.path)
        if u.path == "/":
            body = PAGE.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif u.path == "/api/history":
            q = parse_qs(u.query)
            raw = q.get("cbs", [""])[0]
            try:
                cbs = [int(x) for x in raw.split(",") if x.strip().isdigit()]
                data = fetch(cbs)
                body = json.dumps(data, ensure_ascii=False).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
            except Exception as e:
                traceback.print_exc()
                body = json.dumps({"error": str(e)}, ensure_ascii=False).encode("utf-8")
                self.send_response(500)
                self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()


def main():
    print("Logging in to Mythic...", flush=True)
    asyncio.run_coroutine_threadsafe(_login(), loop).result(timeout=60)
    print(f"Login succeeded. Open http://127.0.0.1:{WEB_PORT} in your browser.", flush=True)
    ThreadingHTTPServer(("0.0.0.0", WEB_PORT), Handler).serve_forever()


if __name__ == "__main__":
    main()
