#!/usr/bin/env python3
"""
live_history_en.py - Local web dashboard for monitoring Mythic command history
in real time with operation switching (via SDK), multi-callback tabs, sticky
callback metadata, sort toggle, Card / Text view toggle, and copy helpers.
"""
import os
import sys
import inspect
import asyncio
import threading
import json
import base64
import binascii
import traceback
from contextlib import contextmanager
from urllib.parse import urlparse, parse_qs
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from mythic import mythic

# ====== Update for your environment ======
USERNAME          = "mythic_admin"
PASSWORD          = "password"
SERVER_IP         = "127.0.0.1"
SERVER_PORT       = 7443
WEB_PORT          = 8600
LIMIT             = 300
SUPPRESS_C_STDERR = True
# =========================================


@contextmanager
def quiet_c_stderr():
    if not SUPPRESS_C_STDERR:
        yield
        return
    devnull_fd = os.open(os.devnull, os.O_WRONLY)
    saved_stderr_fd = os.dup(2)
    try:
        sys.stderr.flush()
        os.dup2(devnull_fd, 2)
        yield
    finally:
        try:
            sys.stderr.flush()
            os.dup2(saved_stderr_fd, 2)
        finally:
            os.close(saved_stderr_fd)
            os.close(devnull_fd)


OPERATOR_ID_QUERY = """
query GetOperatorId($username: String!) {
  operator(where: {username: {_eq: $username}}) {
    id
    username
  }
}
"""

OP_QUERY = """
query AllOps {
  operation(order_by: {id: desc}) {
    id
    name
    complete
  }
}
"""

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
    operation_id
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
    operation_id
  }
}
"""

SEQ_ALL_QUERY = """
query SeqAll {
  task(order_by: {id: asc}) {
    id
    callback { display_id operation_id }
  }
}
"""

SEQ_FILTER_QUERY = """
query SeqFilter($cbs: [Int!]) {
  task(where: {callback: {display_id: {_in: $cbs}}}, order_by: {id: asc}) {
    id
    callback { display_id operation_id }
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
    callback { display_id operation_id }
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
    callback { display_id operation_id }
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
current_operator_id = None


async def _resolve_operator_id():
    global current_operator_id
    for attr in ("current_operator_id", "operator_id", "id"):
        val = getattr(mythic_instance, attr, None)
        if isinstance(val, int):
            current_operator_id = val
            return current_operator_id
    raw = await _exec_gql(OPERATOR_ID_QUERY, {"username": USERNAME}, timeout=10)
    data = unwrap_gql(raw)
    ops = data.get("operator") or []
    if ops and ops[0].get("id") is not None:
        current_operator_id = int(ops[0]["id"])
    return current_operator_id


async def _login():
    global mythic_instance
    with quiet_c_stderr():
        mythic_instance = await mythic.login(
            username=USERNAME,
            password=PASSWORD,
            server_ip=SERVER_IP,
            server_port=SERVER_PORT,
            ssl=True,
            timeout=-1,
        )
    await _resolve_operator_id()


async def _exec_gql(query, variables=None, timeout=15):
    with quiet_c_stderr():
        return await asyncio.wait_for(
            mythic.execute_custom_query(
                mythic_instance, query=query, variables=variables or {}
            ),
            timeout=timeout,
        )


async def _switch_operation(op_id):
    op_int = int(op_id)
    uid = current_operator_id or await _resolve_operator_id()
    if uid is None:
        raise RuntimeError("operator_id could not be resolved")

    with quiet_c_stderr():
        res = mythic.update_current_operation_for_user(
            mythic_instance,
            int(uid),
            op_int,
        )
        if inspect.isawaitable(res):
            await asyncio.wait_for(res, timeout=15)

    return {"ok": True, "operator_id": int(uid), "operation_id": op_int}


def switch_operation(op_id):
    return asyncio.run_coroutine_threadsafe(_switch_operation(op_id), loop).result(timeout=20)


async def _fetch(cbs):
    has_filter = bool(cbs)
    cb_error = ""
    op_error = ""
    ops_by_id = {}

    def register_op(op_id, name=None, complete=False):
        if op_id is None:
            return
        key = str(op_id)
        label = str(name) if name else f"Op {op_id}"
        if key not in ops_by_id:
            ops_by_id[key] = {"id": op_id, "name": label, "complete": bool(complete)}
        elif name and ops_by_id[key]["name"].startswith("Op "):
            ops_by_id[key]["name"] = str(name)

    loaded_ops = False
    if hasattr(mythic, "get_operations"):
        try:
            with quiet_c_stderr():
                res = mythic.get_operations(mythic=mythic_instance)
                if inspect.isawaitable(res):
                    res = await asyncio.wait_for(res, timeout=15)
            if isinstance(res, list):
                for o in res:
                    if isinstance(o, dict):
                        register_op(o.get("id"), o.get("name"), o.get("complete", False))
                loaded_ops = True
        except Exception as e:
            op_error = f"get_operations error: {e}"

    if not loaded_ops:
        try:
            raw_op = await _exec_gql(OP_QUERY, {}, timeout=15)
            op_data = unwrap_gql(raw_op)
            for o in (op_data.get("operation") or []):
                register_op(o.get("id"), o.get("name"), o.get("complete", False))
            op_error = ""
        except Exception as e:
            op_error = op_error or f"operation query error: {e}"

    callbacks_info = []
    try:
        if has_filter:
            raw_cb = await _exec_gql(CB_FILTER_QUERY, {"cbs": cbs}, timeout=15)
        else:
            raw_cb = await _exec_gql(CB_ALL_QUERY, {}, timeout=15)
        cb_data = unwrap_gql(raw_cb)
        for c in (cb_data.get("callback") or []):
            op_id = c.get("operation_id")
            register_op(op_id)
            op_name = (
                ops_by_id[str(op_id)]["name"]
                if (op_id is not None and str(op_id) in ops_by_id)
                else (f"Op {op_id}" if op_id is not None else "-")
            )
            callbacks_info.append({
                "cb": c.get("display_id", "?"),
                "ip": c.get("ip") or "-",
                "host": c.get("host") or "-",
                "user": c.get("user") or "-",
                "os": c.get("os") or "-",
                "arch": c.get("architecture") or "-",
                "pid": c.get("pid") if c.get("pid") is not None else "-",
                "active": bool(c.get("active", True)),
                "last_checkin": c.get("last_checkin") or "-",
                "op_id": op_id,
                "op_name": op_name,
            })
    except Exception as e:
        cb_error = f"callback query error: {e}"

    if has_filter:
        raw_seq = await _exec_gql(SEQ_FILTER_QUERY, {"cbs": cbs}, timeout=20)
    else:
        raw_seq = await _exec_gql(SEQ_ALL_QUERY, {}, timeout=20)
    seq_data = unwrap_gql(raw_seq)

    seq_map, counter = {}, {}
    for row in (seq_data.get("task") or []):
        cb_row = row.get("callback") or {}
        cb = cb_row.get("display_id")
        register_op(cb_row.get("operation_id"))
        counter[cb] = counter.get(cb, 0) + 1
        seq_map[row["id"]] = counter[cb]

    if has_filter:
        raw_live = await _exec_gql(LIVE_FILTER_QUERY, {"cbs": cbs, "limit": LIMIT}, timeout=20)
    else:
        raw_live = await _exec_gql(LIVE_ALL_QUERY, {"limit": LIMIT}, timeout=20)
    live = unwrap_gql(raw_live)

    tasks_out = []
    for t in (live.get("task") or []):
        chunks = [decode_chunk(r.get("response_text")) for r in (t.get("responses") or [])]
        result = "".join(chunks).rstrip() or "(no output)"
        cmd = f"{t.get('command_name','')} {t.get('display_params','') or ''}".strip()
        cb_obj = t.get("callback") or {}
        cb_id = cb_obj.get("display_id", "?")
        op_id = cb_obj.get("operation_id")
        register_op(op_id)
        op_name = (
            ops_by_id[str(op_id)]["name"]
            if (op_id is not None and str(op_id) in ops_by_id)
            else (f"Op {op_id}" if op_id is not None else "-")
        )
        seq_num = seq_map.get(t["id"], "?")
        task_id = t.get("display_id", "?")
        ts = t.get("timestamp", "")
        status = t.get("status", "")
        op_user = (t.get("operator") or {}).get("username", "")

        all_text = (
            "Callback: CB " + str(cb_id)
            + " | #" + str(seq_num) + " (task " + str(task_id) + ")"
            + " | Operation: " + str(op_name) + "\n"
            + "Timestamp: " + str(ts) + " | Status: " + str(status) + " | Operator: " + str(op_user) + "\n"
            + "Command: " + str(cmd) + "\n"
            + "--- Output ---\n" + str(result)
        )

        tasks_out.append({
            "id": t["id"],
            "cb": cb_id,
            "op_id": op_id,
            "op_name": op_name,
            "seq": seq_num,
            "task": task_id,
            "time": ts,
            "status": status,
            "operator": op_user,
            "cmd": cmd,
            "result": result,
            "all": all_text,
        })

    for c in callbacks_info:
        oid = c.get("op_id")
        if oid is not None and str(oid) in ops_by_id:
            c["op_name"] = ops_by_id[str(oid)]["name"]

    operations_info = sorted(
        ops_by_id.values(),
        key=lambda x: int(x["id"]) if str(x["id"]).isdigit() else 0,
        reverse=True,
    )

    err_joined = " / ".join([x for x in [op_error, cb_error] if x])
    return {
        "operations": operations_info,
        "callbacks": callbacks_info,
        "tasks": tasks_out,
        "cb_error": err_joined,
    }


def fetch(cbs):
    return asyncio.run_coroutine_threadsafe(_fetch(cbs), loop).result(timeout=45)


PAGE = r"""<!doctype html><html><head><meta charset="utf-8">
<title>Mythic Live History</title>
<style>
 *{box-sizing:border-box}
 html,body{background:#1b1b1b;color:#e4e4e4;font-family:monospace;margin:0;height:100vh;display:flex;flex-direction:column;overflow:hidden}
 #top-sticky{flex:none;background:#141414;border-bottom:2px solid #333;z-index:10}
 #bar{height:44px;padding:6px 12px;background:#111;border-bottom:1px solid #2b2b2b;display:flex;flex-wrap:nowrap;align-items:center;gap:8px;overflow-x:auto;white-space:nowrap}
 #bar > *{flex:none}
 input,select,button{font-family:monospace;font-size:13px;padding:4px 8px;background:#2a2a2a;
   color:#e4e4e4;border:1px solid #444;border-radius:4px;cursor:pointer}
 input{cursor:text}
 button:hover,select:hover{background:#363636}
 #status{display:inline-block;min-width:250px;color:#b8c7d9;white-space:nowrap}
 #tabs{height:35px;padding:6px 12px 0;background:#161616;border-bottom:1px solid #333;overflow-x:auto;overflow-y:hidden;white-space:nowrap}
 .tab{display:inline-block;padding:5px 14px;margin-right:4px;cursor:pointer;
   border:1px solid #444;border-bottom:none;border-radius:6px 6px 0 0;background:#222;font-size:13px}
 .tab.active{background:#26456e;border-color:#3a68a6;font-weight:bold;color:#fff}
 #cb-meta{min-height:36px;padding:7px 12px;background:#1c222b;border-bottom:1px solid #2e3b4e;font-size:12px;line-height:1.4;overflow-x:auto;white-space:nowrap}
 .meta-row{display:inline-flex;gap:16px;align-items:center}
 .meta-item b{color:#9ecbff}
 .pill-on{background:#1e4d2b;color:#8ef5a8;padding:1px 7px;border-radius:10px;font-size:11px}
 .pill-off{background:#4d2424;color:#ff9c9c;padding:1px 7px;border-radius:10px;font-size:11px}
 #scroll-area{flex:1;overflow-y:scroll;overflow-x:auto;padding:10px 12px 28px;overflow-anchor:none}
 .task{border:1px solid #333;border-radius:6px;margin:8px 0;overflow:hidden;background:#181818}
 .head{padding:6px 10px;background:#222;font-size:12px;display:flex;justify-content:space-between;align-items:center;gap:8px;overflow-x:auto;white-space:nowrap}
 .head-left{display:inline-flex;align-items:center;gap:6px;flex:none}
 .head-actions{display:inline-flex;gap:4px;flex:none}
 .badge{display:inline-block;background:#26456e;color:#fff;border-radius:4px;
   padding:1px 7px;font-weight:bold}
 .seq-tag{color:#e6eef8;font-weight:bold}
 .op-badge{display:inline-block;background:#2f3a29;color:#b8e994;border:1px solid #4a613d;
   border-radius:4px;padding:1px 6px;font-size:11px}
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
   font-size:13px;line-height:1.45;white-space:pre;
   user-select:text;-webkit-user-select:text;
 }
 .term-sep{color:#555}
 .term-head{color:#b9cde0;white-space:pre}
 .term-cmd{color:#7cc7ff;font-weight:bold}
 .term-out{color:#e6e6e6}
 #toast{position:fixed;bottom:16px;left:50%;transform:translateX(-50%);
   background:#26456e;color:#fff;padding:8px 16px;border-radius:6px;opacity:0;
   transition:opacity .2s;pointer-events:none;z-index:99}
 #toast.show{opacity:1}
</style></head><body>
<div id="top-sticky">
  <div id="bar">
    <span>Op:</span>
    <select id="op-sel"><option value="all">All Operations</option></select>
    <span>Callbacks:</span>
    <input id="cbs" value="" size="18" placeholder="blank = all (e.g. 20,21)">
    <span>Every</span>
    <input id="iv" value="2" size="2">
    <span>sec</span>
    <button id="order-btn">↓ Descending (Newest)</button>
    <button id="view-btn">View: Card</button>
    <button id="copy-sel-btn">Copy Selection</button>
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
let isSelecting = false;
let pendingRenderForce = false;
let lastDataSig = "";
let lastRenderedSig = "";
let lastOperations = [];
let lastCallbacks = [];
let lastTasks = [];
let lastCbError = "";
let currentShown = [];
let selectedOp = 'all';
let selectedTab = 'all';

function esc(s){
  return String(s == null ? "" : s).replace(/[&<>]/g, function(c){
    return {'&':'&amp;','<':'&lt;','>':'&gt;'}[c];
  });
}
function oneLine(s){
  return String(s == null ? "" : s).replace(/[\r\n]+/g, " ").trim();
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
  window._tt = setTimeout(function(){ t.classList.remove('show'); }, 1200);
}
function copyText(text, label){
  if(navigator.clipboard && navigator.clipboard.writeText){
    navigator.clipboard.writeText(text || '').catch(function(){});
  }
  toast('Copied ' + label);
}
function copySelectionOrAll(){
  const sel = window.getSelection ? window.getSelection().toString() : "";
  if(sel && sel.length > 0){
    copyText(sel, 'selected text');
    return;
  }
  const logEl = document.getElementById('log');
  const fallbackText = logEl ? (logEl.innerText || "") : "";
  copyText(fallbackText, 'visible log');
}
function copyCmd(i){ copyText(currentShown[i].cmd, 'command'); }
function copyOut(i){ copyText(currentShown[i].result, 'output'); }
function copyAll(i){ copyText(currentShown[i].all, 'full entry with timestamp'); }
function selectTab(t){ selectedTab = String(t); render(true); }

function renderOpSelect(){
  const sel = document.getElementById('op-sel');
  const opMap = {};
  for(const o of lastOperations){
    if(o.id != null) opMap[String(o.id)] = oneLine(o.name);
  }
  for(const c of lastCallbacks){
    if(c.op_id != null && !opMap[String(c.op_id)]){
      opMap[String(c.op_id)] = oneLine(c.op_name) || ('Op ' + c.op_id);
    }
  }
  for(const t of lastTasks){
    if(t.op_id != null && !opMap[String(t.op_id)]){
      opMap[String(t.op_id)] = oneLine(t.op_name) || ('Op ' + t.op_id);
    }
  }
  const ids = Object.keys(opMap).sort(function(a, b){ return Number(b) - Number(a); });
  if(selectedOp !== 'all' && !opMap[selectedOp]){
    selectedOp = 'all';
  }
  const nextHtml = ['<option value="all">All Operations</option>'].concat(
    ids.map(function(id){
      const selAttr = (String(id) === String(selectedOp)) ? ' selected' : '';
      return '<option value="' + esc(id) + '"' + selAttr + '>' + esc(opMap[id]) + ' (ID:' + esc(id) + ')</option>';
    })
  ).join('');
  if(sel.innerHTML !== nextHtml){
    sel.innerHTML = nextHtml;
  }
  sel.value = selectedOp;
}

function getOpFilteredCallbacks(){
  if(selectedOp === 'all') return lastCallbacks.slice();
  return lastCallbacks.filter(function(c){ return String(c.op_id) === String(selectedOp); });
}

function getOpFilteredTasks(){
  if(selectedOp === 'all') return lastTasks.slice();
  return lastTasks.filter(function(t){ return String(t.op_id) === String(selectedOp); });
}

function getTabIds(cbsInputList, opCbs, opTasks){
  const fetchedIds = opCbs.map(function(c){ return String(c.cb); });
  if(cbsInputList.length > 0){
    const merged = cbsInputList.slice();
    for(const id of fetchedIds){
      if(merged.indexOf(id) === -1) merged.push(id);
    }
    return merged;
  }
  const taskIds = [];
  for(const t of opTasks){
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
  const nextHtml = validTabs.map(function(t){
    const active = (String(t) === String(selectedTab)) ? ' active' : '';
    const label = (t === 'all') ? 'All' : ('CB ' + t);
    return '<span class="tab' + active + '" data-tab="' + esc(t) + '">' + esc(label) + '</span>';
  }).join('');
  const tabsEl = document.getElementById('tabs');
  if(tabsEl.innerHTML !== nextHtml){
    tabsEl.innerHTML = nextHtml;
    Array.prototype.forEach.call(document.querySelectorAll('#tabs .tab'), function(el){
      el.onclick = function(){ selectTab(el.getAttribute('data-tab')); };
    });
  }
}

function getCurrentOpLabel(){
  if(selectedOp === 'all') return 'All Operations';
  const found = lastOperations.find(function(o){ return String(o.id) === String(selectedOp); });
  return found ? (oneLine(found.name) + ' (ID:' + found.id + ')') : ('Op ' + selectedOp);
}

function renderCbMeta(tabIds, opCbs, shownTasks){
  const box = document.getElementById('cb-meta');
  const opLabel = getCurrentOpLabel();
  let nextHtml = '';

  if(selectedTab === 'all'){
    const totalCb = tabIds.length;
    const activeCount = opCbs.filter(function(c){ return c.active; }).length;
    const cbsArr = parseCbs();
    const cbScope = cbsArr.length > 0 ? ('Filtered (' + cbsArr.join(',') + ')') : 'All (Unfiltered)';
    nextHtml =
      '<div class="meta-row">' +
        '<span class="meta-item"><b>Operation:</b> ' + esc(opLabel) + '</span>' +
        '<span class="meta-item"><b>Callbacks:</b> ' + esc(cbScope) + ' / ' + totalCb + ' (' + activeCount + ' Active)</span>' +
        '<span class="meta-item"><b>Shown Tasks:</b> ' + shownTasks.length + '</span>' +
        '<span class="meta-item"><b>Sort:</b> ' + (sortDesc ? 'Descending (Newest)' : 'Ascending (Oldest)') + '</span>' +
        (lastCbError ? '<span class="err">(' + esc(lastCbError) + ')</span>' : '') +
      '</div>';
  } else {
    const info = opCbs.find(function(c){ return String(c.cb) === String(selectedTab); })
      || lastCallbacks.find(function(c){ return String(c.cb) === String(selectedTab); });

    if(!info){
      nextHtml =
        '<div class="meta-row">' +
          '<span class="meta-item"><b>Operation:</b> ' + esc(opLabel) + '</span>' +
          '<span class="meta-item"><b>CB ' + esc(selectedTab) + '</b></span>' +
          '<span class="dim">No callback metadata available (Shown tasks: ' + shownTasks.length + ')</span>' +
        '</div>';
    } else {
      const statusPill = info.active
        ? '<span class="pill-on">Active</span>'
        : '<span class="pill-off">Inactive</span>';

      nextHtml =
        '<div class="meta-row">' +
          '<span class="meta-item"><b>CB ' + esc(info.cb) + '</b> ' + statusPill + '</span>' +
          '<span class="meta-item"><b>Op:</b> ' + esc(oneLine(info.op_name) || opLabel) + '</span>' +
          '<span class="meta-item"><b>IP:</b> ' + esc(oneLine(info.ip)) + '</span>' +
          '<span class="meta-item"><b>Host:</b> ' + esc(oneLine(info.host)) + '</span>' +
          '<span class="meta-item"><b>User:</b> ' + esc(oneLine(info.user)) + '</span>' +
          '<span class="meta-item"><b>OS/Arch:</b> ' + esc(oneLine(info.os) + ' / ' + oneLine(info.arch)) + '</span>' +
          '<span class="meta-item"><b>PID:</b> ' + esc(oneLine(info.pid)) + '</span>' +
          '<span class="meta-item"><b>Last Checkin:</b> ' + esc(oneLine(info.last_checkin)) + '</span>' +
          '<span class="meta-item"><b>Tasks:</b> ' + shownTasks.length + '</span>' +
        '</div>';
    }
  }

  if(box.innerHTML !== nextHtml){
    box.innerHTML = nextHtml;
  }
}

function block(t, i){
  const opClean = oneLine(t.op_name);
  const timeClean = oneLine(t.time);
  const statusClean = oneLine(t.status);
  const operClean = oneLine(t.operator);
  return '<div class="task">' +
    '<div class="head">' +
      '<div class="head-left">' +
        '<span class="badge">CB ' + esc(t.cb) + '</span>' +
        '<span class="seq-tag">#' + esc(t.seq) + '</span>' +
        '<span class="dim">(task ' + esc(t.task) + ')</span>' +
        '<span class="op-badge">' + esc(opClean) + '</span>' +
        '<span class="dim">|</span><span>' + esc(timeClean) + '</span>' +
        '<span class="dim">|</span><span>' + esc(statusClean) + '</span>' +
        (operClean ? '<span class="dim">|</span><span>' + esc(operClean) + '</span>' : '') +
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
    const meta = '[CB ' + oneLine(t.cb)
      + ' | #' + oneLine(t.seq) + ' (task ' + oneLine(t.task) + ')'
      + ' | ' + oneLine(t.op_name)
      + ' | ' + oneLine(t.time)
      + ' | ' + oneLine(t.status)
      + (oneLine(t.operator) ? (' | ' + oneLine(t.operator)) : '') + ']';
    return '<span class="term-sep">------------------------------------------------------------</span>\n' +
      '<span class="term-head">' + esc(meta) + '</span>\n' +
      '<span class="term-cmd">$ ' + esc(t.cmd) + '</span>\n' +
      '<span class="term-out">' + esc(t.result) + '</span>';
  });
  return '<pre class="term-view">' + parts.join('\n') + '\n<span class="term-sep">------------------------------------------------------------</span></pre>';
}

function render(force){
  if(isSelecting){
    pendingRenderForce = pendingRenderForce || Boolean(force);
    return;
  }

  renderOpSelect();
  const cbsInputList = parseCbs();
  const opCbs = getOpFilteredCallbacks();
  const opTasks = getOpFilteredTasks();
  const tabIds = getTabIds(cbsInputList, opCbs, opTasks);
  renderTabs(tabIds);

  let shown = opTasks.slice();
  if(selectedTab !== 'all'){
    shown = shown.filter(function(t){ return String(t.cb) === String(selectedTab); });
  }
  shown.sort(function(a, b){
    return sortDesc ? (Number(b.id) - Number(a.id)) : (Number(a.id) - Number(b.id));
  });
  currentShown = shown;

  renderCbMeta(tabIds, opCbs, shown);

  const viewSig = JSON.stringify({
    textMode: textMode,
    sortDesc: sortDesc,
    selectedOp: selectedOp,
    selectedTab: selectedTab,
    ids: shown.map(function(t){ return t.id + ':' + t.status + ':' + t.result.length; })
  });

  if(!force && viewSig === lastRenderedSig){
    return;
  }
  lastRenderedSig = viewSig;

  const scrollArea = document.getElementById('scroll-area');
  const prevTop = scrollArea.scrollTop;
  const prevLeft = scrollArea.scrollLeft;

  const nextLogHtml = textMode
    ? renderTerminalView(shown)
    : (shown.map(function(t, i){ return block(t, i); }).join('') || '(No tasks to display)');

  const logEl = document.getElementById('log');
  if(logEl.innerHTML !== nextLogHtml){
    logEl.innerHTML = nextLogHtml;
    scrollArea.scrollTop = prevTop;
    scrollArea.scrollLeft = prevLeft;
  }
}

document.getElementById('scroll-area').addEventListener('mousedown', function(ev){
  if(ev.button === 0) isSelecting = true;
});
window.addEventListener('mouseup', function(){
  if(isSelecting){
    isSelecting = false;
    if(pendingRenderForce){
      const f = pendingRenderForce;
      pendingRenderForce = false;
      render(f);
    }
  }
});

async function switchOpOnServer(opVal){
  if(opVal === 'all') return;
  try{
    const r = await fetch('/api/switch_op?op_id=' + encodeURIComponent(opVal), { cache: 'no-store' });
    const data = await r.json();
    if(!r.ok || data.error) throw new Error(data.error || ('HTTP ' + r.status));
    toast('Switched operation (ID:' + opVal + ')');
  }catch(e){
    toast('Switch error: ' + String((e && e.message) || e));
  }
}

async function tick(forceRender){
  if(!running) return;
  const cbs = parseCbs().join(',');
  try{
    const url = '/api/history' + (cbs ? ('?cbs=' + encodeURIComponent(cbs)) : '');
    const r = await fetch(url, { cache: 'no-store' });
    const text = await r.text();
    let data;
    try { data = JSON.parse(text); } catch(_){ throw new Error('Invalid JSON: ' + text.slice(0, 300)); }
    if(!r.ok || data.error) throw new Error(data.error || ('HTTP ' + r.status));

    const sig = JSON.stringify(data);
    const dataChanged = (sig !== lastDataSig);
    lastDataSig = sig;

    lastOperations = data.operations || [];
    lastCallbacks = data.callbacks || [];
    lastTasks = data.tasks || [];
    lastCbError = data.cb_error || "";

    const scopeText = cbs ? ('CB ' + cbs) : 'All CBs';
    const opShownCount = getOpFilteredTasks().length;
    document.getElementById('status').textContent =
      '(' + scopeText + ' / ' + opShownCount + ' tasks / ' + new Date().toLocaleTimeString() + ')';

    render(Boolean(forceRender || dataChanged));
  }catch(e){
    const msg = String((e && e.message) || e);
    document.getElementById('status').innerHTML = '<span class="err">Error</span>';
    document.getElementById('cb-meta').innerHTML = '<span class="err">Fetch Error: ' + esc(msg) + '</span>';
    document.getElementById('log').innerHTML = '<pre class="err">' + esc(msg) + '</pre>';
  }
}

document.getElementById('op-sel').addEventListener('change', async function(){
  selectedOp = this.value;
  selectedTab = 'all';
  render(true);
  await switchOpOnServer(selectedOp);
  tick(true);
});
document.getElementById('order-btn').onclick = function(){
  sortDesc = !sortDesc;
  this.textContent = sortDesc ? '↓ Descending (Newest)' : '↑ Ascending (Oldest)';
  render(true);
};
document.getElementById('view-btn').onclick = function(){
  textMode = !textMode;
  this.textContent = textMode ? 'View: Text' : 'View: Card';
  render(true);
};
document.getElementById('copy-sel-btn').onclick = function(){
  copySelectionOrAll();
};
document.getElementById('toggle').onclick = function(){
  running = !running;
  this.textContent = running ? '⏸ Pause' : '▶ Resume';
};
document.getElementById('cbs').addEventListener('change', function(){ tick(true); });

function loopTick(){
  tick(false).finally(function(){
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
        elif u.path == "/api/switch_op":
            q = parse_qs(u.query)
            op_raw = q.get("op_id", [""])[0].strip()
            try:
                if not op_raw.isdigit():
                    raise ValueError("invalid op_id")
                info = switch_operation(int(op_raw))
                body = json.dumps(info, ensure_ascii=False).encode("utf-8")
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
    print(f"Login succeeded (operator_id={current_operator_id}). Open http://127.0.0.1:{WEB_PORT} in your browser.", flush=True)
    ThreadingHTTPServer(("0.0.0.0", WEB_PORT), Handler).serve_forever()


if __name__ == "__main__":
    main()
