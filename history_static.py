#!/usr/bin/env python3
"""
generate_history_static.py
Fetch callbacks & tasks across ALL operations by switching current operation
via Mythic SDK, then export a standalone static HTML viewer.
"""
import os
import sys
import inspect
import asyncio
import base64
import binascii
import json
from contextlib import contextmanager
from pathlib import Path
from mythic import mythic

# ====== Update for your environment ======
USERNAME = "mythic_admin"
PASSWORD = "password"
SERVER_IP = "127.0.0.1"
SERVER_PORT = 7443
LIMIT = 300
SUPPRESS_C_STDERR = True
OUT_FILE = Path("mythic_history_static.html")
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
    current_operation_id
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

SEQ_ALL_QUERY = """
query SeqAll {
  task(order_by: {id: asc}) {
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


def decode_chunk(s):
    if not s:
        return ""
    t = "".join(str(s).split())
    t += "=" * (-len(t) % 4)
    try:
        return base64.b64decode(t).decode("utf-8", errors="replace")
    except (binascii.Error, ValueError):
        return str(s)


def unwrap_gql(res):
    if isinstance(res, dict) and "data" in res and isinstance(res["data"], dict):
        return res["data"]
    return res or {}


async def _exec_gql(client, query, variables=None, timeout=20):
    q_str = query if isinstance(query, str) else str(query)
    vars_dict = variables if isinstance(variables, dict) else {}
    with quiet_c_stderr():
        try:
            return await asyncio.wait_for(
                mythic.execute_custom_query(
                    client,
                    query=q_str,
                    variables=vars_dict,
                ),
                timeout=timeout,
            )
        except TypeError:
            from gql import gql as gql_parse
            parsed = gql_parse(q_str)
            return await asyncio.wait_for(
                mythic.execute_custom_query(
                    client,
                    query=parsed,
                    variables=vars_dict,
                ),
                timeout=timeout,
            )


async def _resolve_operator_info(client):
    uid = None
    orig_op = None
    for attr in ("current_operator_id", "operator_id", "id"):
        val = getattr(client, attr, None)
        if isinstance(val, int):
            uid = val
            break
    try:
        raw = await _exec_gql(client, OPERATOR_ID_QUERY, {"username": USERNAME}, timeout=10)
        ops = unwrap_gql(raw).get("operator") or []
        if ops:
            if ops[0].get("id") is not None:
                uid = int(ops[0]["id"])
            if ops[0].get("current_operation_id") is not None:
                orig_op = int(ops[0]["current_operation_id"])
    except Exception:
        pass
    return uid, orig_op


async def _switch_op(client, uid, op_id):
    with quiet_c_stderr():
        res = mythic.update_current_operation_for_user(
            client,
            int(uid),
            int(op_id),
        )
        if inspect.isawaitable(res):
            await asyncio.wait_for(res, timeout=15)


async def collect_snapshot():
    with quiet_c_stderr():
        client = await mythic.login(
            username=USERNAME,
            password=PASSWORD,
            server_ip=SERVER_IP,
            server_port=SERVER_PORT,
            ssl=True,
            timeout=-1,
        )

    uid, orig_op = await _resolve_operator_info(client)
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
                res = mythic.get_operations(mythic=client)
                if inspect.isawaitable(res):
                    res = await asyncio.wait_for(res, timeout=15)
            if isinstance(res, list):
                for o in res:
                    if isinstance(o, dict):
                        register_op(o.get("id"), o.get("name"), o.get("complete", False))
                loaded_ops = bool(ops_by_id)
        except Exception:
            pass

    if not loaded_ops:
        raw_op = await _exec_gql(client, OP_QUERY, {})
        for o in (unwrap_gql(raw_op).get("operation") or []):
            register_op(o.get("id"), o.get("name"), o.get("complete", False))

    # 取得対象にする Operation ID 一覧（取れなかった場合は 1..10 を走査）
    target_op_ids = [
        int(o["id"]) for o in ops_by_id.values() if str(o.get("id", "")).isdigit()
    ]
    if not target_op_ids:
        target_op_ids = list(range(1, 11))

    callbacks_by_key = {}
    tasks_by_id = {}

    for target_op in sorted(set(target_op_ids)):
        if uid is not None:
            try:
                await _switch_op(client, uid, target_op)
            except Exception:
                continue

        # 切替後に operation 名も再取得できる場合は補完
        try:
            raw_op = await _exec_gql(client, OP_QUERY, {})
            for o in (unwrap_gql(raw_op).get("operation") or []):
                register_op(o.get("id"), o.get("name"), o.get("complete", False))
        except Exception:
            pass

        try:
            raw_cb = await _exec_gql(client, CB_ALL_QUERY, {})
            for c in (unwrap_gql(raw_cb).get("callback") or []):
                op_id = c.get("operation_id") if c.get("operation_id") is not None else target_op
                register_op(op_id)
                op_name = (
                    ops_by_id[str(op_id)]["name"]
                    if str(op_id) in ops_by_id
                    else f"Op {op_id}"
                )
                cb_disp = c.get("display_id", "?")
                cb_key = f"{op_id}:{c.get('id', cb_disp)}"
                callbacks_by_key[cb_key] = {
                    "cb": cb_disp,
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
                }
        except Exception:
            pass

        seq_map, counter = {}, {}
        try:
            raw_seq = await _exec_gql(client, SEQ_ALL_QUERY, {})
            for row in (unwrap_gql(raw_seq).get("task") or []):
                cb_row = row.get("callback") or {}
                cb = cb_row.get("display_id")
                op = cb_row.get("operation_id") if cb_row.get("operation_id") is not None else target_op
                register_op(op)
                key = (op, cb)
                counter[key] = counter.get(key, 0) + 1
                seq_map[row["id"]] = counter[key]
        except Exception:
            pass

        try:
            raw_live = await _exec_gql(client, LIVE_ALL_QUERY, {"limit": LIMIT})
            for t in (unwrap_gql(raw_live).get("task") or []):
                chunks = [decode_chunk(r.get("response_text")) for r in (t.get("responses") or [])]
                result = "".join(chunks).rstrip() or "(no output)"
                cmd = f"{t.get('command_name', '')} {t.get('display_params', '') or ''}".strip()
                cb_obj = t.get("callback") or {}
                cb_id = cb_obj.get("display_id", "?")
                op_id = cb_obj.get("operation_id") if cb_obj.get("operation_id") is not None else target_op
                register_op(op_id)
                op_name = (
                    ops_by_id[str(op_id)]["name"]
                    if str(op_id) in ops_by_id
                    else f"Op {op_id}"
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

                tasks_by_id[t["id"]] = {
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
                }
        except Exception:
            pass

    # 元の Operation に戻す
    if uid is not None and orig_op is not None:
        try:
            await _switch_op(client, uid, orig_op)
        except Exception:
            pass

    callbacks_info = list(callbacks_by_key.values())
    for c in callbacks_info:
        oid = c.get("op_id")
        if oid is not None and str(oid) in ops_by_id:
            c["op_name"] = ops_by_id[str(oid)]["name"]

    tasks_out = sorted(tasks_by_id.values(), key=lambda x: int(x["id"]), reverse=True)
    for t in tasks_out:
        oid = t.get("op_id")
        if oid is not None and str(oid) in ops_by_id:
            t["op_name"] = ops_by_id[str(oid)]["name"]

    operations_info = sorted(
        ops_by_id.values(),
        key=lambda x: int(x["id"]) if str(x["id"]).isdigit() else 0,
        reverse=True,
    )

    return {
        "operations": operations_info,
        "callbacks": callbacks_info,
        "tasks": tasks_out,
    }


HTML_TEMPLATE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Mythic Static History</title>
<style>
  * { box-sizing: border-box; }
  html, body {
    background: #1b1b1b; color: #e4e4e4;
    font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
    margin: 0; min-height: 100vh; max-width: 100%;
  }
  #top {
    position: sticky; top: 0; z-index: 10;
    background: #141414; border-bottom: 2px solid #333; max-width: 100%;
  }
  #bar {
    padding: 8px 12px; background: #111; border-bottom: 1px solid #2b2b2b;
    display: flex; flex-wrap: wrap; align-items: center; gap: 8px; max-width: 100%;
  }
  select, button {
    font-family: inherit; font-size: 13px; padding: 4px 9px;
    background: #2a2a2a; color: #e4e4e4; border: 1px solid #444;
    border-radius: 4px; cursor: pointer; max-width: 100%;
  }
  button:hover, select:hover { background: #363636; }
  #summary { color: #b8c7d9; font-size: 12px; }
  #tabs {
    padding: 6px 12px 0; background: #161616; border-bottom: 1px solid #333;
    display: flex; flex-wrap: wrap; gap: 4px; max-width: 100%;
  }
  .tab {
    padding: 5px 14px; cursor: pointer;
    border: 1px solid #444; border-bottom: none; border-radius: 6px 6px 0 0;
    background: #222; color: #e4e4e4; font-size: 13px; white-space: nowrap;
  }
  .tab.active {
    background: #26456e; border-color: #3a68a6; font-weight: bold; color: #fff;
  }
  #cb-meta {
    min-height: 36px; padding: 7px 12px; background: #1c222b;
    border-bottom: 1px solid #2e3b4e; font-size: 12px; line-height: 1.45;
    max-width: 100%; overflow-wrap: anywhere; word-break: break-word;
  }
  .meta-row {
    display: flex; flex-wrap: wrap; gap: 6px 16px; align-items: center; max-width: 100%;
  }
  .meta-item {
    min-width: 0; max-width: 100%; overflow-wrap: anywhere; word-break: break-word;
  }
  .meta-item b { color: #9ecbff; }
  .pill-on {
    background: #1e4d2b; color: #8ef5a8; padding: 1px 7px;
    border-radius: 10px; font-size: 11px; white-space: nowrap;
  }
  .pill-off {
    background: #4d2424; color: #ff9c9c; padding: 1px 7px;
    border-radius: 10px; font-size: 11px; white-space: nowrap;
  }
  #log { padding: 10px 12px 28px; max-width: 100%; }
  .task {
    border: 1px solid #333; border-radius: 6px; margin: 8px 0;
    overflow: hidden; background: #181818; max-width: 100%;
  }
  .head {
    padding: 6px 10px; background: #222; font-size: 12px;
    display: flex; flex-wrap: wrap; justify-content: space-between; align-items: center;
    gap: 6px 8px; max-width: 100%;
  }
  .head-left {
    display: flex; flex-wrap: wrap; align-items: center; gap: 6px;
    min-width: 0; max-width: 100%; overflow-wrap: anywhere; word-break: break-word;
  }
  .head-actions { display: inline-flex; flex-wrap: wrap; gap: 4px; }
  .badge {
    display: inline-block; background: #26456e; color: #fff;
    border-radius: 4px; padding: 1px 7px; font-weight: bold; white-space: nowrap;
  }
  .seq-tag { color: #e6eef8; font-weight: bold; white-space: nowrap; }
  .op-badge {
    display: inline-block; background: #2f3a29; color: #b8e994;
    border: 1px solid #4a613d; border-radius: 4px; padding: 1px 6px;
    font-size: 11px; overflow-wrap: anywhere; word-break: break-word;
  }
  .dim { color: #9aa; }
  .cmd {
    padding: 7px 10px; color: #8fd0ff; white-space: pre-wrap;
    overflow-wrap: anywhere; word-break: break-word; background: #151a20;
  }
  .result {
    padding: 8px 10px; white-space: pre-wrap; overflow-wrap: anywhere;
    word-break: break-word; background: #161616;
    max-height: 420px; overflow-y: auto; overflow-x: hidden;
    border-top: 1px solid #2c2c2c;
  }
  .cp { font-size: 11px; padding: 2px 8px; background: #323232; border: 1px solid #4a4a4a; }
  .cp:hover { background: #444; }
  .err { color: #ff6b6b; white-space: pre-wrap; overflow-wrap: anywhere; }
  .term-view {
    margin: 0; padding: 12px 14px; background: #101010; color: #e6e6e6;
    font-size: 13px; line-height: 1.45; white-space: pre-wrap;
    overflow-wrap: anywhere; word-break: break-word; max-width: 100%;
  }
  .term-sep { color: #555; }
  .term-head { color: #b9cde0; }
  .term-cmd { color: #7cc7ff; font-weight: bold; }
  #toast {
    position: fixed; bottom: 16px; left: 50%; transform: translateX(-50%);
    background: #26456e; color: #fff; padding: 8px 16px; border-radius: 6px;
    opacity: 0; transition: opacity .2s; pointer-events: none; z-index: 99;
  }
  #toast.show { opacity: 1; }
</style>
</head>
<body>
<div id="top">
  <div id="bar">
    <span>Op:</span>
    <select id="op-sel"></select>
    <button id="order-btn">↓ Descending (Newest)</button>
    <button id="view-btn">View: Card</button>
    <span id="summary">(Initializing...)</span>
  </div>
  <div id="tabs"></div>
  <div id="cb-meta"></div>
</div>
<div id="log">(Initializing...)</div>
<div id="toast"></div>
<script>
window.addEventListener("error", function(ev) {
  const logEl = document.getElementById("log");
  const sumEl = document.getElementById("summary");
  if (sumEl) sumEl.innerHTML = '<span class="err">JS Error</span>';
  if (logEl) {
    logEl.innerHTML = '<pre class="err">' + String(ev.message || ev) + ' (line ' + (ev.lineno || '?') + ')</pre>';
  }
});

const DATA = __DATA_JSON__;
const OPERATIONS = Array.isArray(DATA.operations) ? DATA.operations : [];
const CALLBACKS = Array.isArray(DATA.callbacks) ? DATA.callbacks : [];
const TASKS = Array.isArray(DATA.tasks) ? DATA.tasks : [];

let selectedOp = "all";
let selectedTab = "all";
let sortDesc = true;
let textMode = false;
let currentShown = [];

function esc(s) {
  return String(s == null ? "" : s).replace(/[&<>]/g, function(c) {
    return { "&": "&amp;", "<": "&lt;", ">": "&gt;" }[c];
  });
}
function oneLine(s) {
  return String(s == null ? "" : s).replace(/[\r\n]+/g, " ").trim();
}
function toast(msg) {
  const el = document.getElementById("toast");
  el.textContent = msg;
  el.classList.add("show");
  clearTimeout(window._tt);
  window._tt = setTimeout(function() { el.classList.remove("show"); }, 1200);
}
function copyText(text, label) {
  if (navigator.clipboard && navigator.clipboard.writeText) {
    navigator.clipboard.writeText(text || "").catch(function() {});
  }
  toast("Copied " + label);
}
function copyCmd(i) { copyText(currentShown[i].cmd, "command"); }
function copyOut(i) { copyText(currentShown[i].result, "output"); }
function copyAll(i) { copyText(currentShown[i].all, "full entry"); }

function buildOpMap() {
  const map = {};
  for (const o of OPERATIONS) {
    if (o && o.id != null) map[String(o.id)] = oneLine(o.name);
  }
  for (const c of CALLBACKS) {
    if (c && c.op_id != null && !map[String(c.op_id)]) {
      map[String(c.op_id)] = oneLine(c.op_name) || ("Op " + c.op_id);
    }
  }
  for (const t of TASKS) {
    if (t && t.op_id != null && !map[String(t.op_id)]) {
      map[String(t.op_id)] = oneLine(t.op_name) || ("Op " + t.op_id);
    }
  }
  return map;
}

function renderOpSelect() {
  const opMap = buildOpMap();
  const sel = document.getElementById("op-sel");
  const ids = Object.keys(opMap).sort(function(a, b) { return Number(b) - Number(a); });
  if (selectedOp !== "all" && !opMap[selectedOp]) selectedOp = "all";

  sel.innerHTML = ['<option value="all">All Operations</option>'].concat(
    ids.map(function(id) {
      const s = String(id) === String(selectedOp) ? " selected" : "";
      return '<option value="' + esc(id) + '"' + s + '>' + esc(opMap[id]) + ' (ID:' + esc(id) + ')</option>';
    })
  ).join("");
  sel.value = selectedOp;
}

function getOpCallbacks() {
  if (selectedOp === "all") return CALLBACKS.slice();
  return CALLBACKS.filter(function(c) { return String(c.op_id) === String(selectedOp); });
}

function getOpTasks() {
  if (selectedOp === "all") return TASKS.slice();
  return TASKS.filter(function(t) { return String(t.op_id) === String(selectedOp); });
}

function getTabIds(opCbs, opTasks) {
  const set = new Set();
  for (const c of opCbs) {
    if (c && c.cb != null && String(c.cb) !== "?") set.add(String(c.cb));
  }
  for (const t of opTasks) {
    if (t && t.cb != null && String(t.cb) !== "?") set.add(String(t.cb));
  }
  return Array.from(set).sort(function(a, b) { return Number(a) - Number(b); });
}

function renderTabs(tabIds) {
  const valid = ["all"].concat(tabIds);
  if (valid.indexOf(selectedTab) === -1) selectedTab = "all";
  const el = document.getElementById("tabs");
  el.innerHTML = valid.map(function(t) {
    const active = String(t) === String(selectedTab) ? " active" : "";
    const label = t === "all" ? "All" : ("CB " + t);
    return '<button class="tab' + active + '" onclick="selectTab(\'' + esc(t) + '\')">' + esc(label) + '</button>';
  }).join("");
}

function selectTab(t) {
  selectedTab = String(t);
  render();
}

function getOpLabel() {
  if (selectedOp === "all") return "All Operations";
  const map = buildOpMap();
  return map[selectedOp] ? (map[selectedOp] + " (ID:" + selectedOp + ")") : ("Op " + selectedOp);
}

function renderCbMeta(tabIds, opCbs, shown) {
  const box = document.getElementById("cb-meta");
  const opLabel = getOpLabel();

  if (selectedTab === "all") {
    const activeCount = opCbs.filter(function(c) { return Boolean(c.active); }).length;
    box.innerHTML =
      '<div class="meta-row">' +
        '<span class="meta-item"><b>Operation:</b> ' + esc(opLabel) + '</span>' +
        '<span class="meta-item"><b>Callbacks:</b> ' + tabIds.length + ' (' + activeCount + ' Active)</span>' +
        '<span class="meta-item"><b>Shown Tasks:</b> ' + shown.length + '</span>' +
        '<span class="meta-item"><b>Sort:</b> ' + (sortDesc ? "Descending (Newest)" : "Ascending (Oldest)") + '</span>' +
      '</div>';
    return;
  }

  const info = opCbs.find(function(c) { return String(c.cb) === String(selectedTab); })
    || CALLBACKS.find(function(c) { return String(c.cb) === String(selectedTab); });

  if (!info) {
    box.innerHTML =
      '<div class="meta-row">' +
        '<span class="meta-item"><b>Operation:</b> ' + esc(opLabel) + '</span>' +
        '<span class="meta-item"><b>CB ' + esc(selectedTab) + '</b></span>' +
        '<span class="dim">No callback metadata (Shown tasks: ' + shown.length + ')</span>' +
      '</div>';
    return;
  }

  const pill = info.active
    ? '<span class="pill-on">Active</span>'
    : '<span class="pill-off">Inactive</span>';

  box.innerHTML =
    '<div class="meta-row">' +
      '<span class="meta-item"><b>CB ' + esc(info.cb) + '</b> ' + pill + '</span>' +
      '<span class="meta-item"><b>Op:</b> ' + esc(oneLine(info.op_name) || opLabel) + '</span>' +
      '<span class="meta-item"><b>IP:</b> ' + esc(oneLine(info.ip)) + '</span>' +
      '<span class="meta-item"><b>Host:</b> ' + esc(oneLine(info.host)) + '</span>' +
      '<span class="meta-item"><b>User:</b> ' + esc(oneLine(info.user)) + '</span>' +
      '<span class="meta-item"><b>OS/Arch:</b> ' + esc(oneLine(info.os) + " / " + oneLine(info.arch)) + '</span>' +
      '<span class="meta-item"><b>PID:</b> ' + esc(oneLine(info.pid)) + '</span>' +
      '<span class="meta-item"><b>Last Checkin:</b> ' + esc(oneLine(info.last_checkin)) + '</span>' +
      '<span class="meta-item"><b>Tasks:</b> ' + shown.length + '</span>' +
    '</div>';
}

function cardHtml(t, i) {
  const oper = oneLine(t.operator);
  return (
    '<div class="task">' +
      '<div class="head">' +
        '<div class="head-left">' +
          '<span class="badge">CB ' + esc(t.cb) + '</span>' +
          '<span class="seq-tag">#' + esc(t.seq) + '</span>' +
          '<span class="dim">(task ' + esc(t.task) + ')</span>' +
          '<span class="op-badge">' + esc(oneLine(t.op_name)) + '</span>' +
          '<span class="dim">|</span><span>' + esc(oneLine(t.time)) + '</span>' +
          '<span class="dim">|</span><span>' + esc(oneLine(t.status)) + '</span>' +
          (oper ? ('<span class="dim">|</span><span>' + esc(oper) + '</span>') : '') +
        '</div>' +
        '<div class="head-actions">' +
          '<button class="cp" onclick="copyCmd(' + i + ')">copy cmd</button>' +
          '<button class="cp" onclick="copyOut(' + i + ')">copy out</button>' +
          '<button class="cp" onclick="copyAll(' + i + ')">copy all</button>' +
        '</div>' +
      '</div>' +
      '<div class="cmd">' + esc(t.cmd) + '</div>' +
      '<div class="result">' + esc(t.result) + '</div>' +
    '</div>'
  );
}

function textHtml(shown) {
  if (!shown.length) return '<pre class="term-view">(No tasks to display)</pre>';
  const parts = shown.map(function(t) {
    const meta = '[CB ' + oneLine(t.cb)
      + ' | #' + oneLine(t.seq) + ' (task ' + oneLine(t.task) + ')'
      + ' | ' + oneLine(t.op_name)
      + ' | ' + oneLine(t.time)
      + ' | ' + oneLine(t.status)
      + (oneLine(t.operator) ? (' | ' + oneLine(t.operator)) : '') + ']';
    return '<span class="term-sep">------------------------------------------------------------</span>\n'
      + '<span class="term-head">' + esc(meta) + '</span>\n'
      + '<span class="term-cmd">$ ' + esc(t.cmd) + '</span>\n'
      + '<span class="term-out">' + esc(t.result) + '</span>';
  });
  return '<pre class="term-view">' + parts.join('\n') + '\n<span class="term-sep">------------------------------------------------------------</span></pre>';
}

function render() {
  renderOpSelect();
  const opCbs = getOpCallbacks();
  const opTasks = getOpTasks();
  const tabIds = getTabIds(opCbs, opTasks);
  renderTabs(tabIds);

  let shown = opTasks.slice();
  if (selectedTab !== "all") {
    shown = shown.filter(function(t) { return String(t.cb) === String(selectedTab); });
  }
  shown.sort(function(a, b) {
    return sortDesc ? (Number(b.id) - Number(a.id)) : (Number(a.id) - Number(b.id));
  });
  currentShown = shown;

  renderCbMeta(tabIds, opCbs, shown);
  document.getElementById("summary").textContent = '(' + getOpLabel() + ' / ' + shown.length + ' tasks)';

  const logEl = document.getElementById("log");
  logEl.innerHTML = textMode
    ? textHtml(shown)
    : (shown.map(function(t, i) { return cardHtml(t, i); }).join("") || "(No tasks to display)");
}

document.getElementById("op-sel").addEventListener("change", function() {
  selectedOp = this.value;
  selectedTab = "all";
  render();
});
document.getElementById("order-btn").onclick = function() {
  sortDesc = !sortDesc;
  this.textContent = sortDesc ? "↓ Descending (Newest)" : "↑ Ascending (Oldest)";
  render();
};
document.getElementById("view-btn").onclick = function() {
  textMode = !textMode;
  this.textContent = textMode ? "View: Text" : "View: Card";
  render();
};

render();
</script>
</body>
</html>
"""


def build_html(payload: dict) -> str:
    safe_json = json.dumps(payload, ensure_ascii=False).replace("</", "<\\/")
    return HTML_TEMPLATE.replace("__DATA_JSON__", safe_json)


async def main():
    payload = await collect_snapshot()
    OUT_FILE.write_text(build_html(payload), encoding="utf-8")
    print(
        f"Saved static HTML to {OUT_FILE.resolve()} "
        f"(ops={len(payload['operations'])}, cbs={len(payload['callbacks'])}, tasks={len(payload['tasks'])})"
    )


if __name__ == "__main__":
    asyncio.run(main())
