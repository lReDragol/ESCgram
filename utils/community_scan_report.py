from __future__ import annotations

import json
from datetime import datetime
from html import escape
from typing import Any, Dict, Iterable, List


def _fmt_int(value: Any) -> str:
    try:
        return f"{int(value or 0):,}".replace(",", " ")
    except Exception:
        return "0"


def _fmt_ts(value: Any) -> str:
    try:
        ts = int(value or 0)
    except Exception:
        ts = 0
    if ts <= 0:
        return "n/a"
    try:
        return datetime.fromtimestamp(ts).strftime("%d.%m.%Y %H:%M")
    except Exception:
        return str(ts)


def _safe_json(data: Any) -> str:
    raw = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    return raw.replace("</", "<\\/")


def _risk_level(score: Any) -> str:
    try:
        value = int(score or 0)
    except Exception:
        value = 0
    if value >= 70:
        return "Критический"
    if value >= 50:
        return "Высокий"
    if value >= 25:
        return "Средний"
    return "Низкий"


def _summary_cards(summary: Dict[str, Any], community: Dict[str, Any]) -> str:
    coordinated = len(list(community.get("coordinated_users") or [])) if isinstance(community, dict) else 0
    cards = [
        ("Чатов в сообществе", _fmt_int(summary.get("total_chats")), "Обнаруженные узлы сообщества"),
        ("Доступных чатов", _fmt_int(summary.get("accessible_chats")), "Чаты, историю которых удалось прочитать"),
        ("Сообщений", _fmt_int(summary.get("total_messages")), "Суммарный объём истории"),
        ("Новых сообщений", _fmt_int(summary.get("new_messages")), "Найдено инкрементально на последнем проходе"),
        ("Участников", _fmt_int(summary.get("users")), "Уникальные отправители в выгрузке"),
        ("Подозрительных", _fmt_int(summary.get("suspicious_users")), "Аккаунты с risk score >= 18"),
        ("Координация", _fmt_int(coordinated), "Пользователи, замеченные в нескольких чатах"),
        ("Средний риск", _fmt_int(summary.get("risk_score_avg")), _risk_level(summary.get("risk_score_avg"))),
    ]
    return "".join(
        (
            '<article class="summary-card">'
            f'<div class="summary-card__label">{escape(label)}</div>'
            f'<div class="summary-card__value">{escape(value)}</div>'
            f'<div class="summary-card__hint">{escape(hint)}</div>'
            "</article>"
        )
        for label, value, hint in cards
    )


_STYLESHEET = """
:root{
  color-scheme:dark;
  --bg:#09111d;
  --bg-soft:#0f1b2d;
  --panel:#101c2f;
  --panel-2:#13243a;
  --line:rgba(151,181,219,.16);
  --text:#eaf2ff;
  --muted:#8ea7c8;
  --accent:#53b7ff;
  --accent-2:#77e7b4;
  --warn:#ffb454;
  --danger:#ff6b8b;
  --ok:#4ade80;
  --shadow:0 18px 50px rgba(0,0,0,.28);
}
*{box-sizing:border-box}
html,body{margin:0;padding:0;background:radial-gradient(circle at top,#13233b 0,#09111d 52%,#060c15 100%);color:var(--text);font:14px/1.5 "Segoe UI",Inter,sans-serif}
body{min-height:100vh}
a{color:var(--accent);text-decoration:none}
.app{padding:18px}
.topbar{position:sticky;top:0;z-index:20;display:grid;gap:14px;padding:16px 18px;border:1px solid var(--line);border-radius:22px;background:rgba(9,17,29,.92);backdrop-filter:blur(14px);box-shadow:var(--shadow)}
.topbar__head{display:flex;align-items:flex-start;justify-content:space-between;gap:18px}
.topbar__title h1{margin:0;font-size:24px;line-height:1.15}
.topbar__title p{margin:6px 0 0;color:var(--muted)}
.pill-row{display:flex;flex-wrap:wrap;gap:8px}
.pill{display:inline-flex;align-items:center;gap:6px;padding:5px 10px;border:1px solid var(--line);border-radius:999px;background:rgba(255,255,255,.03);color:var(--muted);font-size:12px}
.pill--accent{color:#d8f1ff;border-color:rgba(83,183,255,.35);background:rgba(83,183,255,.12)}
.pill--warn{color:#ffe0b2;border-color:rgba(255,180,84,.35);background:rgba(255,180,84,.12)}
.pill--danger{color:#ffd2da;border-color:rgba(255,107,139,.35);background:rgba(255,107,139,.12)}
.topbar__filters{display:grid;grid-template-columns:minmax(240px,2fr) repeat(5,minmax(130px,1fr));gap:10px}
.control{display:grid;gap:6px}
.control label{font-size:12px;color:var(--muted)}
.control input,.control select{width:100%;padding:11px 12px;border-radius:12px;border:1px solid var(--line);background:var(--panel-2);color:var(--text);outline:none}
.control input:focus,.control select:focus{border-color:rgba(83,183,255,.45);box-shadow:0 0 0 3px rgba(83,183,255,.12)}
.summary-grid{display:grid;grid-template-columns:repeat(8,minmax(0,1fr));gap:10px}
.summary-card{padding:14px;border:1px solid var(--line);border-radius:18px;background:linear-gradient(180deg,rgba(83,183,255,.08),rgba(255,255,255,.02));min-height:108px}
.summary-card__label{font-size:12px;color:var(--muted)}
.summary-card__value{margin-top:10px;font-size:28px;font-weight:700;letter-spacing:.02em}
.summary-card__hint{margin-top:8px;color:var(--muted);font-size:12px}
.layout{display:grid;grid-template-columns:320px minmax(0,1fr) 320px;gap:16px;margin-top:16px;min-height:calc(100vh - 240px)}
.sidebar,.workspace,.inspector{border:1px solid var(--line);border-radius:22px;background:rgba(10,17,29,.88);box-shadow:var(--shadow)}
.sidebar,.inspector{padding:14px}
.sidebar{display:grid;grid-template-rows:auto auto 1fr;gap:12px;min-height:720px}
.sidebar__title,.inspector__title{display:flex;align-items:center;justify-content:space-between;gap:10px}
.sidebar__title h2,.inspector__title h2{margin:0;font-size:16px}
.sidebar__sub,.inspector__sub{color:var(--muted);font-size:12px}
.sidebar__filters{display:grid;grid-template-columns:1fr 1fr;gap:10px}
.chat-list{display:grid;gap:10px;align-content:start;overflow:auto;padding-right:2px}
.chat-item{padding:12px;border:1px solid var(--line);border-radius:18px;background:var(--panel);cursor:pointer;transition:transform .15s ease,border-color .15s ease,background .15s ease}
.chat-item:hover{transform:translateY(-1px);border-color:rgba(83,183,255,.28)}
.chat-item.active{border-color:rgba(83,183,255,.48);background:linear-gradient(180deg,rgba(83,183,255,.12),rgba(255,255,255,.02))}
.chat-item__top,.chat-item__meta,.chat-item__stats{display:flex;align-items:center;justify-content:space-between;gap:8px}
.chat-item__name{font-weight:600}
.chat-item__meta,.chat-item__stats{margin-top:7px;color:var(--muted);font-size:12px}
.chat-item__spark{margin-top:9px}
.workspace{display:grid;grid-template-rows:auto auto 1fr;padding:16px}
.workspace-header{display:grid;gap:14px;padding-bottom:14px;border-bottom:1px solid var(--line)}
.workspace-header__main{display:flex;align-items:flex-start;justify-content:space-between;gap:16px}
.workspace-header h2{margin:0;font-size:28px;line-height:1.1}
.workspace-header__meta{display:flex;flex-wrap:wrap;gap:8px;margin-top:10px}
.workspace-header__kpis{display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:10px}
.kpi{padding:12px;border:1px solid var(--line);border-radius:16px;background:var(--panel)}
.kpi__label{font-size:12px;color:var(--muted)}
.kpi__value{display:block;margin-top:8px;font-size:22px;font-weight:700}
.tabbar{display:flex;gap:10px;overflow:auto;padding:14px 0 10px}
.tabbar button{padding:10px 14px;border-radius:12px;border:1px solid var(--line);background:var(--panel);color:var(--muted);cursor:pointer}
.tabbar button.active{color:var(--text);border-color:rgba(83,183,255,.48);background:rgba(83,183,255,.12)}
.workspace-body{overflow:auto;padding-top:6px}
.tab-panel{display:none;gap:14px}
.tab-panel.active{display:grid}
.panel-grid{display:grid;grid-template-columns:repeat(12,minmax(0,1fr));gap:14px}
.card{grid-column:span 12;padding:16px;border:1px solid var(--line);border-radius:18px;background:var(--panel)}
.card h3{margin:0 0 10px;font-size:16px}
.card h4{margin:0 0 8px;font-size:14px}
.card p{margin:0;color:var(--muted)}
.card--half{grid-column:span 6}
.card--third{grid-column:span 4}
.card--two-third{grid-column:span 8}
.metric-strip{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:10px}
.metric-chip{padding:12px;border-radius:14px;background:rgba(255,255,255,.03);border:1px solid rgba(255,255,255,.04)}
.metric-chip strong{display:block;font-size:20px}
.chip-row{display:flex;flex-wrap:wrap;gap:8px;margin-top:10px}
.chip{display:inline-flex;align-items:center;padding:5px 9px;border-radius:999px;border:1px solid var(--line);background:rgba(255,255,255,.03);font-size:12px;color:var(--muted)}
.chip--accent{color:#d8f1ff;border-color:rgba(83,183,255,.35);background:rgba(83,183,255,.12)}
.chip--danger{color:#ffd3dc;border-color:rgba(255,107,139,.35);background:rgba(255,107,139,.12)}
.chip--warn{color:#ffe8c5;border-color:rgba(255,180,84,.34);background:rgba(255,180,84,.12)}
.chip--ok{color:#ddffea;border-color:rgba(74,222,128,.3);background:rgba(74,222,128,.12)}
.chart{width:100%;min-height:160px}
.chart svg{width:100%;height:auto;display:block}
.table-controls{display:flex;flex-wrap:wrap;gap:10px;margin-bottom:12px}
.table-wrap{overflow:auto;border:1px solid var(--line);border-radius:16px}
table{width:100%;border-collapse:collapse;min-width:760px}
th,td{padding:11px 12px;border-bottom:1px solid rgba(255,255,255,.06);text-align:left;vertical-align:top}
th{position:sticky;top:0;background:#122035;color:var(--muted);font-size:12px;letter-spacing:.04em;text-transform:uppercase}
tbody tr{cursor:pointer}
tbody tr:hover{background:rgba(83,183,255,.06)}
.person{display:flex;align-items:center;gap:10px;min-width:180px}
.avatar,.avatar-fallback{width:34px;height:34px;border-radius:50%;flex:0 0 34px}
.avatar{object-fit:cover;background:#1f3556}
.avatar-fallback{display:grid;place-items:center;background:linear-gradient(135deg,#21466d,#15304f);font-size:12px;font-weight:700;color:#dff1ff}
.muted{color:var(--muted)}
.empty{padding:24px;border:1px dashed rgba(255,255,255,.14);border-radius:16px;color:var(--muted);text-align:center}
.related-list,.timeline,.link-list{display:grid;gap:10px}
.related-item,.timeline-item,.link-item{padding:12px;border-radius:14px;background:rgba(255,255,255,.03);border:1px solid rgba(255,255,255,.04)}
.network-wrap{display:grid;grid-template-columns:minmax(0,1fr) 320px;gap:12px}
.network-canvas{min-height:380px;border:1px solid var(--line);border-radius:18px;background:radial-gradient(circle at center,rgba(83,183,255,.07),rgba(255,255,255,.01))}
.network-side{display:grid;gap:10px;align-content:start}
.heatmap{display:grid;grid-template-columns:repeat(24,minmax(0,1fr));gap:6px}
.heat-cell{padding:10px 4px;border-radius:10px;text-align:center;font-size:11px;background:rgba(255,255,255,.03);border:1px solid rgba(255,255,255,.04)}
.event-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}
.inspector{display:grid;grid-template-rows:auto 1fr;gap:12px;min-height:720px}
.inspector__body{overflow:auto}
.inspector-card{padding:14px;border-radius:16px;background:var(--panel);border:1px solid var(--line)}
.inspector-card + .inspector-card{margin-top:12px}
.inspector-list{display:grid;gap:8px;margin-top:10px}
.inspector-list div{display:flex;justify-content:space-between;gap:10px}
.brief .summary-grid{grid-template-columns:repeat(4,minmax(0,1fr))}
.brief .layout{grid-template-columns:280px minmax(0,1fr)}
.brief .inspector{display:none}
@media (max-width:1440px){
  .summary-grid{grid-template-columns:repeat(4,minmax(0,1fr))}
  .layout{grid-template-columns:280px minmax(0,1fr)}
  .inspector{display:none}
}
@media (max-width:1080px){
  .topbar__filters{grid-template-columns:repeat(2,minmax(0,1fr))}
  .layout{grid-template-columns:1fr}
  .sidebar{min-height:auto}
  .workspace-header__kpis{grid-template-columns:repeat(3,minmax(0,1fr))}
  .card--half,.card--third,.card--two-third{grid-column:span 12}
  .network-wrap{grid-template-columns:1fr}
}
@media (max-width:720px){
  .app{padding:10px}
  .topbar{padding:14px}
  .topbar__head{flex-direction:column}
  .topbar__filters,.summary-grid,.workspace-header__kpis,.event-grid{grid-template-columns:1fr}
}
"""


def build_community_scan_stylesheet() -> str:
    return _STYLESHEET.strip() + "\n"


_HTML_TEMPLATE = """
<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>__TITLE__</title>
  <link rel="stylesheet" href="__CSS_HREF__" />
</head>
<body>
  <div class="app">
    <header class="topbar">
      <div class="topbar__head">
        <div class="topbar__title">
          <h1>__TITLE__</h1>
          <p>Аналитический workspace: overview first, zoom and filter, затем детали по выбранному чату.</p>
        </div>
        <div class="pill-row">__ROOT_META__</div>
      </div>
      <div class="topbar__filters">
        <div class="control">
          <label for="globalSearch">Поиск по чатам, людям и ссылкам</label>
          <input id="globalSearch" type="search" placeholder="strbypass, username, ссылка, участник" />
        </div>
        <div class="control">
          <label for="typeFilter">Тип</label>
          <select id="typeFilter">
            <option value="all">Все типы</option>
            <option value="channel">Каналы</option>
            <option value="supergroup">Супергруппы</option>
            <option value="group">Группы</option>
            <option value="bot">Боты</option>
          </select>
        </div>
        <div class="control">
          <label for="riskFilter">Риск</label>
          <select id="riskFilter">
            <option value="all">Все уровни</option>
            <option value="critical">Критический</option>
            <option value="high">Высокий</option>
            <option value="medium">Средний</option>
            <option value="low">Низкий</option>
          </select>
        </div>
        <div class="control">
          <label for="accessFilter">Доступ</label>
          <select id="accessFilter">
            <option value="all">Все</option>
            <option value="open">Доступные</option>
            <option value="closed">Закрытые</option>
          </select>
        </div>
        <div class="control">
          <label for="sortFilter">Сортировка</label>
          <select id="sortFilter">
            <option value="risk">По риску</option>
            <option value="messages">По сообщениям</option>
            <option value="members">По участникам</option>
            <option value="fresh">По свежести</option>
            <option value="new">По новым сообщениям</option>
          </select>
        </div>
        <div class="control">
          <label for="viewMode">Режим</label>
          <select id="viewMode">
            <option value="analyst">Analyst mode</option>
            <option value="brief">Executive summary</option>
          </select>
        </div>
      </div>
      <div class="summary-grid">__SUMMARY_CARDS__</div>
    </header>

    <div class="layout">
      <aside class="sidebar">
        <div class="sidebar__title">
          <div>
            <h2>Чаты и каналы</h2>
            <div class="sidebar__sub">Навигация по сообществу и быстрые фильтры.</div>
          </div>
          <span class="pill">Экспорт: __GENERATED_AT__</span>
        </div>
        <div class="sidebar__filters">
          <div class="control">
            <label for="sidebarQuickFilter">Быстрый фильтр</label>
            <select id="sidebarQuickFilter">
              <option value="all">Все чаты</option>
              <option value="active">Только активные</option>
              <option value="risky">Только рискованные</option>
              <option value="botlike">С bot-сигналами</option>
              <option value="open">Только доступные</option>
            </select>
          </div>
          <div class="control">
            <label for="sidebarMetric">Мини-метрика</label>
            <select id="sidebarMetric">
              <option value="messages">Сообщения</option>
              <option value="members">Участники</option>
              <option value="risk">Риск</option>
            </select>
          </div>
        </div>
        <div id="chatList" class="chat-list"></div>
      </aside>

      <main class="workspace">
        <section id="chatHeader" class="workspace-header"></section>
        <nav class="tabbar">
          <button type="button" data-tab-btn="overview" class="active">Обзор</button>
          <button type="button" data-tab-btn="activity">Активность</button>
          <button type="button" data-tab-btn="participants">Участники</button>
          <button type="button" data-tab-btn="bots">Боты и риск</button>
          <button type="button" data-tab-btn="network">Связи</button>
          <button type="button" data-tab-btn="content">Контент</button>
          <button type="button" data-tab-btn="events">События</button>
        </nav>
        <section class="workspace-body">
          <div id="tab-overview" class="tab-panel active"></div>
          <div id="tab-activity" class="tab-panel"></div>
          <div id="tab-participants" class="tab-panel"></div>
          <div id="tab-bots" class="tab-panel"></div>
          <div id="tab-network" class="tab-panel"></div>
          <div id="tab-content" class="tab-panel"></div>
          <div id="tab-events" class="tab-panel"></div>
        </section>
      </main>

      <aside class="inspector">
        <div class="inspector__title">
          <div>
            <h2>Инспектор</h2>
            <div class="inspector__sub">Детали выбранного чата или участника.</div>
          </div>
        </div>
        <div id="inspectorBody" class="inspector__body"></div>
      </aside>
    </div>
  </div>
  <script>
__SCRIPT__
  </script>
</body>
</html>
"""


_SCRIPT_PART_1 = """
const chats = Array.isArray(REPORT_DATA.chats) ? REPORT_DATA.chats : [];
const users = Array.isArray(REPORT_DATA.users) ? REPORT_DATA.users : [];
const edges = Array.isArray(REPORT_DATA.edges) ? REPORT_DATA.edges : [];
const aggregateUsers = new Map(users.map((item) => [String(item && item.sender_id || ""), item]));
const state = {
  query: "",
  type: "all",
  risk: "all",
  access: "all",
  sort: "risk",
  quick: "all",
  metric: "messages",
  view: "analyst",
  tab: "overview",
  chatId: "",
  userId: "",
  memberSearch: "",
  memberSort: "messages",
  botSearch: "",
  botSort: "risk",
};

const el = {
  chatList: document.getElementById("chatList"),
  chatHeader: document.getElementById("chatHeader"),
  inspectorBody: document.getElementById("inspectorBody"),
  globalSearch: document.getElementById("globalSearch"),
  typeFilter: document.getElementById("typeFilter"),
  riskFilter: document.getElementById("riskFilter"),
  accessFilter: document.getElementById("accessFilter"),
  sortFilter: document.getElementById("sortFilter"),
  sidebarQuickFilter: document.getElementById("sidebarQuickFilter"),
  sidebarMetric: document.getElementById("sidebarMetric"),
  viewMode: document.getElementById("viewMode"),
  panels: {
    overview: document.getElementById("tab-overview"),
    activity: document.getElementById("tab-activity"),
    participants: document.getElementById("tab-participants"),
    bots: document.getElementById("tab-bots"),
    network: document.getElementById("tab-network"),
    content: document.getElementById("tab-content"),
    events: document.getElementById("tab-events"),
  },
};

const isObj = (value) => !!value && typeof value === "object" && !Array.isArray(value);
const escapeHtml = (value) => String(value ?? "").replace(/[&<>"']/g, (char) => ({ "&":"&amp;", "<":"&lt;", ">":"&gt;", '"':"&quot;", "'":"&#39;" }[char] || char));
const norm = (value) => String(value ?? "").toLowerCase().replace(/https?:\\/\\/t\\.me\\//g, "").replace(/t\\.me\\//g, "").replace(/[^a-zа-яё0-9]+/gi, "");
const fmtInt = (value) => {
  const num = Number(value || 0);
  if (!Number.isFinite(num)) return "0";
  return Math.round(num).toLocaleString("ru-RU");
};
const fmtPct = (value, digits = 1) => {
  const num = Number(value || 0);
  if (!Number.isFinite(num)) return "0%";
  return `${(num * 100).toFixed(digits)}%`;
};
const fmtTs = (value) => {
  const ts = Number(value || 0);
  if (!Number.isFinite(ts) || ts <= 0) return "n/a";
  try {
    return new Date(ts * 1000).toLocaleString("ru-RU");
  } catch (_) {
    return String(value ?? "");
  }
};
const getChatId = (chat) => String(chat && (chat.chat_id ?? chat.id) || "");
const chatStats = (chat) => isObj(chat && chat.stats) ? chat.stats : {};
const riskScore = (chat) => Number((chatStats(chat).risk || {}).score || 0);
const riskLevel = (score) => score >= 70 ? "critical" : score >= 50 ? "high" : score >= 25 ? "medium" : "low";
const riskLabel = (score) => ({ critical: "Критический", high: "Высокий", medium: "Средний", low: "Низкий" }[riskLevel(score)]);
const messagesCount = (chat) => Number(chatStats(chat).total_messages || chat && chat.new_messages || 0);
const membersCount = (chat) => Number(chat && chat.members_count || chatStats(chat).total_senders || 0);
const newMessages = (chat) => Number(chat && chat.new_messages || 0);
const signalCount = (chat) => Number((chatStats(chat).suspicious_senders || []).length || 0);
const chatName = (chat) => String(chat && (chat.title || getChatId(chat)) || "");
const initials = (value) => String(value || "?").split(/\\s+/).filter(Boolean).slice(0, 2).map((part) => part[0]).join("").slice(0, 2).toUpperCase() || "?";
const chip = (text, kind = "") => `<span class="chip ${kind ? `chip--${kind}` : ""}">${escapeHtml(text)}</span>`;
const avatarNode = (src, label) => src ? `<img class="avatar" loading="lazy" src="${escapeHtml(src)}" alt="${escapeHtml(label)}" />` : `<div class="avatar-fallback">${escapeHtml(initials(label))}</div>`;

const trendPoints = (rows, labelKey, valueKey, width = 320, height = 120, color = "#53b7ff") => {
  const list = Array.isArray(rows) ? rows : [];
  if (!list.length) return '<div class="empty">Недостаточно данных для графика.</div>';
  const values = list.map((item) => Math.max(0, Number(item && item[valueKey] || 0)));
  const max = Math.max(1, ...values);
  const step = list.length > 1 ? width / (list.length - 1) : width;
  const points = values.map((value, index) => `${(index * step).toFixed(2)},${(height - (value / max) * (height - 8) - 4).toFixed(2)}`).join(" ");
  const area = `0,${height} ${points} ${width},${height}`;
  const labelFirst = escapeHtml(String(list[0] && list[0][labelKey] || ""));
  const labelLast = escapeHtml(String(list[list.length - 1] && list[list.length - 1][labelKey] || ""));
  return `<div class="chart"><svg viewBox="0 0 ${width} ${height + 24}" aria-label="chart"><polyline points="${area}" fill="rgba(83,183,255,.10)" stroke="none"></polyline><polyline points="${points}" fill="none" stroke="${color}" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"></polyline><text x="0" y="${height + 18}" fill="#8ea7c8" font-size="11">${labelFirst}</text><text x="${width}" y="${height + 18}" text-anchor="end" fill="#8ea7c8" font-size="11">${labelLast}</text></svg></div>`;
};

const barChart = (rows, labelKey, valueKey, width = 540, height = 180, color = "#77e7b4") => {
  const list = Array.isArray(rows) ? rows.slice(0, 12) : [];
  if (!list.length) return '<div class="empty">Нет значимых данных.</div>';
  const values = list.map((item) => Math.max(0, Number(item && item[valueKey] || 0)));
  const max = Math.max(1, ...values);
  const slot = width / Math.max(1, list.length);
  const barWidth = Math.max(18, slot - 10);
  const bars = list.map((item, index) => {
    const value = Math.max(0, Number(item && item[valueKey] || 0));
    const label = escapeHtml(String(item && item[labelKey] || ""));
    const h = (value / max) * (height - 28);
    const x = index * slot + (slot - barWidth) / 2;
    const y = height - h - 20;
    return `<g><rect x="${x.toFixed(2)}" y="${y.toFixed(2)}" width="${barWidth.toFixed(2)}" height="${h.toFixed(2)}" rx="6" fill="${color}"></rect><text x="${(x + barWidth / 2).toFixed(2)}" y="${height - 6}" text-anchor="middle" fill="#8ea7c8" font-size="10">${label.slice(0, 12)}</text><title>${label}: ${fmtInt(value)}</title></g>`;
  }).join("");
  return `<div class="chart"><svg viewBox="0 0 ${width} ${height}" aria-label="bars">${bars}</svg></div>`;
};

const heatmap24 = (rows) => {
  const values = Array.from({ length: 24 }, (_, hour) => {
    const match = (Array.isArray(rows) ? rows : []).find((item) => Number(item && item.hour || 0) === hour);
    return { hour, count: Number(match && match.count || 0) };
  });
  const max = Math.max(1, ...values.map((item) => item.count));
  return `<div class="heatmap">${values.map((item) => {
    const alpha = 0.08 + (item.count / max) * 0.72;
    return `<div class="heat-cell" style="background:rgba(83,183,255,${alpha.toFixed(2)})"><div>${String(item.hour).padStart(2, "0")}:00</div><strong>${fmtInt(item.count)}</strong></div>`;
  }).join("")}</div>`;
};

const findChat = (chatId) => chats.find((item) => getChatId(item) === String(chatId)) || null;
const relatedChats = (chatId) => {
  const linked = [];
  const seen = new Set();
  edges.forEach((edge) => {
    const source = String(edge && edge.source || "");
    const target = String(edge && edge.target || "");
    if (!source || !target) return;
    const other = source === String(chatId) ? target : target === String(chatId) ? source : "";
    if (!other || seen.has(other)) return;
    const row = findChat(other);
    if (!row) return;
    linked.push({ row, edge });
    seen.add(other);
  });
  return linked.sort((a, b) => riskScore(b.row) - riskScore(a.row) || messagesCount(b.row) - messagesCount(a.row));
};

const filteredChats = () => {
  const q = norm(state.query);
  const list = chats.filter((chat) => {
    const stats = chatStats(chat);
    if (state.type !== "all" && String(chat.type || "").toLowerCase() !== state.type) return false;
    if (state.access === "open" && !chat.accessible) return false;
    if (state.access === "closed" && !!chat.accessible) return false;
    if (state.quick === "active" && messagesCount(chat) <= 0) return false;
    if (state.quick === "risky" && riskScore(chat) < 25) return false;
    if (state.quick === "botlike" && signalCount(chat) <= 0) return false;
    if (state.quick === "open" && !chat.accessible) return false;
    if (state.risk !== "all" && riskLevel(riskScore(chat)) !== state.risk) return false;
    if (!q) return true;
    const searchText = [
      chat.title,
      chat.username,
      chat.about,
      (stats.sender_details || []).slice(0, 32).map((item) => `${item.name || ""} ${item.username || ""}`).join(" "),
      (chat.top_links || []).slice(0, 12).map((item) => item.url || "").join(" "),
    ].join(" ");
    return norm(searchText).includes(q);
  });
  const sorters = {
    risk: (a, b) => riskScore(b) - riskScore(a) || messagesCount(b) - messagesCount(a),
    messages: (a, b) => messagesCount(b) - messagesCount(a) || riskScore(b) - riskScore(a),
    members: (a, b) => membersCount(b) - membersCount(a) || messagesCount(b) - messagesCount(a),
    fresh: (a, b) => Number(chatStats(b).last_message_date || 0) - Number(chatStats(a).last_message_date || 0),
    new: (a, b) => newMessages(b) - newMessages(a) || messagesCount(b) - messagesCount(a),
  };
  list.sort(sorters[state.sort] || sorters.risk);
  return list;
};

const ensureSelectedChat = () => {
  const list = filteredChats();
  if (!list.length) {
    state.chatId = "";
    return null;
  }
  if (!state.chatId || !list.some((item) => getChatId(item) === state.chatId)) {
    const preferred = list.find((item) => item.is_root) || list[0];
    state.chatId = getChatId(preferred);
  }
  return findChat(state.chatId);
};

const renderSidebar = () => {
  const rows = filteredChats();
  if (!rows.length) {
    el.chatList.innerHTML = '<div class="empty">Фильтр скрыл все чаты. Измените запрос или снимите ограничения.</div>';
    return;
  }
  el.chatList.innerHTML = rows.map((chat) => {
    const stats = chatStats(chat);
    const score = riskScore(chat);
    const metricValue = state.metric === "members" ? membersCount(chat) : state.metric === "risk" ? score : messagesCount(chat);
    const metricLabel = state.metric === "members" ? "участников" : state.metric === "risk" ? "риск" : "сообщений";
    return `
      <div class="chat-item ${state.chatId === getChatId(chat) ? "active" : ""}" data-chat-id="${escapeHtml(getChatId(chat))}">
        <div class="chat-item__top">
          <div class="chat-item__name">${escapeHtml(chatName(chat))}</div>
          ${chip(riskLabel(score), score >= 50 ? "danger" : score >= 25 ? "warn" : "ok")}
        </div>
        <div class="chat-item__meta">
          <span>${escapeHtml(String(chat.type || "chat"))}${chat.username ? ` · @${escapeHtml(chat.username)}` : ""}</span>
          <span>${chat.accessible ? "доступ" : "закрыт"}</span>
        </div>
        <div class="chat-item__stats">
          <span>${fmtInt(metricValue)} ${metricLabel}</span>
          <span>${fmtInt(newMessages(chat))} новых</span>
        </div>
        <div class="chat-item__spark">${trendPoints(stats.daily_activity || [], "day", "count", 250, 44, "#53b7ff")}</div>
      </div>
    `;
  }).join("");
};

const renderHeader = (chat) => {
  if (!chat) {
    el.chatHeader.innerHTML = '<div class="empty">Нет выбранного чата.</div>';
    return;
  }
  const stats = chatStats(chat);
  const risk = stats.risk || {};
  const factors = Array.isArray(risk.factors) ? risk.factors.slice(0, 6) : [];
  el.chatHeader.innerHTML = `
    <div class="workspace-header__main">
      <div>
        <h2>${escapeHtml(chatName(chat))}</h2>
        <div class="workspace-header__meta">
          ${chip(chat.type || "chat")}
          ${chat.username ? chip(`@${chat.username}`) : ""}
          ${chip(chat.accessible ? "Доступен" : "Закрыт", chat.accessible ? "ok" : "warn")}
          ${chip(`Риск ${fmtInt(risk.score)}`, Number(risk.score || 0) >= 50 ? "danger" : Number(risk.score || 0) >= 25 ? "warn" : "ok")}
          ${chat.members_count ? chip(`${fmtInt(chat.members_count)} участников`) : ""}
          ${chat.is_root ? chip("Корневой узел", "accent") : ""}
        </div>
        <div class="chip-row">${factors.map((item) => chip(item.label || item.key || "signal", item.severity === "high" || item.severity === "critical" ? "danger" : item.severity === "medium" ? "warn" : "ok")).join("")}</div>
      </div>
      <div class="muted">Последнее обновление: ${fmtTs(chat.last_scanned_at || stats.last_message_date || REPORT_DATA.completed_at)}</div>
    </div>
    <div class="workspace-header__kpis">
      <div class="kpi"><span class="kpi__label">Сообщения</span><strong class="kpi__value">${fmtInt(stats.total_messages)}</strong></div>
      <div class="kpi"><span class="kpi__label">Участники</span><strong class="kpi__value">${fmtInt(stats.total_senders || chat.members_count || 0)}</strong></div>
      <div class="kpi"><span class="kpi__label">Новые на проходе</span><strong class="kpi__value">${fmtInt(chat.new_messages)}</strong></div>
      <div class="kpi"><span class="kpi__label">Просмотры</span><strong class="kpi__value">${fmtInt(stats.total_views)}</strong></div>
      <div class="kpi"><span class="kpi__label">Пересылки</span><strong class="kpi__value">${fmtInt(stats.total_forwards)}</strong></div>
      <div class="kpi"><span class="kpi__label">Подозрительных</span><strong class="kpi__value">${fmtInt((stats.suspicious_senders || []).length)}</strong></div>
    </div>
  `;
};
"""


_SCRIPT_PART_2 = """
const renderOverview = (chat) => {
  const stats = chatStats(chat);
  const related = relatedChats(getChatId(chat)).slice(0, 6);
  const suspicious = Array.isArray(stats.suspicious_senders) ? stats.suspicious_senders.slice(0, 5) : [];
  const factors = Array.isArray((stats.risk || {}).factors) ? stats.risk.factors : [];
  const anomalies = Array.isArray(stats.anomaly_flags) ? stats.anomaly_flags : [];
  el.panels.overview.innerHTML = `
    <div class="panel-grid">
      <section class="card">
        <h3>Что важно сейчас</h3>
        <div class="metric-strip">
          <div class="metric-chip"><span class="muted">Пик активности</span><strong>${escapeHtml(String(((stats.activity_summary || {}).peak_day || {}).day || "n/a"))}</strong><span class="muted">${fmtInt((((stats.activity_summary || {}).peak_day || {}).count || 0))} сообщений</span></div>
          <div class="metric-chip"><span class="muted">Пиковый час</span><strong>${String((((stats.activity_summary || {}).peak_hour || {}).hour || 0)).padStart(2, "0")}:00</strong><span class="muted">${fmtInt((((stats.activity_summary || {}).peak_hour || {}).count || 0))} сообщений</span></div>
          <div class="metric-chip"><span class="muted">Off-hours share</span><strong>${fmtPct(((stats.activity_summary || {}).off_hours_share || 0))}</strong><span class="muted">Ночные публикации</span></div>
          <div class="metric-chip"><span class="muted">Подозрительные</span><strong>${fmtInt(suspicious.length)}</strong><span class="muted">Локально в этом чате</span></div>
        </div>
      </section>
      <section class="card card--two-third">
        <h3>Главный тренд</h3>
        ${trendPoints(stats.daily_activity || [], "day", "count", 760, 220, "#53b7ff")}
      </section>
      <section class="card card--third">
        <h3>Распределение отправителей</h3>
        ${barChart((stats.sender_details || []).slice(0, 8).map((item) => ({ label: item.username || item.name || item.sender_id, value: item.count })), "label", "value", 320, 220, "#77e7b4")}
      </section>
      <section class="card card--half">
        <h3>Аномалии и сигналы</h3>
        <div class="chip-row">${anomalies.map((item) => chip(item, "warn")).join("") || '<span class="muted">Сильные флаги не выявлены.</span>'}</div>
        <div class="related-list" style="margin-top:12px;">${factors.slice(0, 8).map((item) => `<div class="related-item"><strong>${escapeHtml(item.label || item.key || "signal")}</strong><div class="muted">${escapeHtml(item.detail || "")}</div></div>`).join("") || '<div class="empty">Подробные риск-факторы отсутствуют.</div>'}</div>
      </section>
      <section class="card card--half">
        <h3>Связанные чаты</h3>
        <div class="related-list">${related.map((item) => `<div class="related-item" data-chat-id="${escapeHtml(getChatId(item.row))}"><strong>${escapeHtml(chatName(item.row))}</strong><div class="muted">${escapeHtml(String(item.edge && item.edge.label || "link"))} · риск ${fmtInt(riskScore(item.row))} · ${fmtInt(messagesCount(item.row))} сообщений</div></div>`).join("") || '<div class="empty">Связей не найдено.</div>'}</div>
      </section>
      <section class="card">
        <h3>Топ-5 участников</h3>
        <div class="table-wrap">
          <table>
            <thead><tr><th>Пользователь</th><th>Сообщения</th><th>Доля</th><th>Активные дни</th><th>Последнее сообщение</th><th>Риск</th></tr></thead>
            <tbody>${(stats.sender_details || []).slice(0, 5).map((item) => `<tr data-user-id="${escapeHtml(String(item.sender_id || ""))}"><td><div class="person">${avatarNode(item.avatar || "", item.name || item.username || item.sender_id)}<div><strong>${escapeHtml(item.name || item.sender_id)}</strong><div class="muted">${item.username ? `@${escapeHtml(item.username)}` : escapeHtml(String(item.type || ""))}</div></div></div></td><td>${fmtInt(item.count)}</td><td>${fmtPct(item.share || 0)}</td><td>${fmtInt(item.active_days)}</td><td>${fmtTs(item.last_date)}</td><td>${chip(`${fmtInt(item.risk_score)} / ${riskLabel(Number(item.risk_score || 0))}`, Number(item.risk_score || 0) >= 50 ? "danger" : Number(item.risk_score || 0) >= 25 ? "warn" : "ok")}</td></tr>`).join("") || '<tr><td colspan="6" class="muted">Нет данных.</td></tr>'}</tbody>
          </table>
        </div>
      </section>
    </div>
  `;
};

const renderActivity = (chat) => {
  const stats = chatStats(chat);
  const spikes = [...(stats.daily_activity || [])].sort((a, b) => Number(b.count || 0) - Number(a.count || 0)).slice(0, 8);
  const postDaily = ((stats.post_performance || {}).daily || []).map((item) => ({ label: item.day, value: item.views || item.posts || 0 }));
  el.panels.activity.innerHTML = `
    <div class="panel-grid">
      <section class="card card--two-third">
        <h3>Динамика сообщений по дням</h3>
        ${trendPoints(stats.daily_activity || [], "day", "count", 760, 220, "#53b7ff")}
      </section>
      <section class="card card--third">
        <h3>Распределение по часам</h3>
        ${heatmap24(stats.hourly_activity || [])}
      </section>
      <section class="card card--half">
        <h3>Просмотры и вовлечение постов</h3>
        ${barChart(postDaily, "label", "value", 520, 220, "#ffb454")}
      </section>
      <section class="card card--half">
        <h3>Всплески активности</h3>
        <div class="timeline">${spikes.map((item) => `<div class="timeline-item"><strong>${escapeHtml(String(item.day || ""))}</strong><div class="muted">${fmtInt(item.count)} сообщений</div></div>`).join("") || '<div class="empty">Всплески не выделены.</div>'}</div>
      </section>
    </div>
  `;
};

const sortMembers = (rows, mode) => {
  const sorters = {
    messages: (a, b) => Number(b.count || 0) - Number(a.count || 0),
    activity: (a, b) => Number(b.active_days || 0) - Number(a.active_days || 0),
    last: (a, b) => Number(b.last_date || 0) - Number(a.last_date || 0),
    reactions: (a, b) => Number(b.total_reactions || 0) - Number(a.total_reactions || 0),
    risk: (a, b) => Number(b.risk_score || 0) - Number(a.risk_score || 0),
  };
  return [...rows].sort(sorters[mode] || sorters.messages);
};

const renderParticipants = (chat) => {
  const stats = chatStats(chat);
  const source = Array.isArray(stats.sender_details) ? stats.sender_details : [];
  const q = norm(state.memberSearch);
  const filtered = sortMembers(source.filter((item) => !q || norm(`${item.name || ""} ${item.username || ""} ${item.type || ""}`).includes(q)), state.memberSort);
  el.panels.participants.innerHTML = `
    <div class="panel-grid">
      <section class="card">
        <h3>Участники чата</h3>
        <div class="table-controls">
          <div class="control" style="min-width:240px"><label for="memberSearch">Поиск по людям</label><input id="memberSearch" type="search" value="${escapeHtml(state.memberSearch)}" placeholder="Имя, username, тип" /></div>
          <div class="control" style="min-width:220px"><label for="memberSort">Сортировка</label><select id="memberSort"><option value="messages"${state.memberSort === "messages" ? " selected" : ""}>По сообщениям</option><option value="activity"${state.memberSort === "activity" ? " selected" : ""}>По активности</option><option value="last"${state.memberSort === "last" ? " selected" : ""}>По последней дате</option><option value="reactions"${state.memberSort === "reactions" ? " selected" : ""}>По реакциям</option><option value="risk"${state.memberSort === "risk" ? " selected" : ""}>По риску</option></select></div>
        </div>
        <div class="table-wrap">
          <table>
            <thead><tr><th>Пользователь</th><th>Сообщения</th><th>Доля</th><th>Активные дни</th><th>Реакции</th><th>Пересылки</th><th>Последнее сообщение</th><th>Риск</th></tr></thead>
            <tbody>${filtered.map((item) => `<tr data-user-id="${escapeHtml(String(item.sender_id || ""))}"><td><div class="person">${avatarNode(item.avatar || "", item.name || item.username || item.sender_id)}<div><strong>${escapeHtml(item.name || item.sender_id)}</strong><div class="muted">${item.username ? `@${escapeHtml(item.username)}` : ""}${item.type ? ` · ${escapeHtml(item.type)}` : ""}</div></div></div></td><td>${fmtInt(item.count)}</td><td>${fmtPct(item.share || 0)}</td><td>${fmtInt(item.active_days)}</td><td>${fmtInt(item.total_reactions)}</td><td>${fmtInt(item.total_forwards)}</td><td>${fmtTs(item.last_date)}</td><td>${chip(`${fmtInt(item.risk_score)}`, Number(item.risk_score || 0) >= 50 ? "danger" : Number(item.risk_score || 0) >= 25 ? "warn" : "ok")}</td></tr>`).join("") || '<tr><td colspan="8" class="muted">Нет данных по отправителям.</td></tr>'}</tbody>
          </table>
        </div>
      </section>
    </div>
  `;
};

const renderBots = (chat) => {
  const stats = chatStats(chat);
  const source = Array.isArray(stats.sender_details) ? stats.sender_details : [];
  const suspects = sortMembers(source.filter((item) => Number(item.risk_score || 0) >= 18 || (Array.isArray(item.risk_flags) && item.risk_flags.length)), state.botSort);
  const q = norm(state.botSearch);
  const filtered = suspects.filter((item) => !q || norm(`${item.name || ""} ${item.username || ""} ${(item.risk_flags || []).join(" ")}`).includes(q));
  const reasonCounter = {};
  filtered.forEach((item) => (item.risk_flags || []).forEach((flag) => { reasonCounter[flag] = (reasonCounter[flag] || 0) + 1; }));
  const reasonRows = Object.entries(reasonCounter).sort((a, b) => Number(b[1]) - Number(a[1]));
  el.panels.bots.innerHTML = `
    <div class="panel-grid">
      <section class="card card--half">
        <h3>Сводка по bot-like сигналам</h3>
        <div class="metric-strip">
          <div class="metric-chip"><span class="muted">Bot-like senders</span><strong>${fmtInt(((stats.bot_summary || {}).bot_like_senders || 0))}</strong></div>
          <div class="metric-chip"><span class="muted">Bot-like message share</span><strong>${fmtPct(((stats.bot_summary || {}).bot_like_message_share || 0))}</strong></div>
          <div class="metric-chip"><span class="muted">Numeric usernames</span><strong>${fmtInt(((stats.bot_summary || {}).numeric_username_senders || 0))}</strong></div>
          <div class="metric-chip"><span class="muted">Special-symbol senders</span><strong>${fmtInt(((stats.bot_summary || {}).special_symbol_senders || 0))}</strong></div>
        </div>
      </section>
      <section class="card card--half">
        <h3>Причины срабатывания</h3>
        <div class="chip-row">${reasonRows.map(([reason, count]) => chip(`${reason}: ${fmtInt(count)}`, count >= 3 ? "danger" : "warn")).join("") || '<span class="muted">Нет срабатываний.</span>'}</div>
      </section>
      <section class="card">
        <h3>Подозрительные пользователи</h3>
        <div class="table-controls">
          <div class="control" style="min-width:240px"><label for="botSearch">Поиск</label><input id="botSearch" type="search" value="${escapeHtml(state.botSearch)}" placeholder="Имя, username, причина" /></div>
          <div class="control" style="min-width:220px"><label for="botSort">Сортировка</label><select id="botSort"><option value="risk"${state.botSort === "risk" ? " selected" : ""}>По риску</option><option value="messages"${state.botSort === "messages" ? " selected" : ""}>По сообщениям</option><option value="activity"${state.botSort === "activity" ? " selected" : ""}>По активности</option><option value="reactions"${state.botSort === "reactions" ? " selected" : ""}>По реакциям</option><option value="last"${state.botSort === "last" ? " selected" : ""}>По последней дате</option></select></div>
        </div>
        <div class="table-wrap">
          <table>
            <thead><tr><th>Пользователь</th><th>Риск</th><th>Сообщения</th><th>Активные дни</th><th>Off-hours</th><th>Повторы</th><th>Причины</th></tr></thead>
            <tbody>${filtered.map((item) => `<tr data-user-id="${escapeHtml(String(item.sender_id || ""))}"><td><div class="person">${avatarNode(item.avatar || "", item.name || item.username || item.sender_id)}<div><strong>${escapeHtml(item.name || item.sender_id)}</strong><div class="muted">${item.username ? `@${escapeHtml(item.username)}` : ""}</div></div></div></td><td>${chip(`${fmtInt(item.risk_score)} / ${riskLabel(Number(item.risk_score || 0))}`, Number(item.risk_score || 0) >= 50 ? "danger" : "warn")}</td><td>${fmtInt(item.count)}</td><td>${fmtInt(item.active_days)}</td><td>${fmtPct(item.off_hours_share || 0)}</td><td>${fmtPct(item.repetitive_share || 0)}</td><td>${(item.risk_flags || []).map((flag) => chip(flag, "warn")).join("")}</td></tr>`).join("") || '<tr><td colspan="7" class="muted">Подозрительные пользователи не найдены.</td></tr>'}</tbody>
          </table>
        </div>
      </section>
    </div>
  `;
};
"""


_SCRIPT_PART_3 = """
const renderNetwork = (chat) => {
  const centerId = getChatId(chat);
  const linked = relatedChats(centerId).slice(0, 10);
  const width = 720;
  const height = 380;
  const cx = width / 2;
  const cy = height / 2;
  const radius = 120;
  const nodes = linked.map((item, index) => {
    const angle = (Math.PI * 2 * index) / Math.max(1, linked.length);
    return {
      id: getChatId(item.row),
      title: chatName(item.row),
      x: cx + Math.cos(angle) * (radius + (index % 2) * 36),
      y: cy + Math.sin(angle) * (radius + (index % 2) * 36),
      risk: riskScore(item.row),
      label: item.edge && item.edge.label || "link",
    };
  });
  const svg = `
    <svg viewBox="0 0 ${width} ${height}" class="network-svg" aria-label="Граф связей">
      ${nodes.map((node) => `<line x1="${cx}" y1="${cy}" x2="${node.x.toFixed(2)}" y2="${node.y.toFixed(2)}" stroke="rgba(83,183,255,.28)" stroke-width="1.5"></line>`).join("")}
      <circle cx="${cx}" cy="${cy}" r="36" fill="#53b7ff"></circle>
      <text x="${cx}" y="${cy + 5}" text-anchor="middle" fill="#06101b" font-size="12" font-weight="700">${escapeHtml(chatName(chat).slice(0, 12))}</text>
      ${nodes.map((node) => `<g><circle cx="${node.x.toFixed(2)}" cy="${node.y.toFixed(2)}" r="24" fill="${node.risk >= 50 ? "#ff6b8b" : node.risk >= 25 ? "#ffb454" : "#77e7b4"}"></circle><text x="${node.x.toFixed(2)}" y="${(node.y + 4).toFixed(2)}" text-anchor="middle" fill="#06101b" font-size="10" font-weight="700">${escapeHtml(node.title.slice(0, 10))}</text><title>${escapeHtml(node.title)} · ${escapeHtml(node.label)}</title></g>`).join("")}
    </svg>
  `;
  el.panels.network.innerHTML = `
    <div class="panel-grid">
      <section class="card">
        <h3>Граф связей сообщества</h3>
        <div class="network-wrap">
          <div class="network-canvas">${svg}</div>
          <div class="network-side">
            ${(linked).map((item) => `<div class="related-item" data-chat-id="${escapeHtml(getChatId(item.row))}"><strong>${escapeHtml(chatName(item.row))}</strong><div class="muted">${escapeHtml(String(item.edge && item.edge.label || "link"))} · ${fmtInt(messagesCount(item.row))} сообщений · риск ${fmtInt(riskScore(item.row))}</div></div>`).join("") || '<div class="empty">Связей не найдено.</div>'}
          </div>
        </div>
      </section>
    </div>
  `;
};

const renderContent = (chat) => {
  const stats = chatStats(chat);
  const posts = Array.isArray((stats.post_performance || {}).top_posts) ? stats.post_performance.top_posts : [];
  const anomalies = Array.isArray((stats.post_performance || {}).anomalies) ? stats.post_performance.anomalies : [];
  const links = Array.isArray(chat.top_links) ? chat.top_links : [];
  el.panels.content.innerHTML = `
    <div class="panel-grid">
      <section class="card card--half">
        <h3>Контент и ссылки</h3>
        <div class="link-list">${links.map((item) => `<div class="link-item"><strong>${escapeHtml(item.url || "")}</strong><div class="muted">${fmtInt(item.count)} упоминаний</div></div>`).join("") || '<div class="empty">Ссылки не обнаружены.</div>'}</div>
      </section>
      <section class="card card--half">
        <h3>Аномалии постов</h3>
        <div class="timeline">${anomalies.slice(0, 10).map((item) => `<div class="timeline-item"><strong>#${fmtInt(item.id)}</strong><div class="muted">${escapeHtml((item.flags || []).join(", "))}</div><div class="muted">views ${fmtInt(item.views)} · reactions ${fmtInt(item.reactions_total)} · forwards ${fmtInt(item.forwards)}</div></div>`).join("") || '<div class="empty">Аномалий по постам нет.</div>'}</div>
      </section>
      <section class="card">
        <h3>Топ-посты</h3>
        <div class="table-wrap">
          <table>
            <thead><tr><th>Пост</th><th>Дата</th><th>Просмотры</th><th>Реакции</th><th>Пересылки</th><th>ER</th><th>FR</th></tr></thead>
            <tbody>${posts.map((item) => `<tr><td><strong>#${fmtInt(item.id)}</strong><div class="muted">${escapeHtml(item.text_preview || "Без текста")}</div></td><td>${fmtTs(item.date)}</td><td>${fmtInt(item.views)}</td><td>${fmtInt(item.reactions_total)}</td><td>${fmtInt(item.forwards)}</td><td>${fmtPct(item.engagement_rate || 0)}</td><td>${fmtPct(item.forward_rate || 0)}</td></tr>`).join("") || '<tr><td colspan="7" class="muted">Нет данных по постам.</td></tr>'}</tbody>
          </table>
        </div>
      </section>
    </div>
  `;
};

const renderEvents = (chat) => {
  const stats = chatStats(chat);
  const events = [];
  const peakDay = ((stats.activity_summary || {}).peak_day || {});
  if (peakDay.day) events.push({ title: "Пик активности", detail: `${peakDay.day} · ${fmtInt(peakDay.count)} сообщений` });
  if (chat.last_scanned_at) events.push({ title: "Последний проход", detail: fmtTs(chat.last_scanned_at) });
  if (chat.new_messages) events.push({ title: "Новые сообщения", detail: `${fmtInt(chat.new_messages)} новых сообщений на текущем проходе` });
  (stats.anomaly_flags || []).forEach((flag) => events.push({ title: "Аномалия", detail: flag }));
  (REPORT_DATA.notes || []).slice(0, 4).forEach((note) => events.push({ title: "Примечание", detail: note }));
  ((stats.post_performance || {}).anomalies || []).slice(0, 6).forEach((item) => events.push({ title: `Пост #${fmtInt(item.id)}`, detail: `${(item.flags || []).join(", ")} · views ${fmtInt(item.views)}` }));
  el.panels.events.innerHTML = `
    <div class="panel-grid">
      <section class="card">
        <h3>События и временная шкала</h3>
        <div class="timeline">${events.map((item) => `<div class="timeline-item"><strong>${escapeHtml(item.title || "")}</strong><div class="muted">${escapeHtml(item.detail || "")}</div></div>`).join("") || '<div class="empty">Событий недостаточно для таймлайна.</div>'}</div>
      </section>
    </div>
  `;
};

const currentUser = () => {
  if (!state.userId) return null;
  const chat = findChat(state.chatId);
  const stats = chat ? chatStats(chat) : {};
  const local = Array.isArray(stats.sender_details) ? stats.sender_details.find((item) => String(item.sender_id || "") === state.userId) : null;
  const aggregate = aggregateUsers.get(state.userId) || null;
  return Object.assign({}, aggregate || {}, local || {});
};

const renderInspector = () => {
  const chat = findChat(state.chatId);
  const user = currentUser();
  if (!chat) {
    el.inspectorBody.innerHTML = '<div class="empty">Нет данных.</div>';
    return;
  }
  if (user) {
    el.inspectorBody.innerHTML = `
      <div class="inspector-card">
        <div class="person">${avatarNode(user.avatar || "", user.name || user.username || user.sender_id)}<div><strong>${escapeHtml(user.name || user.sender_id || "")}</strong><div class="muted">${user.username ? `@${escapeHtml(user.username)}` : ""}${user.type ? ` · ${escapeHtml(user.type)}` : ""}</div></div></div>
        <div class="inspector-list">
          <div><span>Risk score</span><strong>${fmtInt(user.risk_score)}</strong></div>
          <div><span>Сообщений</span><strong>${fmtInt(user.count)}</strong></div>
          <div><span>Активные дни</span><strong>${fmtInt(user.active_days)}</strong></div>
          <div><span>Последняя активность</span><strong>${fmtTs(user.last_date)}</strong></div>
          <div><span>Чатов в сообществе</span><strong>${fmtInt(user.chat_count)}</strong></div>
          <div><span>Реакции</span><strong>${fmtInt(user.total_reactions)}</strong></div>
        </div>
        <div class="chip-row">${(user.risk_flags || []).map((flag) => chip(flag, "warn")).join("") || '<span class="muted">Явные флаги отсутствуют.</span>'}</div>
      </div>
      <div class="inspector-card">
        <h3>Контекст</h3>
        <div class="inspector-list">
          <div><span>Доля сообщений</span><strong>${fmtPct(user.share || 0)}</strong></div>
          <div><span>Off-hours</span><strong>${fmtPct(user.off_hours_share || 0)}</strong></div>
          <div><span>Повторы</span><strong>${fmtPct(user.repetitive_share || 0)}</strong></div>
          <div><span>Special symbols</span><strong>${fmtPct(user.special_symbol_share || 0)}</strong></div>
        </div>
        <div class="muted" style="margin-top:10px">Чаты: ${escapeHtml((user.chat_titles || []).join(", ") || chatName(chat))}</div>
      </div>
    `;
    return;
  }
  const stats = chatStats(chat);
  el.inspectorBody.innerHTML = `
    <div class="inspector-card">
      <strong>${escapeHtml(chatName(chat))}</strong>
      <div class="muted" style="margin-top:6px">${chat.username ? `@${escapeHtml(chat.username)}` : "Без username"} · ${escapeHtml(String(chat.type || "chat"))}</div>
      <div class="inspector-list">
        <div><span>Риск</span><strong>${fmtInt((stats.risk || {}).score || 0)} / ${escapeHtml(riskLabel(Number((stats.risk || {}).score || 0)))}</strong></div>
        <div><span>Сообщений</span><strong>${fmtInt(stats.total_messages)}</strong></div>
        <div><span>Участников</span><strong>${fmtInt(stats.total_senders || chat.members_count || 0)}</strong></div>
        <div><span>Просмотры</span><strong>${fmtInt(stats.total_views)}</strong></div>
        <div><span>Пересылки</span><strong>${fmtInt(stats.total_forwards)}</strong></div>
        <div><span>Последнее сообщение</span><strong>${fmtTs(stats.last_message_date)}</strong></div>
      </div>
    </div>
    <div class="inspector-card">
      <h3>Ключевые сигналы</h3>
      <div class="chip-row">${((stats.risk || {}).factors || []).slice(0, 10).map((item) => chip(item.label || item.key || "signal", item.severity === "high" || item.severity === "critical" ? "danger" : item.severity === "medium" ? "warn" : "ok")).join("") || '<span class="muted">Сигналов пока нет.</span>'}</div>
    </div>
    <div class="inspector-card">
      <h3>Связанные чаты</h3>
      <div class="related-list">${relatedChats(getChatId(chat)).slice(0, 8).map((item) => `<div class="related-item" data-chat-id="${escapeHtml(getChatId(item.row))}"><strong>${escapeHtml(chatName(item.row))}</strong><div class="muted">${escapeHtml(String(item.edge && item.edge.label || "link"))}</div></div>`).join("") || '<div class="empty">Связей не найдено.</div>'}</div>
    </div>
  `;
};

const renderTab = (chat) => {
  Object.entries(el.panels).forEach(([name, panel]) => panel.classList.toggle("active", state.tab === name));
  if (!chat) return;
  if (state.tab === "overview") renderOverview(chat);
  else if (state.tab === "activity") renderActivity(chat);
  else if (state.tab === "participants") renderParticipants(chat);
  else if (state.tab === "bots") renderBots(chat);
  else if (state.tab === "network") renderNetwork(chat);
  else if (state.tab === "content") renderContent(chat);
  else if (state.tab === "events") renderEvents(chat);
};

const renderAll = () => {
  const chat = ensureSelectedChat();
  renderSidebar();
  renderHeader(chat);
  renderTab(chat);
  renderInspector();
  document.body.classList.toggle("brief", state.view === "brief");
  document.querySelectorAll("[data-tab-btn]").forEach((button) => button.classList.toggle("active", button.dataset.tabBtn === state.tab));
};

el.globalSearch.addEventListener("input", (event) => { state.query = event.target.value || ""; renderAll(); });
el.typeFilter.addEventListener("change", (event) => { state.type = event.target.value || "all"; renderAll(); });
el.riskFilter.addEventListener("change", (event) => { state.risk = event.target.value || "all"; renderAll(); });
el.accessFilter.addEventListener("change", (event) => { state.access = event.target.value || "all"; renderAll(); });
el.sortFilter.addEventListener("change", (event) => { state.sort = event.target.value || "risk"; renderAll(); });
el.sidebarQuickFilter.addEventListener("change", (event) => { state.quick = event.target.value || "all"; renderAll(); });
el.sidebarMetric.addEventListener("change", (event) => { state.metric = event.target.value || "messages"; renderAll(); });
el.viewMode.addEventListener("change", (event) => { state.view = event.target.value || "analyst"; renderAll(); });
document.querySelectorAll("[data-tab-btn]").forEach((button) => button.addEventListener("click", () => { state.tab = button.dataset.tabBtn || "overview"; renderAll(); }));
document.addEventListener("click", (event) => {
  const chatTarget = event.target.closest("[data-chat-id]");
  if (chatTarget) {
    state.chatId = chatTarget.dataset.chatId || "";
    state.userId = "";
    renderAll();
    return;
  }
  const userTarget = event.target.closest("[data-user-id]");
  if (userTarget) {
    state.userId = userTarget.dataset.userId || "";
    renderInspector();
  }
});
document.addEventListener("input", (event) => {
  if (event.target && event.target.id === "memberSearch") {
    state.memberSearch = event.target.value || "";
    renderParticipants(findChat(state.chatId));
  }
  if (event.target && event.target.id === "botSearch") {
    state.botSearch = event.target.value || "";
    renderBots(findChat(state.chatId));
  }
});
document.addEventListener("change", (event) => {
  if (event.target && event.target.id === "memberSort") {
    state.memberSort = event.target.value || "messages";
    renderParticipants(findChat(state.chatId));
  }
  if (event.target && event.target.id === "botSort") {
    state.botSort = event.target.value || "risk";
    renderBots(findChat(state.chatId));
  }
});

const initialChat = chats.find((item) => item && item.is_root) || chats[0] || null;
state.chatId = initialChat ? getChatId(initialChat) : "";
renderAll();
"""


def _build_script(data_json: str) -> str:
    return "\n".join(
        [
            f"const REPORT_DATA = {data_json};",
            _SCRIPT_PART_1,
            _SCRIPT_PART_2,
            _SCRIPT_PART_3,
        ]
    )


def build_community_scan_report(title: str, report_data: Dict[str, Any], *, css_href: str) -> str:
    data = dict(report_data or {})
    summary = data.get("summary") if isinstance(data.get("summary"), dict) else {}
    community = data.get("community") if isinstance(data.get("community"), dict) else {}
    root_meta = "".join(
        [
            f'<span class="pill pill--accent">Запрос: {escape(str(data.get("query") or ""))}</span>',
            f'<span class="pill">Root: {escape(str(summary.get("root_title") or data.get("title") or title or ""))}</span>',
            f'<span class="pill {("pill--warn" if str(summary.get("root_access") or "") != "ok" else "")}">Доступ: {escape(str(summary.get("root_access") or "unknown"))}</span>',
            f'<span class="pill">Скан: {_fmt_ts(data.get("completed_at"))}</span>',
            f'<span class="pill">Пользователей: {_fmt_int(summary.get("users"))}</span>',
        ]
    )
    html = _HTML_TEMPLATE
    html = html.replace("__TITLE__", escape(str(title or data.get("title") or data.get("query") or "Community Scan")))
    html = html.replace("__CSS_HREF__", escape(str(css_href or "report.css")))
    html = html.replace("__SUMMARY_CARDS__", _summary_cards(summary, community))
    html = html.replace("__ROOT_META__", root_meta)
    html = html.replace("__GENERATED_AT__", escape(_fmt_ts(data.get("generated_at"))))
    html = html.replace("__SCRIPT__", _build_script(_safe_json(data)))
    return html
