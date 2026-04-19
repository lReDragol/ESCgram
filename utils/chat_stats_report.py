from __future__ import annotations

from datetime import datetime
from html import escape
from typing import Any, Dict, Iterable, List


def _fmt_int(value: Any) -> str:
    try:
        return f"{int(value or 0):,}".replace(",", " ")
    except Exception:
        return "0"


def _fmt_float(value: Any, digits: int = 2) -> str:
    try:
        return f"{float(value or 0):.{digits}f}"
    except Exception:
        return f"{0:.{digits}f}"


def _fmt_ts(value: Any) -> str:
    try:
        ts = int(value or 0)
    except Exception:
        ts = 0
    if ts <= 0:
        return "n/a"
    try:
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return str(ts)


def _chart_svg(
    title: str,
    points: Iterable[Dict[str, Any]],
    *,
    label_key: str,
    value_key: str,
    color: str = "#3da9fc",
    width: int = 920,
    height: int = 220,
) -> str:
    rows = [dict(item) for item in list(points or []) if isinstance(item, dict)]
    if not rows:
        return ""
    values: List[int] = []
    labels: List[str] = []
    for item in rows:
        try:
            values.append(max(0, int(item.get(value_key) or 0)))
        except Exception:
            values.append(0)
        labels.append(str(item.get(label_key) or "").strip())
    max_value = max(values or [1]) or 1
    left = 24
    right = 10
    top = 10
    bottom = 34
    plot_w = max(120, width - left - right)
    plot_h = max(80, height - top - bottom)
    count = max(1, len(rows))
    slot = plot_w / float(count)
    bar_w = max(2.0, slot * 0.78)
    label_step = max(1, count // 12)
    bars: List[str] = []
    labels_svg: List[str] = []
    for idx, value in enumerate(values):
        x = left + idx * slot + (slot - bar_w) / 2.0
        bar_h = (float(value) / float(max_value)) * plot_h if max_value > 0 else 0.0
        y = top + plot_h - bar_h
        label = labels[idx]
        bars.append(
            (
                f'<rect x="{x:.2f}" y="{y:.2f}" width="{bar_w:.2f}" height="{bar_h:.2f}" '
                f'rx="3" ry="3" fill="{color}">'
                f"<title>{escape(label)}: {_fmt_int(value)}</title>"
                "</rect>"
            )
        )
        if idx % label_step == 0 or idx == count - 1:
            label_x = left + idx * slot + slot / 2.0
            labels_svg.append(
                f'<text x="{label_x:.2f}" y="{height - 10}" text-anchor="middle">{escape(label)}</text>'
            )
    guides = []
    for frac in (0.0, 0.5, 1.0):
        guide_y = top + plot_h - (plot_h * frac)
        guide_value = int(round(max_value * frac))
        guides.append(
            f'<line x1="{left}" y1="{guide_y:.2f}" x2="{left + plot_w}" y2="{guide_y:.2f}" stroke="rgba(255,255,255,0.12)" stroke-width="1"/>'
        )
        guides.append(
            f'<text x="4" y="{guide_y + 4:.2f}" text-anchor="start">{escape(_fmt_int(guide_value))}</text>'
        )
    return (
        '<div class="chart">'
        f'<div class="chart-title">{escape(title)}</div>'
        '<div class="chart-scroll">'
        f'<svg viewBox="0 0 {width} {height}" preserveAspectRatio="none" role="img" aria-label="{escape(title)}">'
        + "".join(guides)
        + "".join(bars)
        + "".join(labels_svg)
        + "</svg></div></div>"
    )


def build_chat_statistics_report(title: str, chat_id: str, stats: Dict[str, Any]) -> str:
    data = dict(stats or {})
    senders = [dict(item) for item in list(data.get("sender_details") or []) if isinstance(item, dict)]
    sender_daily = data.get("sender_daily_activity") if isinstance(data.get("sender_daily_activity"), dict) else {}
    suspicious_ids = {
        int(item.get("sender_id") or 0)
        for item in list(data.get("suspicious_senders") or [])
        if isinstance(item, dict)
    }
    top_reactions = [dict(item) for item in list(data.get("top_reactions") or []) if isinstance(item, dict)]
    anomaly_flags = [str(item) for item in list(data.get("anomaly_flags") or []) if str(item or "").strip()]
    polls_summary = data.get("polls_summary") if isinstance(data.get("polls_summary"), dict) else {}
    scope = data.get("scope") if isinstance(data.get("scope"), dict) else {}
    date_range = data.get("date_range") if isinstance(data.get("date_range"), dict) else {}
    snapshot_meta = data.get("snapshot_meta") if isinstance(data.get("snapshot_meta"), dict) else {}
    risk = data.get("risk") if isinstance(data.get("risk"), dict) else {}
    distribution = data.get("distribution") if isinstance(data.get("distribution"), dict) else {}
    activity_summary = data.get("activity_summary") if isinstance(data.get("activity_summary"), dict) else {}
    bot_summary = data.get("bot_summary") if isinstance(data.get("bot_summary"), dict) else {}
    risk_factors = [dict(item) for item in list(risk.get("factors") or []) if isinstance(item, dict)]

    metric_cards = [
        ("Messages", _fmt_int(data.get("total_messages"))),
        ("Media", _fmt_int(data.get("media_messages"))),
        ("Deleted", _fmt_int(data.get("deleted_messages"))),
        ("Views", _fmt_int(data.get("total_views"))),
        ("Forwards", _fmt_int(data.get("total_forwards"))),
        ("Reactions", _fmt_int(data.get("total_reactions"))),
        ("Senders", _fmt_int(data.get("total_senders"))),
        ("Risk score", _fmt_int(risk.get("score"))),
    ]
    engagement = data.get("engagement") if isinstance(data.get("engagement"), dict) else {}
    overview_rows = [
        ("Chat", str(title or chat_id)),
        ("Chat ID", str(chat_id or "")),
        ("Scope", "full history" if bool(scope.get("is_full_history")) else f"last {_fmt_int(scope.get('message_limit'))} messages"),
        ("First message", _fmt_ts(date_range.get("first_message_date"))),
        ("Last message", _fmt_ts(date_range.get("last_message_date"))),
        ("Latest scan", _fmt_ts(data.get("scanned_at") or snapshot_meta.get("latest", {}).get("scanned_at") if isinstance(snapshot_meta.get("latest"), dict) else 0)),
        ("Risk level", str(risk.get("level") or "low")),
        ("Top-1 share", f"{_fmt_float(float(distribution.get('top1_share') or 0.0) * 100.0, 1)}%"),
    ]

    senders_table_rows: List[str] = []
    sender_details_blocks: List[str] = []
    for item in senders:
        sender_id = int(item.get("sender_id") or 0)
        username = str(item.get("username") or "").strip()
        sender_name = str(item.get("name") or sender_id)
        share = float(item.get("share") or 0.0) * 100.0
        suspicious_badge = '<span class="badge warn">suspicious</span>' if sender_id in suspicious_ids else ""
        risk_score = int(item.get("risk_score") or 0)
        senders_table_rows.append(
            "<tr>"
            f"<td>{escape(sender_name)}</td>"
            f"<td>{escape('@' + username if username else '')}</td>"
            f"<td>{escape(str(item.get('type') or ''))}</td>"
            f"<td>{_fmt_int(item.get('count'))}</td>"
            f"<td>{_fmt_float(share, 1)}%</td>"
            f"<td>{_fmt_int(item.get('media_messages'))}</td>"
            f"<td>{_fmt_int(item.get('deleted_messages'))}</td>"
            f"<td>{_fmt_ts(item.get('last_date'))}</td>"
            f"<td>{suspicious_badge}<span class='badge'>{_fmt_int(risk_score)}</span></td>"
            "</tr>"
        )
        day_series = sender_daily.get(str(sender_id)) if isinstance(sender_daily, dict) else None
        sender_chart = ""
        if isinstance(day_series, list) and day_series:
            sender_chart = _chart_svg(
                "Daily activity",
                day_series,
                label_key="day",
                value_key="count",
                color="#f59e0b" if sender_id in suspicious_ids else "#22c55e",
                width=920,
                height=180,
            )
        sender_details_blocks.append(
            "<details class='user-card'>"
            f"<summary>{escape(sender_name)}"
            f"{' (@' + escape(username) + ')' if username else ''}"
            f" <span class='muted'>messages: {_fmt_int(item.get('count'))}, share: {_fmt_float(share, 1)}%, risk: {_fmt_int(risk_score)}</span>"
            f"{suspicious_badge}</summary>"
            "<div class='user-grid'>"
            f"<div><strong>Type</strong><span>{escape(str(item.get('type') or 'n/a'))}</span></div>"
            f"<div><strong>Media</strong><span>{_fmt_int(item.get('media_messages'))}</span></div>"
            f"<div><strong>Deleted</strong><span>{_fmt_int(item.get('deleted_messages'))}</span></div>"
            f"<div><strong>Views</strong><span>{_fmt_int(item.get('total_views'))}</span></div>"
            f"<div><strong>Forwards</strong><span>{_fmt_int(item.get('total_forwards'))}</span></div>"
            f"<div><strong>Last message</strong><span>{_fmt_ts(item.get('last_date'))}</span></div>"
            f"<div><strong>Active days</strong><span>{_fmt_int(item.get('active_days'))}</span></div>"
            f"<div><strong>Peak day share</strong><span>{_fmt_float(float(item.get('peak_day_share') or 0.0) * 100.0, 1)}%</span></div>"
            f"<div><strong>Off-hours share</strong><span>{_fmt_float(float(item.get('off_hours_share') or 0.0) * 100.0, 1)}%</span></div>"
            f"<div><strong>Deleted share</strong><span>{_fmt_float(float(item.get('deleted_share') or 0.0) * 100.0, 1)}%</span></div>"
            "</div>"
            f"<p class='muted' style='margin-top:12px;'>{escape(', '.join(str(flag) for flag in list(item.get('risk_flags') or []) if str(flag).strip()))}</p>"
            f"{sender_chart}"
            "</details>"
        )

    reactions_html = "".join(
        f"<li><span>{escape(str(item.get('emoji') or '?'))}</span><strong>{_fmt_int(item.get('count'))}</strong></li>"
        for item in top_reactions
    )
    anomalies_html = "".join(f"<li>{escape(flag)}</li>" for flag in anomaly_flags)

    charts_html = "".join(
        [
            _chart_svg(
                "Hourly activity",
                list(data.get("hourly_activity") or []),
                label_key="hour",
                value_key="count",
                color="#3da9fc",
            ),
            _chart_svg(
                "Daily activity",
                list(data.get("daily_activity") or []),
                label_key="day",
                value_key="count",
                color="#ef476f",
                width=920,
                height=220,
            ),
            _chart_svg(
                "Top senders",
                [
                    {
                        "label": str(item.get("username") or item.get("name") or item.get("sender_id") or ""),
                        "count": int(item.get("count") or 0),
                    }
                    for item in senders[:12]
                ],
                label_key="label",
                value_key="count",
                color="#22c55e",
                width=920,
                height=220,
            ),
        ]
    )

    cards_html = "".join(
        f"<div class='metric-card'><span>{escape(label)}</span><strong>{escape(value)}</strong></div>"
        for label, value in metric_cards
    )
    overview_html = "".join(
        f"<div class='kv'><span>{escape(label)}</span><strong>{escape(value)}</strong></div>"
        for label, value in overview_rows
    )
    engagement_html = "".join(
        [
            f"<div class='kv'><span>Views / message</span><strong>{escape(_fmt_float(engagement.get('views_per_message')))}</strong></div>",
            f"<div class='kv'><span>Reactions / 100</span><strong>{escape(_fmt_float(engagement.get('reactions_per_100_messages')))}</strong></div>",
            f"<div class='kv'><span>Forwards / 100</span><strong>{escape(_fmt_float(engagement.get('forwards_per_100_messages')))}</strong></div>",
            f"<div class='kv'><span>Total polls</span><strong>{_fmt_int(polls_summary.get('total_polls'))}</strong></div>",
        ]
    )
    activity_html = "".join(
        [
            f"<div class='kv'><span>Active days</span><strong>{_fmt_int(activity_summary.get('active_days'))}</strong></div>",
            f"<div class='kv'><span>Messages / active day</span><strong>{escape(_fmt_float(activity_summary.get('messages_per_active_day')))}</strong></div>",
            f"<div class='kv'><span>Peak day</span><strong>{_fmt_int(activity_summary.get('peak_day', {}).get('count'))}</strong></div>",
            f"<div class='kv'><span>Night share</span><strong>{escape(_fmt_float(float(activity_summary.get('off_hours_share') or 0.0) * 100.0, 1))}%</strong></div>",
        ]
    )
    distribution_html = "".join(
        [
            f"<div class='kv'><span>Top-1 share</span><strong>{escape(_fmt_float(float(distribution.get('top1_share') or 0.0) * 100.0, 1))}%</strong></div>",
            f"<div class='kv'><span>Top-3 share</span><strong>{escape(_fmt_float(float(distribution.get('top3_share') or 0.0) * 100.0, 1))}%</strong></div>",
            f"<div class='kv'><span>Concentration</span><strong>{escape(_fmt_float(distribution.get('concentration_index'), 3))}</strong></div>",
            f"<div class='kv'><span>Suspicious msg share</span><strong>{escape(_fmt_float(float(distribution.get('suspicious_message_share') or 0.0) * 100.0, 1))}%</strong></div>",
        ]
    )
    bot_html = "".join(
        [
            f"<div class='kv'><span>Bot-like senders</span><strong>{_fmt_int(bot_summary.get('bot_like_senders'))}</strong></div>",
            f"<div class='kv'><span>Bot-like msg share</span><strong>{escape(_fmt_float(float(bot_summary.get('bot_like_message_share') or 0.0) * 100.0, 1))}%</strong></div>",
            f"<div class='kv'><span>Numeric usernames</span><strong>{_fmt_int(bot_summary.get('numeric_username_senders'))}</strong></div>",
            f"<div class='kv'><span>Risk level</span><strong>{escape(str(risk.get('level') or 'low'))}</strong></div>",
        ]
    )
    risk_factors_html = "".join(
        f"<li><span>{escape(str(item.get('label') or item.get('key') or 'risk'))}</span><strong>+{_fmt_int(item.get('score'))}</strong></li>"
        for item in risk_factors
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{escape(str(title or chat_id))} statistics</title>
  <style>
    :root {{
      color-scheme: dark;
      --bg: #08111d;
      --panel: #122133;
      --panel-2: #18283d;
      --text: #e8eef7;
      --muted: #8ea0b8;
      --line: rgba(255,255,255,0.08);
      --accent: #3da9fc;
      --warn: #f59e0b;
      --danger: #ef476f;
    }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; font: 14px/1.45 Segoe UI, Helvetica, Arial, sans-serif; background: radial-gradient(circle at top, #17304d 0%, var(--bg) 46%); color: var(--text); }}
    .wrap {{ max-width: 1280px; margin: 0 auto; padding: 32px 24px 48px; }}
    h1, h2, h3 {{ margin: 0 0 14px; }}
    h1 {{ font-size: 34px; }}
    h2 {{ font-size: 20px; margin-top: 28px; }}
    p, ul {{ margin: 0; }}
    .lede {{ color: var(--muted); margin-top: 8px; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 12px; }}
    .metric-card, .panel {{ background: color-mix(in srgb, var(--panel) 82%, black); border: 1px solid var(--line); border-radius: 16px; padding: 16px; }}
    .metric-card span, .kv span, .muted {{ color: var(--muted); }}
    .metric-card strong {{ display: block; margin-top: 6px; font-size: 28px; }}
    .meta-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 12px; margin-top: 14px; }}
    .kv {{ display: flex; justify-content: space-between; gap: 12px; padding: 8px 0; border-bottom: 1px solid rgba(255,255,255,0.05); }}
    .kv:last-child {{ border-bottom: none; }}
    .chart {{ background: color-mix(in srgb, var(--panel-2) 85%, black); border: 1px solid var(--line); border-radius: 16px; padding: 14px; margin-top: 14px; }}
    .chart-title {{ font-weight: 600; margin-bottom: 10px; }}
    .chart-scroll {{ overflow-x: auto; }}
    svg {{ width: 100%; min-width: 720px; height: 220px; }}
    svg text {{ fill: #8ea0b8; font-size: 11px; font-family: Segoe UI, Helvetica, Arial, sans-serif; }}
    .list-inline {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 10px; list-style: none; padding: 0; margin-top: 12px; }}
    .list-inline li {{ display: flex; justify-content: space-between; gap: 12px; background: color-mix(in srgb, var(--panel) 85%, black); border: 1px solid var(--line); border-radius: 12px; padding: 12px 14px; }}
    .table-wrap {{ overflow-x: auto; margin-top: 14px; }}
    table {{ width: 100%; border-collapse: collapse; min-width: 860px; }}
    th, td {{ text-align: left; padding: 10px 12px; border-bottom: 1px solid rgba(255,255,255,0.06); vertical-align: top; }}
    th {{ color: var(--muted); font-weight: 600; }}
    .badge {{ display: inline-flex; align-items: center; padding: 2px 8px; border-radius: 999px; font-size: 11px; background: rgba(61,169,252,0.18); color: #b8dcff; }}
    .badge.warn {{ background: rgba(245,158,11,0.18); color: #ffd18c; }}
    .split {{ display: grid; grid-template-columns: 1.2fr 0.8fr; gap: 18px; align-items: start; }}
    .user-card {{ margin-top: 12px; background: color-mix(in srgb, var(--panel) 85%, black); border: 1px solid var(--line); border-radius: 14px; padding: 12px 14px; }}
    .user-card summary {{ cursor: pointer; font-weight: 600; }}
    .user-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 10px; margin-top: 14px; }}
    .user-grid div {{ background: rgba(255,255,255,0.03); border-radius: 10px; padding: 10px 12px; }}
    .user-grid strong {{ display: block; font-size: 12px; color: var(--muted); margin-bottom: 4px; }}
    @media (max-width: 980px) {{
      .split {{ grid-template-columns: 1fr; }}
      h1 {{ font-size: 28px; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="panel">
      <h1>{escape(str(title or chat_id))}</h1>
      <p class="lede">Chat statistics export with charts, sender breakdown and activity slices.</p>
      <div class="grid" style="margin-top:18px;">{cards_html}</div>
      <div class="meta-grid">{overview_html}</div>
    </div>

    <div class="split" style="margin-top:18px;">
      <div class="panel">
        <h2>Engagement</h2>
        <div class="meta-grid">{engagement_html}</div>
      </div>
      <div class="panel">
        <h2>Risk Summary</h2>
        <div class="meta-grid">{distribution_html}</div>
      </div>
    </div>

    <div class="split" style="margin-top:18px;">
      <div class="panel">
        <h2>Activity Summary</h2>
        <div class="meta-grid">{activity_html}</div>
      </div>
      <div class="panel">
        <h2>Bot Signals</h2>
        <div class="meta-grid">{bot_html}</div>
      </div>
    </div>

    <div class="split" style="margin-top:18px;">
      <div class="panel">
        <h2>Anomalies</h2>
        <ul class="list-inline">{anomalies_html or '<li><span>none</span><strong>ok</strong></li>'}</ul>
      </div>
      <div class="panel">
        <h2>Risk Factors</h2>
        <ul class="list-inline">{risk_factors_html or '<li><span>stable</span><strong>0</strong></li>'}</ul>
      </div>
    </div>

    <div class="panel" style="margin-top:18px;">
      <h2>Activity charts</h2>
      {charts_html}
    </div>

    <div class="split" style="margin-top:18px;">
      <div class="panel">
        <h2>Top reactions</h2>
        <ul class="list-inline">{reactions_html or '<li><span>n/a</span><strong>0</strong></li>'}</ul>
      </div>
      <div class="panel">
        <h2>Polls</h2>
        <div class="meta-grid">
          <div class="kv"><span>Total</span><strong>{_fmt_int(polls_summary.get('total_polls'))}</strong></div>
          <div class="kv"><span>Open</span><strong>{_fmt_int(polls_summary.get('open_polls'))}</strong></div>
          <div class="kv"><span>Closed</span><strong>{_fmt_int(polls_summary.get('closed_polls'))}</strong></div>
          <div class="kv"><span>Total voters</span><strong>{_fmt_int(polls_summary.get('total_voters'))}</strong></div>
        </div>
      </div>
    </div>

    <div class="panel" style="margin-top:18px;">
      <h2>Sender table</h2>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Name</th>
              <th>Username</th>
              <th>Type</th>
              <th>Messages</th>
              <th>Share</th>
              <th>Media</th>
              <th>Deleted</th>
              <th>Last message</th>
              <th>Flags</th>
            </tr>
          </thead>
          <tbody>{''.join(senders_table_rows) or '<tr><td colspan="9">No sender data</td></tr>'}</tbody>
        </table>
      </div>
    </div>

    <div class="panel" style="margin-top:18px;">
      <h2>Per-user details</h2>
      {''.join(sender_details_blocks) or '<p class="muted">No user sections available.</p>'}
    </div>
  </div>
</body>
</html>
"""
