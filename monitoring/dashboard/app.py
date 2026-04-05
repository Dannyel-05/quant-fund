"""
Apollo Web Dashboard — Flask application.
Dark themed, Bootstrap 5, Chart.js. Session-based auth (24h).
All database access is strictly read-only.
"""
import json
import logging
import os
import sqlite3
import subprocess
from datetime import datetime, timedelta, timezone
from functools import wraps
from pathlib import Path
from typing import Any, Optional

import yaml

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parents[2]

DB_CLOSELOOP = str(_ROOT / "closeloop" / "storage" / "closeloop.db")
DB_CLOSELOOP_DATA = str(_ROOT / "closeloop_data.db")
DB_HISTORICAL = str(_ROOT / "output" / "historical_db.db")
DB_FRONTIER = str(_ROOT / "frontier" / "storage" / "frontier.db")
DB_DEEPDATA = str(_ROOT / "deepdata" / "storage" / "deepdata.db")
DB_INTELLIGENCE = str(_ROOT / "output" / "intelligence_db.db")

START_DATE = "2026-04-03"


def _load_config() -> dict:
    try:
        return yaml.safe_load((_ROOT / "config" / "settings.yaml").read_text())
    except Exception:
        return {}


def _db_path() -> str:
    try:
        if Path(DB_CLOSELOOP).stat().st_size > 100:
            return DB_CLOSELOOP
    except Exception:
        pass
    return DB_CLOSELOOP_DATA


def _safe_query(db_path: str, sql: str, params=()) -> list:
    try:
        conn = sqlite3.connect(db_path, timeout=8)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(sql, params).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        logger.debug(f"dashboard DB query failed: {e}")
        return []


def _safe_scalar(db_path: str, sql: str, params=(), default=None):
    rows = _safe_query(db_path, sql, params)
    if rows:
        return list(rows[0].values())[0]
    return default


# ── Dark theme CSS ────────────────────────────────────────────────────────────

DARK_CSS = """
body { background: #1a1a2e; color: #e0e0e0; font-family: 'Segoe UI', sans-serif; }
.card { background: #16213e; border: 1px solid #0f3460; border-radius: 8px; }
.card-header { background: #0f3460; border-bottom: 1px solid #e94560; }
.table { color: #e0e0e0; }
.table-dark { background: #16213e; }
.table-hover tbody tr:hover { background: #0f3460; }
.badge-bull { background: #00c853; }
.badge-bear { background: #ff1744; }
.badge-neutral { background: #9e9e9e; }
.badge-crisis { background: #b71c1c; }
.badge-euphoria { background: #7b1fa2; }
.nav-link { color: #e0e0e0 !important; }
.nav-link:hover, .nav-link.active { color: #e94560 !important; }
.btn-apollo { background: #e94560; border: none; color: white; }
.btn-apollo:hover { background: #c62a47; color: white; }
.status-dot { width: 12px; height: 12px; border-radius: 50%; display: inline-block; }
.dot-green { background: #00c853; }
.dot-amber { background: #ffc107; }
.dot-red { background: #ff1744; }
.pnl-pos { color: #00c853; }
.pnl-neg { color: #ff1744; }
.chart-container { position: relative; height: 300px; }
a { color: #64b5f6; }
input, select { background: #0f3460 !important; color: #e0e0e0 !important; border-color: #0f3460 !important; }
"""

# ── HTML templates (inline — no separate template files needed) ───────────────

BASE_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Apollo Fund — {title}</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>{css}</style>
</head>
<body>
<nav class="navbar navbar-dark py-2 px-4" style="background:#0f3460;border-bottom:2px solid #e94560">
  <a class="navbar-brand fw-bold" href="/">🤖 Apollo Fund</a>
  <div class="d-flex gap-3">
    <a class="nav-link{home_active}" href="/">Home</a>
    <a class="nav-link{pos_active}" href="/positions">Positions</a>
    <a class="nav-link{perf_active}" href="/performance">Performance</a>
    <a class="nav-link{data_active}" href="/data">Data</a>
    <a class="nav-link{regime_active}" href="/regime">Regime</a>
    <a class="nav-link" href="/logout">Logout</a>
  </div>
</nav>
<div class="container-fluid py-3">
{content}
</div>
<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
{scripts}
</body>
</html>"""

LOGIN_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Apollo — Login</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
<style>{css}</style>
</head>
<body class="d-flex align-items-center justify-content-center" style="min-height:100vh">
<div class="card p-4" style="width:360px">
  <h4 class="text-center mb-3">🤖 Apollo Fund</h4>
  {error}
  <form method="POST">
    <div class="mb-3">
      <label class="form-label">Username</label>
      <input type="text" name="username" class="form-control" value="apollo" required>
    </div>
    <div class="mb-3">
      <label class="form-label">Password</label>
      <input type="password" name="password" class="form-control" required>
    </div>
    <button type="submit" class="btn btn-apollo w-100">Login</button>
  </form>
</div>
</body>
</html>"""


def _base(title: str, content: str, scripts: str = "", active: str = "home") -> str:
    return BASE_HTML.format(
        title=title,
        css=DARK_CSS,
        home_active=" active" if active == "home" else "",
        pos_active=" active" if active == "positions" else "",
        perf_active=" active" if active == "performance" else "",
        data_active=" active" if active == "data" else "",
        regime_active=" active" if active == "regime" else "",
        content=content,
        scripts=scripts,
    )


# ── Flask app ─────────────────────────────────────────────────────────────────

def create_app() -> "Flask":
    from flask import Flask, request, redirect, url_for, session, Response
    app = Flask(__name__, template_folder=None)
    app.secret_key = os.urandom(32)
    app.permanent_session_lifetime = timedelta(hours=24)

    cfg = _load_config()
    DASHBOARD_PASSWORD = cfg.get("dashboard", {}).get("password", "apollo2026")

    def login_required(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            if not session.get("logged_in"):
                return redirect(url_for("login"))
            return f(*args, **kwargs)
        return decorated

    # ── Auth routes ───────────────────────────────────────────────────────────

    @app.route("/login", methods=["GET", "POST"])
    def login():
        error = ""
        if request.method == "POST":
            if (request.form.get("username") == "apollo" and
                    request.form.get("password") == DASHBOARD_PASSWORD):
                session.permanent = True
                session["logged_in"] = True
                return redirect(url_for("home"))
            error = '<div class="alert alert-danger">Invalid credentials</div>'
        return LOGIN_HTML.format(css=DARK_CSS, error=error)

    @app.route("/logout")
    def logout():
        session.clear()
        return redirect(url_for("login"))

    # ── Home page ─────────────────────────────────────────────────────────────

    @app.route("/")
    @login_required
    def home():
        # Bot status
        pid = "?"
        bot_running = False
        try:
            result = subprocess.run(["ps", "aux"], capture_output=True, text=True, timeout=5)
            for line in result.stdout.splitlines():
                if "python3 main.py" in line and "grep" not in line:
                    parts = line.split()
                    pid = parts[1] if len(parts) > 1 else "?"
                    bot_running = True
                    break
        except Exception:
            pass

        status_badge = (
            '<span class="badge bg-success fs-5">BOT RUNNING ✅</span>'
            if bot_running else
            '<span class="badge bg-danger fs-5">BOT STOPPED ❌</span>'
        )

        # Regime
        regime = "NEUTRAL"
        confidence = 0.0
        try:
            row = _safe_query(DB_FRONTIER,
                "SELECT level, umci FROM umci_history ORDER BY recorded_at DESC LIMIT 1")
            if row:
                regime = row[0].get("level", "NEUTRAL")
                confidence = row[0].get("umci", 0.0) or 0.0
        except Exception:
            pass

        regime_colours = {"BULL": "success", "NEUTRAL": "secondary", "BEAR": "warning",
                         "CRISIS": "danger", "EUPHORIA": "purple"}
        regime_badge = f'<span class="badge bg-{regime_colours.get(regime, "secondary")} fs-6">{regime} ({confidence:.0f}% confidence)</span>'

        # Phase and trades
        phase = "1"
        real_trades = 0
        try:
            status_file = _ROOT / "output" / "bot_status.json"
            if status_file.exists():
                data = json.loads(status_file.read_text())
                raw = str(data.get("phase", "1"))
                import re
                m = re.search(r"(\d)", raw)
                phase = m.group(1) if m else "1"
        except Exception:
            pass

        real_trades = _safe_scalar(_db_path(),
            "SELECT COUNT(*) FROM trade_ledger WHERE is_phantom=0 AND ABS(net_pnl)>0.01",
            default=0) or 0

        # Open positions count
        open_pos = _safe_scalar(_db_path(),
            "SELECT COUNT(*) FROM trade_ledger WHERE exit_date IS NULL AND is_phantom=0",
            default=0) or 0

        # Today's PnL
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        today_pnl = _safe_scalar(_db_path(),
            "SELECT SUM(net_pnl) FROM trade_ledger WHERE exit_date=? AND is_phantom=0",
            (today,), default=0.0) or 0.0

        pnl_class = "pnl-pos" if today_pnl >= 0 else "pnl-neg"
        pnl_sign = "+" if today_pnl >= 0 else ""

        # Collector dots
        collector_names = [
            "shipping", "commodity", "sec_fulltext", "technology", "government",
            "consumer", "alternative_quiver", "reddit", "social_influence",
            "finnhub", "options_flow", "insider", "jobs",
        ]
        log_path = _ROOT / "logs" / "quant_fund.log"
        collector_grid = ""
        try:
            with open(log_path, "r", errors="replace") as f:
                log_content = f.read()[-50000:]
            for cn in collector_names:
                has_error = "ERROR" in log_content and cn in log_content.split("ERROR")[0][-500:]
                dot_class = "dot-amber" if has_error else "dot-green"
                collector_grid += f'<span title="{cn}" class="status-dot {dot_class} me-1 mb-1"></span>'
        except Exception:
            collector_grid = '<span class="text-muted">Collector data unavailable</span>'

        # Equity curve data
        equity_rows = _safe_query(_db_path(),
            "SELECT exit_date, net_pnl FROM trade_ledger "
            "WHERE exit_date IS NOT NULL AND entry_date>=? AND is_phantom=0 "
            "ORDER BY exit_date ASC", (START_DATE,))
        eq_labels, eq_data = [], []
        cum = 0.0
        daily: dict[str, float] = {}
        for r in equity_rows:
            day = (r.get("exit_date") or "")[:10]
            if day:
                daily[day] = daily.get(day, 0.0) + (r.get("net_pnl", 0) or 0)
        for day in sorted(daily.keys()):
            cum += daily[day]
            eq_labels.append(day)
            eq_data.append(round(cum, 2))

        # Win rate / Sharpe for quick stats
        closed = _safe_query(_db_path(),
            "SELECT net_pnl FROM trade_ledger WHERE exit_date IS NOT NULL AND is_phantom=0")
        all_closed = [r.get("net_pnl", 0) or 0 for r in closed]
        win_rate = len([x for x in all_closed if x > 0]) / len(all_closed) if all_closed else 0.0
        total_closed = len(all_closed)

        content = f"""
<div class="row g-3 mb-3">
  <div class="col-md-2">{status_badge}</div>
  <div class="col-md-4">{regime_badge}</div>
  <div class="col-md-3">
    <span class="badge bg-info fs-6">PHASE-{phase} | {real_trades} real trades</span>
  </div>
  <div class="col-md-3">
    <span class="badge bg-dark border fs-6">
      Today: <span class="{pnl_class}">{pnl_sign}£{today_pnl:,.0f}</span>
    </span>
  </div>
</div>

<div class="row g-3 mb-3">
  <div class="col-md-4">
    <div class="card p-3">
      <div class="card-header mb-2"><strong>📡 Collectors (13)</strong></div>
      <div class="d-flex flex-wrap p-2">{collector_grid}</div>
    </div>
  </div>
  <div class="col-md-5">
    <div class="card p-3">
      <div class="card-header mb-2"><strong>📈 Equity Curve</strong></div>
      <div class="chart-container"><canvas id="equityChart"></canvas></div>
    </div>
  </div>
  <div class="col-md-3">
    <div class="card p-3">
      <div class="card-header mb-2"><strong>📊 Quick Stats</strong></div>
      <table class="table table-sm mb-0">
        <tr><td>Open Positions</td><td class="text-end fw-bold">{open_pos}</td></tr>
        <tr><td>Win Rate</td><td class="text-end fw-bold">{win_rate:.1%}</td></tr>
        <tr><td>Total Trades</td><td class="text-end fw-bold">{total_closed}</td></tr>
        <tr><td>All-time PnL</td><td class="text-end fw-bold {'pnl-pos' if cum >= 0 else 'pnl-neg'}">£{cum:+,.0f}</td></tr>
      </table>
    </div>
  </div>
</div>

<script>
setTimeout(() => location.reload(), 60000);
</script>
"""
        scripts = f"""
<script>
const eq = new Chart(document.getElementById('equityChart'), {{
  type: 'line',
  data: {{
    labels: {json.dumps(eq_labels[-60:])},
    datasets: [{{
      label: 'Cumulative PnL (£)',
      data: {json.dumps(eq_data[-60:])},
      borderColor: '#00c853',
      backgroundColor: 'rgba(0,200,83,0.1)',
      fill: true,
      tension: 0.3,
      pointRadius: 0,
    }}]
  }},
  options: {{
    responsive: true, maintainAspectRatio: false,
    plugins: {{ legend: {{ labels: {{ color: '#e0e0e0' }} }} }},
    scales: {{
      x: {{ ticks: {{ color: '#9e9e9e', maxTicksLimit: 6 }}, grid: {{ color: '#0f3460' }} }},
      y: {{ ticks: {{ color: '#9e9e9e' }}, grid: {{ color: '#0f3460' }} }}
    }}
  }}
}});
</script>"""

        return _base("Dashboard", content, scripts, "home")

    # ── Positions page ────────────────────────────────────────────────────────

    @app.route("/positions")
    @login_required
    def positions():
        rows = _safe_query(_db_path(),
            "SELECT ticker, entry_date, entry_price, exit_price, pnl_pct, net_pnl, "
            "position_size, signals_at_entry, holding_days, sector, direction, macro_regime, "
            "id "
            "FROM trade_ledger WHERE exit_date IS NULL AND is_phantom=0 "
            "ORDER BY pnl_pct DESC")

        rows_html = ""
        for r in rows:
            pnl_pct = r.get("pnl_pct", 0) or 0
            net_pnl = r.get("net_pnl", 0) or 0
            pnl_class = "pnl-pos" if pnl_pct >= 0 else "pnl-neg"
            sign = "+" if pnl_pct >= 0 else ""
            rows_html += f"""
<tr data-bs-toggle="modal" data-bs-target="#posModal"
    data-ticker="{r.get('ticker','?')}" style="cursor:pointer">
  <td>{r.get('ticker','?')}</td>
  <td>{(r.get('entry_date') or '')[:10]}</td>
  <td>£{r.get('entry_price',0):.2f}</td>
  <td>—</td>
  <td class="{pnl_class}">{sign}{pnl_pct:.1f}%</td>
  <td class="{pnl_class}">{sign}£{net_pnl:.0f}</td>
  <td>{r.get('position_size',0):.0f}</td>
  <td>{(r.get('signals_at_entry') or '')[:20]}</td>
  <td>{r.get('holding_days',0)}</td>
  <td>{r.get('sector','?')}</td>
</tr>"""

        content = f"""
<div class="d-flex justify-content-between align-items-center mb-3">
  <h4>📊 Open Positions ({len(rows)})</h4>
  <a href="/positions/export" class="btn btn-apollo btn-sm">Export CSV</a>
</div>
<input type="text" id="searchInput" class="form-control mb-3" placeholder="Search positions..."
  oninput="filterTable(this.value)">
<div class="card">
  <table class="table table-hover mb-0" id="posTable">
    <thead style="background:#0f3460">
      <tr>
        <th>Ticker</th><th>Entry Date</th><th>Entry £</th><th>Current £</th>
        <th>PnL%</th><th>PnL£</th><th>Size</th><th>Signal</th><th>Days</th><th>Sector</th>
      </tr>
    </thead>
    <tbody>{rows_html or '<tr><td colspan="10" class="text-center">No open positions</td></tr>'}</tbody>
  </table>
</div>

<!-- Modal -->
<div class="modal fade" id="posModal" tabindex="-1">
  <div class="modal-dialog modal-xl">
    <div class="modal-content" style="background:#16213e;color:#e0e0e0">
      <div class="modal-header" style="border-color:#e94560">
        <h5 class="modal-title">Position Detail: <span id="modalTicker"></span></h5>
        <button type="button" class="btn-close btn-close-white" data-bs-dismiss="modal"></button>
      </div>
      <div class="modal-body" id="modalBody">Loading...</div>
    </div>
  </div>
</div>
"""
        scripts = """
<script>
function filterTable(val) {
  const rows = document.querySelectorAll('#posTable tbody tr');
  rows.forEach(r => {
    r.style.display = r.textContent.toLowerCase().includes(val.toLowerCase()) ? '' : 'none';
  });
}

document.getElementById('posModal').addEventListener('show.bs.modal', function(e) {
  const btn = e.relatedTarget;
  const ticker = btn.dataset.ticker;
  document.getElementById('modalTicker').textContent = ticker;
  document.getElementById('modalBody').innerHTML = '<p class="text-center">Loading ' + ticker + '...</p>';
  fetch('/api/position/' + ticker)
    .then(r => r.json())
    .then(data => {
      document.getElementById('modalBody').innerHTML = data.html || '<p>No data</p>';
    })
    .catch(() => { document.getElementById('modalBody').innerHTML = '<p>Error loading data</p>'; });
});
</script>"""
        return _base("Positions", content, scripts, "positions")

    @app.route("/positions/export")
    @login_required
    def positions_export():
        from flask import Response
        rows = _safe_query(_db_path(),
            "SELECT ticker, entry_date, entry_price, pnl_pct, net_pnl, position_size, "
            "signals_at_entry, holding_days, sector, direction "
            "FROM trade_ledger WHERE exit_date IS NULL AND is_phantom=0 ORDER BY pnl_pct DESC")
        lines = ["ticker,entry_date,entry_price,pnl_pct,net_pnl,size,signal,days,sector,direction"]
        for r in rows:
            lines.append(",".join(str(r.get(k, "")) for k in
                ["ticker","entry_date","entry_price","pnl_pct","net_pnl",
                 "position_size","signals_at_entry","holding_days","sector","direction"]))
        return Response("\n".join(lines), mimetype="text/csv",
                       headers={"Content-Disposition": "attachment;filename=positions.csv"})

    @app.route("/api/position/<ticker>")
    @login_required
    def api_position(ticker):
        from flask import jsonify
        try:
            trade = _safe_query(_db_path(),
                "SELECT * FROM trade_ledger WHERE ticker=? AND exit_date IS NULL AND is_phantom=0 "
                "ORDER BY entry_date DESC LIMIT 1", (ticker,))
            if not trade:
                return jsonify({"html": f"<p>No open position found for {ticker}</p>"})
            t = trade[0]

            # Signal attribution
            attr = _safe_query(_db_path(),
                "SELECT * FROM pnl_attribution WHERE trade_id=?", (t.get("id", 0),))
            attr_rows = ""
            for a in attr[:5]:
                attr_rows += f"<tr><td>{a.get('signal_name','?')}</td><td>{a.get('signal_strength_at_entry',0):.3f}</td><td>£{a.get('attributed_pnl',0):+.0f}</td></tr>"

            # News
            news = _safe_query(DB_HISTORICAL,
                "SELECT headline, sentiment_raw, published_date FROM news_context "
                "WHERE ticker=? ORDER BY published_date DESC LIMIT 3", (ticker,))
            news_html = ""
            for n in news:
                sent = n.get("sentiment_raw", 0) or 0
                icon = "🟢" if sent > 0.1 else ("🔴" if sent < -0.1 else "⚪")
                news_html += f"<li>{icon} {n.get('headline','?')[:80]} <small class='text-muted'>({(n.get('published_date',''))[:10]})</small></li>"

            html = f"""
<div class="row">
  <div class="col-md-6">
    <h6>Entry Details</h6>
    <table class="table table-sm">
      <tr><td>Entry Date</td><td>{t.get('entry_date','?')[:10]}</td></tr>
      <tr><td>Entry Price</td><td>£{t.get('entry_price',0):.2f}</td></tr>
      <tr><td>PnL</td><td class="{'pnl-pos' if (t.get('pnl_pct',0) or 0)>=0 else 'pnl-neg'}">{t.get('pnl_pct',0):+.1f}%</td></tr>
      <tr><td>Days Held</td><td>{t.get('holding_days',0)}</td></tr>
      <tr><td>Sector</td><td>{t.get('sector','?')}</td></tr>
      <tr><td>Regime at Entry</td><td>{t.get('macro_regime','?')}</td></tr>
    </table>
  </div>
  <div class="col-md-6">
    <h6>Signal Attribution</h6>
    <table class="table table-sm">
      <tr><th>Signal</th><th>Strength</th><th>PnL</th></tr>
      {attr_rows or '<tr><td colspan="3">No attribution data</td></tr>'}
    </table>
  </div>
</div>
<div class="mt-3">
  <h6>Recent News</h6>
  <ul class="list-unstyled">
    {news_html or '<li class="text-muted">No news data</li>'}
  </ul>
</div>"""
            return jsonify({"html": html})
        except Exception as e:
            return jsonify({"html": f"<p>Error: {e}</p>"})

    # ── Performance page ──────────────────────────────────────────────────────

    @app.route("/performance")
    @login_required
    def performance():
        closed = _safe_query(_db_path(),
            "SELECT * FROM trade_ledger WHERE exit_date IS NOT NULL AND is_phantom=0 "
            "AND entry_date>=? ORDER BY exit_date DESC", (START_DATE,))

        total = len(closed)
        wins = [t for t in closed if (t.get("net_pnl", 0) or 0) > 0]
        losses = [t for t in closed if (t.get("net_pnl", 0) or 0) <= 0]
        win_rate = len(wins) / total if total > 0 else 0.0
        total_pnl = sum(t.get("net_pnl", 0) or 0 for t in closed)
        avg_win = sum(t.get("net_pnl", 0) or 0 for t in wins) / len(wins) if wins else 0
        avg_loss = sum(t.get("net_pnl", 0) or 0 for t in losses) / len(losses) if losses else 0
        expectancy = (win_rate * avg_win) + ((1 - win_rate) * avg_loss)

        # Equity curve
        daily: dict[str, float] = {}
        for t in closed:
            day = (t.get("exit_date") or "")[:10]
            if day:
                daily[day] = daily.get(day, 0.0) + (t.get("net_pnl", 0) or 0)
        eq_labels, eq_data = [], []
        cum = 0.0
        for day in sorted(daily.keys()):
            cum += daily[day]
            eq_labels.append(day)
            eq_data.append(round(cum, 2))

        # Best/worst
        best = max(closed, key=lambda t: t.get("pnl_pct", 0) or 0) if closed else {}
        worst = min(closed, key=lambda t: t.get("pnl_pct", 0) or 0) if closed else {}

        # Closed trades table (last 20)
        trade_rows = ""
        for t in closed[:20]:
            pnl_pct = t.get("pnl_pct", 0) or 0
            pnl_class = "pnl-pos" if pnl_pct >= 0 else "pnl-neg"
            sign = "+" if pnl_pct >= 0 else ""
            trade_rows += f"""
<tr>
  <td>{t.get('ticker','?')}</td>
  <td>{(t.get('entry_date',''))[:10]}</td>
  <td>{(t.get('exit_date',''))[:10]}</td>
  <td class="{pnl_class}">{sign}{pnl_pct:.1f}%</td>
  <td class="{pnl_class}">{sign}£{t.get('net_pnl',0):.0f}</td>
  <td>{(t.get('signals_at_entry',''))[:20]}</td>
  <td>{t.get('holding_days',0)}</td>
</tr>"""

        content = f"""
<div class="row g-3 mb-3">
  <div class="col-md-3">
    <div class="card p-3 text-center">
      <div class="fs-2 fw-bold {'pnl-pos' if total_pnl >= 0 else 'pnl-neg'}">£{total_pnl:+,.0f}</div>
      <div class="text-muted">All-time PnL</div>
    </div>
  </div>
  <div class="col-md-3">
    <div class="card p-3 text-center">
      <div class="fs-2 fw-bold">{win_rate:.1%}</div>
      <div class="text-muted">Win Rate ({len(wins)}/{total})</div>
    </div>
  </div>
  <div class="col-md-3">
    <div class="card p-3 text-center">
      <div class="fs-2 fw-bold {'pnl-pos' if avg_win >= 0 else 'pnl-neg'}">£{avg_win:+,.0f}</div>
      <div class="text-muted">Avg Win</div>
    </div>
  </div>
  <div class="col-md-3">
    <div class="card p-3 text-center">
      <div class="fs-2 fw-bold {'pnl-pos' if expectancy >= 0 else 'pnl-neg'}">£{expectancy:+,.0f}</div>
      <div class="text-muted">Expectancy/Trade</div>
    </div>
  </div>
</div>

<div class="row g-3 mb-3">
  <div class="col-md-8">
    <div class="card p-3">
      <div class="card-header mb-2"><strong>📈 Equity Curve</strong></div>
      <div class="chart-container" style="height:350px"><canvas id="equityChart"></canvas></div>
    </div>
  </div>
  <div class="col-md-4">
    <div class="card p-3">
      <div class="card-header mb-2"><strong>🏆 Best/Worst</strong></div>
      <table class="table table-sm">
        <tr><td>Best Trade</td><td class="pnl-pos">{best.get('ticker','?')} {best.get('pnl_pct',0):+.1f}%</td></tr>
        <tr><td>Worst Trade</td><td class="pnl-neg">{worst.get('ticker','?')} {worst.get('pnl_pct',0):+.1f}%</td></tr>
        <tr><td>Avg Loss</td><td class="pnl-neg">£{avg_loss:+,.0f}</td></tr>
      </table>
    </div>
  </div>
</div>

<div class="card">
  <div class="card-header"><strong>📋 Recent Closed Trades</strong></div>
  <table class="table table-hover mb-0">
    <thead style="background:#0f3460">
      <tr><th>Ticker</th><th>Entry</th><th>Exit</th><th>PnL%</th><th>PnL£</th><th>Signal</th><th>Days</th></tr>
    </thead>
    <tbody>{trade_rows or '<tr><td colspan="7" class="text-center">No closed trades</td></tr>'}</tbody>
  </table>
</div>
"""
        scripts = f"""
<script>
const eq = new Chart(document.getElementById('equityChart'), {{
  type: 'line',
  data: {{
    labels: {json.dumps(eq_labels)},
    datasets: [{{
      label: 'Cumulative PnL (£)',
      data: {json.dumps(eq_data)},
      borderColor: '#00c853',
      backgroundColor: 'rgba(0,200,83,0.1)',
      fill: true,
      tension: 0.3,
      pointRadius: 0,
    }}]
  }},
  options: {{
    responsive: true, maintainAspectRatio: false,
    plugins: {{ legend: {{ labels: {{ color: '#e0e0e0' }} }} }},
    scales: {{
      x: {{ ticks: {{ color: '#9e9e9e', maxTicksLimit: 8 }}, grid: {{ color: '#0f3460' }} }},
      y: {{ ticks: {{ color: '#9e9e9e' }}, grid: {{ color: '#0f3460' }} }}
    }}
  }}
}});
</script>"""
        return _base("Performance", content, scripts, "performance")

    # ── Data page ─────────────────────────────────────────────────────────────

    @app.route("/data")
    @login_required
    def data():
        # Database inventory
        dbs = [
            ("closeloop", _db_path()),
            ("historical", DB_HISTORICAL),
            ("frontier", DB_FRONTIER),
            ("deepdata", DB_DEEPDATA),
            ("intelligence", DB_INTELLIGENCE),
        ]
        db_rows = ""
        for name, path in dbs:
            try:
                conn = sqlite3.connect(path, timeout=5)
                tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
                total_rows = 0
                for (tname,) in tables:
                    try:
                        total_rows += conn.execute(f"SELECT COUNT(*) FROM {tname}").fetchone()[0]
                    except Exception:
                        pass
                conn.close()
                sz = os.path.getsize(path) / 1e6
                mtime = datetime.fromtimestamp(os.path.getmtime(path)).strftime("%Y-%m-%d %H:%M")
                db_rows += f"<tr><td>{name}</td><td>{len(tables)}</td><td>{total_rows:,}</td><td>{sz:.1f} MB</td><td>{mtime}</td></tr>"
            except Exception as e:
                db_rows += f"<tr><td>{name}</td><td colspan='4' class='text-warning'>Error: {e}</td></tr>"

        # Recent errors
        log_path = _ROOT / "logs" / "quant_fund.log"
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        errors = []
        try:
            with open(log_path, "r", errors="replace") as f:
                for line in f:
                    if today_str in line and ("ERROR" in line or "CRITICAL" in line):
                        errors.append(f"<li><code>{line.strip()[:120]}</code></li>")
        except Exception:
            pass
        error_html = "\n".join(errors[-20:]) if errors else "<li class='text-success'>No errors today</li>"

        content = f"""
<h4 class="mb-3">🗄️ Data & System</h4>
<div class="row g-3">
  <div class="col-md-7">
    <div class="card">
      <div class="card-header"><strong>Database Inventory</strong></div>
      <table class="table table-hover mb-0">
        <thead style="background:#0f3460">
          <tr><th>Database</th><th>Tables</th><th>Total Rows</th><th>Size</th><th>Last Modified</th></tr>
        </thead>
        <tbody>{db_rows}</tbody>
      </table>
    </div>
  </div>
  <div class="col-md-5">
    <div class="card">
      <div class="card-header"><strong>⚠️ Today's Errors</strong></div>
      <div class="p-3" style="max-height:400px;overflow-y:auto">
        <ul class="list-unstyled">{error_html}</ul>
      </div>
    </div>
  </div>
</div>
"""
        return _base("Data", content, "", "data")

    # ── Regime page ───────────────────────────────────────────────────────────

    @app.route("/regime")
    @login_required
    def regime():
        umci_rows = _safe_query(DB_FRONTIER,
            "SELECT * FROM umci_history ORDER BY recorded_at DESC LIMIT 1")
        current_regime = "NEUTRAL"
        confidence = 0.5
        mult = 1.0
        halt = False
        probs = [0.05, 0.10, 0.40, 0.35, 0.10]

        if umci_rows:
            r = umci_rows[0]
            current_regime = r.get("level", "NEUTRAL")
            confidence = r.get("umci", 0.5) or 0.5
            mult = r.get("position_mult", 1.0) or 1.0
            halt = bool(r.get("halt", False))
            try:
                bd = json.loads(r.get("full_breakdown", "{}") or "{}")
                probs = [
                    bd.get("p_crisis", 0.05), bd.get("p_bear", 0.10),
                    bd.get("p_neutral", 0.40), bd.get("p_bull", 0.35),
                    bd.get("p_euphoria", 0.10),
                ]
            except Exception:
                pass

        # Recent regime history
        history = _safe_query(DB_FRONTIER,
            "SELECT level, recorded_at FROM umci_history "
            "WHERE recorded_at >= date('now', '-30 days') ORDER BY recorded_at ASC LIMIT 100")
        regime_history_rows = ""
        for h in history[-20:]:
            level = h.get("level", "?")
            ts = (h.get("recorded_at") or "")[:16]
            colour = {"BULL": "success", "BEAR": "danger", "NEUTRAL": "secondary",
                     "CRISIS": "danger", "EUPHORIA": "info"}.get(level, "secondary")
            regime_history_rows += f"<tr><td>{ts}</td><td><span class='badge bg-{colour}'>{level}</span></td></tr>"

        # Rates/macro context
        rates_row = _safe_query(DB_HISTORICAL,
            "SELECT * FROM rates_signals ORDER BY calc_date DESC LIMIT 1")
        macro_html = ""
        if rates_row:
            r = rates_row[0]
            macro_html = f"""
<table class="table table-sm">
  <tr><td>Yield Curve Slope</td><td>{r.get('yield_curve_slope', 'N/A')}</td></tr>
  <tr><td>Rates Regime</td><td>{r.get('rates_regime', 'N/A')}</td></tr>
  <tr><td>HY Spread</td><td>{r.get('hy_spread', 'N/A')}</td></tr>
  <tr><td>IG Spread</td><td>{r.get('ig_spread', 'N/A')}</td></tr>
  <tr><td>Credit Stress</td><td>{r.get('credit_stress_level', 'N/A')}</td></tr>
</table>"""

        halt_badge = '<span class="badge bg-danger">HALT ACTIVE ⚠️</span>' if halt else '<span class="badge bg-success">No Halt</span>'

        content = f"""
<div class="row g-3 mb-3">
  <div class="col-md-4">
    <div class="card p-3 text-center">
      <div class="fs-1 fw-bold">{current_regime}</div>
      <div class="text-muted">Current Regime | {confidence:.0f}% confidence</div>
      <div class="mt-2">{halt_badge}</div>
      <div class="mt-2 text-info">Sizing multiplier: {mult}x</div>
    </div>
  </div>
  <div class="col-md-4">
    <div class="card p-3">
      <div class="card-header mb-2"><strong>🎲 Regime Probabilities</strong></div>
      <canvas id="regimePie" height="200"></canvas>
    </div>
  </div>
  <div class="col-md-4">
    <div class="card p-3">
      <div class="card-header mb-2"><strong>📊 Macro Indicators</strong></div>
      {macro_html or '<p class="text-muted">No macro data available</p>'}
    </div>
  </div>
</div>

<div class="row g-3">
  <div class="col-md-6">
    <div class="card">
      <div class="card-header"><strong>📅 Regime History (30d)</strong></div>
      <div style="max-height:300px;overflow-y:auto">
        <table class="table table-sm mb-0">
          <thead style="background:#0f3460"><tr><th>Timestamp</th><th>Regime</th></tr></thead>
          <tbody>{regime_history_rows or '<tr><td colspan="2" class="text-muted text-center">No history</td></tr>'}</tbody>
        </table>
      </div>
    </div>
  </div>
</div>
"""
        scripts = f"""
<script>
new Chart(document.getElementById('regimePie'), {{
  type: 'pie',
  data: {{
    labels: ['Crisis', 'Bear', 'Neutral', 'Bull', 'Euphoria'],
    datasets: [{{
      data: {json.dumps([round(p, 3) for p in probs])},
      backgroundColor: ['#b71c1c', '#ff6b6b', '#9e9e9e', '#00e5ff', '#00c853'],
    }}]
  }},
  options: {{
    responsive: true,
    plugins: {{
      legend: {{ labels: {{ color: '#e0e0e0' }} }}
    }}
  }}
}});
</script>"""
        return _base("Regime", content, scripts, "regime")

    return app


def start_dashboard():
    """Start the Flask dashboard. Called as a daemon thread from main.py."""
    try:
        cfg = _load_config()
        dashboard_cfg = cfg.get("dashboard", {})
        port = int(dashboard_cfg.get("port", 8080))
        host = dashboard_cfg.get("host", "0.0.0.0")

        app = create_app()
        logger.info(f"Dashboard starting on {host}:{port}")
        app.run(host=host, port=port, debug=False, use_reloader=False, threaded=True)
    except Exception as e:
        logger.error(f"Dashboard failed to start: {e}")
