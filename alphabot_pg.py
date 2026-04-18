
#!/usr/bin/env python3
"""
alphabot_pg.py — Base de données AlphaBot PRO + Analyse IA Claude
• PostgreSQL si DATABASE_URL est défini (Render / VPS)
• SQLite en fallback automatique (local / dev)
• Claude AI pour analyse signaux et rapports automatiques
"""
import os, json, threading, time, requests
from datetime import datetime, timezone, timedelta

# ── Backend auto-détecté ─────────────────────────────────────────────
_DATABASE_URL = os.getenv("DATABASE_URL", "")
_USE_PG       = bool(_DATABASE_URL)
_db_lock      = threading.Lock()

if _USE_PG:
    import psycopg2
    import psycopg2.extras
    _conn = psycopg2.connect(_DATABASE_URL, sslmode="require")
    _conn.autocommit = True
    _dbl  = _conn
else:
    import sqlite3
    _DB_PATH = os.getenv("DB_FILE", "ab10.db")
    _conn    = sqlite3.connect(_DB_PATH, check_same_thread=False)
    _conn.row_factory = sqlite3.Row
    _dbl     = _conn

# ── Claude AI Config ─────────────────────────────────────────────────
CLAUDE_API_KEY = os.getenv("ANTHROPIC_API_KEY", "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")
CLAUDE_MODEL   = "claude-sonnet-4-20250514"
CLAUDE_URL     = "https://api.anthropic.com/v1/messages"

def claude_analyze(prompt: str, max_tokens: int = 1000) -> str:
    """Appel Claude API pour analyse IA."""
    if not CLAUDE_API_KEY:
        return "⚠️ ANTHROPIC_API_KEY non configurée."
    try:
        headers = {
            "x-api-key": CLAUDE_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        }
        payload = {
            "model": CLAUDE_MODEL,
            "max_tokens": max_tokens,
            "messages": [{"role": "user", "content": prompt}],
        }
        r = requests.post(CLAUDE_URL, headers=headers, json=payload, timeout=30)
        r.raise_for_status()
        data = r.json()
        return data["content"][0]["text"].strip()
    except Exception as e:
        return f"⚠️ Erreur Claude : {e}"

# ── Helpers bas niveau ───────────────────────────────────────────────
def _cursor():
    if _USE_PG:
        return _conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return _conn.cursor()

def db_run(sql, params=()):
    with _db_lock:
        try:
            cur = _cursor()
            cur.execute(sql, params)
            if not _USE_PG:
                _conn.commit()
            cur.close()
        except Exception as e:
            print("[DB] db_run error:", e)

def db_one(sql, params=()):
    with _db_lock:
        try:
            cur = _cursor()
            cur.execute(sql, params)
            row = cur.fetchone()
            cur.close()
            return dict(row) if row else None
        except Exception as e:
            print("[DB] db_one error:", e)
            return None

def db_all(sql, params=()):
    with _db_lock:
        try:
            cur = _cursor()
            cur.execute(sql, params)
            rows = cur.fetchall()
            cur.close()
            return [dict(r) for r in rows] if rows else []
        except Exception as e:
            print("[DB] db_all error:", e)
            return []

def _ph(n=1):
    """Placeholder : %s pour PG, ? pour SQLite"""
    return "%s" if _USE_PG else "?"

P = _ph()

# ── Init tables ──────────────────────────────────────────────────────
def db_init():
    stmts = [
        """CREATE TABLE IF NOT EXISTS users (
            uid        BIGINT PRIMARY KEY,
            username   TEXT,
            plan       TEXT DEFAULT 'FREE',
            pro_until  TEXT,
            ref_by     BIGINT,
            joined     TEXT,
            sig_today  INTEGER DEFAULT 0,
            sig_date   TEXT,
            active     INTEGER DEFAULT 1
        )""",
        """CREATE TABLE IF NOT EXISTS payments (
            id         SERIAL PRIMARY KEY,
            uid        BIGINT,
            amount     REAL,
            txid       TEXT,
            status     TEXT DEFAULT 'PENDING',
            created    TEXT
        )""" if _USE_PG else
        """CREATE TABLE IF NOT EXISTS payments (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            uid        BIGINT,
            amount     REAL,
            txid       TEXT,
            status     TEXT DEFAULT 'PENDING',
            created    TEXT
        )""",
        """CREATE TABLE IF NOT EXISTS signals (
            id         SERIAL PRIMARY KEY,
            uid        BIGINT,
            pair       TEXT,
            side       TEXT,
            entry      REAL,
            sl         REAL,
            tp         REAL,
            score      INTEGER,
            sent_at    TEXT,
            result     TEXT DEFAULT 'OPEN',
            pnl        REAL DEFAULT 0,
            close_price REAL DEFAULT 0,
            rr_ratio   REAL DEFAULT 0,
            ai_comment TEXT
        )""" if _USE_PG else
        """CREATE TABLE IF NOT EXISTS signals (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            uid        BIGINT,
            pair       TEXT,
            side       TEXT,
            entry      REAL,
            sl         REAL,
            tp         REAL,
            score      INTEGER,
            sent_at    TEXT,
            result     TEXT DEFAULT 'OPEN',
            pnl        REAL DEFAULT 0,
            close_price REAL DEFAULT 0,
            rr_ratio   REAL DEFAULT 0,
            ai_comment TEXT
        )""",
        """CREATE TABLE IF NOT EXISTS challenge (
            id         INTEGER PRIMARY KEY DEFAULT 1,
            balance    REAL DEFAULT 5.0,
            peak       REAL DEFAULT 5.0,
            trades     INTEGER DEFAULT 0,
            wins       INTEGER DEFAULT 0,
            losses     INTEGER DEFAULT 0,
            updated    TEXT
        )""",
        """CREATE TABLE IF NOT EXISTS reports (
            id         SERIAL PRIMARY KEY,
            rtype      TEXT,
            date_str   TEXT,
            sent       INTEGER DEFAULT 0
        )""" if _USE_PG else
        """CREATE TABLE IF NOT EXISTS reports (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            rtype      TEXT,
            date_str   TEXT,
            sent       INTEGER DEFAULT 0
        )""",
        """CREATE TABLE IF NOT EXISTS ai_memory (
            setup_key  TEXT PRIMARY KEY,
            wins       INTEGER DEFAULT 0,
            losses     INTEGER DEFAULT 0,
            pnl_total  REAL DEFAULT 0,
            updated    TEXT
        )""",
    ]
    for s in stmts:
        db_run(s)
    # Colonnes supplémentaires si migration depuis ancienne version
    for col_sql in [
        "ALTER TABLE signals ADD COLUMN IF NOT EXISTS close_price REAL DEFAULT 0",
        "ALTER TABLE signals ADD COLUMN IF NOT EXISTS rr_ratio REAL DEFAULT 0",
        "ALTER TABLE signals ADD COLUMN IF NOT EXISTS ai_comment TEXT",
    ]:
        try:
            db_run(col_sql)
        except Exception:
            pass
    # Seed challenge si vide
    if not db_one("SELECT id FROM challenge WHERE id=1"):
        db_run(
            "INSERT INTO challenge (id,balance,peak,trades,wins,losses,updated) VALUES (1,5.0,5.0,0,0,0,{})".format(P),
            (datetime.now(timezone.utc).isoformat(),)
        )

# ── Utilisateurs ────────────────────────────────────────────────────
def db_register(uid, username=""):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if _USE_PG:
        db_run("""INSERT INTO users (uid,username,plan,joined,sig_today,sig_date,active)
                  VALUES (%s,%s,'FREE',%s,0,%s,1)
                  ON CONFLICT (uid) DO UPDATE SET username=EXCLUDED.username, active=1""",
               (uid, username, now, now))
    else:
        db_run("""INSERT OR IGNORE INTO users (uid,username,plan,joined,sig_today,sig_date,active)
                  VALUES (?,?,'FREE',?,0,?,1)""", (uid, username, now, now))
        db_run("UPDATE users SET username=?, active=1 WHERE uid=?", (username, uid))

def db_pro(uid, txid="MANUAL", days=30):
    if days is None:
        until = "9999-12-31"
    else:
        until = (datetime.now(timezone.utc) + timedelta(days=days)).strftime("%Y-%m-%d")
    db_run("UPDATE users SET plan='PRO', pro_until={0} WHERE uid={0}".format(P), (until, uid))

def db_free(uid):
    db_run("UPDATE users SET plan='FREE', pro_until=NULL WHERE uid={}".format(P), (uid,))

def db_downgrade_pro(uid):
    db_free(uid)

def db_activate_pro(uid, days=30):
    db_pro(uid, days=days)

def is_pro(uid):
    row = db_one("SELECT plan, pro_until FROM users WHERE uid={}".format(P), (uid,))
    if not row: return False
    if row["plan"] != "PRO": return False
    until = row.get("pro_until")
    if not until or until == "9999-12-31": return True
    try:
        return datetime.now(timezone.utc).date() <= datetime.strptime(until, "%Y-%m-%d").date()
    except:
        return False

def get_plan(uid):
    row = db_one("SELECT plan FROM users WHERE uid={}".format(P), (uid,))
    return row["plan"] if row else "FREE"

def get_pro_info(uid):
    return db_one("SELECT * FROM users WHERE uid={}".format(P), (uid,))

db_get_pro_info = get_pro_info

def get_refs(uid):
    rows = db_all("SELECT uid, username FROM users WHERE ref_by={}".format(P), (uid,))
    return rows

db_get_refs = get_refs

def pro_users():
    return db_all("SELECT * FROM users WHERE plan='PRO'")

def free_users():
    return db_all("SELECT * FROM users WHERE plan='FREE'")

def all_users():
    return db_all("SELECT * FROM users")

def find_user(uid):
    return db_one("SELECT * FROM users WHERE uid={}".format(P), (uid,))

db_find_by_username = lambda username: db_one(
    "SELECT * FROM users WHERE username={}".format(P), (username,))

def db_get_pro_users():  return pro_users()
def db_get_free_users(): return free_users()

def inactive_users(days=3):
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    return db_all("SELECT * FROM users WHERE joined < {} OR joined IS NULL".format(P), (cutoff,))

db_get_inactive_users = inactive_users

# ── Compteurs signaux ────────────────────────────────────────────────
def count_today(uid):
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    row = db_one("SELECT sig_today, sig_date FROM users WHERE uid={}".format(P), (uid,))
    if not row: return 0
    if row.get("sig_date") != today: return 0
    return row.get("sig_today", 0) or 0

def count_incr(uid):
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    row = db_one("SELECT sig_today, sig_date FROM users WHERE uid={}".format(P), (uid,))
    if not row: return
    if row.get("sig_date") != today:
        db_run("UPDATE users SET sig_today=1, sig_date={0} WHERE uid={0}".format(P), (today, uid))
    else:
        db_run("UPDATE users SET sig_today=sig_today+1 WHERE uid={}".format(P), (uid,))

db_count_increment = count_incr
db_count_today     = count_today

# ── Expiration PRO ───────────────────────────────────────────────────
def check_expiry():
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    rows  = db_all("SELECT uid FROM users WHERE plan='PRO' AND pro_until IS NOT NULL AND pro_until != '9999-12-31'")
    expired = []
    for r in rows:
        row = db_one("SELECT pro_until FROM users WHERE uid={}".format(P), (r["uid"],))
        if row and row.get("pro_until", "9999-12-31") < today:
            db_free(r["uid"])
            expired.append(r["uid"])
    return expired

# ── Signaux & tracking ───────────────────────────────────────────────
def save_signal(uid, sig):
    now = datetime.now(timezone.utc).isoformat()
    db_run("""INSERT INTO signals (uid,pair,side,entry,sl,tp,score,sent_at,result,pnl,close_price,rr_ratio)
              VALUES ({0},{0},{0},{0},{0},{0},{0},{0},'OPEN',0,0,0)""".format(P),
           (uid, sig.get("name","?"), sig.get("side","?"),
            sig.get("entry",0), sig.get("sl",0), sig.get("tp",0),
            sig.get("score",0), now))

def open_signals(uid=None):
    if uid:
        return db_all("SELECT * FROM signals WHERE uid={} AND result='OPEN'".format(P), (uid,))
    return db_all("SELECT * FROM signals WHERE result='OPEN'")

def close_track(sig_id, result, pnl=0):
    db_run("UPDATE signals SET result={0}, pnl={0} WHERE id={0}".format(P),
           (result, pnl, sig_id))

# ── Calcul PnL & RR automatique ─────────────────────────────────────
def compute_rr(entry: float, sl: float, tp: float, close_price: float, side: str) -> dict:
    """
    Calcule le RR atteint et le PnL en % selon prix de clôture.
    side : 'BUY' ou 'SELL'
    Retourne dict: result (TP/SL/OPEN), rr_ratio, pnl_pct
    """
    try:
        entry, sl, tp, close_price = float(entry), float(sl), float(tp), float(close_price)
        risk  = abs(entry - sl)
        if risk == 0:
            return {"result": "OPEN", "rr_ratio": 0, "pnl_pct": 0}
        reward = abs(tp - entry)
        rr_max = reward / risk  # ex: 2.0 = RR2

        if side.upper() == "BUY":
            move = close_price - entry
        else:
            move = entry - close_price

        rr_reached = move / risk  # positif = profit, négatif = perte
        pnl_pct    = (move / entry) * 100

        if close_price >= tp if side.upper() == "BUY" else close_price <= tp:
            result = "WIN"
        elif close_price <= sl if side.upper() == "BUY" else close_price >= sl:
            result = "LOSS"
        else:
            result = "OPEN"

        return {
            "result":   result,
            "rr_ratio": round(rr_reached, 2),
            "rr_max":   round(rr_max, 2),
            "pnl_pct":  round(pnl_pct, 3),
        }
    except Exception as e:
        return {"result": "OPEN", "rr_ratio": 0, "pnl_pct": 0, "error": str(e)}

def close_signal_with_price(sig_id: int, close_price: float) -> dict:
    """
    Ferme un signal à un prix donné, calcule RR/PnL, met à jour la DB,
    génère un commentaire Claude et retourne le rapport complet.
    """
    sig = db_one("SELECT * FROM signals WHERE id={}".format(P), (sig_id,))
    if not sig:
        return {"error": "Signal introuvable"}

    calc = compute_rr(
        sig["entry"], sig["sl"], sig["tp"], close_price, sig.get("side","BUY")
    )

    # Commentaire IA Claude
    ai_comment = claude_comment_signal(sig, calc, close_price)

    # Mise à jour DB
    db_run(
        "UPDATE signals SET result={0}, pnl={0}, close_price={0}, rr_ratio={0}, ai_comment={0} WHERE id={0}".format(P),
        (calc["result"], calc["pnl_pct"], close_price, calc["rr_ratio"], ai_comment, sig_id)
    )
    # Mémoire IA
    setup_key = "{}_{}".format(sig.get("pair","?"), sig.get("side","?"))
    mem_record(setup_key, calc["result"] == "WIN", calc["pnl_pct"])

    return {**calc, "ai_comment": ai_comment, "signal": sig}

def claude_comment_signal(sig: dict, calc: dict, close_price: float) -> str:
    """Claude génère un commentaire court sur le signal fermé."""
    result_label = calc.get("result", "OPEN")
    rr           = calc.get("rr_ratio", 0)
    pnl          = calc.get("pnl_pct", 0)
    emoji_result = "✅" if result_label == "WIN" else ("❌" if result_label == "LOSS" else "⏳")

    prompt = f"""Tu es analyste trading ICT/SMC. Commente ce signal en 2 lignes max, en français, de manière professionnelle.

Signal: {sig.get('pair','?')} {sig.get('side','?')}
Entrée: {sig.get('entry',0)} | SL: {sig.get('sl',0)} | TP: {sig.get('tp',0)}
Prix de clôture: {close_price}
Résultat: {result_label} | RR atteint: {rr} | PnL: {pnl}%

Réponds directement le commentaire, sans intro."""

    return claude_analyze(prompt, max_tokens=150)

# ── Rapports IA journalier / hebdomadaire ────────────────────────────
def generate_daily_report_ai(date_str: str = None) -> str:
    """
    Génère un rapport journalier complet avec analyse Claude.
    Retourne le texte Telegram formaté (Markdown).
    """
    if not date_str:
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    sigs = db_all(
        "SELECT * FROM signals WHERE sent_at LIKE {}".format(P),
        ("%{}%".format(date_str),)
    )
    if not sigs:
        return f"📊 *Rapport du {date_str}*\n\nAucun signal ce jour."

    wins   = [s for s in sigs if s.get("result") == "WIN"]
    losses = [s for s in sigs if s.get("result") == "LOSS"]
    open_s = [s for s in sigs if s.get("result") == "OPEN"]

    total_pnl = sum(s.get("pnl", 0) for s in sigs)
    win_rate  = round(len(wins) / max(len(wins)+len(losses),1) * 100, 1)

    # Détail signaux
    details = []
    for s in sigs:
        r = s.get("result","OPEN")
        rr = s.get("rr_ratio", 0)
        pnl = s.get("pnl", 0)
        emoji = "✅" if r=="WIN" else ("❌" if r=="LOSS" else "⏳")
        rr_label = f"RR{rr:+.1f}" if rr else ""
        details.append(f"  {emoji} {s.get('pair','?')} {s.get('side','?')} {rr_label} | PnL: {pnl:+.2f}%")

    details_str = "\n".join(details)

    # Prompt Claude pour analyse globale
    prompt = f"""Tu es analyste trading senior. Analyse cette journée de trading en 3-4 lignes max, en français.

Date: {date_str}
Signaux: {len(sigs)} | Wins: {len(wins)} | Losses: {len(losses)} | OPEN: {len(open_s)}
Win rate: {win_rate}% | PnL global: {total_pnl:+.2f}%
Paires tradées: {', '.join(set(s.get('pair','?') for s in sigs))}

Pertes (si on avait pris tous les SL): {sum(abs(s.get('pnl',0)) for s in losses):+.2f}%

Donne une analyse concise et professionnelle. Commence directement l'analyse, pas d'intro."""

    ai_analysis = claude_analyze(prompt, max_tokens=300)

    report = f"""📊 *Rapport Journalier — {date_str}*

📈 *Résultats*
• Signaux: `{len(sigs)}` | ✅ `{len(wins)}` | ❌ `{len(losses)}` | ⏳ `{len(open_s)}`
• Win Rate: `{win_rate}%`
• PnL Global: `{total_pnl:+.2f}%`

📋 *Détail Signaux*
{details_str}

🤖 *Analyse IA*
{ai_analysis}"""

    return report

def generate_weekly_report_ai() -> str:
    """
    Génère un rapport hebdomadaire complet avec analyse Claude.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")
    sigs   = db_all("SELECT * FROM signals WHERE sent_at > {}".format(P), (cutoff,))

    if not sigs:
        return "📊 *Rapport Hebdomadaire*\n\nAucun signal cette semaine."

    wins   = [s for s in sigs if s.get("result") == "WIN"]
    losses = [s for s in sigs if s.get("result") == "LOSS"]
    total_pnl = sum(s.get("pnl", 0) for s in sigs)
    win_rate  = round(len(wins) / max(len(wins)+len(losses),1) * 100, 1)

    # Meilleures et pires paires
    paires = {}
    for s in sigs:
        p = s.get("pair","?")
        paires.setdefault(p, {"wins":0,"losses":0,"pnl":0})
        paires[p]["wins"]   += 1 if s.get("result")=="WIN" else 0
        paires[p]["losses"] += 1 if s.get("result")=="LOSS" else 0
        paires[p]["pnl"]    += s.get("pnl",0)

    top_pair  = max(paires, key=lambda p: paires[p]["pnl"]) if paires else "N/A"
    worst_pair = min(paires, key=lambda p: paires[p]["pnl"]) if paires else "N/A"

    prompt = f"""Tu es analyste trading ICT/SMC senior. Fais un bilan hebdomadaire en 4-5 lignes max, en français.

Semaine analysée: 7 derniers jours
Signaux totaux: {len(sigs)} | Wins: {len(wins)} | Losses: {len(losses)}
Win Rate: {win_rate}% | PnL cumulé: {total_pnl:+.2f}%
Meilleure paire: {top_pair} (+{paires.get(top_pair,{}).get('pnl',0):.2f}%)
Pire paire: {worst_pair} ({paires.get(worst_pair,{}).get('pnl',0):.2f}%)

Perte maximale si tous les SL pris: {sum(abs(s.get('pnl',0)) for s in losses):.2f}%

Inclus: bilan, points forts, axes d'amélioration. Pas d'intro."""

    ai_analysis = claude_analyze(prompt, max_tokens=400)

    report = f"""📊 *Rapport Hebdomadaire AlphaBot*

📈 *Performance 7 jours*
• Signaux: `{len(sigs)}` | ✅ `{len(wins)}` | ❌ `{len(losses)}`
• Win Rate: `{win_rate}%`
• PnL Cumulé: `{total_pnl:+.2f}%`
• 🏆 Top paire: `{top_pair}` | ⚠️ Pire: `{worst_pair}`

🤖 *Analyse IA Claude*
{ai_analysis}"""

    return report

def get_signal_report_text(sig_id: int, close_price: float) -> str:
    """
    Rapport Telegram formaté pour un signal fermé.
    Appelle close_signal_with_price et formate le message.
    """
    data = close_signal_with_price(sig_id, close_price)
    if "error" in data:
        return f"⚠️ {data['error']}"

    sig    = data.get("signal", {})
    result = data.get("result", "OPEN")
    rr     = data.get("rr_ratio", 0)
    pnl    = data.get("pnl_pct", 0)
    rr_max = data.get("rr_max", 0)
    ai_txt = data.get("ai_comment", "")

    emoji  = "✅ TAKE PROFIT" if result == "WIN" else ("❌ STOP LOSS" if result == "LOSS" else "⏳ OPEN")

    msg = f"""{emoji}

📌 *{sig.get('pair','?')} — {sig.get('side','?')}*
• Entrée: `{sig.get('entry',0)}`
• SL: `{sig.get('sl',0)}` | TP: `{sig.get('tp',0)}`
• Clôture: `{close_price}`

📊 *Performance*
• RR atteint: `{rr:+.2f}` / RR max: `{rr_max:.2f}`
• PnL: `{pnl:+.3f}%`

🤖 *Analyse IA*
{ai_txt}"""

    return msg

# ── Statistiques ─────────────────────────────────────────────────────
def daily_stats(date_str=None):
    if not date_str:
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    sigs = db_all("SELECT * FROM signals WHERE sent_at LIKE {}".format(P),
                  ("%{}%".format(date_str),))
    wins   = sum(1 for s in sigs if s.get("result") == "WIN")
    losses = sum(1 for s in sigs if s.get("result") == "LOSS")
    return {"date": date_str, "signals": len(sigs), "wins": wins, "losses": losses}

db_daily_stats = daily_stats

def weekly_stats():
    cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")
    sigs   = db_all("SELECT * FROM signals WHERE sent_at > {}".format(P), (cutoff,))
    wins   = sum(1 for s in sigs if s.get("result") == "WIN")
    losses = sum(1 for s in sigs if s.get("result") == "LOSS")
    return {"signals": len(sigs), "wins": wins, "losses": losses}

db_weekly_stats = weekly_stats

def global_stats():
    total  = db_one("SELECT COUNT(*) as c FROM users") or {}
    pro    = db_one("SELECT COUNT(*) as c FROM users WHERE plan='PRO'") or {}
    sigs   = db_one("SELECT COUNT(*) as c FROM signals") or {}
    pays   = db_one("SELECT COUNT(*) as c FROM payments WHERE status='CONFIRMED'") or {}
    today  = db_one("SELECT COUNT(*) as c FROM signals WHERE sent_at LIKE {}".format(P),
                    ("%{}%".format(datetime.now(timezone.utc).strftime("%Y-%m-%d")),)) or {}
    return (total.get("c",0), pro.get("c",0), sigs.get("c",0),
            pays.get("c",0), today.get("c",0))

db_global_stats = global_stats

def rep_sent(rtype, date_str):
    row = db_one("SELECT sent FROM reports WHERE rtype={0} AND date_str={0}".format(P),
                 (rtype, date_str))
    return bool(row and row.get("sent"))

def mark_rep(rtype, date_str):
    if _USE_PG:
        db_run("""INSERT INTO reports (rtype, date_str, sent) VALUES (%s,%s,1)
                  ON CONFLICT DO NOTHING""", (rtype, date_str))
    else:
        db_run("INSERT OR IGNORE INTO reports (rtype, date_str, sent) VALUES (?,?,1)",
               (rtype, date_str))

# ── Paiements ────────────────────────────────────────────────────────
def save_pay(uid, amount, txid, status="PENDING"):
    now = datetime.now(timezone.utc).isoformat()
    db_run("INSERT INTO payments (uid,amount,txid,status,created) VALUES ({0},{0},{0},{0},{0})".format(P),
           (uid, amount, txid, status, now))

def pending_pays():
    return db_all("SELECT * FROM payments WHERE status='PENDING'")

db_save_payment     = save_pay
db_pending_payments = pending_pays

# ── Challenge ────────────────────────────────────────────────────────
def chal_get():
    row = db_one("SELECT * FROM challenge WHERE id=1")
    if not row:
        return {"balance": 5.0, "peak": 5.0, "trades": 0, "wins": 0, "losses": 0}
    return dict(row)

def chal_save(data):
    data["updated"] = datetime.now(timezone.utc).isoformat()
    db_run("""UPDATE challenge SET balance={0},peak={0},trades={0},
              wins={0},losses={0},updated={0} WHERE id=1""".format(P),
           (data.get("balance",5.0), data.get("peak",5.0),
            data.get("trades",0),   data.get("wins",0),
            data.get("losses",0),   data["updated"]))

# ── Mémoire IA ───────────────────────────────────────────────────────
def mem_query(setup_key):
    row = db_one("SELECT wins, losses, pnl_total FROM ai_memory WHERE setup_key={}".format(P),
                 (setup_key,))
    if not row: return 0, 0, 0.0
    return row.get("wins",0), row.get("losses",0), row.get("pnl_total",0.0)

def mem_record(setup_key, win: bool, pnl=0.0):
    now = datetime.now(timezone.utc).isoformat()
    w, l, pt = mem_query(setup_key)
    if w == 0 and l == 0:
        db_run("""INSERT INTO ai_memory (setup_key,wins,losses,pnl_total,updated)
                  VALUES ({0},{0},{0},{0},{0})""".format(P),
               (setup_key, 1 if win else 0, 0 if win else 1, pnl, now))
    else:
        wins_new = w + (1 if win else 0)
        loss_new = l + (0 if win else 1)
        db_run("""UPDATE ai_memory SET wins={0},losses={0},pnl_total={0},updated={0}
                  WHERE setup_key={0}""".format(P),
               (wins_new, loss_new, pt + pnl, now, setup_key))

def best_setups(n=5):
    rows = db_all("""SELECT setup_key, wins, losses, pnl_total FROM ai_memory
                     WHERE wins+losses >= 3
                     ORDER BY (wins * 1.0 / (wins+losses+1)) DESC""")
    return rows[:n]

def worst_setups(n=5):
    rows = db_all("""SELECT setup_key, wins, losses, pnl_total FROM ai_memory
                     WHERE wins+losses >= 3
                     ORDER BY (wins * 1.0 / (wins+losses+1)) ASC""")
    return rows[:n]

# ── Migration SQLite → PostgreSQL ────────────────────────────────────
def migrate_sqlite_to_pg(sqlite_path="ab10.db"):
    if not _USE_PG:
        print("[MIGRATE] Pas de DATABASE_URL — migration ignorée")
        return
    try:
        import sqlite3 as _sl
        src = _sl.connect(sqlite_path)
        src.row_factory = _sl.Row
        for table in ["users", "payments", "signals", "challenge", "reports", "ai_memory"]:
            try:
                rows = src.execute("SELECT * FROM {}".format(table)).fetchall()
                for r in rows:
                    d = dict(r)
                    cols = ", ".join(d.keys())
                    vals = ", ".join(["%s"] * len(d))
                    db_run("INSERT INTO {} ({}) VALUES ({}) ON CONFLICT DO NOTHING".format(
                        table, cols, vals), tuple(d.values()))
                print("[MIGRATE] {} : {} lignes migrées".format(table, len(rows)))
            except Exception as e:
                print("[MIGRATE] {} ignoré : {}".format(table, e))
        src.close()
        print("[MIGRATE] ✅ Migration SQLite → PostgreSQL terminée")
    except Exception as e:
        print("[MIGRATE] Erreur : {}".format(e))
