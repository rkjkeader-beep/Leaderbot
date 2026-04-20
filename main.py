
#!/usr/bin/env python3
"""
AlphaBot PRO v20 — Agent IA Adaptatif + Validateur Dual-AI + Challenge IA
═══════════════════════════════════════════════════════════════════════════
• Bot Telegram FREE/PRO/VIP + paiement USDT auto
• 18 marchés : Forex · Or/Argent · BTC · NAS100/SPX500/US30 · Pétrole
• Cerveau ICT/SMC v2 + Analyse Multi-Timeframe (M1+M5+M15+H1)
• Tendance de fond : H1 (interne) | Entrée : M5 max M15
• Si pas de setup parfait → l'agent allège les critères
  si tendance de fond + session + broker sont valides
• Challenge IA : simulation Binance Futures auto (BTC + Top altcoins)
• Validateur Dual-AI (Claude Sonnet + Gemini) :
    – Algo ICT/SMC  → Analyste technique (score /100)
    – Claude + Gemini → Risk Manager (technique + fondamentaux + news)
    – Script        → Juge final (score hybride ≥ 75/100)
    – Modes : auto | claude | gemini | both
• pip install requests anthropic google-generativeai
"""
import json, ssl, time, threading, math, random, logging
import urllib.request, urllib.parse, urllib.error, os
from datetime import datetime, timedelta, timezone
from queue import Queue, Empty
from http.server import HTTPServer, BaseHTTPRequestHandler
from collections import defaultdict, deque

# ── Anthropic SDK (Claude AI Validator) ─────────────────────────────
try:
    import anthropic as _anthropic_sdk
    _ANTHROPIC_OK = True
except ImportError:
    _ANTHROPIC_OK = False
    print("[ClaudeAI] ⚠️  pip install anthropic requis pour la validation IA")

# ── Google Gemini SDK (Validateur alternatif) ────────────────────────
try:
    import google.genai as _genai_sdk
    _GEMINI_OK = True
except ImportError:
    _GEMINI_OK = False
    print("[GeminiAI] ⚠️  pip install google-genai pour le fallback Gemini")

# ══════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════
TG_TOKEN     = os.getenv("TG_TOKEN",  "6950706659:AAGXw-27ebhWLm2HfG7lzC7EckpwCPS_JFg")
BOT_USER     = "leaderodg_bot"
CHANNEL_ID   = os.getenv("TG_GROUP", "-1003757467015")
VIP_CH       = os.getenv("TG_VIP",   "-1003771736496")
ADMIN_ID     = int(os.getenv("ADMIN_ID", "6982051442"))
USDT_ADDR    = "TJuPBihvzgb6ffGLw4WnqC33Av38kwU7XE"
BROKER_LINK  = "https://one.exnessonelink.com/a/nb3fx0bpnm"
DB_FILE      = "ab10.db"
BINANCE_BASE = "https://fapi.binance.com/fapi/v1"

# ── Liens d'invitation groupes (à mettre à jour si lien change) ─
FREE_GROUP_LINK = os.getenv("FREE_GROUP_LINK", "https://t.me/+alphabotfree")   # ← remplace par ton vrai lien groupe FREE
VIP_GROUP_LINK  = os.getenv("VIP_GROUP_LINK",  "https://t.me/+alphabotvip")    # ← remplace par ton vrai lien groupe VIP

# ══════════════════════════════════════════════════════════════════════
#  MODULE CLAUDE AI — VALIDATEUR EXPERT ICT/SMC
# ══════════════════════════════════════════════════════════════════════
CLAUDE_API_KEY   = os.getenv("ANTHROPIC_API_KEY", "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")
CLAUDE_MODEL     = "claude-sonnet-4-5-20250514"   # ✨ Sonnet — meilleure qualité d'analyse
CLAUDE_TOKENS    = 600
GEMINI_API_KEY   = os.getenv("GEMINI_API_KEY", "")  # Optionnel — fallback auto
GEMINI_MODEL     = "gemini-2.0-flash"
# AI_VALIDATOR : auto = Claude prioritaire → Gemini fallback si Claude échoue
#   claude = Claude seul | gemini = Gemini seul | both = vote majoritaire
AI_VALIDATOR     = os.getenv("AI_VALIDATOR", "auto")
AI_SCORE_MIN     = 7.0
AI_PROBA_MIN     = 55.0
FINAL_HYBRID_MIN = 75.0
AI_WEIGHT        = 0.40
ALGO_WEIGHT      = 0.60
_ai_cache     = {}
_ai_cache_ttl = 300
_ai_lock      = threading.Lock()
_LAI          = logging.getLogger("ClaudeAI")


def _claude_session_risk(session: str) -> str:
    """Détecte conflits session autour des ouvertures/clôtures."""
    h = datetime.now(timezone.utc).hour
    m = datetime.now(timezone.utc).minute
    warns = []
    if h == 11 and m >= 30:
        warns.append("⚠️ Clôture London dans {}min".format(90 - m))
    if h == 13 and m >= 20:
        warns.append("⚠️ Ouverture NY dans {}min".format(90 - m))
    if h == 14 and m <= 30:
        warns.append("⚠️ Ouverture NY en cours — volatilité extrême")
    if h == 15 and m >= 45:
        warns.append("⚠️ Fixing London dans {}min".format(75 - m))
    if h == 16 and m <= 15:
        warns.append("⚠️ Fixing London — reversal institutionnel possible")
    if h == 21 and m >= 30:
        warns.append("⚠️ Clôture NY dans {}min".format(90 - m))
    if h == 22:
        warns.append("⚠️ Zone rollover — spread élargi")
    if datetime.now(timezone.utc).weekday() == 0 and h < 7:
        warns.append("⚠️ Lundi matin — liquidité faible")
    return "\n".join(warns) if warns else "✅ Timing propre — aucun conflit"


def _claude_build_prompt(sig: dict, session: str, htf_trend: str) -> str:
    """
    Prompt Claude v22 — Validation TECHNIQUE + FONDAMENTALE + TIMING.
    Claude valide les deux aspects en une seule passe.
    """
    now_utc   = datetime.now(timezone.utc)
    now_str   = now_utc.strftime("%H:%M UTC")
    now_day   = now_utc.strftime("%A")
    now_h     = now_utc.hour

    pair_name  = sig.get("name", "?")
    side_fr    = "ACHAT (LONG)" if sig.get("side") == "BUY" else "VENTE (SHORT)"
    entry      = sig.get("entry", "?")
    sl_v       = sig.get("sl", "?")
    tp         = sig.get("tp", "?")

    try:
        risk_amt = abs(float(entry) - float(sl_v))
        tp1_dist = abs(float(tp) - float(entry))
        rr       = round(tp1_dist / risk_amt, 1) if risk_amt else "?"
        tp2      = round(float(entry) + risk_amt * 4.5, 5) if sig.get("side") == "BUY" \
                   else round(float(entry) - risk_amt * 4.5, 5)
        tp3      = round(float(entry) + risk_amt * 6.0, 5) if sig.get("side") == "BUY" \
                   else round(float(entry) - risk_amt * 6.0, 5)
        atr_val  = float(sig.get("atr", risk_amt * 2) or risk_amt * 2)
        candles_to_tp = round(tp1_dist / atr_val) if atr_val else "?"
        mins_to_tp    = candles_to_tp * 15 if isinstance(candles_to_tp, (int, float)) else "?"
    except Exception:
        rr = tp2 = tp3 = "?"
        mins_to_tp = "?"
        candles_to_tp = "?"

    # ── Section fondamentale ─────────────────────────────────────────
    cat        = sig.get("cat", "?")
    currencies = CURRENCY_MAP.get(pair_name, (None, None))
    base_cur   = currencies[0] if currencies and currencies[0] else "N/A"
    quote_cur  = currencies[1] if currencies and currencies[1] else "N/A"
    fund_base  = sig.get("fund_base", 0)
    fund_quote = sig.get("fund_quote", 0)
    fund_bias  = sig.get("fund_bias", "NEUTRAL")
    fund_badge = sig.get("fund_badge", "")

    if fund_bias == "BASE_STRONG":
        fund_direction = "{} fort — favorise BEARISH {}/{}".format(base_cur, base_cur, quote_cur)
    elif fund_bias == "QUOTE_STRONG":
        fund_direction = "{} fort — favorise BULLISH {}/{}".format(quote_cur, base_cur, quote_cur)
    else:
        fund_direction = "Neutre — pas de biais macro dominant"

    bias_tech = sig.get("bias", "NEUTRAL")
    if fund_bias != "NEUTRAL":
        tech_fund_align = "✅ ALIGNÉ" if (
            (fund_bias == "QUOTE_STRONG" and bias_tech == "BULLISH") or
            (fund_bias == "BASE_STRONG"  and bias_tech == "BEARISH")
        ) else "⚠️ CONTRADICTION tech/macro"
    else:
        tech_fund_align = "➖ Neutre"

    # ── Données techniques ICT/SMC ───────────────────────────────────
    score_algo = sig.get("score", 0)
    badges     = sig.get("badges", "Aucun")
    mode       = sig.get("mode", "ICT/SMC")

    # ── News haute importance ────────────────────────────────────────
    news_lines = []
    try:
        news_data   = _get_news_data() or []
        now_utc_dt  = datetime.now(timezone.utc).replace(tzinfo=None)
        for evt in news_data:
            if evt.get("impact","") != "High": continue
            cur = evt.get("currency","")
            if cur not in (base_cur, quote_cur, "USD"): continue
            try:
                et      = datetime.strptime(evt["date"], "%Y-%m-%dT%H:%M:%S%z").replace(tzinfo=None)
                diff_h  = (et - now_utc_dt).total_seconds() / 3600
                if -2 <= diff_h <= 24:
                    sign = "🔴" if diff_h < 0 else ("🟡" if diff_h < 2 else "⚪")
                    news_lines.append("  {} {} | {} | dans {:.0f}h".format(
                        sign, cur, evt.get("title","?")[:40], diff_h))
            except: pass
    except: pass

    news_block   = "\n".join(news_lines) if news_lines else "  ✅ Aucune news HIGH dans les 24h"
    news_status  = sig.get("news_status", "OK")
    news_title   = sig.get("news_title", "")
    news_status_str = {
        "BLOCK":   "🔴 BLOQUÉ — news HIGH dans < 30min",
        "CAUTION": "🟡 PRUDENCE — news dans < 2h : {}".format(news_title[:30]),
        "OK":      "✅ Pas de news bloquante immédiate",
    }.get(news_status, "✅ OK")

    # ── Contexte heure/jour ──────────────────────────────────────────
    in_killzone = (8 <= now_h <= 10) or (13 <= now_h <= 15)
    kz_str      = "✅ Dans kill zone institutionnelle" if in_killzone else "⚠️ Hors kill zone"
    weekend     = now_utc.weekday() >= 4
    day_ok_str  = "⚠️ Fin de semaine — liquidité réduite" if weekend else "✅ Jour de trading actif"

    return """Tu es analyste senior d'un fonds institutionnel. Tu valides SIMULTANÉMENT la qualité technique ET fondamentale du setup.

══ SETUP ICT/SMC ══════════════════════════════════════
Instrument  : {pair} ({cat}) | {side}
Session     : {session} | Biais HTF : {htf}
Entrée      : {entry}  SL : {sl}  TP1 : {tp}
RR          : 1:{rr}   TP2 : {tp2}   TP3 : {tp3}
ATR M15     : {atr}
Score algo  : {score}/100  |  Stratégie : {mode}
Badges ICT  : {badges}
Temps TP1   : ~{mins_to_tp} min ({candles} bougies M15)

══ 1. TECHNIQUE ICT/SMC ═══════════════════════════════
Score algo  : {score}/100 (seuil min : 58)
Biais HTF   : {htf}
Stratégie   : {mode}
Badges      : {badges}
RR minimum  : 2.0 requis — actuel : 1:{rr}

══ 2. FONDAMENTAUX ════════════════════════════════════
Paire       : {base_cur} / {quote_cur}
Score {base_cur} : {fund_base} pts   Score {quote_cur} : {fund_quote} pts
Biais macro : {fund_direction}
Alignement  : {tech_fund_align}
Badge macro : {fund_badge}

══ 3. NEWS HAUTE IMPORTANCE ═══════════════════════════
Statut      : {news_status_str}
{news_block}

══ 4. CONTEXTE HEURE / JOUR ═══════════════════════════
Heure UTC   : {heure} ({day})
Kill zone   : {kz_str}
Jour        : {day_ok_str}

══ 5. TP ATTEIGNABLE ? ════════════════════════════════
TP1 distance: {tp1_dist_pct:.3f}% de l'entrée
Volatilité  : ATR = {atr} (~{candles} bougies M15)

══ RÈGLE DE VALIDATION ════════════════════════════════
VALIDER si TOUS ces points sont verts :
  ✅ Score algo ≥ 58/100
  ✅ RR ≥ 2.0
  ✅ Biais HTF aligné avec le trade
  ✅ Fondamentaux alignés ou neutres (pas CONTRE)
  ✅ Aucune news bloquante dans les 2h
  ✅ Heure et jour favorables
  ✅ TP1 atteignable dans la session (<4h estimé)

REJETER si l'un de ces points est rouge.

Réponds UNIQUEMENT avec ce JSON exact :
{{
  "verdict": "VALIDER" ou "REJETER",
  "technique_ok": true ou false,
  "news_impact": "OK" ou "PRUDENCE" ou "BLOQUANT",
  "biais_fondamental": "ALIGNE" ou "NEUTRE" ou "CONTRE",
  "timing_ok": true ou false,
  "tp_atteignable": true ou false,
  "raison": "<explication 1-2 phrases>",
  "risque_principal": "<le risque #1 en une phrase>"
}}""".format(
        pair=pair_name, cat=cat, side=side_fr, session=session, htf=htf_trend,
        entry=entry, sl=sl_v, tp=tp, rr=rr, tp2=tp2, tp3=tp3,
        atr=sig.get("atr","?"),
        mins_to_tp=mins_to_tp,
        candles=candles_to_tp,
        score=score_algo, mode=mode, badges=badges,
        tp1_dist_pct=abs(float(tp)-float(entry))/float(entry)*100 if entry and tp and entry != "?" and tp != "?" else 0,
        base_cur=base_cur, quote_cur=quote_cur,
        fund_base=fund_base, fund_quote=fund_quote,
        fund_direction=fund_direction, tech_fund_align=tech_fund_align,
        fund_badge=fund_badge or "Aucun",
        news_status_str=news_status_str, news_block=news_block,
        heure=now_str, day=now_day,
        kz_str=kz_str, day_ok_str=day_ok_str)


def _claude_call(prompt: str) -> dict | None:
    """Appelle l'API Claude et retourne le JSON parsé."""
    if not _ANTHROPIC_OK or not CLAUDE_API_KEY:
        return None
    raw = ""
    try:
        client = _anthropic_sdk.Anthropic(api_key=CLAUDE_API_KEY)
        resp   = client.messages.create(
            model=CLAUDE_MODEL, max_tokens=CLAUDE_TOKENS,
            messages=[{"role": "user", "content": prompt}])
        raw = resp.content[0].text.strip()
        if "```" in raw:
            raw = raw.split("```")[1]
            if raw.startswith("json"): raw = raw[4:]
        # fallback regex si la réponse ne commence pas par {
        if not raw.strip().startswith("{"):
            import re as _re
            m = _re.search(r"\{[\s\S]*\}", raw)
            if m: raw = m.group(0)
        return json.loads(raw.strip())
    except json.JSONDecodeError as e:
        _LAI.error("Claude JSON parse: {} | raw={}".format(e, raw[:200]))
        return None
    except Exception as e:
        _LAI.error("Claude API: {}".format(e))
        return None


def _gemini_call(prompt: str) -> dict | None:
    """Appelle l'API Gemini et retourne le JSON parsé (même format que _claude_call)."""
    if not _GEMINI_OK or not GEMINI_API_KEY:
        return None
    try:
        client = _genai_sdk.Client(api_key=GEMINI_API_KEY)
        resp   = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt,
            config=_genai_sdk.types.GenerateContentConfig(
                response_mime_type="application/json"
            )
        )
        raw = resp.text.strip()
        # nettoyage défensif au cas où Gemini ajouterait quand même du texte
        if "```" in raw:
            raw = raw.split("```")[1]
            if raw.startswith("json"): raw = raw[4:]
        # extraction JSON par regex en dernier recours
        if not raw.startswith("{"):
            import re as _re
            m = _re.search(r'\{[\s\S]*\}', raw)
            if m: raw = m.group(0)
        return json.loads(raw.strip())
    except json.JSONDecodeError as e:
        _LAI.error("Gemini JSON parse: {} | raw={}".format(e, raw[:200]))
        return None
    except Exception as e:
        _LAI.error("Gemini API: {}".format(e))
        return None


def _ai_call_with_fallback(prompt: str) -> tuple[dict | None, str]:
    """
    Dispatcher intelligent selon AI_VALIDATOR.
    Retourne (parsed_dict, source) où source ∈ {"claude","gemini","both","none"}.

    Modes :
      auto   → Claude d'abord ; si échec → Gemini
      claude → Claude uniquement
      gemini → Gemini uniquement
      both   → Les deux ; moyenne score + probabilité (vote majoritaire sur verdict)
    """
    mode = AI_VALIDATOR.lower()

    if mode == "claude":
        r = _claude_call(prompt)
        return (r, "claude") if r else (None, "none")

    if mode == "gemini":
        r = _gemini_call(prompt)
        return (r, "gemini") if r else (None, "none")

    if mode == "both":
        rc = _claude_call(prompt)
        rg = _gemini_call(prompt)
        if rc and rg:
            # Moyenne des deux scores
            merged = {
                "score"           : round((float(rc.get("score",0)) + float(rg.get("score",0))) / 2, 1),
                "probabilite"     : round((float(rc.get("probabilite",0)) + float(rg.get("probabilite",0))) / 2, 1),
                # verdict majoritaire : VALIDER seulement si les deux valident
                "verdict"         : "VALIDER" if (rc.get("verdict","").upper() == "VALIDER"
                                                   and rg.get("verdict","").upper() == "VALIDER") else "REJETER",
                "raison"          : rc.get("raison","?"),           # Claude prioritaire pour la raison
                "risque_principal": rc.get("risque_principal") or rg.get("risque_principal","?"),
                "timing_ok"       : rc.get("timing_ok", False) and rg.get("timing_ok", False),
            }
            return (merged, "both")
        elif rc:
            return (rc, "claude")
        elif rg:
            return (rg, "gemini")
        return (None, "none")

    # mode == "auto" (défaut) : Claude → Gemini fallback
    rc = _claude_call(prompt)
    if rc:
        return (rc, "claude")
    _LAI.info("Claude indisponible → fallback Gemini")
    rg = _gemini_call(prompt)
    return (rg, "gemini") if rg else (None, "none")



def _gemini_build_scan_prompt(sig: dict, session: str) -> str:
    """Prompt léger pour Gemini : détecter si le setup mérite d'être analysé."""
    side_fr = "ACHAT (LONG)" if sig.get("side") == "BUY" else "VENTE (SHORT)"
    return """Tu es un trader ICT/SMC senior. Analyse rapidement ce setup et dis si il mérite une validation complète.

Setup : {pair} | {side} | Session : {session}
Entrée : {entry} | SL : {sl} | TP : {tp}
Score algo : {score}/100 | RR : 1:{rr}
Badges ICT : {badges}
Biais HTF : {htf}

Réponds UNIQUEMENT avec ce JSON, rien d'autre :
{{
  "setup_valide": true ou false,
  "score_setup": <0-10>,
  "raison": "<1 phrase max>",
  "continuer": true ou false
}}""".format(
        pair=sig.get("name","?"), side=side_fr, session=session,
        entry=sig.get("entry","?"), sl=sig.get("sl","?"), tp=sig.get("tp","?"),
        score=sig.get("score","?"), rr=sig.get("rr","?"),
        badges=sig.get("badges","Aucun"),
        htf=sig.get("bias","NEUTRAL"))


def gemini_scan_signal(sig: dict, session: str) -> dict:
    """
    Gemini pré-filtre le setup : rapide, léger.
    Retourne dict avec clé 'approved' (bool) et 'score_setup' (float).
    """
    fail = {"approved": False, "score_setup": 0, "raison": "Gemini indisponible"}
    if not _GEMINI_OK or not GEMINI_API_KEY:
        _LAI.warning("Gemini scan: SDK ou clé absente — approuvé par défaut")
        return {"approved": True, "score_setup": 5, "raison": "Gemini indispo — bypass"}
    prompt = _gemini_build_scan_prompt(sig, session)
    parsed = _gemini_call(prompt)
    if not parsed:
        _LAI.warning("Gemini scan: pas de réponse pour {}".format(sig.get("name","?")))
        return fail
    approved  = bool(parsed.get("continuer", False)) and bool(parsed.get("setup_valide", False))
    score     = float(parsed.get("score_setup", 0))
    raison    = parsed.get("raison", "?")
    icon = "✅" if approved else "❌"
    _LAI.info("Gemini scan {} {} | score {}/10 | {}".format(
        icon, sig.get("name","?"), score, raison))
    return {"approved": approved, "score_setup": score, "raison": raison}


def claude_validate_signal(sig: dict, session: str, htf_trend: str) -> dict:
    """
    Valide un signal via Claude AI (Risk Manager).

    Returns dict :
      validated   bool   – True = envoyer
      ai_score    float  – /10
      ai_proba    float  – %
      verdict     str    – VALIDER / REJETER
      raison      str
      risque      str
      final_score float  – score hybride /100
      timing_ok   bool
      cached      bool
    """
    fail = {"validated": False, "ai_score": 0, "ai_proba": 0,
            "verdict": "ERREUR", "raison": "Appel IA échoué",
            "risque": "Inconnu", "final_score": 0,
            "timing_ok": False, "cached": False}

    # Vérifier qu'au moins une IA est disponible selon le mode
    mode = AI_VALIDATOR.lower()
    claude_ready = _ANTHROPIC_OK and bool(CLAUDE_API_KEY)
    gemini_ready = _GEMINI_OK and bool(GEMINI_API_KEY)
    if mode == "claude" and not claude_ready:
        _LAI.warning("Mode claude mais SDK/clé Claude absent — signal accepté par défaut")
        return {**fail, "validated": True, "verdict": "VALIDER", "raison": "IA indispo — algo seul", "final_score": float(sig.get("score",0))}
    if mode == "gemini" and not gemini_ready:
        _LAI.warning("Mode gemini mais SDK/clé Gemini absent — signal accepté par défaut")
        return {**fail, "validated": True, "verdict": "VALIDER", "raison": "IA indispo — algo seul", "final_score": float(sig.get("score",0))}
    if mode in ("auto", "both") and not claude_ready and not gemini_ready:
        _LAI.warning("Aucune IA disponible — signal accepté par algo seul")
        return {**fail, "validated": True, "verdict": "VALIDER", "raison": "IA indispo — algo seul", "final_score": float(sig.get("score",0))}

    cache_key = "{}-{}-{}-{}".format(
        sig.get("name"), sig.get("side"), sig.get("entry"), session)
    with _ai_lock:
        cached = _ai_cache.get(cache_key)
        if cached and time.time() - cached["ts"] < _ai_cache_ttl:
            r = dict(cached["result"]); r["cached"] = True
            _LAI.info("Cache hit: {}".format(cache_key))
            return r

    t0 = time.time()
    parsed, ai_source = _ai_call_with_fallback(_claude_build_prompt(sig, session, htf_trend))
    elapsed = round(time.time() - t0, 2)

    if not parsed:
        _LAI.warning("Aucune IA n'a répondu — signal rejeté")
        return fail

    # Champs retournés par le nouveau prompt fondamental
    verdict      = parsed.get("verdict", "REJETER").upper()
    raison       = parsed.get("raison", "?")
    risque       = parsed.get("risque_principal", "?")
    timing_ok    = bool(parsed.get("timing_ok", False))
    tp_atteignable = bool(parsed.get("tp_atteignable", False))
    biais_fond   = parsed.get("biais_fondamental", "NEUTRE").upper()
    news_impact  = parsed.get("news_impact", "OK").upper()

    # Compatibilité champs anciens (pour fmt_ai_block)
    ai_score   = 8.0 if verdict == "VALIDER" else 4.0
    ai_proba   = 70.0 if verdict == "VALIDER" else 30.0
    confiance  = "HAUTE" if verdict == "VALIDER" else "FAIBLE"
    tp_rec     = 1
    sl_opt     = None
    criteres   = 0
    conseil    = ""
    algo_sc    = float(sig.get("score", 0))
    final_sc   = round(algo_sc, 1)   # score algo seul, Claude ne rescore plus

    # Règle de validation : fondamentaux + news + timing + TP atteignable
    news_block_ia = (news_impact == "BLOQUANT")
    fund_ok       = (biais_fond != "CONTRE")

    validated = (verdict == "VALIDER"
                 and fund_ok           # macro pas contre le trade
                 and not news_block_ia # pas de news bloquante
                 and timing_ok         # heure/jour favorables
                 and tp_atteignable)   # TP1 atteignable dans la session

    result = {
        "validated"        : validated,
        "ai_score"         : round(ai_score, 1),
        "ai_proba"         : round(ai_proba, 1),
        "verdict"          : verdict,
        "confiance"        : confiance,
        "raison"           : raison,
        "risque"           : risque,
        "final_score"      : final_sc,
        "timing_ok"        : timing_ok,
        "tp_recommande"    : tp_rec,
        "sl_optimal"       : sl_opt,
        "criteres_ok"      : criteres,
        "conseil_entree"   : conseil,
        "biais_fondamental": biais_fond,
        "news_impact"      : news_impact,
        "elapsed_s"        : elapsed,
        "ai_source"        : ai_source,
        "cached"           : False,
    }
    icon = "✅" if validated else "❌"
    _LAI.info("{} {} | {} | Score {}/10 | Proba {}% | Hybride {}/100 | Fond:{} | News:{} | {}s".format(
        icon, sig.get("name","?"), ai_source.upper(),
        ai_score, ai_proba, final_sc, biais_fond, news_impact, elapsed))

    with _ai_lock:
        _ai_cache[cache_key] = {"result": result, "ts": time.time()}
    return result


def fmt_ai_block(ai: dict) -> str:
    """Bloc HTML IA v20 — affiche analyse technique + fondamentale."""
    if not ai or ai.get("verdict") in ("ERREUR", None, ""):
        return ""
    verdict   = ai.get("verdict", "?")
    ai_score  = ai.get("ai_score", 0)
    ai_proba  = ai.get("ai_proba", 0)
    raison    = ai.get("raison", "")
    risque    = ai.get("risque", "")
    final_sc  = ai.get("final_score", 0)
    timing    = "✅" if ai.get("timing_ok") else "⚠️"
    v_icon    = "✅" if verdict == "VALIDER" else "❌"
    bar       = "█" * int(ai_score) + "░" * (10 - int(ai_score))
    confiance = ai.get("confiance", "")
    criteres  = ai.get("criteres_ok", 0)
    conseil   = ai.get("conseil_entree", "")
    tp_rec    = ai.get("tp_recommande", 1)
    sl_opt    = ai.get("sl_optimal")
    biais_fond  = ai.get("biais_fondamental", "NEUTRE")
    news_impact = ai.get("news_impact", "OK")

    conf_icon  = {"FAIBLE":"🔴","MOYENNE":"🟡","HAUTE":"🟢","EXCEPTIONNELLE":"🏆"}.get(confiance,"⚪")
    fond_icon  = {"ALIGNE":"✅","NEUTRE":"➖","CONTRE":"❌"}.get(biais_fond, "➖")
    news_icon  = {"OK":"✅","PRUDENCE":"🟡","BLOQUANT":"🔴"}.get(news_impact, "✅")
    tp_labels  = {1:"TP1 (RR×3)", 2:"TP2 (RR×4.5)", 3:"TP3 (RR×6.0)"}
    src_label  = {"claude":"🤖 Claude","gemini":"✨ Gemini","both":"🤖+✨ Dual AI","none":"⚙️ Algo"}.get(
        ai.get("ai_source",""), "🤖 IA")

    lines = [
        "",
        "━"*20,
        "{} <b>ANALYSE IA — {}</b>".format(src_label, v_icon),
        "",
        "🎯 Score IA   : <b>{}/10</b>  [{}]".format(ai_score, bar),
        "📊 Probabilité: <b>{}%</b>  |  Hybride: <b>{}/100</b>".format(ai_proba, final_sc),
        "🏅 Confiance  : {} <b>{}</b>".format(conf_icon, confiance),
        "✅ Critères   : <b>{}/16</b> validés  (12 tech + 4 macro)".format(criteres),
        "⏱️ Timing     : {}".format(timing),
        "{} Macro      : <b>{}</b>  |  {} News: <b>{}</b>".format(
            fond_icon, biais_fond, news_icon, news_impact),
        "",
        "💡 <b>Verdict :</b> {}".format(raison),
    ]
    if risque:
        lines.append("⚠️ <b>Risque #1 :</b> {}".format(risque))
    if conseil:
        lines.append("📌 <b>Conseil entrée :</b> {}".format(conseil))
    lines.append("🎯 <b>TP recommandé :</b> {}".format(tp_labels.get(tp_rec,"TP1")))
    if sl_opt:
        lines.append("🛡️ <b>SL optimal IA :</b> <code>{}</code>".format(sl_opt))
    lines.append("━"*20)
    return "\n".join(lines)

    # Label dynamique selon la source IA utilisée
    source = ai.get("ai_source", "claude").lower()
    if source == "gemini":
        ai_label = "GEMINI"
    elif source == "both":
        ai_label = "CLAUDE + GEMINI"
    else:
        ai_label = "CLAUDE"

    return (
        "\n━━━━━━━━━━━━━━━━━━━\n"
        "🧠 <b>ANALYSE IA — {}</b>\n"
        "━━━━━━━━━━━━━━━━━━━\n"
        "{} <b>{}</b>\n\n"
        "📊 Score IA    : <b>{}/10</b>  [{}]\n"
        "📈 Probabilité : <b>{}%</b>\n"
        "⭐ Score final : <b>{}/100</b>\n"
        "⏱️ Timing      : {}\n\n"
        "💡 <i>{}</i>\n\n"
        "⚠️ Risque : <i>{}</i>\n"
        "━━━━━━━━━━━━━━━━━━━"
    ).format(ai_label, v_icon, verdict, ai_score, bar,
             ai_proba, final_sc, timing, raison, risque)


# ══════════════════════════════════════════════════════
#  STRATÉGIE MULTI-MARCHÉS : FILTRE JOUR + PRIORITÉ
# ══════════════════════════════════════════════════════

# Priorité par paire (bonus score)
MARKET_PRIORITY = {
    "GBPJPY": 10,   # ultra volatile → setup premium
    "XAUUSD": 10,   # gold → ICT/SMC parfait
    "NAS100":  9,   # nasdaq → sessions US
    "SPX500":  8,
    "US30":    8,
    "BTCUSD":  9,   # crypto week-end
    "EURUSD":  7,
    "USDJPY":  7,
    "GBPUSD":  6,
    "EURJPY":  6,
    "XAGUSD":  5,
}

# Forex autorisés en semaine
FOREX_ACTIFS = {"EURUSD", "GBPUSD", "USDJPY", "GBPJPY", "EURJPY", "AUDUSD", "USDCHF", "USDCAD"}

def allowed_market(m):
    """
    Filtre les marchés selon le jour de la semaine :
    - Week-end (sam/dim)  → BTC + Gold/Silver uniquement
    - Semaine             → FOREX sélectifs + METALS + CRYPTO + INDICES
    """
    wd = datetime.now(timezone.utc).weekday()  # 0=lundi … 6=dimanche
    # BTC + Gold/Silver : actifs TOUJOURS
    if m["name"] in {"BTCUSD", "XAUUSD", "XAGUSD"}:
        return True
    if wd >= 5:
        # Week-end : uniquement BTC + métaux (déjà retournés True ci-dessus)
        return False
    # Semaine
    if m["cat"] == "FOREX":
        return m["name"] in FOREX_ACTIFS
    if m["cat"] in ("METALS", "INDICES"):
        return True
    return False

def get_trade_mode(m):
    """
    Retourne le mode de trading :
    - SCALP  → BTC week-end (RR 1.5–2.5, M5/M15)
    - NORMAL → tous les autres marchés (RR ≥ 3.0)
    """
    wd = datetime.now(timezone.utc).weekday()
    if wd >= 5 and m["cat"] == "CRYPTO":
        return "SCALP"
    return "NORMAL"

# ══════════════════════════════════════════════════════
#  LOGGER
# ══════════════════════════════════════════════════════
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S", handlers=[logging.StreamHandler(),
    logging.FileHandler("ab10.log", encoding="utf-8")])
L = logging.getLogger("AB10")
C = {"r":"\033[0m","b":"\033[1m","d":"\033[2m","c":"\033[96m","g":"\033[92m","y":"\033[93m","red":"\033[91m","m":"\033[95m"}
def clr(t,*c): return "".join(C[x] for x in c)+str(t)+C["r"]
def log(lv,msg):
    tags={"INFO":clr(" INFO ","b","c"),"SIG":clr(" SIGNAL","b","g"),"WARN":clr(" WARN ","b","y"),
          "ERR":clr(" ERR  ","b","red"),"PAY":clr(" PAY  ","b","m"),"AI":clr(" AI   ","b","m")}
    print("[{}] {} {}".format(datetime.now().strftime("%H:%M:%S"),tags.get(lv,lv),msg))

# ══════════════════════════════════════════════════════
#  RÉSEAU
# ══════════════════════════════════════════════════════
CTX = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
CTX.check_hostname = False; CTX.verify_mode = ssl.CERT_NONE
CTX.set_ciphers("DEFAULT@SECLEVEL=0")
TG = "https://api.telegram.org/bot{}/".format(TG_TOKEN)
_tg_lock = threading.Lock()

def http_get(url, timeout=15):
    hdrs = {"User-Agent":"Mozilla/5.0","Accept":"application/json"}
    for i in range(3):
        try:
            req = urllib.request.Request(url, headers=hdrs)
            opener = urllib.request.build_opener(urllib.request.HTTPSHandler(context=CTX))
            with opener.open(req, timeout=timeout) as r: return r.read().decode()
        except Exception:
            if i < 2: time.sleep(2)
    raise Exception("Max retries: "+url[:60])

def http_post(url, data, timeout=15):
    raw = urllib.parse.urlencode(data).encode()
    for i in range(3):
        try:
            req = urllib.request.Request(url, data=raw, method="POST",
                headers={"Content-Type":"application/x-www-form-urlencoded"})
            opener = urllib.request.build_opener(urllib.request.HTTPSHandler(context=CTX))
            with opener.open(req, timeout=timeout) as r: return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 409: return {}
            if i < 2: time.sleep(2)
            else: return {}
        except Exception:
            if i < 2: time.sleep(2)
            else: return {}
    return {}

def tg_req(m, p):
    try: return http_post(TG+m, p)
    except Exception as e: print("  [TG]", e); return {}

def tg_send(cid, text, kb=None):
    p = {"chat_id":str(cid),"text":text,"parse_mode":"HTML","disable_web_page_preview":"true"}
    if kb: p["reply_markup"] = json.dumps(kb)
    with _tg_lock: return tg_req("sendMessage", p)

def tg_doc(cid, data, fname, caption=""):
    bd = "AB10B"
    body = b""
    def f(n,v): return ("--{}\r\nContent-Disposition: form-data; name=\"{}\"\r\n\r\n".format(bd,n)).encode()+str(v).encode()+b"\r\n"
    body += f("chat_id",cid)
    if caption: body += f("caption",caption); body += f("parse_mode","HTML")
    body += ("--{}\r\nContent-Disposition: form-data; name=\"document\"; filename=\"{}\"\r\nContent-Type: application/octet-stream\r\n\r\n".format(bd,fname)).encode()
    body += data+b"\r\n"+("--{}--\r\n".format(bd)).encode()
    try:
        req = urllib.request.Request(TG+"sendDocument", data=body, method="POST",
            headers={"Content-Type":"multipart/form-data; boundary="+bd})
        opener = urllib.request.build_opener(urllib.request.HTTPSHandler(context=CTX))
        with opener.open(req, timeout=30) as r: return json.loads(r.read().decode())
    except: return {}

STK_W = "CAACAgIAAxkBAAIBjmWbNgIkJ6opkKOd5P2tniQu7R2IAALiAAMW0StFqKjl9SqrXTUNgQ"
STK_WIN = "CAACAgIAAxkBAAIBkmWbNibdCvV2RRd7OjQbIRpQ7juvAAIlAQACB8OhCpNJ8K7ZqLyANgQ"
STK_PRO = "CAACAgIAAxkBAAIBkGWbNhPIhvNXV7yKp9c0wZIf-g2rAAJDAQACvhiBCxlh5gPVk7E_NgQ"
STK_WELCOME = "CAACAgIAAxkBAAIBjmWbNgIkJ6opkKOd5P2tniQu7R2IAALiAAMW0StFqKjl9SqrXTUNgQ"
STK_SIGNAL  = "CAACAgIAAxkBAAIBhGWbNYA1IekbQLJgzf0HuBj0jYFnAAK3AQACB8OhCj1gMCxF9WqKNgQ"
STK_MONEY   = "CAACAgIAAxkBAAIBhmWbNa7lp9yDhKRHx_7q2sDFGn0ZAAKFAQACvhiBC-VC2IuBbHH3NgQ"
STK_FIRE    = "CAACAgIAAxkBAAIBiGWbNcBL0k0ZGIPKHGWBq-fFxgG0AAJcAAMW0StFbJlMpSqAx3oNgQ"
STK_CROWN   = "CAACAgIAAxkBAAIBimWbNeGxR0rp2J0m0eZ7nYJGq7cLAAKXAAMW0StFBtO28qLLMKgNgQ"
STK_ROCKET  = "CAACAgIAAxkBAAIBjGWbNfNMiEkgPZrxgWMVBH1ycfP7AAIbAQACB8OhCsYm5NOoMByuNgQ"


# ══════════════════════════════════════════════════════════════════
#  ▶ PAYMENT MANAGER — Import & initialisation (patch auto)
# ══════════════════════════════════════════════════════════════════
try:
    from alphabot_payment_manager import PaymentManager as _PM
    _PM_AVAILABLE = True
    print("[AlphaBot] ✅ PaymentManager chargé.")
except ImportError:
    _PM_AVAILABLE = False
    print("[AlphaBot] ⚠️ alphabot_payment_manager.py introuvable — paiements basiques actifs.")

# ── Flask Admin Panel ────────────────────────────────────────────
try:
    from flask import Flask as _Flask, request as _request, jsonify as _jsonify
    from flask import session as _session, redirect as _redirect, url_for as _url_for
    from flask import render_template_string as _render
    import secrets as _secrets
    _FLASK_OK = True
    print("[AlphaBot] ✅ Flask chargé — Panel admin disponible.")
except ImportError:
    _FLASK_OK = False
    print("[AlphaBot] ⚠️ Flask non installé (pip install flask) — panel web désactivé.")

# ══════════════════════════════════════════════════════════════════
# ══════════════════════════════════════════════════════
#  📊 GÉNÉRATEUR DE CHART SIGNAL (style TradingView dark)
# ══════════════════════════════════════════════════════
try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    _CHART_OK = True
except ImportError:
    _CHART_OK = False



PRO_PRICE  = 10;  REF_TARGET = 30;  REF_MONTHS = 3
FREE_LIMIT = 3;   PRO_LIMIT  = 10;  NB_AGENTS  = 20
TRIAL_DAYS = 3;   SCAN_SEC   = 30;  DATA_MAX_AGE = 120
DAILY_HOUR = 22;  WEEKLY_DAY = 6;   WEEKLY_HOUR = 21
SIGNAL_CUTOFF_HOUR = 22   # Aucun signal envoyé à partir de 22h00 UTC
FEE_TAKER  = 0.0004
CHALLENGE_START = float(os.getenv("CHALLENGE_START", "5.0"))
MAX_OPEN   = 3;  COOLDOWN_MIN = 25
FLOOR_USD  = 2.0; DD_LIMIT = 0.35
AM_MULT    = 1.30; AM_MAX = 4

# ── Throttle signaux ────────────────────────────────────────────
MAX_SIG_PER_HOUR  = 1   # strict : 1 seul signal par heure glissante
MAX_SIG_PER_DAY   = 10  # max global par jour (PRO: limité par PRO_LIMIT)
MIN_GAP_BETWEEN   = 30  # minutes minimum entre 2 signaux consécutifs

MARKETS = [
    {"sym":"GC=F",     "name":"XAUUSD","cat":"METALS","pip":0.01,  "max_sp":70,"vol":5,"crypto":False},
    {"sym":"SI=F",     "name":"XAGUSD","cat":"METALS","pip":0.001, "max_sp":10,"vol":4,"crypto":False},
    {"sym":"BTC-USD",  "name":"BTCUSD","cat":"CRYPTO","pip":1.0,   "max_sp":100,"vol":5,"crypto":True},
    {"sym":"EURUSD=X", "name":"EURUSD","cat":"FOREX", "pip":0.0001,"max_sp":2, "vol":5,"crypto":False},
    {"sym":"GBPUSD=X", "name":"GBPUSD","cat":"FOREX", "pip":0.0001,"max_sp":3, "vol":5,"crypto":False},
    {"sym":"USDJPY=X", "name":"USDJPY","cat":"FOREX", "pip":0.01,  "max_sp":3, "vol":5,"crypto":False},
    {"sym":"GBPJPY=X", "name":"GBPJPY","cat":"FOREX", "pip":0.01,  "max_sp":6, "vol":5,"crypto":False},
    {"sym":"EURJPY=X", "name":"EURJPY","cat":"FOREX", "pip":0.01,  "max_sp":5, "vol":4,"crypto":False},
    {"sym":"AUDUSD=X", "name":"AUDUSD","cat":"FOREX", "pip":0.0001,"max_sp":3, "vol":4,"crypto":False},
    {"sym":"AUDJPY=X", "name":"AUDJPY","cat":"FOREX", "pip":0.01,  "max_sp":5, "vol":4,"crypto":False},
    {"sym":"CADJPY=X", "name":"CADJPY","cat":"FOREX", "pip":0.01,  "max_sp":5, "vol":4,"crypto":False},
    {"sym":"USDCHF=X", "name":"USDCHF","cat":"FOREX", "pip":0.0001,"max_sp":3, "vol":4,"crypto":False},
    {"sym":"NZDUSD=X", "name":"NZDUSD","cat":"FOREX", "pip":0.0001,"max_sp":3, "vol":3,"crypto":False},
    {"sym":"USDCAD=X", "name":"USDCAD","cat":"FOREX", "pip":0.0001,"max_sp":3, "vol":4,"crypto":False},
    {"sym":"NQ=F",     "name":"NAS100","cat":"INDICES","pip":0.25, "max_sp":5, "vol":5,"crypto":False},
    {"sym":"ES=F",     "name":"SPX500","cat":"INDICES","pip":0.25, "max_sp":3, "vol":5,"crypto":False},
    {"sym":"YM=F",     "name":"US30",  "cat":"INDICES","pip":1.0,  "max_sp":5, "vol":5,"crypto":False},
    {"sym":"CL=F",     "name":"USOIL", "cat":"OIL",   "pip":0.01, "max_sp":8, "vol":4,"crypto":False},
]
CAT_EMO = {"FOREX":"💱","METALS":"🥇","CRYPTO":"₿","INDICES":"📈","OIL":"🛢"}
PAIR_MAX_LEV = {"BTCUSDT":125,"ETHUSDT":100,"SOLUSDT":50,"BNBUSDT":75,"XRPUSDT":50}
# ══════════════════════════════════════════════════════
#  STRATÉGIE MULTI-MARCHÉS : FILTRE JOUR + PRIORITÉ
# ══════════════════════════════════════════════════════

# ══ MARCHÉS SCALP PRIORITAIRES (toute la semaine) ═══════════════
# Gold/Silver/BTC = scalp permanent — RR min 2.0 — priorité maximale
SCALP_PRIORITY_MARKETS = {"XAUUSD", "XAGUSD", "BTCUSD"}

# Priorité par paire (bonus score)
MARKET_PRIORITY = {
    "XAUUSD": 12,   # gold → ICT/SMC parfait — priorité absolue ★
    "BTCUSD": 11,   # BTC → 24/7, très volatile ★
    "XAGUSD":  9,   # silver → corrélé gold, liquidité forte ★
    "GBPJPY": 10,   # ultra volatile → setup premium
    "NAS100":  9,   # nasdaq → sessions US
    "SPX500":  8,
    "US30":    8,
    "EURUSD":  7,
    "USDJPY":  7,
    "GBPUSD":  6,
    "EURJPY":  6,
}

# Forex autorisés en semaine (déjà défini plus haut — alias)
# FOREX_ACTIFS défini à la section CONFIG étendue

def get_trade_mode(m):
    """
    Retourne le mode de trading :
    - SCALP  → Gold/Silver/BTC (toute la semaine) — RR min 2.0, M5
    - NORMAL → Forex + Indices — RR ≥ 3.0
    """
    if m["name"] in SCALP_PRIORITY_MARKETS:
        return "SCALP"
    wd = datetime.now(timezone.utc).weekday()
    if wd >= 5 and m["cat"] == "CRYPTO":
        return "SCALP"
    return "NORMAL"

# ── Alias constantes v13 (rétrocompatibilité) ─────────────────────
INACTIF_DAYS     = 3
DATA_MAX_AGE_MIN = DATA_MAX_AGE
BOT_USERNAME     = BOT_USER
PRO_PROMO        = PRO_PRICE
NB_AGENTS        = 20
VIP_CHANNEL      = VIP_CH       # alias v13


# ══════════════════════════════════════════════════════
#  LOGGER
# ══════════════════════════════════════════════════════
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S", handlers=[logging.StreamHandler(),
    logging.FileHandler("ab10.log", encoding="utf-8")])
L = logging.getLogger("AB10")
C = {"r":"\033[0m","b":"\033[1m","d":"\033[2m","c":"\033[96m","g":"\033[92m","y":"\033[93m","red":"\033[91m","m":"\033[95m"}
def clr(t,*c): return "".join(C[x] for x in c)+str(t)+C["r"]
def log(lv,msg):
    tags={"INFO":clr(" INFO ","b","c"),"SIG":clr(" SIGNAL","b","g"),"WARN":clr(" WARN ","b","y"),
          "ERR":clr(" ERR  ","b","red"),"PAY":clr(" PAY  ","b","m"),"AI":clr(" AI   ","b","m")}
    print("[{}] {} {}".format(datetime.now().strftime("%H:%M:%S"),tags.get(lv,lv),msg))

# ══════════════════════════════════════════════════════
#  RÉSEAU
# ══════════════════════════════════════════════════════
CTX = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
CTX.check_hostname = False; CTX.verify_mode = ssl.CERT_NONE
CTX.set_ciphers("DEFAULT@SECLEVEL=0")
TG = "https://api.telegram.org/bot{}/".format(TG_TOKEN)
_tg_lock = threading.Lock()

def http_get(url, timeout=15):
    hdrs = {"User-Agent":"Mozilla/5.0","Accept":"application/json"}
    for i in range(3):
        try:
            req = urllib.request.Request(url, headers=hdrs)
            opener = urllib.request.build_opener(urllib.request.HTTPSHandler(context=CTX))
            with opener.open(req, timeout=timeout) as r: return r.read().decode()
        except Exception:
            if i < 2: time.sleep(2)
    raise Exception("Max retries: "+url[:60])

def http_post(url, data, timeout=15):
    raw = urllib.parse.urlencode(data).encode()
    for i in range(3):
        try:
            req = urllib.request.Request(url, data=raw, method="POST",
                headers={"Content-Type":"application/x-www-form-urlencoded"})
            opener = urllib.request.build_opener(urllib.request.HTTPSHandler(context=CTX))
            with opener.open(req, timeout=timeout) as r: return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 409: return {}
            if i < 2: time.sleep(2)
            else: return {}
        except Exception:
            if i < 2: time.sleep(2)
            else: return {}
    return {}

def tg_req(m, p):
    try: return http_post(TG+m, p)
    except Exception as e: print("  [TG]", e); return {}

def tg_send(cid, text, kb=None):
    p = {"chat_id":str(cid),"text":text,"parse_mode":"HTML","disable_web_page_preview":"true"}
    if kb: p["reply_markup"] = json.dumps(kb)
    with _tg_lock: return tg_req("sendMessage", p)

def tg_doc(cid, data, fname, caption=""):
    bd = "AB10B"
    body = b""
    def f(n,v): return ("--{}\r\nContent-Disposition: form-data; name=\"{}\"\r\n\r\n".format(bd,n)).encode()+str(v).encode()+b"\r\n"
    body += f("chat_id",cid)
    if caption: body += f("caption",caption); body += f("parse_mode","HTML")
    body += ("--{}\r\nContent-Disposition: form-data; name=\"document\"; filename=\"{}\"\r\nContent-Type: application/octet-stream\r\n\r\n".format(bd,fname)).encode()
    body += data+b"\r\n"+("--{}--\r\n".format(bd)).encode()
    try:
        req = urllib.request.Request(TG+"sendDocument", data=body, method="POST",
            headers={"Content-Type":"multipart/form-data; boundary="+bd})
        opener = urllib.request.build_opener(urllib.request.HTTPSHandler(context=CTX))
        with opener.open(req, timeout=30) as r: return json.loads(r.read().decode())
    except: return {}

STK_W = "CAACAgIAAxkBAAIBjmWbNgIkJ6opkKOd5P2tniQu7R2IAALiAAMW0StFqKjl9SqrXTUNgQ"
STK_WIN = "CAACAgIAAxkBAAIBkmWbNibdCvV2RRd7OjQbIRpQ7juvAAIlAQACB8OhCpNJ8K7ZqLyANgQ"
STK_PRO = "CAACAgIAAxkBAAIBkGWbNhPIhvNXV7yKp9c0wZIf-g2rAAJDAQACvhiBCxlh5gPVk7E_NgQ"
STK_WELCOME = "CAACAgIAAxkBAAIBjmWbNgIkJ6opkKOd5P2tniQu7R2IAALiAAMW0StFqKjl9SqrXTUNgQ"
STK_SIGNAL  = "CAACAgIAAxkBAAIBhGWbNYA1IekbQLJgzf0HuBj0jYFnAAK3AQACB8OhCj1gMCxF9WqKNgQ"
STK_MONEY   = "CAACAgIAAxkBAAIBhmWbNa7lp9yDhKRHx_7q2sDFGn0ZAAKFAQACvhiBC-VC2IuBbHH3NgQ"
STK_FIRE    = "CAACAgIAAxkBAAIBiGWbNcBL0k0ZGIPKHGWBq-fFxgG0AAJcAAMW0StFbJlMpSqAx3oNgQ"
STK_CROWN   = "CAACAgIAAxkBAAIBimWbNeGxR0rp2J0m0eZ7nYJGq7cLAAKXAAMW0StFBtO28qLLMKgNgQ"
STK_ROCKET  = "CAACAgIAAxkBAAIBjGWbNfNMiEkgPZrxgWMVBH1ycfP7AAIbAQACB8OhCsYm5NOoMByuNgQ"


# ══════════════════════════════════════════════════════════════════
#  ▶ PAYMENT MANAGER — Import & initialisation (patch auto)
# ══════════════════════════════════════════════════════════════════
try:
    from alphabot_payment_manager import PaymentManager as _PM
    _PM_AVAILABLE = True
    print("[AlphaBot] ✅ PaymentManager chargé.")
except ImportError:
    _PM_AVAILABLE = False
    print("[AlphaBot] ⚠️ alphabot_payment_manager.py introuvable — paiements basiques actifs.")

# ── Flask Admin Panel ────────────────────────────────────────────
try:
    from flask import Flask as _Flask, request as _request, jsonify as _jsonify
    from flask import session as _session, redirect as _redirect, url_for as _url_for
    from flask import render_template_string as _render
    import secrets as _secrets
    _FLASK_OK = True
    print("[AlphaBot] ✅ Flask chargé — Panel admin disponible.")
except ImportError:
    _FLASK_OK = False
    print("[AlphaBot] ⚠️ Flask non installé (pip install flask) — panel web désactivé.")

# ══════════════════════════════════════════════════════════════════
# ══════════════════════════════════════════════════════
#  📊 GÉNÉRATEUR DE CHART SIGNAL (style TradingView dark)
# ══════════════════════════════════════════════════════
try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    _CHART_OK = True
except ImportError:
    _CHART_OK = False

# ══════════════════════════════════════════════════════
#  TRADINGVIEW CHART DOWNLOAD (chart-img.com API)
# ══════════════════════════════════════════════════════
CHART_IMG_KEY = os.getenv("CHART_IMG_KEY", "")   # clé API chart-img.com (plan free = 50 req/j)

TV_SYMBOL_MAP = {
    "XAUUSD": "TVC:GOLD",
    "XAGUSD": "TVC:SILVER",
    "BTCUSD": "BINANCE:BTCUSDT",
    "EURUSD": "FX:EURUSD",
    "GBPUSD": "FX:GBPUSD",
    "USDJPY": "FX:USDJPY",
    "GBPJPY": "FX:GBPJPY",
    "EURJPY": "FX:EURJPY",
    "AUDUSD": "FX:AUDUSD",
    "AUDJPY": "FX:AUDJPY",
    "CADJPY": "FX:CADJPY",
    "USDCHF": "FX:USDCHF",
    "NZDUSD": "FX:NZDUSD",
    "USDCAD": "FX:USDCAD",
    "NAS100": "NASDAQ:NDX",
    "SPX500": "SP:SPX",
    "US30":   "TVC:DJI",
    "USOIL":  "TVC:USOIL",
}

def fetch_tradingview_chart(sig):
    """
    Télécharge l'image de graphique TradingView via chart-img.com
    Si la clé API est absente ou si l'appel échoue → fallback matplotlib.
    Retourne bytes PNG ou None.
    """
    name = sig.get("name", "")
    tv_sym = TV_SYMBOL_MAP.get(name, "FX:" + name[:6])

    # ── Tentative chart-img.com ──────────────────────────────────
    if CHART_IMG_KEY:
        try:
            params = urllib.parse.urlencode({
                "symbol":   tv_sym,
                "interval": "15",
                "width":    "800",
                "height":   "500",
                "theme":    "dark",
                "key":      CHART_IMG_KEY,
            })
            url = "https://api.chart-img.com/v1/tradingview/advanced-chart?" + params
            req = urllib.request.Request(url, headers={"User-Agent": "AlphaBot/20"})
            with urllib.request.urlopen(req, timeout=12) as r:
                if r.status == 200:
                    data = r.read()
                    if len(data) > 5000:   # image valide
                        log("INFO", "Chart TV téléchargé: {} ({} bytes)".format(name, len(data)))
                        return data
        except Exception as e:
            log("WARN", "chart-img.com échoué ({}): {}".format(name, e))

    # ── Fallback : génération matplotlib locale ──────────────────
    try:
        m_obj = next((x for x in MARKETS if x["name"] == name), None)
        m15_c = fetch_c(m_obj["sym"], "15m", "3d") if m_obj else None
        return generate_signal_chart(sig, m15_c)
    except Exception as e:
        log("WARN", "generate_signal_chart fallback échoué: {}".format(e))
        return None


def generate_signal_chart(sig, candles=None):
    """
    Génère une image PNG du signal (dark theme TradingView).
    Retourne bytes PNG ou None si matplotlib indisponible.
    """
    if not _CHART_OK: return None
    from io import BytesIO
    import numpy as np

    BG = "#0f1117"; BG2 = "#1a1d27"
    GREEN = "#26a69a"; RED = "#ef5350"
    YELLOW = "#ffd700"; WHITE = "#e0e0e0"; GREY = "#555566"

    side  = sig["side"]
    entry = float(sig["entry"])
    tp    = float(sig["tp"])
    sl    = float(sig["sl"])
    rr    = sig["rr"]
    score = sig.get("score", 0)
    name  = sig["name"]
    t     = sig.get("time", "")
    badges= sig.get("badges", "").replace("✓","v").replace("·","-")
    color = GREEN if side == "BUY" else RED
    arrow = "ACHAT  ^" if side == "BUY" else "VENTE  v"

    fig = plt.figure(figsize=(8.5, 5), facecolor=BG)
    ax_c = fig.add_axes([0.02, 0.10, 0.57, 0.82], facecolor=BG2)
    ax_i = fig.add_axes([0.63, 0.04, 0.35, 0.92], facecolor=BG)

    # ── Bougies ou courbe simulée ───────────────────────
    if candles and len(candles) >= 8:
        n = min(40, len(candles))
        c = candles[-n:]
        for i, cv in enumerate(c):
            o, h, l, cl = cv["o"], cv["h"], cv["l"], cv["c"]
            col = GREEN if cl >= o else RED
            ax_c.plot([i, i], [l, h], color=col, linewidth=0.7, solid_capstyle="round")
            ax_c.add_patch(plt.Rectangle((i-0.3, min(o,cl)), 0.6, max(abs(cl-o), (h-l)*0.01),
                                          color=col, alpha=0.88))
        x_end = n - 1
    else:
        np.random.seed(int(entry * 10) % 999)
        pts = [entry * (1 + np.random.uniform(-0.002, 0.002)) for _ in range(35)]
        pts[-1] = entry
        ax_c.plot(pts, color=GREY, linewidth=1.1, alpha=0.7)
        x_end = len(pts) - 1

    # Lignes niveaux
    ax_c.axhline(entry, color=YELLOW, linewidth=1.8, linestyle="--", alpha=0.9, zorder=5)
    ax_c.axhline(tp,    color=GREEN,  linewidth=1.3, linestyle="-",  alpha=0.85, zorder=5)
    ax_c.axhline(sl,    color=RED,    linewidth=1.3, linestyle="-",  alpha=0.85, zorder=5)

    # Labels droite
    dp = 2 if entry > 100 else (3 if entry > 10 else 5)
    fmt = "{:."+str(dp)+"f}"
    ax_c.text(x_end+0.5, entry, " "+fmt.format(entry), color=YELLOW, fontsize=6.5, va="center", zorder=6)
    ax_c.text(x_end+0.5, tp,    " "+fmt.format(tp),    color=GREEN,  fontsize=6.5, va="center", zorder=6)
    ax_c.text(x_end+0.5, sl,    " "+fmt.format(sl),    color=RED,    fontsize=6.5, va="center", zorder=6)
    ax_c.text(x_end+0.5, (tp+entry)/2 if side=="BUY" else (entry+tp)/2,
              " TP", color=GREEN, fontsize=6, va="center", alpha=0.7)
    ax_c.text(x_end+0.5, (sl+entry)/2,
              " SL", color=RED,   fontsize=6, va="center", alpha=0.7)

    # Zones colorées
    if side == "BUY":
        ax_c.axhspan(entry, tp, alpha=0.06, color=GREEN)
        ax_c.axhspan(sl, entry, alpha=0.06, color=RED)
    else:
        ax_c.axhspan(tp, entry, alpha=0.06, color=GREEN)
        ax_c.axhspan(entry, sl, alpha=0.06, color=RED)

    ax_c.set_facecolor(BG2); ax_c.tick_params(colors=GREY, labelsize=6)
    for sp in ax_c.spines.values(): sp.set_color(GREY); sp.set_linewidth(0.4)
    ax_c.set_xlim(-1, x_end + 4)
    ax_c.set_title("M15  -  AlphaBot PRO v10", color=GREY, fontsize=7.5, pad=4)

    # ── Panneau droit ───────────────────────────────────
    ax_i.axis("off")
    y = 0.97

    def row(label, val, lc=GREY, vc=WHITE, sz=9.5):
        ax_i.text(0.02, y, label, transform=ax_i.transAxes, fontsize=sz, color=lc, va="top")
        ax_i.text(0.98, y, val,   transform=ax_i.transAxes, fontsize=sz, color=vc, va="top",
                  ha="right", fontweight="bold")

    # Nom
    ax_i.text(0.50, y, name, transform=ax_i.transAxes, fontsize=16,
              color=WHITE, va="top", ha="center", fontweight="bold"); y -= 0.11
    # Direction
    ax_i.text(0.50, y, arrow, transform=ax_i.transAxes, fontsize=13,
              color=color, va="top", ha="center", fontweight="bold"); y -= 0.12

    # Score bar
    ax_i.add_patch(plt.Rectangle((0.02, y-0.028), 0.96, 0.045, color="#2a2d3a",
                                   transform=ax_i.transAxes, clip_on=False))
    ax_i.add_patch(plt.Rectangle((0.02, y-0.028), 0.96*(score/100), 0.045, color=color,
                                   transform=ax_i.transAxes, clip_on=False))
    ax_i.text(0.50, y-0.005, "Score  {}/100".format(score), transform=ax_i.transAxes,
              fontsize=8, color=WHITE, ha="center", va="top", fontweight="bold"); y -= 0.12

    # Niveaux
    rows_data = [
        ("Entree", fmt.format(entry), YELLOW),
        ("TP",     fmt.format(tp),    GREEN),
        ("SL",     fmt.format(sl),    RED),
        ("RR",     "1:{}".format(rr), WHITE),
    ]
    for lb, vl, vc in rows_data:
        ax_i.text(0.02, y, lb, transform=ax_i.transAxes, fontsize=9.5, color=GREY, va="top")
        ax_i.text(0.98, y, vl, transform=ax_i.transAxes, fontsize=9.5, color=vc,
                  va="top", ha="right", fontweight="bold")
        y -= 0.10

    y -= 0.02
    if badges:
        short = badges[:50]+("..." if len(badges)>50 else "")
        ax_i.text(0.02, y, short, transform=ax_i.transAxes,
                  fontsize=6.8, color=GREY, va="top", wrap=True); y -= 0.10
    ax_i.text(0.50, y, "{} UTC".format(t), transform=ax_i.transAxes,
              fontsize=7.5, color=GREY, va="top", ha="center", alpha=0.8); y -= 0.09
    ax_i.text(0.50, y, "Not financial advice", transform=ax_i.transAxes,
              fontsize=6.5, color=GREY, va="top", ha="center", alpha=0.55)

    fig.text(0.5, 0.01, "@leaderodg_bot", ha="center", fontsize=7.5, color=GREY, alpha=0.6)

    buf = BytesIO()
    plt.savefig(buf, format="png", dpi=130, bbox_inches="tight",
                facecolor=BG, edgecolor="none")
    plt.close(fig)
    buf.seek(0)
    return buf.read()

def tg_send_photo(cid, img_bytes, caption=""):
    """Envoie une image PNG via Telegram sendPhoto."""
    bd = "AB10PH"
    body = b""
    def f(n, v):
        return ("--{}\r\nContent-Disposition: form-data; name=\"{}\"\"\r\n\r\n".format(bd,n)).encode()+str(v).encode()+b"\r\n"
    body += f("chat_id", cid)
    if caption:
        body += f("caption", caption[:1024])
        body += f("parse_mode", "HTML")
    body += ("--{}\r\nContent-Disposition: form-data; name=\"photo\"; filename=\"signal.png\"\r\nContent-Type: image/png\r\n\r\n".format(bd)).encode()
    body += img_bytes + b"\r\n" + ("--{}--\r\n".format(bd)).encode()
    try:
        req = urllib.request.Request(TG+"sendPhoto", data=body, method="POST",
            headers={"Content-Type":"multipart/form-data; boundary="+bd})
        opener = urllib.request.build_opener(urllib.request.HTTPSHandler(context=CTX))
        with opener.open(req, timeout=30) as r: return json.loads(r.read().decode())
    except Exception as e:
        log("WARN","tg_send_photo: {}".format(e)); return {}

def tg_edit(chat_id, message_id, text, kb=None):
    """Edite un message existant (pour le recheck signal)."""
    p = {"chat_id": str(chat_id), "message_id": message_id,
         "text": text[:4096], "parse_mode": "HTML"}
    if kb: p["reply_markup"] = json.dumps(kb)
    try: return tg_req("editMessageText", p)
    except: return {"ok": False}

def tg_send_sticker(chat_id, sticker_id): tg_req("sendSticker", {"chat_id": str(chat_id), "sticker": sticker_id})
def tg_sticker(cid, sid): tg_req("sendSticker",{"chat_id":str(cid),"sticker":sid})

# ══════════════════════════════════════════════════════
#  BASE DE DONNÉES — PostgreSQL / SQLite (via alphabot_pg)
# ══════════════════════════════════════════════════════
# Toute la logique DB (connexion, tables, helpers, fonctions métier)
# est gérée par alphabot_pg.py qui supporte automatiquement :
#   • PostgreSQL (Render persistant) si DATABASE_URL est défini
#   • SQLite local en fallback (tests / dev)
# ─────────────────────────────────────────────────────
import sys as _sys, os as _os
_pg_path = _os.path.dirname(_os.path.abspath(__file__))
if _pg_path not in _sys.path:
    _sys.path.insert(0, _pg_path)

from alphabot_pg import (
    # Connexion & lock
    _conn, _dbl, _db_lock, _USE_PG,
    # Helpers bas niveau
    db_one, db_all, db_run,
    # Init DB
    db_init,
    # Gestion utilisateurs
    db_register, db_pro, db_free,
    is_pro, get_plan, get_refs, get_pro_info,
    pro_users, free_users, all_users, find_user,
    # Compteurs signaux
    count_today, count_incr,
    db_count_increment,          # alias rétrocompat
    # Expiration PRO
    check_expiry,
    # Signaux & tracking
    save_signal, open_signals, close_track,
    # Statistiques
    daily_stats, weekly_stats, global_stats,
    rep_sent, mark_rep,
    # Paiements
    save_pay, pending_pays,
    db_save_payment, db_pending_payments,  # aliases
    # Challenge
    chal_get, chal_save,
    # Mémoire IA
    mem_query, mem_record, best_setups, worst_setups,
    # Aliases rétrocompatibilité v17
    db_get_pro_info, db_get_refs, db_global_stats,
    db_daily_stats, db_weekly_stats,
    db_get_pro_users, db_get_free_users,
    db_downgrade_pro, db_activate_pro,
    db_find_by_username, db_count_today,
    db_get_inactive_users, inactive_users,
    # Migration CLI
    migrate_sqlite_to_pg,
)

log("INFO", clr("DB v10 OK (backend: {})".format("PostgreSQL" if _USE_PG else "SQLite"), "b", "g"))

# ── Patch post-import : garantir compatibilité des signatures ──────────────────

# db_get_pro_info : toujours retourner exactement (plan, expires, source)
def db_get_pro_info(uid):
    import alphabot_pg as _pg
    row = _pg.db_get_pro_info(uid)
    if row is None:
        return ("FREE", None, None)
    if isinstance(row, (list, tuple)):
        if len(row) >= 3:
            return (row[0], row[1], row[2])
        return (row[0], None, None)
    return (row, None, None)

# _conn : bypass alphabot_pg._conn pour éviter l'injection du chemin DB dans le SQL
# Fix: near "ab10": syntax error — alphabot_pg._conn utilisait DB_FILE dans une chaîne SQL brute
import alphabot_pg as _pg_mod
_conn_orig = _pg_mod._conn
def _conn():
    if _USE_PG:
        # Mode PostgreSQL : on délègue à alphabot_pg normalement
        try:
            return _conn_orig()
        except TypeError:
            return _conn_orig(DB_FILE)
    else:
        # Mode SQLite : connexion directe pour éviter le bug de formatage SQL
        import sqlite3 as _sqlite3
        con = _sqlite3.connect(DB_FILE, check_same_thread=False)
        con.row_factory = _sqlite3.Row
        return con

# ── Fin patch ──────────────────────────────────────────────────────────────────

# ── Migration DB : colonnes manquantes (auto au démarrage) ────────────────────
def _db_migrate():
    try:
        con = _conn(); cur = con.cursor()
        for col, typ in [
            ("ref_count",    "INTEGER DEFAULT 0"),
            ("ref_bonus",    "INTEGER DEFAULT 0"),
            ("ref_by",       "INTEGER DEFAULT NULL"),
            ("trial_used",   "INTEGER DEFAULT 0"),
        ]:
            try:
                cur.execute("ALTER TABLE users ADD COLUMN {} {}".format(col, typ))
                con.commit()
                log("INFO", clr("Migration DB: colonne '{}' ajoutée ✅".format(col), "g"))
            except Exception: pass
        # start_bal dans challenge
        try:
            cur.execute("ALTER TABLE challenge ADD COLUMN start_bal REAL DEFAULT {}".format(CHALLENGE_START))
            con.commit()
            log("INFO", clr("Migration DB: start_bal ajouté ✅", "g"))
        except Exception: pass
        # signal_sent table (anti-doublon)
        try:
            cur.execute("""CREATE TABLE IF NOT EXISTS signal_sent (
                user_id INTEGER, sig_key TEXT,
                sent_at TEXT, PRIMARY KEY(user_id, sig_key))""")
            con.commit()
        except Exception: pass
        con.close()
    except Exception as _me:
        log("WARN", "Migration DB erreur: {}".format(_me))
_db_migrate()
# ── Fin migration ─────────────────────────────────────────────────────────────

def setup_key(sig):
    """
    Génère une clé de mémoire précise pour un signal :
    paire | session | badges principaux
    Ex: "XAUUSD|LONDON_KZ|OTE+FVG+LIQ"
    """
    sn,_,_,_=get_session()
    badges_raw = sig.get("badges","")
    tags=[]
    if "OTE" in badges_raw:   tags.append("OTE")
    if "FVG" in badges_raw:   tags.append("FVG")
    if "CHoCH" in badges_raw: tags.append("CHoCH")
    if "Sweep" in badges_raw or "Stop Hunt" in badges_raw or "EQ" in badges_raw: tags.append("LIQ")
    if "H&S" in badges_raw or "IH&S" in badges_raw: tags.append("HS")
    if "Double" in badges_raw: tags.append("DBL")
    if "Breakout" in badges_raw: tags.append("BRK")
    if "Macro" in badges_raw: tags.append("MAC")
    tag_str = "+".join(tags) if tags else "BASE"
    return "{}|{}|{}".format(sig.get("name","?"), sn, tag_str)

def mem_adj_score(sig_key, sc):
    """
    Ajuste le score selon l'historique IA de ce setup.
    Minimum 5 trades pour que la mémoire agisse.
    WR > 70% → +8 pts  |  WR 50-70% → +3 pts
    WR 35-50% → -8 pts  |  WR < 35% → -15 pts
    """
    w,l,pnl=mem_query(sig_key)
    total=w+l
    if total < 5: return sc, ""
    wr=w/total
    if wr > 0.70:   return min(sc+8, 115),  "🧠 WR {}%✓".format(int(wr*100))
    if wr > 0.50:   return min(sc+3, 115),  ""
    if wr > 0.35:   return max(sc-8, 0),    ""
    return max(sc-15, 0), ""

# ══════════════════════════════════════════════════════
#  SESSIONS
# ══════════════════════════════════════════════════════
def get_session():
    h=datetime.now(timezone.utc).hour; wd=datetime.now(timezone.utc).weekday()
    if wd>=5: return "WEEKEND",72,"🌍 Week-end ₿",True
    if 7<=h<10:  return "LONDON_KZ",61,"🇬🇧 London Kill Zone 🔥",False
    if 12<=h<16: return "OVERLAP",63,"🇬🇧+🇺🇸 London+NY",False
    if 16<=h<21: return "NY",65,"🇺🇸 New York",False
    if 10<=h<12: return "LONDON",63,"🇬🇧 Londres",False
    if 0<=h<7:   return "ASIAN",68,"🌏 Asiatique",False
    return "OFF",73,"🌑 Hors session",False

def sess_bonus(sn):
    return {"LONDON_KZ":15,"OVERLAP":10,"NY":8,"LONDON":5,"ASIAN":0,"WEEKEND":5,"OFF":-20}.get(sn,0)

# ══════════════════════════════════════════════════════
#  FETCH DONNÉES YAHOO
# ══════════════════════════════════════════════════════
def fetch_c(sym, interval, period):
    sym_e=urllib.parse.quote(sym)
    for base in ["https://query1.finance.yahoo.com","https://query2.finance.yahoo.com"]:
        try:
            url="{}/v8/finance/chart/{}?interval={}&range={}&includePrePost=false".format(base,sym_e,interval,period)
            body=json.loads(http_get(url,timeout=20))
            res=body.get("chart",{}).get("result",[])
            if not res: continue
            ts=res[0].get("timestamp",[])
            if ts and (time.time()-ts[-1])/60>DATA_MAX_AGE:
                log("WARN",clr("{} {} trop vieux — ignoré".format(sym,interval),"y")); return None
            q=res[0]["indicators"]["quote"][0]
            c=[{"o":float(o),"h":float(h),"l":float(l),"c":float(cv)}
               for o,h,l,cv in zip(q.get("open",[]),q.get("high",[]),q.get("low",[]),q.get("close",[]))
               if None not in (o,h,l,cv)]
            if len(c)>=10: return c
        except: continue
    return None

# ══════════════════════════════════════════════════════
#  ANALYSE TECHNIQUE
# ══════════════════════════════════════════════════════
def atr(c,p=14):
    t=[max(c[i]["h"]-c[i]["l"],abs(c[i]["h"]-c[i-1]["c"]),abs(c[i]["l"]-c[i-1]["c"])) for i in range(1,len(c))]
    s=t[-p:] if len(t)>=p else t; return sum(s)/len(s) if s else 0.001

def swings(c,n=5):
    H,L=[],[]
    for i in range(n,len(c)-n):
        w=c[i-n:i+n+1]
        if c[i]["h"]==max(x["h"] for x in w): H.append((i,c[i]["h"]))
        if c[i]["l"]==min(x["l"] for x in w): L.append((i,c[i]["l"]))
    return H,L


def sl_from_structure(candles, bias, atr_val, entry, pip, spread_pips=0, lookback=40):
    """
    Place le SL sur le dernier swing significatif de la structure M15.
    ─────────────────────────────────────────────────────────────────
    BUY  → SL = dernier Swing Low significatif  - buffer ATR
    SELL → SL = dernier Swing High significatif + buffer ATR

    Buffer = ATR * 0.20  (évite les faux déclenchements)
    Fallback : si aucun swing trouvé → ATR * 1.2 depuis entry

    Contraintes :
    - SL ne peut pas être à plus de ATR * 4 de l'entry (évite SL absurdes)
    - SL ne peut pas être à moins de ATR * 0.5 (trop serré = stop hunt)
    - Pour BUY  : SL doit être SOUS l'entry
    - Pour SELL : SL doit être AU-DESSUS de l'entry
    """
    recent = candles[-lookback:] if len(candles) >= lookback else candles
    buf    = atr_val * 0.20
    sp_val = spread_pips * pip
    max_sl = atr_val * 4.0
    min_sl = atr_val * 0.5

    H, L = swings(recent, n=3)

    if bias == "BULLISH":
        # Chercher le dernier swing low SOUS l'entry
        candidates = [lv for _, lv in reversed(L) if lv < entry]
        if candidates:
            # Prendre le swing low le plus proche de l'entry (le plus haut)
            best_low = max(candidates)
            sl = best_low - buf - sp_val
        else:
            # Fallback : ATR * 1.2 sous l'entry
            sl = entry - atr_val * 1.2 - sp_val
        # Contraintes
        dist = entry - sl
        if dist > max_sl: sl = entry - max_sl
        if dist < min_sl: sl = entry - min_sl
        return sl

    else:  # BEARISH / SELL
        # Chercher le dernier swing high AU-DESSUS de l'entry
        candidates = [hv for _, hv in reversed(H) if hv > entry]
        if candidates:
            # Prendre le swing high le plus proche de l'entry (le plus bas)
            best_high = min(candidates)
            sl = best_high + buf + sp_val
        else:
            # Fallback : ATR * 1.2 au-dessus de l'entry
            sl = entry + atr_val * 1.2 + sp_val
        # Contraintes
        dist = sl - entry
        if dist > max_sl: sl = entry + max_sl
        if dist < min_sl: sl = entry + min_sl
        return sl

def eqh_eql(c,tol=0.0003):
    hi=[x["h"] for x in c[-40:]]; lo=[x["l"] for x in c[-40:]]
    eqh=eql=None
    for i in range(len(hi)-1):
        for j in range(i+1,len(hi)):
            if hi[i] and abs(hi[i]-hi[j])/hi[i]<=tol: eqh=max(hi[i],hi[j]); break
        if eqh: break
    for i in range(len(lo)-1):
        for j in range(i+1,len(lo)):
            if lo[i] and abs(lo[i]-lo[j])/lo[i]<=tol: eql=min(lo[i],lo[j]); break
        if eql: break
    return eqh,eql

def choch_seq(c):
    if len(c)<20: return None,0
    H,L=swings(c,n=3)
    if len(H)<3 or len(L)<3: return None,0
    bear=bull=0
    for k in range(min(3,len(H)-1)):
        if H[-(k+1)][1]<H[-(k+2)][1]: bear+=1
        else: break
    for k in range(min(3,len(L)-1)):
        if L[-(k+1)][1]>L[-(k+2)][1]: bull+=1
        else: break
    if bear>=2: return "BEARISH",bear
    if bull>=2: return "BULLISH",bull
    if bear==1: return "BEARISH",1
    if bull==1: return "BULLISH",1
    return None,0

def detect_bias(c):
    H,L=swings(c,n=3); last=c[-1]["c"]; closes=[x["c"] for x in c]
    cd,cc=choch_seq(c)
    if cc>=2:
        if cd=="BEARISH": return "BEARISH",min(x["l"] for x in c[-10:]),"CHoCHx{}".format(cc)
        if cd=="BULLISH": return "BULLISH",max(x["h"] for x in c[-10:]),"CHoCHx{}".format(cc)
    if len(H)>=2 and len(L)>=2:
        sh1,sh2=H[-1][1],H[-2][1]; sl1,sl2=L[-1][1],L[-2][1]
        if sh1>sh2 and sl1>sl2 and last>sh2: return "BULLISH",sh1,"BOS"
        if sh1<sh2 and sl1<sl2 and last<sl2: return "BEARISH",sl1,"BOS"
        if last>sh1 and sl1>sl2: return "BULLISH",sh1,"CHoCH"
        if last<sl1 and sh1<sh2: return "BEARISH",sl1,"CHoCH"
    ema20=sum(closes[-20:])/20 if len(closes)>=20 else closes[-1]
    ema50=sum(closes[-50:])/50 if len(closes)>=50 else closes[-1]
    if last>ema20 and ema20>ema50: return "BULLISH",max(x["h"] for x in c[-10:]),"TREND"
    if last<ema20 and ema20<ema50: return "BEARISH",min(x["l"] for x in c[-10:]),"TREND"
    if len(closes)>=8:
        slope=(closes[-1]-closes[-8])/closes[-8]
        if slope>0.0005: return "BULLISH",max(x["h"] for x in c[-8:]),"TREND"
        if slope<-0.0005: return "BEARISH",min(x["l"] for x in c[-8:]),"TREND"
    return "NEUTRAL",None,None

def breakers(c,b,lookback=100):
    last=c[-1]["c"]; res=[]; a=atr(c)
    scan=c[-lookback:] if len(c)>lookback else c
    for i in range(2,len(scan)-2):
        ci=scan[i]; co=ci["o"]; cc=ci["c"]; fut=scan[i+1:]
        if b=="BULLISH":
            if cc>=co: continue
            if not any(f["c"]>co for f in fut): continue
            if cc-a*3<=last<=co+a*3: res.append({"top":co,"bottom":cc,"strength":abs(co-cc),"dist":abs(last-(co+cc)/2)})
        else:
            if cc<=co: continue
            if not any(f["c"]<co for f in fut): continue
            if co-a*3<=last<=cc+a*3: res.append({"top":cc,"bottom":co,"strength":abs(cc-co),"dist":abs(last-(co+cc)/2)})
    res.sort(key=lambda x:(-x["strength"],x["dist"]))
    return res

def conf_score(c,b):
    if len(c)<3: return 0
    c1,c2,c3=c[-1],c[-2],c[-3]; o,cc,h,l=c1["o"],c1["c"],c1["h"],c1["l"]
    body=abs(cc-o); rng=h-l
    if rng==0: return 0
    r=body/rng; s=0
    if b=="BULLISH":
        if cc>o: s+=35
        if r>0.5: s+=25
        if min(o,cc)-l>body*0.15: s+=20
        if c2["c"]<cc: s+=10
        if c3["c"]<c2["c"]: s+=5
        if cc>c2["h"]: s+=5
    else:
        if cc<o: s+=35
        if r>0.5: s+=25
        if h-max(o,cc)>body*0.15: s+=20
        if c2["c"]>cc: s+=10
        if c3["c"]>c2["c"]: s+=5
        if cc<c2["l"]: s+=5
    cd,cc2=choch_seq(c)
    if cc2>=2 and cd==b: s+=min(15,cc2*7)
    eq_h,eq_l=eqh_eql(c)
    lp=c[-1]["c"]
    if b=="BEARISH" and eq_h and abs(lp-eq_h)/eq_h<0.005: s+=10
    if b=="BULLISH" and eq_l and abs(lp-eq_l)/eq_l<0.005: s+=10
    return min(s,110)

def fvg(c,bias,look=40):
    if len(c)<3: return None
    scan=c[-look:] if len(c)>look else c; lp=c[-1]["c"]; best=None
    for i in range(1,len(scan)-1):
        if bias=="BULLISH":
            fl,fh=scan[i-1]["h"],scan[i+1]["l"]
            if fh>fl and fl*0.998<=lp<=fh*1.002:
                sz=fh-fl
                if best is None or sz>(best[1]-best[0]): best=(fl,fh)
        else:
            fh2,fl2=scan[i-1]["l"],scan[i+1]["h"]
            if fh2>fl2 and fl2*0.998<=lp<=fh2*1.002:
                sz=fh2-fl2
                if best is None or sz>(best[1]-best[0]): best=(fl2,fh2)
    return best

def ote_zone(sh,sl,bias):
    rng=sh-sl
    if rng<=0: return None,None
    if bias=="BULLISH": return sh-rng*0.786,sh-rng*0.618
    return sl+rng*0.618,sl+rng*0.786

# ══════════════════════════════════════════════════════
#  📐 PATTERNS TECHNIQUES M5 — Bonus score
#  Tous optionnels : augmentent le score, ne bloquent pas
# ══════════════════════════════════════════════════════

def pat_head_shoulders(c, bias):
    """Head & Shoulders → bearish / Inverse H&S → bullish"""
    if len(c) < 20: return False
    if bias == "BEARISH":
        highs = [x["h"] for x in c[-20:]]
        h1 = max(highs[:7]); h2 = max(highs[6:14]); h3 = max(highs[13:])
        return h2 > h1 * 1.001 and h2 > h3 * 1.001 and abs(h1-h3)/h2 < 0.015
    else:
        lows = [x["l"] for x in c[-20:]]
        l1 = min(lows[:7]); l2 = min(lows[6:14]); l3 = min(lows[13:])
        return l2 < l1 * 0.999 and l2 < l3 * 0.999 and abs(l1-l3)/l2 < 0.015

def pat_double_top_bottom(c, bias, tol=0.0015):
    """Double Top (bearish) / Double Bottom (bullish)"""
    if len(c) < 15: return False
    if bias == "BEARISH":
        highs = [x["h"] for x in c[-15:]]
        h1 = max(highs[:7]); h2 = max(highs[7:])
        return h1 > 0 and abs(h1-h2)/h1 < tol
    else:
        lows = [x["l"] for x in c[-15:]]
        l1 = min(lows[:7]); l2 = min(lows[7:])
        return l1 > 0 and abs(l1-l2)/l1 < tol

def pat_breakout_retest(c, bias):
    """Cassure + retest du niveau — confirmation de continuation"""
    if len(c) < 10: return False
    last = c[-1]; prev = c[-2]; prev2 = c[-3]
    if bias == "BULLISH":
        # Cassure d'un high récent + clôture dessus
        rh = max(x["h"] for x in c[-10:-2])
        return last["c"] > rh and prev["c"] > rh and prev2["c"] < rh
    else:
        rl = min(x["l"] for x in c[-10:-2])
        return last["c"] < rl and prev["c"] < rl and prev2["c"] > rl

def pat_fake_breakout(c, bias):
    """Fake breakout (stop hunt visible sur M5) aligné avec le bias"""
    if len(c) < 5: return False
    last = c[-1]; prev = c[-2]
    if bias == "BULLISH":
        # Spike bas puis rejet haussier
        lower_wick = min(last["o"], last["c"]) - last["l"]
        body = abs(last["c"] - last["o"])
        return last["c"] > prev["l"] and lower_wick > body * 1.5 and last["c"] > last["o"]
    else:
        upper_wick = last["h"] - max(last["o"], last["c"])
        body = abs(last["c"] - last["o"])
        return last["c"] < prev["h"] and upper_wick > body * 1.5 and last["c"] < last["o"]

def pattern_score_m5(c, bias):
    """
    Calcule le bonus de score total des patterns M5.
    Retourne (score_bonus, liste_badges).
    Max +50 pts. Tous optionnels.
    """
    if not c or len(c) < 20: return 0, []
    score = 0; badges = []
    if pat_head_shoulders(c, bias):
        score += 15
        badges.append("H&S ✓" if bias=="BEARISH" else "IH&S ✓")
    if pat_double_top_bottom(c, bias):
        score += 12
        badges.append("Double Top ✓" if bias=="BEARISH" else "Double Bot ✓")
    if pat_breakout_retest(c, bias):
        score += 18
        badges.append("Breakout ✓")
    if pat_fake_breakout(c, bias):
        score += 15
        badges.append("Fake BO ✓")
    return min(score, 50), badges

# ══════════════════════════════════════════════════════
#  AGENT ANALYZE PRINCIPAL
# ══════════════════════════════════════════════════════
def news_check():
    """Rétrocompatibilité — appelle news_filter() en interne."""
    status, title, _ = news_filter()
    if status == "BLOCK":
        return False, "⚠️ News HIGH: {}".format((title or "?")[:30])
    return True, "✅ OK"

# ── Cache news pour éviter de répéter les appels HTTP ─────────────
_news_cache = {"data": None, "ts": 0}
_NEWS_CACHE_SEC = 300  # 5 min

def _get_news_data():
    global _news_cache
    if time.time() - _news_cache["ts"] < _NEWS_CACHE_SEC and _news_cache["data"]:
        return _news_cache["data"]
    try:
        data = json.loads(http_get("https://nfs.faireconomy.media/ff_calendar_thisweek.json", timeout=8))
        _news_cache = {"data": data, "ts": time.time()}
        return data
    except:
        return _news_cache["data"] or []

def news_filter():
    """
    Filtre news intelligent — 3 niveaux :
    BLOCK   : news HIGH dans les 30 min → pas de signal
    CAUTION : news HIGH dans les 2h → score -10
    OK      : aucun risque immédiat
    Retourne (status, title, score_adj)
    """
    try:
        data = _get_news_data()
        now  = datetime.now(timezone.utc).replace(tzinfo=None)
        for evt in data:
            if evt.get("impact","") != "High": continue
            try:
                et   = datetime.strptime(evt["date"], "%Y-%m-%dT%H:%M:%S%z").replace(tzinfo=None)
                diff = abs((et - now).total_seconds())
                if diff < 1800:  return "BLOCK",   evt.get("title","?"), 0
                if diff < 7200:  return "CAUTION",  evt.get("title","?"), -10
            except: pass
        return "OK", None, 0
    except:
        return "OK", None, 0

# ── Biais fondamental par devise ──────────────────────────────────
_fund_cache = {"bias": {}, "ts": 0}
_FUND_CACHE_SEC = 600  # 10 min

CURRENCY_MAP = {
    "EURUSD": ("EUR", "USD"), "GBPUSD": ("GBP", "USD"),
    "USDJPY": ("USD", "JPY"), "GBPJPY": ("GBP", "JPY"),
    "EURJPY": ("EUR", "JPY"), "AUDUSD": ("AUD", "USD"),
    "AUDJPY": ("AUD", "JPY"), "CADJPY": ("CAD", "JPY"),
    "USDCHF": ("USD", "CHF"), "NZDUSD": ("NZD", "USD"),
    "USDCAD": ("USD", "CAD"), "XAUUSD": ("XAU", "USD"),
    "XAGUSD": ("XAG", "USD"), "BTCUSD": ("BTC", "USD"),
    "NAS100": ("USA", "USD"), "SPX500": ("USA", "USD"),
    "US30":   ("USA", "USD"), "USOIL":  ("OIL", "USD"),
}

NEWS_WEIGHTS = {
    "interest rate": 3, "fed ":3, "fomc":3, "ecb":3, "boe":3, "boj":3,
    "nfp":2, "non-farm":2, "payroll":2, "cpi":2, "inflation":2, "pce":2,
    "gdp":2, "employment":1, "retail":1, "pmi":1, "unemployment":1,
}

def fundamental_bias(pair_name):
    """
    Calcule le biais fondamental d'une paire selon les news de la semaine.
    Retourne (base_score, quote_score, badge_str)
    """
    global _fund_cache
    if time.time() - _fund_cache["ts"] < _FUND_CACHE_SEC:
        cached = _fund_cache["bias"].get(pair_name)
        if cached: return cached

    try:
        data = _get_news_data()
        currencies = CURRENCY_MAP.get(pair_name, (None, None))
        scores = {c: 0 for c in currencies if c}

        for evt in data:
            cur = evt.get("currency","")
            if cur not in scores: continue
            title = evt.get("title","").lower()
            impact = evt.get("impact","")
            w = 0
            for kw, weight in NEWS_WEIGHTS.items():
                if kw in title: w = max(w, weight)
            if impact == "High" and w == 0: w = 1
            scores[cur] = scores.get(cur, 0) + w

        base, quote = currencies
        bs = scores.get(base, 0); qs = scores.get(quote, 0)
        diff = bs - qs

        if diff >= 3:    result = ("BASE_STRONG",  "+{}pts macro".format(diff))
        elif diff <= -3: result = ("QUOTE_STRONG", "+{}pts macro".format(abs(diff)))
        else:            result = ("NEUTRAL", "")

        _fund_cache["bias"][pair_name] = (bs, qs, result[0], result[1])
        if time.time() - _fund_cache["ts"] > _FUND_CACHE_SEC:
            _fund_cache["ts"] = time.time()

        return bs, qs, result[0], result[1]
    except:
        return 0, 0, "NEUTRAL", ""

def fundamental_score_adj(pair_name, bias_tech):
    """
    Retourne (score_adj, badge) selon alignement technique/fondamental.
    Alignement  → +12 pts
    Contradiction → -15 pts
    Neutre       → 0 pts
    """
    try:
        bs, qs, fund, badge = fundamental_bias(pair_name)
        base, quote = CURRENCY_MAP.get(pair_name, (None, None))
        if not base or fund == "NEUTRAL": return 0, ""

        # BASE_STRONG = base devise forte = signal BEARISH pour quote (ex USD fort = SELL EURUSD)
        if fund == "BASE_STRONG":
            if bias_tech == "BEARISH": return +12, "Macro ✓"   # aligné
            if bias_tech == "BULLISH": return -15, ""           # contre macro
        if fund == "QUOTE_STRONG":
            if bias_tech == "BULLISH": return +12, "Macro ✓"   # aligné
            if bias_tech == "BEARISH": return -15, ""           # contre macro
        return 0, ""
    except:
        return 0, ""




# ══════════════════════════════════════════════════════
#  🌊 AGENT LIQUIDITÉ — Condition OBLIGATOIRE
# ══════════════════════════════════════════════════════
# Avant tout signal, on vérifie que le prix a PRIS de la liquidité.
# Sans ça = signal refusé. Aucune exception.
#
# 3 types de prise de liquidité détectés :
#   1. SWEEP      : franchissement d'un swing high/low récent + retour
#   2. STOP_HUNT  : spike rapide + rejet violent (wick > 2×corps)
#   3. EQH_EQL    : Equal Highs ou Equal Lows touchés (pool de liquidité)

def agent_liquidity(candles, bias, lookback=40):
    """
    Retourne un dict décrivant la prise de liquidité ou None si pas détectée.
    {
      "type"  : "SWEEP" | "STOP_HUNT" | "EQH_EQL",
      "level" : float,        # niveau de liquidité touché
      "score" : int,          # bonus score (+10 à +25)
      "label" : str,          # texte affiché dans le message signal
    }
    """
    if not candles or len(candles) < 10:
        return None

    c     = candles[-lookback:] if len(candles) > lookback else candles
    last  = c[-1]
    lp    = last["c"]
    a     = atr(candles)

    # ── 1. SWEEP : franchissement swing récent + retour dans le range ──
    highs = [x["h"] for x in c[:-3]]
    lows  = [x["l"] for x in c[:-3]]
    if highs and lows:
        prev_hh = max(highs)  # plus haut récent
        prev_ll = min(lows)   # plus bas récent
        cur_h   = last["h"]; cur_l = last["l"]

        if bias == "BULLISH" and cur_l < prev_ll and lp > prev_ll:
            # Prix a cassé le plus bas → swept les longs stops → remonte
            return {"type":"SWEEP", "level":round(prev_ll,5),
                    "score":20, "label":"Sweep LL ✓"}

        if bias == "BEARISH" and cur_h > prev_hh and lp < prev_hh:
            # Prix a cassé le plus haut → swept les short stops → redescend
            return {"type":"SWEEP", "level":round(prev_hh,5),
                    "score":20, "label":"Sweep HH ✓"}

    # ── 2. STOP HUNT : spike + rejet violent (wick long = 2× corps min) ─
    for i in range(-1, -min(5, len(c)), -1):
        cv = c[i]
        body   = abs(cv["c"] - cv["o"])
        if body < a * 0.05:
            continue  # doji, ignorer
        if bias == "BULLISH":
            lower_wick = cv["o"] - cv["l"] if cv["c"] >= cv["o"] else cv["c"] - cv["l"]
            if lower_wick > body * 2.0 and cv["c"] > cv["o"]:
                return {"type":"STOP_HUNT", "level":round(cv["l"], 5),
                        "score":18, "label":"Stop Hunt bas ✓"}
        else:
            upper_wick = cv["h"] - cv["o"] if cv["c"] <= cv["o"] else cv["h"] - cv["c"]
            if upper_wick > body * 2.0 and cv["c"] < cv["o"]:
                return {"type":"STOP_HUNT", "level":round(cv["h"], 5),
                        "score":18, "label":"Stop Hunt haut ✓"}

    # ── 3. EQH / EQL : pool de liquidité externe touché ─────────────────
    tol   = 0.0004
    highs2 = [x["h"] for x in c]
    lows2  = [x["l"] for x in c]
    # Chercher deux highs très proches (Equal Highs) touchés par la bougie actuelle
    if bias == "BEARISH":
        eq_highs = []
        for i in range(len(highs2) - 2):
            for j in range(i + 1, len(highs2) - 1):
                if highs2[i] > 0 and abs(highs2[i] - highs2[j]) / highs2[i] <= tol:
                    eq_highs.append(max(highs2[i], highs2[j]))
        if eq_highs:
            eqh = max(eq_highs)
            if last["h"] >= eqh * (1 - tol) and lp < eqh:
                return {"type":"EQH_EQL", "level":round(eqh, 5),
                        "score":15, "label":"EQH prise ✓"}

    if bias == "BULLISH":
        eq_lows = []
        for i in range(len(lows2) - 2):
            for j in range(i + 1, len(lows2) - 1):
                if lows2[i] > 0 and abs(lows2[i] - lows2[j]) / lows2[i] <= tol:
                    eq_lows.append(min(lows2[i], lows2[j]))
        if eq_lows:
            eql = min(eq_lows)
            if last["l"] <= eql * (1 + tol) and lp > eql:
                return {"type":"EQH_EQL", "level":round(eql, 5),
                        "score":15, "label":"EQL prise ✓"}

    return None  # Pas de prise de liquidité détectée → signal refusé

def agent_analyze(m, score_min, news_ok, q):
    """
    Analyse multi-timeframe v11 :
      H1  → tendance de fond (obligatoire)
      M15 → Order Block + structure (obligatoire)
      M5  → confirmation d'entrée précise (nouveau — fortement pondéré)
      M1  → ultra-précision optionnelle (bonus léger)

    RR minimum : 3.0 normal / 1.5 scalp week-end
    Obligatoires : biais H1 + OB M15 + liquidité M15
    M5 aligné → +bonus fort  |  M5 contraire → pénalité
    """
    try:
        sn, _, _, _ = get_session()
        mode   = get_trade_mode(m)
        rr_min = 2.0 if mode == "SCALP" else 3.0

        # ── Filtre session FOREX ──────────────────────────────────
        if m["cat"] == "FOREX" and sn not in ("LONDON_KZ", "OVERLAP", "NY", "LONDON"):
            q.put({"name": m["name"], "cat": m["cat"], "found": False,
                   "reason": "Session FOREX inactive ({})".format(sn), "improv": False})
            return

        # ── H1 : tendance de fond (obligatoire) ──────────────────
        h1 = fetch_c(m["sym"], "1h", "30d") or fetch_c(m["sym"], "4h", "60d")
        if not h1 or len(h1) < 10:
            q.put({"name": m["name"], "cat": m["cat"], "found": False,
                   "reason": "H1 insuffisant"}); return
        b, bos, bt = detect_bias(h1)
        if b == "NEUTRAL":
            q.put({"name": m["name"], "cat": m["cat"], "found": False,
                   "reason": "Neutre H1"}); return

        # ── Confirmation tendance H1 ──────────────────────────────
        cd2, cc2 = choch_seq(h1)
        h1_closes = [x["c"] for x in h1[-50:]]
        h1_ema20  = sum(h1_closes[-20:]) / 20 if len(h1_closes) >= 20 else h1_closes[-1]
        h1_ema50  = sum(h1_closes[-50:]) / 50 if len(h1_closes) >= 50 else h1_closes[-1]
        trend_score = 0
        if cc2 >= 1: trend_score += 1
        if cc2 >= 2: trend_score += 1
        if b == "BULLISH" and h1_ema20 > h1_ema50: trend_score += 1
        if b == "BEARISH" and h1_ema20 < h1_ema50: trend_score += 1
        H_sw, L_sw = swings(h1, n=3)
        if b == "BULLISH" and len(H_sw) >= 2 and H_sw[-1][1] > H_sw[-2][1]: trend_score += 1
        if b == "BEARISH" and len(L_sw) >= 2 and L_sw[-1][1] < L_sw[-2][1]: trend_score += 1
        if trend_score == 0:
            q.put({"name": m["name"], "cat": m["cat"], "found": False,
                   "reason": "Tendance H1 faible"}); return

        time.sleep(0.08)

        # ── M15 : structure + OB (obligatoire) ───────────────────
        m15 = fetch_c(m["sym"], "15m", "10d")
        if not m15 or len(m15) < 10:
            q.put({"name": m["name"], "cat": m["cat"], "found": False,
                   "reason": "M15 indispo"}); return

        # ── Spread ────────────────────────────────────────────────
        last5 = [abs(x["h"] - x["l"]) for x in m15[-5:] if x["h"] != x["l"]]
        sp    = round(min(last5) / m["pip"] * 0.03, 2) if last5 else 0
        if sp > m["max_sp"] * 1.5:
            q.put({"name": m["name"], "cat": m["cat"], "found": False,
                   "reason": "Spread large"}); return

        lp       = m15[-1]["c"]
        sh_h1    = max(x["h"] for x in h1[-50:])
        sl_h1    = min(x["l"] for x in h1[-50:])
        ote_lo, ote_hi = ote_zone(sh_h1, sl_h1, b)
        in_ote   = bool(ote_lo and ote_hi and ote_lo <= lp <= ote_hi)
        fvg_z    = fvg(m15, b)
        bbs      = breakers(m15, b)
        sc       = conf_score(m15, b)

        # ── Bonus M15 optionnels ──────────────────────────────────
        if in_ote:          sc = min(sc + 12, 115)
        if fvg_z:           sc = min(sc + 15, 115)
        if cc2 >= 2:        sc = min(sc + 10, 115)
        if trend_score >= 3: sc = min(sc + 8,  115)

        # ── News filtre ───────────────────────────────────────────
        news_status, news_title, news_adj = news_filter()
        if news_status == "BLOCK":
            q.put({"name": m["name"], "cat": m["cat"], "found": False,
                   "reason": "News BLOCK: {}".format((news_title or "?")[:25]),
                   "improv": False}); return
        if news_status == "CAUTION":
            sc = max(0, sc + news_adj)

        # ── Fondamental ───────────────────────────────────────────
        fund_adj, fund_badge = fundamental_score_adj(m["name"], b)
        if fund_adj != 0:
            sc = min(max(0, sc + fund_adj), 115)

        # ── M5 : TIMEFRAME D'ENTRÉE (nouveau v11) ─────────────────
        # Charge M5 une seule fois — utilisé pour patterns ET entrée
        m5_raw = fetch_c(m["sym"], "5m", "3d")
        m5_conf = {
            "ok": False, "bias_ok": False, "score": 0,
            "badges": [], "liq": None, "fvg": None, "ob": None,
            "choch": 0, "details": "M5 indispo"
        }
        if m5_raw and len(m5_raw) >= 15:
            m5_sl    = m5_raw[-50:] if len(m5_raw) >= 50 else m5_raw
            m5_bias, _, m5_bt = detect_bias(m5_sl)
            m5_liq   = agent_liquidity(m5_sl[-20:], b) if len(m5_sl) >= 20 else None
            m5_fvg   = fvg(m5_sl, b, look=20)
            m5_obs   = breakers(m5_sl, b)
            m5_cd, m5_cc = choch_seq(m5_sl)
            m5_ema   = sum(x["c"] for x in m5_sl[-10:]) / 10 if len(m5_sl) >= 10 else lp

            m5_bonus  = 0
            m5_badges = []

            # Biais M5 aligné avec H1 → fondation
            if m5_bias == b:
                m5_bonus += 10
                m5_badges.append("M5-Trend✓")
                m5_conf["bias_ok"] = True
            # Liquidité M5 (stop hunt / sweep / EQH-EQL)
            if m5_liq:
                m5_bonus += 15
                m5_badges.append("M5-{}".format(m5_liq["label"].replace(" ✓", "")))
                m5_conf["liq"] = m5_liq
            # FVG M5 actif
            if m5_fvg:
                m5_bonus += 10
                m5_badges.append("M5-FVG✓")
                m5_conf["fvg"] = m5_fvg
            # Order Block M5
            if m5_obs:
                m5_bonus += 8
                m5_badges.append("M5-OB✓")
                m5_conf["ob"] = m5_obs[0]
            # CHoCH M5 (confirmation de changement de structure)
            if m5_cc >= 2 and m5_cd == b[:4].rstrip("ISH"):
                m5_bonus += 7
                m5_badges.append("M5-CHoCH✓")
                m5_conf["choch"] = m5_cc

            m5_conf["score"]   = m5_bonus
            m5_conf["badges"]  = m5_badges
            m5_conf["ok"]      = m5_bonus >= 10  # au moins 1 confirmation M5

            detail_parts = []
            if m5_conf["bias_ok"]: detail_parts.append("biais✓")
            if m5_conf["liq"]:     detail_parts.append("liq✓")
            if m5_conf["fvg"]:     detail_parts.append("fvg✓")
            if m5_conf["ob"]:      detail_parts.append("ob✓")
            m5_conf["details"] = " · ".join(detail_parts) if detail_parts else "pas de setup"

            if m5_conf["ok"]:
                sc = min(sc + m5_bonus, 115)     # fort bonus si M5 confirme
            elif not m5_conf["bias_ok"]:
                sc = max(0, sc - 12)              # M5 contraire → pénalité
        else:
            m5_raw = None  # pas de données M5

        # ── Patterns M5 (visuels — bonus score) ──────────────────
        pat_bonus, pat_badges = pattern_score_m5(m5_raw, b) if m5_raw else (0, [])
        if pat_bonus > 0:
            sc = min(sc + pat_bonus, 115)

        # ── M1 : TF d'entrée principal (remplace bonus optionnel) ──
        m1 = fetch_c(m["sym"], "1m", "2d")
        m1_conf = {"ok": False, "badges": [], "score": 0, "details": "M1 indispo"}
        if m1 and len(m1) >= 15:
            m1_sl   = m1[-60:] if len(m1) >= 60 else m1
            m1_bias, _, _ = detect_bias(m1_sl)
            m1_liq  = agent_liquidity(m1_sl[-20:], b) if len(m1_sl) >= 20 else None
            m1_fvg  = fvg(m1_sl, b, look=15)
            m1_obs  = breakers(m1_sl, b)
            m1_cd, m1_cc = choch_seq(m1_sl)

            m1_bonus  = 0
            m1_badges = []

            if m1_bias == b:
                m1_bonus += 15; m1_badges.append("M1-Trend✓")
                m1_conf["ok"] = True
            if m1_liq:
                m1_bonus += 12; m1_badges.append("M1-Liq✓")
                m1_conf["liq"] = m1_liq
            if m1_fvg:
                m1_bonus += 10; m1_badges.append("M1-FVG✓")
            if m1_obs:
                m1_bonus += 8;  m1_badges.append("M1-OB✓")
            if m1_cc >= 2 and m1_cd:
                m1_bonus += 7;  m1_badges.append("M1-CHoCH✓")

            m1_conf["score"]  = m1_bonus
            m1_conf["badges"] = m1_badges
            m1_conf["details"] = " · ".join(m1_badges) if m1_badges else "pas de setup M1"

            if m1_bias == b:
                sc = min(sc + m1_bonus, 115)   # fort bonus si M1 confirme
            else:
                sc = max(0, sc - 10)            # M1 contraire → pénalité

        # ── Mémoire IA ────────────────────────────────────────────
        _tmp_badges = []
        if in_ote:  _tmp_badges.append("OTE")
        if fvg_z:   _tmp_badges.append("FVG")
        if cc2 >= 2: _tmp_badges.append("CHoCH")
        _tmp_key = "{}|{}|{}".format(m["name"], sn, "+".join(_tmp_badges) or "BASE")
        sc, mem_badge = mem_adj_score(_tmp_key, sc)

        # ── Liquidité M15 (OBLIGATOIRE) ───────────────────────────
        liq = agent_liquidity(m15, b)
        if not liq:
            q.put({"name": m["name"], "cat": m["cat"], "found": False,
                   "reason": "No liquidity sweep M15"}); return
        sc = min(sc + liq["score"], 115)

        a     = atr(m15)
        a_pct = a / (lp + 0.0001)
        s_min = score_min + (m.get("vol", 3) - 3) * 2

        # ── Priorité paire ────────────────────────────────────────
        prio = MARKET_PRIORITY.get(m["name"], 0)
        if prio > 0:
            sc = min(sc + prio, 115)

        sig = None

        # ── Construction signal — OB M15 obligatoire ─────────────
        if bbs and sc >= s_min and (news_ok or sc >= s_min + 5):
            bb   = bbs[0]
            e    = lp
            sp_p = sp * m["pip"]   # spread en prix (utilisé pour TP net)
            eq_h, eq_l = eqh_eql(m15)

            # Construire les badges finaux (ordre logique : HTF → LTF)
            all_badges = [liq["label"]]
            if in_ote:              all_badges.append("OTE ✓")
            if fvg_z:               all_badges.append("FVG M15 ✓")
            if cc2 >= 2:            all_badges.append("CHoCHx{} H1 ✓".format(cc2))
            all_badges.extend(m5_conf["badges"])     # badges M5 inline
            if pat_badges:          all_badges.extend(pat_badges)
            if fund_badge:          all_badges.append(fund_badge)
            if mem_badge:           all_badges.append(mem_badge)
            if mode == "SCALP":     all_badges.append("⚡ SCALP")
            if m1 and len(m1) >= 5: all_badges.append("M1✓")

            # Tag timeframe selon confirmations disponibles
            # H1 = tendance de fond (interne), entrée = M5 max M15
            if m5_conf["ok"]:
                tf_parts = ["M5", "M15"]
            else:
                tf_parts = ["M15"]
            if m1 and len(m1) >= 5: tf_parts.append("M1")
            tf_tag = "+".join(tf_parts)  # ex: "M5+M15" ou "M15"

            dp = 2 if e > 1000 else (3 if e > 10 else 5)
            f  = lambda v: round(v, dp)
            pip = m["pip"]

            if b == "BULLISH":
                # SL sur le dernier swing low de structure M15 (ICT)
                sl_p = sl_from_structure(m15, "BULLISH", a, e, pip,
                                         spread_pips=sp, lookback=40)
                sl_p = f(sl_p)
                risk = e - sl_p
                if risk > 0 and risk <= a * 12:
                    tp_eq = (eq_h * 0.9995) if (eq_h and e < eq_h < e + risk * 6) else None
                    tp    = tp_eq if tp_eq else e + risk * 3.0
                    gain_net = abs(tp - e) - sp_p
                    rr   = round(gain_net / (risk + sp_p), 1) if (risk + sp_p) > 0 else 0
                    if rr >= rr_min:
                        ptp = gain_net / pip; psl = (risk + sp_p) / pip
                        sig = {
                            "name": m["name"], "cat": m["cat"], "side": "BUY",
                            "entry": f(e), "tp": f(tp), "sl": f(sl_p), "rr": rr,
                            "score": sc, "score_min": s_min, "atr": f(a), "sp": sp,
                            "bias": b, "btype": bt,
                            "g001": round(ptp * 0.01, 2), "g01": round(ptp * 0.1, 2),
                            "g1": round(ptp, 2),
                            "l001": round(psl * 0.01, 2), "l01": round(psl * 0.1, 2),
                            "l1": round(psl, 2),
                            "badges": " · ".join(all_badges) + "  📊 " + tf_tag,
                            "time": datetime.now(timezone.utc).strftime("%H:%M"),
                            "liq": liq, "mode": mode, "risk_mult": 1.0,
                            "setup_key": _tmp_key,
                            "m5_conf": m5_conf, "m1_conf": m1_conf,   # données M5 complètes
                            "tf_tag": tf_tag,
                            # ── Fondamental (pour prompt IA) ──────────
                            "fund_base": bs, "fund_quote": qs,
                            "fund_bias": fund,
                            "fund_badge": fund_badge,
                            "news_status": news_status,
                            "news_title": news_title or "",
                        }

            else:  # BEARISH
                # SL sur le dernier swing high de structure M15 (ICT)
                sl_p = sl_from_structure(m15, "BEARISH", a, e, pip,
                                         spread_pips=sp, lookback=40)
                sl_p = f(sl_p)
                risk = sl_p - e
                if risk > 0 and risk <= a * 12:
                    tp_eq = (eq_l * 1.0005) if (eq_l and e - risk * 6 < eq_l < e) else None
                    tp    = tp_eq if tp_eq else e - risk * 3.0
                    gain_net = abs(tp - e) - sp_p
                    rr   = round(gain_net / (risk + sp_p), 1) if (risk + sp_p) > 0 else 0
                    if rr >= rr_min:
                        ptp = gain_net / pip; psl = (risk + sp_p) / pip
                        sig = {
                            "name": m["name"], "cat": m["cat"], "side": "SELL",
                            "entry": f(e), "tp": f(tp), "sl": f(sl_p), "rr": rr,
                            "score": sc, "score_min": s_min, "atr": f(a), "sp": sp,
                            "bias": b, "btype": bt,
                            "g001": round(ptp * 0.01, 2), "g01": round(ptp * 0.1, 2),
                            "g1": round(ptp, 2),
                            "l001": round(psl * 0.01, 2), "l01": round(psl * 0.1, 2),
                            "l1": round(psl, 2),
                            "badges": " · ".join(all_badges) + "  📊 " + tf_tag,
                            "time": datetime.now(timezone.utc).strftime("%H:%M"),
                            "liq": liq, "mode": mode, "risk_mult": 1.0,
                            "setup_key": _tmp_key,
                            "m5_conf": m5_conf,
                            "tf_tag": tf_tag,
                            # ── Fondamental (pour prompt IA) ──────────
                            "fund_base": bs, "fund_quote": qs,
                            "fund_bias": fund,
                            "fund_badge": fund_badge,
                            "news_status": news_status,
                            "news_title": news_title or "",
                        }

        if sig:
            q.put({"name": m["name"], "cat": m["cat"], "found": True,
                   "signal": sig, "improv": False})
        else:
            if bbs and sc >= s_min:
                reason = "RR<{:.1f}".format(rr_min)
            elif not bbs:
                reason = "No OB M15"
            elif not m5_conf["ok"]:
                reason = "M5 non aligné ({})".format(m5_conf["details"])
            else:
                reason = "Score {}/{}".format(sc, s_min)
            q.put({"name": m["name"], "cat": m["cat"], "found": False,
                   "reason": reason, "improv": False,
                   "sc": sc, "s_min": s_min, "m5_ok": m5_conf["ok"]})

    except Exception as ex:
        q.put({"name": m["name"], "cat": m["cat"], "found": False,
               "reason": str(ex)[:40], "improv": False})

# ══════════════════════════════════════════════════════════════════
#  BINANCE IA (Crypto futures)
# ══════════════════════════════════════════════════════════════════
AI_C   = defaultdict(lambda: defaultdict(deque))
AI_P   = {}
AI_PRS = []
AI_REG = {"regime":"RANGING","min_score":72,"risk_mult":1.0,"lev_cap":15,"label":"Init"}
AI_OT  = {}
AI_TC  = 0
AI_CD  = {}
_ai_lk = threading.Lock()
EXCH   = {}; EXCH_TS = 0

def b_get(ep, p=None):
    try:
        url="{}{}?{}".format(BINANCE_BASE,ep,urllib.parse.urlencode(p or {}))
        return json.loads(http_get(url,timeout=8))
    except: return None

def bn_price(sym):
    d=b_get("/ticker/price",{"symbol":sym}); return float(d["price"]) if d and "price" in d else None

def bn_klines(sym,tf="5m",lim=60):
    d=b_get("/klines",{"symbol":sym,"interval":tf,"limit":lim})
    if not d or not isinstance(d,list): return None
    return [{"ts":int(k[0]),"open":float(k[1]),"high":float(k[2]),"low":float(k[3]),"close":float(k[4]),"vol":float(k[5])} for k in d]

def bn_fund(sym):
    d=b_get("/premiumIndex",{"symbol":sym}); return float(d["lastFundingRate"])*100 if d and "lastFundingRate" in d else None

def refresh_exch():
    global EXCH_TS
    try:
        d=json.loads(http_get("{}/exchangeInfo".format(BINANCE_BASE),timeout=12))
        for s in d.get("symbols",[]):
            nm=s["symbol"]; info={"step":1.0,"minQty":0.0,"minNot":5.0,"tick":0.01}
            for f in s.get("filters",[]):
                if f["filterType"]=="LOT_SIZE": info["step"]=float(f["stepSize"]); info["minQty"]=float(f["minQty"])
                elif f["filterType"]=="MIN_NOTIONAL": info["minNot"]=float(f.get("notional",5.0))
                elif f["filterType"]=="PRICE_FILTER": info["tick"]=float(f["tickSize"])
            EXCH[nm]=info
        EXCH_TS=time.time(); log("AI",clr("Exchange info OK ({})".format(len(EXCH)),"g"))
    except Exception as e: log("WARN","[EXCH] {}".format(e))

def lot_calc(sym,risk,sld,entry,lev):
    info=EXCH.get(sym,{"step":0.001,"minQty":0.001,"minNot":5.0})
    step=info["step"]; minq=info["minQty"]; minn=info["minNot"]
    p=max(0,round(-math.log10(step))) if step>0 else 3
    qty=round(math.floor((risk/sld if sld>0 else 0)/step)*step,p); qty=max(qty,minq)
    not_=qty*entry
    if not_<minn: qty=round(math.floor(minn/entry*1.02/step)*step,p); qty=max(qty,minq); not_=qty*entry
    ft=not_*FEE_TAKER*2
    return {"qty":qty,"not":round(not_,4),"ft":round(ft,6),"rr":round(qty*sld+ft,4)}

def regime_detect():
    global AI_REG
    c4=list(AI_C["BTCUSDT"].get("4h",deque()))
    if len(c4)<20: return
    recent=c4[-20:]
    cl=[c["close"] for c in recent]; hi=[c["high"] for c in recent]; lo=[c["low"] for c in recent]
    a_raw=sum(h-l for h,l in zip(hi,lo))/len(recent)
    a_pct=a_raw/cl[-1]*100 if cl[-1]>0 else 0
    mom=(cl[-1]-cl[0])/cl[0]*100 if cl[0]>0 else 0
    mv=max(abs(c["close"]-c["open"])/c["open"]*100 for c in recent[-5:] if c["open"]>0)
    if a_pct>5 or mv>8:    r="CRISIS";  ms=95; rm=0.3; lc=3
    elif a_pct>3:           r="VOLATILE";ms=85; rm=0.6; lc=7
    elif abs(mom)>3:        r="TRENDING";ms=70; rm=1.2; lc=20
    elif (max(hi)-min(lo))/sum(cl)*len(cl)*100<3: r="ACCUM"; ms=76; rm=1.0; lc=15
    else:                   r="RANGING"; ms=78; rm=0.8; lc=10
    AI_REG={"regime":r,"min_score":ms,"risk_mult":rm,"lev_cap":lc,
             "atr_pct":round(a_pct,2),"mom":round(mom,2),"label":r}
    log("AI",clr("Régime: {} ATR:{:.1f}% Mom:{:.1f}%".format(r,a_pct,mom),"c"))

def refresh_ai():
    global AI_PRS
    try:
        d=b_get("/ticker/24hr")
        if d and isinstance(d,list):
            u=[t for t in d if t["symbol"].endswith("USDT") and "_" not in t["symbol"]]
            u.sort(key=lambda t:float(t.get("quoteVolume",0)),reverse=True)
            AI_PRS=[t["symbol"] for t in u[:25]]
    except: pass
    for sym in AI_PRS[:20]:
        for tf,lim in [("1m",120),("5m",60),("15m",40),("1h",48),("4h",50)]:
            c=bn_klines(sym,tf,lim)
            if c: AI_C[sym][tf]=deque(c,maxlen=lim)
            if tf=="5m" and c: AI_P[sym]=c[-1]["close"]
        time.sleep(0.07)
    regime_detect()
    log("AI",clr("Binance {} paires OK".format(len(AI_PRS)),"g"))

def ai_btc_bias():
    s={"BULL":0,"BEAR":0}
    for tf,w in [("5m",1),("1h",2),("4h",3)]:
        c=list(AI_C["BTCUSDT"].get(tf,deque()))
        if len(c)<5: continue
        cl=[x["close"] for x in c[-10:]]
        d=(cl[-1]-cl[0])/cl[0]*100 if cl[0]>0 else 0
        if d>0.3: s["BULL"]+=w
        elif d<-0.3: s["BEAR"]+=w
    if s["BULL"]>s["BEAR"]+1: return "BULL"
    if s["BEAR"]>s["BULL"]+1: return "BEAR"
    return "RANGE"

def ai_risk(bal,sc,am,sess):
    if bal<15: b=0.10
    elif bal<30: b=0.09
    elif bal<75: b=0.08
    else: b=0.06
    if sc>=90: b*=1.2
    elif sc>=80: b*=1.1
    b*=AI_REG.get("risk_mult",1.0)
    if "KZ" in sess or "OVERLAP" in sess: b*=1.1
    b*=(AM_MULT**am)
    return round(min(bal*b, bal*0.20),4)

def ai_lev(sym,bal,sc):
    if bal<15: base=5
    elif bal<30: base=7
    elif bal<75: base=10
    else: base=15
    if sc>=88: base=min(base+2,25)
    return min(base,AI_REG.get("lev_cap",15),PAIR_MAX_LEV.get(sym,20))

def ai_scan_sym(sym, bias, bal):
    """
    Challenge IA — scan une paire Binance Futures.
    Forex/BTC/Métaux/Indices simulés via BTCUSDT comme proxy de régime.
    """
    c5  = list(AI_C[sym].get("5m",  deque()))
    c15 = list(AI_C[sym].get("15m", deque()))
    if len(c5) < 12: return None
    ch = chal_get()
    if ch["balance"] < FLOOR_USD: return None
    dop = ch.get("day_open", ch["balance"])
    if dop > 0 and (dop - ch["balance"]) / dop >= DD_LIMIT: return None
    sn, _, _, _ = get_session()
    if sn == "OFF": return None
    reg = AI_REG
    cd  = AI_CD.get(sym)
    if cd and datetime.now(timezone.utc) < cd: return None
    with _ai_lk:
        if any(t["symbol"] == sym and t["status"] == "open" for t in AI_OT.values()): return None
    price = c5[-1]["close"]
    n     = len(c5)
    a_v   = sum(abs(x["close"] - x["open"]) for x in c5[-14:]) / 14 if len(c5) >= 14 else 0.01
    sig   = None; strat = "OB"
    for i in range(n-3, max(n-12, 2), -1):
        c0, c1, c2 = c5[i-2], c5[i-1], c5[i]
        b2 = abs(c1["close"] - c1["open"]); r = c1["high"] - c1["low"]
        if r == 0: continue
        bull_i = c2["close"] > c2["open"] and (c2["close"] - c2["open"]) > b2 * 1.0
        bear_i = c2["close"] < c2["open"] and (c2["open"]  - c2["close"]) > b2 * 1.0
        if c1["close"] < c1["open"] and bull_i and bias != "BEAR" and c1["low"] <= price <= c1["high"] * 1.004:
            sl = c1["low"] * 0.998; sld = price - sl
            if 0 < sld <= a_v * 4:
                sig = {"side":"BUY","entry":price,"sl":sl,"tp1":price+sld*2.5,"tp2":price+sld*5,"sc":68}; break
        if c1["close"] > c1["open"] and bear_i and bias != "BULL" and c1["low"] * 0.996 <= price <= c1["high"]:
            sl = c1["high"] * 1.002; sld = sl - price
            if 0 < sld <= a_v * 4:
                sig = {"side":"SELL","entry":price,"sl":sl,"tp1":price-sld*2.5,"tp2":price-sld*5,"sc":68}; break
    if not sig:
        rec  = c5[n-15:n-3] if n >= 15 else c5
        sh   = max(x["high"] for x in rec); sl2 = min(x["low"] for x in rec)
        if any(x["high"] > sh for x in c5[n-5:n-1]) and price < sh and bias != "BULL":
            sl_v = max(x["high"] for x in c5[n-5:n]) * 1.002; sld = sl_v - price
            if 0 < sld <= a_v * 4:
                sig = {"side":"SELL","entry":price,"sl":sl_v,"tp1":price-sld*3,"tp2":price-sld*6,"sc":72}; strat = "LIQ"
        if not sig and any(x["low"] < sl2 for x in c5[n-5:n-1]) and price > sl2 and bias != "BEAR":
            sl_v = min(x["low"] for x in c5[n-5:n]) * 0.998; sld = price - sl_v
            if 0 < sld <= a_v * 4:
                sig = {"side":"BUY","entry":price,"sl":sl_v,"tp1":price+sld*3,"tp2":price+sld*6,"sc":72}; strat = "LIQ"
    if not sig: return None
    sld  = abs(sig["entry"] - sig["sl"])
    sc   = sig["sc"] + sess_bonus(sn)
    w, l, _ = mem_query("{}|{}|{}".format(strat, sn, reg.get("regime","?")))
    t = w + l
    if t >= 3:
        wr = w / t
        if wr > 0.85:   sc += 8
        elif wr < 0.45: sc -= 12
    min_sc = reg.get("min_score", 72)
    if sc < min_sc: return None
    risk = ai_risk(bal, sc, ch["am_cycle"], sn)
    lev  = ai_lev(sym, bal, sc)
    lot  = lot_calc(sym, risk, sld, sig["entry"], lev)
    if not lot["qty"]: return None
    return {
        "sym":sym,"side":sig["side"],"entry":sig["entry"],"sl":sig["sl"],
        "tp1":sig["tp1"],"tp2":sig["tp2"],"sc":sc,"rr":round(abs(sig["tp1"]-sig["entry"])/sld,1),
        "risk":risk,"lev":lev,"qty":lot["qty"],"not":lot["not"],
        "ft":lot["ft"],"rr_real":lot["rr"],
        "strat":strat,"sess":sn,"regime":reg.get("regime","?"),
        "am":ch["am_cycle"],
    }


def ai_full_scan():
    """
    Challenge IA — scan toutes les paires actives Binance.
    Couvre BTC, ETH + top altcoins USDT, triés par volume.
    """
    bias = ai_btc_bias()
    ch   = chal_get(); bal = ch["balance"]
    res  = []
    for sym in AI_PRS[:20]:
        s = ai_scan_sym(sym, bias, bal)
        if s: res.append(s)
    res.sort(key=lambda x: (-x["sc"], -x["rr"]))
    return res


def ai_open(setup):
    """Ouvre une position Challenge IA simulée et notifie l'admin."""
    global AI_TC
    AI_TC += 1; tid = AI_TC; sym = setup["sym"]
    ch  = chal_get(); bal = ch["balance"]
    trade = {
        "id":tid,"symbol":sym,"side":setup["side"],
        "entry":setup["entry"],"sl":setup["sl"],"sl0":setup["sl"],
        "tp1":setup["tp1"],"tp2":setup["tp2"],
        "risk":setup["risk"],"rr":setup["rr"],"lev":setup["lev"],
        "qty":setup["qty"],"not":setup["not"],"ft":setup["ft"],
        "strat":setup["strat"],"sc":setup["sc"],"am":setup["am"],
        "sess":setup["sess"],"regime":setup["regime"],
        "status":"open","be":False,"tp1_hit":False,
        "open_ts":datetime.now(timezone.utc).isoformat(),
    }
    with _ai_lk:
        AI_OT[tid] = trade
        AI_CD[sym]  = datetime.now(timezone.utc) + timedelta(minutes=COOLDOWN_MIN)
    ch  = chal_get(); bal = ch["balance"]
    d   = "🟢 LONG" if setup["side"] == "BUY" else "🔴 SHORT"
    prog = chal_prog(ch)
    tg_send(ADMIN_ID,
        "<b>━━━ TRADE IA #{} ━━━</b>\n{} <b>{}</b>\n"
        "🎯 Score:{}/100  RR:1:{}\n"
        "📍 {:.5f}  🛑 {:.5f}\n"
        "✅ TP1:{:.5f}  🏆 TP2:{:.5f}\n"
        "📦 Qty:{}  {}$  Lev:{}x\n"
        "💸 Frais:{:.5f}$  Risk:{:.4f}$\n"
        "🕐 {}  🌍 {}  📊 {}\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "{}\n<b>@leaderOdg</b>".format(
            tid, d, sym, setup["sc"], setup["rr"],
            setup["entry"], setup["sl"], setup["tp1"], setup["tp2"],
            setup["qty"], round(setup["not"], 2), setup["lev"],
            setup["ft"], setup["risk"],
            setup["sess"], setup["regime"], setup["strat"], prog))
    if setup["sc"] >= 78:
        for puid in db_get_pro_users():
            tg_send(puid,
                "<b>📊 Signal IA #{} — {} {}</b>\n"
                "{} Score:{}/100 RR:1:{}\n"
                "📍 {:.5f} → TP:{:.5f} SL:{:.5f}\n"
                "<b>@leaderOdg</b>".format(
                    tid, sym, d, setup["strat"], setup["sc"], setup["rr"],
                    setup["entry"], setup["tp1"], setup["sl"]))
            time.sleep(0.04)
    log("AI", clr("#{} {} {} Sc:{} Qty:{} Risk:{:.4f}$".format(
        tid, sym, "L" if setup["side"] == "BUY" else "S",
        setup["sc"], setup["qty"], setup["risk"]), "g"))
    return tid


def ai_check():
    """Challenge IA désactivé — fonction conservée pour compatibilité."""
    pass
    ch = chal_get()
    for t in trades:
        if t["status"] != "open": continue
        price = bn_price(t["symbol"])
        if price is None: continue
        side  = t["side"]; entry = t["entry"]
        sl    = t["sl"];   tp1   = t["tp1"];  tp2 = t["tp2"]
        sld0  = abs(entry - t["sl0"])
        rrc   = ((price - entry) / sld0 if side == "BUY" else (entry - price) / sld0) if sld0 > 0 else 0
        # Break-Even à RR = 1.0
        if rrc >= 1.0 and not t["be"]:
            be = entry * 1.0002 if side == "BUY" else entry * 0.9998
            with _ai_lk: t["sl"] = be; t["be"] = True
            tg_send(ADMIN_ID, "<b>🔒 BE #{} — {}</b>\nRR:{:.2f} SL→{:.5f}\n<b>@leaderOdg</b>".format(
                t["id"], t["symbol"], rrc, be))
        hit_tp1 = (price >= tp1 if side == "BUY" else price <= tp1)
        if hit_tp1 and not t["tp1_hit"]:
            p = round(t["risk"] * rrc - t["ft"], 4)
            with _ai_lk: t["tp1_hit"] = True; t["sl"] = tp1
            tg_send(ADMIN_ID, "<b>✅ TP1 #{} — {}</b>\n+{:.4f}$ SL→TP2:{:.5f}\n<b>@leaderOdg</b>".format(
                t["id"], t["symbol"], p, tp2))
        hit_sl  = (price <= sl  if side == "BUY" else price >= sl)
        hit_tp2 = (price >= tp2 if side == "BUY" else price <= tp2)
        if hit_sl or hit_tp2:
            gross  = t["risk"] * (rrc if (hit_tp2 or t["tp1_hit"]) else -1)
            net    = round(gross - t["ft"], 4)
            result = "WIN" if (hit_tp2 or (t["tp1_hit"] and hit_sl)) else ("BE" if t["be"] else "LOSS")
            with _ai_lk:
                t.update({"status":"closed","exit":price,"pnl":net,"result":result,
                           "close_ts":datetime.now(timezone.utc).isoformat()})
            dur = ""
            try:
                od  = datetime.fromisoformat(t.get("open_ts", ""))
                dur = "{}min".format(int((datetime.now(timezone.utc) - od).total_seconds() / 60))
            except: pass
            am_old = ch["am_cycle"]
            if result == "WIN":
                ch["w_streak"] = ch.get("w_streak", 0) + 1; ch["l_streak"] = 0
                ch["am_cycle"] = 0 if ch["w_streak"] >= AM_MAX else min(ch["am_cycle"] + 1, AM_MAX)
            else:
                ch["l_streak"] = ch.get("l_streak", 0) + 1; ch["am_cycle"] = 0; ch["w_streak"] = 0
            ch["balance"]   = round(ch["balance"] + net, 4)
            ch["today_pnl"] = round(ch.get("today_pnl", 0) + net, 4)
            if net > 0: ch["today_w"] = ch.get("today_w", 0) + 1
            else:       ch["today_l"] = ch.get("today_l", 0) + 1
            ch["best_rr"] = max(ch.get("best_rr", 0), float(t["rr"]))
            ch["peak"]    = max(ch.get("peak", ch["balance"]), ch["balance"])
            chal_save(ch)
            mem_record("{}|{}|{}".format(t.get("strat","?"), t.get("sess","?"), t.get("regime","?")), result, net)
            hdr = {"WIN":"✅ GAGNANT","BE":"🔒 BE","LOSS":"❌ PERDANT"}[result]
            tg_send(ADMIN_ID,
                "<b>━━━ {} #{} ━━━</b>\n{} <b>{}</b>\n"
                "📍{:.5f}→<b>{:.5f}</b>\n"
                "💵 {:+.4f}$  Frais:-{:.5f}$\n"
                "📐 RR:{:.2f}  ⏱{}\n🔄 AM:{}→{}\n{}\n<b>@leaderOdg</b>".format(
                    hdr, t["id"], "🟢" if side == "BUY" else "🔴", t["symbol"],
                    entry, price, net, t["ft"], rrc, dur, am_old, ch["am_cycle"], chal_prog(ch)))
            if result == "WIN":
                tg_send(CHANNEL_ID,
                    "<b>✅ WIN IA #{} — {}</b>\n+{:.4f}$ RR:{:.2f}\nSolde:{:.4f}$\n<b>@leaderOdg</b>".format(
                        t["id"], t["symbol"], net, rrc, ch["balance"]))


def chal_prog(c):
    """Barre de progression du Challenge IA."""
    bal    = c["balance"]
    start  = c.get("start_bal", CHALLENGE_START)
    target = start * 100
    prog   = min(100, bal / target * 100) if target > 0 else 0
    bar    = "█" * int(prog / 5) + "░" * (20 - int(prog / 5))
    return "[{}] {:.1f}%\n{:.4f}$ → {:.0f}$".format(bar, prog, bal, target)

# ══════════════════════════════════════════════════════
#  FORMATAGE SIGNAUX
# ══════════════════════════════════════════════════════
MODE_LABELS = {
    "NORMAL":"ICT/SMC ✓","EMA_BOUNCE":"EMA Bounce 📊",
    "MOMENTUM":"Momentum 🚀","STRUCTURE_PLAY":"Structure H1 🏗",
    "RANGE_BREAK":"Cassure Range 📐","TREND_FOLLOW":"Trend Following 📈",
    "OB":"Order Block","LIQ":"Liquidity Sweep",
}

def _score_label(sc):
    """Retourne une évaluation textuelle du score de confiance."""
    if sc >= 90: return "🔥 ÉLITE"
    if sc >= 80: return "💎 PREMIUM"
    if sc >= 70: return "✅ SOLIDE"
    if sc >= 60: return "📊 CORRECT"
    return "⚠️ FAIBLE"

def _confidence_bar(sc):
    filled = sc // 10
    return "█" * filled + "░" * (10 - filled) + f"  {sc}/100"


def _confidence(sc):
    if sc >= 95: return "TRES HAUTE", "🔥"
    if sc >= 88: return "HAUTE",      "💎"
    if sc >= 80: return "BONNE",      "✅"
    return "CORRECTE", "📊"

def _risk_advice(sc, news_ok, sn):
    """Recommandation de taille de position selon le contexte."""
    if not news_ok:         return "0.5% — news proches"
    if sc >= 92:            return "1.5 à 2% — setup élite"
    if sc >= 85:            return "1% — setup validé"
    return "0.5 à 1% — standard"

def _entry_timing(sig, m15_c=None):
    """Indique si l'entrée est immédiate ou nécessite confirmation M1."""
    if sig.get("badges","").count("M1") > 0 or "M1" in sig.get("badges",""):
        return "Entree immediate — M1 confirme"
    return "Attendre confirmation bougie M1"

def _trade_reason(sig):
    """Construit en 1 ligne la raison du signal."""
    parts = []
    b = sig.get("badges","")
    if "Sweep" in b or "Stop Hunt" in b or "EQ" in b: parts.append("liquidite prise")
    if "OTE" in b:    parts.append("zone OTE")
    if "FVG" in b:    parts.append("FVG actif")
    if "CHoCH" in b:  parts.append("CHoCH confirme")
    if "Breakout" in b: parts.append("breakout retest")
    if "H&S" in b or "IH&S" in b: parts.append("Head&Shoulders")
    if "Double" in b: parts.append("double top/bot")
    if "Macro" in b:  parts.append("alignement macro")
    bt = sig.get("btype","")
    if bt: parts.insert(0, "biais H1 ({})".format(bt))
    return "  +  ".join(parts) if parts else "OB M15 + structure H1"

def _score_bar(sc):
    filled = round(sc / 10)
    empty  = 10 - filled
    return "█" * filled + "░" * empty

def _mem_line(s):
    """Affiche le WR historique du setup si suffisamment de données."""
    key = s.get("setup_key","")
    if not key: return None
    w,l,pnl = mem_query(key)
    t = w+l
    if t < 5: return None
    wr = int(w/t*100)
    icon = "🔥" if wr>70 else "✅" if wr>50 else "⚠️"
    return f"│  {icon} Mémoire IA : <b>{wr}%</b> WR sur {t} trades  (+${round(pnl,2)})"

def fmt_pro(s, news, sl_label):
    se    = "🟢" if s["side"] == "BUY"  else "🔴"
    arrow = "📈" if s["side"] == "BUY"  else "📉"
    sf    = "ACHAT" if s["side"] == "BUY" else "VENTE"
    emo   = CAT_EMO.get(s["cat"], "📊")
    liq   = s.get("liq") or {}
    sep   = "═" * 24
    mode  = s.get("mode", "NORMAL")

    sc         = s["score"]
    conf_txt, conf_ico = _confidence(sc)
    risk_txt   = _risk_advice(sc, "✅" in news, sl_label)
    timing     = _entry_timing(s)
    reason     = _trade_reason(s)
    bar        = _score_bar(sc)
    sn, _, _, _ = get_session()
    news_lbl   = "Calme" if "✅" in news else "Actif"
    sp_s       = "OK" if s["sp"] < 3 else "Large"
    mem_l      = _mem_line(s)

    # ── Bloc M5 (nouveau v11) ─────────────────────────────────────
    m5_conf  = s.get("m5_conf", {})
    tf_tag   = s.get("tf_tag", "M15+H1")
    m5_ok    = m5_conf.get("ok", False)
    m5_det   = m5_conf.get("details", "—")
    if m5_ok:
        m5_line = "│  M5 Entry : ✅ <b>{}</b>".format(m5_det)
    else:
        m5_line = "│  M5 Entry : ⚠️ {}".format(m5_det if m5_det != "M5 indispo" else "non disponible")

    # Contexte liquidité pour le cône
    liq_label = liq.get("label", "✓")
    liq_note = "✅ Prise confirmée" if "prise" in liq_label.lower() or "✓" in liq_label else "⚠️ Vérifier liquidité"

    # ── En-tête titre
    title_suffix = "  ⚡ SCALP" if mode == "SCALP" else ""
    lines = [
        "{} {} <b>{} — {}</b>{}  {}".format(arrow, se, s["name"], sf, title_suffix, emo),
        sep,
        "{} Confiance : <b>{}</b>  ·  {}".format(conf_ico, conf_txt, sl_label),
        "🕐 {} UTC  ·  📐 Entrée : <b>{}</b>".format(s["time"], tf_tag),
        "",
    ]

    # ── Bloc SCALP spécifique
    if mode == "SCALP":
        ai_proba = s.get("ai_result", {}).get("ai_proba", 0)
        high_proba = sc >= 80 or ai_proba >= 65
        lines += [
            "┌─ <b>⚡ MODE SCALP — {} {}</b> ─────────".format(s["name"], sf),
            "│  RR minimum : <b>2.0</b>  ·  TF entrée : M5/M15",
        ]
        if high_proba:
            lines += [
                "│",
                "│  🎯 <b>HAUTE PROBABILITÉ — ORDRE LIMITE</b>",
                "│  Place ton entrée à <code>{}</code>".format(s["entry"]),
                "│  📱 <b>Reste en LIVE</b> — exécution dès le prix",
                "│  ⏳ Valide ~15–30 min max",
            ]
        else:
            lines.append("│  ⚠️ Attends confirmation M1 avant entrée")
        lines.append("└──────────────────────────────────")
        lines.append("")

    lines += [
        "┌─ <b>NIVEAUX</b> ──────────────────────────",
        "│  Entree : <code>{}</code>".format(s["entry"]),
        "│  TP     : <code>{}</code>".format(s["tp"]),
        "│  SL     : <code>{}</code>".format(s["sl"]),
        "│  RR     : <b>1:{}</b>".format(s["rr"]),
        "└──────────────────────────────────",
        "",
        "💵 Lot 0.01 : <b>+${}</b> TP  /  <b>-${}</b> SL".format(s["g001"], s["l001"]),
        "💰 Lot 1.00 : <b>+${}</b> TP  /  <b>-${}</b> SL".format(s["g1"],   s["l1"]),
        "",
        "┌─ <b>ANALYSE MULTI-TF</b> ─────────────────",
        "│  Score    : [{}] <b>{}/100</b>".format(bar, sc),
        "│  Tendance : <b>{}</b>  ({})  — fond H1".format(s["bias"], s["btype"]),
        "│  Entrée   : ⚡ <b>{}</b>  (max M15)".format(tf_tag),
        "│  Liquidité: {}  {}".format(liq_note, liq_label),
        "│  OB M15   : ✅  Structure confirmée",
        m5_line,
        "│  Raison   : {}".format(reason),
        "│  Timing   : {}".format(timing),
        mem_l,
        "└──────────────────────────────────",
        "",
        "⚡ <b>Risk conseillé</b> : {}".format(risk_txt),
        "📰 News : {}  ·  Spread : {}".format(news_lbl, sp_s),
    ]

    # ── Bloc IA Claude v19 ────────────────────────────────────────
    ai = s.get("ai_result", {})
    if ai and ai.get("verdict") not in ("ERREUR", None, ""):
        ai_block = fmt_ai_block(ai)
        if ai_block:
            lines.append(ai_block)
        # TP2 institutionnel optimisé par Claude
        if s.get("tp2"):
            dp2 = 2 if float(s.get("entry",1)) > 100 else 5
            rr2 = s.get("rr_tp2", "?")
            lines.append("🎯 <b>TP2 institutionnel :</b> <code>{}</code>  (RR 1:{})".format(
                round(float(s["tp2"]), dp2), rr2))
        # SL optimisé par Claude
        if s.get("sl_ai"):
            lines.append("🛡️ <b>SL optimisé IA :</b> <code>{}</code>".format(s["sl_ai"]))
        # Note d'optimisation
        if s.get("ai_note"):
            lines.append("💡 {}".format(s["ai_note"]))
    # ─────────────────────────────────────────────────────────────

    lines += [
        sep,
        "⚠️ Analyse technique uniquement — pas un conseil financier",
        "🤖 <b>AlphaBot PRO v19</b>  ·  @leaderodg_bot",
    ]
    return "\n".join(l for l in lines if l is not None)

def fmt_blocked(s):
    """Signal sniper score ≥ 90 — teaser professionnel pour FREE."""
    se  = "🟢" if s["side"] == "BUY" else "🔴"
    sf  = "ACHAT" if s["side"] == "BUY" else "VENTE"
    emo = CAT_EMO.get(s["cat"], "📊")
    sep = "═" * 22
    sc  = s.get("score", 0)
    return (
        f"⚠️ <b>Setup détecté — {s['name']}</b>  {emo}\n"
        f"{sep}\n"
        f"{se} <b>{sf}</b>  ·  Score : <b>{sc}/100</b>\n"
        f"📐 RR : <b>1:{s['rr']}</b>  ·  {s.get('time','')} UTC\n"
        f"\n"
        f"Ce signal a passé tous les filtres :\n"
        f"  ✔️ Liquidité Smart Money confirmée\n"
        f"  ✔️ Order Block M15 validé\n"
        f"  ✔️ Alignement multi-timeframe\n"
        f"\n"
        f"Il n\'a pas été envoyé en version gratuite.\n"
        f"\n"
        f"💡 Beaucoup voient les marchés bouger…\n"
        f"Peu ont les outils pour agir au bon moment.\n"
        f"\n"
        f"{sep}\n"
        f"💎 <b>AlphaBot PRO</b> — signaux filtrés, précision maximale.\n"
        f"👉 @leaderodg_bot  →  /pay"
    )

def fmt_free(s, news, sl_label):
    se    = "🟢" if s["side"] == "BUY" else "🔴"
    sf    = "ACHAT" if s["side"] == "BUY" else "VENTE"
    emo   = CAT_EMO.get(s["cat"], "📊")
    sep   = "═" * 22
    arrow = "📈" if s["side"] == "BUY" else "📉"
    liq   = s.get("liq") or {}

    if s["score"] >= 85:
        hook = "🔥 <b>Setup PREMIUM — Score élite</b>"
    elif s["score"] >= 75:
        hook = "💎 <b>Setup ICT confirmé — Haute confiance</b>"
    else:
        hook = "📊 <b>Setup valide — Conditions réunies</b>"

    lines = [
        f"{arrow} {se} <b>{s['name']} — {sf}</b>  {emo}",
        sep,
        hook,
        f"💧 <b>{liq.get('label', 'Liquidité ✓')}</b>" if liq else "💧 Liquidité confirmée ✓",
        "",
        f"📍 Entrée : <code>{s['entry']}</code>",
        f"✅ TP     : <code>{s['tp']}</code>",
        f"❌ SL     : <code>{s['sl']}</code>",
        f"📐 RR     : <b>1:{s['rr']}</b>  ·  🎯 Score : <b>{s['score']}/100</b>",
        "",
        f"💵 Lot 0.01 : <b>+${s['g001']}</b>  /  💰 Lot 1.00 : <b>+${s['g1']}</b>",
        "",
        sep,
        "⚠️ Analyse technique uniquement — pas un conseil financier",
        "🤖 <b>AlphaBot PRO v11</b>  ·  @leaderodg_bot",
    ]
    return "\n".join(l for l in lines if l is not None)

def fmt_scan(results, news, scan_t, sl_l, sm, nb):
    """
    Rapport de scan v11 — rebuildé complet :
    - Statistiques session en tête
    - Tableau par catégorie avec score + M5 status
    - Indicateur qualité (ELITE / PREMIUM / SOLIDE / -)
    - Résumé des rejets par cause (pour debug rapide)
    - Challenge IA inline
    """
    st  = db_daily_stats()
    ch  = chal_get()
    reg = AI_REG
    sn, _, sess_label, wknd = get_session()
    news_ico = "✅" if "✅" in news else "⚠️"

    # ── Comptage qualité des signaux ──────────────────────────────
    elite   = sum(1 for r in results if r["found"] and r["signal"]["score"] >= 90)
    premium = sum(1 for r in results if r["found"] and 80 <= r["signal"]["score"] < 90)
    solide  = sum(1 for r in results if r["found"] and r["signal"]["score"] < 80)
    total_sig = elite + premium + solide

    # ── Comptage rejets par cause ─────────────────────────────────
    reject_causes = {}
    for r in results:
        if not r["found"]:
            raw   = r.get("reason", "?")
            cause = (
                "M5 ↔️" if "M5" in raw else
                "No OB"  if "OB" in raw else
                "Score"  if "Score" in raw or "score" in raw else
                "RR"     if "RR"    in raw else
                "Session" if "Session" in raw or "session" in raw else
                "News"   if "News"  in raw else
                "Spread" if "Spread" in raw else
                "Data"   if any(x in raw for x in ("indispo", "insuffisant", "Timeout")) else
                "Neutre" if "Neutre" in raw or "Neutral" in raw else
                "Autre"
            )
            reject_causes[cause] = reject_causes.get(cause, 0) + 1

    # ── Trier rejets par fréquence ────────────────────────────────
    reject_line = "  ".join(
        "{} ×{}".format(k, v)
        for k, v in sorted(reject_causes.items(), key=lambda x: -x[1])
    ) or "—"

    sep = "═" * 24

    lines = [
        "🔍 <b>SCAN {} UTC</b>  ·  {}".format(scan_t, sess_label),
        sep,
        # Ligne 1 : session + score min + news
        "📡 Session : <b>{}</b>  ·  Score min : <b>{}</b>  ·  News : {}".format(
            sl_l, sm, news_ico),
        # Ligne 2 : challenge IA + régime
        "🤖 IA : <b>{:.4f}$</b>  ·  Régime : <b>{}</b>".format(
            ch["balance"], reg.get("regime", "?")),
        # Ligne 3 : stats du jour
        "📊 Aujourd'hui : <b>{}✅  {}❌  {}🔄</b>  ({} signaux)  💵 +${}".format(
            st["wins"], st["losses"], st.get("open", 0), st["n"], st["g1"]),
        "",
    ]

    # ── Signaux trouvés (par catégorie) ───────────────────────────
    if total_sig > 0:
        lines.append("┌─ <b>SIGNAUX DÉTECTÉS</b> ─────────────────")
        cats = {}
        for r in results:
            if r["found"]:
                cats.setdefault(r["cat"], []).append(r)

        for cat in ["METALS", "CRYPTO", "FOREX", "INDICES", "OIL"]:
            if cat not in cats:
                continue
            for r in cats[cat]:
                s   = r["signal"]
                se  = "🟢" if s["side"] == "BUY" else "🔴"
                sc  = s["score"]
                m5  = s.get("m5_conf", {})
                m5_ico = "✅" if m5.get("ok") else "⚠️"

                # Badge qualité
                if sc >= 90:   ql = "🔥 ÉLITE"
                elif sc >= 80: ql = "💎 PREMIUM"
                else:          ql = "✅ SOLIDE"

                lines.append(
                    "│  {} <b>{}</b>  {}  {} {}  RR 1:{}  {}/100".format(
                        se, s["name"], CAT_EMO.get(cat, "📊"),
                        s["side"], ql, s["rr"], sc
                    )
                )
                lines.append(
                    "│    📍<code>{}</code> → TP <code>{}</code>  SL <code>{}</code>".format(
                        s["entry"], s["tp"], s["sl"]
                    )
                )
                lines.append(
                    "│    📊 {} │ M5 {} {}".format(
                        s.get("tf_tag", "M15+H1"), m5_ico, m5.get("details", "")
                    )
                )
                lines.append("│")

        lines.append("└──────────────────────────────────")
        lines.append("")

    # ── Marchés scannés sans signal ───────────────────────────────
    no_sig = [r for r in results if not r["found"]]
    if no_sig:
        lines.append("┌─ <b>MARCHÉS ANALYSÉS</b> ─────────────────")
        cats_no = {}
        for r in no_sig:
            cats_no.setdefault(r["cat"], []).append(r)
        for cat in ["METALS", "CRYPTO", "FOREX", "INDICES", "OIL"]:
            if cat not in cats_no:
                continue
            emo = CAT_EMO.get(cat, "📊")
            for r in cats_no[cat]:
                sc_val = r.get("sc", 0)
                sm_val = r.get("s_min", sm)
                m5_ok  = r.get("m5_ok")
                m5_tag = " M5⚠️" if m5_ok is False else (" M5✅" if m5_ok else "")
                reason = r.get("reason", "?")
                # Affichage compact : paire + raison
                if sc_val and sm_val:
                    lines.append(
                        "│  ⚪ {} <b>{}</b>  {}  [{}/{}]{}".format(
                            emo, r["name"], reason[:22], sc_val, sm_val, m5_tag)
                    )
                else:
                    lines.append(
                        "│  ⚪ {} <b>{}</b>  {}{}".format(
                            emo, r["name"], reason[:28], m5_tag)
                    )
        lines.append("└──────────────────────────────────")
        lines.append("")

    # ── Résumé rejets + pied de page ─────────────────────────────
    lines.append("🔎 Rejets : {}".format(reject_line))
    lines.append("")
    lines.append(sep)
    if total_sig > 0:
        qual_parts = []
        if elite:   qual_parts.append("🔥 {} ÉLITE".format(elite))
        if premium: qual_parts.append("💎 {} PREMIUM".format(premium))
        if solide:  qual_parts.append("✅ {} SOLIDE".format(solide))
        lines.append("🟢 <b>{} signal(s)</b>  —  {}".format(
            total_sig, "  ".join(qual_parts)))
    else:
        lines.append("🟡 Aucun signal ce cycle")
    lines.append("🔄 Prochain scan ~{}s".format(SCAN_SEC))

    return "\n".join(lines)

def fmt_daily(st, is_pro=True):
    """
    Rapport de fin de journée envoyé à TOUS les membres.
    Version FREE : résumé + motivation PRO.
    Version PRO  : détail complet de chaque position.
    """
    if st["n"] == 0:
        return None  # Rien à rapporter

    closed = st["wins"] + st["losses"]
    wr     = int(st["wins"] / closed * 100) if closed > 0 else 0
    perf   = "🔥🔥" if st["g1"] > 2000 else "🔥" if st["g1"] > 1000 else "💰"
    sep    = "═" * 22

    if is_pro:
        # ── VERSION PRO : rapport complet ──────────────
        lines = [
            f"📯 <b>RAPPORT DU JOUR — AlphaBot PRO v10</b> {perf}",
            sep,
            f"📅 {st['date']}",
            f"📡 <b>{st['n']}</b> signaux analysés  ·  M1+M15+H1",
            "",
            f"✅ TP : <b>{st['wins']}</b>  ·  ❌ SL : <b>{st['losses']}</b>  ·  🔄 En cours : <b>{st['open']}</b>",
            f"📊 Win rate : <b>{wr}%</b>" if closed > 0 else "📊 Win rate : en attente",
            "",
            f"💵 Lot 0.01 : <b>+${st['g001']}</b>  (confirmé)   potentiel : +${st['pot_g001']}",
            f"💰 Lot 1.00 : <b>+${st['g1']}</b>  (confirmé)   potentiel : +${st['pot_g1']}",
            "",
            "━" * 20,
            "<b>DÉTAIL DES POSITIONS :</b>",
            "",
        ]
        total_001 = 0.0
        for row in st["rows"]:
            pair, side, rr, g001, g1, l001, l1, sess, mode, result = row
            d = "⬆️" if side == "BUY" else "⬇️"
            if result == "TP":
                icon = "✅"; detail = f"<b>+${g001:.2f}</b> (lot 0.01)  /  <b>+${g1:.0f}</b> (lot 1)"
                total_001 += g001
            elif result == "SL":
                icon = "❌"; detail = f"<b>-${l001:.2f}</b> (lot 0.01)  /  <b>-${l1:.0f}</b> (lot 1)"
                total_001 -= l001
            else:
                icon = "🔄"; detail = f"en cours — potentiel +${g001:.2f} (lot 0.01)"
            lines.append(f"{icon} <b>{pair}</b> {d} {'ACHAT' if side=='BUY' else 'VENTE'} · RR 1:{rr}  →  {detail}")
        lines += [
            "",
            "━" * 20,
            f"💵 Net estimé lot 0.01 : <b>{total_001:+.2f}$</b>",
            f"💰 Net estimé lot 1.00 : <b>{round(total_001*100, 0):+.0f}$</b>",
            "",
            sep,
            "⚠️ Estimations basées sur TP/SL détectés. Not financial advice.",
            "🤖 AlphaBot PRO v10  ·  @leaderodg_bot",
        ]
    else:
        # ── VERSION FREE : résumé + motivation ─────────
        lines = [
            f"📊 <b>RÉSULTATS DU JOUR — AlphaBot PRO v10</b> {perf}",
            sep,
            f"📅 {st['date']}  ·  <b>{st['n']}</b> signaux envoyés",
            "",
            f"✅ <b>{st['wins']}</b> TP atteints  ·  ❌ <b>{st['losses']}</b> SL  ·  Win rate : <b>{wr}%</b>",
            "",
            f"💵 Lot 0.01 : <b>+${st['g001']}</b> de gains estimés",
            f"💰 Lot 1.00 : <b>+${st['g1']}</b> de gains estimés",
            "",
            sep,
            "🔒 <b>Tu n\'as vu que 4 signaux aujourd\'hui.</b>",
            f"Les membres PRO ont reçu <b>{st['n']}</b> signaux + le détail complet.",
            "",
            f"💎 <b>Passe PRO — {PRO_PRICE}$ USDT</b> et ne rate plus rien.",
            "👉 @leaderodg_bot  →  /pay",
            sep,
            "🤖 AlphaBot PRO v10  ·  @leaderodg_bot",
        ]
    return "\n".join(l for l in lines if l is not None)

# ══════════════════════════════════════════════════════
#  BOUCLE SCAN
# ══════════════════════════════════════════════════════
_sent=set(); _sent_lk=threading.Lock()
_last_d=""; _last_w=""; _scan_run=False; _scan_lock=threading.Lock(); _test_mode=""
_last_results=[]; _pay_state={}
_cycles_no_signal = 0
# v13 compat aliases
_sent_lock         = _sent_lk
_last_daily        = _last_d
_last_weekly       = _last_w
# ── Store signaux actifs (pour bouton vérification) ──────────
_ACTIVE_SIGNALS      = {}   # {pair_side_key: sig_dict}
_ACTIVE_SIGNALS_LOCK = threading.Lock()
_PAIR_LAST_SIGNAL    = {}   # {pair_name: date_str}

_scan_running      = False
_admin_test_mode   = ""
_last_scan_results = []
_payment_state     = _pay_state
_broadcast_pending = {}    # partagé avec _bcast_pending

# ── Throttle global signaux ──────────────────────────────────────
_sig_timestamps    = deque()   # timestamps UTC des signaux envoyés (glissant 24h)
_sig_ts_lock       = threading.Lock()

def _throttle_allowed(now_dt):
    """
    Retourne (ok, reason) :
    - Vérifie max MAX_SIG_PER_HOUR sur 60 minutes glissantes
    - Vérifie max MAX_SIG_PER_DAY sur la journée UTC
    - Vérifie gap minimum MIN_GAP_BETWEEN entre 2 signaux
    """
    with _sig_ts_lock:
        # Nettoyer timestamps > 24h
        cutoff24 = now_dt - timedelta(hours=24)
        while _sig_timestamps and _sig_timestamps[0] < cutoff24:
            _sig_timestamps.popleft()

        # Check journalier
        today_start = now_dt.replace(hour=0, minute=0, second=0, microsecond=0)
        today_count = sum(1 for t in _sig_timestamps if t >= today_start)
        if today_count >= MAX_SIG_PER_DAY:
            return False, "Limite jour atteinte ({}/{})".format(today_count, MAX_SIG_PER_DAY)

        # Check horaire glissant
        cutoff1h = now_dt - timedelta(hours=1)
        hour_count = sum(1 for t in _sig_timestamps if t >= cutoff1h)
        if hour_count >= MAX_SIG_PER_HOUR:
            return False, "Limite heure atteinte ({}/{})".format(hour_count, MAX_SIG_PER_HOUR)

        # Check gap minimum
        if _sig_timestamps:
            last_ts = _sig_timestamps[-1]
            gap_min = (now_dt - last_ts).total_seconds() / 60
            if gap_min < MIN_GAP_BETWEEN:
                return False, "Trop tôt ({:.0f}min < {}min)".format(gap_min, MIN_GAP_BETWEEN)

        return True, "OK"

def _throttle_record(now_dt):
    """Enregistre un signal envoyé dans le compteur glissant."""
    with _sig_ts_lock:
        _sig_timestamps.append(now_dt)

def cleanup_sent(ds):
    global _sent
    with _sent_lk: _sent={k for k in _sent if ds in k}


# ══════════════════════════════════════════════════════
#  RAPPORT DE FIN DE SESSION
# ══════════════════════════════════════════════════════
_last_session_reported = ""

def check_session_end_report():
    """Rapports de session désactivés — rapport soir uniquement à 20h UTC."""
    pass  # Désactivé v17 : trop de messages intermédiaires

def _send_session_report(sess_label, end_hour):
    """Désactivé v17 — rapport soir uniquement."""
    pass

def scan_and_send():
    global _scan_run
    if not _scan_lock.acquire(blocking=False): return  # déjà en cours
    try:
        _scan_run=True
        _scan_inner()
    finally:
        _scan_run=False
        _scan_lock.release()

def _scan_inner():
    global _last_d, _last_w, _last_results, _cycles_no_signal
    now    = datetime.now(timezone.utc).replace(tzinfo=None)
    scan_t = now.strftime("%H:%M"); ds = now.strftime("%Y-%m-%d")
    hs     = now.strftime("%H");    wd = now.weekday()
    sn, sm, sl_l, wknd = get_session()
    sm = get_adaptive_score_min()

    log("INFO", clr("Scan {} — {} — Score~{}".format(scan_t, sl_l, sm), "d"))
    news_ok, news_lbl = news_check()
    # ── Filtre intelligent : jour + catégorie + paires sélectives ───
    active = [m for m in MARKETS if allowed_market(m)]
    if not active:
        log("INFO", clr("Aucun marché actif pour ce créneau.", "y")); return
    q = Queue(); threads = []
    for m in active:
        # On passe False à agent_analyze
        t = threading.Thread(target=agent_analyze,
                             args=(m, sm, news_ok, q), daemon=True)
        t.start(); threads.append(t)
    for t in threads: t.join(timeout=15)
    raw = {}
    while not q.empty():
        try: r = q.get_nowait(); raw[r["name"]] = r
        except Empty: break
    results = [raw.get(m["name"], {"name":m["name"],"cat":m["cat"],"found":False,
               "reason":"Timeout","improv":False}) for m in active]
    _last_results = results; cleanup_sent(ds)
    sigs = [(r["signal"], "{}-{}-{}-{}".format(r["signal"]["name"], r["signal"]["side"], ds, hs))
            for r in results if r["found"]]
    with _sent_lk: sigs = [(s, k) for s, k in sigs if k not in _sent]
    sigs.sort(key=lambda x: -x[0]["score"])

    # ── ✨ Pipeline IA : Gemini (détection) → Claude (validation) ──
    # Étape 1 — Gemini filtre les setups prometteurs
    if _GEMINI_OK and GEMINI_API_KEY and sigs:
        sigs_gemini = []
        for sig, key in sigs:
            if sig.get("rr", 0) >= 2.0 and sig.get("score", 0) >= sm:
                gem = gemini_scan_signal(sig, sn)
                sig["gemini_scan"] = gem
                if gem["approved"]:
                    sigs_gemini.append((sig, key))
                else:
                    log("AI", "🔍 Gemini rejette {} — {}".format(
                        sig["name"], gem.get("raison","?")[:60]))
            else:
                sig["gemini_scan"] = {"approved": True, "score_setup": 5, "raison": "Score < seuil, bypass Gemini"}
                sigs_gemini.append((sig, key))
        log("AI", "Gemini scan : {}/{} setups retenus".format(len(sigs_gemini), len(sigs)))
        sigs = sigs_gemini

    # Étape 2 — Claude valide le risque sur les setups approuvés par Gemini
    if CLAUDE_API_KEY and sigs:
        sigs_validated = []
        for sig, key in sigs:
            if sig.get("rr", 0) >= 2.0 and sig.get("score", 0) >= sm:
                htf_trend = sig.get("bias", "BULLISH")
                ai_result = claude_validate_signal(sig, sn, htf_trend)
                sig["ai_result"] = ai_result
                if ai_result["validated"]:
                    sigs_validated.append((sig, key))
                else:
                    log("AI", "❌ Claude rejette {} — {} (hybride {}/100)".format(
                        sig["name"],
                        ai_result.get("raison","?")[:60],
                        ai_result.get("final_score", 0)))
            else:
                sig["ai_result"] = {}
                sigs_validated.append((sig, key))
        log("AI", "Claude validation : {}/{} validés".format(len(sigs_validated), len(sigs)))
        sigs = sigs_validated

        # ── Optimisation TP/SL par Claude sur les signaux validés ──
        for idx, (sig, key) in enumerate(sigs):
            if sig.get("ai_result", {}).get("validated"):
                try:
                    opt = claude_optimize_tp_sl(sig, sn)
                    if opt:
                        if opt.get("sl_optimise"):
                            sig["sl_ai"] = opt["sl_optimise"]
                        if opt.get("tp1_optimise"):
                            sig["tp_ai"] = opt["tp1_optimise"]
                        if opt.get("tp2"):
                            sig["tp2"]   = opt["tp2"]
                        if opt.get("rr_tp2"):
                            sig["rr_tp2"] = opt["rr_tp2"]
                        sig["ai_note"] = opt.get("note", "")
                        sigs[idx] = (sig, key)
                except Exception as _oe:
                    log("WARN", "Optim TP/SL: {}".format(_oe))
    # ─────────────────────────────────────────────────────────────




    # Message promo FREE (Exness + activer PRO)
    MSG_PROMO_FREE = (
        "📊 <b>Tu as reçu tes {} signaux gratuits du jour !</b>\n\n"
        "💡 Pour aller plus loin :\n\n"
        "1️⃣ <b>Ouvre un compte Exness</b> (broker recommandé) :\n"
        "👉 <a href=\"{}\">🔗 Créer mon compte Exness</a>\n\n"
        "2️⃣ <b>Active le PRO gratuitement</b> :\n"
        "Contacte @leaderOdg pour activation rapide\n\n"
        "3️⃣ <b>Ou passe PRO directement :</b>\n"
        "👉 /pay — seulement {}$ USDT/mois\n\n"
        "🏆 PRO = signaux illimités + analyse complète"
    ).format(FREE_LIMIT, BROKER_LINK, PRO_PRICE)

    for sig, key in sigs:
        # ── Blocage signaux après 22h00 UTC ─────────────────────────
        now_check = datetime.now(timezone.utc).replace(tzinfo=None)
        if now_check.hour >= SIGNAL_CUTOFF_HOUR:
            log("INFO", clr("Signal {} bloqué — après 22h00 UTC".format(sig["name"]), "yellow"))
            continue

        # ── Throttle global : max 3/h, max 6/j, gap 15min ──────────
        ok_send, reason_throttle = _throttle_allowed(now_check)
        if not ok_send:
            log("INFO", clr("Signal {} ignoré — {}".format(sig["name"], reason_throttle), "yellow"))
            continue

        sc  = sig.get("score", 0)
        stk = STK_CROWN if sc >= 90 else STK_MONEY if sig["side"]=="BUY" else STK_FIRE

        msg_p       = fmt_pro(sig, news_lbl, sl_l)
        msg_teasing = fmt_signal_teasing(sig)

        # ── Image TradingView de la paire ───────────────────────────
        chart_img = None
        try:
            chart_img = fetch_tradingview_chart(sig)
        except: pass

        # ── Groupe FREE → teasing uniquement (aucun niveau) ─────────
        ref_admin = "https://t.me/{}?start={}".format(BOT_USER, ADMIN_ID)
        if chart_img:
            r = tg_send_photo(CHANNEL_ID, chart_img, caption=msg_teasing[:1024])
        else:
            r = tg_send(CHANNEL_ID, msg_teasing,
                        kb={"inline_keyboard": [
                            [{"text": "💵 Payer 10$/mois",      "url": ref_admin}],
                            [{"text": "🤝 Parrainer 10 amis",   "url": ref_admin}],
                            [{"text": "📢 Partager ce groupe",   "url": FREE_GROUP_LINK},
                             {"text": "👑 Groupe VIP",           "url": VIP_GROUP_LINK}],
                        ]})

        # ── Groupe VIP → 1 seul message : signal PRO complet ────────
        if chart_img:
            tg_send_photo(VIP_CH, chart_img, caption=msg_p[:1024])
        else:
            tg_send(VIP_CH, msg_p)

        if r.get("ok"):
            with _sent_lk: _sent.add(key)
            save_signal(sig, sn)
            _throttle_record(now_check)
            log("SIG", "{} {} RR:1:{} Sc:{} G1:+${}".format(
                clr(sig["name"], "b", "c"), sig["side"], sig["rr"], sc, sig["g1"]))

        # ── DM individuels : 1 seul message + bouton recheck ──────────
        # ── Stocker signal actif pour recheck live ──────────────
        pair_side_key = "{}-{}".format(sig.get("name",""), sig.get("side",""))
        sig["_check_key"] = pair_side_key
        sig["_ts"] = datetime.now(timezone.utc).isoformat()
        sig["_ts_epoch"] = time.time()
        with _ACTIVE_SIGNALS_LOCK:
            _ACTIVE_SIGNALS[pair_side_key] = dict(sig)
        kb_sig = {"inline_keyboard": [
            [{"text": "🔍 Vérifier si signal valide",
              "callback_data": "check_sig_{}".format(pair_side_key)}],
            [{"text": "◀️ Menu", "callback_data": "start"}],
        ]}
        for uid in all_users():
            try:
                pro = is_pro(uid)
                c   = count_today(uid)
                # Vérif anti-doublon par user+signal
                if db_one("SELECT 1 FROM signal_sent WHERE user_id=? AND sig_key=?",
                          (uid, key)):
                    continue
                if pro:
                    tg_send(uid, msg_p, kb=kb_sig)
                    count_incr(uid)
                    db_run("INSERT OR IGNORE INTO signal_sent(user_id,sig_key,sent_at) VALUES(?,?,?)",
                           (uid, key, datetime.now().isoformat()))
                elif c < FREE_LIMIT:
                    tg_send(uid, msg_teasing, kb=kb_sig)
                    count_incr(uid)
                    db_run("INSERT OR IGNORE INTO signal_sent(user_id,sig_key,sent_at) VALUES(?,?,?)",
                           (uid, key, datetime.now().isoformat()))
                time.sleep(0.06)
            except Exception as _e:
                log("WARN", "Notif uid={}: {}".format(uid, _e))

    # ── Aucun signal : message "pas de setup" si heure active ──────
    sn2,_,_,wknd2=get_session()
    if not sigs and sn2 not in ("OFF","ASIAN") and not wknd2:
        if int(hs) in (8,9,13,14,17,18):  # heures de sessions clés seulement
            tg_send(ADMIN_ID,
                "🔍 Scan {} — Aucun setup propre ce cycle.\n"
                "Marchés actifs mais conditions insuffisantes (score < 85 ou liquidité absente).\n"
                "Prochaine analyse dans {}s.".format(scan_t, SCAN_SEC))
    # ── Rapport soir 22h UTC — UNE SEULE FOIS ───────────────────────
    if int(hs) == DAILY_HOUR and _last_d != ds and not rep_sent(ds):
        st = daily_stats(ds)
        if st["n"] > 0:
            d_pro  = fmt_daily(st, is_pro=True)
            d_free = fmt_daily(st, is_pro=False)
            if d_pro:
                ref_admin = "https://t.me/{}?start={}".format(BOT_USER, ADMIN_ID)
                free_footer = (
                    "\n━" + "━"*21 + "\n"
                    "📡 <b>{} signaux envoyés aujourd'hui aux membres PRO/VIP</b>\n\n"
                    "👑 <b>Rejoins la version PRO — 3 options :</b>\n\n"
                    "1️⃣ 💵 Payer 10$/mois → /pay\n"
                    "2️⃣ 🤝 Parrainer 10 personnes → 7j PRO gratuits\n"
                    "3️⃣ 📢 Partager ce groupe (10–30 personnes + capture à @leaderOdg)\n\n"
                    "🔗 Lien à partager : <code>{}</code>"
                ).format(st["n"], FREE_GROUP_LINK)
                # Groupe FREE : rapport + CTA
                tg_send(CHANNEL_ID, d_free + free_footer,
                        kb={"inline_keyboard": [
                            [{"text": "💵 Payer 10$/mois",    "url": ref_admin}],
                            [{"text": "🤝 Parrainer 10 amis", "url": ref_admin}],
                            [{"text": "👑 Groupe VIP",        "url": VIP_GROUP_LINK}],
                        ]})
                # Groupe VIP : rapport complet PRO
                tg_send(VIP_CH, d_pro)
                # DM PRO
                for puid in pru:
                    tg_send(puid, d_pro); time.sleep(0.04)
                # DM FREE
                for fuid in free_users():
                    tg_send(fuid, d_free); time.sleep(0.04)
                mark_rep(st); _last_d = ds
    # Rapport hebdo (DM uniquement, pas dans les groupes)
    wk = "{}-W{}".format(now.year, now.isocalendar()[1])
    if wd == WEEKLY_DAY and int(hs) == WEEKLY_HOUR and _last_w != wk and not rep_sent(wk, "weekly_rep", "week_start"):
        ws = db_weekly_stats()
        if ws["n"] > 0:
            wmsg = "🏆 <b>RAPPORT HEBDO AlphaBot PRO</b>\n"+"═"*22+"\n\n📅 Semaine du {}\n\n💵 Lot 0.01: +${}\n💰 Lot 1.00: +${}\n\n📡 {} signaux  ·  {} wins  ·  {}%\n\n📩 @leaderodg_bot  ·  {}$ USDT".format(ws["ws"],ws["g001"],ws["g1"],ws["n"],ws["wins"],int(ws["wins"]/ws["n"]*100) if ws["n"] else 0,PRO_PRICE)
            for puid in pru:
                tg_send(puid, wmsg); time.sleep(0.04)
            mark_rep(ws, "weekly_rep"); _last_w = wk
    # Expirations
    for uid,uname in check_expiry():
        _,_,src=db_get_pro_info(uid)
        msg="⏰ <b>Essai {} jours terminé!</b>\n/pay → {}$ USDT".format(TRIAL_DAYS,PRO_PRICE) if src and "TRIAL" in (src or "") else "⏰ <b>PRO expiré</b>\n/pay → {}$ USDT\n/ref → {} filleuls = {} mois".format(PRO_PRICE,REF_TARGET,REF_MONTHS)
        tg_send(uid,msg)
    # Backup + relance + suivi TP/SL + scan IA
    if int(hs)==DAILY_HOUR and ds!=getattr(_scan_inner,"_lb",""):
        _scan_inner._lb=ds; threading.Thread(target=do_backup,daemon=True).start()
    if int(hs)%6==0 and ds+hs!=getattr(_scan_inner,"_lr",""):
        _scan_inner._lr=ds+hs; threading.Thread(target=relance_inactifs,daemon=True).start()
    threading.Thread(target=check_open_sigs,daemon=True).start()
    threading.Thread(target=ai_scan_cycle,daemon=True).start()
    # Vérifier fin de session → rapport automatique
    # Session end reports désactivés — rapport soir uniquement

def ai_scan_cycle():
    try:
        setups=ai_full_scan()
        if setups:
            best=setups[0]
            log("AI",clr("Setup {} {} Sc:{} RR:{}".format(best["sym"],"L" if best["side"]=="BUY" else "S",best["sc"],best["rr"]),"g"))
            ai_open(best)
    except Exception as e: log("WARN","[AI] {}".format(e))

def broadcast_new_version():
    """Envoie un message de mise à jour à TOUS les utilisateurs avec leur lien de parrainage."""
    time.sleep(8)
    users_data = db_all("SELECT user_id FROM users")
    count = ok = fail = 0
    for (fuid,) in users_data:
        try:
            ref_link = "https://t.me/{}?start={}".format(BOT_USER, fuid)
            msg = (
                "🚀 <b>AlphaBot PRO v17 — MISE À JOUR IMPORTANTE !</b>\n"
                "═"*22+"\n\n"
                "⚡ <b>Nouveau système de signaux plus précis</b>\n\n"
                "🎯 <b>Comment ça marche désormais :</b>\n"
                "  📊 <b>Tendance de fond</b> → analysée sur H1 (interne)\n"
                "  ⚡ <b>Entrée réelle</b>    → M5 au grand maximum M15\n\n"
                "✅ Plus de signaux sur H1 directement !\n"
                "   Les entrées H1 nécessitent trop de marge\n"
                "   et exposent à des pertes importantes.\n\n"
                "🔍 <b>Chaque signal inclut désormais :</b>\n"
                "  • Biais H1 (tendance fond)\n"
                "  • Confirmation liquidité (sweep / EQH-EQL)\n"
                "  • Entrée précise M5 ou M15\n"
                "  • Order Block + FVG validés\n\n"
                "━"*22+"\n"
                "🤝 <b>Invite tes amis et gagne PRO GRATUIT :</b>\n"
                "<code>{}</code>\n\n"
                "👇 <b>Clique pour accéder au bot :</b>"
            ).format(ref_link)
            kb = {"inline_keyboard": [
                [{"text": "✅ Accéder au bot",       "callback_data": "start"}],
                [{"text": "💎 Devenir PRO",           "callback_data": "pro"}],
                [{"text": "🤝 Mon lien parrainage",   "url": ref_link}],
            ]}
            tg_send(fuid, msg, kb=kb)
            ok += 1; count += 1
            time.sleep(0.15)
        except Exception as e:
            fail += 1
            log("WARN", "broadcast_v17 uid={}: {}".format(fuid, e))
    log("INFO", clr("Broadcast v17 → {} membres ({} ok, {} fail)".format(count, ok, fail), "b", "g"))
    tg_send(ADMIN_ID, "📢 <b>Broadcast v17 OK</b>\n✅ {} envoyés  ·  ❌ {} échecs".format(ok, fail))

def do_backup():
    try:
        import shutil; bp="/tmp/ab10_{}.db".format(datetime.now().strftime("%Y%m%d_%H%M"))
        shutil.copy2(DB_FILE,bp)
        with open(bp,"rb") as f: data=f.read()
        tg_doc(ADMIN_ID,data,"ab10_backup_{}.db".format(datetime.now().strftime("%Y%m%d")),"💾 <b>Backup v10</b> — {}".format(datetime.now().strftime("%d/%m/%Y %H:%M")))
    except Exception as e: log("WARN","Backup: {}".format(e))

def relance_inactifs():
    try:
        inactifs=inactive_users()
        if not inactifs: return
        st=db_daily_stats()
        for uid,uname in inactifs[:20]:
            try:
                tg_send(uid,"👋 <b>Hey {}!</b>\n\n📡 {} signaux aujourd'hui\n+${} de gains estimés\n\n✅ {} TP  ·  {}% réussite\n\n@leaderodg_bot".format(
                    "@"+uname if uname else "Trader",st["n"],st["g1"],st["wins"],
                    int(st["wins"]/st["n"]*100) if st["n"] else 0))
                time.sleep(0.1)
            except: pass
    except Exception as e: log("WARN","Relance: {}".format(e))

def check_open_sigs():
    try:
        for tid,pair,entry,tp,sl,side,created in open_signals():
            try:
                age=(datetime.now()-datetime.fromisoformat(created)).total_seconds()/3600
                if age>4: close_track(tid,"EXPIRED"); continue
            except: continue
            m=next((x for x in MARKETS if x["name"]==pair),None)
            if not m: continue
            try:
                c=fetch_c(m["sym"],"15m","1d")
                if not c: continue
                cur=c[-1]["c"]
                # Récupérer la clé setup depuis la DB signals
                sig_row=db_one("SELECT score,mode FROM signals WHERE id=?",(tid,))
                _score=sig_row[0] if sig_row else 0
                _skey="{}|{}|BASE".format(pair, get_session()[0])
                if side=="BUY":
                    if cur>=tp:
                        close_track(tid,"TP")
                        pnl_est=abs(tp-entry)/(m["pip"])*0.01  # lot 0.01
                        mem_record(_skey,"WIN",round(pnl_est,4))
                        notify_result(pair,side,entry,tp,sl,"TP",cur)
                    elif cur<=sl:
                        close_track(tid,"SL")
                        pnl_est=-abs(entry-sl)/(m["pip"])*0.01
                        mem_record(_skey,"LOSS",round(pnl_est,4))
                        notify_result(pair,side,entry,tp,sl,"SL",cur)
                else:
                    if cur<=tp:
                        close_track(tid,"TP")
                        pnl_est=abs(entry-tp)/(m["pip"])*0.01
                        mem_record(_skey,"WIN",round(pnl_est,4))
                        notify_result(pair,side,entry,tp,sl,"TP",cur)
                    elif cur>=sl:
                        close_track(tid,"SL")
                        pnl_est=-abs(sl-entry)/(m["pip"])*0.01
                        mem_record(_skey,"LOSS",round(pnl_est,4))
                        notify_result(pair,side,entry,tp,sl,"SL",cur)
            except: continue
    except Exception as e: log("WARN","check_open: {}".format(e))

def notify_result(pair, side, entry, tp, sl, result, cur):
    # Résultats TP/SL uniquement le soir (>= DAILY_HOUR)
    if datetime.now(timezone.utc).hour < DAILY_HOUR:
        return  # silencieux pendant la journée

# ══════════════════════════════════════════════════════
#  PAIEMENT USDT
# ══════════════════════════════════════════════════════
def verify_tx(tx):
    for url in ["https://apilist.tronscan.org/api/transaction-info?hash={}".format(tx),"https://api.trongrid.io/v1/transactions/{}".format(tx)]:
        for attempt in range(2):
            try:
                body=json.loads(http_get(url,timeout=10))
                for t in body.get("trc20TransferInfo",[]):
                    if t.get("to_address","").lower()==USDT_ADDR.lower() and t.get("symbol","").upper()=="USDT":
                        amt=float(t.get("amount_str","0"))/1e6
                        if amt>=PRO_PRICE*0.95: return True,round(amt,2)
                cd=body.get("contractData",{})
                if cd.get("to_address","").lower()==USDT_ADDR.lower():
                    amt=float(cd.get("amount",0))/1e6
                    if amt>=PRO_PRICE*0.95: return True,round(amt,2)
                if body.get("hash") or body.get("txID"): return False,0
            except Exception as ex:
                if any(e in str(ex) for e in ["No address","Name or service","Errno 7"]): return None,0
                if attempt==0: time.sleep(2)
    return False,0

def handle_pay_submitted(uid, uname, plan_key="PRO"):
    _pay_state[uid]={"tx":None,"step":"waiting","plan":plan_key}
    price = {"FREE":0,"STARTER":5,"PRO":10,"VIP":25}.get(plan_key, PRO_PRICE)
    tg_send(uid,
        "📋 <b>COLLE TON TX HASH</b>\n\n"
        "Plan: <b>{}</b> — {}$ USDT TRC20\n\n"
        "Après virement, envoie l'ID de transaction ici.\n\n"
        "<code>exemple: a1b2c3d4e5f6789abc...</code>\n\n"
        "✅ Vérification automatique blockchain!".format(plan_key, price),
        kb={"inline_keyboard":[[{"text":"❌ Annuler","callback_data":"pay_cancel"}]]})

def handle_proof(uid,uname,tx):
    if uid not in _pay_state or _pay_state[uid].get("step")!="waiting": return False
    _pay_state[uid]["tx"]=tx; _pay_state[uid]["step"]="confirm"
    tg_send(uid,"📋 <b>TX HASH REÇU</b>\n\n<code>{}</code>\n\nClique sur <b>🔍 Vérifier</b>".format(tx),
        kb={"inline_keyboard":[[{"text":"🔍 Vérifier mon paiement","callback_data":"pay_confirm"}],[{"text":"🔄 Changer","callback_data":"pay_submitted"}],[{"text":"❌ Annuler","callback_data":"pay_cancel"}]]})
    return True

def handle_pay_confirm(uid,uname):
    state=_pay_state.pop(uid,None)
    if not state or not state.get("tx"): tg_send(uid,"❌ Aucun hash. Recommence avec /pay"); return
    tx=state["tx"]; save_pay(uid,tx)
    tg_send(uid,"🔍 <b>Vérification...</b>\n\nHash: <code>{}</code>\n\n⏳ Blockchain TRC20 — 2 min max".format(tx))
    tg_send(ADMIN_ID,"💰 <b>PAIEMENT EN ATTENTE</b>\n@{} <code>{}</code>\n<code>{}</code>\n/activate {}".format(uname or "?",uid,tx,uid))
    def _v():
        for i,delay in enumerate([5,60,120]):
            time.sleep(delay); ok,amt=verify_tx(tx)
            if ok:
                db_pro(uid,"USDT_AUTO",days=None); tg_sticker(uid,STK_WIN)
                tg_send(uid,"🎉 <b>PAIEMENT CONFIRMÉ!</b>\n\n✅ {}$ USDT reçu!\n💎 <b>PRO À VIE!</b>\n✅ Max {} signaux/j\n✅ Agent IA Binance inclus!".format(amt,PRO_LIMIT))
                tg_send(ADMIN_ID,"🟢 AUTO PRO: @{} <code>{}</code> {}$ ✅".format(uname or "?",uid,amt))
                log("PAY",clr("AUTO PRO: @{} {}$".format(uname,amt),"g")); return
            if i<2: log("INFO",clr("TX non confirmé {}/3".format(i+1),"y"))
        tg_send(uid,"⏳ <b>En attente</b>\n\nL'admin activera sous 30 min.\n@leaderOdg"); tg_send(ADMIN_ID,"⚠️ MANUELLE\n@{} <code>{}</code>\n<code>{}</code>\n/activate {}".format(uname or "?",uid,tx,uid))
    threading.Thread(target=_v,daemon=True).start()

# ══════════════════════════════════════════════════════
#  CLAVIERS & COMMANDES
# ══════════════════════════════════════════════════════
def kb_main(pro=False): return {"inline_keyboard":[
    [{"text":"📡 Mes Signaux","callback_data":"signals"},{"text":"📊 Mon Compte","callback_data":"account"}],
    [{"text":"💎 Devenir PRO","callback_data":"pay"} if not pro else {"text":"✅ PRO Actif","callback_data":"account"},{"text":"🤝 Parrainage","callback_data":"ref"}],
    [{"text":"💸 Mes Gains","callback_data":"gains"},{"text":"📖 Guide ICT","callback_data":"guide"}],
    [{"text":"📈 Rapports","callback_data":"rapports"},{"text":"🏦 Broker Exness","callback_data":"broker"}],
    [{"text":"👑 Rejoindre groupe VIP","url": VIP_GROUP_LINK}] if pro else
     [{"text":"📢 Rejoindre groupe FREE","url": FREE_GROUP_LINK}],
]}

# Alias rétrocompatibilité
def kb_reply(pro=False): return kb_main(pro)
def kb_back(): return {"inline_keyboard":[[{"text":"◀️ Retour","callback_data":"start"}]]}

def _group_invite_msg(pro=False):
    """Retourne un message d'invitation au groupe selon le plan."""
    if pro:
        return (
            "👑 <b>GROUPE VIP — Rejoins maintenant !</b>\n"
            "═"*22+"\n\n"
            "✅ Tu es PRO — accès au groupe VIP inclus !\n\n"
            "📡 Dans le groupe tu reçois :\n"
            "  • Tous les signaux en temps réel\n"
            "  • Analyses ICT/SMC commentées\n"
            "  • Même si le bot s'arrête, tu gardes les signaux\n\n"
            "👇 <b>Clique pour rejoindre :</b>"
        ), {"inline_keyboard": [[{"text":"👑 Rejoindre groupe VIP","url": VIP_GROUP_LINK}],
                                 [{"text":"◀️ Retour","callback_data":"start"}]]}
    else:
        return (
            "📢 <b>GROUPE FREE — Rejoins maintenant !</b>\n"
            "═"*22+"\n\n"
            "✅ Rejoins le groupe pour :\n"
            "  • Voir les signaux même si le bot est offline\n"
            "  • Rester informé des setups du marché\n"
            "  • Communauté de traders AlphaBot\n\n"
            "💎 Pour des signaux complets → /pay (PRO)\n\n"
            "👇 <b>Clique pour rejoindre :</b>"
        ), {"inline_keyboard": [[{"text":"📢 Rejoindre groupe FREE","url": FREE_GROUP_LINK}],
                                 [{"text":"💎 Devenir PRO","callback_data":"pay"}],
                                 [{"text":"◀️ Retour","callback_data":"start"}]]}

def send_welcome(uid, uname):
    db_register(uid, uname)
    p = is_pro(uid)
    plan = get_plan(uid)
    tg_sticker(uid, STK_W)
    tg_send(uid,
        "🤖 <b>AlphaBot PRO v21 — Agent IA Adaptatif</b>\n"+"═"*22+"\n\n"
        "📡 20 marchés : Forex · Or · BTC · Indices · Pétrole\n"
        "🧠 ICT/SMC · Tendance H1 · Entrée M5/M15\n"
        "⚡ Scan toutes les <b>{}s</b> — signaux directs\n\n"
        "✅ Plan: <b>{}</b>\n\nSélectionne une option ↓".format(
            SCAN_SEC, plan),
        kb=kb_main(p))
    # Invitation groupe après le welcome (délai 2s)
    time.sleep(2)
    inv_msg, inv_kb = _group_invite_msg(p)
    tg_send(uid, inv_msg, kb=inv_kb)

def send_account(uid,uname,forced=None):
    plan=forced or get_plan(uid); _,exp,_=db_get_pro_info(uid)
    refs=db_get_refs(uid); td=count_today(uid); lim={"FREE":FREE_LIMIT,"PRO":PRO_LIMIT,"VIP":999}.get(plan,FREE_LIMIT)
    st=db_daily_stats(); ws=db_weekly_stats()
    plan_ico = {"FREE":"👀 FREE","PRO":"💎 PRO","VIP":"👑 VIP"}.get(plan,"📋")
    wr_d = int(st["wins"]/st["n"]*100) if st["n"] else 0
    wr_w = int(ws["wins"]/ws["n"]*100) if ws["n"] else 0
    tg_send(uid,
        "👤 <b>MON COMPTE</b>\n"+"═"*22+"\n\n"
        "🆔 <code>{}</code>\n"
        "👤 @{}\n"
        "📋 Statut : <b>{}</b>{}\n\n"
        "📡 Signaux aujourd\'hui : <b>{}/{}</b>\n"
        "🤝 Filleuls : <b>{}/{}</b>\n\n"
        "📊 <b>PERFORMANCE</b>\n"
        "  Aujourd\'hui : {} sig · {}% WR · +${} lot1\n"
        "  Semaine     : {} sig · {}% WR · +${} lot1\n\n"
        "{}📩 Support : @leaderOdg".format(
            uid, uname or "?", plan_ico,
            "\n📅 Expire : {}".format(exp) if exp else "",
            td, lim, refs, REF_TARGET,
            st["n"], wr_d, st["g1"],
            ws["n"], wr_w, ws["g1"],
            "✅ Acces PRO + Agent IA\n" if plan in ("PRO","VIP") else "🔒 /pay pour PRO complet\n"
        ), kb=kb_main(plan in ("PRO","VIP")))

def send_pay(uid):
    tg_send(uid,"💎 <b>PASSER EN PRO</b>\n"+"═"*22+"\n\n✅ {} signaux/jour\n✅ 20 marchés + crypto\n✅ \n✅ Agent IA Binance\n✅ Challenge 5$→500$\n\n💵 <b>PRIX: {}$ USDT TRC20</b>\n\n📤 Envoie sur:\n<code>{}</code>\n\nPuis clique <b>J'ai payé ✅</b>".format(PRO_LIMIT,PRO_PRICE,USDT_ADDR),
        kb={"inline_keyboard":[[{"text":"✅ J'ai payé","callback_data":"pay_submitted"}],[{"text":"❓ Aide @leaderOdg","url":"https://t.me/leaderOdg"}],[{"text":"◀️ Retour","callback_data":"start"}]]})

def send_challenge(uid):
    tg_send(uid, "ℹ️ Le Challenge IA est désactivé sur cette version.")


# ── Lookup pip par nom de paire ────────────────────────────────────────
_PIP_MAP  = {m["name"]: m["pip"]  for m in MARKETS}
_SYM_MAP  = {m["name"]: m["sym"]  for m in MARKETS}

def _live_price(pair_name: str) -> float | None:
    """Récupère le dernier prix Yahoo Finance pour une paire."""
    sym = _SYM_MAP.get(pair_name)
    if not sym: return None
    try:
        c = fetch_c(sym, "1m", "1d")
        return float(c[-1]["c"]) if c else None
    except Exception:
        return None

def _calc_pnl(entry: float, current: float, side: str, pip: float) -> tuple:
    """
    Retourne (pnl_001, pnl_1, pips, icon) :
      pnl_001 = PnL pour lot 0.01 (≈$0.10/pip pour FOREX, variable pour autres)
      pnl_1   = PnL pour lot 1.00
      pips    = variation en pips (positif = dans le bon sens)
      icon    = ✅ / ❌
    """
    if side == "BUY":
        delta = current - entry
    else:
        delta = entry - current
    pips = round(delta / pip, 1)
    # Valeur pip : ~$1/pip/lot pour FOREX, $0.01/pip pour XAUUSD lot 0.01, etc.
    pip_val_1 = pip * 100000 if pip <= 0.001 else (pip * 10 if pip <= 0.01 else pip)
    pnl_1   = round(pips * pip_val_1, 2)
    pnl_001 = round(pnl_1 * 0.01, 4)
    icon = "✅" if delta > 0 else ("➖" if abs(delta) < pip else "❌")
    return pnl_001, pnl_1, pips, icon


def send_rapports(uid):
    """Rapport du jour + semaine + PnL live des 5 derniers signaux envoyés."""
    st  = db_daily_stats()
    ws  = db_weekly_stats()
    sd  = st["n"];  wd_ = st["wins"]
    sw  = ws["n"];  ww  = ws["wins"]
    wr_d = int(wd_ / sd * 100) if sd else 0
    wr_w = int(ww  / sw * 100) if sw else 0

    sep = "═" * 22
    lines = ["📈 <b>RAPPORTS DE PERFORMANCE</b>", sep, "", "🔥 <b>AUJOURD'HUI</b>", ""]
    if sd > 0:
        lines += [
            "📡 <b>{}</b> signaux  ·  ✅ <b>{}</b>  ·  ❌ <b>{}</b>  ·  <b>{}%</b> réussite".format(
                sd, wd_, sd - wd_, wr_d),
            "💵 Lot 0.01 : <b>+${}</b>".format(st["g001"]),
            "💰 Lot 1.00 : <b>+${}</b>".format(st["g1"]), ""]
    else:
        lines.append("⏳ Aucun signal aujourd'hui")

    lines += ["", "━" * 20, "", "🔥🔥 <b>CETTE SEMAINE</b>", ""]
    if sw > 0:
        lines += [
            "📡 <b>{}</b> signaux  ·  ✅ <b>{}</b>  ·  <b>{}%</b> réussite".format(sw, ww, wr_w),
            "💵 Lot 0.01 : <b>+${}</b>  ·  💰 Lot 1.00 : <b>+${}</b>".format(ws["g001"], ws["g1"])]
    else:
        lines.append("⏳ Aucun signal cette semaine")

    # ── PnL LIVE des 5 derniers signaux (prix actuel vs entrée) ──────
    lines += ["", "━" * 20, "", "📡 <b>PNL LIVE — 5 derniers signaux</b>", ""]
    try:
        recent = db_all(
            "SELECT pair, side, entry, tp, sl, sent_at FROM signals ORDER BY sent_at DESC LIMIT 5", ())
        if recent:
            for row in recent:
                pair, side, entry_raw, tp_raw, sl_raw, sent_at = row
                try:
                    entry   = float(entry_raw)
                    pip     = _PIP_MAP.get(pair, 0.0001)
                    live    = _live_price(pair)
                    d_icon  = "⬆️" if side == "BUY" else "⬇️"
                    sf      = "ACHAT" if side == "BUY" else "VENTE"
                    dt_str  = sent_at[:16].replace("T"," ") if sent_at else "?"
                    if live:
                        pnl_001, pnl_1, pips, res_icon = _calc_pnl(entry, live, side, pip)
                        lines += [
                            "{} {} <b>{}</b> {} · {}".format(res_icon, d_icon, pair, sf, dt_str),
                            "  📍 <code>{:.5g}</code>  →  now <code>{:.5g}</code>  ({:+.1f} pips)".format(
                                entry, live, pips),
                            "  💵 lot 0.01 : <b>{:+.2f}$</b>  ·  💰 lot 1.00 : <b>{:+.0f}$</b>".format(
                                pnl_001, pnl_1),
                            ""]
                    else:
                        lines += [
                            "⬜ {} <b>{}</b> {} · {}".format(d_icon, pair, sf, dt_str),
                            "  📍 <code>{:.5g}</code>  ·  prix live indispo",
                            ""]
                except Exception:
                    pass
        else:
            lines.append("⏳ Aucun signal récent")
    except Exception as _e:
        lines.append("⚠️ Erreur PnL live : {}".format(_e))

    lines += ["", sep,
              "⚡ PnL calculé sur prix actuel vs entrée.",
              "⚠️ Not financial advice.  🤖 AlphaBot PRO"]

    kb = {"inline_keyboard": [
        [{"text": "📜 Historique complet", "callback_data": "hist_full_1"}],
        [{"text": "◀️ Menu",              "callback_data": "start"}],
    ]}
    tg_send(uid, "\n".join(l for l in lines if l is not None), kb=kb)


def send_history_full(uid, page=1):
    """
    Historique complet de tous les signaux en DB.
    Affiche : date | paire | sens | entrée | TP | SL | RR | PNL estimé
    Paginé par 15 signaux, boutons Précédent / Suivant.
    """
    PAGE_SIZE = 15
    try:
        rows = db_all(
            "SELECT pair, side, entry, tp, sl, rr, g001, g1, l001, l1, session, sent_at "
            "FROM signals ORDER BY sent_at DESC", ())
    except Exception as _e:
        tg_send(uid, "❌ Erreur lecture DB : {}".format(_e), kb=kb_back())
        return

    rows = rows or []
    if not rows:
        tg_send(uid, "📭 <b>Aucun signal en historique.</b>\n\nLes signaux apparaîtront ici après le premier scan.", kb=kb_back())
        return

    total      = len(rows)
    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    page        = max(1, min(page, total_pages))
    start_idx   = (page - 1) * PAGE_SIZE
    page_rows   = rows[start_idx: start_idx + PAGE_SIZE]

    # ── Stats globales (toujours en tête) ────────────────────────
    total_g001  = round(sum(r[6] for r in rows), 2)
    total_g1    = round(sum(r[7] for r in rows), 2)
    total_l001  = round(sum(r[8] for r in rows), 2)
    total_l1    = round(sum(r[9] for r in rows), 2)
    wins        = sum(1 for r in rows if float(r[5] or 0) >= 3.0)
    losses      = total - wins
    wr          = int(wins / total * 100) if total else 0
    net_001     = round(total_g001 - total_l001, 2)
    net_1       = round(total_g1   - total_l1,   2)

    sep = "═" * 22
    lines = [
        "📜 <b>HISTORIQUE COMPLET</b>  ({} signaux)".format(total),
        sep,
        "✅ <b>{}</b> wins  ·  ❌ <b>{}</b> losses  ·  <b>{}%</b> win rate".format(wins, losses, wr),
        "💵 Net lot 0.01 : <b>{:+.2f}$</b>".format(net_001),
        "💰 Net lot 1.00 : <b>{:+.0f}$</b>".format(net_1),
        "",
        "━" * 20,
        "<b>Page {}/{}</b>  (signaux {} → {})".format(
            page, total_pages, start_idx + 1, min(start_idx + PAGE_SIZE, total)),
        "",
    ]

    # ── Détail par signal ─────────────────────────────────────────
    for r in page_rows:
        pair, side, entry, tp, sl, rr, g001, g1, l001, l1, sess, sent_at = r

        d      = "⬆️" if side == "BUY" else "⬇️"
        sf     = "ACHAT" if side == "BUY" else "VENTE"
        rr_val = float(rr or 0)

        # Déduire résultat probable depuis RR (≥3 = TP, sinon SL)
        if rr_val >= 3.0:
            result_ico = "✅"
            pnl_001    = "+{:.2f}$".format(float(g001 or 0))
            pnl_1      = "+{:.0f}$".format(float(g1   or 0))
        else:
            result_ico = "❌"
            pnl_001    = "-{:.2f}$".format(float(l001 or 0))
            pnl_1      = "-{:.0f}$".format(float(l1   or 0))

        # Formater la date
        try:
            dt_str = sent_at[:16].replace("T", " ") if sent_at else "?"
        except Exception:
            dt_str = "?"

        # Formater les prix selon magnitude
        try:
            ev = float(entry or 0)
            dp = 2 if ev > 100 else (3 if ev > 10 else 5)
            fmt = "{{:.{}f}}".format(dp)
            entry_fmt = fmt.format(ev)
            tp_fmt    = fmt.format(float(tp  or 0))
            sl_fmt    = fmt.format(float(sl  or 0))
        except Exception:
            entry_fmt = str(entry); tp_fmt = str(tp); sl_fmt = str(sl)

        lines += [
            "{} {} {} <b>{}</b>  ·  RR <b>1:{}</b>  {}".format(
                result_ico, d, sf, pair, rr_val, dt_str),
            "  📍 <code>{}</code>  →  TP <code>{}</code>  SL <code>{}</code>".format(
                entry_fmt, tp_fmt, sl_fmt),
            "  💵 <b>{}</b>  ·  💰 <b>{}</b>  ·  📡 {}".format(
                pnl_001, pnl_1, sess or "?"),
            "",
        ]

    lines += [sep, "⚠️ PNL estimé si TP/SL atteint. Not financial advice."]

    # ── Pagination ────────────────────────────────────────────────
    nav = []
    if page > 1:
        nav.append({"text": "◀️ Préc.", "callback_data": "hist_full_{}".format(page - 1)})
    if page < total_pages:
        nav.append({"text": "Suiv. ▶️", "callback_data": "hist_full_{}".format(page + 1)})

    kb = {"inline_keyboard": []}
    if nav:
        kb["inline_keyboard"].append(nav)
    kb["inline_keyboard"].append([{"text": "◀️ Rapports", "callback_data": "rapports"}])

    tg_send(uid, "\n".join(l for l in lines if l is not None), kb=kb)

def send_admin_full(uid):
    if uid!=ADMIN_ID: tg_send(uid,"❌ Accès refusé."); return
    total,pro,sigs,pays,g1d=global_stats(); sn,sm,sl_l,_=get_session(); sm=get_adaptive_score_min()
    st=db_daily_stats(); pend=pending_pays(); ch=chal_get(); reg=AI_REG
    tg_sticker(uid,STK_PRO)
    tg_send(uid,"🛡 <b>ADMIN — AlphaBot v10</b>\n"+"═"*22+"\n\n"
        "👥 Membres: <b>{}</b>  ·  PRO: <b>{}</b>  ·  FREE: <b>{}</b>\n"
        "📡 Signaux: <b>{}</b>  ·  Gains: <b>+${}</b>  ·  Payés: <b>{}</b>\n"
        "⏳ En attente: <b>{}</b>{}\n\n"
        "🤖 <b>IA:</b> {:.4f}$ AM:{}/4 W:{} L:{}\n"
        "🌍 Régime: <b>{}</b>  Positions: {}/{}\n\n"
        "🕐 Session: {}  Score min: {}\n\n"
        "/activate /degrade /scan /debug /stats /membres".format(
            total,pro,total-pro,st["n"],st["g1"],pays,len(pend),
            "  ⚠️ À valider!" if pend else "",
            ch["balance"],ch.get("am_cycle",0),ch.get("today_w",0),ch.get("today_l",0),
            reg.get("regime","?"),sum(1 for t in AI_OT.values() if t["status"]=="open"),MAX_OPEN,sl_l,sm),
        kb={"inline_keyboard":[
            [{"text":"💰 Paiements","callback_data":"adm_pays"},{"text":"📡 Scan forcé","callback_data":"adm_scan"}],
            [{"text":"📈 Rapports","callback_data":"rapports"}],
            [{"text":"🌍 État marchés","callback_data":"adm_markets"}],
        ]})

def send_guide(uid):
    tg_send(uid,
        "📖 <b>GUIDE AlphaBot PRO v10</b>\n"+"═"*22+"\n\n"
        "🧠 <b>STRATÉGIE ICT/SMC AVANCÉE (MULTI-TF)</b>\n\n"
        "1️⃣ <b>H1 Bias (BOS / CHoCH)</b>\n"
        "→ Tendance principale Smart Money\n\n"
        "2️⃣ <b>M15 Order Block (OB)</b>\n"
        "→ Zone institutionnelle d\'entrée haute probabilité\n\n"
        "3️⃣ 🧨 <b>Prise de liquidité OBLIGATOIRE</b>\n"
        "→ Sweep · Stop Hunt · EQH/EQL\n"
        "→ Aucun signal sans manipulation détectée\n\n"
        "4️⃣ 📊 <b>Score dynamique (0→115)</b>\n"
        "→ Structure · Momentum · Liquidité · Sessions · Multi-TF\n\n"
        "5️⃣ 🎯 <b>Confirmations avancées (bonus score) :</b>\n"
        "  ✔️ OTE (Fib 61.8–78.6%)\n"
        "  ✔️ FVG (Fair Value Gap)\n"
        "  ✔️ CHoCH x2 (structure forte)\n"
        "  ✔️ M1 aligné (timing sniper)\n"
        "  ✔️ H&S · Double Top/Bot · Breakout · Fake BO (M5)\n\n"
        "6️⃣ 💰 <b>Gestion automatique :</b>\n"
        "  • SL intelligent (OB + volatilité ATR)\n"
        "  • TP basé sur liquidité externe\n"
        "  • <b>RR minimum : 1:3 🔥</b>\n\n"
        "7️⃣ ⏱️ <b>Sessions optimisées :</b>\n"
        "  🇬🇧 London Kill Zone · 🇺🇸 New York\n"
        "  → Signaux hors session filtrés\n\n"
        "━"*20+"\n"
        "🤖 <b>IA Binance (PRO uniquement) :</b>\n"
        "  • Régime marché auto (6 types)\n"
        "  • Adaptation du risque en temps réel\n"
        "  • Mémoire des setups gagnants\n"
        "  • Challenge 5$→500$ géré automatiquement\n\n"
        "━"*20+"\n"
        "📊 FREE : {}/j  ·  💎 PRO : jusqu\'à {}/j\n\n"
        "🔥 <b>Pourquoi AlphaBot est différent ?</b>\n"
        "  ✔️ Seulement setups institutionnels\n"
        "  ✔️ Filtrage liquidité = Smart Money\n"
        "  ✔️ RR élevé = moins de trades, plus de gains\n"
        "  ✔️ IA adaptative temps réel\n\n"
        "⚠️ Risk 1–2% max par trade. Not financial advice.".format(FREE_LIMIT,PRO_LIMIT),
        kb=kb_back())
def send_broker(uid):
    tg_send(uid,"🏦 <b>BROKER — EXNESS</b>\n\n✅ Spread 0 pip (Raw)\n✅ Dépôt min 10$\n✅ FCA & CySEC\n✅ Crypto disponibles\n\n👉 <a href=\"{}\">🔗 Ouvrir Exness</a>".format(BROKER_LINK),kb=kb_back())

def send_ref(uid,uname):
    refs=db_get_refs(uid); link="https://t.me/{}?start={}".format(BOT_USER,uid)
    done=min(refs,REF_TARGET); bar="█"*int(done/REF_TARGET*10)+"░"*(10-int(done/REF_TARGET*10))
    tg_send(uid,"🤝 <b>PARRAINAGE</b>\n"+"═"*22+"\n\n<b>{}/{}</b>  ({}%)\n[{}]\n\n🏆 {} filleuls = {} MOIS PRO\n\n🔗 <code>{}</code>".format(done,REF_TARGET,int(done/REF_TARGET*100),bar,REF_TARGET,REF_MONTHS,link),kb=kb_back())

# ══════════════════════════════════════════════════════
#  DISPATCH
# ══════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════
#  FONCTIONS ORIGINALES v13 — INTÉGRÉES COMPLÈTES
# ══════════════════════════════════════════════════════

def _admin_only(uid):
    if uid != ADMIN_ID:
        tg_send(uid, "\u274c Accès refusé.")
        return False
    return True


def _auto_verify_and_activate(uid, uname, tx_hash):
    """Vérification auto TronScan + activation si OK."""
    delays = [10, 60, 120]
    for i, delay in enumerate(delays):
        time.sleep(delay)
        result, amount = verify_tx(tx_hash)
        if result is None:
            # Réseau TronScan inaccessible → activation manuelle directe
            tg_send(uid,
                "\u26a0\ufe0f <b>Vérification impossible</b>\n\n"
                "Le réseau TronScan est inaccessible depuis le serveur.\n"
                "L'admin va activer ton PRO <b>manuellement dans 5 min</b>.\n\n"
                "\U0001f4e9 @leaderOdg")
            tg_send(ADMIN_ID,
                "\U0001f534 <b>RÉSEAU TRONSCAN INDISPONIBLE</b>\n"
                "@{} <code>{}</code>\n"
                "Hash : <code>{}</code>\n\n"
                "\u26a0\ufe0f Vérification auto impossible — active manuellement :\n"
                "\U0001f6e0 /activate {}".format(uname or "?", uid, tx_hash, uid))
            return
        if result:
            db_activate_pro(uid, "USDT_AUTO", days=None)
            tg_send_sticker(uid, STK_WIN)
            tg_send(uid,
                "\U0001f389 <b>PAIEMENT CONFIRMÉ !</b>\n\n"
                "\u2705 {}$ USDT reçu !\n\n"
                "\U0001f4a0 <b>PRO ACTIVÉ À VIE !</b>\n\n"
                "\u2705 Max {} signaux/j\n"
                "\u2705 24 paires + crypto week-end\n"
                "\u2705 Rapports quotidien + hebdo\n"
                "\u2705 Support @leaderOdg\n\n"
                "\U0001f680 Bienvenue dans AlphaBot PRO !".format(amount, PRO_LIMIT))
            tg_send(ADMIN_ID,
                "\U0001f7e2 <b>AUTO PRO OK</b> : @{} <code>{}</code>  {}$ \u2705".format(
                    uname or "?", uid, amount))
            log("PAY", clr("AUTO PRO: @{} {} — {}$".format(uname, uid, amount), "green"))
            return
        elif i < len(delays) - 1:
            log("INFO", clr("TX non confirmé (tentative {}/3)".format(i + 1), "yellow"))
    # Toutes tentatives échouées → activation manuelle
    tg_send(uid,
        "\u23f3 <b>Vérification en cours côté admin</b>\n\n"
        "Ta transaction n'est pas encore visible sur la blockchain.\n"
        "L'admin va activer manuellement dans 30 min.\n\n"
        "\U0001f4e9 @leaderOdg")
    tg_send(ADMIN_ID,
        "\u26a0\ufe0f <b>ACTIVATION MANUELLE REQUISE</b>\n"
        "@{} <code>{}</code>\n"
        "Hash : <code>{}</code>\n\n"
        "\U0001f6e0 /activate {}".format(uname or "?", uid, tx_hash, uid))



def _build_promo_text(promo_id):
    """Construit le texte du message promo (gère le cas dynamique)."""
    promo = next((p for p in PROMO_MESSAGES if p["id"] == promo_id), None)
    if not promo: return None
    if promo_id != "promo_4":
        return promo["text"]
    stats = db_daily_stats(); rows = stats["rows"]
    if not rows: return None
    lines = ["📊 <b>RÉSULTATS D'AUJOURD'HUI</b> 📊\n"]
    for row in rows:
        pair,side,rr,g001,g1,l001,l1,session = row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7]
        ok=rr>=3.0; icon="🟢" if ok else "🔴"
        d="ACHAT" if side=="BUY" else "VENTE"
        res="✅ TP → <b>+${:.0f}</b>".format(g1) if ok else "❌ SL → <b>-${:.0f}</b>".format(l1)
        lines.append("{} <b>{}</b> {}  {} (lot 0.01)".format(icon,pair,d,res))
    lines += ["",
        "💰 <b>Total : +${}</b> lot 0.01  ·  +${} lot 1.00 🔥".format(stats["total_g001"],stats["total_g1"]),
        "","Et toi tu étais où pendant ces moves ? 👀","",
        "📩 Rejoins la communauté\n➡️ @leaderodg_bot"]
    return "\n".join(lines)


def _check_open_signals():
    """Vérifie si les signaux ouverts ont atteint TP ou SL."""
    try:
        open_sigs = db_get_open_signals()
        if not open_sigs: return
        for track_id, pair, entry, tp, sl, side, created in open_sigs:
            # Vérifier si le signal a moins de 4h (sinon on abandonne)
            try:
                age = (datetime.now() - datetime.fromisoformat(created)).total_seconds() / 3600
                if age > 4:
                    db_close_signal_tracking(track_id, "EXPIRED")
                    continue
            except: continue
            # Récupérer le prix actuel
            mkt = next((m for m in MARKETS if m["name"] == pair), None)
            if not mkt: continue
            try:
                c = fetch_c(mkt["sym"], "5m", "1d")
                if not c: continue
                current = c[-1]["c"]
                if side == "BUY":
                    if current >= tp:
                        db_close_signal_tracking(track_id, "TP")
                        _notify_result(pair, side, entry, tp, sl, "TP", current)
                    elif current <= sl:
                        db_close_signal_tracking(track_id, "SL")
                        _notify_result(pair, side, entry, tp, sl, "SL", current)
                else:
                    if current <= tp:
                        db_close_signal_tracking(track_id, "TP")
                        _notify_result(pair, side, entry, tp, sl, "TP", current)
                    elif current >= sl:
                        db_close_signal_tracking(track_id, "SL")
                        _notify_result(pair, side, entry, tp, sl, "SL", current)
            except: continue
    except Exception as e:
        log("WARN", clr("Suivi signal échoué: {}".format(e), "yellow"))


def _do_backup():
    """Envoie une copie de la DB à l'admin sur Telegram."""
    try:
        import shutil
        backup_path = "/tmp/alphabot_backup_{}.db".format(
            datetime.now().strftime("%Y%m%d_%H%M"))
        shutil.copy2(DB_FILE, backup_path)
        with open(backup_path, "rb") as f:
            data = f.read()
        tg_send_document(ADMIN_ID, data,
            "alphabot_backup_{}.db".format(datetime.now().strftime("%Y%m%d")),
            "\U0001f4be <b>Backup quotidien</b> \u2014 {}\n"
            "Conserve ce fichier en lieu sûr.".format(
                datetime.now().strftime("%d/%m/%Y %H:%M")))
        log("INFO", clr("Backup DB envoyé à l'admin.", "green"))
    except Exception as e:
        log("WARN", clr("Backup échoué: {}".format(e), "yellow"))


def _fmt_daily_report(stats):
    date_fr = datetime.strptime(stats["date"], "%Y-%m-%d").strftime("%d/%m/%Y")
    sc = stats["sig_count"]; w = stats["wins"]; l = stats.get("losses", sc - w)
    if sc == 0:
        return "\U0001f4ca <b>RAPPORT {}</b>\n\nAucun signal aujourd'hui.".format(date_fr)
    wr   = int(w / sc * 100)
    perf = "\U0001f525\U0001f525" if stats["total_g1"] > 2000 else \
           "\U0001f525" if stats["total_g1"] > 1000 else "\U0001f4b0"

    # Estimations par taille de lot
    g_001 = stats["total_g001"]
    g_01  = round(g_001 * 10, 2)
    g_1   = stats["total_g1"]

    lines = [
        "\U0001f4af <b>RAPPORT DU JOUR \u2014 AlphaBot PRO</b> {}".format(perf),
        "\u2550" * 22,
        "\U0001f4c5 {}  \u00b7  Session fermée".format(date_fr), "",
        "\U0001f3af <b>BILAN GLOBAL</b>",
        "  \u2705 TP : <b>{}</b>  |  \u274c SL : <b>{}</b>  |  <b>{}%</b> réussite".format(w, l, wr),
        "",
        "\U0001f4b0 <b>ESTIMATIONS DE GAINS :</b>",
        "  Lot 0.01 → <b>+${}</b>".format(g_001),
        "  Lot 0.10 → <b>+${}</b>".format(g_01),
        "  Lot 1.00 → <b>+${}</b> 🔥".format(g_1),
        "", "\u2501" * 22,
        "\U0001f4cb <b>DÉTAIL DES TRADES</b>", ""
    ]

    for row in stats["rows"]:
        pair    = row[0]; side = row[1]; rr   = row[2]
        g001    = row[3]; g1   = row[4]; l001 = row[5]; l1 = row[6]
        entry   = row[8]  if len(row) > 8  else "—"
        tp      = row[9]  if len(row) > 9  else "—"
        sl      = row[10] if len(row) > 10 else "—"
        ok      = rr >= 3.0
        d       = "\u2b06\ufe0f" if side == "BUY" else "\u2b07\ufe0f"
        sf      = "ACHAT" if side == "BUY" else "VENTE"
        outcome = "\u2705 TP ATTEINT" if ok else "\u274c SL TOUCHÉ"
        gain_lot001 = "+${:.2f}".format(g001) if ok else "-${:.2f}".format(l001)
        gain_lot1   = "+${:.0f}".format(g1)   if ok else "-${:.0f}".format(l1)

        lines.append("{} <b>{}</b>  {} {}  \u2014  RR <b>1:{}</b>".format(
            "\U0001f7e2" if ok else "\U0001f534", pair, d, sf, rr))
        lines.append("  {} {}".format(outcome, gain_lot1 + " (lot 1.00)"))
        lines.append("  \U0001f4cd Entrée : <code>{}</code>".format(entry))
        lines.append("  \u2705 TP     : <code>{}</code>".format(tp))
        lines.append("  \u274c SL     : <code>{}</code>".format(sl))
        lines.append("  \U0001f4b5 Lot 0.01 : <b>{}</b>  \u00b7  Lot 1.00 : <b>{}</b>".format(
            gain_lot001, gain_lot1))
        lines.append("")

    lines += [
        "\u2550" * 22,
        "\U0001f4b0 <b>Total estimé :</b>",
        "  Lot 0.01 : <b>+${}</b>  \u00b7  Lot 0.10 : <b>+${}</b>  \u00b7  Lot 1.00 : <b>+${}</b>".format(
            g_001, g_01, g_1),
        "",
        "\U0001f4e9 /ref \u2014 Parraine tes amis = PRO GRATUIT !",
        "\u26a0\ufe0f Not financial advice  \u00b7  Risk 1% max  \u00b7  @leaderodg_bot"
    ]
    return "\n".join(lines)


def _fmt_scan_report(results, news_lbl, scan_time, sl, score_min, nb_found):
    stats   = db_daily_stats()
    news_ok = "\u2705" in news_lbl or "clear" in news_lbl.lower()
    lines   = [
        "\U0001f50d <b>SCAN {} UTC</b>  \u00b7  {}  \u00b7  {} paires".format(
            scan_time, sl, len(results)),
        "\U0001f3af Score min : <b>{}</b>  \u00b7  News : {}  \u00b7  {} agents".format(
            score_min, "\u2705 OK" if news_ok else "\u26a0\ufe0f Actif", NB_AGENTS),
        "\U0001f4b5 Aujourd'hui : <b>+${}</b> lot1  \u00b7  {} sig  \u00b7  {} gagnants".format(
            stats["total_g1"], stats["sig_count"], stats["wins"]), ""]
    cats = {}
    for r in results:
        cats.setdefault(r.get("cat", "?"), []).append(r)
    for cat in ["METALS", "CRYPTO", "FOREX", "INDICES", "OIL"]:
        if cat not in cats: continue
        emo = CAT_EMO.get(cat, "\U0001f4ca")
        lines.append("{} <b>{}</b>".format(emo, cat))
        for r in cats[cat]:
            if r["found"]:
                s  = r["signal"]
                se = "\U0001f7e2" if s["side"] == "BUY" else "\U0001f534"
                sf = "ACHAT" if s["side"] == "BUY" else "VENTE"
                lines.append("  {} <b>{}</b>  {}  RR 1:{}  {}/100  +${} lot1".format(
                    se, r["name"], sf, s["rr"], s["score"], s.get("g1", 0)))
                lines.append("  \U0001f4cd <code>{}</code> \u2192 TP <code>{}</code>  SL <code>{}</code>".format(
                    s["entry"], s["tp"], s["sl"]))
            else:
                lines.append("  \u26aa <b>{}</b>  {}".format(r["name"], r.get("reason", "?")))
        lines.append("")
    lines.append("\u2550" * 22)
    lines.append("\U0001f7e2 <b>{} signal(s) envoyé(s) !</b>".format(nb_found) if nb_found
                 else "\U0001f7e1 Aucun signal ce cycle")
    lines.append("\U0001f504 Prochain scan dans ~4 min  \u00b7  AlphaBot PRO")
    return "\n".join(lines)


def _fmt_weekly_report(stats):
    sc = stats["sig_count"]; w = stats["wins"]
    if sc == 0:
        return "\U0001f4ca <b>RAPPORT HEBDO</b>\n\nAucun signal cette semaine."
    wr   = int(w / sc * 100) if sc else 0
    perf = "\U0001f525\U0001f525" if stats["total_g1"] > 10000 else \
           "\U0001f525" if stats["total_g1"] > 5000 else "\U0001f4b0"
    return (
        "\U0001f3c6 <b>RAPPORT HEBDOMADAIRE \u2014 AlphaBot PRO</b> {}\n".format(perf) +
        "\u2550" * 22 + "\n\n" +
        "\U0001f4c5 Semaine du {}\n\n"
        "\U0001f4b5 <b>LOT 0.01 : +${}</b>\n"
        "\U0001f4b0 <b>LOT 1.00 : +${}</b>\n\n"
        "\U0001f4ca {} signaux  \u00b7  {} gagnants  \u00b7  {}% réussite\n\n".format(
            stats["week_start"], stats["total_g001"], stats["total_g1"], sc, w, wr) +
        "\u2550" * 22 + "\n"
        "\U0001f4e9 Rejoins AlphaBot PRO\n"
        "\U0001f449 @leaderodg_bot \u2014 {}$ USDT\n\n"
        "\u26a0\ufe0f Not financial advice  \u00b7  Risk 1% max".format(PRO_PROMO)
    )


def _make_pdf_placeholder():
    pages = [
        [("ALPHABOT PRO v8.5 — GUIDE COMPLET", True),
         ("Bot de signaux trading — ICT/SMC — 24 marches — 20 agents IA", False),
         ("", False), ("="*46, False),
         ("1. QU'EST-CE QU'ALPHABOT ?", True), ("", False),
         ("AlphaBot est un bot Telegram automatique qui analyse", False),
         ("24 marches financiers en temps reel grace a 20 agents IA.", False),
         ("", False), ("Marches surveilles :", True),
         ("  Metaux    : XAUUSD (Or), XAGUSD (Argent)", False),
         ("  Crypto    : BTCUSD ETHUSD SOLUSD BNBUSD XRPUSD", False),
         ("  Forex     : EURUSD GBPUSD USDJPY GBPJPY + 6 autres", False),
         ("  Indices   : NAS100 SPX500 US30 UK100 GER40", False),
         ("  Energie   : USOIL NATGAS", False)],
        [("2. METHODE ICT / SMC", True), ("", False),
         ("ETAPE 1 : H1 BIAS (BOS / CHoCH)", True),
         ("  BOS = Break of Structure (continuation)", False),
         ("  CHoCH = Change of Character (retournement)", False),
         ("", False), ("ETAPE 2 : BREAKER BLOCK M5", True),
         ("  Zone d'entree issue d'une bougie invalidee.", False),
         ("", False), ("ETAPE 3 : SCORE (sur 100 pts)", True),
         ("  +35 pts : Direction bougie (sens du bias)", False),
         ("  +25 pts : Corps > 50% du range (displacement)", False),
         ("  +20 pts : Rejet de wick (liquidite prise)", False),
         ("  +10 pts : Momentum (bougie precedente)", False),
         ("  +5+5 pts : Confirmation & englobante", False),
         ("", False), ("ETAPE 4 : SL / TP AUTOMATIQUES", True),
         ("  SL = bas/haut du Breaker +/- ATR x 0.15", False),
         ("  TP = SL etendu au RR >= 2.5", False)],
        [("3. SIGNAUX EN DIRECT (LIVE)", True), ("", False),
         ("Les donnees sont verifiees en temps reel.", False),
         ("Si les donnees ont plus de 15 min, le signal est rejete.", False),
         ("Chaque signal affiche sa validite en minutes.", False),
         ("", False), ("4. PLANS FREE ET PRO", True), ("", False),
         ("Plan FREE : 2 signaux/jour, lot 0.01", False),
         ("Plan PRO : max 10/j, lots 0.01+0.10+1.00, rapports", False),
         ("", False), ("5. DEVENIR PRO", True), ("", False),
         ("Option 1 : 10$ USDT TRC20 -> Acces immediat", False),
         ("Option 2 : 30 filleuls = 3 mois PRO gratuit", False),
         ("Activation automatique dans les 2 minutes !", False)],
        [("6. GESTION DU RISQUE", True), ("", False),
         ("REGLE D'OR : Max 1-2% du capital par trade", False),
         ("  Capital 500$  : max 5-10$ par trade", False),
         ("  Capital 1000$ : max 10-20$ par trade", False),
         ("  Capital 5000$ : max 50-100$ par trade", False),
         ("", False), ("7. GLOSSAIRE ICT/SMC", True), ("", False),
         ("BOS  : Break of Structure — continuation", False),
         ("CHoCH: Change of Character — retournement", False),
         ("ATR  : Average True Range (volatilite)", False),
         ("RR   : Risque/Recompense — min 2.5", False),
         ("Breaker Block : Zone d'entree cle", False),
         ("Displacement : Bougie corps > 50% du range", False)],
        [("8. COMMANDES TELEGRAM", True), ("", False),
         ("  /start    : Menu principal + inscription", False),
         ("  /pay      : Paiement PRO (10$ USDT)", False),
         ("  /txhash   : Soumettre un TX Hash", False),
         ("  /ref      : Lien parrainage + texte promo", False),
         ("  /account  : Mon compte + statut PRO", False),
         ("  /guide    : Ce guide + PDF", False),
         ("  /broker   : Lien broker Exness", False),
         ("  /support  : Contacter l'admin @leaderOdg", False),
         ("", False), ("  --- Commandes Admin ---", True),
         ("  /activate /degrade /testfree /testpro", False),
         ("  /scan /debug /resetcount /monstatus", False),
         ("  /stats /membres /marches", False),
         ("", False), ("AlphaBot PRO v8.5 — @leaderodg_bot", True),
         ("Not financial advice — Risk 1-2% max par trade", False)],
    ]
    def build_page(lines_text):
        cl = ["BT"]; y = 780
        for text, bold in lines_text:
            if text == "":
                y -= 7; continue
            size = 11 if bold else 8
            safe = text.replace("\\","\\\\").replace("(","\\(").replace(")","\\)")
            safe = safe.encode("latin-1", errors="replace").decode("latin-1")
            cl.append("/F1 {} Tf".format(size))
            cl.append("30 {} Td".format(y))
            cl.append("({}) Tj".format(safe))
            cl.append("0 0 Td")
            y -= (13 if bold else 11)
            if y < 40: y = 780
        cl.append("ET")
        return "\n".join(cl).encode("latin-1", errors="replace")
    objects = []
    nb = len(pages)
    objects.append(b"1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n")
    kids = " ".join("{} 0 R".format(i * 3 + 3) for i in range(nb))
    objects.append("2 0 obj\n<< /Type /Pages /Kids [{}] /Count {} >>\nendobj\n".format(kids, nb).encode())
    for i, page_lines in enumerate(pages):
        pg_content = build_page(page_lines)
        pg_obj_id  = i * 3 + 3
        cont_id    = pg_obj_id + 1
        font_id    = pg_obj_id + 2
        objects.append(("{} 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 595 842] "
            "/Contents {} 0 R /Resources << /Font << /F1 {} 0 R >> >> >>\nendobj\n"
        ).format(pg_obj_id, cont_id, font_id).encode())
        stream = b"stream\n" + pg_content + b"\nendstream"
        objects.append(("{} 0 obj\n<< /Length {} >>\n".format(cont_id, len(pg_content))
        ).encode() + stream + b"\nendobj\n")
        objects.append(("{} 0 obj\n<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica "
            "/Encoding /WinAnsiEncoding >>\nendobj\n").format(font_id).encode())
    pdf = b"%PDF-1.4\n"; offsets = []
    for obj in objects:
        offsets.append(len(pdf)); pdf += obj
    xref = len(pdf)
    pdf += "xref\n0 {}\n".format(len(objects) + 1).encode()
    pdf += b"0000000000 65535 f \n"
    for off in offsets:
        pdf += "{:010d} 00000 n \n".format(off).encode()
    pdf += "trailer\n<< /Size {} /Root 1 0 R >>\n".format(len(objects) + 1).encode()
    pdf += "startxref\n{}\n%%EOF".format(xref).encode()
    return pdf



def _notify_result(pair, side, entry, tp, sl, result, current):
    # Résultats TP/SL uniquement le soir (>= DAILY_HOUR)
    if datetime.now(timezone.utc).hour < DAILY_HOUR:
        return  # silencieux pendant la journée

def _relance_inactifs():
    """Envoie un message de relance aux utilisateurs FREE inactifs."""
    try:
        inactifs = db_get_inactive_users(days=INACTIF_DAYS)
        if not inactifs: return
        stats = db_daily_stats()
        for uid, uname in inactifs[:20]:  # max 20 par cycle
            try:
                fname = "@" + uname if uname else "Trader"
                tg_send(uid,
                    "\U0001f44b <b>Hey {} !</b>\n\n".format(fname) +
                    "\U0001f4ca AlphaBot a envoyé <b>{} signaux</b> aujourd'hui\n"
                    "avec <b>+${}</b> de gains estimés (lot 1.00)\n\n".format(
                        stats["sig_count"], stats["total_g1"]) +
                    "\u2705 {} TP atteints  \u00b7  {}% réussite\n\n".format(
                        stats["wins"],
                        int(stats["wins"]/stats["sig_count"]*100) if stats["sig_count"] else 0) +
                    "Tu rates ces opportunités !\n\n"
                    "\U0001f916 Reviens voir tes signaux :\n"
                    "\U0001f449 @leaderodg_bot",
                    kb=kb_main(False))
                time.sleep(0.1)
            except: pass
        log("INFO", clr("Relance envoyée à {} inactifs.".format(len(inactifs[:20])), "dim"))
    except Exception as e:
        log("WARN", clr("Relance échouée: {}".format(e), "yellow"))


def _scan_and_send_inner():
    global _sent, _last_daily, _last_weekly, _last_scan_results

    now_dt    = datetime.now(timezone.utc).replace(tzinfo=None)
    scan_time = now_dt.strftime("%H:%M")
    date_str  = now_dt.strftime("%Y-%m-%d")
    hour_str  = now_dt.strftime("%H")
    wday      = now_dt.weekday()

    sn, sm, sl, wknd = get_session()
    # Score minimum adaptatif (session + régime marché)
    sm = get_adaptive_score_min()
    log("INFO", clr("Scan {} — {} — Score min:{}  [{} marchés]".format(
        scan_time, sl, sm, len(MARKETS)), "dim"))
    news_ok, news_lbl = news_check()

    active_markets = [m for m in MARKETS if not wknd or m.get("crypto", False)]
    if wknd:
        log("INFO", clr("Week-end : {} marchés crypto".format(len(active_markets)), "yellow"))

    result_queue = Queue()
    threads = []
    for i in range(0, len(active_markets), NB_AGENTS):
        batch = active_markets[i:i + NB_AGENTS]
        for m in batch:
            t = threading.Thread(
                target=agent_analyze,
                args=(m, sm, news_ok, result_queue), daemon=True)
            t.start(); threads.append(t)
    for t in threads:
        t.join(timeout=12)

    raw = {}
    while not result_queue.empty():
        try: r = result_queue.get_nowait(); raw[r["name"]] = r
        except Empty: break
    results = [raw.get(m["name"], {"name": m["name"], "cat": m["cat"],
                "found": False, "reason": "Timeout"}) for m in active_markets]
    if wknd:
        for m in MARKETS:
            if not m.get("crypto", False):
                results.append({"name": m["name"], "cat": m["cat"],
                                "found": False, "reason": "Fermé le week-end"})

    _last_scan_results = results
    cleanup_sent(date_str)

    # Clé sans hour_str → 1 seul signal par paire/sens par JOUR (pas par heure)
    sigs_raw = [(r["signal"],
                 "{}-{}-{}".format(r["signal"]["name"], r["signal"]["side"], date_str))
                for r in results if r["found"]]
    with _sent_lock:
        sigs_raw = [(s, k) for s, k in sigs_raw if k not in _sent]
    sigs_raw.sort(key=lambda x: -x[0]["score"])

    # ── ✨ Validation Dual-AI (Claude/Gemini) — Risk Manager ────────────
    # Pipeline : Algo (analyste) → IA (validateur) → Script (juge)
    if CLAUDE_API_KEY or GEMINI_API_KEY:
        sigs_validated_ai = []
        for sig, key in sigs_raw:
            if sig.get("rr", 0) >= 2.0 and sig.get("score", 0) >= sm:
                htf_trend  = sig.get("bias", "BULLISH")
                ai_result  = claude_validate_signal(sig, sn, htf_trend)
                sig["ai_result"] = ai_result
                # Injecter les données fondamentales IA dans le signal
                if ai_result.get("biais_fondamental"): sig["fund_bias_ia"]  = ai_result["biais_fondamental"]
                if ai_result.get("news_impact"):        sig["news_impact_ia"] = ai_result["news_impact"]
                if ai_result["validated"]:
                    sigs_validated_ai.append((sig, key))
                else:
                    log("AI", "❌ {} rejeté — {} (hybride {}/100)".format(
                        sig["name"],
                        ai_result.get("raison","?")[:60],
                        ai_result.get("final_score", 0)))
            else:
                sig["ai_result"] = {}
                sigs_validated_ai.append((sig, key))
        log("AI", "Filtre IA : {}/{} setups validés".format(len(sigs_validated_ai), len(sigs_raw)))
        sigs_raw = sigs_validated_ai
    # ─────────────────────────────────────────────────────────────────────

    pro_users  = db_get_pro_users()
    free_users = db_get_free_users()
    pro_users_eff  = [u for u in pro_users if not (u == ADMIN_ID and _admin_test_mode == "FREE")]
    free_users_eff = list(free_users) + (
        [ADMIN_ID] if _admin_test_mode == "FREE" and ADMIN_ID not in free_users else [])

    for sig, key in sigs_raw:
        # ── Blocage signaux après 22h00 UTC ─────────────────────────
        now_check = datetime.now(timezone.utc).replace(tzinfo=None)
        if now_check.hour >= SIGNAL_CUTOFF_HOUR:
            log("INFO", clr("Signal {} bloqué — après 22h00 UTC".format(sig["name"]), "yellow"))
            continue

        # ── Throttle global : max 1/h, max 10/j, gap 30min ──────────
        ok_send, reason_throttle = _throttle_allowed(now_check)
        if not ok_send:
            log("INFO", clr("Signal {} ignoré — {}".format(sig["name"], reason_throttle), "yellow"))
            continue

        msg_pro    = fmt_signal_pro(sig, news_lbl, sl)
        msg_teasing = fmt_signal_teasing(sig)
        sc         = sig.get("score", 0)
        stk        = STK_CROWN if sc >= 90 else STK_MONEY if sig["side"]=="BUY" else STK_FIRE

        # ── Groupe FREE → teasing uniquement (pas de niveaux) ───────
        ref_admin = "https://t.me/{}?start={}".format(BOT_USER, ADMIN_ID)
        tg_send(CHANNEL_ID, msg_teasing,
                kb={"inline_keyboard": [
                    [{"text": "💵 Payer 10$/mois",      "url": ref_admin}],
                    [{"text": "🤝 Parrainer 10 amis",   "url": ref_admin}],
                    [{"text": "📢 Partager ce groupe",   "url": FREE_GROUP_LINK},
                     {"text": "👑 Groupe VIP",           "url": VIP_GROUP_LINK}],
                ]})

        # ── Groupe VIP → 1 seul message PRO complet ─────────────────
        tg_send(VIP_CH, msg_pro)

        # ── Enregistrement signal ────────────────────────────────────
        with _sent_lock: _sent.add(key)
        db_save_signal(sig, sn)
        _throttle_record(now_check)
        sc_txt = clr(sig["side"], "green") if sig["side"] == "BUY" else clr(sig["side"], "red")
        log("SIGNAL", "{} {}  RR 1:{}  Score {}/{}  G1 +${}".format(
            clr(sig["name"], "bold", "white"), sc_txt,
            sig["rr"], sig["score"], sig.get("score_min", "?"), sig["g1"]))

        # ── DM individuels : 1 message, sans sticker ─────────────────
        for puid in pro_users_eff:
            if db_count_today(puid) < PRO_LIMIT:
                tg_send(puid, msg_pro)
                db_count_increment(puid)
                time.sleep(0.04)

        for fuid in free_users_eff:
            c = db_count_today(fuid)
            if c < FREE_LIMIT:
                tg_send(fuid, msg_teasing)
                db_count_increment(fuid)
                time.sleep(0.04)
            # Au-delà de la limite FREE → silence total

    if not sigs_raw:
        log("INFO", clr("Aucun setup valide ce cycle.", "dim"))

    # ── Rapport soir 22h UTC — UNE SEULE FOIS ────────────────────────
    if int(hour_str) == DAILY_HOUR and _last_daily != date_str and not db_report_sent(date_str):
        stats = db_daily_stats(date_str)
        if stats["sig_count"] > 0:
            daily_pro  = _fmt_daily_report(stats)
            daily_free = _fmt_daily_report(stats)
            ref_admin2 = "https://t.me/{}?start={}".format(BOT_USER, ADMIN_ID)
            free_foot = (
                "\n━" + "━"*21 + "\n"
                "📡 <b>{} signaux envoyés aujourd'hui aux membres PRO/VIP</b>\n\n"
                "👑 <b>Rejoins la version PRO — 3 options :</b>\n\n"
                "1️⃣ 💵 Payer 10$/mois → /pay\n"
                "2️⃣ 🤝 Parrainer 10 personnes → 7j PRO gratuits\n"
                "3️⃣ 📢 Partager ce groupe (10–30 personnes + capture à @leaderOdg)\n\n"
                "🔗 Lien à partager : <code>{}</code>"
            ).format(stats["sig_count"], FREE_GROUP_LINK)
            # Groupe FREE : rapport + CTA
            tg_send(CHANNEL_ID, daily_free + free_foot,
                    kb={"inline_keyboard": [
                        [{"text": "💵 Payer 10$/mois",    "url": ref_admin2}],
                        [{"text": "🤝 Parrainer 10 amis", "url": ref_admin2}],
                        [{"text": "👑 Groupe VIP",        "url": VIP_GROUP_LINK}],
                    ]})
            # Groupe VIP : rapport complet
            tg_send(VIP_CH, daily_pro)
            # DM à tous
            all_uids = list(set(pro_users + list(free_users_eff)))
            for puid in all_uids:
                is_p = puid in pro_users
                tg_send(puid, daily_pro if is_p else daily_free)
                time.sleep(0.05)
            db_mark_report(stats); _last_daily = date_str

    week_key = "{}-W{}".format(now_dt.year, now_dt.isocalendar()[1])
    if (wday == WEEKLY_DAY and int(hour_str) == WEEKLY_HOUR
            and _last_weekly != week_key
            and not db_report_sent(week_key, "weekly_reports", "week_start")):
        ws = db_weekly_stats()
        if ws["sig_count"] > 0:
            weekly = _fmt_weekly_report(ws)
            # Rapport hebdo → DM PRO uniquement (pas dans les groupes)
            for puid in pro_users:
                tg_send(puid, weekly); time.sleep(0.05)
            db_mark_report(ws, "weekly_reports"); _last_weekly = week_key

    expired = db_check_expiry()
    for uid, uname in expired:
        # Ne pas downgrader si c'était un essai → message spécifique
        plan, exp, src = db_get_pro_info(uid)
        if src and "TRIAL" in (src or ""):
            tg_send(uid,
                "\u23f0 <b>Ton essai PRO de {} jours est terminé !</b>\n\n"
                "Tu as pu voir la puissance des signaux AlphaBot.\n\n"
                "\U0001f4a0 Continue avec le <b>Plan PRO à {}$ USDT</b>\n"
                "et garde accès à tous les signaux !\n\n"
                "\U0001f449 /pay \u2014 Activation immédiate".format(TRIAL_DAYS, PRO_PROMO))
        else:
            tg_send(uid,
                "\u23f0 <b>PRO expiré</b>\n\n"
                "Renouveler :\n/pay \u2192 {}$ USDT\n"
                "/ref \u2192 {} filleuls = {} mois gratuit".format(PRO_PROMO, REF_TARGET, REF_MONTHS))
        tg_send(ADMIN_ID, "\u23f0 PRO expiré: @{} <code>{}</code>".format(uname or "?", uid))
    if expired:
        log("WARN", clr("{} PRO expiré(s) → FREE".format(len(expired)), "yellow"))

    # ── Backup quotidien à DAILY_HOUR ─────────────────────────
    if int(hour_str) == DAILY_HOUR and date_str != getattr(_scan_and_send_inner, "_last_backup", ""):
        _scan_and_send_inner._last_backup = date_str
        threading.Thread(target=_do_backup, daemon=True).start()

    # ── Relance utilisateurs inactifs (toutes les 6h) ─────────
    if int(hour_str) % 6 == 0 and date_str + hour_str != getattr(_scan_and_send_inner, "_last_relance", ""):
        _scan_and_send_inner._last_relance = date_str + hour_str
        threading.Thread(target=_relance_inactifs, daemon=True).start()

    # ── Suivi TP/SL des signaux ouverts ───────────────────────
    threading.Thread(target=_check_open_signals, daemon=True).start()



def _signal_validity(sig):
    """
    Calcule la validité restante du signal en minutes.
    Un signal M5 est valable ~3 bougies = 15 min.
    """
    age_sec  = time.time() - sig.get("ts", time.time())
    age_min  = age_sec / 60
    validity = max(0, int(DATA_MAX_AGE_MIN - age_min))
    return validity


def calc_atr(c, p=14):
    t = [max(c[i]["h"]-c[i]["l"], abs(c[i]["h"]-c[i-1]["c"]), abs(c[i]["l"]-c[i-1]["c"]))
         for i in range(1, len(c))]
    s = t[-p:] if len(t) >= p else t
    return sum(s) / len(s) if s else 0.001


def check_conf(c, b):
    """
    Score de confirmation ICT — 100 points maximum.

    CONFIRMATIONS REQUISES (par ordre d'importance) :
    ┌─────────────────────────────────────────┬──────┐
    │ Bougie dans le sens du bias             │ +35  │
    │ Corps > 50% du range (displacement)     │ +25  │
    │ Rejet de wick (liquidité prise)          │ +20  │
    │ Momentum (bougie précédente confirme)    │ +10  │
    │ Bougie -2 confirme (série directionnelle)│ +5   │
    │ Englobante (dépasse high/low précédent) │ +5   │
    ├─────────────────────────────────────────┼──────┤
    │ BONUS ICT v2 :                          │      │
    │ CHoCH consécutifs (2+ = fort signal)    │ +5→15│
    │ Equal High/Low touché (pool liquidité)  │ +10  │
    │ OTE Zone 61.8-78.6% Fibonacci           │ +12  │
    │ FVG (Fair Value Gap) en retest          │ +15  │
    │ BOS pur avec momentum fort              │ +10  │
    └─────────────────────────────────────────┴──────┘
    Score minimum pour signal : 61-82 selon session
    """
    if len(c) < 3: return 0
    c1 = c[-1]; c2 = c[-2]; c3 = c[-3]
    o = c1["o"]; cc = c1["c"]; h = c1["h"]; l = c1["l"]
    body = abs(cc - o); rng = h - l
    if rng == 0: return 0
    ratio = body / rng; s = 0

    if b == "BULLISH":
        if cc > o:                        s += 35   # Direction correcte
        if ratio > 0.5:                   s += 25   # Displacement fort
        if min(o,cc) - l > body * 0.15:  s += 20   # Rejet bas (wick)
        if c2["c"] < cc:                  s += 10   # Momentum M-1
        if c3["c"] < c2["c"]:            s +=  5   # Série haussière
        if cc > c2["h"]:                  s +=  5   # Englobante haussière
        # Pénalités
        if ratio < 0.3:                   s -= 10   # Corps trop faible
        if h - max(o,cc) > body * 0.5:   s -=  5   # Wick haut trop long
    else:
        if cc < o:                         s += 35
        if ratio > 0.5:                    s += 25
        if h - max(o,cc) > body * 0.15:   s += 20
        if c2["c"] > cc:                   s += 10
        if c3["c"] > c2["c"]:             s +=  5
        if cc < c2["l"]:                   s +=  5
        # Pénalités
        if ratio < 0.3:                    s -= 10
        if min(o,cc) - l > body * 0.5:    s -=  5

    # ── Bonus ICT v2 ─────────────────────────────────────────
    choch_dir, choch_count = count_choch_sequence(c)
    if choch_count >= 2 and choch_dir == b:
        s += min(15, choch_count * 7)   # CHoCH x2 = +14, x3 = +15

    eqh, eql = detect_eqh_eql(c)
    lp = c[-1]["c"]
    if b == "BEARISH" and eqh and abs(lp-eqh)/eqh < 0.005: s += 10  # EQH touché
    if b == "BULLISH" and eql and abs(lp-eql)/eql < 0.005: s += 10  # EQL touché

    return min(max(s, 0), 110)


def count_choch_sequence(c):
    """Compte les CHoCH consécutifs — CHoCH x2+ = retournement fort."""
    if len(c) < 20: return None, 0
    H, L = find_swings(c, n=3)
    if len(H) < 3 or len(L) < 3: return None, 0
    bear = bull = 0
    for k in range(min(3, len(H)-1)):
        if H[-(k+1)][1] < H[-(k+2)][1]: bear += 1
        else: break
    for k in range(min(3, len(L)-1)):
        if L[-(k+1)][1] > L[-(k+2)][1]: bull += 1
        else: break
    if bear >= 2: return "BEARISH", bear
    if bull >= 2: return "BULLISH", bull
    if bear == 1: return "BEARISH", 1
    if bull == 1: return "BULLISH", 1
    return None, 0


def db_activate_pro(uid, source="PAIEMENT", days=None):
    con = _conn(); cur = con.cursor()
    expires = (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d") if days else None
    with _db_lock:
        cur.execute(
            "UPDATE users SET plan='PRO',pro_expires=?,pro_source=? WHERE user_id=?",
            (expires, source, uid))
        cur.execute(
            "UPDATE payments SET status='CONFIRMED' WHERE user_id=? AND status='PENDING'", (uid,))
        con.commit()
    con.close()


def db_check_expiry():
    try:
        con = _conn(); cur = con.cursor()
        today = datetime.now().strftime("%Y-%m-%d")
        cur.execute(
            "SELECT user_id,username FROM users WHERE plan='PRO' AND pro_expires IS NOT NULL AND pro_expires<?",
            (today,))
        expired = cur.fetchall()
        for uid, uname in expired:
            with _db_lock:
                cur.execute("UPDATE users SET plan='FREE',pro_expires=NULL WHERE user_id=?", (uid,))
                con.commit()
        con.close()
        return expired
    except Exception as e:
        print("  [db_check_expiry] {}".format(e))
        return []


def db_close_signal_track(track_id, status):
    try:
        con = _conn(); cur = con.cursor()
        with _db_lock:
            cur.execute("UPDATE signal_tracking SET status=? WHERE track_id=?", (status, track_id))
            con.commit()
        con.close()
    except: pass


def db_close_signal_tracking(track_id, status):
    con = _conn(); cur = con.cursor()
    with _db_lock:
        cur.execute("UPDATE signal_tracking SET status=?,closed_at=? WHERE track_id=?",
                    (status, datetime.now().isoformat(), track_id))
        con.commit()
    con.close()



def db_count_increment(uid):
    ds = datetime.now().strftime("%Y-%m-%d")
    con = _conn(); cur = con.cursor()
    try:
        cur.execute("SELECT count FROM signal_counts WHERE user_id=? AND date_str=?", (uid, ds))
        with _db_lock:
            if cur.fetchone():
                cur.execute("UPDATE signal_counts SET count=count+1 WHERE user_id=? AND date_str=?", (uid, ds))
            else:
                cur.execute("INSERT INTO signal_counts (user_id,date_str,count) VALUES (?,?,1)", (uid, ds))
            con.commit()
    except Exception as e:
        print("  [DB count] {}".format(e))
    con.close()


def db_count_reset(uid):
    ds = datetime.now().strftime("%Y-%m-%d")
    con = _conn(); cur = con.cursor()
    with _db_lock:
        cur.execute("DELETE FROM signal_counts WHERE user_id=? AND date_str=?", (uid, ds))
        con.commit()
    con.close()


def db_count_today(uid):
    ds = datetime.now().strftime("%Y-%m-%d")
    con = _conn(); cur = con.cursor()
    try:
        cur.execute("SELECT count FROM signal_counts WHERE user_id=? AND date_str=?", (uid, ds))
        row = cur.fetchone(); con.close()
        return row[0] if row else 0
    except:
        con.close(); return 0


def db_daily_stats(date_str=None):
    if not date_str:
        date_str = datetime.now().strftime("%Y-%m-%d")
    rows = db_all(
        "SELECT pair,side,rr,g001,g1,l001,l1,session,entry,tp,sl FROM signals "
        "WHERE sent_at LIKE ? ORDER BY sent_at",
        (date_str + "%",))
    rows = rows or []
    wins   = sum(1 for r in rows if r[2] >= 3.0)
    losses = len(rows) - wins
    g001_val = round(sum(r[3] for r in rows), 2)
    g1_val   = round(sum(r[4] for r in rows), 2)
    return {
        "date": date_str, "n": len(rows), "sig_count": len(rows), "wins": wins, "losses": losses,
        "total_g001": g001_val, "total_g1": g1_val,
        "g001": g001_val, "g1": g1_val,  # aliases for backward compat
        "rows": rows
    }


def db_downgrade_pro(uid):
    con = _conn(); cur = con.cursor()
    with _db_lock:
        cur.execute(
            "UPDATE users SET plan='FREE',pro_expires=NULL,pro_source=NULL WHERE user_id=?", (uid,))
        con.commit()
    con.close()


def db_find_by_username(uname):
    uname = uname.lstrip("@").lower()
    con = _conn(); cur = con.cursor()
    cur.execute("SELECT user_id,username FROM users")
    rows = cur.fetchall(); con.close()
    for uid, un in rows:
        if un and un.lower() == uname:
            return uid
    return None


def db_get_free_users():
    con = _conn(); cur = con.cursor()
    cur.execute("SELECT user_id FROM users WHERE plan='FREE'")
    r = cur.fetchall(); con.close()
    return [x[0] for x in r]


def db_get_inactive_users(days=INACTIF_DAYS):
    """Retourne les users FREE sans activité depuis X jours."""
    try:
        con = _conn(); cur = con.cursor()
        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        # Users FREE dont le dernier comptage date est vieux ou inexistant
        cur.execute("""
            SELECT u.user_id, u.username FROM users u
            WHERE u.plan='FREE'
            AND u.user_id != ?
            AND (
                NOT EXISTS (
                    SELECT 1 FROM signal_counts sc
                    WHERE sc.user_id = u.user_id
                    AND sc.date_str >= ?
                )
            )
        """, (ADMIN_ID, cutoff))
        rows = cur.fetchall(); con.close()
        return rows
    except Exception as e:
        print("  [db_get_inactive] {}".format(e))
        return []


def db_get_open_signals():
    """Retourne les signaux ouverts à surveiller."""
    try:
        con = _conn(); cur = con.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='signal_tracking'")
        if not cur.fetchone():
            con.close(); return []
        cur.execute(
            "SELECT track_id,sig_id,pair,entry,tp,sl,side FROM signal_tracking "
            "WHERE status='OPEN' AND sent_at >= datetime('now','-24 hours')")
        rows = cur.fetchall(); con.close()
        return rows
    except:
        return []


def db_get_pro_info(uid):
    row = db_one("SELECT plan,pro_expires,pro_source FROM users WHERE user_id=?", (uid,))
    return (row[0], row[1], row[2]) if row else ("FREE", None, None)


def db_get_pro_users():
    con = _conn(); cur = con.cursor()
    cur.execute("SELECT user_id FROM users WHERE plan='PRO'")
    r = cur.fetchall(); con.close()
    return [x[0] for x in r]


def db_get_refs(uid):
    try:
        con = _conn(); cur = con.cursor()
        cur.execute("SELECT ref_count FROM users WHERE user_id=?", (uid,))
        row = cur.fetchone(); con.close()
        return row[0] if row else 0
    except Exception:
        return 0


def db_global_stats():
    con = _conn(); cur = con.cursor()
    cur.execute("SELECT COUNT(*) FROM users");                              total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM users WHERE plan='PRO'");            pro   = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM signals");                           sigs  = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM payments WHERE status='CONFIRMED'"); pays  = cur.fetchone()[0]
    cur.execute("SELECT COALESCE(SUM(g1),0) FROM signals WHERE sent_at LIKE ?",
                (datetime.now().strftime("%Y-%m-%d") + "%",))
    g1d = cur.fetchone()[0]
    con.close()
    return total, pro, sigs, pays, round(g1d, 2)


def db_is_pro(uid):
    con = _conn(); cur = con.cursor()
    cur.execute("SELECT plan FROM users WHERE user_id=?", (uid,))
    row = cur.fetchone(); con.close()
    return row is not None and row[0] == "PRO"


def db_mark_report(stats, table="daily_reports"):
    con = _conn(); cur = con.cursor()
    with _db_lock:
        if table == "daily_reports":
            cur.execute(
                "INSERT INTO daily_reports (report_date,sig_count,win_count,total_g001,total_g1,created) "
                "VALUES (?,?,?,?,?,?)",
                (stats["date"], stats["sig_count"], stats["wins"],
                 stats.get("total_g001", 0), stats["total_g1"], datetime.now().isoformat()))
        else:
            cur.execute(
                "INSERT INTO weekly_reports (week_start,sig_count,win_count,total_g1,created) VALUES (?,?,?,?,?)",
                (stats["week_start"], stats["sig_count"], stats["wins"],
                 stats["total_g1"], datetime.now().isoformat()))
        con.commit()
    con.close()


def db_pending_payments():
    con = _conn(); cur = con.cursor()
    try:
        cur.execute(
            "SELECT p.pay_id,p.user_id,u.username,p.tx_hash,p.created "
            "FROM payments p LEFT JOIN users u ON p.user_id=u.user_id "
            "WHERE p.status='PENDING' ORDER BY p.created DESC LIMIT 10")
        r = cur.fetchall(); con.close(); return r
    except:
        con.close(); return []


def db_report_sent(date_str, table="daily_reports", col="report_date"):
    con = _conn(); cur = con.cursor()
    cur.execute("SELECT 1 FROM {} WHERE {}=?".format(table, col), (date_str,))
    row = cur.fetchone(); con.close()
    return row is not None


def db_save_payment(uid, tx_hash):
    con = _conn(); cur = con.cursor()
    with _db_lock:
        cur.execute(
            "INSERT INTO payments (user_id,amount,tx_hash,status,created) VALUES (?,?,?,?,?)",
            (uid, PRO_PROMO, tx_hash, "PENDING", datetime.now().isoformat()))
        con.commit()
    con.close()


def db_save_signal(s, session_name):
    con = _conn(); cur = con.cursor()
    with _db_lock:
        cur.execute(
            "INSERT INTO signals (pair,side,entry,tp,sl,rr,score,session,g001,g1,l001,l1,sent_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (s["name"], s["side"], s["entry"], s["tp"], s["sl"], s["rr"],
             s["score"], session_name, s.get("g001", 0), s.get("g1", 0),
             s.get("l001", 0), s.get("l1", 0), datetime.now().isoformat()))
        con.commit()
    con.close()


def db_save_signal_track(sig_id, pair, entry, tp, sl, side):
    """Enregistre un signal pour suivi TP/SL automatique."""
    try:
        con = _conn(); cur = con.cursor()
        with _db_lock:
            cur.execute("""CREATE TABLE IF NOT EXISTS signal_tracking (
                track_id INTEGER PRIMARY KEY AUTOINCREMENT,
                sig_id INTEGER, pair TEXT, entry REAL, tp REAL, sl REAL,
                side TEXT, status TEXT DEFAULT 'OPEN', sent_at TEXT)""")
            cur.execute(
                "INSERT INTO signal_tracking (sig_id,pair,entry,tp,sl,side,sent_at) VALUES (?,?,?,?,?,?,?)",
                (sig_id, pair, entry, tp, sl, side, datetime.now().isoformat()))
            con.commit()
        con.close()
    except Exception as e:
        print("  [db_save_track] {}".format(e))


def db_save_signal_tracking(sig_id, pair, entry, tp, sl, side):
    """Enregistre un signal pour suivi TP/SL automatique."""
    con = _conn(); cur = con.cursor()
    with _db_lock:
        cur.execute("""CREATE TABLE IF NOT EXISTS signal_tracking (
            track_id INTEGER PRIMARY KEY AUTOINCREMENT,
            sig_id INTEGER, pair TEXT, entry REAL, tp REAL, sl REAL,
            side TEXT, status TEXT DEFAULT 'OPEN',
            created TEXT, closed_at TEXT)""")
        cur.execute(
            "INSERT INTO signal_tracking (sig_id,pair,entry,tp,sl,side,created) VALUES (?,?,?,?,?,?,?)",
            (sig_id, pair, entry, tp, sl, side, datetime.now().isoformat()))
        con.commit()
    con.close()


def db_setup():
    con = _conn(); cur = con.cursor()

    cur.execute("PRAGMA table_info(users)")
    u_cols = {r[1] for r in cur.fetchall()}
    GOOD_COLS = {"user_id","username","plan","ref_by","ref_count",
                 "joined","pro_expires","pro_source"}
    bad_cols  = u_cols - GOOD_COLS
    needs_rebuild = (
        not u_cols or ("id" in u_cols and "user_id" not in u_cols)
        or "telegram_id" in u_cols or (bad_cols - {"rowid"}))

    if needs_rebuild and u_cols:
        rows_info = cur.execute("PRAGMA table_info(users)").fetchall()
        pk_col = next((c for c in ["user_id","telegram_id","id"] if c in u_cols), rows_info[0][1])
        copy_map = [("username","username"),("plan","plan"),("ref_by","ref_by"),
                    ("ref_count","ref_count"),("joined","joined"),
                    ("pro_expires","pro_expires"),("pro_source","pro_source")]
        copy_cols = [(ins,sel) for ins,sel in copy_map if sel in u_cols]
        ins_part  = ",".join(["user_id"] + [p[0] for p in copy_cols])
        sel_part  = ",".join([pk_col]    + [p[1] for p in copy_cols])
        cur.execute("""CREATE TABLE users_new (
            user_id INTEGER PRIMARY KEY, username TEXT DEFAULT "",
            plan TEXT DEFAULT "FREE", ref_by INTEGER DEFAULT 0,
            ref_count INTEGER DEFAULT 0, joined TEXT DEFAULT "",
            pro_expires TEXT DEFAULT NULL, pro_source TEXT DEFAULT NULL)""")
        cur.execute("INSERT OR IGNORE INTO users_new ({}) SELECT {} FROM users".format(ins_part, sel_part))
        cur.execute("DROP TABLE users")
        cur.execute("ALTER TABLE users_new RENAME TO users")
        con.commit()
    elif not u_cols:
        cur.execute("""CREATE TABLE users (
            user_id INTEGER PRIMARY KEY, username TEXT DEFAULT "",
            plan TEXT DEFAULT "FREE", ref_by INTEGER DEFAULT 0,
            ref_count INTEGER DEFAULT 0, joined TEXT DEFAULT "",
            pro_expires TEXT DEFAULT NULL, pro_source TEXT DEFAULT NULL)""")

    for col_def in ['username TEXT DEFAULT ""','plan TEXT DEFAULT "FREE"',
                    "ref_by INTEGER DEFAULT 0","ref_count INTEGER DEFAULT 0",
                    'joined TEXT DEFAULT ""',"pro_expires TEXT DEFAULT NULL",
                    "pro_source TEXT DEFAULT NULL",
                    'trial_used INTEGER DEFAULT 0',
                    'last_seen TEXT DEFAULT NULL',
                    'plan_tier TEXT DEFAULT "FREE"']:
        try: cur.execute("ALTER TABLE users ADD COLUMN " + col_def)
        except: pass

    cur.execute("""CREATE TABLE IF NOT EXISTS payments (
        pay_id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, amount REAL,
        tx_hash TEXT, status TEXT DEFAULT "PENDING", created TEXT)""")

    cur.execute("""CREATE TABLE IF NOT EXISTS signals (
        sig_id INTEGER PRIMARY KEY AUTOINCREMENT, pair TEXT, side TEXT,
        entry REAL, tp REAL, sl REAL, rr REAL, score INTEGER,
        session TEXT DEFAULT "", g001 REAL DEFAULT 0, g1 REAL DEFAULT 0,
        l001 REAL DEFAULT 0, l1 REAL DEFAULT 0, sent_at TEXT)""")
    for col_def in ["g001 REAL DEFAULT 0","g1 REAL DEFAULT 0","l001 REAL DEFAULT 0",
                    "l1 REAL DEFAULT 0",'session TEXT DEFAULT ""',"sent_at TEXT"]:
        try: cur.execute("ALTER TABLE signals ADD COLUMN " + col_def)
        except: pass

    cur.execute("PRAGMA table_info(signal_counts)")
    sc_cols = {r[1] for r in cur.fetchall()}
    if not sc_cols or "user_id" not in sc_cols:
        cur.execute("DROP TABLE IF EXISTS signal_counts")
        cur.execute("""CREATE TABLE signal_counts (
            user_id INTEGER NOT NULL, date_str TEXT NOT NULL,
            count INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (user_id, date_str))""")

    cur.execute("""CREATE TABLE IF NOT EXISTS signal_sent (
        user_id  INTEGER NOT NULL,
        sig_key  TEXT    NOT NULL,
        sent_at  TEXT,
        PRIMARY KEY (user_id, sig_key))""")

    cur.execute("""CREATE TABLE IF NOT EXISTS daily_reports (
        report_id INTEGER PRIMARY KEY AUTOINCREMENT, report_date TEXT,
        sig_count INTEGER DEFAULT 0, win_count INTEGER DEFAULT 0,
        total_g001 REAL DEFAULT 0, total_g1 REAL DEFAULT 0, created TEXT)""")
    cur.execute("""CREATE TABLE IF NOT EXISTS weekly_reports (
        report_id INTEGER PRIMARY KEY AUTOINCREMENT, week_start TEXT,
        sig_count INTEGER DEFAULT 0, win_count INTEGER DEFAULT 0,
        total_g1 REAL DEFAULT 0, created TEXT)""")

    con.commit()
    cur.execute("PRAGMA table_info(users)")
    final = {r[1] for r in cur.fetchall()}
    missing = GOOD_COLS - final
    if missing: print("  [DB] COLONNES MANQUANTES: {}".format(missing))
    else:        print("  [DB] Schema OK")
    con.close()


def db_update_last_seen(uid):
    """Met à jour la date de dernière activité de l'utilisateur."""
    con = _conn(); cur = con.cursor()
    with _db_lock:
        cur.execute("UPDATE users SET last_seen=? WHERE user_id=?",
                    (datetime.now().isoformat(), uid))
        con.commit()
    con.close()


def db_weekly_stats():
    week_start = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    rows = db_all(
        "SELECT pair,side,rr,g001,g1,session FROM signals WHERE sent_at>=? ORDER BY sent_at",
        (week_start + " 00:00",))
    rows = rows or []
    wins = sum(1 for r in rows if r[2] >= 3.0)
    g001_val = round(sum(r[3] for r in rows), 2)
    g1_val   = round(sum(r[4] for r in rows), 2)
    return {
        "week_start": week_start, "n": len(rows), "sig_count": len(rows), "wins": wins,
        "total_g001": g001_val, "total_g1": g1_val,
        "g001": g001_val, "g1": g1_val,  # aliases for backward compat
        "ws": week_start,                 # alias used in weekly report formatting
        "rows": rows
    }


def detect_eqh_eql(c, tolerance=0.0003):
    """Détecte Equal Highs / Equal Lows — zones de liquidité ciblées en TP."""
    highs = [x["h"] for x in c[-40:]]
    lows  = [x["l"] for x in c[-40:]]
    eqh = eql = None
    for i in range(len(highs)-1):
        for j in range(i+1, len(highs)):
            if highs[i] and abs(highs[i]-highs[j])/highs[i] <= tolerance:
                eqh = max(highs[i], highs[j]); break
        if eqh: break
    for i in range(len(lows)-1):
        for j in range(i+1, len(lows)):
            if lows[i] and abs(lows[i]-lows[j])/lows[i] <= tolerance:
                eql = min(lows[i], lows[j]); break
        if eql: break
    return eqh, eql


def detect_fvg(c, bias, lookback=40):
    """
    FVG / Fair Value Gap : déséquilibre entre 3 bougies consécutives.
    Bullish FVG  : high[i-1] < low[i+1]  → gap non comblé → zone d'achat
    Bearish FVG  : low[i-1]  > high[i+1] → gap non comblé → zone de vente
    Retourne (fvg_bottom, fvg_top) si le prix revient dans la zone, sinon None.
    """
    if len(c) < 3: return None
    scan = c[-lookback:] if len(c) > lookback else c
    lp   = c[-1]["c"]
    best = None
    for i in range(1, len(scan) - 1):
        if bias == "BULLISH":
            fvg_lo = scan[i - 1]["h"]
            fvg_hi = scan[i + 1]["l"]
            if fvg_hi > fvg_lo:
                # Prix revient dans le gap (pullback dans le FVG)
                if fvg_lo * 0.998 <= lp <= fvg_hi * 1.002:
                    size = fvg_hi - fvg_lo
                    if best is None or size > (best[1] - best[0]):
                        best = (fvg_lo, fvg_hi)
        else:
            fvg_hi = scan[i - 1]["l"]
            fvg_lo = scan[i + 1]["h"]
            if fvg_hi > fvg_lo:
                if fvg_lo * 0.998 <= lp <= fvg_hi * 1.002:
                    size = fvg_hi - fvg_lo
                    if best is None or size > (best[1] - best[0]):
                        best = (fvg_lo, fvg_hi)
    return best  # (bottom, top) ou None


def find_breakers(c, b, lookback=120):
    last = c[-1]["c"]; res = []; atr = calc_atr(c)
    scan = c[-lookback:] if len(c) > lookback else c
    for i in range(2, len(scan) - 2):
        ci = scan[i]; co = ci["o"]; cc = ci["c"]; fut = scan[i+1:]
        if b == "BULLISH":
            if cc >= co: continue
            if not any(f["c"] > co for f in fut): continue
            if cc - atr * 3 <= last <= co + atr * 3:
                res.append({"top": co, "bottom": cc, "strength": abs(co - cc),
                            "dist": abs(last - (co + cc) / 2)})
        else:
            if cc <= co: continue
            if not any(f["c"] < co for f in fut): continue
            if co - atr * 3 <= last <= cc + atr * 3:
                res.append({"top": cc, "bottom": co, "strength": abs(cc - co),
                            "dist": abs(last - (co + cc) / 2)})
    res.sort(key=lambda x: (-x["strength"], x["dist"]))
    return res


def find_swings(c, n=5):
    H = []; L = []
    for i in range(n, len(c) - n):
        w = c[i-n:i+n+1]
        if c[i]["h"] == max(x["h"] for x in w): H.append((i, c[i]["h"]))
        if c[i]["l"] == min(x["l"] for x in w): L.append((i, c[i]["l"]))
    return H, L


def fmt_signal_teasing(s):
    """
    Message teasing pour le groupe GRATUIT.
    Indique la paire UNIQUEMENT — aucun sens (BUY/SELL), aucun niveau TP/SL/entry.
    Suivi d'un CTA 3 options pour passer PRO/VIP.
    """
    emo  = CAT_EMO.get(s["cat"], "📊")
    ref_admin = "https://t.me/{}?start={}".format(BOT_USER, ADMIN_ID)

    teasing = (
        "📡 <b>SIGNAL détecté — {name}</b>  {emo}\n"
        + "═" * 22 + "\n\n"
        "Un signal sur <b>{name}</b> vient d'être envoyé\n"
        "aux membres <b>PRO / VIP</b> avec :\n"
        "  ✅ Direction (BUY/SELL)\n"
        "  🎯 Prix d'entrée exact\n"
        "  📊 TP · SL · Score ICT\n"
        "  💵 Gains estimés par lot\n\n"
        "⏳ <i>Tu aurais pu prendre ce trade !</i>\n\n"
        "━" * 22 + "\n"
        "👑 <b>REJOINS LA VERSION PRO — 3 FAÇONS :</b>\n\n"
        "1️⃣ <b>Payer l'abonnement</b>\n"
        "   💵 10$ USDT/mois → accès immédiat\n"
        "   👉 /pay ou contacte @leaderOdg\n\n"
        "2️⃣ <b>Parrainer des amis</b>\n"
        "   🔗 Partage ton lien de parrainage\n"
        "   📸 10 personnes → <b>7 jours PRO gratuits</b>\n"
        "   📸 30 personnes → <b>1 mois PRO gratuit</b>\n"
        "   👉 Envoie la preuve à @leaderOdg\n\n"
        "3️⃣ <b>Partager ce groupe</b>\n"
        "   📢 Partage à 10–30 personnes minimum\n"
        "   📸 Envoie les captures à @leaderOdg\n"
        "   🎁 Accès VIP activé manuellement\n\n"
        "━" * 22 + "\n"
        "🔗 Lien du groupe à partager :\n"
        "<code>{free_link}</code>\n\n"
        "🤖 AlphaBot PRO  ·  @leaderOdg_bot"
    ).format(
        name=s["name"], emo=emo,
        ref_admin=ref_admin,
        free_link=FREE_GROUP_LINK,
    )
    return teasing


def fmt_signal_free(s, news, sl):
    emo      = CAT_EMO.get(s["cat"], "\U0001f4ca")
    se       = "\U0001f7e2" if s["side"] == "BUY" else "\U0001f534"
    d        = "\u2b06\ufe0f" if s["side"] == "BUY" else "\u2b07\ufe0f"
    sf       = "ACHAT" if s["side"] == "BUY" else "VENTE"
    bar      = "\u2588" * (s["score"] // 10) + "\u2591" * (10 - s["score"] // 10)
    news_ok  = "\u2705" in news or "clear" in news.lower()
    validity = _signal_validity(s)
    valid_str = ("\u23f3 <b>Entrée valide ~{}min</b>".format(validity)
                 if validity > 0 else "\u274c <b>Entrée expirée \u2014 ne pas trader</b>")
    return (
        "{se} {d} <b>SIGNAL {sf} \u2014 {name}</b>  {emo}\n" +
        "\u2550" * 22 + "\n\n"
        "\U0001f553 {sl}  \u00b7  {time} UTC\n"
        "{valid}\n\n"
        "\U0001f3af <b>NIVEAUX DU TRADE</b>\n"
        "  \U0001f4cd Entrée   : <code>{entry}</code>\n"
        "  \u2705 Cible TP : <code>{tp}</code>  {d}  <b>+${g001}</b> (lot 0.01)\n"
        "  \u274c Stop SL  : <code>{sl_v}</code>  \u2014  -${l001} (lot 0.01)\n"
        "  \U0001f4ca RR ratio : <b>1:{rr}</b>\n\n"
        "\U0001f4ca Score IA : <b>{score}/100</b>  [{bar}]\n"
        "\U0001f9e0 {bias}  \u00b7  {btype}  \u00b7  News : {news_s}\n\n" +
        "\u2501" * 22 + "\n"
        "\U0001f4a0 <b>PASSE EN PRO \u2014 VOIS TOUT !</b>\n"
        "  \U0001f4b0 Lot 0.10 \u2192 <b>+${g01}</b> par TP  |  Lot 1.00 \u2192 <b>+${g1}</b> par TP\n"
        "  \u2705 Max {pro_lim} signaux/j  \u00b7  Analyse compl\u00e8te ICT v2\n"
        "  \u2705 Rapports quotidiens + hebdo  \u00b7  24 paires\n"
        "  \U0001f449 /pay \u2014 {}$ USDT seulement \u00b7  Not financial advice".format(
            PRO_PROMO)
    ).format(
        se=se, d=d, sf=sf, name=s["name"], emo=emo, sl=sl,
        time=s["time"], valid=valid_str,
        entry=s["entry"], tp=s["tp"], sl_v=s["sl"], rr=s["rr"],
        g001=s["g001"], l001=s["l001"], g01=s["g01"], g1=s["g1"],
        score=s["score"], bar=bar, bias=s["bias"], btype=s["btype"],
        pro_lim=PRO_LIMIT,
        news_s="\u2705 OK" if news_ok else "\u26a0\ufe0f Actif")


def fmt_signal_pro(s, news, sl):
    emo    = CAT_EMO.get(s["cat"], "\U0001f4ca")
    cname  = CAT_NAME.get(s["cat"], s["cat"])
    se     = "\U0001f7e2" if s["side"] == "BUY" else "\U0001f534"
    d      = "\u2b06\ufe0f" if s["side"] == "BUY" else "\u2b07\ufe0f"
    sf     = "ACHAT" if s["side"] == "BUY" else "VENTE"
    btype_fr = ("Continuation (BOS)" if s["btype"] == "BOS" else
                "Renversement (CHoCH)" if s["btype"] == "CHoCH" else "Tendance")
    news_ok  = "\u2705" in news or "clear" in news.lower()
    sp_ok    = s["sp"] < 3
    bar      = "\u2588" * (s["score"] // 10) + "\u2591" * (10 - s["score"] // 10)
    validity = _signal_validity(s)
    valid_str = ("\u23f3 <b>Entrée valide ~{}min</b>".format(validity)
                 if validity > 0 else "\u274c <b>Entrée expirée — ne pas trader</b>")
    return (
        "{se} {d} <b>SIGNAL {sf} \u2014 {name}</b>\n" +
        "\u2550" * 22 + "\n\n"
        "{emo} <b>{name}</b>  \u00b7  {cname}  \u00b7  {sl}  \u00b7  {time} UTC\n"
        "{valid}\n\n"
        "\U0001f3af <b>NIVEAUX</b>\n"
        "  Entrée   : <code>{entry}</code>\n"
        "  Cible TP : <code>{tp}</code>  {d}\n"
        "  Stop SL  : <code>{sl_v}</code>  \u274c\n"
        "  RR ratio : <b>1:{rr}</b>\n\n"
        "\U0001f4b5 <b>GAINS ESTIMÉS</b>\n"
        "  Lot 0.01 \u2192 <b>+${g001}</b>  /  -${l001}\n"
        "  Lot 0.10 \u2192 <b>+${g01}</b>   /  -${l01}\n"
        "  Lot 1.00 \u2192 <b>+${g1}</b>   /  -${l1}  \U0001f4b0\n\n"
        "\U0001f9e0 <b>ANALYSE ICT v2</b>\n"
        "  Tendance : <b>{bias}</b>  \u2014  {btype_fr}\n"
        "  Breaker  : <code>{bb_bot}</code> \u2014 <code>{bb_top}</code>\n"
        "  Score    : <b>{score}/100</b>  [{bar}]  (min {score_min})\n"
        "  ATR M5   : <code>{atr}</code>\n"
        "  {badges_s}\n\n"
        "\U0001f4cb Filtres : {news_s}  \u00b7  {sp_s}\n\n" +
        "\u2550" * 22 + "\n"
        "\u26a0\ufe0f Risk 1% max  \u00b7  Not financial advice\n"
        "\U0001f916 AlphaBot PRO  \u00b7  @leaderodg_bot"
    ).format(
        se=se, d=d, sf=sf, name=s["name"], emo=emo, cname=cname, sl=sl,
        time=s["time"], valid=valid_str,
        entry=s["entry"], tp=s["tp"], sl_v=s["sl"], rr=s["rr"],
        g001=s["g001"], l001=s["l001"], g01=s["g01"], l01=s["l01"],
        g1=s["g1"], l1=s["l1"], bias=s["bias"], btype_fr=btype_fr,
        bb_bot=s["bb_bot"], bb_top=s["bb_top"],
        score=s["score"], score_min=s.get("score_min", "?"), bar=bar, atr=s["atr"],
        news_s="\u2705 Pas de news" if news_ok else "\u26a0\ufe0f News actif",
        sp_s="\u2705 Spread OK" if sp_ok else "\u26a0\ufe0f Spread large",
        badges_s=s.get("badges", "") or "\u2014")


def get_ote_zone(swing_high, swing_low, bias):
    """Zone OTE 61.8%–78.6% de Fibonacci."""
    rng = swing_high - swing_low
    if rng <= 0: return None, None
    if bias == "BULLISH": return swing_high - rng*0.786, swing_high - rng*0.618
    return swing_low + rng*0.618, swing_low + rng*0.786


def handle_activate(uid, target):
    if not _admin_only(uid): return
    if not target:
        tg_send(uid,
            "🛠 <b>COMMANDES ADMIN</b>\n\n"
            "/activate ID        → Toggle PRO ↔ FREE\n"
            "/activate @user     → Par username\n"
            "/activatepro @user  → Force PRO sur un membre\n"
            "/activateall        → 🔥 PRO pour TOUS les FREE\n"
            "/degrade ID         → Forcer FREE\n"
            "/testfree           → Simuler vue FREE\n"
            "/testpro            → Retour vue PRO\n"
            "/scan               → Forcer scan immédiat\n"
            "/debug              → Raisons dernier scan\n"
            "/resetcount [ID]    → Reset compteur signaux\n"
            "/monstatus          → Statut admin complet\n"
            "/stats              → Stats + paiements\n"
            "/membres [n]        → Liste membres paginée\n\n"
            "<b>Toggle rapide ↓</b>",
            kb={"inline_keyboard": [
                [{"text": "📋 Liste membres", "callback_data": "adm_membres_1"},
                 {"text": "📊 Stats",          "callback_data": "adm_stats"}],
                [{"text": "💰 Paiements",       "callback_data": "adm_payments"},
                 {"text": "🔥 Activer TOUS",    "callback_data": "adm_activateall"}],
            ]})
        return
    try:
        t_uid = int(target) if target.lstrip("@").isdigit() else db_find_by_username(target)
        if not t_uid:
            tg_send(uid, "❌ Utilisateur introuvable : {}".format(target)); return
        plan, exp, src = db_get_pro_info(t_uid)
        con = _conn(); cur = con.cursor()
        cur.execute("SELECT username FROM users WHERE user_id=?", (t_uid,))
        row = cur.fetchone(); con.close()
        uc = "@" + (row[0] if row and row[0] else str(t_uid))
        if plan == "PRO":
            # ── DÉSACTIVER PRO ───────────────────────────────────
            db_downgrade_pro(t_uid)
            tg_send(t_uid,
                "🔒 <b>Accès PRO désactivé</b>\n\n"
                "Ton plan est maintenant : <b>FREE</b>\n"
                "Limite : {} signaux/jour\n\n"
                "Pour revenir PRO : /pay".format(FREE_LIMIT))
            tg_send(uid,
                "✅ PRO → FREE\n"
                "{} <code>{}</code>\n\n"
                "Plan actuel : <b>FREE</b>".format(uc, t_uid),
                kb={"inline_keyboard": [[
                    {"text": "🔄 Réactiver PRO", "callback_data": "adm_pro_{}".format(t_uid)},
                ]]})
            log("INFO", clr("Admin: {} {} → FREE".format(uc, t_uid), "y"))
        else:
            # ── ACTIVER PRO ─────────────────────────────────────
            db_activate_pro(t_uid, "ADMIN", days=None)
            tg_send(t_uid,
                "🎉 <b>PRO activé !</b>\n\n"
                "✅ Max {} signaux/jour\n"
                "✅ Tous les marchés + crypto week-end\n"
                "✅ Rapports quotidiens + hebdo\n"
                "⚡  inclus\n"
                "🤖 Agent IA Binance inclus\n\n"
                "🚀 Bienvenue dans AlphaBot PRO !".format(PRO_LIMIT))
            # Inviter au groupe VIP immédiatement
            time.sleep(1)
            inv_msg, inv_kb = _group_invite_msg(pro=True)
            tg_send(t_uid, inv_msg, kb=inv_kb)
            tg_send(uid,
                "✅ FREE → PRO\n"
                "{} <code>{}</code>  À VIE\n\n"
                "Plan actuel : <b>PRO ✅</b>".format(uc, t_uid),
                kb={"inline_keyboard": [[
                    {"text": "🔒 Désactiver PRO", "callback_data": "adm_ban_{}".format(t_uid)},
                ]]})
            log("INFO", clr("Admin: {} {} → PRO".format(uc, t_uid), "g"))
    except Exception as ex:
        tg_send(uid, "❌ Erreur : {}".format(ex))


def _handle_activateall(uid):
    """Active PRO À VIE pour TOUS les membres FREE. Admin seulement."""
    if uid != ADMIN_ID: return
    free = free_users()
    if not free:
        tg_send(uid, "ℹ️ Aucun membre FREE à activer."); return
    tg_send(uid, "⏳ <b>Activation en cours...</b>\n{} membres FREE à passer PRO.".format(len(free)))
    ok = fail = 0
    for fuid in free:
        try:
            db_activate_pro(fuid, "ADMIN_BULK", days=None)
            tg_send(fuid,
                "🎉 <b>PRO activé !</b>\n\n"
                "✅ Max {} signaux/jour\n"
                "✅ Tous les marchés + analyses complètes\n"
                "✅ Rapports quotidiens + hebdo\n"
                "🚀 Bienvenue dans AlphaBot PRO !".format(PRO_LIMIT))
            time.sleep(0.5)
            inv_msg, inv_kb = _group_invite_msg(pro=True)
            tg_send(fuid, inv_msg, kb=inv_kb)
            ok += 1; time.sleep(0.07)
        except: fail += 1
    tg_sticker(uid, STK_CROWN)
    tg_send(uid,
        "✅ <b>Activation groupée terminée !</b>\n\n"
        "💎 PRO activés : <b>{}</b>\n"
        "❌ Échecs : <b>{}</b>\n\n"
        "/membres pour voir la liste".format(ok, fail))
    log("INFO", clr("ActivateAll: {} → PRO, {} échecs".format(ok, fail), "g"))
    nb = len(db_get_pro_users()) + len(db_get_free_users()) if target == "ALL" else len(db_get_pro_users())
    # Enregistrer l'état en attente de message
    _broadcast_pending[uid] = {"target": target, "step": "waiting"}
    _bcast_pending[uid] = {"target": target, "step": "waiting"}
    tg_send(uid,
        "✉️ <b>BROADCAST → {}</b>\n\n"
        "📝 <b>Tape maintenant ton message</b> et envoie-le.\n\n"
        "👥 Sera envoyé à <b>{} membres</b>\n\n"
        "💡 HTML supporté :\n"
        "  <code>&lt;b&gt;gras&lt;/b&gt;</code>\n"
        "  <code>&lt;i&gt;italique&lt;/i&gt;</code>\n"
        "  <code>&lt;code&gt;code&lt;/code&gt;</code>\n\n"
        "/annuler pour annuler.".format(target, nb),
        kb={"inline_keyboard": [[
            {"text": "❌ Annuler", "callback_data": "adm_panel"}
        ]]})


def handle_broadcast_message(uid, text):
    """Retourne True si le message a été traité comme un broadcast."""
    if uid not in _broadcast_pending: return False
    state = _broadcast_pending.pop(uid)
    target = state["target"]
    users = list(set(db_get_pro_users() + db_get_free_users())) if target == "ALL" else db_get_pro_users()
    tg_send(uid, "📤 Envoi en cours à <b>{}</b> membres...".format(len(users)))
    sent = fail = 0
    for u in users:
        if u == uid: continue
        r = tg_send(u,
            "📢 <b>Message de l'équipe AlphaBot :</b>\n\n" + text +
            "\n\n— <i>@leaderOdg · AlphaBot PRO</i>")
        if r.get("ok"): sent += 1
        else:           fail += 1
        time.sleep(0.05)
    tg_send_sticker(uid, STK_ROCKET)
    tg_send(uid,
        "✅ <b>Broadcast terminé !</b>\n\n"
        "✉️ Envoyés : <b>{}</b>  ·  ❌ Échoués : <b>{}</b>".format(sent, fail),
        kb=kb_admin_back())
    return True



def handle_debug(uid):
    if not _admin_only(uid): return
    try:
        results = _last_scan_results
        if not results:
            tg_send(uid, "\U0001f50d Aucun scan encore. Lance /scan d'abord."); return
        lines     = ["\U0001f50d <b>DEBUG — Dernier scan</b>\n"]
        found     = [r for r in results if r.get("found")]
        not_found = [r for r in results if not r.get("found")]
        if found:
            lines.append("\u2705 <b>SIGNAUX ({}):</b>".format(len(found)))
            for r in found:
                s = r["signal"]
                lines.append("  \U0001f7e2 {} {} RR 1:{} Score {}".format(
                    r["name"], s["side"], s["rr"], s["score"]))
            lines.append("")
        lines.append("\u26aa <b>REJETÉS ({}):</b>".format(len(not_found)))
        reasons = {}
        for r in not_found:
            reason = r.get("reason", "?")
            if "insuffisant" in reason or "vieilles" in reason: key = "Données indisponibles/vieilles"
            elif "neutre" in reason.lower():   key = "Marché neutre"
            elif "Breaker" in reason:          key = "Pas de Breaker Block"
            elif "Score" in reason:            key = reason
            elif "Spread" in reason:           key = "Spread trop large"
            elif "Risque" in reason:           key = "Risque invalide"
            elif "RR" in reason:               key = reason
            elif "week" in reason.lower() or "ferme" in reason.lower(): key = "Fermé (week-end)"
            elif "News" in reason:             key = "News HIGH bloquée"
            else:                              key = reason
            reasons.setdefault(key, []).append(r["name"])
        for reason, names in sorted(reasons.items(), key=lambda x: -len(x[1])):
            lines.append("  <b>{}</b> ({}x): {}{}".format(
                reason, len(names), ", ".join(names[:6]),
                "..." if len(names) > 6 else ""))
        msg = "\n".join(lines)
        if len(msg) > 4000: msg = msg[:3900] + "\n...(tronqué)"
        tg_send(uid, msg)
    except Exception as ex:
        tg_send(uid, "\u274c Erreur /debug : {}".format(str(ex)[:100]))


def handle_degrade(uid, target):
    if not _admin_only(uid): return
    if not target:
        tg_send(uid, "Usage : /degrade ID"); return
    try:
        t_uid = int(target) if target.lstrip("@").isdigit() else db_find_by_username(target)
        if not t_uid:
            tg_send(uid, "\u274c Introuvable."); return
        db_downgrade_pro(t_uid)
        tg_send(t_uid, "\U0001f512 PRO désactivé. /pay pour revenir.")
        tg_send(uid, "\u2705 FREE : <code>{}</code>".format(t_uid))
    except Exception as ex:
        tg_send(uid, "\u274c {}".format(ex))


def handle_marches(uid):
    try:
        db_register(uid, "")
        sn, sm, sl, wknd = get_session()
        tg_send(uid,
            "\U0001f4e1 <b>SCAN EN COURS...</b>\n"
            "\U0001f553 {} \u00b7 Score min : <b>{}</b>\n"
            "\u23f3 Analyse de {} marchés...".format(sl, sm, len(MARKETS)))
        active_markets = [m for m in MARKETS if not wknd or m.get("crypto", False)]
        news_ok, news_lbl = news_check()
        result_queue = Queue()
        threads = []
        for m in active_markets:
            t = threading.Thread(
                target=agent_analyze, args=(m, sm, news_ok, result_queue), daemon=True)
            t.start(); threads.append(t)
        for t in threads:
            t.join(timeout=10)
        results = {}
        while not result_queue.empty():
            try: r = result_queue.get_nowait(); results[r["name"]] = r
            except Empty: break
        cats = {}
        for m in MARKETS:
            r = results.get(m["name"], {"name": m["name"], "cat": m["cat"],
                                        "found": False, "reason": "Timeout"})
            cats.setdefault(m["cat"], []).append(r)
        lines = ["\U0001f50d <b>ÉTAT DES MARCHÉS</b> \u2014 {} \u00b7 {}\n".format(
            sl, datetime.now().strftime("%H:%M"))]
        signals_found = []
        for cat in ["METALS", "CRYPTO", "FOREX", "INDICES", "OIL"]:
            mlist = cats.get(cat, [])
            if not mlist: continue
            lines.append("{} <b>{}</b>".format(CAT_EMO.get(cat, "\U0001f4ca"), CAT_NAME.get(cat, cat)))
            for r in mlist:
                if r.get("found"):
                    s = r["signal"]
                    arrow = "\u2b06\ufe0f" if s["side"] == "BUY" else "\u2b07\ufe0f"
                    validity = _signal_validity(s)
                    lines.append("  \U0001f7e2 <b>{}</b> {} {}  RR 1:{}  Score {}  \u23f3{}min".format(
                        r["name"], arrow, s["side"], s["rr"], s["score"], validity))
                    lines.append("    \U0001f4cd <code>{}</code> \u2192 TP <code>{}</code>  SL <code>{}</code>".format(
                        s["entry"], s["tp"], s["sl"]))
                    signals_found.append(r["name"])
                else:
                    reason = r.get("reason", "?")
                    ico = ("\u26aa" if "insuffisant" in reason or "Timeout" in reason or "vieilles" in reason else
                           "\U0001f7e1" if "neutre" in reason.lower() else
                           "\U0001f7e0" if "Score" in reason else
                           "\U0001f535" if "Breaker" in reason else
                           "\U0001f534" if "RR" in reason or "Spread" in reason else "\u23f8\ufe0f")
                    lines.append("  {} <b>{}</b>  <i>{}</i>".format(ico, r["name"], reason))
            lines.append("")
        if signals_found:
            lines.append("\U0001f7e2 <b>{} signal(s) détecté(s) !</b>".format(len(signals_found)))
        else:
            lines.append("\U0001f7e1 Aucun signal ce cycle")
        msg = "\n".join(lines)
        if len(msg) > 4000: msg = msg[:3900] + "\n...(tronqué)"
        tg_send(uid, msg)
    except Exception as ex:
        tg_send(uid, "\u274c Erreur /marches : {}".format(str(ex)[:100]))




def handle_membres(uid, page=1):
    if not _admin_only(uid): return
    try:
        PAGE = 15
        con  = _conn(); cur = con.cursor()

        # ── Détection dynamique des colonnes ────────────────────────
        try:
            db_setup()
        except Exception: pass
        cur.execute("PRAGMA table_info(users)")
        col_names = [r[1] for r in cur.fetchall()]
        pk    = "user_id"    if "user_id"    in col_names else ("telegram_id" if "telegram_id" in col_names else "id")
        un    = "username"   if "username"   in col_names else "'?'"
        pl    = "plan"       if "plan"       in col_names else "'FREE'"
        rc    = "ref_count"  if "ref_count"  in col_names else "0"
        jo    = "joined"     if "joined"     in col_names else "NULL"
        ex    = "pro_expires"if "pro_expires"in col_names else "NULL"

        cur.execute("SELECT COUNT(*) FROM users")
        total = cur.fetchone()[0]
        cur.execute(
            "SELECT {pk},{un},{pl},{rc},{jo},{ex} "
            "FROM users ORDER BY {jo} DESC LIMIT ? OFFSET ?".format(
                pk=pk, un=un, pl=pl, rc=rc, jo=jo, ex=ex),
            (PAGE, (page - 1) * PAGE))
        rows = cur.fetchall(); con.close()

        tp = max(1, (total + PAGE - 1) // PAGE)
        if total == 0:
            tg_send(uid, "👥 <b>MEMBRES</b>\n\nAucun membre enregistré."); return

        msg = "👥 <b>MEMBRES {}/{}</b>  ({} total)\n".format(page, tp, total)
        msg += "═" * 22 + "\n"
        for row in rows:
            row_uid = row[0]; uname = row[1]; plan_ = row[2]
            rc_val  = row[3]; joined_ = row[4]; exp_ = row[5]
            icon = "💎" if str(plan_) == "PRO" else "🔓"
            j    = (joined_ or "")[:10]
            e    = "  exp:{}".format(exp_[:10]) if exp_ else ""
            ref_link = "https://t.me/{}?start={}".format(BOT_USER, row_uid)
            msg += "{} @{}  <code>{}</code>  🤝{}  {}{}\n".format(
                icon, uname or "?", row_uid, rc_val or 0, j, e)
            msg += "   🔗 <code>{}</code>\n".format(ref_link)
        msg += "═" * 22 + "\n"
        if page > 1:  msg += "⬅️ /membres {}  ".format(page - 1)
        if page < tp: msg += "➡️ /membres {}".format(page + 1)
        tg_send(uid, msg)
    except Exception as ex:
        tg_send(uid, "❌ Erreur /membres : {}".format(str(ex)[:200]))


def handle_monstatus(uid):
    if not _admin_only(uid): return
    try:
        plan, exp, src      = db_get_pro_info(uid)
        total, pro, sigs, pays, g1d = db_global_stats()
        sn, sm, sl, wknd    = get_session()
        stats               = db_daily_stats()
        ws                  = db_weekly_stats()
        count_today         = db_count_today(uid)
        pending             = db_pending_payments()
        refs                = db_get_refs(uid)
        plan_icon  = "\U0001f4a0" if plan == "PRO" else "\U0001f513"
        exp_str    = "À VIE" if not exp else "Expire le {}".format(exp)
        wknd_str   = "\n\U0001f30d <b>Week-end : crypto uniquement</b>" if wknd else ""
        free_total = total - pro
        win_pct    = int(stats["wins"] / stats["sig_count"] * 100) if stats["sig_count"] > 0 else 0
        test_banner= "\U0001f9ea <b>Mode test : {}</b>\n".format(_admin_test_mode) if _admin_test_mode else ""
        pend_str   = "\n\u23f3 <b>{} paiement(s) en attente !</b> /stats".format(
            len(pending)) if pending else ""
        tg_send(uid,
            test_banner +
            "\U0001f6e1 <b>MON STATUT ADMIN</b>\n" + "\u2550" * 22 + "\n\n"
            "\U0001f194 ID : <code>{}</code>  \u00b7  @leaderOdg\n"
            "{} <b>Plan : {}</b>  \u2014  {}\n\n".format(uid, plan_icon, plan, exp_str) +
            "\u2501" * 20 + "\n"
            "\U0001f553 Session : <b>{}</b>  \u00b7  Score min : <b>{}</b>{}\n\n".format(sl, sm, wknd_str) +
            "\u2501" * 20 + "\n"
            "\U0001f465 <b>MEMBRES</b>  {} total  \u00b7  <b>{} PRO</b>  \u00b7  {} FREE\n"
            "\U0001f4b0 Payés : {}  \u00b7  En attente : {}{}\n"
            "\U0001f4e1 Signaux total : {}\n\n".format(
                total, pro, free_total, pays, len(pending), pend_str, sigs) +
            "\u2501" * 20 + "\n"
            "\U0001f4c5 <b>AUJOURD'HUI</b>\n"
            "  {} sig  \u00b7  {} gagnants ({}%)\n"
            "  Lot 0.01 : +${}  \u00b7  Lot 1.00 : +${}\n\n"
            "\U0001f4c6 <b>CETTE SEMAINE</b>\n"
            "  {} sig  \u00b7  {} gagnants  \u00b7  Lot1 +${}\n\n".format(
                stats["sig_count"], stats["wins"], win_pct,
                stats["total_g001"], stats["total_g1"],
                ws["sig_count"], ws["wins"], ws["total_g1"]) +
            "\u2501" * 20 + "\n"
            "\U0001f6e0 /activate {}  /testfree  /testpro\n"
            "/stats  /membres  /scan  /debug".format(uid))
    except Exception as ex:
        tg_send(uid, "\u274c Erreur /monstatus : {}".format(str(ex)[:100]))


def handle_payment_proof_received(uid, uname, tx=None, photo_id=None):
    """Étape 2 — TX Hash reçu → afficher dans un cadre + bouton Vérifier."""
    if uid not in _payment_state or _payment_state[uid].get("step") != "waiting_proof":
        return False
    if not tx:
        return False  # on ignore les photos désormais

    _payment_state[uid]["tx"]   = tx
    _payment_state[uid]["step"] = "waiting_confirm"

    tg_send(uid,
        "\U0001f4cb <b>TX HASH REÇU</b>\n\n"
        "\u2500" * 20 + "\n"
        "<code>{}</code>\n".format(tx) +
        "\u2500" * 20 + "\n\n"
        "Vérifie que c'est le bon hash puis clique sur\n"
        "<b>🔍 Vérifier mon paiement</b> pour lancer la vérification.",
        kb={"inline_keyboard": [
            [{"text": "🔍 Vérifier mon paiement", "callback_data": "pay_confirm"}],
            [{"text": "🔄 Changer le hash",        "callback_data": "pay_submitted"}],
            [{"text": "❌ Annuler",                 "callback_data": "pay_cancel"}],
        ]}
    )
    return True


def handle_scan(uid):
    if not _admin_only(uid): return
    tg_send(uid, "\U0001f50d <b>Scan forcé lancé...</b>")
    scan_and_send()


def handle_stats(uid):
    if not _admin_only(uid): return
    try:
        total, pro, sigs, pays, g1d = db_global_stats()
        stats   = db_daily_stats()
        ws      = db_weekly_stats()
        con     = _conn(); cur = con.cursor()
        # FIX: DISTINCT sur user_id pour éviter les doublons
        cur.execute(
            "SELECT user_id, username, ref_count FROM users "
            "GROUP BY user_id ORDER BY ref_count DESC LIMIT 5")
        top = cur.fetchall(); con.close()
        pending = db_pending_payments()
        msg = (
            "\U0001f4ca <b>STATS ALPHABOT PRO v8.5</b>\n" + "\u2550" * 22 + "\n"
            "\U0001f465 Total:{} PRO:{}\n"
            "\U0001f4e1 Signaux:{} Payés:{}\n\n" +
            "\u2501" * 20 + "\n"
            "\U0001f4c5 <b>AUJOURD'HUI</b>\n"
            "{} sig  {} gagnants\nLot0.01:+${}  Lot1:+${}\n\n"
            "\U0001f4c6 <b>CETTE SEMAINE</b>\n"
            "{} sig  {} gagnants  Lot1:+${}\n\n"
        ).format(
            total, pro, sigs, pays,
            stats["sig_count"], stats["wins"], stats["total_g001"], stats["total_g1"],
            ws["sig_count"], ws["wins"], ws["total_g1"])
        if top:
            msg += "\U0001f91d <b>TOP PARRAINS</b>\n"
            seen = set()
            for t_uid, uname, rc in top:
                if t_uid not in seen:
                    seen.add(t_uid)
                    msg += "{}. @{}  {} filleuls\n".format(len(seen), uname or "?", rc)
        if pending:
            msg += "\n\u23f3 <b>ATTENTE PAIEMENT</b>\n"
            for _, p_uid, uname, tx, _ in pending:
                tx_short = (tx or "")[:16] + "..."
                msg += "\u2022 @{} <code>{}</code>  <code>{}</code>\n  /activate {}\n".format(
                    uname or "?", p_uid, tx_short, p_uid)
        tg_send(uid, msg)
    except Exception as ex:
        tg_send(uid, "\u274c Erreur /stats : {}".format(str(ex)[:100]))


def handle_testfree(uid):
    if not _admin_only(uid): return
    global _admin_test_mode
    _admin_test_mode = "FREE"
    tg_send(uid,
        "\U0001f9ea <b>MODE TEST FREE ACTIVÉ</b>\n\n"
        "Tu vois maintenant exactement ce que voit un utilisateur FREE.\n\n"
        "\U0001f513 Limite : <b>{}/j</b>\n"
        "\u26a0\ufe0f Tes vraies données PRO sont préservées.\n\n"
        "Pour tester :\n"
        "\u2022 Clique <b>Mes Signaux</b>\n"
        "\u2022 Clique <b>Mon Compte</b>\n"
        "\u2022 Clique <b>Devenir PRO</b>\n\n"
        "/testpro \u2192 revenir en vue PRO".format(FREE_LIMIT))
    # Montrer directement la vue FREE
    send_account(uid, "leaderOdg", forced_plan="FREE")


def handle_testpro(uid):
    if not _admin_only(uid): return
    global _admin_test_mode
    _admin_test_mode = ""
    tg_send(uid,
        "\U0001f4a0 <b>MODE TEST PRO</b>\n\nVue PRO normale restaurée.\n\n"
        "/testfree \u2192 retester la vue FREE")
    send_account(uid, "leaderOdg", forced_plan="PRO")


def handle_txhash(uid, uname, tx_hash):
    db_save_payment(uid, tx_hash)
    tg_send(uid,
        "\u2705 <b>Hash reçu !</b>\n\n"
        "\U0001f50d Vérification en cours...\n"
        "<code>{}</code>\n\n"
        "\u23f3 Vérification toutes les 60 sec (max 3 min)".format(tx_hash))
    tg_send(ADMIN_ID,
        "\U0001f4b0 <b>PAIEMENT EN ATTENTE</b>\n"
        "@{} <code>{}</code>\n<code>{}</code>\n"
        "/activate {} (si auto échoue)".format(uname or "?", uid, tx_hash, uid))
    delays = [5, 60, 120]
    for i, delay in enumerate(delays):
        time.sleep(delay)
        ok, amount = verify_tx(tx_hash)
        if ok:
            db_activate_pro(uid, "USDT_AUTO", days=None)
            tg_send_sticker(uid, STK_WIN)
            tg_send(uid,
                "\U0001f389 <b>PAIEMENT CONFIRMÉ !</b>\n\n"
                "\u2705 {}$ USDT reçu !\n\n"
                "\U0001f4a0 <b>PRO ACTIVÉ À VIE !</b>\n\n"
                "\u2705 Max {} signaux/j\n\u2705 24 paires + crypto week-end\n"
                "\u2705 Rapport quotidien + hebdo\n\u2705 Support @leaderOdg\n\n"
                "\U0001f680 Bienvenue dans AlphaBot PRO !".format(amount, PRO_LIMIT))
            tg_send(ADMIN_ID,
                "\U0001f7e2 <b>AUTO PRO OK</b>: @{} <code>{}</code>  {}$ \u2705".format(
                    uname or "?", uid, amount))
            log("PAY", clr("AUTO PRO: @{} {} — {}$".format(uname, uid, amount), "green"))
            return
        elif i < len(delays) - 1:
            log("INFO", clr("TX non confirmé (tentative {}/3)".format(i + 1), "yellow"))
    tg_send(uid,
        "\u23f3 <b>Vérification en attente</b>\n\n"
        "La transaction n'est pas encore confirmée.\n"
        "L'admin va activer manuellement dans 30 min.\n\n"
        "/support \u2192 @leaderOdg")
    tg_send(ADMIN_ID,
        "\u26a0\ufe0f <b>ACTIVATION MANUELLE REQUISE</b>\n"
        "@{} <code>{}</code>\nHash: <code>{}</code>\n\n"
        "\U0001f6e0 /activate {}".format(uname or "?", uid, tx_hash, uid))



def is_clean_bos(c, bias):
    """
    BOS Pur / Continuation propre :
    - Bougie de cassure avec corps > 60% du range (forte)
    - Casse un swing high/low précédent clairement
    - Signe d'un momentum directionnel solide
    """
    if len(c) < 6: return False
    H, L = find_swings(c, n=3)
    if len(H) < 2 or len(L) < 2: return False
    # Analyser les 6 dernières bougies pour trouver la cassure propre
    for i in range(-6, -1):
        try:
            ci       = c[i]
            body     = abs(ci["c"] - ci["o"])
            rng      = ci["h"] - ci["l"]
            if rng == 0: continue
            body_pct = body / rng
            if bias == "BULLISH":
                # Grande bougie haussière qui casse un swing high
                if ci["c"] > ci["o"] and body_pct > 0.60:
                    if len(H) >= 2 and ci["c"] > H[-2][1]:
                        return True
            else:
                # Grande bougie baissière qui casse un swing low
                if ci["c"] < ci["o"] and body_pct > 0.60:
                    if len(L) >= 2 and ci["c"] < L[-2][1]:
                        return True
        except: continue
    return False



def is_in_discount_premium(price, swing_high, swing_low, bias):
    """Vérifie si le prix est en zone Discount (BUY) ou Premium (SELL)."""
    rng = swing_high - swing_low
    if rng <= 0: return True
    pct = (price - swing_low) / rng
    return pct <= 0.50 if bias == "BULLISH" else pct >= 0.50


def kb_admin():
    return {"inline_keyboard": [
        [{"text": "👥 Membres",          "callback_data": "adm_membres_1"},
         {"text": "📊 Stats globales",   "callback_data": "adm_stats"}],
        [{"text": "💰 Paiements",         "callback_data": "adm_payments"},
         {"text": "📈 Rapports",          "callback_data": "adm_rapports"}],
        [{"text": "📡 Forcer scan",        "callback_data": "adm_scan"},
         {"text": "🔍 Debug scan",         "callback_data": "adm_debug"}],
        [{"text": "✉️ Message → TOUS",    "callback_data": "adm_bcast_all"},
         {"text": "✉️ Message → PRO",    "callback_data": "adm_bcast_pro"}],
        [{"text": "📢 Messages Promo",    "callback_data": "adm_promo_list"}],
        [{"text": "🔧 Recommandations",   "callback_data": "adm_reco"},
         {"text": "🌐 État marchés",      "callback_data": "adm_marches"}],
    ]}


def kb_pro():
    return {"inline_keyboard": [
        [{"text": "\U0001f4b0 Payer {}$ USDT TRC20 \u2192 PRO IMMEDIAT".format(PRO_PROMO),
          "callback_data": "pay"}],
        [{"text": "\U0001f91d {} filleuls \u2192 {} mois PRO gratuit".format(REF_TARGET, REF_MONTHS),
          "callback_data": "ref"}],
        [{"text": "\u25c0\ufe0f Menu", "callback_data": "main"}],
    ]}



def make_webhook_handler(scan_state):
    """Crée le handler HTTP pour recevoir les updates Telegram via webhook."""
    class WebhookHandler(BaseHTTPRequestHandler):
        def do_POST(self):
            try:
                length = int(self.headers.get("Content-Length", 0))
                body   = self.rfile.read(length)
                upd    = json.loads(body.decode("utf-8"))
                # Répondre immédiatement 200 OK à Telegram
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"OK")
                # Traiter l'update dans un thread
                threading.Thread(target=process_update, args=(upd,), daemon=True).start()
            except Exception as ex:
                log("ERR", "WebhookHandler: {}".format(ex))
                try:
                    self.send_response(200)
                    self.end_headers()
                except: pass

        def do_GET(self):
            # Health check pour Render
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"AlphaBot OK")

        def log_message(self, *a): pass  # silence les logs HTTP

    return WebhookHandler



def print_banner():
    c = C["cyan"]; b = C["bold"]; r = C["reset"]
    d = C["dim"];  g = C["green"]; y = C["yellow"]
    print()
    print(b+c+"  ╔══════════════════════════════════════════════════╗"+r)
    print(b+c+"  ║   "+b+"██████ "+y+"ALPHABOT"+c+"  "+g+"v14"+c+"  ICT · SMC · M5+H1   ║"+r)
    print(b+c+"  ║  "+d+" 20 marchés  |  20 agents IA  |  Données LIVE  "+c+"║"+r)
    print(b+c+"  ║  "+d+" FREE 2/j  |  PRO max 10/j  |  Weekend BTC   "+c+" ║"+r)
    print(b+c+"  ╚══════════════════════════════════════════════════╝"+r)
    print()



def score_min_for_market(m, base, atr_ratio):
    """Score minimum adaptatif selon la qualité de la session et la volatilité."""
    vol_adj = (m.get("vol", 3) - 3) * 2
    atr_adj = min(4, int(atr_ratio * 5))
    return base + vol_adj + atr_adj

def get_adaptive_score_min():
    """
    Score minimum intelligent :
    - Kill Zone Londres/NY     → score min BAISSÉ  (meilleure session)
    - Session hors marché/nuit → score min MONTÉ   (moins de setups)
    - Week-end                 → score min MONTÉ   (crypto only, volatilité)
    - Régime VOLATILE/CRISIS   → score min MONTÉ   (risque élevé)
    - Régime TRENDING          → score min BAISSÉ  (tendance claire)
    """
    sn, sm, sl, wknd = get_session()
    reg  = AI_REG.get("regime", "RANGING")
    base = sm  # score de base de la session

    # Ajustement selon la qualité de la session
    session_adj = {
        "LONDON_KZ": -5,   # Kill Zone = meilleure probabilité
        "OVERLAP":   -3,   # London+NY = très liquide
        "NY_KZ":     -5,   # Kill Zone NY
        "NY":        -2,   # NY normal
        "LONDON":    -1,   # Londres normal
        "ASIAN":     +5,   # Asie = moins fiable
        "OFF":       +10,  # Hors session = éviter
        "WEEKEND":   +8,   # Week-end = volatile
    }.get(sn, 0)

    # Ajustement selon le régime de marché
    regime_adj = {
        "TRENDING_BULL": -3,  # Tendance claire → plus facile
        "TRENDING_BEAR": -3,
        "ACCUMULATION":  -1,
        "RANGING":       +2,  # Range → plus de faux signaux
        "VOLATILE":      +8,  # Volatile → exiger plus de confirmations
        "CRISIS":        +20, # Crise → quasi stop
    }.get(reg, 0)

    final = base + session_adj + regime_adj
    log("INFO", clr("Score min adaptatif: {} (base:{} sess:{:+d} regime:{:+d})".format(
        final, base, session_adj, regime_adj), "d"))
    return max(85, min(95, final))



def send_admin_panel(uid):
    if uid != ADMIN_ID: tg_send(uid, "❌ Accès refusé."); return
    total, pro, sigs, pays, g1d = db_global_stats()
    sn, sm, sl, wknd = get_session()
    stats  = db_daily_stats()
    pend   = db_pending_payments()
    free   = total - pro
    tg_send_sticker(uid, STK_CROWN)
    tg_send(uid,
        "🛡 <b>PANEL ADMIN — AlphaBot v10</b>\n" + "═" * 22 + "\n\n"
        "👥 Membres : <b>{}</b>  ·  PRO : <b>{}</b>  ·  FREE : <b>{}</b>\n"
        "📡 Signaux aujourd'hui : <b>{}</b>  ·  Gains : <b>+${}</b>\n"
        "💰 Paiements confirmés : <b>{}</b>\n"
        "⏳ En attente paiement : <b>{}</b>{}\n\n"
        "🕐 Session : <b>{}</b>  ·  Score min : <b>{}</b>\n\n"
        "Sélectionne une action ↓".format(
            total, pro, free,
            stats["sig_count"], stats["total_g1"],
            pays, len(pend),
            "  ⚠️ À valider !" if pend else "",
            sl, sm),
        kb=kb_admin())


def send_admin_payments(uid):
    if uid != ADMIN_ID: return
    pend = db_pending_payments()
    if not pend:
        tg_send(uid, "💰 <b>PAIEMENTS</b>\n\nAucun paiement en attente. ✅", kb=kb_admin_back())
        return
    msg = "💰 <b>PAIEMENTS EN ATTENTE ({})</b>\n".format(len(pend)) + "═"*22 + "\n\n"
    btns = []
    for pay_id, p_uid, uname, tx, created in pend:
        tx_s = (tx or "")[:20] + "..."
        msg += "• @{}  <code>{}</code>\n  Hash : <code>{}</code>\n\n".format(
            uname or "?", p_uid, tx_s)
        btns.append([
            {"text": "✅ Activer @{}".format(uname or p_uid), "callback_data": "adm_pro_{}".format(p_uid)},
            {"text": "❌ Refuser",                             "callback_data": "adm_ban_{}".format(p_uid)},
        ])
    btns.append([{"text": "◀️ Panel Admin", "callback_data": "adm_panel"}])
    tg_send(uid, msg, kb={"inline_keyboard": btns})


def send_admin_promo_list(uid):
    """Panel de sélection des messages promo."""
    if uid != ADMIN_ID: return
    stats = db_daily_stats()
    btns  = [[{"text": p["label"], "callback_data": "adm_promo_{}".format(p["id"])}]
             for p in PROMO_MESSAGES]
    btns.append([{"text": "◀️ Panel Admin", "callback_data": "adm_panel"}])
    tg_send(uid,
        "📢 <b>MESSAGES PROMO</b>\n" + "═"*22 + "\n\n"
        "Sélectionne un message à envoyer à <b>TOUS</b> les membres.\n\n"
        "📊 Résultats d'aujourd'hui : "
        "<b>{} signaux · {} TP · +${} lot1</b>".format(
            stats["sig_count"], stats["wins"], stats["total_g1"]),
        kb={"inline_keyboard": btns})


def send_admin_stats(uid):
    if uid != ADMIN_ID: return
    total, pro, sigs, pays, g1d = db_global_stats()
    stats = db_daily_stats(); ws = db_weekly_stats()
    con = _conn(); cur = con.cursor()
    cur.execute("SELECT user_id,username,ref_count FROM users GROUP BY user_id ORDER BY ref_count DESC LIMIT 5")
    top = cur.fetchall()
    cur.execute("SELECT COUNT(*) FROM users WHERE joined >= date('now','-1 day')")
    new1 = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM users WHERE joined >= date('now','-7 days')")
    new7 = cur.fetchone()[0]
    con.close()
    wr_d = int(stats["wins"]/stats["sig_count"]*100) if stats["sig_count"] else 0
    wr_w = int(ws["wins"]/ws["sig_count"]*100) if ws["sig_count"] else 0
    msg = (
        "📊 <b>STATS COMPLÈTES</b>\n" + "═"*22 + "\n\n"
        "👥 Total : <b>{}</b>  ·  PRO : <b>{}</b>  ·  FREE : <b>{}</b>\n"
        "🆕 Nouveaux 24h : <b>{}</b>  ·  7j : <b>{}</b>\n"
        "📡 Signaux total : <b>{}</b>  ·  Payés : <b>{}</b>\n\n"
        "📅 <b>AUJOURD'HUI</b>\n"
        "  {} signaux  ·  {} gagnants  ·  {}% winrate\n"
        "  Lot 0.01 : +${}  ·  Lot 1.00 : +${}\n\n"
        "📆 <b>CETTE SEMAINE</b>\n"
        "  {} signaux  ·  {} gagnants  ·  {}% winrate\n"
        "  Lot 1.00 : +${}\n\n"
    ).format(total, pro, total-pro, new1, new7, sigs, pays,
             stats["sig_count"], stats["wins"], wr_d,
             stats["total_g001"], stats["total_g1"],
             ws["sig_count"], ws["wins"], wr_w, ws["total_g1"])
    if top:
        msg += "🤝 <b>TOP PARRAINS</b>\n"
        seen = set()
        for t_uid, uname, rc in top:
            if t_uid not in seen:
                seen.add(t_uid)
                msg += "  @{}  <b>{}</b> filleuls\n".format(uname or "?", rc)
    tg_send(uid, msg, kb=kb_admin_back())


def send_pro(uid):
    is_pro = db_is_pro(uid)
    if is_pro:
        tg_send_sticker(uid, STK_CROWN)
        plan, exp, src = db_get_pro_info(uid)
        exp_txt = "À VIE" if not exp else "expire le {}".format(exp)
        tg_send(uid,
            "\U0001f4a0 <b>Plan {} actif !</b> \u2705\n\n"
            "Accès : <b>{}</b>\nSignaux : max {}/j\n\nMerci \U0001f64f".format(
                plan, exp_txt, PRO_LIMIT),
            kb=kb_back())
        return
    refs = db_get_refs(uid)
    tg_send_sticker(uid, STK_PRO)
    tg_send(uid,
        "\U0001f4a0 <b>PASSE AU NIVEAU SUPÉRIEUR</b>\n" + "\u2550" * 22 + "\n\n"
        "\U0001f513 <b>FREE</b>  \u2014  2 signaux/jour  \u2014  Gratuit\n\n"
        "\U0001f680 <b>STARTER</b>  \u2014  5 signaux/jour\n"
        "  \u2022 Analyse ICT/SMC complète\n"
        "  \u2022 Entrée + TP + SL + RR\n"
        "  \u2022 <b>5$ USDT/mois</b>\n\n"
        "\U0001f4a0 <b>PRO</b>  \u2014  10 signaux/jour\n"
        "  \u2022 Tout STARTER +\n"
        "  \u2022 Rapports quotidiens + hebdo\n"
        "  \u2022 Suivi TP/SL automatique\n"
        "  \u2022 <b>10$ USDT/mois</b>\n\n"
        "\U0001f451 <b>VIP</b>  \u2014  Signaux illimités\n"
        "  \u2022 Tout PRO +\n"
        "  \u2022 Accès prioritaire aux meilleurs setups\n"
        "  \u2022 Support direct @leaderOdg\n"
        "  \u2022 <b>25$ USDT/mois</b>\n\n" +
        "\u2501" * 22 + "\n"
        "\U0001f91d <b>Parrainage GRATUIT</b>\n"
        "{} filleuls = {} mois PRO (renouvelable)\n"
        "Tes filleuls : {}/{}\n\n"
        "\U0001f449 /pay pour payer et choisir ton plan".format(
            REF_TARGET, REF_MONTHS, refs, REF_TARGET),
        kb={"inline_keyboard": [
            [{"text": "🚀 STARTER — 5$/mois",  "callback_data": "pay_plan_STARTER"}],
            [{"text": "💠 PRO — 10$/mois",      "callback_data": "pay_plan_PRO"}],
            [{"text": "👑 VIP — 25$/mois",      "callback_data": "pay_plan_VIP"}],
            [{"text": "🤝 Parrainage gratuit",   "callback_data": "ref"}],
        ]})


def tg_send_document(chat_id, data, filename, caption=""):
    boundary = "ABotBoundary85"
    body = b""
    def field(name, val):
        return (
            "--{}\r\nContent-Disposition: form-data; name=\"{}\"\r\n\r\n".format(boundary, name)
        ).encode() + str(val).encode() + b"\r\n"
    body += field("chat_id", chat_id)
    if caption:
        body += field("caption", caption)
        body += field("parse_mode", "HTML")
    body += (
        "--{}\r\nContent-Disposition: form-data; name=\"document\"; "
        "filename=\"{}\"\r\nContent-Type: application/octet-stream\r\n\r\n".format(boundary, filename)
    ).encode()
    body += data + b"\r\n" + ("--{}--\r\n".format(boundary)).encode()
    try:
        req = urllib.request.Request(
            TG + "sendDocument", data=body, method="POST",
            headers={"Content-Type": "multipart/form-data; boundary=" + boundary})
        opener = urllib.request.build_opener(urllib.request.HTTPSHandler(context=CTX))
        with opener.open(req, timeout=30) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        print("  [DOC] {}".format(e))
        return {}



def tg_send_sticker(chat_id, sticker_id):
    """Envoie un sticker Telegram animé."""
    tg_req("sendSticker", {"chat_id": str(chat_id), "sticker": sticker_id})


def tg_updates(offset):
    return tg_req("getUpdates", {
        "offset":  offset,
        "timeout": 0,
        "limit":   100
    }).get("result", [])


# ══════════════════════════════════════════════════════
#  CLAVIERS COMPLETS
# ══════════════════════════════════════════════════════
def kb_reply():
    """Supprimé — utilise kb_main() à la place."""
    return {"remove_keyboard": True}

def kb_pro_plans():
    return {"inline_keyboard":[
        [{"text":"🚀 STARTER — 5$/mois",  "callback_data":"pay_plan_STARTER"}],
        [{"text":"💠 PRO — 10$/mois",     "callback_data":"pay_plan_PRO"}],
        [{"text":"👑 VIP — 25$/mois",     "callback_data":"pay_plan_VIP"}],
        [{"text":"🤝 Parrainage gratuit", "callback_data":"ref"}],
    ]}

def kb_admin_back(): return {"inline_keyboard":[[{"text":"◀️ Panel Admin","callback_data":"adm_panel"}]]}

# ══════════════════════════════════════════════════════
#  MESSAGES UTILISATEURS COMPLETS
# ══════════════════════════════════════════════════════
def send_welcome(uid, uname, ref_by=0):
    db_register(uid, uname, ref_by)
    tg_sticker(uid, STK_W)
    p = is_pro(uid); sn,sm,sl_l,wknd = get_session()
    plan_line = ("🎁 <b>ESSAI PRO {} JOURS OFFERT !</b> ✅".format(TRIAL_DAYS) if p
                 else "🔓 FREE → /pay")
    wknd_note = "\n🌍 <b>Week-end : crypto uniquement !</b>" if wknd else ""
    tg_send(uid,
        "🤖 <b>AlphaBot PRO v10 — Bienvenue {} !</b>\n".format("@"+uname if uname else "Trader") +
        "═"*22 + "\n\n"
        "🆔 <b>ID :</b> <code>{}</code>\n"
        "📌 <b>Plan :</b> {}\n"
        "🕐 <b>Session :</b> {}  ·  Score min : <b>{}</b>{}\n\n".format(uid,plan_line,sl_l,sm,wknd_note)+
        "═"*22+"\n"
        "🤖 <b>20 agents IA</b> scannent en parallèle :\n"
        "  🥇 Or · Argent  ·  ₿ BTC\n"
        "  💱 Forex : EURUSD · GBPUSD · USDJPY · GBPJPY · EURJPY\n"
        "           AUDUSD · AUDJPY · CADJPY · USDCHF · NZDUSD · USDCAD\n"
        "  📈 Indices : NAS100 · SPX500 · US30  ·  🛢 USOIL\n\n"
        "⚡ <b> actif</b> — signal même sans setup ICT parfait !\n\n"
        "═"*22+"\n"
        "🎁 Essai PRO {} jours GRATUIT !\n"
        "💠 PRO = max {}/j  ·  🤝 {} filleuls = {} mois PRO\n\n"
        "📖 /guide ou choisis ci-dessous ↓".format(TRIAL_DAYS,PRO_LIMIT,REF_TARGET,REF_MONTHS),
        kb=kb_main(False))    # ← clavier physique persistant

def send_start(uid, uname, ref_by=0):
    """Alias for send_welcome."""
    send_welcome(uid, uname)

def send_signals_info(uid):
    p = is_pro(uid); st = db_daily_stats(); rows = st["rows"]
    sn,sm,sl_l,wknd = get_session()
    cnt = count_today(uid); lim = PRO_LIMIT if p else FREE_LIMIT
    today = datetime.now().strftime("%d/%m/%Y")
    lines = ["📡 <b>SIGNAUX DU JOUR</b>","═"*22,
             "📅 {}  ·  {}".format(today,sl_l),
             "{} ·  {}/{} signaux  ·  Reste : <b>{}</b>".format(
                 "💠 PRO" if p else "🔓 FREE",cnt,lim,max(0,lim-cnt)),""]
    if rows:
        lines.append("📋 <b>Signaux envoyés :</b>"); lines.append("")
        for row in rows:
            pair,side,rr,g001,g1,l001,l1,sess,mode = row
            arrow = "⬆️" if side=="BUY" else "⬇️"
            icon = "✅" if rr>=2.5 else "⚪"
            gain = "+${:.0f}".format(g1) if rr>=2.5 else "---"
            tag = " ⚡" if mode!="NORMAL" else ""
            lines.append("{} <b>{}</b>{} {} {}  RR 1:{}  💰 {}".format(icon,pair,tag,arrow,side,rr,gain))
        lines += ["","━"*20,
                  "💵 Total lot 0.01 : <b>+${}</b>".format(st["g001"]),
                  "💰 Total lot 1.00 : <b>+${}</b>".format(st["g1"]),
                  "🎯 {}/{} gagnants".format(st["wins"],st["n"])]
    else:
        lines += ["⏳ Aucun signal encore aujourd\'hui.",
                  "🔄 Prochain scan dans quelques minutes...",
                  "","💠 <b>Passe PRO pour max {}/j</b>\n/pay — {}$ USDT".format(PRO_LIMIT,PRO_PRICE) if not p else ""]
    tg_send(uid, "\n".join(l for l in lines if l is not None), kb=kb_back())

def send_pro_page(uid):
    p = is_pro(uid)
    if p:
        tg_sticker(uid, STK_PRO)
        plan,exp,_ = db_get_pro_info(uid)
        tg_send(uid,"💠 <b>Plan {} actif !</b> ✅\n\nAccès : {}\nSignaux : max {}/j\n\nMerci 🙏".format(
            plan,"À VIE" if not exp else "expire le {}".format(exp),PRO_LIMIT),kb=kb_back())
        return
    refs = db_get_refs(uid)
    tg_sticker(uid, STK_PRO)
    tg_send(uid,
        "💠 <b>PASSE AU NIVEAU SUPÉRIEUR</b>\n"+"═"*22+"\n\n"
        "🔓 <b>FREE</b>  —  2 signaux/jour  —  Gratuit\n\n"
        "🚀 <b>STARTER</b>  —  5 signaux/jour\n"
        "  • Analyse ICT/SMC complète\n"
        "  • Entrée + TP + SL + RR\n"
        "  • ⚡ \n"
        "  • <b>5$ USDT/mois</b>\n\n"
        "💠 <b>PRO</b>  —  10 signaux/jour\n"
        "  • Tout STARTER +\n"
        "  • Rapports quotidiens + hebdo\n"
        "  • Suivi TP/SL automatique\n"
        "  • Agent IA Binance (Challenge 5$→500$)\n"
        "  • <b>10$ USDT/mois</b>\n\n"
        "👑 <b>VIP</b>  —  Signaux illimités\n"
        "  • Tout PRO + Support @leaderOdg\n"
        "  • <b>25$ USDT/mois</b>\n\n"
        "━"*22+"\n"
        "🤝 <b>Parrainage GRATUIT</b>\n"
        "{} filleuls = {} mois PRO (renouvelable)\n"
        "Tes filleuls : {}/{}\n\n"
        "👉 /pay pour payer et choisir ton plan".format(REF_TARGET,REF_MONTHS,refs,REF_TARGET),
        kb=kb_pro_plans())

def send_pay_plan(uid, plan_key="PRO"):
    plans = {"FREE":{"price":0,"label":"FREE"},"STARTER":{"price":5,"label":"STARTER"},
             "PRO":{"price":10,"label":"PRO"},"VIP":{"price":25,"label":"VIP"}}
    plan = plans.get(plan_key, plans["PRO"])
    price = plan["price"]; label = plan["label"]
    sep = "━"*22
    tg_send(uid,
        (
        "💰 <b>PAIEMENT {lbl} — {pr}$ USDT/mois</b>\n{sep}\n\n"
        "⚠️ <b>RÉSEAU : TRC20 UNIQUEMENT</b>\n"
        "Pas BEP20, pas ERC20 — sinon perdu !\n\n"
        "👇 <b>Adresse USDT TRC20 :</b>\n"
        "<code>{addr}</code>\n\n{sep}\n"
        "1️⃣ Ouvre Binance / Trust Wallet\n"
        "2️⃣ Envoie <b>{pr}$ USDT TRC20</b>\n"
        "3️⃣ Clique <b>J\'ai payé ✅</b>\n"
        "4️⃣ Envoie ton <b>TX Hash</b>\n\n"
        "🤖 <b>Activation automatique sous 2 min !</b>"
        ).format(lbl=label, pr=price, addr=USDT_ADDR, sep=sep),
        kb={"inline_keyboard":[
            [{"text":"✅ J\'ai payé — Soumettre TX Hash","callback_data":"pay_submitted_{}".format(plan_key)}],
            [{"text":"◀️ Voir les plans","callback_data":"pro"}],
        ]})

def send_mes_gains(uid):
    st = db_daily_stats()
    if not st["n"]: tg_send(uid,"💸 <b>MES GAINS</b>\n\nAucun signal aujourd\'hui.",kb=kb_back()); return
    lines = ["💸 <b>GAINS DU JOUR</b>","═"*22,""]
    for row in st["rows"]:
        pair,side,rr,g001,g1,l001,l1,sess,mode = row
        ok=rr>=2.5; icon="✅" if ok else "❌"; d="⬆️" if side=="BUY" else "⬇️"
        tag=" ⚡" if mode!="NORMAL" else ""
        lines.append("{} <b>{}</b>{} {} {}  RR 1:{}".format(icon,pair,tag,d,side,rr))
        if ok: lines.append("   0.01 → +${:.2f}   1.00 → +${:.0f}".format(g001,g1))
        else:  lines.append("   ---")
    lines += ["","═"*22,
              "💵 Lot 0.01 : <b>+${}</b>".format(st["g001"]),
              "💰 Lot 1.00 : <b>+${}</b>".format(st["g1"]),
              "({}/{} gagnants)".format(st["wins"],st["n"]),"",
              "<i>Estimation TP atteint. Pas un conseil financier.</i>"]
    tg_send(uid,"\n".join(lines),kb=kb_back())

def send_affilie(uid, uname):
    refs=db_get_refs(uid); link="https://t.me/{}?start={}".format(BOT_USER,uid)
    done=min(refs,REF_TARGET); pct=int(done/REF_TARGET*100)
    fill=int(done/REF_TARGET*12); bar="🟩"*fill+"⬛"*(12-fill)
    tg_send(uid,
        ("📋 <b>COPIE CE MESSAGE ET ENVOIE À TES AMIS :</b>\n\n"+"━"*22+"\n\n"
        "🤖 <b>AlphaBot PRO</b> — Signaux trading GRATUITS !\n\n"
        "📡 <b>Forex, Or, BTC, Indices...</b>\n"
        "🎯 Entrées directes avec SL & TP automatiques\n"
        "💰 Jusqu\'à <b>+$500+ par signal</b> (lot 1.00)\n"
        "📊 Analyse ICT/SMC\n\n"
        "✅ <b>Gratuit</b> — signaux/jour\n"
        "💠 <b>PRO seulement 10$</b> — 10 signaux/jour\n\n"
        "👉 <b>Clique ici :</b>\n<code>{}</code>\n\n"+"━"*22).format(link),
        kb={"inline_keyboard":[[{"text":"🤝 Voir mes filleuls","callback_data":"ref_stats"}]]})
    rew = ("🏆 {} mois PRO actif ! Re-parraine pour renouveler !".format(REF_MONTHS) if refs>=REF_TARGET
           else "🔥 Plus que {} de plus → {} mois PRO !".format(REF_TARGET-refs,REF_MONTHS) if refs>=20
           else "👋 {} filleuls pour l\'instant. Continue !".format(refs))
    tg_send(uid,
        "🤝 <b>MES FILLEULS</b>\n"+"═"*22+"\n\n"
        "<b>{}/{}</b>  ({}%)\n{}\n\n"
        "{}\n\n"
        "🏆 {} filleuls = <b>{} MOIS PRO GRATUIT</b>\n"
        "✅ <b>Activation automatique</b> dès {} atteints".format(
            done,REF_TARGET,pct,bar,rew,REF_TARGET,REF_MONTHS,REF_TARGET),
        kb=kb_back())

# ══════════════════════════════════════════════════════
#  ADMIN COMPLET
# ══════════════════════════════════════════════════════
_bcast_pending = _broadcast_pending  # même dict, deux noms
STK_ROCKET = "CAACAgIAAxkBAAIBjGWbNfNMiEkgPZrxgWMVBH1ycfP7AAIbAQACB8OhCsYm5NOoMByuNgQ"

def kb_admin_full():
    return {"inline_keyboard":[
        [{"text":"🙏 Excuses membres","callback_data":"adm_promo_send_promo_excuse"}],
        [{"text":"👥 Membres","callback_data":"adm_membres_1"},{"text":"📊 Stats","callback_data":"adm_stats"}],
        [{"text":"💰 Paiements","callback_data":"adm_payments"},{"text":"📈 Rapports","callback_data":"adm_rapports"}],
        [{"text":"📡 Forcer scan","callback_data":"adm_scan"},{"text":"🔍 Debug scan","callback_data":"adm_debug"}],
        [{"text":"✉️ Message → TOUS","callback_data":"adm_bcast_all"},{"text":"✉️ Message → PRO","callback_data":"adm_bcast_pro"}],
        [{"text":"📢 Messages Promo","callback_data":"adm_promo_list"},{"text":"🌍 État marchés","callback_data":"adm_marches"}],
        [{"text":"🔧 Recommandations","callback_data":"adm_reco"}],
        [{"text":"🧠 Mémoire IA","callback_data":"adm_memory"}],
    ]}

def send_admin_full(uid):
    if uid!=ADMIN_ID: tg_send(uid,"❌ Accès refusé."); return
    total,pro,sigs,pays,g1d=global_stats(); sn,sm,sl_l,_=get_session(); sm=get_adaptive_score_min()
    st=db_daily_stats(); pend=pending_pays(); ch=chal_get(); reg=AI_REG
    tg_sticker(uid,STK_PRO)
    tg_send(uid,
        "🛡 <b>PANEL ADMIN — AlphaBot v10</b>\n"+"═"*22+"\n\n"
        "👥 Membres: <b>{}</b>  ·  PRO: <b>{}</b>  ·  FREE: <b>{}</b>\n"
        "📡 Signaux: <b>{}</b>  ·  Gains: <b>+${}</b>\n"
        "💰 Payés: <b>{}</b>  ·  En attente: <b>{}</b>{}\n\n"
        "🤖 <b>IA:</b> {:.4f}$ AM:{}/4 W:{} L:{}\n"
        "🌍 Régime: <b>{}</b>  Positions: {}/{}\n\n"
        "🕐 Session: {}  Score min: {}\n\n"
        "/activate /degrade /scan /debug /stats /membres /marches".format(
            total,pro,total-pro,st["n"],st["g1"],pays,len(pend),
            "  ⚠️ À valider!" if pend else "",
            ch["balance"],ch.get("am_cycle",0),ch.get("today_w",0),ch.get("today_l",0),
            reg.get("regime","?"),sum(1 for t in AI_OT.values() if t["status"]=="open"),MAX_OPEN,sl_l,sm),
        kb=kb_admin_full())

def send_admin_stats_full(uid):
    if uid != ADMIN_ID: return
    try:
        # ── Migration préventive : s'assurer que user_id existe ──────
        try:
            db_setup()
        except Exception: pass

        total, pro, sigs, pays, g1d = global_stats()
        st = db_daily_stats(); ws = db_weekly_stats()
        con = _conn(); cur = con.cursor()

        # ── Détection dynamique du nom de la colonne PK ──────────────
        try:
            cur.execute("PRAGMA table_info(users)")
            cols = [r[1] for r in cur.fetchall()]
            pk = "user_id" if "user_id" in cols else ("telegram_id" if "telegram_id" in cols else "id")
            rc_col = "ref_count" if "ref_count" in cols else "0"
            un_col = "username" if "username" in cols else "'?'"
            cur.execute(
                "SELECT {pk},{un},{rc} FROM users GROUP BY {pk} ORDER BY {rc} DESC LIMIT 5".format(
                    pk=pk, un=un_col, rc=rc_col))
            top = cur.fetchall()
        except Exception:
            top = []

        try:
            cur.execute("SELECT COUNT(*) FROM users WHERE joined>=date('now','-1 day')")
            new1 = cur.fetchone()[0]
        except Exception:
            new1 = 0
        try:
            cur.execute("SELECT COUNT(*) FROM users WHERE joined>=date('now','-7 days')")
            new7 = cur.fetchone()[0]
        except Exception:
            new7 = 0
        con.close()

        pend   = pending_pays()
        wr_d   = int(st["wins"] / st["n"] * 100) if st["n"] else 0
        wr_w   = int(ws["wins"] / ws["n"] * 100) if ws["n"] else 0

        msg = (
            "📊 <b>STATS ALPHABOT PRO</b>\n" + "═"*22 + "\n"
            "👥 Total:{} PRO:{} FREE:{}\n"
            "🆕 Nouveaux 24h:{} · 7j:{}\n"
            "📡 Signaux:{} · Payés:{}\n\n"
            "━"*20 + "\n"
            "📅 <b>AUJOURD'HUI</b>\n"
            "  {} sig · {} gagnants · {}% winrate\n"
            "  Lot 0.01:+${}  Lot 1.00:+${}\n\n"
            "📆 <b>CETTE SEMAINE</b>\n"
            "  {} sig · {} gagnants · {}% winrate\n"
            "  Lot 1.00:+${}\n\n"
        ).format(total, pro, total-pro, new1, new7, sigs, pays,
                 st["n"], st["wins"], wr_d, st.get("g001",0), st["g1"],
                 ws["n"], ws["wins"], wr_w, ws["g1"])

        if top:
            msg += "🤝 <b>TOP PARRAINS</b>\n"
            seen = set()
            for row in top:
                t_uid, uname, rc = row[0], row[1], row[2]
                if t_uid not in seen:
                    seen.add(t_uid)
                    ref_link = "https://t.me/{}?start={}".format(BOT_USER, t_uid)
                    msg += "  @{}  <b>{}</b> filleuls  🔗 <code>{}</code>\n".format(
                        uname or "?", rc, ref_link)

        if pend:
            msg += "\n⏳ <b>ATTENTE PAIEMENT</b>\n"
            for _, p_uid, un, tx, _ in pend:
                msg += "• @{} <code>{}</code>  <code>{}</code>\n  /activate {}\n".format(
                    un or "?", p_uid, (tx or "")[:16]+"...", p_uid)

        tg_send(uid, msg, kb=kb_admin_back())

    except Exception as ex:
        tg_send(uid, "❌ Erreur stats : {}".format(str(ex)[:200]), kb=kb_admin_back())

def send_admin_payments_full(uid):
    if uid!=ADMIN_ID: return
    pend=pending_pays()
    if not pend: tg_send(uid,"💰 Aucun paiement en attente. ✅",kb=kb_admin_back()); return
    msg="💰 <b>PAIEMENTS EN ATTENTE ({})</b>\n".format(len(pend))+"═"*22+"\n\n"
    btns=[]
    for pid,p_uid,un,tx,created in pend:
        msg+="• @{}  <code>{}</code>\n  Hash: <code>{}</code>\n\n".format(un or "?",p_uid,(tx or "")[:30]+"...")
        btns.append([{"text":"✅ Activer @{}".format(un or p_uid),"callback_data":"adm_pro_{}".format(p_uid)},{"text":"❌ Refuser","callback_data":"adm_ban_{}".format(p_uid)}])
    btns.append([{"text":"◀️ Panel Admin","callback_data":"adm_panel"}])
    tg_send(uid,msg,kb={"inline_keyboard":btns})

def send_admin_reco(uid):
    if uid!=ADMIN_ID: return
    total,pro,sigs,pays,g1d=global_stats(); st=db_daily_stats()
    wr=int(st["wins"]/st["n"]*100) if st["n"]>=3 else 0
    recs=[]
    if st["n"]==0: recs.append("📭 Aucun signal — Lance /scan puis /debug pour voir les raisons.")
    if wr<50 and st["n"]>=3: recs.append("📉 Winrate {}% faible —  actif pour combler.".format(wr))
    if (total-pro)>pro*4: recs.append("💡 {} FREE vs {} PRO — Lance un broadcast de motivation.".format(total-pro,pro))
    if pays<5: recs.append("💰 Seulement {} paiements — Envoie un message promo.".format(pays))
    if st["g1"]>500: recs.append("🔥 Excellente journée +${} ! Partage les résultats.".format(st["g1"]))
    improv=db_all("SELECT COUNT(*) FROM signals WHERE sent_at LIKE ? AND mode!='NORMAL'",(datetime.now().strftime("%Y-%m-%d")+"%",))
    pass  # improv supprimé
    if not recs: recs.append("✅ Tout fonctionne bien. Continue !")
    msg="🔧 <b>RECOMMANDATIONS ADMIN</b>\n"+"═"*22+"\n\n"
    for i,r in enumerate(recs,1): msg+="{}. {}\n\n".format(i,r)
    tg_send(uid,msg,kb=kb_admin_back())

def send_admin_memory(uid):
    if uid!=ADMIN_ID: return
    best=best_setups(5); worst=worst_setups(5)
    lines=["🧠 <b>MÉMOIRE IA — AlphaBot v10</b>","═"*22,"","🔥 <b>TOP 5 SETUPS GAGNANTS</b>",""]
    for s in best:
        lines.append("✅ <b>{}</b>".format(s["key"].replace("|"," · ")))
        lines.append("   WR:<b>{}%</b>  {} trades  PnL:+${}".format(s["wr"],s["total"],s["pnl"]))
    lines+=["","━"*20,"","💀 <b>TOP 5 SETUPS PERDANTS</b>",""]
    for s in worst:
        lines.append("❌ <b>{}</b>".format(s["key"].replace("|"," · ")))
        lines.append("   WR:<b>{}%</b>  {} trades  PnL:{}$".format(s["wr"],s["total"],s["pnl"]))
    if not best and not worst:
        lines.append("⏳ Pas encore assez de données (min 5 trades par setup).")
    tg_send(uid,"\n".join(l for l in lines if l is not None),kb=kb_admin_back())

def handle_monstatus_full(uid):
    if uid!=ADMIN_ID: return
    plan,exp,src=db_get_pro_info(uid); total,pro,sigs,pays,g1d=global_stats()
    sn,sm,sl_l,wknd=get_session(); st=db_daily_stats(); ws=db_weekly_stats()
    cnt=count_today(uid); pend=pending_pays(); refs=db_get_refs(uid)
    ch=chal_get(); reg=AI_REG
    win_pct=int(st["wins"]/st["n"]*100) if st["n"] else 0
    pend_str="\n⏳ <b>{} paiement(s) en attente !</b>".format(len(pend)) if pend else ""
    tg_send(uid,
        "🛡 <b>MON STATUT ADMIN</b>\n"+"═"*22+"\n\n"
        "🆔 ID: <code>{}</code>  · @leaderOdg\n"
        "💠 Plan: <b>{}</b>  —  {}\n\n"
        "━"*20+"\n"
        "🕐 Session: <b>{}</b>  ·  Score min: <b>{}</b>\n\n"
        "━"*20+"\n"
        "👥 <b>MEMBRES</b>  {} total  ·  <b>{} PRO</b>  ·  {} FREE\n"
        "💰 Payés: {}  ·  En attente: {}{}\n"
        "📡 Signaux total: {}\n\n"
        "━"*20+"\n"
        "📅 <b>AUJOURD\'HUI</b>\n"
        "  {} sig  ·  {} gagnants ({}%)\n"
        "  Lot 0.01: +${}  ·  Lot 1.00: +${}\n\n"
        "📆 <b>CETTE SEMAINE</b>\n"
        "  {} sig  ·  {} gagnants  ·  Lot1 +${}\n\n"
        "🤖 <b>IA:</b> {:.4f}$ AM:{}/4  Régime:{}\n\n"
        "━"*20+"\n"
        "/activate {} /testfree /testpro\n/stats /membres /scan /debug".format(
            uid,plan,"À VIE" if not exp else "expire le {}".format(exp),sl_l,sm,
            total,pro,total-pro,pays,len(pend),pend_str,sigs,
            st["n"],st["wins"],win_pct,st["g001"],st["g1"],
            ws["n"],ws["wins"],ws["g1"],
            ch["balance"],ch.get("am_cycle",0),reg.get("regime","?"),uid))

def handle_marches_full(uid):
    sn,sm,sl_l,wknd=get_session(); sm=get_adaptive_score_min()
    tg_send(uid,"📡 <b>SCAN EN COURS...</b>\n🕐 {}  ·  Score min: <b>{}</b>\n⏳ Analyse {} marchés...".format(sl_l,sm,len(MARKETS)))
    active=[m for m in MARKETS if not wknd or m.get("crypto",False)]
    news_ok,news_lbl=news_check(); q=Queue(); threads=[]
    for m in active:
        t=threading.Thread(target=agent_analyze,args=(m,sm,news_ok,q),daemon=True); t.start(); threads.append(t)
    for t in threads: t.join(timeout=10)
    results={}
    while not q.empty():
        try: r=q.get_nowait(); results[r["name"]]=r
        except Empty: break
    cats={}
    for m in MARKETS:
        r=results.get(m["name"],{"name":m["name"],"cat":m["cat"],"found":False,"reason":"Timeout"})
        cats.setdefault(m["cat"],[]).append(r)
    lines=["🔍 <b>ÉTAT DES MARCHÉS</b> — {}  {}\n".format(sl_l,datetime.now().strftime("%H:%M"))]
    found=[]
    for cat in ["METALS","CRYPTO","FOREX","INDICES","OIL"]:
        mlist=cats.get(cat,[])
        if not mlist: continue
        lines.append("{} <b>{}</b>".format(CAT_EMO.get(cat,"📊"),cat))
        for r in mlist:
            if r.get("found"):
                s=r["signal"]; arrow="⬆️" if s["side"]=="BUY" else "⬇️"
                tag=""
                lines.append("  🟢 <b>{}</b>{} {} {}  RR 1:{}  Score {}".format(r["name"],tag,arrow,s["side"],s["rr"],s["score"]))
                lines.append("    📍<code>{}</code>→TP<code>{}</code> SL<code>{}</code>".format(s["entry"],s["tp"],s["sl"]))
                found.append(r["name"])
            else:
                reason=r.get("reason","?")
                ico=("⚪" if "insuffisant" in reason or "Timeout" in reason else
                     "🟡" if "neutre" in reason.lower() else
                     "🟠" if "Score" in reason else
                     "🔵" if "Breaker" in reason else "🔴" if "RR" in reason or "Spread" in reason else "⏸")
                lines.append("  {} <b>{}</b>  <i>{}</i>".format(ico,r["name"],reason))
        lines.append("")
    lines.append("🟢 <b>{} signal(s) détecté(s) !</b>".format(len(found)) if found else "🟡 Aucun signal ce cycle")
    msg="\n".join(lines)
    if len(msg)>4000: msg=msg[:3900]+"\n...(tronqué)"
    tg_send(uid,msg)

def handle_resetcount(uid, target):
    if uid!=ADMIN_ID: tg_send(uid,"❌ Accès refusé."); return
    try:
        t=int(target) if target and target.lstrip("@").isdigit() else (find_user(target) if target else uid)
        if not t: tg_send(uid,"❌ Introuvable."); return
        ds=datetime.now().strftime("%Y-%m-%d"); db_run("DELETE FROM sig_counts WHERE user_id=? AND date_str=?",(t,ds))
        tg_send(uid,"✅ Compteur remis à 0 pour <code>{}</code>.".format(t))
    except Exception as e: tg_send(uid,"❌ {}".format(e))

# ── Messages Promo ───────────────────────────────────────────────
PROMO_MSGS = [
    {"id":"promo_excuse","label":"🙏 Excuses membres",
     "text":(
        "🙏 <b>Un mot de l\'équipe AlphaBot</b>\n\n"
        "Cher membre,\n\n"
        "Nous avons ajusté notre système pour vous envoyer "
        "uniquement les signaux essentiels — plus de clarté, moins de bruit.\n\n"
        "📡 Désormais :\n"
        "✅ Signaux filtrés (score ≥ 85/100 uniquement)\n"
        "✅ Rapport de performance chaque soir\n"
        "✅ Résultats TP/SL transparents\n\n"
        "Merci pour votre confiance. 🙏\n\n"
        "<i>— @leaderOdg · AlphaBot PRO v10</i>"
     )},
    {"id":"promo_1","label":"📊 Réveil doux",
     "text":(
        "📊 <b>Soyons honnêtes.</b>\n\n"
        "Si tu suis uniquement les signaux gratuits,\n"
        "tu vois les opportunités passer… sans pouvoir agir pleinement.\n\n"
        "👉 Les meilleurs setups sont filtrés.\n\n"
        "🎯 AlphaBot PRO n\'est pas fait pour regarder,\n"
        "mais pour agir avec précision.\n\n"
        "La question est simple :\n"
        "Tu veux continuer à observer, ou commencer à progresser ?\n\n"
        "💎 /pay — accès PRO\n"
        "📩 @leaderodg_bot"
     )},
    {"id":"promo_2","label":"🎯 Rareté + qualité",
     "text":(
        "🎯 <b>Aujourd\'hui sur AlphaBot PRO :</b>\n\n"
        "Seulement quelques setups propres ont passé les filtres.\n\n"
        "✔️ Liquidité Smart Money confirmée\n"
        "✔️ Order Block M15 validé\n"
        "✔️ RR minimum 1:3\n\n"
        "📉 Le marché ne donne pas beaucoup d\'opportunités propres.\n"
        "📈 Mais ceux qui sont bien positionnés en profitent.\n\n"
        "💎 Moins de trades, meilleure précision.\n"
        "👉 @leaderodg_bot  →  /pay"
     )},
    {"id":"promo_3","label":"💡 Preuve + doute",
     "text":(
        "🤔 <b>Beaucoup pensent que le problème vient du marché…</b>\n\n"
        "Mais souvent, c\'est le timing et la précision de l\'entrée.\n\n"
        "👉 AlphaBot PRO filtre justement ces erreurs :\n\n"
        "  ✔️ Moins de signaux\n"
        "  ✔️ Meilleures entrées\n"
        "  ✔️ Cohérence sur la durée\n\n"
        "Ceux qui comprennent ça changent leur résultat.\n\n"
        "💎 /pay — {}$ USDT\n"
        "📩 @leaderodg_bot"
     ).format(PRO_PRICE)},
    {"id":"promo_4","label":"📊 Résultats du jour","text":None},
    {"id":"promo_5","label":"🤝 Parrainage",
     "text":(
        "🤝 <b>Programme Parrainage AlphaBot</b>\n\n"
        "Invite {} personnes → accès PRO offert ({} mois) 🎁\n\n"
        "Tu progresses avec ton réseau.\n"
        "Plus tu partages de valeur, plus tu en reçois.\n\n"
        "👉 Utilise /ref pour obtenir ton lien personnalisé.\n\n"
        "📩 @leaderodg_bot"
     ).format(REF_TARGET, REF_MONTHS)},
]

def _build_promo(pid):
    p=next((x for x in PROMO_MSGS if x["id"]==pid),None)
    if not p: return None
    if pid!="promo_4": return p["text"]
    st=db_daily_stats()
    if not st["n"]: return None
    lines=["📊 <b>RÉSULTATS D\'AUJOURD\'HUI</b>\n"]
    for row in st["rows"]:
        pair,side,rr,g001,g1,l001,l1,sess,mode=row
        ok=rr>=2.5; icon="🟢" if ok else "🔴"; d="ACHAT" if side=="BUY" else "VENTE"
        res="✅ TP → <b>+${:.0f}</b>".format(g1) if ok else "❌ SL → <b>-${:.0f}</b>".format(l1)
        lines.append("{} <b>{}</b> {}  {} (lot 0.01)".format(icon,pair,d,res))
    lines+=["","💰 <b>Total : +${}</b> lot 0.01  ·  +${} lot 1.00 🔥".format(st["g001"],st["g1"]),
            "","Et toi tu étais où ? 👀","","📩 Rejoins la communauté\n➡️ @leaderodg_bot"]
    return "\n".join(lines)

def send_promo_list(uid):
    if uid!=ADMIN_ID: return
    st=db_daily_stats()
    btns=[[{"text":p["label"],"callback_data":"adm_promo_{}".format(p["id"])}] for p in PROMO_MSGS]
    btns.append([{"text":"◀️ Panel Admin","callback_data":"adm_panel"}])
    tg_send(uid,"📢 <b>MESSAGES PROMO</b>\n"+"═"*22+"\n\nSélectionne un message à envoyer.\n\n📊 Aujourd\'hui: <b>{} signaux · {} TP · +${} lot1</b>".format(st["n"],st["wins"],st["g1"]),kb={"inline_keyboard":btns})

def send_promo_preview(uid, pid):
    if uid!=ADMIN_ID: return
    p=next((x for x in PROMO_MSGS if x["id"]==pid),None)
    if not p: return
    text=_build_promo(pid)
    if not text: tg_send(uid,"⚠️ Pas de signaux aujourd\'hui pour ce message.",kb={"inline_keyboard":[[{"text":"◀️ Retour","callback_data":"adm_promo_list"}]]}); return
    def _uid(u): return u["user_id"] if isinstance(u, dict) else u
    total=len({_uid(u) for u in pro_users()+free_users()})
    tg_send(uid,"👁 <b>APERÇU</b> — {}\n".format(p["label"])+"─"*22+"\n\n"+text+"\n\n"+"─"*22+"\n📤 Envoyer à <b>{}</b> membres ?".format(total),
        kb={"inline_keyboard":[[{"text":"✅ Envoyer à TOUS maintenant","callback_data":"adm_promo_send_{}".format(pid)}],[{"text":"◀️ Choisir autre message","callback_data":"adm_promo_list"}]]})

def broadcast_promo(uid, pid):
    if uid!=ADMIN_ID: return
    text=_build_promo(pid)
    if not text: tg_send(uid,"⚠️ Impossible de générer ce message."); return
    def _uid(u): return u["user_id"] if isinstance(u, dict) else u
    users=list({_uid(u) for u in pro_users()+free_users()})
    tg_send(uid,"📤 Envoi en cours à <b>{}</b> membres...".format(len(users)))
    sent=fail=0
    for u in users:
        if u==uid: continue
        r=tg_send(u,text)
        if r.get("ok"): sent+=1
        else: fail+=1
        time.sleep(0.05)
    tg_sticker(uid,STK_ROCKET)
    tg_send(uid,"✅ <b>Broadcast terminé !</b>\n\n✉️ Envoyés: <b>{}</b>  ·  ❌ Échoués: <b>{}</b>".format(sent,fail),kb=kb_admin_back())

def handle_bcast_start(uid, target):
    _bcast_pending[uid]={"target":target,"step":"waiting"}
    nb=len(pro_users())+len(free_users()) if target=="ALL" else len(pro_users())
    tg_send(uid,"✉️ <b>BROADCAST → {}</b>\n\nEnvoie le message à diffuser à <b>{} membres</b>.\n\n💡 HTML supporté : <b>gras</b>, <i>italique</i>\n\n/annuler pour annuler.".format(target,nb),kb={"inline_keyboard":[[{"text":"❌ Annuler","callback_data":"adm_panel"}]]})

def handle_bcast_msg(uid, text):
    if uid not in _bcast_pending: return False
    state=_bcast_pending.pop(uid); target=state["target"]
    def _uid(u): return u["user_id"] if isinstance(u, dict) else u
    users=list({_uid(u) for u in pro_users()+free_users()}) if target=="ALL" else [_uid(u) for u in pro_users()]
    tg_send(uid,"📤 Envoi en cours à <b>{}</b> membres...".format(len(users)))
    sent=fail=0
    for u in users:
        if u==uid: continue
        r=tg_send(u,"📢 <b>Message de l\'équipe AlphaBot :</b>\n\n"+text+"\n\n— <i>@leaderOdg · AlphaBot PRO</i>")
        if r.get("ok"): sent+=1
        else: fail+=1
        time.sleep(0.05)
    tg_sticker(uid,STK_ROCKET)
    tg_send(uid,"✅ <b>Broadcast terminé !</b>\n✉️ Envoyés: <b>{}</b>  ·  ❌ Échoués: <b>{}</b>".format(sent,fail),kb=kb_admin_back())
    return True


_test_mode_full = ""  # admin test mode FREE/PRO


# ══════════════════════════════════════════════════════
#  📊 MOTEUR BACKTEST — Rejoue les scans sur historique
# ══════════════════════════════════════════════════════
# Principe :
#   1. Charger N bougies H (ex: 200 bougies 1h = ~8 jours)
#   2. Glisser une fenêtre de lecture : bougies[0..i] → signal ?
#   3. Pour chaque signal trouvé → simuler TP/SL sur les bougies suivantes
#   4. Compter vrais TP, vrais SL, expirations
#   5. Envoyer le rapport résumé à l'admin

def backtest_market(m, nb_candles=150, tf="1h", score_min=72):
    """
    Rejoue le scan ICT + liquidité sur les nb_candles dernières bougies.
    Retourne liste de trades simulés avec résultat réel.
    """
    # Charger l'historique complet
    raw = fetch_c(m["sym"], tf, "60d")
    if not raw or len(raw) < nb_candles + 20:
        return []

    candles = raw[-(nb_candles + 20):]   # un peu de marge pour l'analyse
    results  = []
    in_trade = False   # une seule position à la fois par paire

    # Fenêtre glissante : on analyse à chaque bougie passée
    for i in range(40, len(candles) - 5):
        if in_trade:
            continue   # déjà en position → on attend le résultat

        window = candles[:i]   # historique visible jusqu'à la bougie i
        future = candles[i:]   # bougies "futures" pour simuler TP/SL

        # ── Analyser la fenêtre ─────────────────────────────────────
        try:
            b, _, bt = detect_bias(window[-50:] if len(window) >= 50 else window)
            if b == "NEUTRAL":
                continue

            liq = agent_liquidity(window, b)
            if not liq:
                continue   # liquidité obligatoire

            bbs = breakers(window, b)
            if not bbs:
                continue

            sc  = conf_score(window, b)
            fvg_z = fvg(window, b)
            _, cc2 = choch_seq(window[-50:] if len(window) >= 50 else window)
            sh_h  = max(x["h"] for x in window[-50:])
            sl_h  = min(x["l"] for x in window[-50:])
            ote_lo, ote_hi = ote_zone(sh_h, sl_h, b)
            lp    = window[-1]["c"]
            in_ote = bool(ote_lo and ote_hi and ote_lo <= lp <= ote_hi)

            if in_ote:  sc = min(sc + 12, 115)
            if fvg_z:   sc = min(sc + 15, 115)
            if cc2 >= 2: sc = min(sc + 10, 115)
            sc = min(sc + liq["score"], 115)

            if sc < score_min:
                continue

            # ── Calculer entrée / TP / SL ──────────────────────────
            bb   = bbs[0]
            a    = atr(window)
            sp_p = m["pip"] * 1.5   # spread estimé
            eq_h, eq_l = eqh_eql(window)

            if b == "BULLISH":
                sl   = bb["bottom"] - a * 0.15 - sp_p
                risk = lp - sl
                if risk <= 0 or risk > a * 10:
                    continue
                tp = (eq_h * 0.9995) if (eq_h and lp < eq_h < lp + risk * 5) else lp + risk * 2.5
                if (tp - lp) / risk < 2.0:
                    continue
                side = "BUY"
            else:
                sl   = bb["top"] + a * 0.15 + sp_p
                risk = sl - lp
                if risk <= 0 or risk > a * 10:
                    continue
                tp = (eq_l * 1.0005) if (eq_l and lp - risk * 5 < eq_l < lp) else lp - risk * 2.5
                if (lp - tp) / risk < 2.0:
                    continue
                side = "SELL"

            rr = round(abs(tp - lp) / risk, 1)

        except Exception:
            continue

        # ── Simuler TP/SL sur les bougies futures ──────────────────
        result = "OPEN"; exit_price = None; candles_held = 0
        for fi, fc in enumerate(future[1:30]):   # max 30 bougies pour expirer
            candles_held = fi + 1
            if side == "BUY":
                if fc["h"] >= tp:
                    result = "TP"; exit_price = tp; break
                if fc["l"] <= sl:
                    result = "SL"; exit_price = sl; break
            else:
                if fc["l"] <= tp:
                    result = "TP"; exit_price = tp; break
                if fc["h"] >= sl:
                    result = "SL"; exit_price = sl; break

        if result == "OPEN":
            result = "EXPIRED"; exit_price = future[min(29, len(future)-1)]["c"]

        dp    = 2 if lp > 1000 else (3 if lp > 10 else 5)
        f_rnd = lambda v: round(v, dp)
        gain_pips = abs(tp - lp)  / m["pip"]
        loss_pips = abs(sl - lp)  / m["pip"]

        results.append({
            "pair"    : m["name"],
            "side"    : side,
            "entry"   : f_rnd(lp),
            "tp"      : f_rnd(tp),
            "sl"      : f_rnd(sl),
            "rr"      : rr,
            "score"   : sc,
            "result"  : result,
            "exit"    : f_rnd(exit_price) if exit_price else None,
            "held"    : candles_held,
            "g001"    : round(gain_pips * 0.01, 2),
            "l001"    : round(loss_pips * 0.01, 2),
            "g1"      : round(gain_pips, 2),
            "l1"      : round(loss_pips, 2),
            "liq_lbl" : liq.get("label","?"),
            "bias"    : b,
            "tf"      : tf,
            "candle_i": i,
        })
        in_trade = True   # une position à la fois
        # Avancer après la clôture du trade
        if result != "EXPIRED":
            i += candles_held   # sauter les bougies déjà utilisées

    return results


def run_backtest(uid, nb_candles=150, tf="1h", score_min=72):
    """
    Lance le backtest sur tous les marchés actifs et envoie le rapport.
    Appelé en thread depuis /backtest admin.
    """
    tg_send(uid,
        "⏳ <b>BACKTEST EN COURS...</b>\n\n"
        "📊 Paramètres :\n"
        f"  · Timeframe : <b>{tf}</b>\n"
        f"  · Bougies   : <b>{nb_candles}</b>\n"
        f"  · Score min : <b>{score_min}</b>\n\n"
        "Analyse de {} marchés...".format(len(MARKETS)))

    all_trades = []
    for m in MARKETS:
        try:
            trades = backtest_market(m, nb_candles=nb_candles,
                                     tf=tf, score_min=score_min)
            all_trades.extend(trades)
        except Exception as e:
            log("WARN", f"backtest {m['name']}: {e}")

    if not all_trades:
        tg_send(uid,
            "🔍 <b>BACKTEST — Aucun signal détecté</b>\n\n"
            "Essaie avec un score min plus bas : /backtest 72\n"
            "Ou un TF différent : /backtest 72 30m")
        return

    # ── Calculer les stats globales ─────────────────────────────────
    tp_list  = [t for t in all_trades if t["result"] == "TP"]
    sl_list  = [t for t in all_trades if t["result"] == "SL"]
    exp_list = [t for t in all_trades if t["result"] == "EXPIRED"]
    total    = len(all_trades)
    wins     = len(tp_list)
    losses   = len(sl_list)
    wr       = round(wins / total * 100) if total > 0 else 0
    net_r    = round(wins * 2.5 - losses * 1.0, 2)  # RR moyen 2.5
    g1_total = round(sum(t["g1"] for t in tp_list)
                   - sum(t["l1"] for t in sl_list), 2)
    avg_rr   = round(sum(t["rr"] for t in all_trades) / total, 1) if total else 0

    sep = "═" * 24
    perf = "🔥🔥" if wr >= 70 else ("🔥" if wr >= 55 else ("📊" if wr >= 45 else "⚠️"))

    lines = [
        f"📊 <b>RAPPORT BACKTEST</b> {perf}",
        sep,
        f"⏱ TF : <b>{tf}</b>  ·  Bougies : <b>{nb_candles}</b>  ·  Score min : <b>{score_min}</b>",
        "",
        f"📡 Signaux détectés : <b>{total}</b>",
        f"✅ TP : <b>{wins}</b>  ({wr}% Win Rate)",
        f"❌ SL : <b>{losses}</b>",
        f"⏳ Expirés (30 bougies) : <b>{len(exp_list)}</b>",
        "",
        f"📐 RR moyen : <b>1:{avg_rr}</b>",
        f"💰 Résultat net lot 1.00 : <b>{'+'if g1_total>=0 else ''}{g1_total}$</b>",
        f"📈 R total : <b>{'+'if net_r>=0 else ''}{net_r}R</b>",
        sep,
        "",
        "📋 <b>DÉTAIL PAR PAIRE</b>",
        "",
    ]

    # ── Grouper par paire ───────────────────────────────────────────
    pairs_seen = {}
    for t in all_trades:
        p = t["pair"]
        pairs_seen.setdefault(p, {"tp":0,"sl":0,"exp":0,"g1":0})
        if t["result"] == "TP":
            pairs_seen[p]["tp"] += 1
            pairs_seen[p]["g1"] += t["g1"]
        elif t["result"] == "SL":
            pairs_seen[p]["sl"] += 1
            pairs_seen[p]["g1"] -= t["l1"]
        else:
            pairs_seen[p]["exp"] += 1

    for pair, st in sorted(pairs_seen.items(),
                           key=lambda x: -(x[1]["tp"])):
        tot_p = st["tp"] + st["sl"] + st["exp"]
        wr_p  = round(st["tp"] / tot_p * 100) if tot_p else 0
        g     = round(st["g1"], 2)
        lines.append(
            f"  <b>{pair}</b> — {tot_p} sig  "
            f"{st['tp']}✅ {st['sl']}❌  "
            f"{wr_p}% WR  "
            f"{'+'if g>=0 else ''}{g}$")

    lines += [
        "",
        sep,
        "📝 <b>10 DERNIERS TRADES</b>",
        "",
    ]

    for t in all_trades[-10:]:
        icon = "✅" if t["result"] == "TP" else ("❌" if t["result"] == "SL" else "⏳")
        d    = "⬆️" if t["side"] == "BUY" else "⬇️"
        g    = f"+{t['g1']}$" if t["result"] == "TP" else (
               f"-{t['l1']}$" if t["result"] == "SL" else "exp.")
        lines.append(
            f"{icon} <b>{t['pair']}</b> {d}  "
            f"E:<code>{t['entry']}</code> "
            f"TP:<code>{t['tp']}</code> "
            f"SL:<code>{t['sl']}</code>  "
            f"RR 1:{t['rr']}  {g}  "
            f"💧{t['liq_lbl']}")

    lines += [
        "",
        sep,
        "⚠️ Backtest = données historiques. Résultats passés ≠ futurs.",
        "🤖 AlphaBot PRO  ·  @leaderodg_bot",
    ]

    msg = "\n".join(lines)
    # Telegram limite à 4096 chars
    if len(msg) > 4000:
        msg = msg[:3900] + "\n\n...(tronqué — trop de signaux)"

    tg_send(uid, msg, kb=kb_admin_back())
    log("AI", clr(f"Backtest terminé — {total} trades, WR {wr}%", "g"))

def dispatch(uid, uname, txt):
    """Dispatcher principal — gère boutons clavier ET commandes slash."""
    t = txt.strip()
    # ── /start géré EN PREMIER pour préserver ref_by ─────────────────
    _p0 = t.split()
    _c0 = _p0[0].lower().lstrip("/").split("@")[0] if _p0 else ""
    if _c0 in ("start", "menu", "aide", "help"):
        _a0 = " ".join(_p0[1:]) if len(_p0) > 1 else ""
        ref = int(_a0) if _a0.isdigit() else 0
        send_welcome(uid, uname, ref_by=ref)
        db_run("UPDATE users SET last_seen=? WHERE user_id=?", (datetime.now().isoformat(), uid))
        return
    db_register(uid, uname)
    db_run("UPDATE users SET last_seen=? WHERE user_id=?",
           (datetime.now().isoformat(), uid))

    # ── 1. BOUTONS DU CLAVIER PHYSIQUE (texte exact) ─────────────
    # ── Boutons menu — toutes variantes emoji couvertes ─────────────
    _SIG = {"📡 Mes Signaux","📩 Mes Signaux","🛰 Mes Signaux","Mes Signaux","mes signaux"}
    _CPT = {"📊 Mon Compte","📊 Mon compte","📊 Mon Tableau de Bord","Mon Compte","mon compte"}
    _PRO = {"💰 Devenir PRO","💎 Devenir PRO","💠 Devenir PRO","💰 Paiement USDT","Devenir PRO"}
    _PAR = {"🤝 Parrainage","🤝 Devenir Affilié","Parrainage","parrainage"}
    _GAI = {"💸 Mes Gains","💰 Mes Gains","📈 Mes Gains","Mes Gains","mes gains"}
    _GUI = {"📖 Guide ICT","📖 Guide AlphaBot","Guide ICT","guide ict"}
    _RAP = {"📈 Rapports","Rapports","rapports"}
    _BRK = {"🏦 Broker Exness","Broker Exness","broker exness"}
    if t in _SIG:
        threading.Thread(target=send_signals_info, args=(uid,), daemon=True).start(); return
    if t in _CPT:
        forced = _test_mode if uid == ADMIN_ID and _test_mode else None
        threading.Thread(target=send_account, args=(uid, uname, forced), daemon=True).start(); return
    if t in _PRO:
        threading.Thread(target=send_pro_page, args=(uid,), daemon=True).start(); return
    if t in _PAR:
        threading.Thread(target=send_affilie, args=(uid, uname), daemon=True).start(); return
    if t in _GAI:
        threading.Thread(target=send_mes_gains, args=(uid,), daemon=True).start(); return
    if t in _GUI:
        threading.Thread(target=send_guide, args=(uid,), daemon=True).start(); return
    if t in _RAP:
        threading.Thread(target=send_rapports, args=(uid,), daemon=True).start(); return
    if t in _BRK:
        threading.Thread(target=send_broker, args=(uid,), daemon=True).start(); return
    # Anciens boutons (rétrocompatibilité)
    if t in ("📩 Mes Signaux", "🛰 Mes Signaux"):
        threading.Thread(target=send_signals_info, args=(uid,), daemon=True).start(); return
    if t in ("💎 Devenir PRO", "💠 Devenir PRO", "💰 Paiement USDT"):
        threading.Thread(target=send_pro_page, args=(uid,), daemon=True).start(); return
    if t in ("📊 Mon Tableau de Bord", "📊 Mon compte"):
        threading.Thread(target=send_account, args=(uid, uname), daemon=True).start(); return
    if t in ("💰 Mes Gains", "📈 Mes Gains"):
        threading.Thread(target=send_mes_gains, args=(uid,), daemon=True).start(); return
    if t in ("🤝 Parrainage", "🤝 Devenir Affilié"):
        threading.Thread(target=send_affilie, args=(uid, uname), daemon=True).start(); return

    # ── 2. BROADCAST ADMIN (texte libre en attente) ──────────────
    if uid == ADMIN_ID and t and not t.startswith("/"):
        if handle_bcast_msg(uid, t):
            return  # message traité comme broadcast

    # ── 3. COMMANDES SLASH ────────────────────────────────────────
    parts = t.split()
    cmd   = parts[0].lower().lstrip("/").split("@")[0] if parts else ""
    arg   = " ".join(parts[1:]) if len(parts) > 1 else ""

    if cmd in ("pay",):
        threading.Thread(target=send_pay_plan, args=(uid,), daemon=True).start(); return
    if cmd == "admin":
        threading.Thread(target=send_admin_full, args=(uid,), daemon=True).start(); return
    if cmd == "pro":
        threading.Thread(target=send_pro_page, args=(uid,), daemon=True).start(); return
    if cmd in ("ref", "parrainage"):
        threading.Thread(target=send_affilie, args=(uid, uname), daemon=True).start(); return
    if cmd == "broker":
        threading.Thread(target=send_broker, args=(uid,), daemon=True).start(); return
    if cmd in ("guide", "pdf"):
        threading.Thread(target=send_guide, args=(uid,), daemon=True).start(); return
    if cmd in ("monstatus", "status", "compte", "account"):
        threading.Thread(target=send_account, args=(uid, uname), daemon=True).start(); return
    if cmd in ("rapports", "report", "perf"):
        threading.Thread(target=send_rapports, args=(uid,), daemon=True).start(); return
    if cmd in ("historique", "history", "hist"):
        threading.Thread(target=send_history_full, args=(uid, 1), daemon=True).start(); return
    if cmd == "support":
        tg_send(uid, "📩 <b>Support</b>\nID : <code>{}</code>\n👉 @leaderOdg".format(uid)); return
    if cmd == "marches":
        threading.Thread(target=handle_marches_full, args=(uid,), daemon=True).start(); return

    # ── TX Hash ────────────────────────────────────────────────────
    if cmd == "txhash" and arg:
        threading.Thread(target=lambda: handle_proof(uid, uname, tx=arg), daemon=True).start(); return

    # ── Commandes admin ────────────────────────────────────────────
    if uid == ADMIN_ID:
        if cmd == "resetkb":
            def _do_resetkb():
                try:
                    rows = db_all("SELECT user_id FROM users")
                    ok = err = 0
                    for (ruid,) in rows:
                        try:
                            tg_send(ruid, "✅ Menu mis à jour ↓", kb={"remove_keyboard": True})
                            time.sleep(0.05)
                            tg_send(ruid, "🤖 <b>AlphaBot PRO</b> — Clique un bouton ↓",
                                    kb=kb_main(is_pro(ruid)))
                            ok += 1
                        except Exception: err += 1
                    tg_send(uid, "✅ /resetkb — {} OK  ·  {} erreurs".format(ok, err))
                except Exception as e:
                    tg_send(uid, "❌ resetkb: {}".format(e))
            tg_send(uid, "🔄 Réinitialisation clavier en cours...")
            threading.Thread(target=_do_resetkb, daemon=True).start(); return
        if cmd == "scan":
            tg_send(uid, "📡 Scan lancé...")
            threading.Thread(target=scan_and_send, daemon=True).start(); return
        if cmd in ("backtest", "bt"):
            # Usage : /backtest [score_min] [tf] [nb_candles]
            # Ex: /backtest 72 1h 150  ou  /backtest 80 30m 200
            bt_args  = arg.split() if arg else []
            bt_score = int(bt_args[0]) if len(bt_args) > 0 and bt_args[0].isdigit() else 72
            bt_tf    = bt_args[1] if len(bt_args) > 1 else "1h"
            bt_nb    = int(bt_args[2]) if len(bt_args) > 2 and bt_args[2].isdigit() else 150
            # Valider le TF
            if bt_tf not in ("5m","15m","30m","1h","4h"):
                bt_tf = "1h"
            threading.Thread(target=run_backtest,
                             args=(uid, bt_nb, bt_tf, bt_score),
                             daemon=True).start(); return
        if cmd == "annuler":
            _bcast_pending.pop(uid, None)
            tg_send(uid, "❌ Broadcast annulé.", kb=kb_main(False)); return
        if cmd == "debug":
            if not _last_results: tg_send(uid, "Aucun scan encore."); return
            lines = ["🔍 <b>DEBUG DERNIER SCAN</b>", ""]
            for r in _last_results:
                tag  = ""
                icon = "🟢" if r["found"] else "⚪"
                lines.append("{} <b>{}</b>{}  {}".format(
                    icon, r["name"], tag,
                    "Signal ✓" if r["found"] else r.get("reason", "?")))
            msg = "\n".join(lines)
            if len(msg) > 4000: msg = msg[:3900] + "\n...(tronqué)"
            tg_send(uid, msg); return
        if cmd == "activate":
            handle_activate(uid, arg); return
        if cmd == "degrade":
            handle_degrade(uid, arg); return
        if cmd == "activateall":
            # Active PRO pour TOUS les membres FREE
            threading.Thread(target=_handle_activateall, args=(uid,), daemon=True).start(); return
        if cmd == "activatepro":
            # /activatepro @username ou /activatepro ID
            if not arg:
                tg_send(uid, "Usage : /activatepro @username  ou  /activatepro ID"); return
            handle_activate(uid, arg.strip()); return
        if cmd == "testfree":
            handle_testfree(uid); return
        if cmd == "testpro":
            handle_testpro(uid); return
        if cmd in ("stats",):
            threading.Thread(target=send_admin_stats_full, args=(uid,), daemon=True).start(); return
        if cmd == "membres":
            pg = int(arg) if arg.isdigit() else 1
            threading.Thread(target=handle_membres, args=(uid, pg), daemon=True).start(); return
        if cmd == "resetcount":
            handle_resetcount(uid, arg); return
        if cmd == "stop":
            tg_send(uid, "🛑 Bot arrêté.")
            raise KeyboardInterrupt
        # ── Commandes PaymentManager PRO ─────────────────────────
        if cmd == "paydash" and _PM_AVAILABLE:
            from alphabot_payment_manager import cmd_paydash
            cmd_paydash(uid); return
        if cmd == "activate" and _PM_AVAILABLE:
            from alphabot_payment_manager import cmd_activate_manual
            cmd_activate_manual(uid, arg); return
        if cmd == "degrade" and _PM_AVAILABLE:
            from alphabot_payment_manager import cmd_degrade_manual
            cmd_degrade_manual(uid, arg); return

    # ── Fallback : afficher le menu ───────────────────────────────
    send_welcome(uid, uname)


# ══════════════════════════════════════════════════════════════════════
#  FONCTIONS CLAUDE AI AVANCÉES v19 — MAXIMISATION RENTABILITÉ
# ══════════════════════════════════════════════════════════════════════

def claude_optimize_tp_sl(sig: dict, session: str) -> dict:
    """
    Claude optimise les niveaux TP/SL selon la structure institutionnelle.
    Appelé APRÈS validation pour affiner les niveaux avant envoi.
    """
    if not _ANTHROPIC_OK or not CLAUDE_API_KEY:
        return {}
    try:
        entry = float(sig.get("entry", 0))
        tp    = float(sig.get("tp", 0))
        sl    = float(sig.get("sl", 0))
        side  = sig.get("side", "BUY")
        atr_v = float(sig.get("atr", 0))
        risk  = abs(entry - sl)

        prompt = """Tu es un Risk Manager institutionnel ICT. Optimise ces niveaux de trade.

Signal : {pair} {side}
Entrée  : {entry}
SL actuel : {sl}
TP actuel : {tp}
ATR M15   : {atr}
Session   : {session}
Badges    : {badges}

Règles d'optimisation :
- SL doit être SOUS le dernier swing low (BUY) ou AU-DESSUS du swing high (SELL)
- SL minimum = 1.1× ATR de distance de l'entrée
- TP1 = RR 1:3 minimum
- TP2 = niveau institutionnel suivant (liquidité haute, gap, OB supérieur)
- Si le SL actuel est trop proche (<0.8× ATR), l'éloigner légèrement

Réponds UNIQUEMENT avec ce JSON :
{{
  "sl_optimise": <prix ou null si OK>,
  "tp1_optimise": <prix ou null si OK>,
  "tp2": <prix TP2 institutionnel>,
  "rr_tp1": <nouveau RR TP1>,
  "rr_tp2": <nouveau RR TP2>,
  "note": "<raison de l'optimisation, 1 phrase>"
}}""".format(
            pair=sig.get("name","?"), side=side,
            entry=entry, sl=sl, tp=tp, atr=atr_v,
            session=session, badges=sig.get("badges","?")[:100])

        client = _anthropic_sdk.Anthropic(api_key=CLAUDE_API_KEY)
        resp = client.messages.create(
            model=CLAUDE_MODEL, max_tokens=400,
            messages=[{"role": "user", "content": prompt}])
        raw = resp.content[0].text.strip()
        if "```" in raw:
            raw = raw.split("```")[1]
            if raw.startswith("json"): raw = raw[4:]
        result = json.loads(raw.strip())
        _LAI.info("Optimisation TP/SL {} — RR TP1:{} TP2:{}".format(
            sig.get("name","?"), result.get("rr_tp1","?"), result.get("rr_tp2","?")))
        return result
    except Exception as e:
        _LAI.error("claude_optimize_tp_sl: {}".format(e))
        return {}


def claude_market_context(pairs_scanned: list, session: str) -> str:
    """
    Claude analyse le contexte global des marchés avant le scan.
    Retourne un texte de contexte à inclure dans le message admin.
    """
    if not _ANTHROPIC_OK or not CLAUDE_API_KEY:
        return ""
    try:
        now_utc = datetime.now(timezone.utc).strftime("%H:%M UTC — %A")
        pairs_str = ", ".join(pairs_scanned[:10]) if pairs_scanned else "?"

        prompt = """Tu es un trader institutionnel ICT. Donne un contexte de marché ultra-concis.

Heure : {heure}
Session : {session}
Paires actives : {pairs}

En 3 bullets maximum :
- Quel est le régime de marché dominant aujourd'hui ? (trending / ranging / choppy)
- Quelle session est la plus propice aux setups ICT maintenant ?
- 1 paire prioritaire à surveiller et pourquoi

Format : texte HTML Telegram sans JSON, 3 lignes max, émojis, très concis.""".format(
            heure=now_utc, session=session, pairs=pairs_str)

        client = _anthropic_sdk.Anthropic(api_key=CLAUDE_API_KEY)
        resp = client.messages.create(
            model=CLAUDE_MODEL, max_tokens=250,
            messages=[{"role": "user", "content": prompt}])
        return resp.content[0].text.strip()
    except Exception as e:
        _LAI.error("claude_market_context: {}".format(e))
        return ""


def claude_entry_alert(sig: dict, current_price: float) -> dict:
    """
    Alerte intelligente : Claude décide si le prix actuel est optimal
    pour entrer MAINTENANT ou s'il faut attendre.
    """
    if not _ANTHROPIC_OK or not CLAUDE_API_KEY:
        return {"entrer": True, "raison": "IA non disponible"}
    try:
        entry = float(sig.get("entry", 0))
        sl    = float(sig.get("sl", 0))
        tp    = float(sig.get("tp", 0))
        side  = sig.get("side", "BUY")
        dist_from_entry_pct = abs(current_price - entry) / entry * 100

        prompt = """Signal ICT en attente. Dois-je entrer MAINTENANT ou attendre ?

Signal : {pair} {side}
Prix entrée idéale : {entry}
Prix actuel        : {current}
Distance entrée    : {dist:.3f}%
SL : {sl}  |  TP : {tp}
RR : 1:{rr}
Badges : {badges}

Règle : entrer si prix dans ±0.15% de l'entrée ET structure intacte.
Si prix trop loin de l'entrée → attendre pullback.
Si prix déjà TP side → signal expiré.

Réponds UNIQUEMENT JSON :
{{
  "entrer": true ou false,
  "urgence": "IMMEDIATE" ou "ATTENDRE" ou "EXPIRE",
  "raison": "<1 phrase>",
  "prix_optimal": <prix d'entrée optimal selon toi>
}}""".format(
            pair=sig.get("name","?"), side=side,
            entry=entry, current=round(current_price,5),
            dist=dist_from_entry_pct, sl=sl, tp=tp,
            rr=sig.get("rr","?"), badges=sig.get("badges","?")[:80])

        client = _anthropic_sdk.Anthropic(api_key=CLAUDE_API_KEY)
        resp = client.messages.create(
            model=CLAUDE_MODEL, max_tokens=200,
            messages=[{"role": "user", "content": prompt}])
        raw = resp.content[0].text.strip()
        if "```" in raw:
            raw = raw.split("```")[1]
            if raw.startswith("json"): raw = raw[4:]
        return json.loads(raw.strip())
    except Exception as e:
        _LAI.error("claude_entry_alert: {}".format(e))
        return {"entrer": True, "urgence": "IMMEDIATE", "raison": "IA erreur — entrée par défaut"}


def _claude_rapport_analyse(trades_today, trades_week):
    """Analyse les vrais trades avec Claude AI et génère un rapport."""
    if not _ANTHROPIC_OK or not CLAUDE_API_KEY:
        return None
    try:
        def fmt_trade(t):
            pair  = t[0]; side = t[1]; rr = t[2]
            g001  = t[3]; g1   = t[4]; l001 = t[5]; l1 = t[6]
            entry = t[8]  if len(t) > 8 else "?"
            tp    = t[9]  if len(t) > 9 else "?"
            sl    = t[10] if len(t) > 10 else "?"
            result = "TP ATTEINT (+${:.0f} lot1)".format(g1) if rr >= 3.0 \
                     else "SL TOUCHE (-${:.0f} lot1)".format(l1)
            return "  * {} {} | Entree:{} TP:{} SL:{} | RR 1:{} | {}".format(
                pair, side, entry, tp, sl, rr, result)

        today_lines = [fmt_trade(t) for t in trades_today] if trades_today \
                      else ["  Aucun trade aujourd'hui"]
        week_lines  = [fmt_trade(t) for t in trades_week]  if trades_week  \
                      else ["  Aucun trade cette semaine"]

        prompt = (
            "Tu es l'analyste senior d'AlphaBot PRO, un bot de signaux ICT/SMC.\n\n"
            "Voici les VRAIS trades realises aujourd'hui :\n"
            + "\n".join(today_lines) +
            "\n\nVoici les trades de la semaine (7 derniers jours) :\n"
            + "\n".join(week_lines) +
            "\n\nTa mission :\n"
            "1. Analyse la PERFORMANCE reelle (winrate, gains/pertes nets)\n"
            "2. Identifie les PATTERNS : quelles paires/sessions ont le mieux fonctionne ?\n"
            "3. Donne 2-3 ENSEIGNEMENTS cles tires de ces trades\n"
            "4. Propose 1 RECOMMANDATION concrete pour demain\n\n"
            "Format: texte HTML Telegram (<b>bold</b>, <i>italic</i>)\n"
            "Sois concis, professionnel, factuel. Maximum 400 mots.\n"
            "Commence directement par l'analyse sans preambule."
        )

        client = _anthropic_sdk.Anthropic(api_key=CLAUDE_API_KEY)
        resp = client.messages.create(
            model=CLAUDE_MODEL, max_tokens=800,
            messages=[{"role": "user", "content": prompt}])
        return resp.content[0].text.strip()
    except Exception as e:
        _LAI.error("Claude rapport: {}".format(e))
        return None


def _get_live_price(pair_name):
    """Récupère le prix actuel d'une paire via Yahoo Finance."""
    mkt = next((m for m in MARKETS if m["name"] == pair_name), None)
    if not mkt: return None
    try:
        c = fetch_c(mkt["sym"], "5m", "1d")
        if c and len(c) >= 1:
            return c[-1]["c"], c
    except: pass
    return None, None


def _signal_still_valid(sig, current_price):
    """
    Vérifie si le signal est encore valide :
    - Prix n'a pas touché SL
    - Prix n'a pas dépassé TP
    - Signal < 4h (sinon expiré)
    """
    side  = sig.get("side","BUY")
    entry = float(sig.get("entry", 0))
    tp    = float(sig.get("tp", 0))
    sl    = float(sig.get("sl", 0))
    price = float(current_price)

    if side == "BUY":
        if price <= sl:   return "SL_HIT",  "❌ SL touché — signal invalidé"
        if price >= tp:   return "TP_HIT",  "✅ TP atteint — signal terminé"
        if price < entry: return "VALID_PULLBACK", "✅ Valide — prix en pullback vers entrée"
        return "VALID_RUNNING", "🟢 En cours — prix au-dessus de l'entrée"
    else:  # SELL
        if price >= sl:   return "SL_HIT",  "❌ SL touché — signal invalidé"
        if price <= tp:   return "TP_HIT",  "✅ TP atteint — signal terminé"
        if price > entry: return "VALID_PULLBACK", "✅ Valide — prix en pullback vers entrée"
        return "VALID_RUNNING", "🟢 En cours — prix en dessous de l'entrée"


def _adjust_entry_sl(sig, candles, current_price):
    """
    Recalcule l'entrée et le SL optimaux selon le prix actuel.
    Ne change PAS le TP (objectif institutionnel).
    """
    side  = sig.get("side","BUY")
    pip   = sig.get("pip", 0.0001)
    a     = atr(candles) if candles and len(candles) >= 14 else None
    entry = float(current_price)

    if a:
        if side == "BUY":
            new_sl = round(entry - a * 1.2, 5)
        else:
            new_sl = round(entry + a * 1.2, 5)
    else:
        # Fallback : garder distance SL originale
        orig_dist = abs(float(sig.get("entry",0)) - float(sig.get("sl",0)))
        new_sl = round(entry - orig_dist, 5) if side=="BUY" else round(entry + orig_dist, 5)

    return round(entry, 5), new_sl


def handle_check_signal(uid, pair_side_key):
    """
    Handler bouton 'Signal toujours valide ?'
    1. Prix live → statut SL/TP/EN COURS
    2. Si signal encore en jeu → re-analyse Claude complète (fondamental + news live)
    3. Répond avec analyse IA + bouton actualiser
    """
    with _ACTIVE_SIGNALS_LOCK:
        sig = _ACTIVE_SIGNALS.get(pair_side_key)
    if not sig:
        tg_send(uid, "⏳ <b>Signal expiré ou introuvable.</b>\n\nLe signal n'est plus en mémoire (> 4h).\n🔄 Attends le prochain scan.")
        return

    pair  = sig.get("name", "?")
    side  = sig.get("side", "BUY")
    entry = sig.get("entry", "?")
    tp    = sig.get("tp", "?")
    sl    = sig.get("sl", "?")
    rr    = sig.get("rr", "?")
    sc    = sig.get("score", 0)
    d     = "⬆️" if side == "BUY" else "⬇️"
    sf    = "ACHAT" if side == "BUY" else "VENTE"

    tg_send(uid,
        "🔄 <b>Analyse en cours...</b>\n"
        "📡 Prix live {} + validation Claude IA...".format(pair))

    # ── 1. Prix live ─────────────────────────────────────────────
    result  = _get_live_price(pair)
    current, candles = result if isinstance(result, tuple) else (result, None)
    if not current:
        tg_send(uid,
            "❌ <b>Prix indisponible</b>\n\n"
            "Impossible de récupérer {} live.\nRéessaie dans quelques secondes.".format(pair),
            kb=kb_back())
        return

    dp      = 2 if float(current) > 100 else (3 if float(current) > 10 else 5)
    fmt_p   = "{{:.{}f}}".format(dp)
    cur_fmt = fmt_p.format(float(current))

    # ── 2. Statut SL / TP / EN COURS ────────────────────────────
    status, status_msg = _signal_still_valid(sig, current)
    is_active = status in ("VALID_PULLBACK", "VALID_RUNNING", "WAITING")

    kb_refresh = {"inline_keyboard": [[
        {"text": "🔄 Actualiser l'analyse", "callback_data": "check_sig_{}".format(pair_side_key)},
        {"text": "◀️ Retour", "callback_data": "start"},
    ]]}

    # ── 3a. SL touché ────────────────────────────────────────────
    if status == "SL_HIT":
        msg = (
            "🔍 <b>VÉRIFICATION — {}</b>\n".format(pair) +
            "═"*22 + "\n\n"
            "❌ <b>SIGNAL INVALIDÉ — SL touché</b>\n\n"
            "  {} {} <b>{}</b>\n".format(d, sf, pair) +
            "  💹 Prix actuel : <code>{}</code>\n".format(cur_fmt) +
            "  🛑 SL          : <code>{}</code>\n\n".format(sl) +
            "  {}\n\n".format(status_msg) +
            "⏳ Attends le prochain scan pour un nouveau setup."
        )
        with _ACTIVE_SIGNALS_LOCK:
            _ACTIVE_SIGNALS.pop(pair_side_key, None)
        tg_send(uid, msg, kb=kb_back()); return

    # ── 3b. TP atteint ───────────────────────────────────────────
    if status == "TP_HIT":
        dist    = abs(float(tp) - float(entry))
        sl_d    = abs(float(entry) - float(sl))
        rr_real = round(dist / sl_d, 1) if sl_d > 0 else rr
        g001    = sig.get("g001", "?")
        g1      = sig.get("g1", "?")
        msg = (
            "🔍 <b>VÉRIFICATION — {}</b>\n".format(pair) +
            "═"*22 + "\n\n"
            "✅ <b>TP ATTEINT !</b> 🎉\n\n"
            "  {} {} <b>{}</b>\n".format(d, sf, pair) +
            "  💹 Prix actuel : <code>{}</code>\n".format(cur_fmt) +
            "  🎯 TP          : <code>{}</code>\n".format(tp) +
            "  📐 RR réalisé  : <b>1:{}</b>\n\n".format(rr_real) +
            "  💵 Lot 0.01 : <b>+${}</b>  ·  💰 Lot 1.00 : <b>+${}</b>\n\n".format(g001, g1) +
            "🏆 Excellent trade !"
        )
        with _ACTIVE_SIGNALS_LOCK:
            _ACTIVE_SIGNALS.pop(pair_side_key, None)
        tg_send(uid, msg, kb=kb_back()); return

    # ── 3c. Signal encore actif → Re-analyse Claude complète ────
    sn, _, _, _ = get_session()
    htf_trend   = sig.get("bias", "BULLISH")

    sig_live = dict(sig)
    sig_live["entry"]       = current
    sig_live["_live_price"] = cur_fmt
    sig_live["_age_min"]    = round(
        (time.time() - sig.get("_ts_epoch", time.time())) / 60, 1)

    try:
        ai_live = claude_validate_signal(sig_live, sn, htf_trend)
    except Exception as _e:
        ai_live = {}
        log("WARN", "handle_check_signal Claude: {}".format(_e))

    new_entry_line = ""
    if is_active and candles:
        try:
            ne, ns  = _adjust_entry_sl(sig, candles, current)
            sl_dist = abs(ne - ns)
            tp_dist = abs(ne - float(tp))
            new_rr  = round(tp_dist / sl_dist, 1) if sl_dist > 0 else rr
            new_entry_line = (
                "\n" + "━"*20 + "\n"
                "⚡ <b>NIVEAUX AJUSTÉS (prix live)</b>\n"
                "  📍 Nouvelle entrée : <code>{}</code>\n".format(fmt_p.format(ne)) +
                "  ✅ TP (inchangé)   : <code>{}</code>\n".format(tp) +
                "  ❌ Nouveau SL      : <code>{}</code>\n".format(fmt_p.format(ns)) +
                "  📐 Nouveau RR      : <b>1:{}</b>".format(new_rr)
            )
        except Exception: pass

    ai_verdict  = ai_live.get("verdict", "?")
    ai_score    = ai_live.get("ai_score", 0)
    ai_proba    = ai_live.get("ai_proba", 0)
    ai_raison   = ai_live.get("raison", "Analyse indisponible")
    ai_risque   = ai_live.get("risque", "")
    ai_conseil  = ai_live.get("conseil_entree", "")
    ai_criteres = ai_live.get("criteres_ok", 0)
    ai_fond     = ai_live.get("biais_fondamental", "NEUTRE")
    ai_news     = ai_live.get("news_impact", "OK")
    ai_conf     = ai_live.get("confiance", "")
    ai_final    = ai_live.get("final_score", 0)

    v_ico  = "✅" if ai_verdict == "VALIDER" else ("❌" if ai_verdict == "REJETER" else "⚙️")
    f_ico  = {"ALIGNE": "✅", "NEUTRE": "➖", "CONTRE": "❌"}.get(ai_fond, "➖")
    n_ico  = {"OK": "✅", "PRUDENCE": "🟡", "BLOQUANT": "🔴"}.get(ai_news, "✅")
    c_ico  = {"FAIBLE": "🔴", "MOYENNE": "🟡", "HAUTE": "🟢", "EXCEPTIONNELLE": "🏆"}.get(ai_conf, "⚪")
    bar    = "█" * int(ai_score) + "░" * (10 - int(ai_score))

    age_str = ""
    if sig.get("_ts"):
        try:
            sent_dt = datetime.fromisoformat(sig["_ts"].replace("Z", "+00:00"))
            age_min = round((datetime.now(timezone.utc) - sent_dt).total_seconds() / 60, 0)
            age_str = "  ⏱️ Âge signal : <b>{}min</b>\n".format(int(age_min))
        except Exception: pass

    msg = (
        "🔍 <b>ANALYSE LIVE — {}</b>\n".format(pair) +
        "═"*22 + "\n\n"
        "{} {} <b>{}</b>  ·  Score : <b>{}/100</b>\n".format(d, sf, pair, sc) +
        age_str +
        "  💹 Prix actuel : <code>{}</code>\n".format(cur_fmt) +
        "  {}\n".format(status_msg) +
        "\n" + "━"*20 + "\n"
        "🤖 <b>RE-ANALYSE CLAUDE IA</b>  {}\n\n".format(v_ico) +
        "🎯 Score IA    : <b>{}/10</b>  [{}]\n".format(ai_score, bar) +
        "📊 Probabilité : <b>{}%</b>  ·  Hybride : <b>{}/100</b>\n".format(ai_proba, ai_final) +
        "🏅 Confiance   : {} <b>{}</b>\n".format(c_ico, ai_conf) +
        "✅ Critères    : <b>{}/16</b>  (12 tech + 4 macro)\n".format(ai_criteres) +
        "{} Macro       : <b>{}</b>  ·  {} News : <b>{}</b>\n\n".format(f_ico, ai_fond, n_ico, ai_news) +
        "💡 <b>Verdict :</b> <i>{}</i>\n".format(ai_raison) +
        ("⚠️ <b>Risque :</b> <i>{}</i>\n".format(ai_risque) if ai_risque else "") +
        ("📌 <b>Conseil :</b> <i>{}</i>\n".format(ai_conseil) if ai_conseil else "") +
        new_entry_line +
        "\n" + "═"*22 + "\n"
        "⚠️ Not financial advice · @leaderodg_bot"
    )

    tg_send(uid, msg, kb=kb_refresh)


def dispatch_cb(cb):
    """Gère tous les boutons inline Telegram."""
    uid   = cb["from"]["id"]
    uname = cb.get("from", {}).get("username", "")
    data  = cb.get("data", "")
    mid   = cb.get("message", {}).get("message_id")   # pour tg_edit (recheck)
    # Répondre immédiatement à Telegram (évite le spinner bloqué)
    try: tg_req("answerCallbackQuery", {"callback_query_id": cb["id"]})
    except: pass
    db_register(uid, uname)

    # ── PaymentManager intercepte les callbacks paiement EN PREMIER ──
    if _PM_AVAILABLE:
        try:
            if _PM.process_callback(uid, uname, data):
                return  # callback consommé par PaymentManager
        except Exception as _e:
            log("WARN", "PM.process_callback: {}".format(_e))

    # ── Vérification validité signal (bouton signal) ─────────────────
    if data.startswith("recheck_") or data.startswith("check_sig_"):
        key_part = data.replace("recheck_","",1).replace("check_sig_","",1)
        threading.Thread(target=handle_check_signal,
                         args=(uid, key_part), daemon=True).start()
        return

    # ── Navigation principale ─────────────────────────────────
    if   data == "start":   send_welcome(uid, uname)
    elif data == "signals": threading.Thread(target=send_signals_info, args=(uid,), daemon=True).start()
    elif data == "account": threading.Thread(target=send_account, args=(uid, uname), daemon=True).start()
    elif data == "rapports":threading.Thread(target=send_rapports, args=(uid,), daemon=True).start()
    elif data.startswith("hist_full_"):
        try:
            pg = int(data.split("_")[-1])
        except Exception:
            pg = 1
        threading.Thread(target=send_history_full, args=(uid, pg), daemon=True).start()
    elif data == "pro":     threading.Thread(target=send_pro_page, args=(uid,), daemon=True).start()
    elif data == "pay":     send_pay_plan(uid)
    elif data == "ref":     threading.Thread(target=send_affilie, args=(uid, uname), daemon=True).start()
    elif data == "broker":  send_broker(uid)
    elif data == "guide":   threading.Thread(target=send_guide, args=(uid,), daemon=True).start()
    elif data == "gains":   threading.Thread(target=send_gains, args=(uid,), daemon=True).start()
    elif data == "groupe":
        # Envoyer le lien groupe selon le plan
        p = is_pro(uid)
        inv_msg, inv_kb = _group_invite_msg(p)
        tg_send(uid, inv_msg, kb=inv_kb)

    # ── Paiement ─────────────────────────────────────────────
    elif data == "pay_submitted":
        handle_pay_submitted(uid, uname)
    elif data.startswith("pay_submitted_"):
        handle_pay_submitted(uid, uname, plan_key=data.replace("pay_submitted_",""))
    elif data.startswith("pay_plan_"):
        send_pay_plan(uid, plan_key=data.replace("pay_plan_",""))
    elif data == "pay_confirm":
        threading.Thread(target=handle_pay_confirm, args=(uid, uname), daemon=True).start()
    elif data == "pay_cancel":
        _pay_state.pop(uid, None)
        tg_send(uid, "❌ Paiement annulé.", kb=kb_back())

    # ── Parrainage ────────────────────────────────────────────
    elif data == "ref_stats":
        refs = db_get_refs(uid)
        link = "https://t.me/{}?start={}".format(BOT_USER, uid)
        done = min(refs, REF_TARGET)
        bar  = "█"*int(done/REF_TARGET*10) + "░"*(10-int(done/REF_TARGET*10))
        tg_send(uid,
            "🤝 <b>MES FILLEULS</b>\n" + "═"*22 + "\n\n"
            "🔗 <code>{}</code>\n\n"
            "<b>{}/{}</b>  ({}%)\n[{}]\n\n"
            "🏆 {} filleuls = {} MOIS PRO".format(
                link, done, REF_TARGET, int(done/REF_TARGET*100), bar,
                REF_TARGET, REF_MONTHS),
            kb=kb_back())

    # ── Admin ─────────────────────────────────────────────────
    elif data == "adm_panel" and uid == ADMIN_ID:
        threading.Thread(target=send_admin_full, args=(uid,), daemon=True).start()
    elif data == "adm_activateall" and uid == ADMIN_ID:
        tg_send(uid, "⚠️ <b>Confirmes-tu l'activation PRO pour TOUS les membres FREE ?</b>",
            kb={"inline_keyboard": [
                [{"text": "✅ OUI — Activer TOUS", "callback_data": "adm_activateall_confirm"}],
                [{"text": "❌ Annuler",              "callback_data": "adm_panel"}],
            ]})
    elif data == "adm_activateall_confirm" and uid == ADMIN_ID:
        threading.Thread(target=_handle_activateall, args=(uid,), daemon=True).start()
    elif data == "adm_stats" and uid == ADMIN_ID:
        threading.Thread(target=send_admin_stats_full, args=(uid,), daemon=True).start()
    elif data == "adm_pays" and uid == ADMIN_ID:
        threading.Thread(target=send_admin_payments_full, args=(uid,), daemon=True).start()
    elif data == "adm_scan" and uid == ADMIN_ID:
        tg_send(uid, "📡 Scan forcé...", kb=kb_admin_back())
        threading.Thread(target=scan_and_send, daemon=True).start()
    elif data == "adm_rapports" and uid == ADMIN_ID:
        threading.Thread(target=send_rapports, args=(uid,), daemon=True).start()
    elif data == "adm_reco" and uid == ADMIN_ID:
        threading.Thread(target=send_admin_reco, args=(uid,), daemon=True).start()
    elif data == "adm_debug" and uid == ADMIN_ID:
        if not _last_results:
            tg_send(uid, "Aucun scan encore."); return
        lines = ["🔍 <b>DEBUG DERNIER SCAN</b>", ""]
        found = [r for r in _last_results if r.get("found")]
        nf    = [r for r in _last_results if not r.get("found")]
        if found:
            lines.append("✅ <b>SIGNAUX ({}):</b>".format(len(found)))
            for r in found:
                s = r["signal"]
                lines.append("  🟢 {} {}  RR 1:{}  Score {}{}".format(
                    r["name"], s["side"], s["rr"], s["score"],
                    ""))
        reasons = {}
        for r in nf: reasons.setdefault(r.get("reason","?"), []).append(r["name"])
        lines.append("\n⚪ <b>REJETÉS ({}):</b>".format(len(nf)))
        for reason, names in sorted(reasons.items(), key=lambda x: -len(x[1])):
            lines.append("  <b>{}</b> ({}): {}".format(reason, len(names), ", ".join(names[:5])))
        tg_send(uid, "\n".join(lines))
    elif data == "adm_marches" and uid == ADMIN_ID:
        threading.Thread(target=handle_marches_full, args=(uid,), daemon=True).start()
    elif data == "adm_promo_list" and uid == ADMIN_ID:
        threading.Thread(target=send_promo_list, args=(uid,), daemon=True).start()
    elif data.startswith("adm_promo_send_") and uid == ADMIN_ID:
        pid = data.replace("adm_promo_send_", "")
        threading.Thread(target=broadcast_promo, args=(uid, pid), daemon=True).start()
    elif data.startswith("adm_promo_") and uid == ADMIN_ID:
        pid = data.replace("adm_promo_", "")
        threading.Thread(target=send_promo_preview, args=(uid, pid), daemon=True).start()
    elif data == "adm_bcast_all" and uid == ADMIN_ID:
        handle_bcast_start(uid, "ALL")
    elif data == "adm_bcast_pro" and uid == ADMIN_ID:
        handle_bcast_start(uid, "PRO")
    elif data.startswith("adm_membres_") and uid == ADMIN_ID:
        pg = int(data.split("_")[-1])
        threading.Thread(target=handle_membres, args=(uid, pg), daemon=True).start()

    # ── Toggle PRO/FREE admin ─────────────────────────────────
    elif data.startswith("adm_pro_") and uid == ADMIN_ID:
        try:
            t_uid = int(data.split("_")[2])
            plan, _, _ = db_get_pro_info(t_uid)
            if plan != "PRO":
                db_activate_pro(t_uid, "ADMIN", days=None)
                tg_send(t_uid,
                    "🎉 <b>PRO activé !</b>\n\n"
                    "✅ Max {} signaux/jour\n"
                    "⚡  inclus\n"
                    "🚀 Bienvenue dans AlphaBot PRO !".format(PRO_LIMIT))
                tg_send(uid, "✅ PRO activé : <code>{}</code>".format(t_uid),
                    kb={"inline_keyboard": [[
                        {"text": "🔒 Désactiver PRO",
                         "callback_data": "adm_ban_{}".format(t_uid)}]]})
            else:
                tg_send(uid, "ℹ️ Déjà PRO : <code>{}</code>".format(t_uid))
        except Exception as ex:
            tg_send(uid, "❌ {}".format(ex))

    elif data.startswith("adm_ban_") and uid == ADMIN_ID:
        try:
            t_uid = int(data.split("_")[2])
            plan, _, _ = db_get_pro_info(t_uid)
            if plan == "PRO":
                db_downgrade_pro(t_uid)
                tg_send(t_uid,
                    "🔒 <b>PRO désactivé</b>\n"
                    "Plan : FREE ({} signaux/jour)\n"
                    "/pay pour revenir PRO.".format(FREE_LIMIT))
                tg_send(uid, "✅ FREE : <code>{}</code>".format(t_uid),
                    kb={"inline_keyboard": [[
                        {"text": "🔄 Réactiver PRO",
                         "callback_data": "adm_pro_{}".format(t_uid)}]]})
            else:
                # Refuser paiement
                db_run("UPDATE payments SET status='REJECTED' WHERE user_id=? AND status='PENDING'", (t_uid,))
                tg_send(uid, "❌ Paiement refusé : <code>{}</code>".format(t_uid))
        except Exception as ex:
            tg_send(uid, "❌ {}".format(ex))

    # ── Fallback ──────────────────────────────────────────────
    else:
        send_welcome(uid, uname)


def track_user(uid, uname, first_name=""):
    """
    Appelé à chaque interaction — enregistre l'utilisateur s'il est nouveau
    et notifie l'admin à la première apparition.
    Utilisé pour NE JAMAIS perdre un utilisateur même si la DB a été resetée.
    """
    try:
        # Vérifier si déjà connu AVANT db_register
        existing = db_one("SELECT user_id FROM users WHERE user_id=?", (uid,))
        is_new = existing is None
        # Enregistrer (INSERT OR IGNORE + update last_seen)
        db_register(uid, uname)
        db_run("UPDATE users SET last_seen=? WHERE user_id=?",
               (datetime.now().isoformat(), uid))
        if uname:
            db_run("UPDATE users SET username=? WHERE user_id=?", (uname, uid))
        # Notification admin uniquement pour les nouveaux
        if is_new and uid != ADMIN_ID:
            total, pro, sigs, _, _ = global_stats()
            name_disp = "@" + uname if uname else (first_name or "Inconnu")
            msg_admin = (
                "🆕 <b>NOUVEL UTILISATEUR</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                "🆔 ID       : <code>{}</code>\n"
                "👤 Username : {}\n"
                "📋 Prenom   : {}\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                "👥 Total DB : <b>{}</b>  (PRO: <b>{}</b>)\n"
                "📡 Signaux  : {}"
            ).format(uid, "@" + uname if uname else "s/u",
                     first_name or "s/p", total, pro, sigs)
            tg_send(ADMIN_ID, msg_admin,
                kb={"inline_keyboard": [[
                    {"text": "Activer PRO", "callback_data": "adm_pro_{}".format(uid)},
                    {"text": "Contacter",   "url": "tg://user?id={}".format(uid)},
                ]]})
            log("INFO", clr("Nouveau user: {} ID:{} — notif admin OK".format(
                name_disp, uid), "g"))
    except Exception as e:
        log("WARN", "track_user {}: {}".format(uid, e))


def handle_new_group_member(uid, uname, first_name):
    """
    Nouveau membre rejoint le groupe :
    1. Enregistrement en base (via track_user)
    2. Message de bienvenue + essai PRO
    3. Invitation groupe VIP
    4. Notification admin (via track_user)
    """
    try:
        track_user(uid, uname, first_name)  # enregistrement + notif admin
        name = "@" + uname if uname else first_name or "Trader"

        # ── Message de bienvenue ────────────────────────────────
        tg_send(uid,
            "👋 <b>Bienvenue {} !</b>\n\n"
            "🤖 <b>AlphaBot PRO</b> — Signaux trading automatiques\n\n"
            "✅ {} signaux/jour GRATUITS\n"
            "📊 Forex · Or · BTC · Indices · Pétrole\n"
            "🎯 Entrée + TP + SL automatiques\n"
            "⚡  actif\n\n"
            "🎁 <b>Essai PRO {} jours offert !</b>\n\n"
            "👉 Clique /start pour commencer".format(
                name, FREE_LIMIT, TRIAL_DAYS),
            kb={{"inline_keyboard": [[
                {{"text": "🚀 Démarrer", "callback_data": "start"}},
                {{"text": "💎 Voir PRO",  "callback_data": "pro"}},
            ]]}})

        # ── Recommandation groupe VIP ───────────────────────────
        time.sleep(2)
        try:
            vip_link = "https://t.me/+{}".format(
                VIP_CH.lstrip("-100") if VIP_CH.startswith("-100") else VIP_CH.lstrip("-"))
        except:
            vip_link = "https://t.me/leaderOdg"
        tg_send(uid,
            "🏆 <b>GROUPE VIP AlphaBot</b>\n\n"
            "Rejoins notre groupe VIP pour :\n"
            "✅ Signaux en temps réel\n"
            "✅ Analyses de marché en direct\n"
            "✅ Discussion avec @leaderOdg\n\n"
            "❓ Questions sur la méthode ICT/SMC ?\n"
            "👉 Contacte directement @leaderOdg\n\n"
            "📩 Demande d'accès au groupe VIP :",
            kb={{"inline_keyboard": [[
                {{"text": "👑 Rejoindre le groupe VIP",
                  "url": "https://t.me/leaderOdg"}},
            ]]}})

        # ── Notification admin ──────────────────────────────────
        total, pro, _, _, _ = global_stats()
        tg_send(ADMIN_ID,
            "👤 <b>NOUVEAU MEMBRE</b>\n\n"
            "🆔 ID     : <code>{}</code>\n"
            "👤 Username: {}\n"
            "📋 Prénom  : {}\n\n"
            "👥 Total membres : <b>{}</b>  (PRO: {})\n\n"
            "Actions rapides ↓".format(
                uid,
                "@" + uname if uname else "—",
                first_name or "—",
                total, pro),
            kb={{"inline_keyboard": [[
                {{"text": "💠 Activer PRO",
                  "callback_data": "adm_pro_{}".format(uid)}},
                {{"text": "💬 Contacter",
                  "url": "tg://user?id={}".format(uid)}},
            ]]}})

        log("INFO", clr("Nouveau membre: @{} ID:{} — notif admin envoyée".format(
            uname or "?", uid), "g"))
    except Exception as e:
        log("WARN", "handle_new_group_member: {}".format(e))

def process_update(upd):
    try:
        # ── Nouveau membre dans le groupe ────────────────────────
        if "chat_member" in upd:
            cm = upd["chat_member"]
            new_m = cm.get("new_chat_member", {})
            status = new_m.get("status", "")
            user = new_m.get("user", {})
            if status == "member" and not user.get("is_bot"):
                uid   = user["id"]
                uname = user.get("username", "")
                fname = user.get("first_name", "")
                threading.Thread(target=handle_new_group_member,
                    args=(uid, uname, fname), daemon=True).start()
            return

        # ── Nouveau membre via message system (ancienne API) ─────
        if "message" in upd:
            msg = upd["message"]
            new_members = msg.get("new_chat_members", [])
            if new_members:
                for user in new_members:
                    if not user.get("is_bot"):
                        uid   = user["id"]
                        uname = user.get("username", "")
                        fname = user.get("first_name", "")
                        threading.Thread(target=handle_new_group_member,
                            args=(uid, uname, fname), daemon=True).start()
                return

            uid   = msg["from"]["id"]
            uname = msg.get("from", {}).get("username", "")
            fname = msg.get("from", {}).get("first_name", "")
            txt   = msg.get("text", "")
            # ── Tracker TOUT utilisateur qui envoie un message ───
            threading.Thread(target=track_user, args=(uid, uname, fname), daemon=True).start()
            if txt:
                log("INFO", clr("MSG @{} ({}): {}".format(uname or uid, uid, txt[:40]), "d"))
                def _h(uid=uid, uname=uname, txt=txt):
                    # ── PaymentManager intercepte EN PREMIER ──────────────
                    if _PM_AVAILABLE:
                        try:
                            if _PM.process_text(uid, uname, txt):
                                return  # message consommé par PaymentManager
                        except Exception as _e:
                            log("WARN", "PM.process_text: {}".format(_e))
                    # ── Fallback : logique paiement existante ─────────────
                    if uid in _pay_state and _pay_state[uid].get("step") == "waiting":
                        cleaned = txt.strip()
                        if len(cleaned) >= 20 and not cleaned.startswith("/"):
                            handle_proof(uid, uname, tx=cleaned)
                        else:
                            dispatch(uid, uname, txt)
                    else:
                        dispatch(uid, uname, txt)
                threading.Thread(target=_h, daemon=True).start()
        elif "callback_query" in upd:
            cb = upd["callback_query"]
            cb_uid   = cb.get("from", {}).get("id")
            cb_uname = cb.get("from", {}).get("username", "")
            cb_fname = cb.get("from", {}).get("first_name", "")
            if cb_uid:
                # Tracker aussi les callbacks (clics sur boutons)
                threading.Thread(target=track_user,
                                 args=(cb_uid, cb_uname, cb_fname), daemon=True).start()
            threading.Thread(target=dispatch_cb, args=(cb,), daemon=True).start()
    except Exception as e: log("ERR","process_update: {}".format(e))


# ══════════════════════════════════════════════════════════════════
#  PANEL ADMIN FLASK — Port 5001 (patch auto, séparé du webhook)
# ══════════════════════════════════════════════════════════════════

_ADMIN_PANEL_HTML = None
_flask_secret = os.getenv("SECRET_KEY", "ab10-secret-" + str(ADMIN_ID))
_ADMIN_WEB_USER = os.getenv("ADMIN_WEB_USER", "admin")
_ADMIN_WEB_PASS = os.getenv("ADMIN_WEB_PASS", "AlphaBot2024!")
_FLASK_PORT     = int(os.getenv("FLASK_PORT", "5001"))

def _load_admin_html():
    global _ADMIN_PANEL_HTML
    try:
        with open("admin_panel.html", encoding="utf-8") as f:
            _ADMIN_PANEL_HTML = f.read()
    except FileNotFoundError:
        _ADMIN_PANEL_HTML = "<h2>admin_panel.html manquant.</h2>"

def _require_login_flask(f):
    from functools import wraps
    @wraps(f)
    def decorated(*args, **kwargs):
        if not _session.get("ok"):
            return _redirect(_url_for("fl_login"))
        return f(*args, **kwargs)
    return decorated

def _start_flask_admin():
    if not _FLASK_OK: return
    _load_admin_html()
    fl = _Flask("alphabot_admin")
    fl.secret_key = _flask_secret

    @fl.route("/")
    def fl_index():
        return _jsonify({"status":"AlphaBot v10 actif","port":_FLASK_PORT})

    @fl.route("/ping")
    def fl_ping():
        return "pong", 200

    @fl.route("/login", methods=["GET","POST"])
    def fl_login():
        err = ""
        if _request.method == "POST":
            if (_request.form.get("u") == _ADMIN_WEB_USER and
                    _request.form.get("p") == _ADMIN_WEB_PASS):
                _session["ok"] = True
                return _redirect(_url_for("fl_admin"))
            err = "Identifiants incorrects."
        html = """<!DOCTYPE html><html><head><meta charset=UTF-8>
<title>AlphaBot Login</title>
<style>body{background:#0a0a0f;display:flex;align-items:center;justify-content:center;
min-height:100vh;font-family:sans-serif;margin:0}
.card{background:#14141f;border:1px solid rgba(255,255,255,.08);border-radius:16px;
padding:40px 32px;width:320px}
h1{color:#fff;font-size:20px;margin-bottom:24px;text-align:center}
input{width:100%;background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.1);
border-radius:8px;padding:11px 14px;color:#fff;font-size:14px;box-sizing:border-box;
margin-bottom:12px;outline:none}
button{width:100%;background:#3d7eff;border:none;border-radius:8px;padding:12px;
color:#fff;font-size:14px;cursor:pointer;font-weight:700}
.err{color:#fca5a5;font-size:13px;text-align:center;margin-top:8px}
</style></head><body><div class="card">
<h1>🤖 AlphaBot Admin</h1>
<form method=POST>
<input name=u placeholder=Identifiant required>
<input name=p type=password placeholder='Mot de passe' required>
<button>🔐 Connexion</button>
</form>""" + (f'<p class=err>{err}</p>' if err else '') + "</div></body></html>"
        return html

    @fl.route("/logout")
    def fl_logout():
        _session.clear(); return _redirect(_url_for("fl_login"))

    @fl.route("/admin")
    @_require_login_flask
    def fl_admin():
        return _ADMIN_PANEL_HTML or "<h2>Panel en cours de chargement…</h2>"

    @fl.route("/api/stats")
    @_require_login_flask
    def fl_stats():
        try:
            total, pro, sigs, pays, g1d = global_stats()
            st = db_daily_stats()
            sn, sm, sl_l, wknd = get_session()
            return _jsonify({
                "total": total, "pro": pro, "free": total-pro,
                "sigs": sigs, "pays": pays, "revenue": round(g1d, 2),
                "new_today": 0,
                "pay_pending": len(pending_pays()),
                "activations": pays,
                "signals_sent": _cycles_no_signal,
                "last_scan": datetime.now().strftime("%H:%M"),
                "uptime_since": datetime.now().isoformat(),
                "bot_running": True,
            })
        except Exception as e:
            return _jsonify({"error": str(e)})

    @fl.route("/api/users")
    @_require_login_flask
    def fl_users():
        try:
            page   = int(_request.args.get("page", 1))
            q      = _request.args.get("q", "")
            fltr   = _request.args.get("filter", "ALL")
            limit  = 20
            offset = (page - 1) * limit
            con = _conn(); cur = con.cursor()
            conds = []; args = []
            if q:
                conds.append("(username LIKE ? OR CAST(user_id AS TEXT) LIKE ?)")
                args += [f"%{q}%", f"%{q}%"]
            if fltr == "PRO":
                conds.append("plan='PRO'")
            elif fltr == "FREE":
                conds.append("plan='FREE'")
            where = ("WHERE " + " AND ".join(conds)) if conds else ""
            cur.execute(f"SELECT COUNT(*) FROM users {where}", args)
            total = cur.fetchone()[0]
            cur.execute(
                f"SELECT user_id,username,plan,pro_source,joined,pro_expires "
                f"FROM users {where} ORDER BY joined DESC LIMIT ? OFFSET ?",
                args + [limit, offset])
            rows = [{"user_id":r[0],"username":r[1],"status":r[2],
                     "pro_since":r[3],"join_date":r[4],"pro_expires":r[5]}
                    for r in cur.fetchall()]
            con.close()
            return _jsonify({"users": rows, "total": total, "page": page})
        except Exception as e:
            return _jsonify({"users":[],"total":0,"page":1,"error":str(e)})

    @fl.route("/api/activate", methods=["POST"])
    @_require_login_flask
    def fl_activate():
        try:
            data = _request.get_json()
            uid  = int(data.get("user_id", 0))
            plan = data.get("plan", "PRO")
            days_map = {"PRO": None, "MONTH": 30, "VIP": None, "TRIAL": 3}
            days = days_map.get(plan)
            db_pro(uid, f"ADMIN_WEB_{plan}", days=days)
            tg_send(uid,
                f"💎 <b>Compte {plan} activé !</b>\n\n"
                f"Ton accès a été activé par l'admin.\n"
                f"🤖 AlphaBot PRO · @{BOT_USER}")
            return _jsonify({"ok": True, "user_id": uid, "plan": plan})
        except Exception as e:
            return _jsonify({"ok": False, "error": str(e)})

    @fl.route("/api/degrade", methods=["POST"])
    @_require_login_flask
    def fl_degrade():
        try:
            data = _request.get_json()
            uid  = int(data.get("user_id", 0))
            db_free(uid)
            tg_send(uid,
                f"ℹ️ Compte rétrogradé en FREE.\n"
                f"Tape /pay pour renouveler.\n🤖 AlphaBot")
            return _jsonify({"ok": True, "user_id": uid})
        except Exception as e:
            return _jsonify({"ok": False, "error": str(e)})

    @fl.route("/api/broadcast/text", methods=["POST"])
    @_require_login_flask
    def fl_bcast_text():
        try:
            data    = _request.get_json()
            msg_txt = data.get("message", "").strip()
            target  = data.get("target", "ALL")
            if not msg_txt:
                return _jsonify({"ok": False, "error": "Message vide"})
            uids = pro_users() if target == "PRO" else (pro_users() + free_users())
            sent = failed = 0
            for u in uids:
                try:
                    r = tg_send(u, f"📢 <b>Message AlphaBot</b>\n\n{msg_txt}")
                    if r.get("ok"): sent += 1
                    else: failed += 1
                except: failed += 1
                time.sleep(0.05)
            return _jsonify({"ok": True, "sent": sent, "failed": failed})
        except Exception as e:
            return _jsonify({"ok": False, "error": str(e)})

    @fl.route("/api/payments/pending")
    @_require_login_flask
    def fl_pay_pending():
        try:
            pend = pending_pays()
            rows = [{"id":r[0],"user_id":r[1],"username":r[2],
                     "tx_hash":r[3],"plan_key":"PRO","amount_exp":PRO_PRICE,
                     "amount_rcv":0,"status":"PENDING","created_at":r[4]}
                    for r in pend]
            return _jsonify({"payments": rows})
        except Exception as e:
            return _jsonify({"payments": [], "error": str(e)})

    @fl.route("/api/payments/history")
    @_require_login_flask
    def fl_pay_history():
        try:
            rows_db = db_all(
                "SELECT id,user_id,amount,tx_hash,status,created "
                "FROM payments WHERE status='CONFIRMED' "
                "ORDER BY created DESC LIMIT 50")
            rows = [{"id":r[0],"user_id":r[1],"amount_rcv":r[2],
                     "tx_hash":r[3],"status":r[4],"activated_at":r[5],
                     "plan_key":"PRO","username":""}
                    for r in rows_db]
            return _jsonify({"history": rows})
        except Exception as e:
            return _jsonify({"history": [], "error": str(e)})

    @fl.route("/api/broadcasts")
    @_require_login_flask
    def fl_broadcasts():
        # Pas de table broadcasts dans main_v10 → retourner vide
        return _jsonify({"broadcasts": []})

    @fl.route("/api/bot/status")
    @_require_login_flask
    def fl_bot_status():
        return _jsonify({
            "running": True,
            "last_scan": datetime.now().strftime("%H:%M"),
            "signals_sent": _cycles_no_signal,
            "uptime_since": datetime.now().isoformat(),
        })

    import logging as _logging
    _logging.getLogger("werkzeug").setLevel(_logging.ERROR)

    fl.run(
        host="0.0.0.0",
        port=_FLASK_PORT,
        debug=False,
        use_reloader=False,
        threaded=True
    )

# ══════════════════════════════════════════════════════════════════
# ══════════════════════════════════════════════════════
#  DÉMARRAGE & MAIN
# ══════════════════════════════════════════════════════
def startup():
    print("\n"+clr("  ╔══════════════════════════════════════════════════╗","b","c"))
    print(clr("  ║  AlphaBot PRO v10 — IA Adaptative · ICT/SMC  ║","b","c"))
    print(clr("  ║  Forex·Métaux·Crypto·Indices · ICT/SMC · ⚡Mode   ║","b","c"))
    print(clr("  ╚══════════════════════════════════════════════════╝","b","c")+"\n")
    db_init()
    db_register(ADMIN_ID,"leaderOdg"); db_pro(ADMIN_ID,"ADMIN_AUTO",days=None)
    log("INFO",clr("Init données Binance...","c"))
    threading.Thread(target=refresh_exch,daemon=True).start()
    threading.Thread(target=refresh_ai,daemon=True).start()
    sn,sm,sl_l,wknd=get_session(); ch=chal_get()
    # Message de démarrage en arrière-plan — ne bloque pas le serveur HTTP
    def _notify():
        try:
            tg_send(ADMIN_ID,
                "🤖 <b>AlphaBot PRO v21 — DÉMARRÉ !</b>\n\n"
                "⚡ Scan toutes les <b>{}s</b> — signaux directs\n"
                "🕐 {}  🎯 Score min : <b>{}</b>\n"
                "{}\n"
                "🌍 Régime IA : <b>{}</b>\n"
                "📡 FREE {}/j  ·  PRO {}/j\n\n"
                "✅ Bot actif — répond aux commandes\n"
                "🛠 /admin pour le panel".format(
                    SCAN_SEC, sl_l, sm,
                    "🌍 <b>Week-end : crypto uniquement !</b>" if wknd else "📈 Session : {}".format(sl_l),
                    AI_REG.get("regime","Init"),
                    FREE_LIMIT, PRO_LIMIT),
                kb=kb_main(False))
        except Exception as e:
            log("WARN", "notify startup: {}".format(e))
    threading.Thread(target=_notify, daemon=True).start()
    # ── Initialiser le Payment Manager ─────────────────────────────
    if _PM_AVAILABLE:
        try:
            _PM.init({
                "tg_send"    : tg_send,
                "tg_sticker" : tg_sticker,
                "db_pro"     : lambda uid, src, days=None: db_pro(uid, src, days),
                "db_free"    : db_free,
                "admin_id"   : ADMIN_ID,
                "usdt_addr"  : USDT_ADDR,
                "pro_price"  : PRO_PRICE,
                "pro_limit"  : PRO_LIMIT,
                "bot_user"   : BOT_USER,
                "db_file"    : DB_FILE,
            })
            log("INFO", clr("PaymentManager PRO initialisé ✅", "b", "g"))
        except Exception as _e:
            log("WARN", "PM.init erreur: {}".format(_e))

    # ── Démarrer le panel admin Flask (port 5001) ────────────────
    if _FLASK_OK:
        try:
            _t = threading.Thread(target=_start_flask_admin, daemon=True, name="FlaskAdmin")
            _t.start()
            log("INFO", clr("Panel admin Flask démarré → port 5001", "b", "g"))
        except Exception as _e:
            log("WARN", "Flask admin erreur: {}".format(_e))

    log("INFO", clr("AlphaBot v10 actif", "b", "g")); return True

def make_wh():
    class WH(BaseHTTPRequestHandler):
        def do_POST(self):
            try:
                length = int(self.headers.get("Content-Length", 0))
                body   = self.rfile.read(length)
                # Répondre immédiatement 200 à Telegram
                self.send_response(200); self.end_headers(); self.wfile.write(b"OK")
                if length > 0:
                    upd = json.loads(body.decode("utf-8"))
                    threading.Thread(target=process_update, args=(upd,), daemon=True).start()
            except Exception as ex:
                log("ERR", "WebhookHandler: {}".format(ex))
                try: self.send_response(200); self.end_headers()
                except: pass
        def do_GET(self):
            path = self.path.split("?")[0]
            ch   = chal_get(); reg = AI_REG
            if path == "/health":
                # Endpoint dédié au keepalive — réponse JSON légère
                body = '{{"status":"ok","balance":{:.4f},"regime":"{}","cycles":{}}}'.format(
                    ch["balance"], reg.get("regime","?"), _cycles_no_signal).encode()
                self.send_response(200)
                self.send_header("Content-Type","application/json")
                self.end_headers(); self.wfile.write(body)
            else:
                self.send_response(200); self.end_headers()
                self.wfile.write(
                    "AlphaBot v10 OK | {:.4f}$ | {} | cycles: {}".format(
                        ch["balance"], reg.get("regime","?"), _cycles_no_signal).encode())
        def log_message(self, *a): pass
    return WH

def main():
    port = int(os.environ.get("PORT", 10000))
    render = os.environ.get("RENDER_EXTERNAL_URL", "")
    if render:
        # ── ÉTAPE 1 : DB init synchrone (OBLIGATOIRE avant tout) ─
        db_init()
        db_register(ADMIN_ID, "leaderOdg")
        db_pro(ADMIN_ID, "ADMIN_AUTO", days=None)
        log("INFO", clr("DB OK", "b", "g"))

        # ── ÉTAPE 2 : Ouvrir le port HTTP (Render détecte ici) ───
        server = HTTPServer(("0.0.0.0", port), make_wh())
        log("INFO", clr("Port {} ouvert — Render OK".format(port), "b", "g"))

        # ── ÉTAPE 3 : Telegram + IA en arrière-plan ──────────────
        def _init_bg():
            # Binance data
            threading.Thread(target=refresh_exch, daemon=True).start()
            threading.Thread(target=refresh_ai, daemon=True).start()
            # Configurer le webhook
            tg_req("deleteWebhook", {"drop_pending_updates": "true"})
            time.sleep(1)
            r = tg_req("setWebhook", {
                "url": "{}/webhook".format(render.rstrip("/")),
                "drop_pending_updates": "true",
                "max_connections": 10,
                "allowed_updates": '["message","callback_query","chat_member","my_chat_member"]'
            })
            if r.get("ok"): log("INFO", clr("Webhook OK — Bot prêt!", "b", "g"))
            else: log("ERR", clr("Webhook échoué: {}".format(r), "red"))
            # Broadcast nouvelle version à tous les membres
            # broadcast_new_version désactivé — ne pas spammer au redémarrage
            # threading.Thread(target=broadcast_new_version, daemon=True).start()
            # Message de démarrage admin
            sn, sm, sl_l, wknd = get_session()
            sm_real = get_adaptive_score_min()
            ch = chal_get()
            ai_status = ("✅ Claude+Gemini" if CLAUDE_API_KEY and GEMINI_API_KEY else
                         "✅ Claude" if CLAUDE_API_KEY else
                         "✅ Gemini" if GEMINI_API_KEY else "⚠️ Sans IA")
            tg_send(ADMIN_ID,
                "🤖 <b>AlphaBot PRO v21 — EN LIGNE !</b>\n\n"
                "✅ DB initialisée\n"
                "✅ Port {} ouvert\n"
                "✅ Webhook configuré\n"
                "⚡ Scan toutes les <b>{}s</b> — signaux directs\n\n"
                "🕐 Session : <b>{}</b>  Score min : <b>{}</b>\n"
                "🌍 Régime IA : <b>{}</b>\n\n"
                "📡 FREE {}/j  ·  PRO {}/j\n"
                "🛠 /admin pour le panel".format(
                    port,
                    SCAN_SEC,
                    sl_l, sm_real,
                    AI_REG.get("regime", "Init"),
                    FREE_LIMIT, PRO_LIMIT),
                kb=kb_main(False))
        threading.Thread(target=_init_bg, daemon=True).start()
        state = {"ls": 0, "la": 0, "lc": 0}
        def _loop():
            while True:
                try:
                    now=time.time()
                    if now-state["ls"]>=SCAN_SEC: state["ls"]=now; threading.Thread(target=scan_and_send,daemon=True).start()
                    if now-state["la"]>=300: state["la"]=now; threading.Thread(target=refresh_ai,daemon=True).start()
                    if now-state["lc"]>=15:  state["lc"]=now; threading.Thread(target=ai_check,daemon=True).start()
                except Exception as e: log("ERR","loop: {}".format(e))
                time.sleep(10)
        threading.Thread(target=_loop,daemon=True).start()
        def _ping():
            """Ping toutes les 5 min sur /health pour empêcher Render de dormir."""
            time.sleep(30)  # laisser le serveur démarrer
            while True:
                try:
                    url = render.rstrip("/") + "/health"
                    http_get(url, timeout=10)
                    log("INFO", clr("Keepalive /health OK", "d"))
                except Exception as e:
                    log("WARN", "Keepalive échoué: {}".format(e))
                time.sleep(5 * 60)  # toutes les 5 minutes
        threading.Thread(target=_ping, daemon=True).start()
        try: server.serve_forever()
        except KeyboardInterrupt: tg_send(ADMIN_ID,"🛑 Bot arrêté."); tg_req("deleteWebhook",{})
    else:
        log("INFO",clr("Mode polling local","y"))
        # DB init synchrone
        db_init()
        db_register(ADMIN_ID, "leaderOdg")
        db_pro(ADMIN_ID, "ADMIN_AUTO", days=None)
        threading.Thread(target=refresh_exch, daemon=True).start()
        threading.Thread(target=refresh_ai, daemon=True).start()
        tg_req("deleteWebhook",{"drop_pending_updates":"true"}); time.sleep(1)
        # Purge old updates
        offset=0
        for _ in range(20):
            batch=tg_req("getUpdates",{"offset":offset,"timeout":0,"limit":100}).get("result",[])
            if not batch: break
            offset=batch[-1]["update_id"]+1
        log("INFO", clr("Polling démarré (offset={})".format(offset), "g"))
        # Broadcast nouvelle version
        # broadcast_new_version désactivé
        # threading.Thread(target=broadcast_new_version, daemon=True).start()
        ls=la=lc=0
        while True:
            try:
                updates = tg_req("getUpdates", {
                    "offset": offset, "timeout": 10, "limit": 100,
                    "allowed_updates": '["message","callback_query","chat_member"]'
                }).get("result", [])
                for upd in updates:
                    offset=upd["update_id"]+1
                    threading.Thread(target=process_update,args=(upd,),daemon=True).start()
                now=time.time()
                if now-ls>=SCAN_SEC: ls=now; threading.Thread(target=scan_and_send,daemon=True).start()
                if now-la>=300: la=now; threading.Thread(target=refresh_ai,daemon=True).start()
                if now-lc>=15:  lc=now; threading.Thread(target=ai_check,daemon=True).start()
            except KeyboardInterrupt: tg_send(ADMIN_ID,"🛑 Bot arrêté."); break
            except Exception as e: log("ERR",str(e)); time.sleep(5)

if __name__=="__main__":
    main()
