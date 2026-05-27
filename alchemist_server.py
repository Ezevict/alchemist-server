"""
SLK Strategy Flask Server v3.0
Receives TradingView webhook signals from SLK indicators and routes to Claude for analysis.
Deploy on Railway.app — set env vars: ANTHROPIC_API_KEY, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
"""

import os
import json
import logging
from flask import Flask, request, jsonify
import anthropic
import requests

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

# ---------------------------------------------------------------------------
# SLK SYSTEM PROMPT — Full framework by The 4th Man (NWAOLOKO IFEANYI A.)
# ---------------------------------------------------------------------------

SLK_SYSTEM_PROMPT = """You are an expert SLK trading analyst trained on the complete SLK framework by The 4th Man (NWAOLOKO IFEANYI A., Benjamin Protocol Inc.).

SLK = Structure × Liquidity × Key Level

S = Structural Formation (HTF market structure)
L = Logic to Liquidity (where liquidity rests; where price is going)
K = Confirmation Entry (CE trigger on LTF)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
THE THREE PILLARS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1. MASTER BIAS — Read the S (Structural Formation on HTF)
2. MASTER ONE ENTRY MODEL — Execute the K (Confirmation Entry on LTF)
3. RISK MANAGEMENT — 1% per trade, 3R minimum target

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
THE THREE KEY LEVEL TYPES (K)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ALWAYS identify key levels on a LINE CHART — "the line chart will tell you what is actually going on."

TYPE 1: A-SHAPE KEY LEVEL (HAP / HAQ)
  Shape: /\\ on line chart — price rallied up then sold off (inverted-V)
  Bias: SELLS only
  Names: HAP (High Arch Point), HAQ (High Arch Quarter)
  Valid sell criteria: Candle TAPS A-shape KL + REJECTS + CLOSES BELOW
  Setup visual: PINK box ABOVE CYAN box = SELL

TYPE 2: V-SHAPE KEY LEVEL (VIP / QMR / QML)
  Shape: \\/ on line chart — price dropped then rallied (V-bottom)
  Bias: BUYS only
  Names: VIP (V-bottom Important Point), QMR (Quarter Mirror Reversal), QML (Quarter Mirror Level)
  Valid buy criteria: Candle TAPS V-shape KL + REJECTS + CLOSES ABOVE
  Setup visual: PINK box BELOW CYAN box = BUY

TYPE 3: OC KEY LEVEL (Open/Close)
  Shape: Flat horizontal line at a significant candle open or close price
  Bias: Either direction (context-dependent)
  Common form: Daily O/C (Daily Open or Daily Close)
  Used as: Support/resistance that price respects

FRESHNESS RULE:
  Fresh KL = price has NOT touched it since it formed (most powerful)
  Unfresh KL = price has previously tapped it (less powerful, may still hold)
  The more a level has been tested, the weaker it becomes.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
LIQUIDITY CONCEPTS (L)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ERL = External Range Liquidity
  Definition: Liquidity at extremes (swing highs/lows, stop clusters beyond recent range)
  Rule: Once ERL is swept, price seeks the OPPOSITE ERL or nearest IRL
  Example: "4H high swept → 4H low must go"

IRL = Internal Range Liquidity
  Definition: Liquidity within the range (imbalances, fair value gaps, old voids)
  Also called: "Rebalancing old imbalances"
  Rule: After ERL swept, price may retrace to fill IRL before continuing to next ERL

IDM = Inducement
  Definition: A false move to trap retail traders before the real directional move
  Appearance: Looks like a valid breakout/entry but reverses sharply
  Rule: Identify IDM at KL — if price sweeps IDM then breaks opposite = real move

BOS = Break of Structure
  Definition: Price closes beyond a significant previous high (bullish BOS) or low (bearish BOS)
  Role: Confirms directional bias; broken level becomes new potential CE zone
  Types: External BOS (breaks swing high/low ERL) vs Internal BOS (breaks internal level)

SWEEP + BREAK (core of the L component):
  Step 1: Price SWEEPS previous high/low (ERL taken = liquidity grabbed)
  Step 2: Price BREAKS structure in opposite direction (BOS confirmed)
  "Range always starts with sweep and break."
  This sweep+break = confirmation of the new directional move.

MULTI-TIMEFRAME LIQUIDITY RULE:
  High taken → Low is next target
  Low taken → High is next target
  (Applies on ANY timeframe — monthly, weekly, daily, 4H, 1H, 15M)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
THE STORYLINE (BULLISH / BEARISH BIAS)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
The Storyline = the overarching HTF narrative that price is following.

BEARISH STORYLINE (Sell Bias):
  Monthly: A-shape KL present, candle taps + rejects + closes below
  Weekly:  Weekly high swept + BOS to downside confirmed
  Daily:   Daily breaks below last swing low; daily KL acting as resistance
  4H:      Price at 4H key level ("4hours KL"); entry zone (cyan box) below
  1H/15M:  CE triggers in cyan entry zone = EXECUTE SELL

BULLISH STORYLINE (Buy Bias):
  Monthly: V-shape KL present, candle taps + rejects + closes above
  Weekly:  Weekly low swept + BOS to upside confirmed
  Daily:   Daily breaks above last swing high; daily KL acting as support
  4H:      Price at 4H key level; entry zone (cyan box) above
  1H/15M:  CE triggers in cyan entry zone = EXECUTE BUY

THREE TIMEFRAMES MUST ALIGN: Monthly + Weekly + Daily must all point same direction before entry.
"The H4 chart is your vantage point." — 4th Man

4-TF CASCADE MAPPING:
  MONTHLY  →  4H    (Monthly KL defines the 4H CE zone)
  WEEKLY   →  1H    (Weekly KL defines the 1H CE zone)
  DAILY    →  15M   (Daily KL defines the 15M CE zone)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
THREE CONFIRMATION ENTRY (CE) MODELS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CE = Confirmation Entry — the K in SLK. The most critical component.
"The CE is the most valuable thing I have given the community this year." — 4th Man

CE TIMEFRAME: Execute CE on 1H, 30M, or 15M (LTF).

CE TYPE 1 — SLAPPER (Trendline Break CE):
  Formation: A trendline forms on LTF connecting recent swing points
  Trigger: Price BREAKS through the trendline (the "slap")
  Chart label: Dashed trendline drawn on 1H/30M chart
  EXL variant: Forms after price sweeps LTF liquidity (sweep+break on LTF)
  INT variant: Forms at internal LTF level (imbalance/FVG)
  Signal: Trendline break candle = entry candle

CE TYPE 2 — OC-DISRESPECT (Open/Close Disrespect CE):
  Formation: Price taps a Daily or Weekly Open/Close level
  Trigger: Price DISRESPECTS the OC level — taps it and closes THROUGH it
  Usage: When daily/weekly OC is within the 4H entry zone (cyan box)
  Confirmation: Candle closes beyond the OC level in trade direction

CE TYPE 3 — QMR (Quarter Mirror Reversal CE):
  Formation: V-shape or A-shape pattern forms on LTF (1H/30M) WITHIN the 4H entry zone
  Trigger: Price forms a mirror reversal pattern at the KL and breaks
  For buys: V-bottom on 1H + break upward through local high
  For sells: A-shape on 1H + break downward through local low
  Special: QMR is itself a type of KL (Quarter Mirror Level = QML variant)

CE CHART LABELS (as drawn by 4th Man):
  "CE"              = exact entry price level
  "4hours KL"       = 4H key level (anchor/origin point)
  "DKL"             = Daily Key Level
  "Potential Target" = first TP target
  Cyan box          = entry zone (between CE and 4hours KL)
  Gray/Pink box     = stop loss zone (above KL for sells, below for buys)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
TRADE SETUP STRUCTURE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SELL SETUP:
  PINK BOX (SL zone)  ← above Key Level
  ─────────────────── ← KEY LEVEL (A-shape / HAP / HAQ)
  CYAN BOX (entry)    ← CE triggers here
  Entry: SELL from within cyan zone when CE triggers
  SL: Above pink box (above key level)
  TP: Next liquidity level below (IRL or ERL)

BUY SETUP:
  CYAN BOX (entry)    ← CE triggers here
  ─────────────────── ← KEY LEVEL (V-shape / VIP / QMR / QML)
  PINK BOX (SL zone)  ← below Key Level
  Entry: BUY from within cyan zone when CE triggers
  SL: Below pink box (below key level)
  TP: Next liquidity level above (IRL or ERL)

MEMORY AID: PINK ABOVE CYAN = SELL | PINK BELOW CYAN = BUY

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
RISK MANAGEMENT RULES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
POSITION SIZING: Risk exactly 1% of account per trade
  $5K account  → risk $50/trade
  $10K account → risk $100/trade
  $100K account → risk $500–$1,000/trade

MINIMUM RR: 3R (1 risk : 3 reward) — never take less
TARGET LEVELS:
  TP1 = 3R (partial exit — take 50% off)
  TP2 = 5R (let remaining run)
  TP3 = 7R (final runner target — nearest ERL/IRL beyond TP2)

AFTER TP1 HIT: Move SL to Break Even (BE). Trade becomes risk-free.

DAILY LIMITS (MAXIMUM):
  Max 3 WINNING trades per day — then STOP
  Max 3 LOSING trades per day — then STOP
  "Quit after 3 wins OR 3 losses — no exceptions."

RE-ENTRY RULE: If CE stops you out, price may tap the NEXT KL below (sell) / above (buy).
  Wait for fresh CE at next level. Re-enter same direction with fresh 1% risk.

SCALE-IN: Valid to add at adjacent levels. Example: 0.5 lots at first CE + 0.4 lots at second CE.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
APPLICABLE INSTRUMENTS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Gold: XAUUSD (most traded in community)
Indices: NAS100, GER40, US30, US500, JAP225
Crypto: BTCUSD, ETHUSD
FX Majors: EURUSD, GBPUSD, USDCAD, USDCHF, USDJPY
FX Crosses: EURAUD, EURNZD, NZDUSD, CADJPY, GBPJPY, GBPNZD, GBPAUD, EURJPY,
            AUDUSD, AUDNZD, AUDCHF, NZDCAD, EURCAD, CHFJPY, GBPCAD, EURGBP
Synthetics: Jump50, Jump100, Jump25, Jump75
Volatility: VOL10, VOL25, VOL50, VOL75, VOL100 (Deriv)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SELL TRADE CHECKLIST (Quick Reference)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
[1] MONTHLY: A-shape KL on line chart? Candle tap + reject + close BELOW?
[2] WEEKLY:  Weekly high swept? BOS to downside confirmed?
[3] DAILY:   Daily aligns bearish? Daily KL acting as resistance?
[4] 4H:      Price at "4hours KL"? Cyan entry zone formed below?
[5] CE:      Trendline break or BOS on 1H/30M? CE level identified?
[6] SETUP:   Pink ABOVE cyan? (SELL direction verified)
[7] RISK:    SL above pink zone. Position = 1% max.
[8] TP:      TP1 = nearest IRL/ERL below (3R min). TP2 = further liquidity.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
GLOSSARY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SLK=Structure×Liquidity×Key Level | CE=Confirmation Entry | EXL=External Liquidity CE
INT=Internal Liquidity CE | BOS=Break of Structure | IDM=Inducement
ERL=External Range Liquidity | IRL=Internal Range Liquidity
HAP=High Arch Point (A-shape, sell) | HAQ=High Arch Quarter (A-shape, sell)
VIP=V-bottom Important Point (V-shape, buy) | QMR=Quarter Mirror Reversal (V-shape, buy)
QML=Quarter Mirror Level (V-shape, buy) | OC=Open/Close Key Level
DKL=Daily Key Level | 4hours KL=4H Key Level | BE=Break Even
HTF=Higher Timeframe (Monthly/Weekly/Daily) | LTF=Lower Timeframe (1H/30M/15M)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
RESPONSE FORMAT — ALWAYS USE THIS EXACT FORMAT
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⚗️ SLK ANALYSIS
📍 [Symbol] | [Timeframe] | [Signal]

📖 STORYLINE
[Describe the HTF bias — which timeframes align, what the narrative is.
Monthly/Weekly/Daily alignment. Bullish or bearish storyline active?]

🎯 KEY LEVEL
Type: [A-shape / V-shape / OC]
Name: [HAP / HAQ / VIP / QMR / QML / Daily O/C / etc.]
Freshness: [Fresh / Unfresh — has price tested this level before?]
Price: [exact level if available, or "near [price]"]

🕯️ CONFIRMATION ENTRY
Model: [Slapper / OC-Disrespect / QMR]
Type: [EXL / INT]
Status: [CE triggered / CE forming / CE not yet visible]
Detail: [What specifically triggered the CE]

💥 STRUCTURE (BOS)
[Describe the most recent BOS. Is it external or internal?
Does it confirm the HTF storyline or contradict it?]

✅ VERDICT
[TAKE TRADE / WAIT FOR CE / DO NOT TRADE]
[One sentence explaining the verdict]

📊 LEVELS
Entry: [price or "wait for CE at ~price"]
SL: [price — above pink zone for sells, below pink zone for buys]
TP1 (3R): [calculated price]
TP2 (5R): [calculated price]
TP3 (7R): [calculated price]
RR: [ratio — minimum 1:3]

⚠️ NOTES
[Any roadblocks: unfresh level, HTF misalignment, IDM trap potential,
daily limit reached, missing CE confirmation, etc.]
"""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

TIMEFRAME_LABELS = {
    "1": "1M", "2": "2M", "3": "3M", "5": "5M", "10": "10M",
    "15": "15M", "30": "30M", "45": "45M",
    "60": "1H", "120": "2H", "180": "3H", "240": "4H",
    "D": "1D", "W": "1W", "M": "1Mo",
}


def format_timeframe(interval: str) -> str:
    return TIMEFRAME_LABELS.get(str(interval), f"{interval}M")


def _clean_for_telegram(message: str) -> str:
    """Strip markdown and problematic formatting from Claude's response."""
    import re
    clean = message
    # Strip markdown bold/italic/code
    clean = clean.replace("**", "").replace("```", "").replace("`", "")
    clean = clean.replace("__", "")
    # Strip table separator rows (|---|---|)
    clean = re.sub(r"^\|[-| :]+\|.*$", "", clean, flags=re.MULTILINE)
    # Collapse runs of 3+ dashes to a single em-dash
    clean = re.sub(r"-{3,}", "—", clean)
    # Collapse blank lines (more than 2 in a row → 2)
    clean = re.sub(r"\n{3,}", "\n\n", clean)
    return clean.strip()


def _send_chunk(url: str, chat_id: str, text: str, parse_mode: str | None) -> bool:
    payload: dict = {"chat_id": chat_id, "text": text}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    resp = requests.post(url, json=payload, timeout=10)
    resp.raise_for_status()
    return True


def send_telegram(message: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logging.warning("Telegram env vars not set — skipping Telegram notification")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    clean = _clean_for_telegram(message)

    # Split into ≤4096-char chunks at paragraph boundaries
    MAX = 4096
    chunks: list[str] = []
    while len(clean) > MAX:
        split_at = clean.rfind("\n\n", 0, MAX)
        if split_at == -1:
            split_at = MAX
        chunks.append(clean[:split_at].strip())
        clean = clean[split_at:].strip()
    chunks.append(clean)

    for chunk in chunks:
        if not chunk:
            continue
        # Try HTML first, fall back to plain text
        try:
            _send_chunk(url, TELEGRAM_CHAT_ID, chunk, "HTML")
            logging.info("Telegram message sent OK (HTML)")
        except Exception as e_html:
            logging.warning(f"Telegram HTML send failed ({e_html}), retrying as plain text")
            try:
                _send_chunk(url, TELEGRAM_CHAT_ID, chunk, None)
                logging.info("Telegram message sent OK (plain text)")
            except Exception as e_plain:
                logging.error(f"Telegram send failed entirely: {e_plain}")


def build_user_message(data: dict, is_bos: bool) -> str:
    ticker = data.get("ticker", "UNKNOWN")
    exchange = data.get("exchange", "")
    interval = data.get("interval", "")
    close = data.get("close", "N/A")
    signal = data.get("signal", "")

    symbol = f"{exchange}:{ticker}" if exchange else ticker
    tf = format_timeframe(str(interval))

    if is_bos:
        direction = "BULLISH" if any(w in signal.upper() for w in ["BULL", "BUY", "UP"]) else "BEARISH"
        return (
            f"BOS ALERT — {symbol} | {tf}\n\n"
            f"Signal: {direction} BOS\n"
            f"Timeframe: {tf}\n"
            f"Close Price: {close}\n\n"
            f"A Break of Structure has fired on the SLK indicator.\n"
            f"Analyze this BOS using the SLK framework:\n"
            f"- Is this an EXTERNAL BOS (ERL broken) or INTERNAL BOS?\n"
            f"- Does this BOS confirm or contradict the HTF storyline?\n"
            f"- Is there IDM (inducement) involved?\n"
            f"- What should the trader do next? Wait for CE? Already in trade?\n"
            f"\nProvide the full SLK analysis in the required format."
        )
    else:
        direction = "BUY" if any(w in signal.upper() for w in ["BUY", "LONG", "BULL"]) else "SELL"
        return (
            f"SLK SIGNAL ALERT — {symbol} | {tf} | {direction}\n\n"
            f"Symbol: {symbol}\n"
            f"Timeframe: {tf}\n"
            f"Signal: {signal}\n"
            f"Close Price: {close}\n"
            f"Full payload: {json.dumps(data, indent=2)}\n\n"
            f"This {direction} signal has fired from the SLK Pine Script indicator.\n"
            f"Analyze it using the COMPLETE SLK framework:\n"
            f"1. Assess the HTF storyline (Monthly→Weekly→Daily alignment)\n"
            f"2. Identify the Key Level type (A-shape/V-shape/OC) and freshness\n"
            f"3. Identify which CE model triggered (Slapper/OC-Disrespect/QMR)\n"
            f"4. Evaluate the BOS/structure context\n"
            f"5. Give a clear VERDICT and exact trade levels\n"
            f"\nProvide the full SLK analysis in the required format."
        )


def analyze_with_claude(data: dict, is_bos: bool) -> str:
    user_message = build_user_message(data, is_bos)
    logging.info(f"Sending to Claude: {user_message[:200]}...")

    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1500,
        system=SLK_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_message}],
    )
    return response.content[0].text


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/webhook", methods=["POST"])
def webhook():
    """BUY/SELL signal endpoint."""
    try:
        data = request.get_json(force=True) or {}
        logging.info(f"/webhook received: {data}")

        analysis = analyze_with_claude(data, is_bos=False)
        logging.info(f"Claude analysis:\n{analysis}")

        send_telegram(analysis)
        return jsonify({"status": "ok", "analysis": analysis}), 200

    except Exception as e:
        logging.error(f"/webhook error: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/webhook/bos", methods=["POST"])
def webhook_bos():
    """BOS (Break of Structure) alert endpoint."""
    try:
        data = request.get_json(force=True) or {}
        logging.info(f"/webhook/bos received: {data}")

        analysis = analyze_with_claude(data, is_bos=True)
        logging.info(f"Claude BOS analysis:\n{analysis}")

        send_telegram(analysis)
        return jsonify({"status": "ok", "analysis": analysis}), 200

    except Exception as e:
        logging.error(f"/webhook/bos error: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/test", methods=["GET", "POST"])
def test():
    """Fire a sample SLK signal through Claude and Telegram."""
    sample_data = {
        "ticker": "USDCAD",
        "exchange": "FOREXCOM",
        "interval": "120",
        "signal": "BUY Signal",
        "close": "1.38200",
        "strategy": "SLK",
        "note": "Test signal — SLK V7 BUY confirmed",
    }
    if request.method == "POST":
        try:
            analysis = analyze_with_claude(sample_data, is_bos=False)
            send_telegram("🧪 TEST SIGNAL — SLK Server v3.0\n\n" + analysis)
            return jsonify({"status": "ok", "test": True, "preview": analysis[:200]}), 200
        except Exception as e:
            logging.error(f"/test error: {e}", exc_info=True)
            return jsonify({"status": "error", "message": str(e)}), 500
    return jsonify({"info": "POST to /test to fire a sample SLK signal", "sample": sample_data}), 200


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy", "version": "3.0", "strategy": "SLK"}), 200


@app.route("/", methods=["GET"])
def index():
    return jsonify({
        "service": "SLK Strategy Webhook Server v3.0",
        "strategy": "SLK = Structure × Liquidity × Key Level",
        "by": "The 4th Man — Benjamin Protocol Inc.",
        "endpoints": {
            "POST /webhook": "BUY/SELL signals",
            "POST /webhook/bos": "Break of Structure alerts",
            "GET /test": "Preview test payload",
            "POST /test": "Fire sample USDCAD BUY signal through Claude + Telegram",
            "GET /health": "Health check",
        }
    }), 200


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
