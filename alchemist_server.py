"""
╔══════════════════════════════════════════════════════════════╗
║     THE ALCHEMIST — Claude AI Trade Analysis Server          ║
║     MSNR x SMC x ICT Webhook → Claude → Telegram            ║
║     Based on "The Alchemist" by Yanu Emmanuel F.             ║
╚══════════════════════════════════════════════════════════════╝

HOW TO RUN:
  1. pip install flask anthropic requests python-dotenv
  2. Create a .env file with your keys (see below)
  3. python alchemist_server.py

.env FILE FORMAT:
  ANTHROPIC_API_KEY=sk-ant-...
  TELEGRAM_BOT_TOKEN=123456:ABC-...
  TELEGRAM_CHAT_ID=-100123456789
  WEBHOOK_SECRET=your_secret_string   (optional, for security)

DEPLOY TO RAILWAY / RENDER:
  - Push this file + requirements.txt to GitHub
  - Connect repo in Railway/Render
  - Add env vars in their dashboard
  - Use the public HTTPS URL as your TradingView webhook
"""

import os
import json
import hmac
import hashlib
import logging
from datetime import datetime
from flask import Flask, request, jsonify
import anthropic
import requests
from dotenv import load_dotenv

load_dotenv()

# ── CONFIG ────────────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY  = os.getenv("ANTHROPIC_API_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")
WEBHOOK_SECRET     = os.getenv("WEBHOOK_SECRET", "")   # optional HMAC check
PORT               = int(os.getenv("PORT", 5000))
DEBUG              = os.getenv("DEBUG", "false").lower() == "true"

# ── LOGGING ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.DEBUG if DEBUG else logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── FLASK + ANTHROPIC ─────────────────────────────────────────────────────────
app    = Flask(__name__)
client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)


# ═══════════════════════════════════════════════════════════════════════════════
# CLAUDE ANALYSIS — THE ALCHEMIST SYSTEM PROMPT
# ═══════════════════════════════════════════════════════════════════════════════

SYSTEM_PROMPT = """
You are The Alchemist — an elite trading analyst trained in the MSNR x SMC x ICT methodology 
developed by Yanu Emmanuel F. of Alchemy Traders Network.

YOUR FRAMEWORK:
─────────────────────────────────────────────────────
1. STORYLINE (HTF Direction):
   - Weekly = Main Direction (most important)
   - Daily  = Retracement/Roadblocks for Weekly
   - H4     = Confirmation/Roadblocks for Daily
   - H1     = Entry refinement (wick/gap candle must appear)
   - Rule: If Weekly is bullish BUT Daily is bearish, Weekly storyline CANNOT continue 
     until the Daily bearish storyline ends. Wait.

2. SNR LEVELS (Malaysian Method):
   - Draw from CLOSE to next OPEN only (body-to-body, ignore wicks)
   - Classic [V] = Support (bearish candle close → bullish candle open)
   - Classic [A] = Resistance (bullish candle close → bearish candle open)
   - GAP SNR = Open ≠ previous close (Hidden Zone on HTF = LTF Breakout)
   - Fresh SNR = NOT yet touched → strong, reliable, uncollected liquidity
   - Unfresh SNR = Already touched/wicked → weaker, skip unless flipped
   - RBS = Resistance Becomes Support (full body breakout above resistance)
   - SBR = Support Becomes Resistance (full body breakout below support)

3. REJECTION CANDLE (Price Rejection):
   - Long upper wick = price rejected higher prices (sell signal)
   - Long lower wick = price rejected lower prices (buy signal)
   - QUALITY: thick body closing in direction of rejection = more authoritative
   - Similar to Pin Bar but with a thicker, more decisive body

4. BREAKOUT (BOS — Break of Structure):
   - ONLY valid when price closes ABOVE/BELOW with FULL BODY (not wick)
   - External BOS only — internal BOS does not count
   - After BOS: WAIT for pullback to the right shoulder or QML before entry
   - 2TF Confirmation Rule: Tap HTF SNR with wick → go 2 timeframes lower for BOS

5. TRENDLINES (Marriage Concept):
   - Connect SNR [V] or [A] points (body-to-body, line chart method)
   - Entry only at POINT #3 of the trendline (not point #2)
   - Trendline + SNR intersecting = MARRIAGE = highest confluence
   - Cannot be used with GAP SNR (only Classic SNR)

6. KILLZONES (Best times to trade):
   - London: 07:00–10:00 GMT (08:00–11:00 WAT) ← PRIMARY
   - New York: 12:00–14:00 GMT (13:00–15:00 WAT) ← PRIMARY
   - Avoid Asian session for MSNR setups

7. COMPLETE ENTRY MODEL:
   Step 1: Price taps FRESH SNR level (wick touch, not body close)
   Step 2: Rejection candle forms (wick + thick body away from level)
   Step 3: One TF lower completes EXTERNAL BOS (full body close)
   Step 4: Enter at Trendline touch (confluence) or pullback to QML/right shoulder
   Step 5: SL = 1.5× ATR below/above entry  |  TP1 = 2× ATR  |  TP2 = next fresh SNR

8. RISK MANAGEMENT:
   - Only trade when ALL conditions are confirmed
   - No fresh level = No storyline = No trade
   - Risk 1–2% per trade
   - Partial close at TP1 (50%), trail SL to entry, let TP2 run
─────────────────────────────────────────────────────

RESPONSE FORMAT:
Always respond with a structured Telegram message using these exact sections:
⚗️ THE ALCHEMIST ANALYSIS
📍 [Symbol] | [Timeframe] | [Signal Direction]

📖 STORYLINE
[Weekly direction + Daily alignment status]

🎯 SNR LEVEL
[Type, freshness, price level, what it means]

🕯️ REJECTION
[Quality of rejection candle, wick/body ratio]

💥 STRUCTURE (BOS)
[External BOS status, confirmation level]

⏰ KILLZONE
[Current session, timing quality]

📐 TRENDLINE
[TL confluence status, marriage concept]

✅ VERDICT
[TAKE TRADE / WAIT / DO NOT TRADE]
[Reason in 1 sentence]

📊 LEVELS
Entry: [price]
SL:    [price] (1.5× ATR)
TP1:   [price] (2× ATR — partial close)
TP2:   [price] (3.5× ATR — next SNR)
RR:    [ratio]

⚠️ NOTES
[Any roadblocks, conflicting signals, or things to watch]

Keep responses concise, actionable, and in The Alchemist's voice: 
confident, precise, institutional-minded. Patience · Discipline · Fearless.
"""


def build_analysis_prompt(data: dict) -> str:
    """Build the user prompt from TradingView webhook data."""

    signal    = data.get("signal", "UNKNOWN")
    symbol    = data.get("symbol", "UNKNOWN")
    exchange  = data.get("exchange", "")
    tf        = data.get("timeframe", "UNKNOWN")
    price     = data.get("price", "N/A")
    atr       = data.get("atr", "N/A")
    bias      = data.get("bias", "UNKNOWN")
    killzone  = data.get("killzone", "UNKNOWN")
    snr_type  = data.get("snr_type", "UNKNOWN")
    sl        = data.get("sl", "N/A")
    tp1       = data.get("tp1", "N/A")
    tp2       = data.get("tp2", "N/A")
    rr_tp1    = data.get("rr_tp1", "N/A")
    rr_tp2    = data.get("rr_tp2", "N/A")
    note      = data.get("note", "")

    # Optional extended data (if sent from Pine Script)
    ema9      = data.get("ema9", "N/A")
    ema21     = data.get("ema21", "N/A")
    ema200    = data.get("ema200", "N/A")
    rsi       = data.get("rsi", "N/A")
    volume    = data.get("volume", "N/A")
    vol_avg   = data.get("vol_avg", "N/A")
    swing_h   = data.get("swing_high", "N/A")
    swing_l   = data.get("swing_low", "N/A")

    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    prompt = f"""
A TradingView alert has fired from The Alchemist Pine Script indicator.
Analyze this setup using the full MSNR x SMC x ICT methodology and give me a complete trade analysis.

═══ ALERT DATA ═══
Timestamp:    {timestamp}
Symbol:       {symbol} ({exchange})
Timeframe:    {tf}
Signal:       {signal}
Price:        {price}
ATR(14):      {atr}

═══ STRATEGY CONDITIONS MET ═══
HTF Bias:     {bias}
Killzone:     {killzone}
SNR Type:     {snr_type}
Note:         {note}

═══ CALCULATED LEVELS ═══
Stop Loss:    {sl}
TP1:          {tp1}
TP2:          {tp2}
RR (TP1):     1:{rr_tp1}
RR (TP2):     1:{rr_tp2}

═══ EXTRA INDICATORS (if available) ═══
EMA 9:        {ema9}
EMA 21:       {ema21}
EMA 200:      {ema200}
RSI(14):      {rsi}
Volume:       {volume}
Vol Avg(20):  {vol_avg}
Swing High:   {swing_h}
Swing Low:    {swing_l}

Using the MSNR x SMC x ICT framework (The Alchemist methodology), provide your complete 
structured analysis and trade verdict. Be specific about what the HTF storyline says,
whether the SNR is genuinely fresh, if the rejection quality is high, whether the BOS 
is external and valid, and whether this is inside the ideal killzone window.

Pay special attention to:
- Are the Weekly and Daily storylines aligned, or is there conflict?
- Is this a Classic SNR, GAP SNR, RBS, or SBR setup?
- Does the rejection candle have a thick body (quality) or thin body (weaker)?
- Is the BOS external (counts) or internal (does not count)?
- What is the trendline confluence status?
- Should I TAKE TRADE, WAIT for better entry, or DO NOT TRADE?
"""
    return prompt.strip()


# ═══════════════════════════════════════════════════════════════════════════════
# TELEGRAM SENDER
# ═══════════════════════════════════════════════════════════════════════════════

def send_telegram(message: str, parse_mode: str = "Markdown") -> bool:
    """Send message to Telegram. Returns True on success."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram credentials not set — skipping send")
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id":    TELEGRAM_CHAT_ID,
        "text":       message,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True
    }

    try:
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        log.info("Telegram message sent successfully")
        return True
    except requests.exceptions.RequestException as e:
        log.error(f"Telegram send failed: {e}")
        # Retry without markdown in case of parse error
        if parse_mode == "Markdown":
            payload["parse_mode"] = "HTML"
            payload["text"] = message.replace("*", "").replace("_", "").replace("`", "")
            try:
                resp = requests.post(url, json=payload, timeout=10)
                resp.raise_for_status()
                return True
            except Exception:
                pass
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# CLAUDE ANALYSIS RUNNER
# ═══════════════════════════════════════════════════════════════════════════════

def get_claude_analysis(data: dict) -> str:
    """Call Claude API with The Alchemist system prompt and return analysis text."""

    prompt = build_analysis_prompt(data)

    try:
        log.info(f"Sending to Claude: {data.get('symbol')} {data.get('signal')}")
        message = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1000,
            system=SYSTEM_PROMPT,
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        analysis = message.content[0].text
        log.info("Claude analysis received")
        return analysis

    except anthropic.APIConnectionError as e:
        log.error(f"Claude connection error: {e}")
        return "⚠️ *Claude Analysis Unavailable*\nCould not connect to Claude API. Check your API key and network."

    except anthropic.RateLimitError:
        log.error("Claude rate limit hit")
        return "⚠️ *Claude Rate Limited*\nToo many requests. Analysis skipped for this signal."

    except anthropic.APIStatusError as e:
        log.error(f"Claude API error {e.status_code}: {e.message}")
        return f"⚠️ *Claude API Error {e.status_code}*\n{e.message}"

    except Exception as e:
        log.error(f"Unexpected error calling Claude: {e}")
        return f"⚠️ *Analysis Error*\n{str(e)}"


# ═══════════════════════════════════════════════════════════════════════════════
# SECURITY HELPER
# ═══════════════════════════════════════════════════════════════════════════════

def verify_signature(payload: bytes, signature: str) -> bool:
    """Optional HMAC-SHA256 signature check for webhook security."""
    if not WEBHOOK_SECRET:
        return True  # No secret set → skip check
    expected = hmac.new(
        WEBHOOK_SECRET.encode(), payload, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, signature)


# ═══════════════════════════════════════════════════════════════════════════════
# ROUTES
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/", methods=["GET"])
def health():
    """Health check — Railway/Render pings this."""
    return jsonify({
        "status":    "online",
        "service":   "The Alchemist — Claude Trade Analysis Server",
        "version":   "2.0.0",
        "endpoints": ["/webhook", "/webhook/bos", "/test"]
    })


@app.route("/webhook", methods=["POST"])
def webhook():
    """
    Main webhook: receives TradingView BUY/SELL alerts from The Alchemist Pine Script.
    Sends full Claude analysis to Telegram.
    """
    # ── Security check (optional) ──
    sig = request.headers.get("X-Signature", "")
    if WEBHOOK_SECRET and not verify_signature(request.data, sig):
        log.warning("Invalid webhook signature — rejected")
        return jsonify({"error": "Invalid signature"}), 401

    # ── Parse JSON (handle plain text from TradingView gracefully) ──
    try:
        data = request.get_json(force=True, silent=True)
        if not data:
            raw = request.data.decode("utf-8", errors="ignore").strip()
            log.warning(f"Non-JSON payload received: {raw[:200]}")
            data = {
                "symbol":   "UNKNOWN",
                "timeframe":"UNKNOWN",
                "signal":   "BUY" if "buy" in raw.lower() else "SELL" if "sell" in raw.lower() else "UNKNOWN",
                "price":    "N/A",
                "atr":      "N/A",
                "bias":     "UNKNOWN",
                "killzone": "UNKNOWN",
                "snr_type": "UNKNOWN",
                "sl":       "N/A",
                "tp1":      "N/A",
                "tp2":      "N/A",
                "rr_tp1":  "N/A",
                "rr_tp2":  "N/A",
                "note":     f"Raw alert: {raw[:300]}"
            }
    except Exception as e:
        log.error(f"Payload parse error: {e}")
        data = {"signal": "UNKNOWN", "symbol": "UNKNOWN", "note": str(e)}

    signal = data.get("signal", "UNKNOWN").upper()
    symbol = data.get("symbol", "UNKNOWN")
    log.info(f"Webhook received: {symbol} — {signal}")

    # ── Quick Telegram alert (instant) ──
    emoji = "🟢" if signal == "BUY" else "🔴" if signal == "SELL" else "🟡"
    quick_msg = (
        f"{emoji} *ALCHEMIST ALERT FIRED*\n"
        f"*{symbol}* | `{data.get('timeframe', '?')}` | *{signal}*\n"
        f"Price: `{data.get('price', '?')}`\n"
        f"Bias: `{data.get('bias', '?')}`\n"
        f"SNR: `{data.get('snr_type', '?')}`\n"
        f"⏳ _Getting Claude analysis..._"
    )
    send_telegram(quick_msg)

    # ── Claude deep analysis ──
    analysis = get_claude_analysis(data)

    # ── Send full analysis to Telegram ──
    send_telegram(analysis)

    return jsonify({
        "status":   "ok",
        "symbol":   symbol,
        "signal":   signal,
        "analyzed": True
    })


@app.route("/webhook/bos", methods=["POST"])
def webhook_bos():
    """
    BOS-only webhook: receives Break of Structure confirmation alerts.
    Sends a lighter notification (no full Claude analysis — just an alert to watch).
    """
    try:
        data = request.get_json(force=True) or {}
    except Exception:
        return jsonify({"error": "Invalid JSON"}), 400

    signal = data.get("signal", "BOS")
    symbol = data.get("symbol", "UNKNOWN")
    note   = data.get("note", "")
    tf     = data.get("timeframe", "?")
    price  = data.get("price", "?")

    direction = "▲ BULLISH" if "BULL" in signal.upper() else "▼ BEARISH"

    bos_msg = (
        f"💥 *EXTERNAL BOS CONFIRMED*\n"
        f"*{symbol}* | `{tf}` | {direction}\n"
        f"Price: `{price}`\n\n"
        f"📌 _Next step: Wait for pullback to fresh SNR_\n"
        f"📌 _Watch for rejection candle at right shoulder / QML_\n"
        f"📌 _Then look for entry confluence with trendline_\n\n"
        f"_{note}_"
    )
    send_telegram(bos_msg)
    log.info(f"BOS alert sent: {symbol} {signal}")

    return jsonify({"status": "ok", "symbol": symbol, "signal": signal})


@app.route("/test", methods=["GET", "POST"])
def test():
    """
    Test endpoint — sends a sample signal through the full pipeline.
    GET: returns sample payload info
    POST: fires a fake signal through Claude + Telegram
    """
    sample_data = {
        "symbol":    "EURUSD",
        "exchange":  "FX",
        "timeframe": "60",
        "signal":    "BUY",
        "price":     "1.08450",
        "atr":       "0.00082",
        "bias":      "BULLISH",
        "killzone":  "ACTIVE",
        "snr_type":  "FRESH_SUPPORT",
        "sl":        "1.08327",
        "tp1":       "1.08614",
        "tp2":       "1.08737",
        "rr_tp1":    "5.0",
        "rr_tp2":    "7.0",
        "ema9":      "1.08410",
        "ema21":     "1.08320",
        "ema200":    "1.07900",
        "rsi":       "44.5",
        "volume":    "18502",
        "vol_avg":   "12300",
        "swing_high":"1.08900",
        "swing_low": "1.08100",
        "note":      "Rejection+BOS+Killzone confirmed — TEST SIGNAL"
    }

    if request.method == "POST":
        log.info("Test endpoint triggered — running full pipeline")
        analysis = get_claude_analysis(sample_data)
        send_telegram(
            "🧪 *TEST SIGNAL — The Alchemist Server*\n" +
            f"Symbol: EURUSD | H1 | BUY\n\n" +
            analysis
        )
        return jsonify({"status": "ok", "test": True, "analysis_preview": analysis[:200] + "..."})

    return jsonify({
        "info":         "POST to /test to fire a sample signal through Claude + Telegram",
        "sample_data":  sample_data
    })


# ═══════════════════════════════════════════════════════════════════════════════
# STARTUP
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    log.info("━" * 60)
    log.info("  ⚗️  THE ALCHEMIST — Claude AI Trade Analysis Server")
    log.info("━" * 60)

    # Validate credentials
    missing = []
    if not ANTHROPIC_API_KEY:
        missing.append("ANTHROPIC_API_KEY")
    if not TELEGRAM_BOT_TOKEN:
        missing.append("TELEGRAM_BOT_TOKEN")
    if not TELEGRAM_CHAT_ID:
        missing.append("TELEGRAM_CHAT_ID")

    if missing:
        log.warning(f"⚠️  Missing env vars: {', '.join(missing)}")
        log.warning("   Set these in your .env file or environment")
    else:
        log.info("✅  All credentials loaded")

    log.info(f"🚀  Server starting on port {PORT}")
    log.info(f"📡  Webhook endpoint: POST /webhook")
    log.info(f"💥  BOS endpoint:     POST /webhook/bos")
    log.info(f"🧪  Test endpoint:    GET/POST /test")
    log.info("━" * 60)

    app.run(host="0.0.0.0", port=PORT, debug=DEBUG)
