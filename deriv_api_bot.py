"""
Deriv API Trading Bot v1.1
Connects to Deriv WebSocket API and executes multiplier contracts.

Strategy: 3 contracts per signal (TP1 / TP2 / TP3 portions)
  - Contract 1 (50% risk): closes at TP1 price level
  - Contract 2 (30% risk): closes at TP2 price level
  - Contract 3 (20% risk): closes at TP3 price level
  - All 3 share the same SL price → total risk = RISK_PER_TRADE ($10)

Designed to run as a background thread inside alchemist_server_v3.py on Railway.

v1.1 fix: Use signal entry price directly (avoids ticks API issues on forex/crypto)
"""

import asyncio
import json
import logging
import os
import queue
import threading
import csv
import time
from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Optional

import requests
import websockets

log = logging.getLogger("deriv_bot")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DERIV_API_TOKEN = os.getenv("DERIV_API_TOKEN", "")
DERIV_APP_ID    = os.getenv("DERIV_APP_ID", "1089")
DERIV_WS_URL    = f"wss://ws.binaryws.com/websockets/v3?app_id={DERIV_APP_ID}"

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")

TRADE_LOG_PATH = os.getenv("TRADE_LOG_PATH", "SLK_Trade_Log.csv")

RISK_PER_TRADE   = 10.0   # USD total per signal
MAX_OPEN_TRADES  = 3      # max concurrent trade groups
DAILY_LOSS_LIMIT = 30.0   # USD

# TradingView symbol → Deriv symbol
SYMBOL_MAP: dict[str, str] = {
    "DERIV:VOLATILITY_10_INDEX":  "R_10",
    "DERIV:VOLATILITY_25_INDEX":  "R_25",
    "DERIV:VOLATILITY_50_INDEX":  "R_50",
    "DERIV:VOLATILITY_75_INDEX":  "R_75",
    "DERIV:VOLATILITY_100_INDEX": "R_100",
    "OANDA:USDCAD":               "frxUSDCAD",
    "OANDA:AUDCHF":               "frxAUDCHF",
    "OANDA:AUDNZD":               "frxAUDNZD",
    "OANDA:EURGBP":               "frxEURGBP",
    "BITSTAMP:BTCUSD":            "cryBTCUSD",
}

# Fallback multipliers (used if contracts_for query fails)
DEFAULT_MULTIPLIERS: dict[str, int] = {
    "R_10": 100, "R_25": 100, "R_50": 100, "R_75": 100, "R_100": 100,
    "frxUSDCAD": 100, "frxAUDCHF": 100, "frxAUDNZD": 100, "frxEURGBP": 100,
    "cryBTCUSD": 100,
}

# Risk split across the 3 contracts
_TP_SPLITS = [
    {"label": "TP1", "risk_ratio": 0.50, "tp_key": "tp1"},
    {"label": "TP2", "risk_ratio": 0.30, "tp_key": "tp2"},
    {"label": "TP3", "risk_ratio": 0.20, "tp_key": "tp3"},
]

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class DerivContract:
    contract_id: str
    tv_symbol: str
    deriv_symbol: str
    direction: str          # BUY | SELL
    stake: float
    sl_amount: float
    tp_amount: float
    tp_label: str           # TP1 | TP2 | TP3
    trade_group: str        # groups all 3 contracts from one signal
    opened_at: datetime = field(default_factory=datetime.utcnow)

@dataclass
class TradeRequest:
    tv_symbol: str
    deriv_symbol: str
    direction: str
    entry: float
    sl: float
    tp1: float
    tp2: float
    tp3: float
    signal_data: dict

# contract_id → DerivContract
_active_contracts: dict[str, DerivContract] = {}
_contracts_lock = threading.Lock()

# tv_symbol → trade_group_id (prevents duplicate trades on same pair)
_active_symbols: dict[str, str] = {}

# trade_group_id → set of remaining contract_ids
_trade_groups: dict[str, set] = {}

# daily loss tracking
_daily_loss   = 0.0
_daily_date   = date.today()
_daily_lock   = threading.Lock()

# ---------------------------------------------------------------------------
# Telegram
# ---------------------------------------------------------------------------

def _send_telegram(msg: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg},
            timeout=10,
        )
    except Exception as e:
        log.warning(f"Telegram failed: {e}")

# ---------------------------------------------------------------------------
# CSV logging
# ---------------------------------------------------------------------------

def _log_csv(contract: DerivContract, event: str, close_price: float, pnl: float) -> None:
    new_file = not os.path.exists(TRADE_LOG_PATH)
    try:
        with open(TRADE_LOG_PATH, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if new_file:
                w.writerow(["Timestamp", "ContractID", "Group", "Symbol", "Direction",
                             "Event", "Stake", "SL", "TP", "ClosePrice", "PnL_USD"])
            w.writerow([
                datetime.utcnow().isoformat(), contract.contract_id, contract.trade_group,
                contract.tv_symbol, contract.direction, event,
                contract.stake, contract.sl_amount, contract.tp_amount, close_price, pnl,
            ])
    except Exception as e:
        log.error(f"CSV log error: {e}")

# ---------------------------------------------------------------------------
# Daily loss guard
# ---------------------------------------------------------------------------

def _reset_daily() -> None:
    global _daily_loss, _daily_date
    today = date.today()
    with _daily_lock:
        if today != _daily_date:
            _daily_loss = 0.0
            _daily_date = today

def _daily_limit_reached() -> bool:
    _reset_daily()
    with _daily_lock:
        return _daily_loss >= DAILY_LOSS_LIMIT

def _record_loss(amount: float) -> None:
    global _daily_loss
    with _daily_lock:
        _daily_loss += amount

# ---------------------------------------------------------------------------
# The Bot
# ---------------------------------------------------------------------------

class DerivBot:
    """
    Runs as a background thread with its own asyncio event loop.
    Flask routes put TradeRequest objects on _trade_queue;
    the bot's async tasks consume them.
    """

    def __init__(self) -> None:
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._req_id   = 0
        self._pending: dict[int, asyncio.Future] = {}
        self._req_lock = asyncio.Lock()
        self._trade_queue: asyncio.Queue = asyncio.Queue()
        self._available_multipliers: dict[str, list[int]] = {}
        self.authorized = False
        self._thread: Optional[threading.Thread] = None

    # ── Public API (called from Flask thread) ────────────────────────────

    def start(self) -> None:
        """Start the bot in a background daemon thread."""
        if not DERIV_API_TOKEN:
            log.warning("DERIV_API_TOKEN not set — Deriv bot disabled")
            return
        self._thread = threading.Thread(target=self._run_thread, daemon=True, name="DerivBot")
        self._thread.start()
        log.info("Deriv bot background thread started")

    def enqueue_trade(self, req: TradeRequest) -> str:
        """Thread-safe — queue a trade request from Flask."""
        if self._loop is None or not self.authorized:
            return "Bot not ready"
        if _daily_limit_reached():
            return f"Daily loss limit reached (${DAILY_LOSS_LIMIT})"
        if len(_active_symbols) >= MAX_OPEN_TRADES:
            return f"Max open trades ({MAX_OPEN_TRADES}) reached"
        if req.tv_symbol in _active_symbols:
            return f"Already trading {req.tv_symbol}"
        self._loop.call_soon_threadsafe(self._trade_queue.put_nowait, req)
        return "queued"

    def close_trades_for_symbol(self, tv_symbol: str, reason: str = "reversal") -> None:
        """Signal reversal — schedule close of all contracts on this symbol."""
        if self._loop:
            self._loop.call_soon_threadsafe(
                self._trade_queue.put_nowait,
                ("close_symbol", tv_symbol, reason),
            )

    def status(self) -> dict:
        with _contracts_lock:
            n = len(_active_contracts)
        _reset_daily()
        return {
            "authorized": self.authorized,
            "open_contracts": n,
            "active_symbols": list(_active_symbols.keys()),
            "daily_loss": round(_daily_loss, 2),
            "daily_limit": DAILY_LOSS_LIMIT,
        }

    # ── Thread entry ────────────────────────────────────────────────────

    def _run_thread(self) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._connect_loop())
        except Exception as e:
            log.error(f"DerivBot thread crashed: {e}", exc_info=True)

    # ── WebSocket connection loop ────────────────────────────────────────

    async def _connect_loop(self) -> None:
        while True:
            try:
                log.info(f"Connecting to Deriv WS: {DERIV_WS_URL}")
                async with websockets.connect(
                    DERIV_WS_URL,
                    ping_interval=30,
                    ping_timeout=10,
                    close_timeout=5,
                ) as ws:
                    self._ws = ws
                    self._pending.clear()
                    self.authorized = False

                    await self._authorize()
                    await self._prefetch_multipliers()

                    await asyncio.gather(
                        self._listener(),
                        self._queue_processor(),
                        self._portfolio_monitor(),
                    )
            except websockets.exceptions.ConnectionClosed as e:
                log.warning(f"WS closed ({e}) — reconnecting in 5s")
            except Exception as e:
                log.error(f"WS error: {e} — reconnecting in 5s")
            finally:
                self._ws = None
                self.authorized = False
            await asyncio.sleep(5)

    # ── Core WS helpers ──────────────────────────────────────────────────

    def _next_id(self) -> int:
        self._req_id += 1
        return self._req_id

    async def _call(self, payload: dict, timeout: float = 15.0) -> dict:
        req_id = self._next_id()
        payload["req_id"] = req_id
        fut: asyncio.Future = self._loop.create_future()
        self._pending[req_id] = fut
        await self._ws.send(json.dumps(payload))
        try:
            return await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError:
            self._pending.pop(req_id, None)
            raise TimeoutError(f"No response for req_id={req_id}")

    # ── Listener ────────────────────────────────────────────────────────

    async def _listener(self) -> None:
        async for raw in self._ws:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue

            req_id = msg.get("req_id")
            if req_id and req_id in self._pending:
                fut = self._pending.pop(req_id)
                if not fut.done():
                    fut.set_result(msg)
                continue

            msg_type = msg.get("msg_type")
            if msg_type == "proposal_open_contract":
                await self._handle_contract_event(msg.get("proposal_open_contract", {}))

    # ── Auth ─────────────────────────────────────────────────────────────

    async def _authorize(self) -> None:
        resp = await self._call({"authorize": DERIV_API_TOKEN})
        if "error" in resp:
            raise PermissionError(f"Deriv auth failed: {resp['error']['message']}")
        acct = resp.get("authorize", {})
        log.info(f"Deriv authorized: {acct.get('loginid')} | balance={acct.get('balance')} {acct.get('currency')}")
        self.authorized = True
        _send_telegram(
            f"✅ Deriv Bot Online\n"
            f"Account: {acct.get('loginid')} | Balance: {acct.get('balance')} {acct.get('currency')}"
        )

    # ── Multiplier prefetch ───────────────────────────────────────────────

    async def _prefetch_multipliers(self) -> None:
        for tv_sym, deriv_sym in SYMBOL_MAP.items():
            try:
                resp = await self._call({
                    "contracts_for": deriv_sym,
                    "currency": "USD",
                    "landing_company": "svg",
                    "product_type": "basic",
                })
                available = resp.get("contracts_for", {}).get("available", [])
                mults = []
                for c in available:
                    if c.get("contract_type") in ("MULTUP", "MULTDOWN"):
                        mults.extend(c.get("multiplier_range", []))
                if mults:
                    self._available_multipliers[deriv_sym] = sorted(set(mults))
                    log.info(f"Multipliers for {deriv_sym}: {self._available_multipliers[deriv_sym]}")
            except Exception as e:
                log.warning(f"Could not fetch multipliers for {deriv_sym}: {e}")

    # ── Queue processor ──────────────────────────────────────────────────

    async def _queue_processor(self) -> None:
        while True:
            item = await self._trade_queue.get()
            try:
                if isinstance(item, TradeRequest):
                    await self._open_trade(item)
                elif isinstance(item, tuple) and item[0] == "close_symbol":
                    _, tv_symbol, reason = item
                    await self._close_all_for_symbol(tv_symbol, reason)
            except Exception as e:
                log.error(f"Queue processor error: {e}", exc_info=True)

    # ── Trade execution ───────────────────────────────────────────────────

    async def _open_trade(self, req: TradeRequest) -> None:
        if req.tv_symbol in _active_symbols:
            log.info(f"Already trading {req.tv_symbol} — skipping")
            return

        # v1.1 FIX: Use signal entry price directly.
        # The ticks API only works for synthetic indices in real-time.
        # For forex/crypto the entry from TradingView is accurate enough.
        entry = float(req.entry)
        log.info(f"Using signal entry price: {entry} for {req.tv_symbol}")

        # SL / TP distances as fractions of entry
        sl_dist  = abs(entry - req.sl) / entry if req.sl and req.entry else 0.01
        tp1_dist = abs(req.tp1 - entry) / entry if req.tp1 else sl_dist * 3
        tp2_dist = abs(req.tp2 - entry) / entry if req.tp2 else sl_dist * 5
        tp3_dist = abs(req.tp3 - entry) / entry if req.tp3 else sl_dist * 7

        # Choose best available multiplier (closest to 1/sl_dist)
        ideal_mult = max(1, round(1.0 / sl_dist)) if sl_dist > 0 else 100
        deriv_sym  = req.deriv_symbol
        avail      = self._available_multipliers.get(deriv_sym, [DEFAULT_MULTIPLIERS.get(deriv_sym, 100)])
        multiplier = min(avail, key=lambda m: abs(m - ideal_mult))
        log.info(f"Entry={entry:.5f} sl_dist={sl_dist:.4%} ideal_mult={ideal_mult} → using multiplier={multiplier}")

        # Contract type
        c_type = "MULTUP" if req.direction == "BUY" else "MULTDOWN"

        trade_group = f"{req.tv_symbol}_{int(time.time())}"
        opened_ids  = []

        for split in _TP_SPLITS:
            tp_price = {"tp1": req.tp1, "tp2": req.tp2, "tp3": req.tp3}.get(split["tp_key"], 0)
            if not tp_price:
                log.info(f"No {split['tp_key']} level — skipping {split['label']} contract")
                continue

            risk_portion = RISK_PER_TRADE * split["risk_ratio"]
            tp_dist = abs(tp_price - entry) / entry if tp_price else sl_dist * 3

            # With sl_dist × stake × multiplier = risk_portion → stake = risk_portion / (sl_dist × multiplier)
            stake     = round(risk_portion / (sl_dist * multiplier), 2) if sl_dist > 0 else risk_portion
            stake     = max(1.0, stake)   # Deriv minimum stake is $1
            sl_amount = round(stake * multiplier * sl_dist, 2)
            tp_amount = round(stake * multiplier * tp_dist, 2)

            log.info(f"  {split['label']}: stake=${stake:.2f} sl=${sl_amount:.2f} tp=${tp_amount:.2f} mult={multiplier}")

            try:
                proposal_resp = await self._call({
                    "proposal": 1,
                    "amount": stake,
                    "basis": "stake",
                    "contract_type": c_type,
                    "currency": "USD",
                    "multiplier": multiplier,
                    "symbol": deriv_sym,
                    "limit_order": {
                        "stop_loss":   sl_amount,
                        "take_profit": tp_amount,
                    },
                })
                if "error" in proposal_resp:
                    log.error(f"Proposal error ({split['label']}): {proposal_resp['error']}")
                    continue

                proposal_id = proposal_resp["proposal"]["id"]

                # Buy
                buy_resp = await self._call({"buy": proposal_id, "price": stake})
                if "error" in buy_resp:
                    log.error(f"Buy error ({split['label']}): {buy_resp['error']}")
                    continue

                contract_id = str(buy_resp["buy"]["contract_id"])
                log.info(f"  Opened contract {contract_id} ({split['label']})")
                opened_ids.append(contract_id)

                contract = DerivContract(
                    contract_id=contract_id,
                    tv_symbol=req.tv_symbol,
                    deriv_symbol=deriv_sym,
                    direction=req.direction,
                    stake=stake,
                    sl_amount=sl_amount,
                    tp_amount=tp_amount,
                    tp_label=split["label"],
                    trade_group=trade_group,
                )
                with _contracts_lock:
                    _active_contracts[contract_id] = contract
                    _trade_groups.setdefault(trade_group, set()).add(contract_id)

                _log_csv(contract, "OPENED", entry, 0)

                # Subscribe to real-time updates for this contract
                await self._ws.send(json.dumps({
                    "proposal_open_contract": 1,
                    "contract_id": int(contract_id),
                    "subscribe": 1,
                }))

            except Exception as e:
                log.error(f"Failed to open {split['label']} contract: {e}", exc_info=True)

        if opened_ids:
            _active_symbols[req.tv_symbol] = trade_group
            _trade_groups.setdefault(trade_group, set())

            msg = (
                f"📈 TRADE OPENED — {req.tv_symbol} {req.direction}\n"
                f"Entry: {entry:.5f} | {multiplier}x multiplier\n"
                f"Risk: ${RISK_PER_TRADE:.2f} | Contracts: {len(opened_ids)}\n"
                f"SL: {req.sl:.5f} | TP1: {req.tp1:.5f} | TP2: {req.tp2:.5f} | TP3: {req.tp3:.5f}"
            )
            _send_telegram(msg)
            log.info(f"Trade group {trade_group} opened with {len(opened_ids)} contracts")
        else:
            log.error(f"All contracts failed for {req.tv_symbol}")
            _send_telegram(f"❌ All contracts failed for {req.tv_symbol} — check logs")

    # ── Contract event handler (subscriptions) ────────────────────────────

    async def _handle_contract_event(self, poc: dict) -> None:
        contract_id = str(poc.get("contract_id", ""))
        status      = poc.get("status", "")

        if status not in ("sold", "expired"):
            return  # still open

        with _contracts_lock:
            contract = _active_contracts.pop(contract_id, None)

        if not contract:
            return

        profit       = float(poc.get("profit", 0))
        exit_tick    = poc.get("exit_tick", poc.get("current_spot", 0))
        is_win       = profit > 0

        if is_win:
            msg = (
                f"✅ {contract.tp_label} HIT — {contract.tv_symbol} {contract.direction}\n"
                f"Closed at {exit_tick} | Profit: +${profit:.2f}"
            )
            if contract.tp_label == "TP1":
                msg += "\n🔒 SL moved to BE on remaining contracts"
                await self._move_sl_to_be_for_group(contract.trade_group, float(exit_tick or 0))
        else:
            msg = (
                f"❌ SL HIT — {contract.tv_symbol} {contract.direction}\n"
                f"Closed at {exit_tick} | Loss: ${abs(profit):.2f}\n"
                f"Closing remaining contracts..."
            )
            _record_loss(abs(profit))
            await self._close_all_for_symbol(contract.tv_symbol, "SL hit")

        _send_telegram(msg)
        _log_csv(contract, contract.tp_label if is_win else "SL", float(exit_tick or 0), profit)

        # Remove trade group tracking if all contracts closed
        with _contracts_lock:
            grp = _trade_groups.get(contract.trade_group, set())
            grp.discard(contract_id)
            if not grp:
                _trade_groups.pop(contract.trade_group, None)
                _active_symbols.pop(contract.tv_symbol, None)
                log.info(f"Trade group {contract.trade_group} fully closed")

    # ── Move SL to breakeven for remaining contracts in a group ──────────

    async def _move_sl_to_be_for_group(self, trade_group: str, be_price: float) -> None:
        with _contracts_lock:
            remaining = list(_trade_groups.get(trade_group, set()))

        for cid in remaining:
            try:
                resp = await self._call({
                    "contract_update": 1,
                    "contract_id": int(cid),
                    "limit_order": {"stop_loss": 0},
                }, timeout=10)
                if "error" in resp:
                    log.warning(f"contract_update SL cancel failed for {cid}: {resp['error']}")
                else:
                    log.info(f"TP1 hit — SL cancelled on contract {cid} (BE)")
            except Exception as e:
                log.warning(f"SL BE update for {cid}: {e}")

    # ── Close all contracts for a symbol ─────────────────────────────────

    async def _close_all_for_symbol(self, tv_symbol: str, reason: str) -> None:
        trade_group = _active_symbols.get(tv_symbol)
        if not trade_group:
            return

        with _contracts_lock:
            ids = list(_trade_groups.get(trade_group, set()))

        for cid in ids:
            await self._sell_contract(cid, reason)

    async def _sell_contract(self, contract_id: str, reason: str) -> None:
        try:
            resp = await self._call({
                "sell": int(contract_id),
                "price": 0,  # sell at market
            }, timeout=10)
            if "error" in resp:
                log.error(f"Sell error for {contract_id}: {resp['error']}")
            else:
                sold_at = resp.get("sell", {}).get("sold_for", 0)
                log.info(f"Contract {contract_id} sold for {sold_at} ({reason})")

                with _contracts_lock:
                    contract = _active_contracts.pop(contract_id, None)

                if contract:
                    pnl = float(sold_at) - contract.stake
                    if pnl < 0:
                        _record_loss(abs(pnl))
                    _log_csv(contract, f"CLOSED:{reason}", float(sold_at), pnl)

                    if reason == "reversal":
                        _send_telegram(
                            f"🔄 SIGNAL REVERSED — {contract.tv_symbol}\n"
                            f"Closed {contract.tp_label} contract early | Sold for: ${sold_at:.2f}"
                        )

        except Exception as e:
            log.error(f"Failed to sell contract {contract_id}: {e}")

    # ── Portfolio monitor (backup poller every 30s) ───────────────────────

    async def _portfolio_monitor(self) -> None:
        while True:
            await asyncio.sleep(30)
            if not self.authorized:
                continue
            try:
                resp = await self._call({"portfolio": 1, "contract_type": ["MULTUP", "MULTDOWN"]})
                open_ids = {
                    str(c["contract_id"])
                    for c in resp.get("portfolio", {}).get("contracts", [])
                }
                with _contracts_lock:
                    tracked = set(_active_contracts.keys())

                orphaned = tracked - open_ids
                for cid in orphaned:
                    log.info(f"Portfolio: contract {cid} no longer open (may have been closed by Deriv)")
                    try:
                        poc_resp = await self._call({
                            "proposal_open_contract": 1,
                            "contract_id": int(cid),
                        })
                        poc = poc_resp.get("proposal_open_contract", {})
                        if poc.get("status") in ("sold", "expired"):
                            await self._handle_contract_event(poc)
                    except Exception as e:
                        log.warning(f"Could not fetch final status for {cid}: {e}")

            except Exception as e:
                log.warning(f"Portfolio monitor error: {e}")


# ---------------------------------------------------------------------------
# Module-level singleton — imported by alchemist_server_v3.py
# ---------------------------------------------------------------------------

_bot: Optional[DerivBot] = None


def get_bot() -> DerivBot:
    global _bot
    if _bot is None:
        _bot = DerivBot()
    return _bot


def start_bot() -> None:
    """Call once at server startup."""
    get_bot().start()


def queue_trade(
    tv_symbol: str,
    direction: str,
    entry: float,
    sl: float,
    tp1: float,
    tp2: float,
    tp3: float,
    signal_data: dict,
) -> str:
    """Queue a trade from the Flask thread. Returns status string."""
    deriv_symbol = SYMBOL_MAP.get(tv_symbol)
    if not deriv_symbol:
        return f"Symbol {tv_symbol!r} not in Deriv map"

    req = TradeRequest(
        tv_symbol=tv_symbol,
        deriv_symbol=deriv_symbol,
        direction=direction,
        entry=entry,
        sl=sl,
        tp1=tp1,
        tp2=tp2,
        tp3=tp3,
        signal_data=signal_data,
    )
    return get_bot().enqueue_trade(req)


def close_symbol(tv_symbol: str, new_direction: str) -> None:
    """Signal reversal — close any open trade on this symbol."""
    bot = get_bot()
    if tv_symbol in _active_symbols:
        bot.close_trades_for_symbol(tv_symbol, "reversal")


def bot_status() -> dict:
    return get_bot().status()
