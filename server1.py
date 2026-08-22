# -*- coding: utf-8 -*-
"""
Jaina Coinone Monitor v4
- Coinone PUBLIC REST ticker polling every 3 seconds (robust fallback)
- WLD/KRW, KAIA/KRW monitoring
- Telegram /start, /status, /test
- Signal alerts only; NO automatic trading
- Runs correctly under: gunicorn server:app
"""

import os
import time
import threading
import requests
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

COINS = {
    "WLD": {"avg": 452.0, "qty": 192_495},
    "KAIA": {"avg": 35.0, "qty": 1_131_289},
}

STATE = {
    s: {
        "price": 0.0,
        "peak": 0.0,
        "signal": "연결 대기",
        "connected": False,
        "last_update": 0,
        "quote_volume": 0.0,
        "best_bid": 0.0,
        "best_ask": 0.0,
        "source": "Coinone REST",
    }
    for s in COINS
}

LOCK = threading.RLock()
SESSION = requests.Session()

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TG_OFFSET = 0
LAST_ALERT = {s: "" for s in COINS}

COINONE_ALL_TICKERS = "https://api.coinone.co.kr/public/v2/ticker_new/KRW"


def log(msg):
    print(time.strftime("%Y-%m-%d %H:%M:%S"), msg, flush=True)


def get_float(v, default=0.0):
    try:
        return float(v)
    except Exception:
        return default


def decide(symbol):
    with LOCK:
        x = dict(STATE[symbol])
        c = COINS[symbol]

    p = x["price"]
    peak = x["peak"]

    if not p or not peak:
        return "연결 대기"

    gain = (p / c["avg"] - 1) * 100
    dd = (p / peak - 1) * 100

    if dd <= -20:
        return "🛑 재매수 중지 · 급락 점검"
    if dd <= -15:
        return "🔵 3차 재매수 후보"
    if dd <= -10:
        return "🔵 2차 재매수 후보"
    if dd <= -5:
        return "🔵 1차 재매수 후보"

    if gain >= 500:
        return "🟠 5배 구간 · 과열 확인"
    if gain >= 400:
        return "🟠 4배 구간 · 추세 확인"
    if gain >= 300:
        return "🟡 3배 구간 · 추가 익절 검토"
    if gain >= 200:
        return "🟡 2배 구간 · 2차 익절 검토"
    if gain >= 100:
        return "🟢 1배 구간 · 1차 익절 검토"

    return "⚪ 홀딩 / 관찰"


def tg_request(method, payload=None):
    if not TOKEN:
        return None
    try:
        r = SESSION.post(
            f"https://api.telegram.org/bot{TOKEN}/{method}",
            json=payload or {},
            timeout=(5, 12),
        )
        return r.json()
    except Exception as e:
        log(f"[Telegram] {method} error: {e}")
        return None


def send_telegram(text, chat_id=None):
    cid = str(chat_id or CHAT_ID or "").strip()
    if not TOKEN or not cid:
        return False
    result = tg_request("sendMessage", {"chat_id": cid, "text": text})
    return bool(result and result.get("ok"))


def make_status(symbol):
    with LOCK:
        x = dict(STATE[symbol])
        c = COINS[symbol]
    p = x["price"]
    peak = x["peak"]
    gain = (p / c["avg"] - 1) * 100 if p else 0.0
    dd = (p / peak - 1) * 100 if p and peak else 0.0

    return (
        f"【{symbol}/KRW】\n"
        f"현재가: {p:,.4f}원\n"
        f"평단: {c['avg']:,.0f}원\n"
        f"보유수량: {c['qty']:,}개\n"
        f"평단 대비: {gain:+.2f}%\n"
        f"최근 고점: {peak:,.4f}원\n"
        f"고점 대비: {dd:+.2f}%\n"
        f"신호: {x['signal']}\n"
        f"데이터: {x['source']}\n"
        f"※ 자동주문 없음"
    )


def maybe_alert(symbol):
    new_signal = decide(symbol)

    with LOCK:
        old_signal = STATE[symbol]["signal"]
        STATE[symbol]["signal"] = new_signal

    alertable = any(k in new_signal for k in ("재매수", "익절", "중지", "과열"))
    if alertable and new_signal != old_signal and LAST_ALERT[symbol] != new_signal:
        LAST_ALERT[symbol] = new_signal
        if CHAT_ID:
            send_telegram("🔔 자이나 코인봇 신호\n\n" + make_status(symbol))


def coinone_rest_loop():
    """
    Uses one request for the whole KRW market every 3 seconds.
    This is more robust on Render than relying only on a long-lived WebSocket.
    """
    while True:
        try:
            r = SESSION.get(
                COINONE_ALL_TICKERS,
                params={"additional_data": "false"},
                timeout=(5, 10),
                headers={"User-Agent": "JainaCoinMonitor/4.0"},
            )
            r.raise_for_status()
            data = r.json()

            if data.get("result") != "success":
                raise RuntimeError(f"Coinone returned: {data}")

            found = set()

            for t in data.get("tickers", []):
                symbol = str(t.get("target_currency", "")).upper()
                if symbol not in COINS:
                    continue

                price = get_float(t.get("last"))
                if price <= 0:
                    continue

                bids = t.get("best_bids") or []
                asks = t.get("best_asks") or []
                best_bid = get_float(bids[0].get("price")) if bids and isinstance(bids[0], dict) else 0.0
                best_ask = get_float(asks[0].get("price")) if asks and isinstance(asks[0], dict) else 0.0

                with LOCK:
                    x = STATE[symbol]
                    x["price"] = price
                    x["peak"] = max(x["peak"], price) if x["peak"] else price
                    x["connected"] = True
                    x["last_update"] = int(time.time())
                    x["quote_volume"] = get_float(t.get("quote_volume"))
                    x["best_bid"] = best_bid
                    x["best_ask"] = best_ask
                    x["source"] = "Coinone REST · 3초 갱신"

                found.add(symbol)
                maybe_alert(symbol)

            for symbol in COINS:
                if symbol not in found:
                    with LOCK:
                        STATE[symbol]["connected"] = False
                        STATE[symbol]["signal"] = "⚠️ 종목 데이터 없음"

            if found:
                log("[Coinone REST] updated: " + ",".join(sorted(found)))

        except Exception as e:
            log(f"[Coinone REST] error: {e}")
            with LOCK:
                for symbol in COINS:
                    STATE[symbol]["connected"] = False

        time.sleep(3)


def telegram_loop():
    global CHAT_ID, TG_OFFSET

    if not TOKEN:
        log("[Telegram] TELEGRAM_BOT_TOKEN missing")
        return

    me = tg_request("getMe")
    if me and me.get("ok"):
        log(f"[Telegram] connected as @{me['result'].get('username', '')}")
    else:
        log(f"[Telegram] token/network check failed: {me}")

    while True:
        try:
            result = tg_request(
                "getUpdates",
                {
                    "offset": TG_OFFSET,
                    "timeout": 5,
                    "allowed_updates": ["message"],
                },
            )

            if not result or not result.get("ok"):
                time.sleep(3)
                continue

            for update in result.get("result", []):
                TG_OFFSET = max(TG_OFFSET, int(update.get("update_id", 0)) + 1)
                msg = update.get("message") or {}
                chat = msg.get("chat") or {}
                cid = str(chat.get("id") or "")
                text = (msg.get("text") or "").strip()

                if not cid:
                    continue

                if not CHAT_ID and chat.get("type") == "private":
                    CHAT_ID = cid
                    log(f"[Telegram] alert chat registered: {CHAT_ID}")

                if text.startswith("/start") or text.lower() == "start":
                    send_telegram(
                        "✅ Jaina Coin Monitor 연결 완료\n\n"
                        "코인원 WLD·KAIA 감시를 시작했습니다.\n"
                        "자동주문은 하지 않습니다.\n\n"
                        "/status  현재 상태\n"
                        "/test    알림 테스트",
                        cid,
                    )
                elif text.startswith("/test"):
                    send_telegram(
                        "🔔 테스트 알림 성공\nTelegram 연결이 정상입니다.",
                        cid,
                    )
                elif text.startswith("/status"):
                    send_telegram(
                        make_status("WLD") + "\n\n" + make_status("KAIA"),
                        cid,
                    )

        except Exception as e:
            log(f"[Telegram loop] error: {e}")
            time.sleep(3)


HTML = r"""
<!doctype html>
<html lang="ko">
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>자이나 코인원 감시봇</title>
<style>
body{font-family:system-ui,-apple-system,BlinkMacSystemFont,"Noto Sans KR",sans-serif;background:#f4f6f8;margin:0;padding:15px;color:#101828}
.wrap{max-width:680px;margin:auto}.card{background:white;border-radius:18px;padding:18px;margin:12px 0;box-shadow:0 2px 12px #0001}
.price{font-size:29px;font-weight:850;margin:5px 0}.sig{font-weight:850;background:#eef2f7;border-radius:11px;padding:12px;margin-top:12px}
.ok{color:#067647;font-weight:700}.bad{color:#b42318;font-weight:700}.small{color:#667085;line-height:1.55;font-size:13px}
</style>
<div class=wrap>
<h2>자이나 코인원 감시봇</h2>
<div class=small>WLD · KAIA / 코인원 시세 / 자동주문 없음</div>
<div id=cards></div>
</div>
<script>
async function refresh(){
  try{
    const d=await (await fetch("/api",{cache:"no-store"})).json();
    cards.innerHTML=Object.entries(d).map(([s,c])=>{
      const connected=c.connected;
      const gain=c.price?((c.price/c.avg-1)*100).toFixed(2):"-";
      const dd=(c.price&&c.peak)?((c.price/c.peak-1)*100).toFixed(2):"-";
      return `<div class=card>
        <b>${s}/KRW</b>
        <div class=price>${c.price?Number(c.price).toLocaleString("ko-KR",{maximumFractionDigits:6})+"원":"연결 대기"}</div>
        <div class=${connected?"ok":"bad"}>${connected?"🟢 코인원 연결":"🔴 연결 대기"}</div>
        <div class=small>
          평단 ${Number(c.avg).toLocaleString()}원 · 평단대비 ${gain}%<br>
          최근고점 ${Number(c.peak||0).toLocaleString("ko-KR",{maximumFractionDigits:6})}원 · 고점대비 ${dd}%<br>
          ${c.source}
        </div>
        <div class=sig>${c.signal}</div>
      </div>`;
    }).join("");
  } catch(e) {
    cards.innerHTML="<div class=card>데이터 불러오기 오류</div>";
  }
}
setInterval(refresh,3000);
refresh();
</script>
"""


@app.route("/")
def home():
    return render_template_string(HTML)


@app.route("/health")
def health():
    return "OK", 200


@app.route("/api")
def api():
    with LOCK:
        return jsonify({s: {**STATE[s], **COINS[s]} for s in COINS})


# IMPORTANT: Gunicorn imports server:app, so threads must start on import.
threading.Thread(target=coinone_rest_loop, daemon=True, name="coinone-rest").start()
threading.Thread(target=telegram_loop, daemon=True, name="telegram").start()
log("[App] v4 background services started")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
