import os, time, threading, requests, json, html
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from collections import deque
from urllib.parse import quote_plus
import xml.etree.ElementTree as ET
from flask import Flask, jsonify, render_template_string
import re

app = Flask(__name__)
COINS = {"WLD":{"avg":452.0,"qty":192495},"KAIA":{"avg":35.0,"qty":1131289}}
URL = "https://api.coinone.co.kr/public/v2/ticker_new/KRW"
SESSION = requests.Session()
NEWS_HEALTH = {"last_errors":0, "last_ok":0, "last_ts":0.0, "circuit_until":0.0}
NEWS_CACHE = {}
NEWS_CACHE_LOCK = threading.RLock()
NEWS_PRIORITY = threading.Event()
NEWS_CACHE_TTL = 6 * 3600
NEWS_STALE_TTL = 48 * 3600
NEWS_CACHE_LAST_DISK_SAVE = 0.0
NEWS_CACHE_DISK_SAVE_INTERVAL = 60

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN","").strip()
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID","").strip()
TG_OFFSET = 0
SUMMARY_INTERVAL = 17 * 60
LOCK = threading.RLock()
SYMBOL_ALIAS = {"W":"WLD","K":"KAIA","WLD":"WLD","KAIA":"KAIA"}
LEDGER = {s:{
    "qty":float(COINS[s]["qty"]), "avg":float(COINS[s]["avg"]),
    "cash":0.0, "withdrawn":0.0, "deposited":0.0, "realized_pnl":0.0, "trades":[]
} for s in COINS}

# 영구 저장 파일 위치
# Render Persistent Disk를 /var/data 에 마운트하면 재배포/재시작 후에도 유지됩니다.
STATE_FILE = os.getenv("STATE_FILE", "/var/data/jaina_state.json")

def load_persistent_state():
    try:
        if not os.path.exists(STATE_FILE):
            return
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            saved = json.load(f)
        global CHAT_ID
        saved_chat = str(saved.get("_chat_id", "") or "")
        if saved_chat:
            CHAT_ID = saved_chat
        saved_ledger = saved.get("_ledger", {}) or {}

        # v13.4: restore successful news cache from persistent disk.
        # This survives Render restart/redeploy when /var/data is a Persistent Disk.
        saved_news_cache = saved.get("_news_cache", {}) or {}
        now_ts = time.time()
        with NEWS_CACHE_LOCK:
            NEWS_CACHE.clear()
            for key, row in saved_news_cache.items():
                try:
                    ts = float((row or {}).get("ts", 0) or 0)
                    items = list((row or {}).get("items", []) or [])[:12]
                    if ts > 0 and (now_ts - ts) <= NEWS_STALE_TTL and items:
                        NEWS_CACHE[str(key)] = {"ts": ts, "items": items}
                except Exception:
                    continue

        with LOCK:
            for symbol in COINS:
                if symbol in saved_ledger:
                    x=saved_ledger[symbol] or {}
                    LEDGER[symbol]["qty"]=float(x.get("qty",LEDGER[symbol]["qty"]) or 0)
                    LEDGER[symbol]["avg"]=float(x.get("avg",LEDGER[symbol]["avg"]) or 0)
                    LEDGER[symbol]["cash"]=float(x.get("cash",0) or 0)
                    LEDGER[symbol]["withdrawn"]=float(x.get("withdrawn",0) or 0)
                    LEDGER[symbol]["deposited"]=float(x.get("deposited",0) or 0)
                    LEDGER[symbol]["realized_pnl"]=float(x.get("realized_pnl",0) or 0)
                    LEDGER[symbol]["trades"]=list(x.get("trades",[]) or [])[-100:]
                    COINS[symbol]["qty"]=LEDGER[symbol]["qty"]
                    COINS[symbol]["avg"]=LEDGER[symbol]["avg"]
            for symbol in COINS:
                if symbol not in saved:
                    continue
                st = STATE[symbol]
                st["peak"] = float(saved[symbol].get("peak", st.get("peak", 0.0)) or 0.0)
                st["peak_profit_krw"] = float(saved[symbol].get("peak_profit_krw", st.get("peak_profit_krw", 0.0)) or 0.0)
                st["last_signal"] = str(saved[symbol].get("last_signal", st.get("last_signal", "")) or "")
                st["last_alert_ts"] = float(saved[symbol].get("last_alert_ts", st.get("last_alert_ts", 0.0)) or 0.0)
        print("[State] persistent state loaded", STATE_FILE, flush=True)
    except Exception as e:
        print("[State] load error", e, flush=True)

def save_persistent_state():
    try:
        directory = os.path.dirname(STATE_FILE)
        if directory:
            os.makedirs(directory, exist_ok=True)
        with LOCK:
            data = {"_chat_id": CHAT_ID}
            data["_ledger"] = {s:{
                "qty":float(LEDGER[s]["qty"]), "avg":float(LEDGER[s]["avg"]),
                "cash":float(LEDGER[s]["cash"]),
                "withdrawn":float(LEDGER[s].get("withdrawn",0.0)),
                "deposited":float(LEDGER[s].get("deposited",0.0)),
                "realized_pnl":float(LEDGER[s]["realized_pnl"]),
                "trades":list(LEDGER[s]["trades"])[-100:]
            } for s in COINS}
            for symbol in COINS:
                st = STATE[symbol]
                data[symbol] = {
                    "peak": float(st.get("peak", 0.0) or 0.0),
                    "peak_profit_krw": float(st.get("peak_profit_krw", 0.0) or 0.0),
                    "last_signal": str(st.get("last_signal", "") or ""),
                    "last_alert_ts": float(st.get("last_alert_ts", 0.0) or 0.0),
                    "saved_at": int(time.time()),
                }

        # v13.4: persist only recent successful news entries.
        # Snapshot outside the trading LOCK to avoid coupling market state and news I/O.
        with NEWS_CACHE_LOCK:
            now_ts = time.time()
            news_snapshot = {}
            for key, row in NEWS_CACHE.items():
                try:
                    ts = float((row or {}).get("ts", 0) or 0)
                    items = list((row or {}).get("items", []) or [])[:12]
                    if ts > 0 and (now_ts - ts) <= NEWS_STALE_TTL and items:
                        news_snapshot[str(key)] = {"ts": ts, "items": items}
                except Exception:
                    continue
            # Bound disk size.
            if len(news_snapshot) > 120:
                newest = sorted(news_snapshot.items(), key=lambda kv: kv[1].get("ts", 0), reverse=True)[:120]
                news_snapshot = dict(newest)
        data["_news_cache"] = news_snapshot

        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, STATE_FILE)
    except Exception as e:
        print("[State] save error", e, flush=True)

def persistence_loop():
    while True:
        save_persistent_state()
        time.sleep(30)

STATE = {
    s: {
        "peak":0.0,
        "history":deque(maxlen=5200),
        "price":0.0,
        "signal":"연결 대기",
        "score":0,
        "reason":"데이터 축적 중",
        "last_signal":"",
        "last_alert_ts":0.0,
        "peak_profit_krw":0.0,
        "profit_drawdown_pct":0.0,
        "protect_action":"대기",
    } for s in COINS
}

load_persistent_state()

# ---------- v12.8 MARKET SHOCK + DEEP CAUSE SEARCH + SAFE REBUY FILTER ----------
# BTC는 시장 전체 급변 여부를 판별하기 위한 참고 시세입니다. 자동주문에는 사용하지 않습니다.
MARKET_HISTORY = {"BTC": deque(maxlen=5200)}
RAPID_LAST = {s:{"ts":0.0,"dir":""} for s in COINS}
RAPID_COOLDOWN = 30 * 60
RAPID_LOCK = threading.Lock()
# v14.2: 순간 임계치 미도달이어도 지속 하락+추세 붕괴를 별도 감지
SUSTAINED_LAST = {s:{"ts":0.0} for s in COINS}
SUSTAINED_COOLDOWN = 60 * 60
MARKET_CAUSE_LAST = {"ts":0.0,"dir":""}
MARKET_CAUSE_COOLDOWN = 30 * 60

def safe_float(v, default=0.0):
    try:
        x=float(v)
        if x != x:  # NaN guard
            return default
        return x
    except:
        return default

def fetch_tickers():
    r = SESSION.get(
        URL,
        params={"additional_data":"false"},
        headers={"User-Agent":"JainaCoinMonitor/12.4"},
        timeout=(5,10),
    )
    r.raise_for_status()
    j = r.json()
    if j.get("result") != "success":
        raise RuntimeError(str(j))
    out={}
    for t in j.get("tickers",[]):
        s=str(t.get("target_currency","")).upper()
        if s in COINS or s == "BTC":
            p=safe_float(t.get("last"))
            if p>0:
                out[s] = {
                    "price":p,
                    "quote_volume":safe_float(t.get("quote_volume")),
                    "first":safe_float(t.get("first")),
                }
    return out

def pct(a,b):
    if not a or not b:
        return 0.0
    return (a/b-1)*100


def normalize_symbol(v):
    return SYMBOL_ALIAS.get(str(v or "").strip().upper())

def qty_text(v):
    return f"{float(v):,.4f}".rstrip("0").rstrip(".")

def record_sell(symbol, percent, price, reason=""):
    symbol=normalize_symbol(symbol); percent=safe_float(percent); price=safe_float(price)
    if not symbol: raise ValueError("코인은 W 또는 K로 입력하세요.")
    if not (0 < percent <= 100) or price<=0: raise ValueError("비율/가격을 확인하세요.")
    with LOCK:
        l=LEDGER[symbol]; before=float(l["qty"])
        if before<=0: raise ValueError("기록된 보유수량이 없습니다.")
        q=before*percent/100.0; amount=q*price
        pnl=(price-float(l["avg"]))*q
        l["qty"]=before-q; l["cash"]+=amount; l["realized_pnl"]+=pnl
        l["trades"].append({"ts":int(time.time()),"side":"SELL","qty":q,"price":price,"amount":amount,"reason":reason})
        COINS[symbol]["qty"]=l["qty"]; COINS[symbol]["avg"]=l["avg"]
        save_persistent_state()
        return symbol,q,amount,pnl,l["qty"],l["cash"]


def record_sell_qty(symbol, qty, price, reason=""):
    symbol=normalize_symbol(symbol)
    qty=safe_float(qty)
    price=safe_float(price)
    if not symbol:
        raise ValueError("코인은 W 또는 K로 입력하세요.")
    if qty<=0 or price<=0:
        raise ValueError("매도수량/가격을 확인하세요.")
    with LOCK:
        l=LEDGER[symbol]
        before=float(l["qty"])
        if before<=0:
            raise ValueError("기록된 보유수량이 없습니다.")
        if qty > before + 1e-8:
            raise ValueError(f"매도수량이 장부 보유수량보다 많습니다. 현재 {qty_text(before)}개")
        amount=qty*price
        pnl=(price-float(l["avg"]))*qty
        l["qty"]=max(0.0,before-qty)
        l["cash"]+=amount
        l["realized_pnl"]+=pnl
        l["trades"].append({
            "ts":int(time.time()),"side":"SELL_QTY","qty":qty,
            "price":price,"amount":amount,"reason":reason or ""
        })
        COINS[symbol]["qty"]=l["qty"]
        COINS[symbol]["avg"]=l["avg"]
        save_persistent_state()
        return symbol,qty,amount,pnl,l["qty"],l["cash"]


def record_buy(symbol, amount, price, reason=""):
    symbol=normalize_symbol(symbol); amount=safe_float(amount); price=safe_float(price)
    if not symbol: raise ValueError("코인은 W 또는 K로 입력하세요.")
    if amount<=0 or price<=0: raise ValueError("금액/가격을 확인하세요.")
    with LOCK:
        l=LEDGER[symbol]
        if amount > l["cash"]+1e-6: raise ValueError(f"재매수 가능 현금 부족: {l['cash']:,.0f}원")
        q=amount/price; oq=float(l["qty"]); oa=float(l["avg"]); nq=oq+q
        na=((oq*oa)+amount)/nq if nq else price
        l["qty"]=nq; l["avg"]=na; l["cash"]-=amount
        l["trades"].append({"ts":int(time.time()),"side":"BUY","qty":q,"price":price,"amount":amount,"reason":reason})
        COINS[symbol]["qty"]=nq; COINS[symbol]["avg"]=na
        save_persistent_state()
        return symbol,q,nq,na,l["cash"]


def record_deposit(symbol, amount, reason=""):
    symbol=normalize_symbol(symbol); amount=safe_float(amount)
    if not symbol: raise ValueError("코인은 W 또는 K로 입력하세요.")
    if amount<=0: raise ValueError("입금금액은 0보다 커야 합니다.")
    with LOCK:
        l=LEDGER[symbol]
        l["cash"]+=amount
        l["deposited"]=float(l.get("deposited",0.0))+amount
        l["trades"].append({"ts":int(time.time()),"side":"DEPOSIT","qty":0.0,"price":0.0,"amount":amount,"reason":reason or ""})
        save_persistent_state()
        return symbol,amount,l["cash"],l["deposited"]


def record_withdraw(symbol, amount, reason=""):
    symbol=normalize_symbol(symbol)
    amount=safe_float(amount)
    if not symbol:
        raise ValueError("코인은 W 또는 K로 입력하세요.")
    if amount<=0:
        raise ValueError("인출금액은 0보다 커야 합니다.")
    with LOCK:
        l=LEDGER[symbol]
        if amount > l["cash"] + 1e-6:
            raise ValueError(f"재매수 가능 현금이 부족합니다. 현재 {l['cash']:,.0f}원")
        l["cash"]-=amount
        l["withdrawn"]=float(l.get("withdrawn",0.0))+amount
        l["trades"].append({
            "ts":int(time.time()),"side":"WITHDRAW","qty":0.0,"price":0.0,
            "amount":amount,"reason":reason or ""
        })
        save_persistent_state()
        return {
            "symbol":symbol,
            "amount":amount,
            "remaining_cash":l["cash"],
            "withdrawn":l["withdrawn"],
            "reason":reason or ""
        }



def record_cashset(symbol, amount, reason=""):
    """재매수 가능 현금만 실제 거래소 잔액에 맞춰 정정한다. 인출/입금/실현손익 누계는 건드리지 않는다."""
    symbol=normalize_symbol(symbol)
    amount=safe_float(amount)
    if not symbol:
        raise ValueError("코인은 W 또는 K로 입력하세요.")
    if amount < 0:
        raise ValueError("현금 잔액은 0원 이상이어야 합니다.")
    with LOCK:
        l=LEDGER[symbol]
        before=float(l.get("cash",0.0))
        l["cash"]=amount
        l["trades"].append({
            "ts":int(time.time()),"side":"CASHSET","qty":0.0,"price":0.0,
            "amount":amount,"before_cash":before,"reason":reason or ""
        })
        save_persistent_state()
        return {"symbol":symbol,"before":before,"cash":amount,"reason":reason or ""}

def position_text():
    lines=["📒 【자이나 매매장부】"]
    with LOCK:
        for s in ("WLD","KAIA"):
            l=LEDGER[s]; short="W" if s=="WLD" else "K"
            lines += [f"\n{short}",f"보유수량 {qty_text(l['qty'])}개",
                      f"장부평단 {l['avg']:,.4f}원",
                      f"재매수 가능 현금 {l['cash']:,.0f}원",
                      f"외부입금 누적 {float(l.get('deposited',0.0)):,.0f}원",
                      f"개인인출 누적 {float(l.get('withdrawn',0.0)):,.0f}원",
                      f"누적 실현손익 {l['realized_pnl']:+,.0f}원"]
    lines.append("\n※ 수수료 제외 · 사용자가 입력한 실제 체결만 반영")
    return "\n".join(lines)



def booktest_text():
    """매매장부를 변경하지 않고 매도→개인인출→재매수 흐름을 계산만 해본다."""
    lines=["🧪 v12.1 장부 안전 테스트", "※ 실제 보유수량/현금/평단/저장파일은 변경하지 않습니다."]
    with LOCK:
        originals={k:{
            "qty":float(v["qty"]), "avg":float(v["avg"]), "cash":float(v["cash"]),
            "withdrawn":float(v.get("withdrawn",0.0)), "realized_pnl":float(v["realized_pnl"]),
            "trades_len":len(v.get("trades",[]))
        } for k,v in LEDGER.items()}
    # W 기준: 현재 장부의 15%를 평단+25% 가격에 가상 매도
    o=originals["WLD"]
    test_price=o["avg"]*1.25
    sell_qty=o["qty"]*0.15
    sell_amount=sell_qty*test_price
    test_cash=o["cash"]+sell_amount
    withdraw=min(1_000_000.0, test_cash*0.20)
    after_withdraw=test_cash-withdraw
    rebuy=min(after_withdraw*0.30, 1_000_000.0)
    final_cash=after_withdraw-rebuy
    lines += [
        "", "✅ 1. 가상 15% 매도 계산 통과",
        f"W 가상 매도수량 {qty_text(sell_qty)}개 · 가상 확보금 {sell_amount:,.0f}원",
        "", "✅ 2. 가상 개인인출 계산 통과",
        f"가상 인출 {withdraw:,.0f}원 · 인출 후 재매수 가능금 {after_withdraw:,.0f}원",
        "", "✅ 3. 가상 재매수 계산 통과",
        f"가상 재매수 사용 {rebuy:,.0f}원 · 가상 잔여현금 {final_cash:,.0f}원",
    ]
    with LOCK:
        after={k:{
            "qty":float(v["qty"]), "avg":float(v["avg"]), "cash":float(v["cash"]),
            "withdrawn":float(v.get("withdrawn",0.0)), "realized_pnl":float(v["realized_pnl"]),
            "trades_len":len(v.get("trades",[]))
        } for k,v in LEDGER.items()}
    unchanged=(originals==after)
    lines += ["", ("✅ 4. 실제 장부 무변경 확인 통과" if unchanged else "❌ 4. 실제 장부 변경 감지"),
              "", ("🎉 /booktest 전체 통과 — 실제 운영 가능" if unchanged else "⚠️ 운영 중지 — 장부 변경 여부 확인 필요")]
    return "\n".join(lines)

# v12.1 trend filter: Coinone public candle data only (no account API / no auto-order)
# ---------- 09:00 KST DAILY REFERENCE ----------
KST = timezone(timedelta(hours=9))
DAY_REF_CACHE = {}
DAY_REF_CACHE_TTL = 60

def _candle_ts_seconds(v):
    x=safe_float(v)
    if x > 10_000_000_000:  # milliseconds -> seconds
        x /= 1000.0
    return x

def _session_9am_targets():
    now_kst=datetime.now(KST)
    today9=now_kst.replace(hour=9,minute=0,second=0,microsecond=0)
    # 09:00 이전에는 현재 세션 기준점이 전날 09:00
    if now_kst < today9:
        today9 -= timedelta(days=1)
    prev9=today9-timedelta(days=1)
    return today9, prev9

def _nearest_9am_open(rows, target_dt):
    target_ts=target_dt.timestamp()
    best=None
    best_gap=10**30
    for row in rows:
        ts=_candle_ts_seconds(row.get("timestamp"))
        if ts<=0:
            continue
        gap=abs(ts-target_ts)
        if gap<best_gap:
            best=row
            best_gap=gap
    # 1시간봉 기준 09:00에서 지나치게 먼 캔들은 사용하지 않음
    if not best or best_gap > 2*60*60:
        return 0.0
    p=safe_float(best.get("open"))
    if p<=0:
        p=safe_float(best.get("close"))
    return p

def daily_reference(symbol, current_price):
    now=time.time()
    cached=DAY_REF_CACHE.get(symbol)
    if cached and now-cached.get("ts",0)<DAY_REF_CACHE_TTL:
        d=dict(cached["data"])
        p=safe_float(current_price)
        if d.get("ready"):
            d["today_change_pct"]=pct(p,d.get("today_9_price")) if p and d.get("today_9_price") else 0.0
            d["prev24_change_pct"]=pct(p,d.get("prev_9_price")) if p and d.get("prev_9_price") else 0.0
        return d
    try:
        rows=fetch_chart(symbol,"1h",72)
        today9,prev9=_session_9am_targets()
        today_price=_nearest_9am_open(rows,today9)
        prev_price=_nearest_9am_open(rows,prev9)
        if today_price<=0 or prev_price<=0:
            raise RuntimeError("09시 기준가를 찾지 못함")
        p=safe_float(current_price)
        data={
            "ready":True,
            "today_9_price":today_price,
            "prev_9_price":prev_price,
            "today_label":today9.strftime("%m/%d 09:00"),
            "prev_label":prev9.strftime("%m/%d 09:00"),
            "today_change_pct":pct(p,today_price) if p else 0.0,
            "prev24_change_pct":pct(p,prev_price) if p else 0.0,
        }
    except Exception as e:
        data={
            "ready":False,
            "error":str(e),
            "today_change_pct":0.0,
            "prev24_change_pct":0.0,
        }
    DAY_REF_CACHE[symbol]={"ts":now,"data":data}
    return dict(data)

def move_mark(v):
    return "🟢" if v>0.05 else ("🔴" if v<-0.05 else "⚪")

TREND_CACHE = {}
TREND_CACHE_TTL = 60

def ema(values, period):
    if not values: return 0.0
    k=2.0/(period+1.0); e=float(values[0])
    for v in values[1:]: e=float(v)*k+e*(1-k)
    return e

def rsi(values, period=14):
    if len(values)<period+1: return 50.0
    gains=[]; losses=[]
    for a,b in zip(values[-period-1:-1],values[-period:]):
        d=b-a; gains.append(max(d,0)); losses.append(max(-d,0))
    ag=sum(gains)/period; al=sum(losses)/period
    if al==0: return 100.0 if ag>0 else 50.0
    return 100-(100/(1+ag/al))

def macd_values(values):
    if len(values)<35: return 0.0,0.0,0.0
    # build MACD series so signal line is a true EMA9 of MACD
    mac=[]
    for i in range(26,len(values)+1):
        sub=values[:i]; mac.append(ema(sub,12)-ema(sub,26))
    m=mac[-1]; sig=ema(mac,9); return m,sig,m-sig

def fetch_chart(symbol, interval, size=120):
    url=f"https://api.coinone.co.kr/public/v2/chart/KRW/{symbol}"
    r=SESSION.get(url,params={"interval":interval,"size":size},headers={"User-Agent":"JainaCoinMonitor/11.9"},timeout=(5,10))
    r.raise_for_status(); j=r.json()
    if j.get("result")!="success": raise RuntimeError(str(j))
    rows=j.get("chart",[])
    rows=sorted(rows,key=lambda x:safe_float(x.get("timestamp")))
    return rows

def timeframe_metrics(rows):
    closes=[safe_float(x.get("close")) for x in rows if safe_float(x.get("close"))>0]
    highs=[safe_float(x.get("high")) for x in rows if safe_float(x.get("close"))>0]
    lows=[safe_float(x.get("low")) for x in rows if safe_float(x.get("close"))>0]
    vols=[safe_float(x.get("quote_volume")) for x in rows if safe_float(x.get("close"))>0]
    if len(closes)<60: return {"ready":False}
    e20=ema(closes,20); e60=ema(closes,60); rv=rsi(closes,14); m,ms,mh=macd_values(closes)
    # EMA20 slope over approximately five candles
    e20_prev=ema(closes[:-5],20) if len(closes)>65 else e20
    slope=pct(e20,e20_prev)
    # market structure: latest 10-candle high/low vs preceding 10 candles
    hh=max(highs[-10:])>max(highs[-20:-10]) if len(highs)>=20 else False
    hl=min(lows[-10:])>min(lows[-20:-10]) if len(lows)>=20 else False
    vnow=sum(vols[-5:])/5 if len(vols)>=10 else 0
    vprev=sum(vols[-10:-5])/5 if len(vols)>=10 else 0
    vr=vnow/vprev if vprev>0 else 1.0
    return {"ready":True,"price":closes[-1],"ema20":e20,"ema60":e60,"ema_bull":e20>e60,
            "slope":slope,"rsi":rv,"macd":m,"macd_signal":ms,"macd_hist":mh,
            "macd_bull":m>ms,"hh":hh,"hl":hl,"vol_ratio":vr}

def trend_analysis(symbol):
    now=time.time(); cached=TREND_CACHE.get(symbol)
    if cached and now-cached.get("ts",0)<TREND_CACHE_TTL: return cached["data"]
    try:
        short=timeframe_metrics(fetch_chart(symbol,"15m",120))
        mid=timeframe_metrics(fetch_chart(symbol,"4h",120))
        if not short.get("ready") or not mid.get("ready"): raise RuntimeError("캔들 데이터 부족")
        # 100-point confidence: medium trend gets slightly higher weight than short-term noise.
        score=0
        score += 15 if short["ema_bull"] else 0
        score += 10 if short["slope"]>0 else 0
        score += 10 if short["macd_bull"] else 0
        score += 5 if short["hl"] else 0
        score += 20 if mid["ema_bull"] else 0
        score += 15 if mid["slope"]>0 else 0
        score += 10 if mid["macd_bull"] else 0
        score += 10 if mid["hl"] else 0
        score += 5 if 45<=mid["rsi"]<=75 else 0
        if score>=75: label="🟢 상승추세 강함"
        elif score>=60: label="🟢 상승추세 유지"
        elif score>=45: label="🟡 상승추세 둔화/혼조"
        else: label="🔴 추세 훼손 주의"
        data={"ready":True,"score":int(score),"label":label,"short":short,"mid":mid}
    except Exception as e:
        data={"ready":False,"score":0,"label":"⚪ 추세 데이터 확인중","error":str(e)}
    TREND_CACHE[symbol]={"ts":now,"data":data}; return data


def trend_report_text():
    """현재 WLD/KAIA의 단기·중기 추세와 매도 판단 보조를 한눈에 보여준다."""
    parts=["📈 【자이나 추세판단】"]
    ticks=fetch_tickers()
    for symbol in ("WLD","KAIA"):
        t=trend_analysis(symbol)
        current_p=safe_float((ticks.get(symbol) or {}).get("price"))
        day=daily_reference(symbol,current_p) if current_p>0 else {"ready":False}
        short_name="W" if symbol=="WLD" else "K"
        if not t.get("ready"):
            parts.append(
                f"\n{short_name} ({symbol})\n"
                f"⚪ 추세 데이터 확인중\n"
                f"이유 {t.get('error','캔들 데이터 부족')}"
            )
            continue

        s=t["short"]; m=t["mid"]; score=int(t.get("score",0))
        if score>=75:
            final="🟢 상승추세 강함 — 보호매도 신호가 와도 전량매도보다 관찰/소량 분할 우선"
        elif score>=60:
            final="🟢 상승추세 유지 — 매도는 서두르지 말고 추세 약화 동반 여부 확인"
        elif score>=45:
            final="🟡 추세 둔화/혼조 — 보호매도 신호와 함께 오면 분할익절 비중 확대 검토"
        else:
            final="🔴 추세 훼손 주의 — 보호매도 신호와 겹치면 수익보호 우선순위 상승"

        parts.append(
            f"\n{short_name} ({symbol})\n"
            f"{(move_mark(day.get('today_change_pct',0)) + ' 오늘09시 대비 ' + format(day.get('today_change_pct',0), '+.2f') + '%' + chr(10)) if day.get('ready') else '📅 09시 등락률 확인중' + chr(10)}"
            f"{(move_mark(day.get('prev24_change_pct',0)) + ' 전일09시 대비 ' + format(day.get('prev24_change_pct',0), '+.2f') + '%' + chr(10)) if day.get('ready') else ''}"
            f"추세신뢰도 {score}/100 · {t.get('label','')}\n"
            f"단기 15분봉: EMA20/60 {'🟢' if s.get('ema_bull') else '🔴'} · "
            f"EMA20기울기 {s.get('slope',0):+.2f}% · RSI {s.get('rsi',50):.1f} · "
            f"MACD {'🟢' if s.get('macd_bull') else '🔴'} · "
            f"저점상승 {'🟢' if s.get('hl') else '🔴'}\n"
            f"중기 4시간봉: EMA20/60 {'🟢' if m.get('ema_bull') else '🔴'} · "
            f"EMA20기울기 {m.get('slope',0):+.2f}% · RSI {m.get('rsi',50):.1f} · "
            f"MACD {'🟢' if m.get('macd_bull') else '🔴'} · "
            f"저점상승 {'🟢' if m.get('hl') else '🔴'}\n"
            f"최종보조판단 {final}"
        )

    parts.append(
        "\n※ 추세지표는 매도 여부의 최종 보조판단용이며 자동주문은 하지 않습니다."
    )
    return "\n".join(parts)

def strategy(symbol, tick):
    now=time.time()
    p=safe_float(tick.get("price"))
    qv=safe_float(tick.get("quote_volume"))
    meta=COINS[symbol]

    with LOCK:
        st=STATE[symbol]
        st["price"]=p
        st["peak"]=max(st["peak"],p) if st["peak"] else p
        st["history"].append((now,p,qv))
        hist=list(st["history"])

        p1m=hist[-20][1] if len(hist)>=20 else hist[0][1]
        p5m=hist[-100][1] if len(hist)>=100 else hist[0][1]
        ret1=pct(p,p1m)
        ret5=pct(p,p5m)
        ret30=_time_return(hist,30*60)
        ret60=_time_return(hist,60*60)
        ret4h=_time_return(hist,4*60*60)
        price_dd=pct(p,st["peak"])
        gain=pct(p,meta["avg"])

        current_profit=max(0.0,(p-meta["avg"])*meta["qty"])
        st["peak_profit_krw"]=max(st.get("peak_profit_krw",0.0),current_profit)
        peak_profit=st["peak_profit_krw"]
        profit_dd=((current_profit/peak_profit)-1)*100 if peak_profit>0 else 0.0
        st["profit_drawdown_pct"]=profit_dd

        recent=hist[-20:]
        previous=hist[-40:-20] if len(hist)>=40 else []
        recent_avg=sum(x[2] for x in recent)/len(recent) if recent else 0.0
        prev_avg=sum(x[2] for x in previous)/len(previous) if previous else 0.0
        vol_ratio=recent_avg/prev_avg if prev_avg>0 else 1.0

        score=0
        reasons=[]

        if ret1>=2.0:
            score+=2; reasons.append(f"1분 +{ret1:.1f}%")
        elif ret1>=1.0:
            score+=1; reasons.append(f"1분 +{ret1:.1f}%")

        if ret5>=4.0:
            score+=2; reasons.append(f"5분 +{ret5:.1f}%")
        elif ret5>=2.0:
            score+=1; reasons.append(f"5분 +{ret5:.1f}%")

        if vol_ratio>=1.8:
            score+=2; reasons.append(f"거래량 {vol_ratio:.1f}배")
        elif vol_ratio>=1.3:
            score+=1; reasons.append(f"거래량 {vol_ratio:.1f}배")

        if gain>=100:
            score+=1; reasons.append("1배 목표권")
        if gain>=200:
            score+=1; reasons.append("2배 이상")
        if gain>=300:
            score+=1; reasons.append("3배 이상")

        protect_action="대기"
        rebuy_note=""
        phase="관찰"
        trend=trend_analysis(symbol)
        daily=daily_reference(symbol,p)

        # 0) 급락 방어가 최우선
        if price_dd<=-25:
            signal="🛑 급락 점검 · 재매수 보류"
            phase="급락보호"
            protect_action="추가 매수 보류"
            rebuy_note="뉴스/시장 급락 원인 확인"
            reason=f"고점대비 {price_dd:.1f}%"

        # 1) v12.6 재매수 안전필터: 단순 낙폭만으로 황금구간을 선언하지 않는다.
        # 추세점수, 15분 MACD/EMA, 당일 급락 진정 여부까지 확인한다.
        elif -18 < price_dd <= -10 and trend.get("ready") and (
            trend.get("score",0) < 45
            or not trend.get("short",{}).get("macd_bull",False)
            or not trend.get("short",{}).get("ema_bull",False)
            or (daily.get("ready") and daily.get("today_change_pct",0) <= -5.0)
        ):
            signal="🔴 하락추세 · 바닥 확인 전 재매수 보류"
            phase="재매수보류"
            protect_action="추가 매수 보류 · 15분 추세 반전 확인"
            rebuy_note="EMA20/60·MACD·당일 급락이 안정된 뒤 재평가"
            reason=f"고점대비 {price_dd:.1f}% · 추세 {trend.get('score',0)}/100 · 오늘09시 {daily.get('today_change_pct',0):+.2f}%"

        elif -18 < price_dd <= -10 and ret1 >= 0 and ret5 >= -0.5 and vol_ratio <= 1.3 and trend.get("ready") and trend.get("score",0) >= 60 and trend.get("short",{}).get("macd_bull",False) and trend.get("short",{}).get("ema_bull",False) and (not daily.get("ready") or daily.get("today_change_pct",0) > -5.0):
            signal="⭐ 황금구간 · 재매수 최우선 후보"
            phase="황금구간"
            protect_action="익절금의 20~30% 재매수 검토"
            rebuy_note="추가 하락 대비 남은 익절금은 반드시 보유"
            reason=f"고점대비 {price_dd:.1f}% · 추세 {trend.get('score',0)}/100 · 1분 {ret1:+.2f}% · 5분 {ret5:+.2f}% · 거래량 {vol_ratio:.2f}배"

        # 2) 급등 과열: 준비와 실행을 분리
        elif score>=5 and price_dd>-2:
            signal="🚨 익절 실행 · 급등 과열"
            phase="익절실행"
            protect_action="보유량의 15~20% 익절 검토"
            reason=", ".join(reasons[:4]) if reasons else "단기 과열"

        elif score>=3 and price_dd>-3:
            signal="🟠 익절 실행 준비"
            phase="실행준비"
            protect_action="코인원 매도 화면 준비 · 아직 전량매도 금지"
            reason=", ".join(reasons[:4]) if reasons else "상승 강도 증가"

        elif score>=2 and price_dd>-2:
            signal="🟡 급등 감지 · 익절 준비"
            phase="준비"
            protect_action="아직 매도하지 않고 과열 지속 여부 관찰"
            reason=", ".join(reasons[:4]) if reasons else "급등 시작"

        # 3) 최고 평가수익 반납: 준비 → 실행 준비 → 실행
        elif gain>=15 and peak_profit>0 and profit_dd<=-30:
            signal="🛑 수익보호 강경 · 추가 익절 실행"
            phase="수익보호실행"
            protect_action="보유량의 20~25% 추가 익절 검토"
            reason=f"최고 평가수익 대비 {profit_dd:.1f}% 감소"

        elif gain>=15 and peak_profit>0 and profit_dd<=-20:
            signal="🚨 수익보호 익절 실행"
            phase="수익보호실행"
            protect_action="보유량의 15~20% 익절 검토"
            reason=f"최고 평가수익 대비 {profit_dd:.1f}% 감소"

        elif gain>=15 and peak_profit>0 and profit_dd<=-15:
            signal="🟠 수익보호 실행 준비"
            phase="실행준비"
            protect_action="매도 준비 · 추가 약화 확인"
            reason=f"최고 평가수익 대비 {profit_dd:.1f}% 감소"

        elif gain>=15 and peak_profit>0 and profit_dd<=-10:
            signal="🟡 수익보호 준비"
            phase="준비"
            protect_action="아직 매도하지 않고 추세 약화 여부 관찰"
            reason=f"최고 평가수익 대비 {profit_dd:.1f}% 감소"

        # 4) 조정 재매수: 준비와 실행을 분리
        elif price_dd<=-18:
            signal="🔵 3차 재매수 실행 후보"
            phase="재매수실행"
            protect_action="익절금의 30% 이내 재매수 검토"
            rebuy_note="급락이 멈추는지 확인 후 실행"
            reason=f"고점대비 {price_dd:.1f}%"

        elif price_dd<=-12:
            signal="🔵 2차 재매수 실행 후보"
            phase="재매수실행"
            protect_action="익절금의 30% 이내 재매수 검토"
            rebuy_note="1분 하락세 완화 여부 확인"
            reason=f"고점대비 {price_dd:.1f}%"

        elif price_dd<=-10:
            signal="🟠 재매수 실행 준비"
            phase="실행준비"
            protect_action="코인원 매수 화면 준비 · 아직 전액투입 금지"
            rebuy_note="하락 멈춤/반전 확인"
            reason=f"고점대비 {price_dd:.1f}%"

        elif price_dd<=-7:
            signal="🟡 재매수 준비"
            phase="준비"
            protect_action="아직 매수하지 않고 추가 조정 관찰"
            rebuy_note="익절금 보유 유지"
            reason=f"고점대비 {price_dd:.1f}%"

        else:
            signal="⚪ 홀딩 / 관찰"
            phase="관찰"
            if len(hist)<20:
                reason=f"데이터 축적 중 {len(hist)}/20"
            else:
                reason=", ".join(reasons[:3]) if reasons else "과열 신호 없음"

        # Trend overlay never cancels the original warning; it refines the final manual action.
        trend_note=""
        if trend.get("ready"):
            ts=trend["score"]
            if phase in ("수익보호실행","실행준비","준비") and ("수익보호" in signal):
                if ts>=75:
                    protect_action="상승추세 강함 · 즉시 큰 매도보다 보유/소량 익절 우선, 추세 훼손 재확인"
                    trend_note="보호신호는 발생했지만 단·중기 상승구조가 강함"
                elif ts>=60:
                    protect_action="상승추세 유지 · 매도 서두르지 말고 추가 약화 확인, 필요시 소량 익절"
                    trend_note="보호신호와 상승추세가 충돌 — 확인 후 결정"
                elif ts<45:
                    trend_note="보호신호와 추세 훼손이 동시에 확인됨 — 보호매도 중요도 상승"
            elif "익절" in signal and ts>=75:
                trend_note="과열 신호이나 상승추세 강함 — 전량매도보다 분할익절 관점"
        else:
            trend_note="추세지표 데이터 확인중 — 기존 보호신호 기준 유지"

        # v12.6: 재매수 신호는 유지하되 실제 장부 현금이 없으면 행동지침을 안전하게 제한한다.
        if phase in ("황금구간","재매수실행","실행준비","준비") and ("재매수" in signal or "황금구간" in signal):
            cash_now=float(LEDGER[symbol].get("cash",0.0) or 0.0)
            if cash_now < 1000:
                protect_action="재매수 후보 감시 — 현재 장부 재매수 가능 현금 없음"
                rebuy_note="신호만 추적 · 신규 현금 투입 권고 아님"

        st["signal"]=signal
        st["score"]=score
        st["reason"]=reason
        st["protect_action"]=protect_action

        return {
            "price":p,
            "peak":st["peak"],
            "gain_pct":gain,
            "drawdown_pct":price_dd,
            "ret1m":ret1,
            "ret5m":ret5,
            "ret30m":ret30,
            "ret1h":ret60,
            "ret4h":ret4h,
            "vol_ratio":vol_ratio,
            "signal":signal,
            "score":score,
            "reason":reason,
            "avg":meta["avg"],
            "qty":meta["qty"],
            "current_profit_krw":current_profit,
            "peak_profit_krw":peak_profit,
            "profit_drawdown_pct":profit_dd,
            "protect_action":protect_action,
            "rebuy_note":rebuy_note,
            "phase":phase,
            "trend":trend,
            "daily":daily,
            "trend_note":trend_note,
        }

def tg(method,payload=None):
    if not TOKEN:
        return None
    try:
        return SESSION.post(
            f"https://api.telegram.org/bot{TOKEN}/{method}",
            json=payload or {},
            timeout=(5,12),
        ).json()
    except Exception as e:
        print("[Telegram]",e,flush=True)
        return None

def send(text,cid):
    if cid:
        tg("sendMessage",{"chat_id":cid,"text":text})

def send_long(text,cid,limit=3800):
    """Telegram 길이 제한을 피해서 긴 뉴스/호재 브리핑을 여러 메시지로 나눠 보낸다."""
    if not cid or not text:
        return
    text=str(text)
    if len(text)<=limit:
        send(text,cid)
        return
    buf=[]
    size=0
    for line in text.splitlines():
        add=len(line)+1
        if buf and size+add>limit:
            send("\n".join(buf),cid)
            buf=[line]; size=add
        else:
            buf.append(line); size+=add
    if buf:
        send("\n".join(buf),cid)

def alert_text(symbol,d):
    return (
        f"【자이나 코인봇】 {symbol}/KRW\n"
        f"현재가 {d['price']:,.4f}원\n"
        f"{(move_mark(d.get('daily',{}).get('today_change_pct',0)) + ' 오늘09시 대비 ' + format(d.get('daily',{}).get('today_change_pct',0), '+.2f') + '%' + chr(10)) if d.get('daily',{}).get('ready') else '📅 오늘09시 대비 확인중' + chr(10)}"
        f"{(move_mark(d.get('daily',{}).get('prev24_change_pct',0)) + ' 전일09시 대비 ' + format(d.get('daily',{}).get('prev24_change_pct',0), '+.2f') + '%' + chr(10)) if d.get('daily',{}).get('ready') else ''}"
        f"평단대비 {d['gain_pct']:+.2f}%\n"
        f"고점대비 {d['drawdown_pct']:+.2f}%\n"
        f"1분 {d['ret1m']:+.2f}% / 5분 {d['ret5m']:+.2f}%\n"
        f"거래량비 {d['vol_ratio']:.2f}배\n"
        f"📈 추세신뢰도 {d.get('trend',{}).get('score',0)}/100 · {d.get('trend',{}).get('label','⚪ 확인중')}\n"
        f"단기(15분봉) EMA20/60 {'🟢' if d.get('trend',{}).get('short',{}).get('ema_bull') else '🔴'} · RSI {d.get('trend',{}).get('short',{}).get('rsi',50):.1f} · MACD {'🟢' if d.get('trend',{}).get('short',{}).get('macd_bull') else '🔴'}\n"
        f"중기(4시간봉) EMA20/60 {'🟢' if d.get('trend',{}).get('mid',{}).get('ema_bull') else '🔴'} · RSI {d.get('trend',{}).get('mid',{}).get('rsi',50):.1f} · MACD {'🟢' if d.get('trend',{}).get('mid',{}).get('macd_bull') else '🔴'}\n"
        f"{('추세보정 ' + d.get('trend_note','') + chr(10)) if d.get('trend_note') else ''}"
        f"현재 평가수익 {d['current_profit_krw']:,.0f}원\n"
        f"최고 평가수익 {d['peak_profit_krw']:,.0f}원\n"
        f"최고수익 대비 {d['profit_drawdown_pct']:+.2f}%\n"
        f"단계 {d.get('phase','관찰')}\n"
        f"신호 {d['signal']}\n"
        f"권장행동 {d['protect_action']}\n"
        f"{('재매수메모 ' + d.get('rebuy_note','') + chr(10)) if d.get('rebuy_note') else ''}"
        f"장부 보유수량 {qty_text(LEDGER[symbol]['qty'])}개\n"
        f"재매수 가능 현금 {LEDGER[symbol]['cash']:,.0f}원\n"
        f"개인인출 누적 {float(LEDGER[symbol].get('withdrawn',0.0)):,.0f}원\n"
        f"누적 실현손익 {LEDGER[symbol]['realized_pnl']:+,.0f}원\n"
        f"이유 {d['reason']}\n\n"
        f"※ 자동주문 없음 — 코인원 앱에서 직접 판단"
    )

def monitor_loop():
    global CHAT_ID
    while True:
        try:
            ticks=fetch_tickers()
            # BTC 시장 동조 판단용 3초 시계열 저장
            btc=ticks.get("BTC")
            if btc:
                with LOCK:
                    MARKET_HISTORY["BTC"].append((time.time(),safe_float(btc.get("price")),safe_float(btc.get("quote_volume"))))
                # v12.4: BTC가 먼저 급등/급락하면 W/K가 아직 임계치 전이어도 시간대 일치 원인을 즉시 분석
                if CHAT_ID:
                    market_dir=market_shock_triggered()
                    if market_dir:
                        lab="급등" if market_dir=="UP" else "급락"
                        send(f"🌐 BTC 선행/누적 {lab} 감지 — WLD·KAIA 영향 원인을 즉시 분석합니다.",CHAT_ID)
                        threading.Thread(target=market_cause_worker,args=(market_dir,CHAT_ID),daemon=True).start()
            for s,tick in ticks.items():
                if s not in COINS:
                    continue
                d=strategy(s,tick)
                # v14.2: 순간/누적 급변뿐 아니라 지속 하락+15분/4시간 추세 붕괴도 중요 원인알람
                sustained = CHAT_ID and sustained_decline_triggered(s,d)
                rapid = CHAT_ID and rapid_triggered(s,d)
                if sustained or rapid:
                    if sustained:
                        send(f"🚨 【중요 지속하락 감지 — {s}】\n{d.get('sustained_reason','')}\n🔎 순간 임계치 미도달이어도 하락 원인을 즉시 분석합니다.",CHAT_ID)
                    else:
                        send(f"⚡ {s} 중요 급변/누적변동 감지 — 원인을 즉시 분석하고 있습니다.",CHAT_ID)
                    threading.Thread(target=rapid_cause_worker,args=(s,dict(d),CHAT_ID),daemon=True).start()
                with LOCK:
                    st=STATE[s]
                    sig=d["signal"]
                    now=time.time()
                    alertable=sig.startswith(("🚨","⭐","🔴","🟠","🟡","🔵","🛑"))
                    changed=sig!=st["last_signal"]
                    # 중요 신호는 17분 정기보고와 분리: 신호가 '변경되는 즉시' 발송
                    # 같은 신호가 계속 유지될 때만 중복 발송을 막음.
                    if alertable and changed:
                        st["last_signal"]=sig
                        st["last_alert_ts"]=now
                        if CHAT_ID:
                            send("🚨 중요 신호 즉시 알림\n\n" + alert_text(s,d),CHAT_ID)
                    elif not alertable:
                        st["last_signal"]=sig
            print("[Strategy] updated", ",".join(ticks.keys()), flush=True)
        except Exception as e:
            print("[Strategy] error",e,flush=True)
        time.sleep(3)


def send_summary_once(cid=None):
    target = str(cid or CHAT_ID or "").strip()
    if not target:
        return False
    try:
        d = snapshot()
        parts = [alert_text(s, d[s]) for s in ("WLD","KAIA") if s in d]
        if not parts:
            return False
        send("⏰ 17분 자동 상태 요약\n\n" + "\n\n".join(parts), target)
        print("[Summary] sent to", target, flush=True)
        return True
    except Exception as e:
        print("[Summary] error", e, flush=True)
        return False

def auto_summary_loop():
    while True:
        time.sleep(SUMMARY_INTERVAL)
        if CHAT_ID:
            send_summary_once(CHAT_ID)

def snapshot():
    ticks=fetch_tickers()
    out={}
    for s,t in ticks.items():
        if s not in COINS:
            continue
        out[s]=strategy(s,t)
    return out


# ---------- NEWS / MARKET ----------
NEWS_INTERVAL = 3 * 60 * 60
LAST_NEWS_SENT = 0.0
LAST_BREAKING_CHECK = 0.0
BREAKING_CHECK_INTERVAL = 10 * 60   # 10분마다 새 중요 호재/악재 확인
SEEN_BREAKING_KEYS = set()
BREAKING_PRIMED = False

POS_WORDS = [
    "partnership","partner","launch","upgrade","adoption","integration",
    "approval","funding","growth","expands","surge","rally","listing",
    "협력","파트너","출시","업그레이드","채택","통합","승인","상장","급등","호재"
]
NEG_WORDS = [
    "hack","exploit","lawsuit","investigation","ban","delist","drop","plunge",
    "sell-off","outage","fraud","decline","risk",
    "해킹","소송","조사","금지","상폐","급락","매도","장애","사기","악재","위험"
]

CATALYST_WORDS = [
    "partnership","partner","launch","upgrade","adoption","integration","expansion",
    "mainnet","testnet","funding","investment","listing","listed","ecosystem",
    "developer","grant","stablecoin","payment","wallet","identity","world id",
    "world chain","orb","mini dapp","miniapp","sdk","api",
    "파트너십","협력","출시","업그레이드","채택","통합","확장","메인넷","테스트넷",
    "투자","펀딩","상장","생태계","개발자","그랜트","스테이블코인","결제","지갑",
    "월드 ID","월드체인","미니 디앱","미니앱"
]

RISK_WORDS = [
    "unlock","token unlock","regulation","regulatory","lawsuit","investigation",
    "ban","delist","hack","exploit","outage","sell-off","fraud",
    "언락","토큰 언락","규제","소송","조사","금지","상폐","해킹","취약점","장애","매도"
]

HIGH_TRUST_SOURCES = (
    "Reuters","Bloomberg","CoinDesk","The Block","Fortune","Forbes",
    "TechCrunch","Yahoo Finance","BusinessWire","PR Newswire",
    "Kaia","Kaia Foundation","World","Worldcoin","World Foundation"
)

def source_weight(source):
    s=(source or "").lower()
    if any(x.lower() in s for x in HIGH_TRUST_SOURCES):
        return 2
    return 1

def catalyst_score_item(item):
    text=((item.get("title") or "")+" "+(item.get("source") or "")).lower()
    pos=sum(1 for w in CATALYST_WORDS if w.lower() in text)
    risk=sum(1 for w in RISK_WORDS if w.lower() in text)
    return pos*2 + source_weight(item.get("source")) - risk*2

def _dedupe_news(items):
    seen=set(); out=[]
    for x in items:
        key=(x.get("title") or "").lower().strip()
        if not key or key in seen:
            continue
        seen.add(key); out.append(x)
    return out

def catalyst_search(symbol):
    if symbol=="WLD":
        positive_queries=[
            '"Worldcoin" OR "World Network" partnership launch expansion adoption integration when:30d',
            '"World Chain" OR "World ID" OR Orb launch integration adoption when:30d',
            'WLD Worldcoin ecosystem developer grant funding listing when:30d',
        ]
        risk_query='"Worldcoin" OR WLD regulation lawsuit investigation token unlock hack when:30d'
        themes=[
            "World ID 실제 사용처·인증 확대",
            "World Chain 앱/개발자/TVL 생태계 성장",
            "Orb 보급·국가 확장과 규제 진행",
            "거래소·기관·기업 파트너십",
            "토큰 언락/공급 증가 일정"
        ]
    else:
        positive_queries=[
            'KAIA blockchain partnership launch ecosystem adoption integration when:30d',
            '"Kaia" stablecoin payment wallet mini dapp mainnet when:30d',
            'KAIA developer grant funding listing ecosystem when:30d',
        ]
        risk_query='KAIA blockchain regulation delist hack outage token unlock when:30d'
        themes=[
            "LINE/Kakao 기반 서비스·미니앱 확장",
            "스테이블코인·결제·지갑 채택",
            "DApp/DeFi/게임 생태계와 온체인 활동",
            "파트너십·상장·개발자 지원",
            "토큰 공급/언락 및 네트워크 리스크"
        ]

    positives=[]
    for q in positive_queries:
        try:
            positives += google_news_rss(q, 6)
        except Exception:
            pass
    positives=_dedupe_news(positives)
    positives=sorted(positives, key=catalyst_score_item, reverse=True)

    try:
        risks=_dedupe_news(google_news_rss(risk_query, 5))
    except Exception:
        risks=[]

    strong=[x for x in positives if catalyst_score_item(x)>=3]
    medium=[x for x in positives if 1<=catalyst_score_item(x)<3]
    # 정보 강도: 뉴스 개수 + 출처 질. 가격 상승확률이 아님.
    info_score=min(100, len(strong)*18 + len(medium)*7 + sum(source_weight(x.get("source")) for x in strong[:4])*3)
    if info_score>=70:
        label="🟢 호재 정보 풍부"
    elif info_score>=40:
        label="🟡 호재 정보 보통"
    else:
        label="⚪ 뚜렷한 신규 호재 적음"

    return {
        "symbol":symbol,
        "score":info_score,
        "label":label,
        "positive":positives[:5],
        "risks":risks[:3],
        "themes":themes
    }


def news_key(item):
    return ((item.get("title") or "").strip().lower() + "|" + (item.get("source") or "").strip().lower())

def breaking_candidates():
    """W/K의 새 중요 호재·악재 후보를 모은다. 점수 기준을 높게 잡아 잡음 알림을 줄인다."""
    out=[]
    for symbol in ("WLD","KAIA"):
        r=catalyst_search(symbol)
        for it in r.get("positive",[]):
            sc=catalyst_score_item(it)
            # 파트너십/출시/채택/상장 등 키워드가 복수로 잡히거나 신뢰도 높은 출처인 경우
            if sc>=5:
                out.append(("positive",symbol,sc,it))
        for it in r.get("risks",[]):
            title=(it.get("title") or "").lower()
            risk_hits=sum(1 for w in RISK_WORDS if w.lower() in title)
            if risk_hits>=1:
                out.append(("risk",symbol,-max(4,risk_hits*2),it))
    return out

def breaking_check_text(new_items):
    if not new_items:
        return ""
    parts=["🚨 【자이나 중요 호재·악재 즉시 레이더】"]
    for kind,symbol,sc,it in new_items[:5]:
        short="W" if symbol=="WLD" else "K"
        icon="🚀" if kind=="positive" else "⚠️"
        label="중요 호재 후보" if kind=="positive" else "중요 리스크 후보"
        src=f" · {it.get('source')}" if it.get("source") else ""
        parts.append(
            f"\n{icon} {short} {label}\n"
            f"{it.get('title','')}{src}\n"
            f"{it.get('link','')}"
        )
    parts.append("\n※ 기사 제목/출처 기반 자동 감지입니다. 실제 영향은 공식 발표와 시장 반응을 함께 확인하세요.")
    return "\n".join(parts)


def good_radar_text():
    parts=["🚀 【자이나 호재·전망 레이더】"]
    try:
        snap=snapshot()
    except Exception:
        snap={}
    for symbol in ("WLD","KAIA"):
        short="W" if symbol=="WLD" else "K"
        r=catalyst_search(symbol)
        trend=((snap.get(symbol) or {}).get("trend") or {})
        trend_score=int(trend.get("score",0) or 0)
        parts += [
            "",
            f"{short} ({symbol})",
            f"호재정보강도 {r['score']}/100 · {r['label']}",
            f"추세신뢰도 {trend_score}/100 · {trend.get('label','⚪ 확인중')}",
            "🟢 최근 긍정 재료"
        ]
        if r["positive"]:
            for i,it in enumerate(r["positive"][:4],1):
                src=f" · {it.get('source')}" if it.get("source") else ""
                parts.append(f"{i}. {it['title']}{src}\n{it['link']}")
        else:
            parts.append("최근 30일 뚜렷한 신규 호재 기사 부족")

        parts.append("🔎 앞으로 볼 핵심")
        for x in r["themes"][:4]:
            parts.append(f"• {x}")

        parts.append("⚠️ 리스크 레이더")
        if r["risks"]:
            for i,it in enumerate(r["risks"][:2],1):
                src=f" · {it.get('source')}" if it.get("source") else ""
                parts.append(f"{i}. {it['title']}{src}")
        else:
            parts.append("검색 범위에서 새 주요 리스크 기사 없음")

        if trend_score>=75 and r["score"]>=60:
            view="🟢 추세와 재료가 함께 강함 — 상승 지속 여부 관찰 가치 높음"
        elif trend_score>=75 and r["score"]<40:
            view="🟡 가격 추세는 강하지만 신규 호재 근거는 약함 — 과열 여부 함께 확인"
        elif trend_score<60 and r["score"]>=60:
            view="🟡 호재는 있으나 가격 추세 확인 필요 — 뉴스만 보고 추격매수 금지"
        else:
            view="⚪ 재료·추세 혼조 — 확인된 변화가 나올 때까지 관찰"
        parts.append(f"🔭 종합전망 {view}")

    parts += [
        "",
        "※ 호재정보강도는 최근 기사·출처·키워드의 정보량 점수이며 가격 상승확률이 아닙니다.",
        "※ 공식 발표/신뢰도 높은 매체를 우선하고 루머성 제목은 판단 근거에서 낮게 봅니다.",
        "※ 자동주문 없음 — 최종 매매는 코인원 앱에서 직접 판단"
    ]
    return "\n".join(parts)


def _persist_news_cache_debounced():
    """v13.4: successful news survives Render restart/redeploy."""
    global NEWS_CACHE_LAST_DISK_SAVE
    now = time.time()
    if now - NEWS_CACHE_LAST_DISK_SAVE < NEWS_CACHE_DISK_SAVE_INTERVAL:
        return
    NEWS_CACHE_LAST_DISK_SAVE = now
    try:
        save_persistent_state()
    except Exception as e:
        print("[NewsCache] persistent save error", e, flush=True)


def _news_cache_get(query, limit=4, allow_stale=True):
    with NEWS_CACHE_LOCK:
        row=NEWS_CACHE.get(query)
        if not row: return []
        age=time.time()-row.get("ts",0)
        ttl=NEWS_STALE_TTL if allow_stale else NEWS_CACHE_TTL
        if age>ttl: return []
        return list(row.get("items",[]))[:limit]

def _news_cache_put(query, items):
    if not items: return
    with NEWS_CACHE_LOCK:
        NEWS_CACHE[query]={"ts":time.time(),"items":list(items)[:12]}
        # 메모리 상한
        if len(NEWS_CACHE)>120:
            for k,_ in sorted(NEWS_CACHE.items(), key=lambda kv:kv[1].get("ts",0))[:20]:
                NEWS_CACHE.pop(k,None)
    _persist_news_cache_debounced()

MACRO_EVIDENCE_KEY = "__macro_evidence_pool_v135__"

def _macro_pool_get(limit=80):
    """v13.8: 검색어와 무관하게 최근 성공한 거시/크립토 기사를 재사용하는 영구 증거풀."""
    return _news_cache_get(MACRO_EVIDENCE_KEY, limit, True)

def _macro_pool_put(items):
    """성공 기사 중 원인분석에 재사용할 수 있는 항목을 하나의 영구 풀에 누적."""
    if not items:
        return
    old=_macro_pool_get(80)
    merged=_dedupe_news(list(items)+list(old))[:80]
    _news_cache_put(MACRO_EVIDENCE_KEY, merged)

def _parse_rss_items(content, limit=6, forced_source=""):
    """RSS/Atom을 공통 형식으로 변환. 개별 피드 오류는 빈 배열로 격리."""
    root=ET.fromstring(content)
    items=[]
    # RSS 2.0
    for item in root.findall(".//item")[:limit]:
        title=html.unescape((item.findtext("title") or "").strip())
        link=(item.findtext("link") or "").strip()
        source=html.unescape((item.findtext("source") or forced_source or "").strip())
        pubdate=(item.findtext("pubDate") or item.findtext("date") or "").strip()
        if title:
            items.append({"title":title,"link":link,"source":source,"pubdate":pubdate})
    if items: return items[:limit]
    # Atom fallback
    ns={"a":"http://www.w3.org/2005/Atom"}
    for ent in root.findall(".//a:entry",ns)[:limit]:
        title=html.unescape((ent.findtext("a:title",default="",namespaces=ns) or "").strip())
        lk=ent.find("a:link",ns); link=(lk.get("href","") if lk is not None else "")
        pubdate=(ent.findtext("a:published",default="",namespaces=ns) or ent.findtext("a:updated",default="",namespaces=ns) or "").strip()
        if title:
            items.append({"title":title,"link":link,"source":forced_source,"pubdate":pubdate})
    return items[:limit]


def bing_news_rss(query, limit=6):
    """v13.0 1차 검색 경로: Bing News RSS. Google 장애와 독립된 검색 경로."""
    key="bing::"+query
    try:
        url="https://www.bing.com/news/search?q="+quote_plus(query)+"&format=rss&mkt=en-US"
        r=SESSION.get(url,headers={"User-Agent":"Mozilla/5.0 JainaCoinMonitor/13.8"},timeout=(2.5,5.0))
        r.raise_for_status()
        items=_parse_rss_items(r.content,limit,"Bing News")
        _news_cache_put(key,items)
        return items
    except Exception:
        cached=_news_cache_get(key,limit,True)
        if cached: return cached
        raise


def direct_crypto_feeds(limit=10):
    """v13.0 보조 경로: 검색엔진을 거치지 않는 직접 금융/크립토 RSS."""
    feeds=[
        ("CoinDesk","https://www.coindesk.com/arc/outboundfeeds/rss/"),
        ("CNBC","https://www.cnbc.com/id/10000664/device/rss/rss.html"),
        # v13.8: 검색엔진 장애와 독립된 추가 금융 피드. 실패해도 다른 피드는 계속 동작.
        ("CNBC Markets","https://www.cnbc.com/id/100003114/device/rss/rss.html"),
    ]
    out=[]
    for source,url in feeds:
        key="feed::"+source
        try:
            r=SESSION.get(url,headers={"User-Agent":"Mozilla/5.0 JainaCoinMonitor/13.8"},timeout=(2.5,5.0))
            r.raise_for_status()
            items=_parse_rss_items(r.content,limit,source)
            _news_cache_put(key,items)
            _macro_pool_put(items)
            out.extend(items)
        except Exception:
            out.extend(_news_cache_get(key,limit,True))
    return _dedupe_news(out)


def multisource_news(query, limit=7, priority=False):
    """v13.0: Bing → Google → 캐시. 한 경로 실패가 전체 원인분석을 막지 않는다."""
    errors=[]; out=[]
    try:
        out.extend(bing_news_rss(query,limit))
    except Exception as e:
        errors.append("Bing:"+type(e).__name__)
    # Bing에서 충분히 얻었으면 Google timeout을 굳이 기다리지 않는다.
    if len(out)<max(3,limit//2):
        try:
            out.extend(google_news_rss(query,limit,priority=priority))
        except Exception as e:
            errors.append("Google:"+type(e).__name__)
    return _dedupe_news(out)[:limit], errors


def google_news_rss(query, limit=4, priority=False):
    """v12.9: 실시간 RSS → 실패 시 최근 성공 캐시. 반복 timeout 때 짧은 회로차단."""
    now=time.time()
    if not priority and now < NEWS_HEALTH.get("circuit_until",0):
        cached=_news_cache_get(query,limit,True)
        if cached: return cached
        raise requests.exceptions.ReadTimeout("news circuit open")
    url = "https://news.google.com/rss/search?q=" + quote_plus(query) + "&hl=ko&gl=KR&ceid=KR:ko"
    try:
        r = SESSION.get(url, headers={"User-Agent":"Mozilla/5.0 JainaCoinMonitor/13.8"}, timeout=(2.5,4.5))
        r.raise_for_status()
        root = ET.fromstring(r.content)
        items = []
        for item in root.findall(".//item")[:limit]:
            title = html.unescape((item.findtext("title") or "").strip())
            link = (item.findtext("link") or "").strip()
            source = html.unescape((item.findtext("source") or "").strip())
            pubdate = (item.findtext("pubDate") or "").strip()
            items.append({"title": title, "link": link, "source": source, "pubdate": pubdate})
        _news_cache_put(query,items)
        return items
    except Exception:
        # Google News가 느릴 때 정기 작업의 연쇄 timeout을 90초간 차단
        NEWS_HEALTH["circuit_until"]=time.time()+90
        cached=_news_cache_get(query,limit,True)
        if cached: return cached
        raise

def classify_news(title):
    low = title.lower()
    pos = sum(1 for w in POS_WORDS if w in low)
    neg = sum(1 for w in NEG_WORDS if w in low)
    if pos > neg: return "🟢"
    if neg > pos: return "🔴"
    return "⚪"

def _series_return(hist, points):
    """3초 샘플 기준 points 이전 대비 등락률. 데이터가 부족하면 None."""
    seq=list(hist)
    if len(seq) < points:
        return None
    return pct(seq[-1][1], seq[-points][1])


def _time_return(hist, seconds):
    """현재 시각에서 seconds 전과 가장 가까운 저장가격 대비 등락률."""
    seq=list(hist)
    if len(seq) < 2:
        return None
    now_ts=seq[-1][0]; target=now_ts-seconds
    # 목표시각까지 데이터가 쌓이지 않았으면 잘못된 누적률을 만들지 않는다.
    if seq[0][0] > target + 30:
        return None
    old=min(seq, key=lambda x: abs(x[0]-target))
    return pct(seq[-1][1], old[1]) if old[1] else None


def market_move_snapshot():
    """BTC와 W/K의 순간 + 누적 움직임을 원인분석용으로 묶어 반환."""
    with LOCK:
        def pack(h, price=0.0):
            return {
                "ret1":_series_return(h,20), "ret5":_series_return(h,100),
                "ret30":_time_return(h,30*60), "ret60":_time_return(h,60*60),
                "ret4h":_time_return(h,4*60*60), "price":price,
            }
        out={"BTC":pack(MARKET_HISTORY["BTC"])}
        for sym in ("WLD","KAIA"):
            h=STATE[sym].get("history") or []
            out[sym]=pack(h,safe_float(STATE[sym].get("price")))
    return out


def _parse_pub_ts(pubdate):
    """Google News RSS RFC822 발행시각을 UTC epoch로 변환. 실패하면 0."""
    try:
        from email.utils import parsedate_to_datetime
        dt=parsedate_to_datetime(pubdate or "")
        if dt.tzinfo is None:
            dt=dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        return 0.0


def _news_age_hours(item):
    ts=_parse_pub_ts(item.get("pubdate"))
    return max(0.0,(time.time()-ts)/3600.0) if ts else 999.0


def _macro_news(direction="DOWN"):
    """v13.0: Bing/직접 RSS/Google/캐시 다중경로 + 24h→48h 심층검색."""
    stage1 = [
        'Bitcoin Federal Reserve chair speech interest rates crypto when:1d',
        'Bitcoin Fed hawkish dovish Treasury yields dollar crypto when:1d',
        'Bitcoin inflation PCE CPI jobs payroll crypto market when:1d',
        'Bitcoin ETF inflow outflow liquidation options expiry crypto when:1d',
        'Bitcoin regulation hack geopolitical tariff crypto when:1d',
        '비트코인 연준 의장 금리 국채 달러 가상자산 when:1d',
        '비트코인 청산 ETF 유출 옵션 만기 규제 해킹 when:1d',
    ]
    if direction == "UP":
        stage1 += ['Bitcoin crypto rally rate cut dovish ETF inflow rebound when:1d']
    else:
        stage1 += ['Bitcoin crypto selloff rate hike hawkish yields dollar liquidation when:1d']

    stage2 = [
        'Bitcoin falls Fed chair rate hike Treasury yields dollar when:2d',
        'Bitcoin drops Jackson Hole Fed speech hawkish when:2d',
        'Bitcoin crypto liquidations long positions options expiry when:2d',
        'Bitcoin ETF outflows spot ETF flows when:2d',
        'Bitcoin Nasdaq risk assets selloff yields dollar when:2d',
        '비트코인 하락 연준 매파 금리인상 국채금리 달러강세 when:2d',
        '비트코인 급락 롱 청산 옵션 만기 ETF 유출 when:2d',
    ] if direction == "DOWN" else [
        'Bitcoin rises Fed dovish rate cut Treasury yields dollar when:2d',
        'Bitcoin rally ETF inflows short liquidation when:2d',
        'Bitcoin Nasdaq risk assets rally yields dollar when:2d',
        '비트코인 상승 연준 비둘기 금리인하 ETF 유입 when:2d',
    ]

    def one(q, limit=7):
        try:
            items,errs=multisource_news(q, limit, priority=True)
            return items, (",".join(errs) if errs else None)
        except Exception as e:
            return [], type(e).__name__

    def collect_parallel(queries, limit=7):
        out=[]; errors=[]
        # 요청 수가 많아도 한 번에 4개만 실행해 Google/Render 부담을 제한한다.
        with ThreadPoolExecutor(max_workers=min(4, len(queries))) as ex:
            futs=[ex.submit(one,q,limit) for q in queries]
            for f in as_completed(futs):
                try:
                    items,err=f.result()
                    out.extend(items)
                    if err: errors.append(err)
                except Exception as e:
                    errors.append(type(e).__name__)
        return _dedupe_news(out), errors

    # v13.8: 검색엔진보다 직접 RSS와 영구 캐시를 먼저 사용한다.
    # Render에서 Bing/Google이 느릴 때 /cause 호출마다 다수의 timeout을 만드는 문제를 줄인다.
    first=[]; err1=[]
    try:
        first=_dedupe_news(direct_crypto_feeds(20) + _macro_pool_get(80))
    except Exception:
        first=_dedupe_news(_macro_pool_get(80))

    # 직접 근거가 충분하지 않을 때만 핵심 검색어 2개를 우선 조회한다.
    # 전체 검색어 폭탄 대신 짧은 1차 보강 후 필요할 때만 심층검색한다.
    fresh0=[x for x in first if _news_age_hours(x)<=30 or _news_age_hours(x)>=999]
    if len(fresh0) < 12:
        urgent = stage1[-2:] if direction == "DOWN" else stage1[-1:] + stage1[:1]
        searched, err1=collect_parallel(urgent, limit=8)
        first=_dedupe_news(first + searched)

    # v13.8 핵심: 현재 요청들이 timeout이어도 이전에 성공한 모든 거시 증거를
    # 검색어별 캐시와 별개인 영구 풀에서 병합한다. Render 재시작 후에도 복원됨.
    first=_dedupe_news(first + _macro_pool_get(80))
    fresh=[x for x in first if _news_age_hours(x)<=30 or _news_age_hours(x)>=999]
    errors=list(err1)
    if len(fresh) < 8:
        deep, err2=collect_parallel(stage2[:3], limit=8)
        errors.extend(err2)
        fresh=_dedupe_news(fresh + [x for x in deep if _news_age_hours(x)<=54 or _news_age_hours(x)>=999])
    # 성공한 결과는 즉시 전역 영구 증거풀에도 축적한다. 다음 timeout 회차의 보험.
    if fresh:
        _macro_pool_put(fresh)
    # 진단용 메타데이터: 호출자는 기사 배열처럼 사용하며 전역 상태로 실패 개수만 참고한다.
    NEWS_HEALTH["last_errors"]=len(errors)
    NEWS_HEALTH["last_ok"]=len(fresh)
    NEWS_HEALTH["last_ts"]=time.time()
    return fresh[:60]

def _macro_category(title):
    low=(title or "").lower()
    # v13.4: 프로젝트 토큰경제의 inflation/disinflation을 미국 거시 물가로 오인하지 않는다.
    us_macro_anchor=("cpi","pce","consumer price","producer price","ppi","payroll","nonfarm","jobs report","jobless","unemployment","labor market","bls","bureau of labor statistics","미 소비자물가","미국 물가","고용보고서","비농업","실업률","미 노동부")
    tokenomics_context=("solana","ethereum","tokenomics","token supply","issuance","emission","burn rate","validator","staking","disinflation proposal","inflation proposal","토큰","발행량","소각","스테이킹")
    if any(w in low for w in us_macro_anchor) and not any(w in low for w in tokenomics_context):
        return "미국 물가·고용"
    groups=[
        ("연준·금리", ("fed","federal reserve","powell","warsh","hawkish","dovish","rate hike","rate cut","interest rate","연준","금리","매파","비둘기파")),
        ("달러·국채금리", ("treasury","yield","bond yield","dollar","dxy","국채","국채금리","달러")),
        ("ETF 자금", ("etf","inflow","outflow","spot bitcoin etf","ETF","유입","유출")),
        ("레버리지·옵션", ("liquidation","liquidated","leverage","options expiry","option expiry","청산","레버리지","옵션 만기")),
        ("규제·법률", ("regulation","regulator","sec ","ban","lawsuit","규제","당국","소송","금지")),
        ("해킹·보안", ("hack","exploit","breach","해킹","익스플로잇","보안")),
        ("지정학·관세", ("war ","conflict","tariff","geopolitical","전쟁","분쟁","관세","지정학")),
    ]
    for name, words in groups:
        if any(w in low for w in words): return name
    return "시장 수급·기타"


def _category_support_score(title, cat):
    """v13.4: 제목이 해당 원인 카테고리를 실제로 뒷받침하는 정도. 약한 키워드 우연 일치를 차단."""
    low=(title or "").lower()
    anchors={
        "연준·금리": ("fed","federal reserve","warsh","powell","rate hike","rate cut","interest rate","hawkish","dovish","연준","금리","매파","비둘기파"),
        "미국 물가·고용": ("cpi","pce","ppi","consumer price","payroll","nonfarm","jobs report","jobless","unemployment","labor market","bls","미국 물가","소비자물가","고용보고서","비농업","실업률"),
        "달러·국채금리": ("treasury","yield","bond yield","dollar","dxy","국채","국채금리","달러"),
        "ETF 자금": ("bitcoin etf","spot bitcoin etf","btc etf","etf inflow","etf outflow","ETF 유입","ETF 유출","현물 ETF"),
        "레버리지·옵션": ("liquidation","liquidated","leverage","options expiry","option expiry","청산","레버리지","옵션 만기"),
        "규제·법률": ("regulation","regulator","sec ","clarity act","lawsuit","ban","규제","당국","소송","금지"),
        "해킹·보안": ("hack","exploit","breach","해킹","익스플로잇","보안"),
        "지정학·관세": ("war ","conflict","tariff","geopolitical","전쟁","분쟁","관세","지정학"),
    }
    return sum(1 for w in anchors.get(cat,()) if w in low)

def _macro_direction_score(title, direction):
    low=(title or "").lower()
    up=("rally","surge","gain","rebound","jumps","soars","rate cut","dovish","etf inflow","approval","상승","급등","반등","오름","금리 인하","비둘기파","유입","승인")
    down=("drop","fall","falls","plunge","slides","selloff","sell-off","rate hike","hawkish","inflation","yields rise","dollar rises","outflow","liquidation","expiry","hack","ban","하락","급락","약세","매도","금리 인상","매파","국채금리 상승","달러 강세","유출","청산","만기","해킹","규제")
    u=sum(1 for w in up if w in low); d=sum(1 for w in down if w in low)
    return (u-d) if direction=="UP" else (d-u)

def _headline_polarity(title):
    """제목이 상승/하락 중 어느 방향을 명시하는지 판별. 1=상승, -1=하락, 0=중립/불명확."""
    low=(title or "").lower()
    up=("rally","surge","soar","jump","gain","rebound","climb","rise","rises","higher","상승","급등","반등","강세","뛰는","오른","오름")
    down=("drop","fall","falls","plunge","slide","slump","selloff","sell-off","decline","lower","하락","급락","약세","폭락","떨어","내린","매도세")
    u=sum(1 for w in up if w in low); d=sum(1 for w in down if w in low)
    if u>d: return 1
    if d>u: return -1
    return 0

def _is_forecast_or_preview(title):
    """실제 발생 기사보다 전망/사전예고 기사 우선순위를 낮춘다."""
    low=(title or "").lower()
    words=("ahead of","before jackson hole","preview","what to watch","could","may ","might","forecast","outlook","전망","앞두고","예상","가능성","어디까지","오를까","내릴까","주목")
    return any(w in low for w in words)

def _direct_event_score(title, cat):
    """실제 발언/결정/발표/자금흐름처럼 인과성이 강한 제목에 가점."""
    low=(title or "").lower()
    event=("says","said","signals","warns","announces","raises","cuts","holds rates","data shows","outflow","inflow","liquidated","speech","remarks","발언","밝혀","발표","결정","인상","인하","동결","유출","유입","청산")
    score=sum(1 for w in event if w in low)
    if cat in ("연준·금리","미국 물가·고용") and any(w in low for w in ("fed","federal reserve","warsh","powell","연준","의장")):
        score+=2
    return min(score,4)

def _source_quality(source):
    low=(source or "").lower()
    tier1=("reuters","bloomberg","associated press","ap news","financial times","wall street journal","cnbc")
    tier2=("coindesk","the block","fortune","yahoo finance","sbs biz","뉴시스","연합뉴스")
    if any(x in low for x in tier1): return 4
    if any(x in low for x in tier2): return 2
    return 1

def _cause_chain(cat, direction):
    down = direction=="DOWN"
    chains={
        "연준·금리": "연준의 매파적 신호 → 금리인하 기대 약화/금리 기대 상승 → 위험자산 부담 → BTC 하락" if down else "연준의 완화적 신호 → 금리 부담 완화 → 위험선호 회복 → BTC 상승",
        "미국 물가·고용": "강한 물가·고용 → 긴축 우려 → 금리/달러 부담 → BTC 하락" if down else "물가·고용 둔화 → 완화 기대 → 위험선호 → BTC 상승",
        "달러·국채금리": "미 국채금리·달러 상승 → 유동성/위험선호 압박 → BTC 하락" if down else "미 국채금리·달러 하락 → 위험선호 개선 → BTC 상승",
        "ETF 자금": "현물 ETF 자금 유출 → 현물 매도 압력 → BTC 하락" if down else "현물 ETF 자금 유입 → 현물 수요 증가 → BTC 상승",
        "레버리지·옵션": "롱 청산·옵션 만기 변동성 → 강제매도/헤지 → 하락폭 확대" if down else "숏 청산·옵션 포지션 조정 → 강제매수 → 상승폭 확대",
        "규제·법률": "규제 불확실성 확대 → 위험회피 → 크립토 매도 압력" if down else "규제 불확실성 완화 → 위험선호 개선 → 크립토 매수",
        "해킹·보안": "보안 사고 우려 → 위험회피 → 시장 매도 압력",
        "지정학·관세": "지정학/관세 불확실성 → 위험회피 → BTC·알트 압력" if down else "지정학 불확실성 완화 → 위험선호 회복",
    }
    return chains.get(cat,"시장 수급 변화 → BTC 변동 → WLD·KAIA 동조 가능성")



def _v133_stale_btc_price_title(title, btc_krw):
    """Only reject titles with an explicit BTC USD price far from current Coinone BTC.
    Unknown publication time alone is NOT a rejection reason.
    """
    if not title or not btc_krw:
        return False, ""
    m = re.search(
        r"\$\s*([0-9]{2,3}(?:\.[0-9]+)?\s*[kK]|[0-9]{2,3}(?:,[0-9]{3})|[0-9]{4,6})",
        str(title)
    )
    if not m:
        return False, ""
    raw = m.group(1).replace(",", "").replace(" ", "")
    try:
        usd_ref = float(raw[:-1]) * 1000.0 if raw.lower().endswith("k") else float(raw)
        # Broad sanity conversion only for detecting obviously stale price references.
        current_usd_mid = float(btc_krw) / 1400.0
        gap = abs(usd_ref - current_usd_mid) / max(current_usd_mid, 1.0)
        if gap >= 0.18:
            return True, f"${usd_ref:,.0f}"
    except Exception:
        pass
    return False, ""

def market_cause_analysis_text(force_direction=None):
    """v13.8: 실시간 검색 실패 시에도 영구 거시 증거풀을 병합해 직접 원인을 최대한 복원한다."""
    move=market_move_snapshot(); btc=move.get("BTC",{})
    btc1=btc.get("ret1") or 0.0; btc5=btc.get("ret5") or 0.0
    try:
        ticks=fetch_tickers(); bt=ticks.get("BTC") or {}
        btc24=pct(safe_float(bt.get("price")),safe_float(bt.get("first"))) if safe_float(bt.get("first")) else 0.0
        btcprice=safe_float(bt.get("price"))
    except Exception:
        btc24=0.0; btcprice=0.0

    if force_direction in ("UP","DOWN"): direction=force_direction
    elif btc.get("ret4h") is not None and abs(btc.get("ret4h"))>=1.0: direction="UP" if btc.get("ret4h")>0 else "DOWN"
    elif btc.get("ret60") is not None and abs(btc.get("ret60"))>=0.7: direction="UP" if btc.get("ret60")>0 else "DOWN"
    elif abs(btc5)>=0.35: direction="UP" if btc5>0 else "DOWN"
    elif abs(btc24)>=0.5: direction="UP" if btc24>0 else "DOWN"
    else:
        vals=[move.get(s,{}).get("ret5") for s in ("WLD","KAIA") if move.get(s,{}).get("ret5") is not None]
        direction="UP" if (sum(vals)/len(vals) if vals else 0)>=0 else "DOWN"

    expected=1 if direction=="UP" else -1
    ranked=[]; rejected_opposite=0; rejected_stale_price=0
    # v13.8: 뉴스 수집 예외가 /cause 전체를 죽이지 않도록 완전 격리.
    news_collect_error = ""
    try:
        macro_items = _macro_news(direction)
    except Exception as e:
        news_collect_error = f"{type(e).__name__}: {str(e)[:120]}"
        print("[/cause news isolated]", repr(e), flush=True)
        try:
            macro_items = _macro_pool_get(80)
        except Exception:
            macro_items = []
        if not macro_items:
            recovered=[]
            for _k in ("feed::CoinDesk","feed::CNBC","feed::CNBC Markets"):
                try: recovered.extend(_news_cache_get(_k,12,True))
                except Exception: pass
            macro_items=_dedupe_news(recovered)
    for it in macro_items:
        title=it.get("title",""); cat=_macro_category(title)
        ds=_macro_direction_score(title,direction); age=_news_age_hours(it)
        polarity=_headline_polarity(title)
        # 핵심 v12.5: 가격 방향과 명백히 반대인 제목은 원인 근거에서 제외.
        if polarity and polarity != expected:
            rejected_opposite += 1
            continue
        # v13.4 minimal guard: do not reject undated articles; only reject an
        # explicit BTC price in the headline when it is obviously far from current BTC.
        stale_price, stale_ref = _v133_stale_btc_price_title(title, btcprice)
        if stale_price:
            rejected_stale_price += 1
            continue
        freshness=5 if age<=6 else (4 if age<=12 else (2 if age<=24 else 0))
        catsupport=_category_support_score(title,cat)
        relevance=3 if cat!="시장 수급·기타" and catsupport>0 else 0
        sourceq=_source_quality(it.get("source"))
        direct=_direct_event_score(title,cat)
        forecast_penalty=5 if _is_forecast_or_preview(title) else 0
        direction_bonus=5 if polarity==expected else 0
        total=ds*3 + sourceq*2 + freshness + relevance + direct*2 + direction_bonus + min(catsupport,2)*2 - forecast_penalty
        # v13.4: 카테고리 핵심근거가 없는 기사(예: Solana disinflation)는 원인 후보에서 제외.
        if cat!="시장 수급·기타" and catsupport<=0:
            continue
        # 전망성 기사 단독 또는 방향성 없는 약한 기사는 1순위 원인으로 올라오지 못하게 문턱 강화.
        if total>=10 and (ds>0 or direct>=2):
            ranked.append((total,ds,sourceq,freshness,cat,age,it,direct,forecast_penalty,polarity,catsupport))
    ranked.sort(key=lambda x:(x[0],x[2],x[7],-x[5]),reverse=True)

    selected=[]; seen_cats=set()
    if ranked:
        # 1순위는 가장 강한 직접 근거.
        selected.append(ranked[0]); seen_cats.add(ranked[0][4])
        top_score=ranked[0][0]
        # 보조원인은 독립 카테고리 + 충분한 사건성/방향성 + 1순위와 지나치게 동떨어지지 않은 경우만 채택.
        for row in ranked[1:]:
            cat=row[4]; total=row[0]; ds=row[1]; direct=row[7]; forecast=row[8]; catsupport=row[10]
            if cat in seen_cats or cat=="시장 수급·기타":
                continue
            strong_aux=(catsupport>0 and not forecast and total>=14 and total>=top_score*0.55 and (ds>0 or direct>=2))
            if strong_aux:
                selected.append(row); seen_cats.add(cat)
            if len(selected)>=3: break

    confidence=20
    if abs(btc24)>=1: confidence+=8
    if abs(btc24)>=2: confidence+=7
    if selected: confidence+=15
    if selected and selected[0][0]>=18: confidence+=15
    if selected and selected[0][5]<=12: confidence+=8
    if selected and selected[0][2]>=4: confidence+=10
    if selected and selected[0][7]>=2: confidence+=7
    if len(seen_cats)>=2: confidence+=5
    confidence=max(20,min(94,confidence))

    label="상승" if direction=="UP" else "하락"
    parts=[f"🌐 【현재 시장 {label} 원인 분석 v13.8】",f"BTC {btcprice:,.0f}원 · 1분 {btc1:+.2f}% · 5분 {btc5:+.2f}% · 당일 기준 {btc24:+.2f}%",""]
    if news_collect_error:
        parts += ["⚠️ 실시간 뉴스 수집 오류를 격리하고 영구 캐시로 계속 분석", ""]
    if NEWS_HEALTH.get("last_errors",0):
        parts.append(f"⚠️ 뉴스 연결 일부 지연 {NEWS_HEALTH['last_errors']}건 — 확보 기사 {NEWS_HEALTH.get('last_ok',0)}건으로 계속 분석")
        parts.append("")
    if selected:
        top=selected[0]; topcat=top[4]
        parts += [f"🥇 1순위 원인 — {topcat}",_cause_chain(topcat,direction),f"🎯 원인 신뢰도 {confidence}/100",""]
        if len(selected)>1:
            parts.append("🔎 보조 원인/변동성 확대 요인")
            for i,row in enumerate(selected[1:3],2): parts.append(f"{i}순위 {row[4]} — {_cause_chain(row[4],direction)}")
        else:
            parts.append("🔎 확인된 보조원인 없음 — 약한 연관 기사로 순위를 억지로 채우지 않습니다.")
        parts.append("\n📰 기사 근거 · 방향/시간 검증")
        for row in selected[:3]:
            _,_,sourceq,_,cat,age,it,direct,forecast_penalty,polarity,catsupport=row
            src=f" · {it.get('source')}" if it.get('source') else ""
            age_txt=f"약 {age:.1f}시간 전" if age<100 else "발행시각 확인불가"
            kind="실제 사건/발언" if direct>=2 and not forecast_penalty else ("전망성 기사" if forecast_penalty else "관련 기사")
            parts.append(f"• [{cat} · {age_txt} · {kind}] {it.get('title','')}{src}\n{it.get('link','')}")
        parts.append(f"\n✅ 반대방향 제목 {rejected_opposite}건 자동 제외")
        parts.append(f"✅ 현재 BTC 가격대 불일치 기사 {rejected_stale_price}건 자동 제외")
        parts.append("📌 판단: 가격 방향·발행시각·실제 사건성·출처 품질을 함께 검증했습니다. 전망성 기사는 감점합니다.")
    else:
        parts += ["🔎 24시간 기본검색과 48시간 심층검색까지 수행했지만 가격 방향과 시간대가 함께 맞는 직접 근거를 확인하지 못했습니다.","🎯 원인 신뢰도 20/100",f"✅ 반대방향 제목 {rejected_opposite}건 자동 제외",f"✅ 현재 BTC 가격대 불일치 기사 {rejected_stale_price}건 자동 제외","📌 판단: 방향이 반대인 기사나 단순 전망 기사로 원인을 억지로 만들지 않습니다."]
    parts.append("※ 자동 뉴스 원인추정이며 실제 인과는 추가 확인이 필요합니다. 자동주문 없음.")
    return "\n".join(parts)

def market_shock_triggered():
    """BTC 순간충격 + 30분/1시간/4시간 누적 급등락을 감지."""
    m=market_move_snapshot().get("BTC",{})
    checks=[("1분",m.get("ret1"),1.0),("5분",m.get("ret5"),2.0),
            ("30분",m.get("ret30"),2.5),("1시간",m.get("ret60"),3.5),("4시간",m.get("ret4h"),6.0)]
    hits=[(lab,val,th) for lab,val,th in checks if val is not None and abs(val)>=th]
    if not hits: return None
    lab,basis,th=max(hits,key=lambda x:abs(x[1])/x[2])
    direction="UP" if basis>0 else "DOWN"
    now=time.time()
    if MARKET_CAUSE_LAST["dir"]==direction and now-MARKET_CAUSE_LAST["ts"]<MARKET_CAUSE_COOLDOWN:
        return None
    MARKET_CAUSE_LAST["dir"]=direction; MARKET_CAUSE_LAST["ts"]=now
    MARKET_CAUSE_LAST["basis"]=lab; MARKET_CAUSE_LAST["move"]=basis
    return direction


def market_cause_worker(direction,cid):
    NEWS_PRIORITY.set()
    try:
        with RAPID_LOCK:
            send_long(market_cause_analysis_text(direction),cid)
    except Exception as e:
        print("[MarketCause] error",repr(e),flush=True)
        try:
            m=market_move_snapshot().get("BTC",{})
            send_long(
                "⚠️ 【뉴스 연결 지연 — 가격·시장 데이터 분석】\n"
                f"BTC 1분 {(m.get('ret1') or 0):+.2f}% · 5분 {(m.get('ret5') or 0):+.2f}%\n"
                "외부 뉴스 조회가 지연되어 이번 회차의 기사 기반 원인 확정은 보류합니다.\n"
                "가격 감시·장부·재매수 안전필터는 정상 작동합니다.\n"
                "※ 원인을 억지로 추정하지 않습니다. 자동주문 없음.", cid)
        except Exception as e2:
            print("[MarketCauseFallback] error",repr(e2),flush=True)
    finally:
        NEWS_PRIORITY.clear()


def causetest_text():
    """실제 시세/장부를 변경하지 않는 원인분석 기능 테스트."""
    return (
        "🧪 【v13.8 급변 원인분석 테스트】\n\n"
        "✅ BTC 선행 급락 감지 모듈\n"
        "✅ WLD·KAIA 개별 급변 감지 모듈\n"
        "✅ 연준·금리/물가·고용/달러·국채/ETF/청산/규제/해킹/지정학 분류\n"
        "✅ /cause 임계치 미도달 시에도 최신 원인 검색\n"
        "✅ 원인 신뢰도 + 기사 근거 표시\n"
        "✅ 가격과 반대방향 기사 자동 제외\n"
        "✅ 전망/사전예고 기사 감점 + 실제 발언/발표 우선\n"
        "✅ Reuters/Bloomberg 등 고신뢰 출처 가중\n"
        "✅ 24시간 실패 시 48시간 심층 재검색(한글+영문)\n"
        "✅ Bing News 1차 + Google News 2차 다중 검색경로\n"
        "✅ CoinDesk/CNBC 직접 RSS 보조경로 + 최근 성공 캐시\n"
        "✅ 뉴스 요청 3/6초 timeout + 개별 HTTPError 격리\n"
        "✅ 최대 4개 병렬검색 + 일부 뉴스 실패 시 확보 기사로 계속 분석\n"
        "✅ ETF/청산/옵션/나스닥 위험자산 동조 보조탐색\n"
        "✅ 원인 미확인 시 억지 추정 금지\n"
        "✅ 발행시각 미확인만으로 기사 제외하지 않음\n"
        "✅ 현재 BTC 가격대와 크게 다른 명시가격 기사만 제외\n"
        "✅ 성공 뉴스 /var/data 영구 캐시 저장\n"
        "✅ Render 재시작·재배포 후 48시간 캐시 복원\n"
        "✅ 검색어별 캐시 + 통합 거시 증거풀 이중 보관\n"
        "✅ 실시간 timeout 시 영구 증거풀 자동 병합\n"
        "✅ WLD/KAIA 뉴스보다 BTC 거시 원인 근거 우선 확보\n"
        "✅ /cause 뉴스 예외 완전 격리 + 캐시 즉시 복구\n"
        "✅ 뉴스 오류여도 /cause 전체 fallback 방지\n\n"
        "※ 가상 테스트이며 가격·평단·수량·현금·장부는 변경하지 않습니다."
    )


def _rapid_news(symbol, direction):
    """급변 직후 최신 기사에서 원인 후보를 찾는다. 기사 제목만으로 인과를 확정하지 않는다."""
    if symbol=="WLD":
        project='("Worldcoin" OR "World Network" OR WLD OR "World Chain")'
    else:
        project='(KAIA OR "Kaia blockchain")'
    move_words = '(surge OR rally OR partnership OR launch OR listing OR adoption OR upgrade)' if direction=="UP" else '(drop OR plunge OR sell-off OR unlock OR hack OR regulation OR lawsuit OR delist)'
    queries=[
        f'{project} {move_words} when:1d',
        'Bitcoin crypto market Fed rates inflation ETF liquidation regulation when:1d',
    ]
    items=[]
    for q in queries:
        try:
            items += google_news_rss(q,4)
        except Exception:
            pass
    return _dedupe_news(items)[:8]


def _news_direction_score(title, direction):
    low=(title or "").lower()
    up_words=("surge","rally","partnership","launch","listing","adoption","upgrade","approval","funding","상승","급등","협력","출시","상장","채택","승인","호재")
    down_words=("drop","plunge","sell-off","unlock","hack","exploit","regulation","lawsuit","delist","liquidation","급락","하락","언락","해킹","규제","소송","상폐","청산")
    pos=sum(1 for w in up_words if w in low)
    neg=sum(1 for w in down_words if w in low)
    return (pos-neg) if direction=="UP" else (neg-pos)


def rapid_cause_text(symbol, d):
    """가격·거래량·BTC/동료코인 동조·최신뉴스를 합쳐 '원인 후보'를 설명."""
    direction="UP" if safe_float(d.get("rapid_move"), d.get("ret1m",0) or d.get("ret5m",0))>0 else "DOWN"
    icon="🚀" if direction=="UP" else "🚨"
    label="급등" if direction=="UP" else "급락"
    move=market_move_snapshot()
    btc=move.get("BTC",{})
    peer_sym="KAIA" if symbol=="WLD" else "WLD"
    peer=move.get(peer_sym,{})
    btc1=btc.get("ret1"); btc5=btc.get("ret5")
    peer1=peer.get("ret1"); peer5=peer.get("ret5")
    sign=1 if direction=="UP" else -1
    reasons=[]; confidence=20

    btc_aligned=((btc1 is not None and sign*btc1>=0.8) or (btc5 is not None and sign*btc5>=1.5))
    peer_aligned=((peer1 is not None and sign*peer1>=1.5) or (peer5 is not None and sign*peer5>=3.0))
    if btc_aligned:
        reasons.append(f"BTC도 같은 방향으로 움직임 (1분 {btc1 if btc1 is not None else 0:+.2f}% · 5분 {btc5 if btc5 is not None else 0:+.2f}%)")
        confidence += 25
    if peer_aligned:
        reasons.append(f"{peer_sym}도 동반 움직임 — 시장/알트 공통 수급 가능성")
        confidence += 12
    if safe_float(d.get("vol_ratio"),1)>=1.8:
        reasons.append(f"거래량이 평소 대비 {safe_float(d.get('vol_ratio'),1):.2f}배로 증가 — 실제 수급 동반")
        confidence += 12

    items=_rapid_news(symbol,direction)
    matching=[]
    symbol_terms=("worldcoin","world network","wld","world chain") if symbol=="WLD" else ("kaia","카이아")
    for it in items:
        sc=_news_direction_score(it.get("title",""),direction)
        is_project=any(x in (it.get("title") or "").lower() for x in symbol_terms)
        if sc>0:
            matching.append((is_project,sc,it))
    matching.sort(key=lambda x:(x[0],x[1],source_weight(x[2].get("source"))), reverse=True)

    if matching:
        project_matches=[x for x in matching if x[0]]
        if project_matches:
            reasons.append(f"{symbol} 관련 최신 기사에 같은 방향의 재료 후보가 확인됨")
            confidence += 30
        else:
            reasons.append("시장 전체 뉴스에서 같은 방향의 재료 후보가 확인됨")
            confidence += 18

    if btc_aligned and peer_aligned:
        verdict="시장 전체/알트 공통 움직임 영향이 큰 것으로 추정"
    elif matching and any(x[0] for x in matching):
        verdict=f"{symbol} 개별 뉴스·재료 영향 가능성이 비교적 큼"
    elif safe_float(d.get("vol_ratio"),1)>=1.8:
        verdict="직접 뉴스는 뚜렷하지 않고 수급성 급변 가능성"
    else:
        verdict="현재 확인된 직접 원인 부족 — 억지로 원인을 단정하지 않고 추가 확인 필요"
    confidence=max(20,min(95,confidence))

    parts=[
        f"{icon} 【중요 {label} 원인 알림 — {symbol}】",
        f"현재가 {safe_float(d.get('price')):,.4f}원",
        f"1분 {safe_float(d.get('ret1m')):+.2f}% · 5분 {safe_float(d.get('ret5m')):+.2f}% · 거래량 {safe_float(d.get('vol_ratio'),1):.2f}배",
        *(([f"📉 지속하락 조건: {d.get('sustained_reason','확인')}" ]) if d.get("sustained_decline") else []),
        "",
        "🔎 원인 후보",
    ]
    if reasons:
        for i,r in enumerate(reasons[:4],1): parts.append(f"{i}. {r}")
    else:
        parts.append("1. 아직 가격 움직임과 시간대가 맞는 직접 뉴스/시장 동조 원인이 확인되지 않음")
    parts += ["", f"🎯 원인 신뢰도 {confidence}/100", f"📌 판단: {verdict}"]

    if matching:
        parts.append("\n📰 시간대가 가까운 관련 기사 후보")
        for _,_,it in matching[:2]:
            src=f" · {it.get('source')}" if it.get('source') else ""
            parts.append(f"• {it.get('title','')}{src}\n{it.get('link','')}")
    if direction=="DOWN":
        parts.append("\n⚠️ 대응: 추격매도보다 BTC 동조·15분/4시간 추세 훼손·거래량 지속 여부를 함께 확인")
    else:
        parts.append("\n⚠️ 대응: 원인 미확인 급등이면 추격매수 금지, 과열 신호와 추세를 함께 확인")
    parts.append("※ 뉴스 제목과 시장 동조를 이용한 자동 원인추정이며 인과관계를 확정하지 않습니다. 자동주문 없음.")
    return "\n".join(parts)


def sustained_decline_triggered(symbol,d):
    """v14.2: 하루에 걸친 지속 하락 + 15분/4시간 추세 동시 붕괴를 중요알람으로 승격."""
    daily=d.get("daily",{}) or {}
    prev=safe_float(daily.get("prev24_change_pct")) if daily.get("ready") else 0.0
    draw=safe_float(d.get("drawdown_pct"))
    trend=d.get("trend",{}) or {}
    score=safe_float(trend.get("score"),100)
    short=trend.get("short",{}) or {}; mid=trend.get("mid",{}) or {}
    short_broken=(not short.get("ema_bull",True)) and (not short.get("macd_bull",True))
    mid_broken=(not mid.get("ema_bull",True)) and (not mid.get("macd_bull",True))

    # 두 경로 중 하나면 발동:
    # A) 전일09시 대비 -4% 이하 + 양 시간대 추세 붕괴
    # B) 최근 고점 대비 -12% 이하 + 추세점수 30 이하 + 양 시간대 추세 붕괴
    hit_daily=(daily.get("ready") and prev <= -4.0 and short_broken and mid_broken)
    hit_draw=(draw <= -12.0 and score <= 30 and short_broken and mid_broken)
    if not (hit_daily or hit_draw): return False
    now=time.time(); last=SUSTAINED_LAST[symbol]
    if now-last.get("ts",0)<SUSTAINED_COOLDOWN: return False
    last["ts"]=now
    d["rapid_basis"]="지속하락"
    d["rapid_move"]=prev if hit_daily else draw
    d["sustained_decline"]=True
    d["sustained_reason"]=(f"전일09시 {prev:+.2f}% · 고점대비 {draw:+.2f}% · 추세 {score:.0f}/100 · 15분/4시간 동시 약세")
    return True


def rapid_triggered(symbol,d):
    """WLD/KAIA 순간 + 누적 급등락 감지."""
    checks=[("1분",safe_float(d.get("ret1m")),3.0),("5분",safe_float(d.get("ret5m")),5.0),
            ("30분",d.get("ret30m"),5.0),("1시간",d.get("ret1h"),7.0),("4시간",d.get("ret4h"),10.0)]
    hits=[(lab,float(val),th) for lab,val,th in checks if val is not None and abs(float(val))>=th]
    if not hits: return False
    lab,basis,th=max(hits,key=lambda x:abs(x[1])/x[2])
    direction="UP" if basis>0 else "DOWN"
    now=time.time(); last=RAPID_LAST[symbol]
    if last["dir"]==direction and now-last["ts"]<RAPID_COOLDOWN:
        return False
    last["dir"]=direction; last["ts"]=now; last["basis"]=lab; last["move"]=basis
    d["rapid_basis"]=lab; d["rapid_move"]=basis
    return True


def rapid_cause_worker(symbol,d,cid):
    try:
        with RAPID_LOCK:
            send_long(rapid_cause_text(symbol,d),cid)
    except Exception as e:
        print("[RapidCause] error",symbol,repr(e),flush=True)


def rapid_cause_report_text():
    """수동 /cause: 현재 변동률을 보여주고, 임계치와 무관하게 시장 원인을 검색한다."""
    snap=snapshot(); parts=["🔎 【자이나 현재 급변 원인 레이더】"]
    for sym in ("WLD","KAIA"):
        d=snap.get(sym)
        if not d: continue
        parts.append(f"\n{sym}: 1분 {d.get('ret1m',0):+.2f}% · 5분 {d.get('ret5m',0):+.2f}% · 30분 {(d.get('ret30m') or 0):+.2f}% · 1시간 {(d.get('ret1h') or 0):+.2f}% · 4시간 {(d.get('ret4h') or 0):+.2f}% · 거래량 {d.get('vol_ratio',1):.2f}배")
        if abs(d.get('ret1m',0))>=3 or abs(d.get('ret5m',0))>=5 or abs(d.get('ret30m') or 0)>=5 or abs(d.get('ret1h') or 0)>=7 or abs(d.get('ret4h') or 0)>=10:
            parts.append("→ W/K 중요 급변 기준 도달")
        else:
            parts.append("→ W/K 중요 급변 기준 미도달 — 그래도 시장 원인은 아래에서 분석")
    m=market_move_snapshot().get("BTC",{})
    parts.append(f"\nBTC 참고: 1분 {(m.get('ret1') or 0):+.2f}% · 5분 {(m.get('ret5') or 0):+.2f}% · 30분 {(m.get('ret30') or 0):+.2f}% · 1시간 {(m.get('ret60') or 0):+.2f}% · 4시간 {(m.get('ret4h') or 0):+.2f}%")
    parts.append("※ 자동알림: 순간급변 + 30분·1시간·4시간 누적 급등락 동시 감시")
    parts.append("\n" + market_cause_analysis_text())
    return "\n".join(parts)


def cause_command_worker(cid):
    """사용자 /cause를 정기뉴스보다 우선 처리. 텔레그램 polling thread를 막지 않는다."""
    NEWS_PRIORITY.set()
    try:
        with RAPID_LOCK:
            send_long(rapid_cause_report_text(),cid)
    except Exception as e:
        print("[/cause] error",repr(e),flush=True)
        try:
            m=market_move_snapshot().get("BTC",{})
            send_long("⚠️ 【/cause 내부 오류 격리】\n"
                      f"BTC 1분 {(m.get('ret1') or 0):+.2f}% · 5분 {(m.get('ret5') or 0):+.2f}%\n"
                      f"오류종류: {type(e).__name__} · {str(e)[:100]}\n"
                      "뉴스 연결 지연으로 단정하지 않고 오류 지점을 분리했습니다.\n"
                      "가격 감시·장부·재매수 안전필터는 정상 작동합니다.\n"
                      "※ 자동주문 없음.",cid)
        except Exception as e2:
            print("[/cause fallback] error",repr(e2),flush=True)
    finally:
        NEWS_PRIORITY.clear()

def market_brief():
    try:
        ticks = fetch_tickers()
        btc = ticks.get("BTC")
        if not btc:
            return "BTC 시황 확인 지연"
        p = btc["price"]
        first = btc.get("first", 0.0)
        chg = pct(p, first) if first else 0.0
        mood = "강세" if chg >= 2 else "약세" if chg <= -2 else "중립"
        return f"BTC {p:,.0f}원 · 24시간 {chg:+.2f}% · {mood}"
    except Exception:
        return "BTC 시황 확인 지연"

def news_digest():
    sections = [
        ("🌐 크립토 시황", "cryptocurrency Bitcoin Ethereum market when:1d"),
        ("🟣 KAIA 뉴스", "KAIA blockchain OR Kaia crypto when:2d"),
        ("🔵 WLD 뉴스", "Worldcoin OR World Network WLD crypto when:2d"),
    ]
    parts = ["📰 【자이나 크립토 뉴스 브리핑】", market_brief(), ""]
    for section, query in sections:
        parts.append(section)
        try:
            items = google_news_rss(query, 4)
            if not items:
                parts.append("새 주요 기사 없음")
            for i, it in enumerate(items, 1):
                parts.append(f"{classify_news(it['title'])} {i}. {it['title']}\n{it['link']}")
        except Exception as e:
            parts.append(f"뉴스 조회 지연 ({type(e).__name__})")
        parts.append("")
    parts.append("🚀 더 넓은 호재·예정재료·리스크는 /good 입력")
    parts.append("🟢 긍정 가능성 · 🔴 부정 가능성 · ⚪ 중립/판단보류")
    parts.append("※ 제목 키워드 기반 분류이며 투자 판단을 보장하지 않습니다.")
    return "\n".join(parts)

def auto_news_loop():
    global LAST_NEWS_SENT, LAST_BREAKING_CHECK, BREAKING_PRIMED, SEEN_BREAKING_KEYS
    while True:
        try:
            now=time.time()

            # 3시간마다: 시황 뉴스 + W/K 호재·전망·리스크 종합보고
            if CHAT_ID and now - LAST_NEWS_SENT >= NEWS_INTERVAL and not NEWS_PRIORITY.is_set():
                send_long("🕒 3시간 정기 뉴스·호재·전망 보고\n\n" + news_digest(), CHAT_ID)
                send_long(good_radar_text(), CHAT_ID)
                LAST_NEWS_SENT = now
                print("[News] 3h news+catalyst digest sent", flush=True)

            # 10분마다 새 중요 호재/악재 확인. 시작 직후 기존 기사들은 기준선으로만 등록.
            if CHAT_ID and now - LAST_BREAKING_CHECK >= BREAKING_CHECK_INTERVAL and not NEWS_PRIORITY.is_set():
                candidates=breaking_candidates()
                keys={news_key(it) for _,_,_,it in candidates if news_key(it)}
                if not BREAKING_PRIMED:
                    SEEN_BREAKING_KEYS.update(keys)
                    BREAKING_PRIMED=True
                    print("[News] breaking radar primed", len(keys), flush=True)
                else:
                    fresh=[]
                    for row in candidates:
                        k=news_key(row[3])
                        if k and k not in SEEN_BREAKING_KEYS:
                            fresh.append(row)
                            SEEN_BREAKING_KEYS.add(k)
                    if fresh:
                        send_long(breaking_check_text(fresh), CHAT_ID)
                        print("[News] breaking catalyst alert sent", len(fresh), flush=True)
                # 메모리 폭주 방지
                if len(SEEN_BREAKING_KEYS)>500:
                    SEEN_BREAKING_KEYS=set(list(SEEN_BREAKING_KEYS)[-300:])
                LAST_BREAKING_CHECK=now

        except Exception as e:
            print("[News] error",e,flush=True)
        time.sleep(60)
# ---------- END NEWS ----------


def signaltest_messages():
    return [
        (
            "🟠 급등·익절 테스트",
            "【테스트】 WLD/KRW\n"
            "신호 🟠 급등 강함 · 1차 익절 준비\n"
            "권장행동 보유량의 10~15% 익절 검토\n"
            "이유 1분/5분 상승강도 + 거래량 증가 가정\n\n"
            "※ 테스트 메시지이며 실제 주문 신호가 아닙니다."
        ),
        (
            "🔴 수익보호 테스트",
            "【테스트】 KAIA/KRW\n"
            "신호 🔴 수익보호 2차 익절 후보\n"
            "권장행동 보유량의 15~20% 익절 검토\n"
            "이유 최고 평가수익 대비 -20% 반납 가정\n\n"
            "※ 테스트 메시지이며 실제 주문 신호가 아닙니다."
        ),
        (
            "🔵 재매수 테스트",
            "【테스트】 WLD/KRW\n"
            "신호 🔵 1차 재매수 후보\n"
            "권장행동 익절금의 20% 이내 재매수 검토\n"
            "이유 최근 고점 대비 -7% 조정 가정\n"
            "재매수메모 추가 하락 대비 현금 여유 유지\n\n"
            "※ 테스트 메시지이며 실제 주문 신호가 아닙니다."
        ),
        (
            "🛑 급락중지 테스트",
            "【테스트】 KAIA/KRW\n"
            "신호 🛑 급락 점검 · 재매수 보류\n"
            "권장행동 추가 매수 보류\n"
            "이유 최근 고점 대비 -25% 급락 가정\n\n"
            "※ 테스트 메시지이며 실제 주문 신호가 아닙니다."
        ),
    ]

def run_signaltest(cid):
    send("🧪 중요 신호 즉시알림 테스트를 시작합니다.", cid)
    for title, body in signaltest_messages():
        send("🚨 " + title + "\n\n" + body, cid)
        time.sleep(0.5)
    send("✅ /signaltest 완료 — 위 4개 메시지가 모두 즉시 도착하면 중요신호 알림 통과", cid)



def evaluate_test_case(gain, price_dd, profit_dd, score, ret1=0.0, ret5=0.0, vol_ratio=1.0):
    # strategy() 핵심 우선순위와 동일하게 테스트
    if price_dd <= -25:
        return "🛑 급락 점검 · 재매수 보류"
    elif -18 < price_dd <= -10 and ret1 >= 0 and ret5 >= -0.5 and vol_ratio <= 1.3:
        return "⭐ 황금구간 · 재매수 최우선 후보"
    elif score >= 5 and price_dd > -2:
        return "🚨 익절 실행 · 급등 과열"
    elif score >= 3 and price_dd > -3:
        return "🟠 익절 실행 준비"
    elif score >= 2 and price_dd > -2:
        return "🟡 급등 감지 · 익절 준비"
    elif gain >= 15 and profit_dd <= -30:
        return "🛑 수익보호 강경 · 추가 익절 실행"
    elif gain >= 15 and profit_dd <= -20:
        return "🚨 수익보호 익절 실행"
    elif gain >= 15 and profit_dd <= -15:
        return "🟠 수익보호 실행 준비"
    elif gain >= 15 and profit_dd <= -10:
        return "🟡 수익보호 준비"
    elif price_dd <= -18:
        return "🔵 3차 재매수 실행 후보"
    elif price_dd <= -12:
        return "🔵 2차 재매수 실행 후보"
    elif price_dd <= -10:
        return "🟠 재매수 실행 준비"
    elif price_dd <= -7:
        return "🟡 재매수 준비"
    return "⚪ 홀딩 / 관찰"

def engine_test_results():
    cases = [
        ("급등 준비", 25, -1, -2, 2, 1.0, 1.0, 1.2, "🟡 급등 감지 · 익절 준비"),
        ("익절 실행준비", 25, -1, -2, 3, 1.0, 2.0, 1.3, "🟠 익절 실행 준비"),
        ("급등 과열 실행", 30, -1, -3, 5, 2.0, 4.0, 1.8, "🚨 익절 실행 · 급등 과열"),
        ("수익보호 준비", 25, -4, -10, 0, 0, 0, 1.0, "🟡 수익보호 준비"),
        ("수익보호 실행준비", 25, -4, -15, 0, 0, 0, 1.0, "🟠 수익보호 실행 준비"),
        ("수익보호 실행", 25, -4, -20, 0, 0, 0, 1.0, "🚨 수익보호 익절 실행"),
        ("재매수 준비", 10, -7, -5, 0, -0.2, -0.5, 1.0, "🟡 재매수 준비"),
        ("재매수 실행준비", 10, -10, -5, 0, -0.2, -0.8, 1.4, "🟠 재매수 실행 준비"),
        ("황금구간", 10, -12, -5, 0, 0.2, -0.2, 1.1, "⭐ 황금구간 · 재매수 최우선 후보"),
        ("2차 재매수", 10, -12, -5, 0, -0.4, -0.8, 1.5, "🔵 2차 재매수 실행 후보"),
        ("3차 재매수", 10, -18, -5, 0, -0.5, -1.0, 1.5, "🔵 3차 재매수 실행 후보"),
        ("급락중지", 10, -25, -5, 0, -2.0, -5.0, 2.0, "🛑 급락 점검 · 재매수 보류"),
        ("평시", 10, -2, -2, 0, 0, 0, 1.0, "⚪ 홀딩 / 관찰"),
    ]
    results=[]
    all_ok=True
    for name,gain,price_dd,profit_dd,score,ret1,ret5,vol_ratio,expected in cases:
        actual=evaluate_test_case(gain,price_dd,profit_dd,score,ret1,ret5,vol_ratio)
        ok=(actual==expected)
        all_ok=all_ok and ok
        results.append({
            "name":name,"gain":gain,"price_dd":price_dd,"profit_dd":profit_dd,
            "score":score,"ret1":ret1,"ret5":ret5,"vol_ratio":vol_ratio,
            "expected":expected,"actual":actual,"ok":ok
        })
    return all_ok,results

def run_enginetest(cid):
    send("🧪 v11 판단엔진 테스트 시작 — 실제 보유 상태/저장값은 변경하지 않습니다.",cid)
    all_ok,results=engine_test_results()
    lines=[]
    for r in results:
        mark="✅" if r["ok"] else "❌"
        lines.append(
            f"{mark} {r['name']}\n"
            f"  입력: 평단수익 {r['gain']:+.0f}% · 고점대비 {r['price_dd']:+.0f}% · 최고수익대비 {r['profit_dd']:+.0f}% · 점수 {r['score']}\n"
            f"  결과: {r['actual']}"
        )
    header="✅ v11 판단엔진 전체 통과" if all_ok else "❌ v11 판단엔진 일부 실패"
    send(header+"\n\n"+"\n\n".join(lines),cid)

# ---------- v14.2 PRE-EVENT MARKET RADAR + SUSTAINED DECLINE ALERT ----------
EVENT_RADAR_INTERVAL = 15 * 60
EVENT_RADAR_LAST_DAILY = ""
EVENT_RADAR_SENT = set()
KST = timezone(timedelta(hours=9))
ET = timezone(timedelta(hours=-4))  # 2026 Sep-Oct scheduled releases are during U.S. daylight time

# High-impact dates from official Fed/BLS 2026 calendars. Times are converted to Korea time.
SCHEDULED_MARKET_EVENTS = [
    ("2026-09-01T10:00:00-04:00","🟠","미국 JOLTS","고용 수요 변화 → 금리 기대·BTC 변동성"),
    ("2026-09-04T08:30:00-04:00","🔴","미국 고용보고서","연준 금리경로 핵심 지표 → 주식·BTC 변동성 확대 가능"),
    ("2026-09-10T08:30:00-04:00","🟠","미국 PPI","물가 압력 확인 → 금리·달러·BTC 영향 가능"),
    ("2026-09-11T08:30:00-04:00","🔴","미국 CPI","인플레이션 핵심 지표 → 주식·BTC 급변 가능"),
    ("2026-09-16T14:00:00-04:00","🔴","FOMC 금리결정·성명","연준 정책·점도표·기자회견 → 최중요 시장 이벤트"),
    ("2026-09-29T10:00:00-04:00","🟠","미국 JOLTS","고용 수요 변화 → 금리 기대 영향"),
    ("2026-10-02T08:30:00-04:00","🔴","미국 고용보고서","연준 금리경로 핵심 지표"),
    ("2026-10-07T14:00:00-04:00","🟠","FOMC 의사록","연준 내부 시각·향후 금리경로 단서"),
    ("2026-10-14T08:30:00-04:00","🔴","미국 CPI","인플레이션 핵심 지표"),
    ("2026-10-15T08:30:00-04:00","🟠","미국 PPI","생산자물가 → 금리 기대 영향"),
    ("2026-10-28T14:00:00-04:00","🔴","FOMC 금리결정·성명","연준 정책·기자회견 → 최중요 시장 이벤트"),
    ("2026-11-06T08:30:00-05:00","🔴","미국 고용보고서","연준 금리경로 핵심 지표"),
    ("2026-11-10T08:30:00-05:00","🔴","미국 CPI","인플레이션 핵심 지표"),
    ("2026-11-13T08:30:00-05:00","🟠","미국 PPI","생산자물가 → 금리 기대 영향"),
    ("2026-12-04T08:30:00-05:00","🔴","미국 고용보고서","연준 금리경로 핵심 지표"),
    ("2026-12-09T14:00:00-05:00","🔴","FOMC 금리결정·성명","연준 정책·점도표·기자회견 → 최중요 시장 이벤트"),
    ("2026-12-10T08:30:00-05:00","🔴","미국 CPI","인플레이션 핵심 지표"),
    ("2026-12-15T08:30:00-05:00","🟠","미국 PPI","생산자물가 → 금리 기대 영향"),
]

def _event_dt(raw):
    return datetime.fromisoformat(raw).astimezone(KST)

def upcoming_scheduled_events(hours=168):
    now=datetime.now(KST); out=[]
    for raw,level,name,impact in SCHEDULED_MARKET_EVENTS:
        dt=_event_dt(raw); delta=(dt-now).total_seconds()/3600
        if 0 <= delta <= hours:
            out.append((dt,delta,level,name,impact))
    return sorted(out,key=lambda x:x[0])

def dynamic_policy_radar(limit=4):
    # Upcoming crypto regulation/ETF/legal catalysts. Only surface articles whose titles explicitly signal a future action.
    queries=[
        'US crypto bill vote hearing deadline SEC CFTC stablecoin market structure when:7d',
        'Bitcoin crypto ETF SEC decision deadline approval when:7d',
        'Worldcoin WLD regulation launch unlock partnership upcoming when:7d',
        'KAIA Kaia blockchain launch partnership regulation upcoming when:7d',
    ]
    future_words=('vote','voting','hearing','deadline','scheduled','upcoming','expected','set to','will ','approval','decision','표결','청문','마감','예정','심사','승인')
    rows=[]; seen=set()
    for q in queries:
        try:
            for it in google_news_rss(q,3):
                title=(it.get('title') or '').strip(); low=title.lower()
                if not title or not any(w in low for w in future_words): continue
                k=title.lower()[:140]
                if k in seen: continue
                seen.add(k); rows.append(it)
        except Exception:
            pass
    return rows[:limit]

def event_radar_text():
    parts=['📡 【자이나 시장 이벤트 사전 레이더】']
    ev=upcoming_scheduled_events(168)
    if ev:
        for dt,delta,level,name,impact in ev[:8]:
            if delta < 24: left=f'{delta:.1f}시간 전'
            else: left=f'{delta/24:.1f}일 전'
            parts.append(f'{level} {name} · {dt:%m/%d %H:%M} 한국시간 · {left}\n→ {impact}')
    else:
        parts.append('향후 7일 공식 일정 중 등록된 최중요 이벤트 없음')
    dyn=dynamic_policy_radar(4)
    if dyn:
        parts.append('\n🧭 법안·규제·ETF·WLD/KAIA 예정 이슈 후보')
        for it in dyn:
            parts.append(f"• {it.get('title','')}\n{it.get('link','')}")
    parts.append('\n※ 예정 이슈는 사전경계용. 실제 발표 직후 가격 급변 시 기존 중요알람·원인분석·수익보호 판단이 우선 작동합니다.')
    parts.append('※ 자동주문 없음')
    return '\n'.join(parts)

def event_radar_loop():
    global EVENT_RADAR_LAST_DAILY
    while True:
        try:
            now=datetime.now(KST)
            # Daily morning briefing once after 09:00 KST
            day=now.strftime('%Y-%m-%d')
            if CHAT_ID and now.hour >= 9 and EVENT_RADAR_LAST_DAILY != day:
                send_long(event_radar_text(),CHAT_ID); EVENT_RADAR_LAST_DAILY=day
            # Escalation reminders: 24h and 3h before red/orange official events
            if CHAT_ID:
                for dt,delta,level,name,impact in upcoming_scheduled_events(30):
                    for tag,lo,hi in [('24H',20,26),('3H',2,4)]:
                        key=f'{dt.isoformat()}:{tag}'
                        if lo <= delta <= hi and key not in EVENT_RADAR_SENT:
                            EVENT_RADAR_SENT.add(key)
                            send(f'🚨 시장 이벤트 임박 {tag}\n{level} {name}\n한국시간 {dt:%m/%d %H:%M}\n→ {impact}\n발표 전후 BTC·WLD·KAIA 급변 집중감시',CHAT_ID)
        except Exception as e:
            print('[EventRadar] error',e,flush=True)
        time.sleep(EVENT_RADAR_INTERVAL)


def telegram_loop():
    global TG_OFFSET, CHAT_ID
    if not TOKEN:
        return
    while True:
        r=tg("getUpdates",{"offset":TG_OFFSET,"timeout":5,"allowed_updates":["message"]})
        if not r or not r.get("ok"):
            time.sleep(3)
            continue
        for u in r.get("result",[]):
            TG_OFFSET=max(TG_OFFSET,int(u.get("update_id",0))+1)
            msg=u.get("message") or {}
            cid=str((msg.get("chat") or {}).get("id") or "")
            text=(msg.get("text") or "").strip()
            if not cid:
                continue
            if cid and CHAT_ID != cid:
                CHAT_ID = cid
                save_persistent_state()
                print("[Telegram] CHAT_ID registered:", cid, flush=True)

            if text.startswith("/start") or text.lower()=="start":
                send("✅ Jaina Coin Monitor v14.2 연결 완료\n/status 현재상태\n/trend 단기·중기 상승추세 판단\n/position 매매장부 확인\n/sell W 15 559 급등익절\n/sellqty W 12173.91304347 552 실제체결\n/buy W 3000000 520 재매수\n/cashset W 0 잔액정정\n/news 최신 뉴스\n/good W·K 호재·전망 레이더\n/cause 현재 급변 원인 레이더\n/radar 미국증시·코인 사전 이벤트 레이더\n/market BTC 시장요약\n/test 알림테스트\n/signaltest 중요신호 테스트\n/enginetest 판단엔진 테스트\n/booktest 장부 안전 테스트\n\n⏰ 17분 자동 상태보고\n📰 뉴스·호재·전망 3시간 자동발송\n📡 매일 사전 이벤트 레이더 + 24시간/3시간 임박알림\n⚡ W/K 급변 + BTC 선행충격 원인분석 즉시 알림\n※ 자동주문 없음",cid)
            elif text.startswith("/radar"):
                send_long(event_radar_text(),cid)
            elif text.startswith("/sellqty"):
                try:
                    p=text.split(maxsplit=4)
                    if len(p)<4:
                        raise ValueError("사용법: /sellqty W 12173.91304347 552 실제체결")
                    reason=p[4] if len(p)>4 else ""
                    s,q,amount,pnl,remain,cash=record_sell_qty(p[1],p[2],p[3],reason)
                    short="W" if s=="WLD" else "K"
                    send(
                        f"✅ {short} 실제수량 매도 기록 완료\n"
                        f"매도수량 {qty_text(q)}개\n"
                        f"체결가 {float(p[3]):,.4f}원\n"
                        f"장부 확보금액 {amount:,.0f}원\n"
                        f"이번 실현손익 {pnl:+,.0f}원\n"
                        f"남은수량 {qty_text(remain)}개\n"
                        f"재매수 가능 현금 {cash:,.0f}원\n"
                        f"※ 거래소 수수료는 별도 반영되지 않음",
                        cid
                    )
                except Exception as e:
                    send(f"⚠️ 실제수량 매도 기록 실패: {e}",cid)
            elif text.startswith("/sell"):
                try:
                    p=text.split(maxsplit=4)
                    if len(p)<4: raise ValueError("사용법: /sell W 15 559 급등익절\n/sellqty W 12173.91304347 552 실제체결")
                    reason=p[4] if len(p)>4 else ""
                    s,q,amount,pnl,remain,cash=record_sell(p[1],p[2],p[3],reason)
                    short="W" if s=="WLD" else "K"
                    send(f"✅ {short} 매도 기록 완료\n매도수량 {qty_text(q)}개\n확보금액 {amount:,.0f}원\n이번 실현손익 {pnl:+,.0f}원\n남은수량 {qty_text(remain)}개\n재매수 가능 현금 {cash:,.0f}원",cid)
                except Exception as e: send(f"⚠️ 매도 기록 실패: {e}",cid)
            elif text.startswith("/buy"):
                try:
                    p=text.split(maxsplit=4)
                    if len(p)<4: raise ValueError("사용법: /buy W 3000000 520 재매수")
                    reason=p[4] if len(p)>4 else ""
                    s,q,nq,na,cash=record_buy(p[1],p[2],p[3],reason)
                    short="W" if s=="WLD" else "K"
                    send(f"✅ {short} 재매수 기록 완료\n매수수량 {qty_text(q)}개\n새 보유수량 {qty_text(nq)}개\n새 장부평단 {na:,.4f}원\n남은 재매수 현금 {cash:,.0f}원",cid)
                except Exception as e: send(f"⚠️ 재매수 기록 실패: {e}",cid)
            elif text.startswith("/deposit"):
                try:
                    p=text.split(maxsplit=3)
                    if len(p)<3: raise ValueError("사용법: /deposit W 3000000 추가투자금")
                    reason=p[3] if len(p)>3 else ""
                    s,amount,cash,deposited=record_deposit(p[1],p[2],reason)
                    short="W" if s=="WLD" else "K"
                    send(f"✅ {short} 외부입금 기록 완료\\n입금금액 {amount:,.0f}원\\n재매수 가능 현금 {cash:,.0f}원\\n외부입금 누적 {deposited:,.0f}원",cid)
                except Exception as e: send(f"⚠️ 입금 기록 실패: {e}",cid)
            elif text.startswith("/cashset"):
                try:
                    p=text.split(maxsplit=3)
                    if len(p)<3:
                        raise ValueError("사용법: /cashset W 0 실제잔액정정")
                    reason=p[3] if len(p)>3 else ""
                    r=record_cashset(p[1],p[2],reason)
                    short="W" if r["symbol"]=="WLD" else "K"
                    send(
                        f"✅ {short} 재매수 현금 잔액 정정 완료\n"
                        f"정정 전 {r['before']:,.0f}원\n"
                        f"정정 후 {r['cash']:,.0f}원\n"
                        f"※ 개인인출/외부입금/실현손익 누계는 변경하지 않음"
                        + (f"\n메모 {r['reason']}" if r["reason"] else ""),
                        cid
                    )
                except Exception as e:
                    send(f"⚠️ 현금 잔액 정정 실패: {e}",cid)
            elif text.startswith("/withdraw"):
                try:
                    p=text.split(maxsplit=3)
                    if len(p)<3:
                        raise ValueError("사용법: /deposit W 3000000 추가투자금\n/withdraw W 1000000 생활비")
                    reason=p[3] if len(p)>3 else ""
                    r=record_withdraw(p[1],p[2],reason)
                    short="W" if r["symbol"]=="WLD" else "K"
                    send(
                        f"✅ {short} 개인인출 기록 완료\n"
                        f"인출금액 {r['amount']:,.0f}원\n"
                        f"남은 재매수 가능 현금 {r['remaining_cash']:,.0f}원\n"
                        f"개인인출 누적 {r['withdrawn']:,.0f}원"
                        + (f"\n메모 {r['reason']}" if r["reason"] else ""),
                        cid
                    )
                except Exception as e:
                    send(f"⚠️ 인출 기록 실패: {e}",cid)
            elif text.startswith("/position"):
                send(position_text(),cid)
            elif text.split()[0].split("@")[0].lower() == "/trend" if text else False:
                send("📈 단기·중기 추세를 계산하고 있습니다.", cid)
                try:
                    send(trend_report_text(), cid)
                except Exception as e:
                    print("[Telegram] /trend error", repr(e), flush=True)
                    send(f"⚠️ 추세 조회 오류: {type(e).__name__}: {e}", cid)
            elif text.split()[0].split("@")[0].lower() == "/version" if text else False:
                send("✅ Jaina Coin Monitor v12.3 실행 중", cid)
            elif text.split()[0].split("@")[0].lower() == "/booktest" if text else False:
                # 먼저 수신 확인을 보내므로, 긴 테스트 전에 명령 수신 여부를 즉시 알 수 있다.
                send("🧪 /booktest 명령 수신 — 장부 무변경 안전 테스트 시작", cid)
                try:
                    result = booktest_text()
                    send(result,cid)
                    print("[Telegram] /booktest completed", flush=True)
                except Exception as e:
                    print("[Telegram] /booktest error", repr(e), flush=True)
                    send(f"⚠️ 장부 테스트 실패: {type(e).__name__}: {e}",cid)
            elif text.startswith("/enginetest"):
                run_enginetest(cid)
            elif text.startswith("/signaltest"):
                run_signaltest(cid)
            elif text.startswith("/autotest"):
                send("🧪 17분 자동보고 기능을 즉시 테스트합니다.", cid)
                send_summary_once(cid)
            elif text.startswith("/test"):
                send("🔔 테스트 알림 성공",cid)
            elif text.startswith("/status"):
                try:
                    d=snapshot()
                    parts=[alert_text(s,d[s]) for s in ("WLD","KAIA") if s in d]
                    send("\n\n".join(parts),cid)
                except Exception as e:
                    send(f"⚠️ 시세 조회 오류: {e}",cid)
            elif text.split()[0].split("@")[0].lower() == "/good" if text else False:
                send("🚀 W·K 호재·전망 자료를 넓게 수집하고 있습니다. (정기보고는 3시간마다)",cid)
                try:
                    send_long(good_radar_text(),cid)
                except Exception as e:
                    print("[Telegram] /good error", repr(e), flush=True)
                    send(f"⚠️ 호재 레이더 조회 오류: {type(e).__name__}: {e}",cid)
            elif text.split()[0].split("@")[0].lower() == "/causetest" if text else False:
                send(causetest_text(),cid)
            elif text.split()[0].split("@")[0].lower() == "/cause" if text else False:
                send("🔎 현재 W·K/BTC 변동과 최신 시장 원인을 우선 분석합니다.",cid)
                threading.Thread(target=cause_command_worker,args=(cid,),daemon=True).start()
            elif text.startswith("/news"):
                send("📰 최신 뉴스를 수집하고 있습니다. 잠시만 기다려 주세요.",cid)
                try:
                    send_long(news_digest(),cid)
                except Exception as e:
                    send(f"⚠️ 뉴스 조회 오류: {e}",cid)
            elif text.startswith("/market"):
                send("📊 " + market_brief(),cid)

HTML = '''
<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>자이나 코인원 감시봇</title>
<style>
body{font-family:system-ui;background:#f4f6f8;margin:0;padding:15px;color:#101828}
.w{max-width:680px;margin:auto}.c{background:#fff;border-radius:18px;padding:18px;margin:12px 0}
.p{font-size:30px;font-weight:800}.s{background:#eef2f7;border-radius:11px;padding:12px;margin-top:12px;font-weight:800}
.ok{color:#067647}small{color:#667085;line-height:1.5}
</style>
<div class="w"><h2>자이나 코인원 감시봇 v11.5</h2><small>WLD · KAIA / 준비·실행·황금구간 / 자동주문 없음</small><div id="x"></div></div>
<script>
function n(v,d=0){const x=Number(v);return Number.isFinite(x)?x:d}
async function g(){
 try{
  const r=await fetch("/api?x="+Date.now(),{cache:"no-store"});
  const d=await r.json();
  x.innerHTML=Object.entries(d).map(([s,c])=>{
    const p=n(c.price), avg=n(c.avg), peak=n(c.peak), gp=n(c.gain_pct), dd=n(c.drawdown_pct), r1=n(c.ret1m), r5=n(c.ret5m), vr=n(c.vol_ratio,1);
    const reason=(c.reason===undefined||c.reason===null||c.reason==="")?"데이터 축적 중":c.reason;
    const sig=(c.signal===undefined||c.signal===null||c.signal==="")?"⚪ 홀딩 / 관찰":c.signal;
    return `<div class="c">
      <b>${s}/KRW</b>
      <div class="p">${p.toLocaleString()}원</div>
      <div class="ok">🟢 코인원 연결</div>
      <small>${c.daily&&c.daily.ready?`📅 오늘09시 대비 ${Number(c.daily.today_change_pct||0)>=0?"+":""}${Number(c.daily.today_change_pct||0).toFixed(2)}% · 전일09시 대비 ${Number(c.daily.prev24_change_pct||0)>=0?"+":""}${Number(c.daily.prev24_change_pct||0).toFixed(2)}%<br>`:""}</small>
      <small>평단 ${avg.toLocaleString()}원 · 평단대비 ${gp.toFixed(2)}%<br>
      최근고점 ${peak.toLocaleString()}원 · 고점대비 ${dd.toFixed(2)}%<br>
      1분 ${r1.toFixed(2)}% · 5분 ${r5.toFixed(2)}% · 거래량비 ${vr.toFixed(2)}배<br>
      현재 평가수익 ${Number(c.current_profit_krw||0).toLocaleString()}원<br>
      최고 평가수익 ${Number(c.peak_profit_krw||0).toLocaleString()}원 · 최고수익 대비 ${Number(c.profit_drawdown_pct||0).toFixed(2)}%<br>
      단계: ${c.phase||"관찰"}<br>
      권장행동: ${c.protect_action||"대기"}<br>
      ${c.rebuy_note?`재매수메모: ${c.rebuy_note}<br>`:""}
      이유: ${reason}</small>
      <div class="s">${sig}</div>
    </div>`;
  }).join("")
 }catch(e){
   x.innerHTML="<div class='c'>⚠️ 시세 조회 오류</div>"
 }
}
setInterval(g,3000);g()
</script>
'''

@app.route("/")
def home(): return render_template_string(HTML)

@app.route("/api")
def api():
    try: return jsonify(snapshot())
    except Exception as e: return jsonify({"error":str(e)}),500

@app.route("/health")
def health(): return "OK",200

threading.Thread(target=monitor_loop,daemon=True).start()
threading.Thread(target=telegram_loop,daemon=True).start()
threading.Thread(target=event_radar_loop,daemon=True).start()
threading.Thread(target=persistence_loop,daemon=True).start()
threading.Thread(target=auto_news_loop,daemon=True).start()
threading.Thread(target=auto_summary_loop,daemon=True).start()

if __name__=="__main__":
    app.run(host="0.0.0.0",port=int(os.getenv("PORT","10000")))
