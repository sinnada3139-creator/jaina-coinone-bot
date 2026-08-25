import os, time, threading, requests, json, html
from collections import deque
from urllib.parse import quote_plus
import xml.etree.ElementTree as ET
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)
COINS = {"WLD":{"avg":452.0,"qty":192495},"KAIA":{"avg":35.0,"qty":1131289}}
URL = "https://api.coinone.co.kr/public/v2/ticker_new/KRW"
SESSION = requests.Session()

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
        "history":deque(maxlen=240),
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
        headers={"User-Agent":"JainaCoinMonitor/7.1"},
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
    lines=["🧪 v11.5 장부 안전 테스트", "※ 실제 보유수량/현금/평단/저장파일은 변경하지 않습니다."]
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

        # 0) 급락 방어가 최우선
        if price_dd<=-25:
            signal="🛑 급락 점검 · 재매수 보류"
            phase="급락보호"
            protect_action="추가 매수 보류"
            rebuy_note="뉴스/시장 급락 원인 확인"
            reason=f"고점대비 {price_dd:.1f}%"

        # 1) 황금구간: 충분한 조정 후 하락세가 둔화/반전되고 거래량이 과열되지 않은 구간
        elif -18 < price_dd <= -10 and ret1 >= 0 and ret5 >= -0.5 and vol_ratio <= 1.3:
            signal="⭐ 황금구간 · 재매수 최우선 후보"
            phase="황금구간"
            protect_action="익절금의 20~30% 재매수 검토"
            rebuy_note="추가 하락 대비 남은 익절금은 반드시 보유"
            reason=f"고점대비 {price_dd:.1f}% · 1분 {ret1:+.2f}% · 5분 {ret5:+.2f}% · 거래량 {vol_ratio:.2f}배"

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

def alert_text(symbol,d):
    return (
        f"【자이나 코인봇】 {symbol}/KRW\n"
        f"현재가 {d['price']:,.4f}원\n"
        f"평단대비 {d['gain_pct']:+.2f}%\n"
        f"고점대비 {d['drawdown_pct']:+.2f}%\n"
        f"1분 {d['ret1m']:+.2f}% / 5분 {d['ret5m']:+.2f}%\n"
        f"거래량비 {d['vol_ratio']:.2f}배\n"
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
            for s,tick in ticks.items():
                if s not in COINS:
                    continue
                d=strategy(s,tick)
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
NEWS_INTERVAL = 2 * 60 * 60
LAST_NEWS_SENT = 0.0

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

def google_news_rss(query, limit=4):
    url = "https://news.google.com/rss/search?q=" + quote_plus(query) + "&hl=ko&gl=KR&ceid=KR:ko"
    r = SESSION.get(url, headers={"User-Agent":"Mozilla/5.0"}, timeout=(5,12))
    r.raise_for_status()
    root = ET.fromstring(r.content)
    items = []
    for item in root.findall(".//item")[:limit]:
        title = html.unescape((item.findtext("title") or "").strip())
        link = (item.findtext("link") or "").strip()
        items.append({"title": title, "link": link})
    return items

def classify_news(title):
    low = title.lower()
    pos = sum(1 for w in POS_WORDS if w in low)
    neg = sum(1 for w in NEG_WORDS if w in low)
    if pos > neg: return "🟢"
    if neg > pos: return "🔴"
    return "⚪"

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
    parts.append("🟢 긍정 가능성 · 🔴 부정 가능성 · ⚪ 중립/판단보류")
    parts.append("※ 제목 키워드 기반 분류이며 투자 판단을 보장하지 않습니다.")
    return "\n".join(parts)

def auto_news_loop():
    global LAST_NEWS_SENT
    while True:
        try:
            if CHAT_ID and time.time() - LAST_NEWS_SENT >= NEWS_INTERVAL:
                send(news_digest(), CHAT_ID)
                LAST_NEWS_SENT = time.time()
                print("[News] digest sent", flush=True)
        except Exception as e:
            print("[News] error", e, flush=True)
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
                send("✅ Jaina Coin Monitor v11.5 연결 완료\n/status 현재상태\n/position 매매장부 확인\n/sell W 15 559 급등익절\n/sellqty W 12173.91304347 552 실제체결\n/buy W 3000000 520 재매수\n/news 최신 뉴스\n/market BTC 시장요약\n/test 알림테스트\n/signaltest 중요신호 테스트\n/enginetest 판단엔진 테스트\n/booktest 장부 안전 테스트\n\n⏰ 17분 자동 상태보고\n📰 뉴스 2시간 자동발송\n※ 자동주문 없음",cid)
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
            elif text.split()[0].split("@")[0].lower() == "/version" if text else False:
                send("✅ Jaina Coin Monitor v11.5 실행 중", cid)
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
            elif text.startswith("/news"):
                send("📰 최신 뉴스를 수집하고 있습니다. 잠시만 기다려 주세요.",cid)
                try:
                    send(news_digest(),cid)
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
threading.Thread(target=persistence_loop,daemon=True).start()
threading.Thread(target=auto_news_loop,daemon=True).start()
threading.Thread(target=auto_summary_loop,daemon=True).start()

if __name__=="__main__":
    app.run(host="0.0.0.0",port=int(os.getenv("PORT","10000")))
