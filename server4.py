import os, time, threading, requests
from collections import deque
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)
COINS = {"WLD":{"avg":452.0,"qty":192495},"KAIA":{"avg":35.0,"qty":1131289}}
URL = "https://api.coinone.co.kr/public/v2/ticker_new/KRW"
SESSION = requests.Session()

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN","").strip()
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID","").strip()
TG_OFFSET = 0
LOCK = threading.RLock()

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
    } for s in COINS
}

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
        headers={"User-Agent":"JainaCoinMonitor/6.1"},
        timeout=(5,10),
    )
    r.raise_for_status()
    j = r.json()
    if j.get("result") != "success":
        raise RuntimeError(str(j))
    out={}
    for t in j.get("tickers",[]):
        s=str(t.get("target_currency","")).upper()
        if s in COINS:
            p=safe_float(t.get("last"))
            if p>0:
                out[s] = {
                    "price":p,
                    "quote_volume":safe_float(t.get("quote_volume")),
                }
    return out

def pct(a,b):
    if not a or not b:
        return 0.0
    return (a/b-1)*100

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

        # 3 sec polling -> 20 samples ~1 min, 100 samples ~5 min
        p1m = hist[-20][1] if len(hist)>=20 else hist[0][1]
        p5m = hist[-100][1] if len(hist)>=100 else hist[0][1]
        ret1 = pct(p,p1m)
        ret5 = pct(p,p5m)
        dd = pct(p,st["peak"])
        gain = pct(p,meta["avg"])

        recent = hist[-20:]
        previous = hist[-40:-20] if len(hist)>=40 else []
        recent_avg = sum(x[2] for x in recent)/len(recent) if recent else 0.0
        prev_avg = sum(x[2] for x in previous)/len(previous) if previous else 0.0
        vol_ratio = recent_avg/prev_avg if prev_avg>0 else 1.0

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

        if dd<=-20:
            signal="🛑 재매수 중지 · 급락 점검"
            reason=f"고점대비 {dd:.1f}%"
        elif dd<=-15:
            signal="🔵 3차 재매수 후보"
            reason=f"고점대비 {dd:.1f}%"
        elif dd<=-10:
            signal="🔵 2차 재매수 후보"
            reason=f"고점대비 {dd:.1f}%"
        elif dd<=-5:
            signal="🔵 1차 재매수 후보"
            reason=f"고점대비 {dd:.1f}%"
        elif score>=5:
            signal="🔴 2차 익절 후보"
            reason=", ".join(reasons[:4]) if reasons else "과열 신호"
        elif score>=3:
            signal="🟠 1차 익절 준비"
            reason=", ".join(reasons[:4]) if reasons else "상승 강도 증가"
        else:
            signal="⚪ 홀딩 / 관찰"
            if len(hist)<20:
                reason=f"데이터 축적 중 {len(hist)}/20"
            else:
                reason=", ".join(reasons[:3]) if reasons else "과열 신호 없음"

        st["signal"]=signal
        st["score"]=score
        st["reason"]=reason

        return {
            "price":p,
            "peak":st["peak"],
            "gain_pct":gain,
            "drawdown_pct":dd,
            "ret1m":ret1,
            "ret5m":ret5,
            "vol_ratio":vol_ratio,
            "signal":signal,
            "score":score,
            "reason":reason,
            "avg":meta["avg"],
            "qty":meta["qty"],
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
        f"신호 {d['signal']}\n"
        f"이유 {d['reason']}\n\n"
        f"※ 자동주문 없음 — 코인원 앱에서 직접 판단"
    )

def monitor_loop():
    global CHAT_ID
    while True:
        try:
            ticks=fetch_tickers()
            for s,tick in ticks.items():
                d=strategy(s,tick)
                with LOCK:
                    st=STATE[s]
                    sig=d["signal"]
                    now=time.time()
                    alertable=sig.startswith(("🔴","🟠","🔵","🛑"))
                    changed=sig!=st["last_signal"]
                    cooldown=now-st["last_alert_ts"]>=900
                    if alertable and changed and cooldown:
                        st["last_signal"]=sig
                        st["last_alert_ts"]=now
                        if CHAT_ID:
                            send(alert_text(s,d),CHAT_ID)
                    elif not alertable:
                        st["last_signal"]=sig
            print("[Strategy] updated", ",".join(ticks.keys()), flush=True)
        except Exception as e:
            print("[Strategy] error",e,flush=True)
        time.sleep(3)

def snapshot():
    ticks=fetch_tickers()
    out={}
    for s,t in ticks.items():
        out[s]=strategy(s,t)
    return out

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
            if not CHAT_ID:
                CHAT_ID=cid
            if text.startswith("/start") or text.lower()=="start":
                send("✅ Jaina Coin Monitor v6.1 연결 완료\n/status 현재상태\n/test 알림테스트\n※ 자동주문 없음",cid)
            elif text.startswith("/test"):
                send("🔔 테스트 알림 성공",cid)
            elif text.startswith("/status"):
                try:
                    d=snapshot()
                    parts=[alert_text(s,d[s]) for s in ("WLD","KAIA") if s in d]
                    send("\n\n".join(parts),cid)
                except Exception as e:
                    send(f"⚠️ 시세 조회 오류: {e}",cid)

HTML = '''
<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>자이나 코인원 감시봇</title>
<style>
body{font-family:system-ui;background:#f4f6f8;margin:0;padding:15px;color:#101828}
.w{max-width:680px;margin:auto}.c{background:#fff;border-radius:18px;padding:18px;margin:12px 0}
.p{font-size:30px;font-weight:800}.s{background:#eef2f7;border-radius:11px;padding:12px;margin-top:12px;font-weight:800}
.ok{color:#067647}small{color:#667085;line-height:1.5}
</style>
<div class="w"><h2>자이나 코인원 감시봇 v6.1</h2><small>WLD · KAIA / 전략 신호 / 자동주문 없음</small><div id="x"></div></div>
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

if __name__=="__main__":
    app.run(host="0.0.0.0",port=int(os.getenv("PORT","10000")))
