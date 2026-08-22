import os, time, threading, requests
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)
COINS = {"WLD":{"avg":452.0,"qty":192495},"KAIA":{"avg":35.0,"qty":1131289}}
URL = "https://api.coinone.co.kr/public/v2/ticker_new/KRW"
SESSION = requests.Session()
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN","").strip()
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID","").strip()
TG_OFFSET = 0
PEAKS = {"WLD":0.0,"KAIA":0.0}
LOCK = threading.RLock()

def f(v):
    try: return float(v)
    except: return 0.0

def fetch_prices():
    r = SESSION.get(URL, params={"additional_data":"false"},
                    headers={"User-Agent":"JainaCoinMonitor/5.1"},
                    timeout=(5,10))
    r.raise_for_status()
    j = r.json()
    if j.get("result") != "success":
        raise RuntimeError(str(j))
    out = {}
    for t in j.get("tickers", []):
        s = str(t.get("target_currency","")).upper()
        if s in COINS:
            p = f(t.get("last"))
            if p > 0: out[s] = p
    return out

def sig(s,p,peak):
    avg = COINS[s]["avg"]
    gain = (p/avg-1)*100
    dd = (p/peak-1)*100 if peak else 0
    if dd <= -20: return "🛑 재매수 중지 · 급락 점검"
    if dd <= -15: return "🔵 3차 재매수 후보"
    if dd <= -10: return "🔵 2차 재매수 후보"
    if dd <= -5: return "🔵 1차 재매수 후보"
    if gain >= 500: return "🟠 5배 구간 · 과열 확인"
    if gain >= 400: return "🟠 4배 구간 · 추세 확인"
    if gain >= 300: return "🟡 3배 구간 · 추가 익절 검토"
    if gain >= 200: return "🟡 2배 구간 · 2차 익절 검토"
    if gain >= 100: return "🟢 1배 구간 · 1차 익절 검토"
    return "⚪ 홀딩 / 관찰"

def snapshot():
    prices = fetch_prices()
    out = {}
    with LOCK:
        for s,m in COINS.items():
            p = prices.get(s,0.0)
            if p > 0:
                PEAKS[s] = max(PEAKS[s], p) if PEAKS[s] else p
            peak = PEAKS[s]
            out[s] = {
                "avg":m["avg"], "qty":m["qty"], "price":p, "peak":peak,
                "connected":bool(p),
                "gain_pct":((p/m["avg"]-1)*100) if p else None,
                "drawdown_pct":((p/peak-1)*100) if p and peak else None,
                "signal":sig(s,p,peak) if p else "연결 대기",
                "source":"Coinone REST · 요청시 직접조회"
            }
    return out

def tg(method,payload=None):
    if not TOKEN: return None
    try:
        return SESSION.post(f"https://api.telegram.org/bot{TOKEN}/{method}",
                            json=payload or {}, timeout=(5,12)).json()
    except Exception as e:
        print("[Telegram]", e, flush=True)
        return None

def send(text,cid):
    if cid: tg("sendMessage",{"chat_id":cid,"text":text})

def telegram_loop():
    global TG_OFFSET, CHAT_ID
    if not TOKEN: return
    while True:
        r = tg("getUpdates",{"offset":TG_OFFSET,"timeout":5,"allowed_updates":["message"]})
        if not r or not r.get("ok"):
            time.sleep(3); continue
        for u in r.get("result",[]):
            TG_OFFSET = max(TG_OFFSET, int(u.get("update_id",0))+1)
            msg = u.get("message") or {}
            cid = str((msg.get("chat") or {}).get("id") or "")
            text = (msg.get("text") or "").strip()
            if not cid: continue
            if not CHAT_ID: CHAT_ID = cid
            if text.startswith("/start") or text.lower()=="start":
                send("✅ Jaina Coin Monitor 연결 완료\n/status 현재상태\n/test 알림테스트\n※ 자동주문 없음",cid)
            elif text.startswith("/test"):
                send("🔔 테스트 알림 성공",cid)
            elif text.startswith("/status"):
                try:
                    d = snapshot(); parts=[]
                    for s in ("WLD","KAIA"):
                        x=d[s]
                        parts.append(f"【{s}/KRW】\n현재가 {x['price']:,.4f}원\n평단 {x['avg']:,.0f}원\n평단대비 {x['gain_pct']:+.2f}%\n고점대비 {x['drawdown_pct']:+.2f}%\n{x['signal']}")
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
.ok{color:#067647}.bad{color:#b42318}small{color:#667085;line-height:1.5}
</style>
<div class="w"><h2>자이나 코인원 감시봇 v5.1</h2><small>WLD · KAIA / 자동주문 없음</small><div id="x"></div></div>
<script>
async function g(){
  try{
    let d=await(await fetch("/api?x="+Date.now(),{cache:"no-store"})).json();
    x.innerHTML=Object.entries(d).map(([s,c])=>`<div class="c"><b>${s}/KRW</b><div class="p">${c.price?Number(c.price).toLocaleString()+"원":"연결 대기"}</div><div class="${c.connected?"ok":"bad"}">${c.connected?"🟢 코인원 연결":"🔴 연결 대기"}</div><small>평단 ${Number(c.avg).toLocaleString()}원 · 평단대비 ${c.gain_pct==null?"-":Number(c.gain_pct).toFixed(2)}%<br>최근고점 ${Number(c.peak||0).toLocaleString()}원 · 고점대비 ${c.drawdown_pct==null?"-":Number(c.drawdown_pct).toFixed(2)}%<br>${c.source}</small><div class="s">${c.signal}</div></div>`).join("")
  }catch(e){x.innerHTML="<div class='c'>⚠️ 시세 조회 오류</div>"}
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

threading.Thread(target=telegram_loop,daemon=True).start()

if __name__=="__main__":
    app.run(host="0.0.0.0",port=int(os.getenv("PORT","10000")))
