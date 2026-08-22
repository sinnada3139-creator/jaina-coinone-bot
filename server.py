# -*- coding: utf-8 -*-
import os,json,time,threading,requests,websocket
from flask import Flask,jsonify,render_template_string

app=Flask(__name__)
COINS={"WLD":{"avg":452.0,"qty":192495},"KAIA":{"avg":35.0,"qty":1131289}}
STATE={s:{"price":0.0,"peak":0.0,"signal":"연결 대기","connected":False,"last_update":0,"volume_power":0.0} for s in COINS}
LOCK=threading.RLock(); TOKEN=os.getenv("TELEGRAM_BOT_TOKEN","").strip(); CHAT_ID=os.getenv("TELEGRAM_CHAT_ID","").strip(); TG_OFFSET=0
LAST_ALERT={s:"" for s in COINS}

def log(x): print(time.strftime("%F %T"),x,flush=True)
def tg(method,payload=None):
    if not TOKEN:return None
    try:return requests.post(f"https://api.telegram.org/bot{TOKEN}/{method}",json=payload or {},timeout=20).json()
    except Exception as e: log(f"[Telegram] error: {e}"); return None

def send(text,cid=None):
    c=str(cid or CHAT_ID or "").strip()
    if not c:return False
    r=tg("sendMessage",{"chat_id":c,"text":text}); return bool(r and r.get("ok"))

def snap(s):
    with LOCK:x=dict(STATE[s]); c=COINS[s]
    p=x["price"]; peak=x["peak"]; gain=(p/c["avg"]-1)*100 if p else 0; dd=(p/peak-1)*100 if p and peak else 0
    return x,c,gain,dd

def decide(s):
    x,c,gain,dd=snap(s)
    if not x["price"]:return "연결 대기"
    if dd<=-20:return "🛑 재매수 중지 · 급락 점검"
    if dd<=-15:return "🔵 3차 재매수 후보"
    if dd<=-10:return "🔵 2차 재매수 후보"
    if dd<=-5:return "🔵 1차 재매수 후보"
    if gain>=500:return "🟠 5배 구간 · 과열 확인"
    if gain>=400:return "🟠 4배 구간 · 추세 확인"
    if gain>=300:return "🟡 3배 구간 · 추가 익절 검토"
    if gain>=200:return "🟡 2배 구간 · 2차 익절 검토"
    if gain>=100:return "🟢 1배 구간 · 1차 익절 검토"
    if x["volume_power"]>=170:return "🔥 체결강도 과열 · 익절 준비"
    return "⚪ 홀딩 / 관찰"

def alert_text(s,signal=None):
    x,c,gain,dd=snap(s); signal=signal or decide(s)
    return f"【자이나 코인봇】 {s}/KRW\n현재가: {x['price']:,.4f}원\n평단: {c['avg']:,.0f}원\n평단 대비: {gain:+.2f}%\n최근 고점: {x['peak']:,.4f}원\n고점 대비: {dd:+.2f}%\n체결강도: {x['volume_power']:.1f}\n신호: {signal}\n\n※ 자동주문 없음"

def maybe_alert(s):
    n=decide(s)
    with LOCK: old=STATE[s]["signal"]; STATE[s]["signal"]=n
    if any(k in n for k in ("재매수","익절","중지","과열")) and n!=old and LAST_ALERT[s]!=n:
        LAST_ALERT[s]=n
        if CHAT_ID: send(alert_text(s,n))

def ping_loop(ws):
    while True:
        time.sleep(600)
        try:
            if not ws.sock or not ws.sock.connected:return
            ws.send(json.dumps({"request_type":"PING"})); log("[Coinone] PING sent")
        except Exception as e: log(f"[Coinone] PING error: {e}"); return

def coinone_loop():
    def on_open(ws):
        log("[Coinone] WebSocket OPEN")
        for s in COINS:
            for ch,topic in [("TICKER",{"quote_currency":"KRW","target_currency":s}),("TRADE",{"quote_currency":"KRW","target_currency":s}),("ORDERBOOK",{"quote_currency":"KRW","target_currency":s}),("CHART",{"quote_currency":"KRW","target_currency":s,"interval":"1m"}),("CHART",{"quote_currency":"KRW","target_currency":s,"interval":"5m"}),("CHART",{"quote_currency":"KRW","target_currency":s,"interval":"15m"})]:
                ws.send(json.dumps({"request_type":"SUBSCRIBE","channel":ch,"topic":topic})); time.sleep(.05)
        threading.Thread(target=ping_loop,args=(ws,),daemon=True).start()
    def on_message(ws,raw):
        try:o=json.loads(raw)
        except:return
        rt=o.get("response_type")
        if rt in ("CONNECTED","PONG"): log(f"[Coinone] {rt}"); return
        if rt=="ERROR": log(f"[Coinone] ERROR {o}"); return
        if rt=="SUBSCRIBED": log(f"[Coinone] SUBSCRIBED {o.get('channel')} {o.get('data')}"); return
        if rt!="DATA":return
        d=o.get("data") or {}; s=d.get("target_currency")
        if s not in COINS:return
        if o.get("channel")=="TICKER" and d.get("last") is not None:
            try:
                p=float(d["last"])
                with LOCK:
                    x=STATE[s]; x["price"]=p; x["peak"]=max(x["peak"],p) if x["peak"] else p; x["connected"]=True; x["last_update"]=int(time.time()); x["volume_power"]=float(d.get("volume_power") or 0)
                maybe_alert(s)
            except Exception as e: log(f"[Coinone] parse error {s}: {e}")
        else:
            with LOCK: STATE[s]["connected"]=True; STATE[s]["last_update"]=int(time.time())
    def on_error(ws,e): log(f"[Coinone] WebSocket ERROR: {e}")
    def on_close(ws,code,reason):
        log(f"[Coinone] CLOSED {code} {reason}")
        with LOCK:
            for s in STATE: STATE[s]["connected"]=False
    while True:
        try:websocket.WebSocketApp("wss://stream.coinone.co.kr",on_open=on_open,on_message=on_message,on_error=on_error,on_close=on_close).run_forever(ping_interval=20,ping_timeout=10)
        except Exception as e: log(f"[Coinone] reconnect: {e}")
        time.sleep(3)

def telegram_loop():
    global CHAT_ID,TG_OFFSET
    if not TOKEN: log("[Telegram] TELEGRAM_BOT_TOKEN missing"); return
    me=tg("getMe")
    if not me or not me.get("ok"): log(f"[Telegram] token failed: {me}"); return
    log(f"[Telegram] connected as @{me['result'].get('username','')}")
    while True:
        r=tg("getUpdates",{"offset":TG_OFFSET,"timeout":20,"allowed_updates":["message"]})
        if not r or not r.get("ok"): time.sleep(3); continue
        for u in r.get("result",[]):
            TG_OFFSET=max(TG_OFFSET,int(u["update_id"])+1); m=u.get("message") or {}; c=m.get("chat") or {}; cid=str(c.get("id") or ""); text=(m.get("text") or "").strip()
            if not cid: continue
            if not CHAT_ID and c.get("type")=="private": CHAT_ID=cid; log(f"[Telegram] alert chat registered: {CHAT_ID}")
            if text.startswith("/start") or text.lower()=="start": send("✅ 자이나 코인원 감시봇 연결 완료\n\nWLD·KAIA 실시간 감시 중입니다.\n자동매매는 하지 않습니다.\n\n/status - 현재 상태\n/test - 알림 테스트",cid)
            elif text.startswith("/status"): send(alert_text("WLD")+"\n\n"+alert_text("KAIA"),cid)
            elif text.startswith("/test"): send("🔔 테스트 알림 성공\nTelegram 연결이 정상입니다.",cid)

HTML='''<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>자이나 코인원 감시봇</title><style>body{font-family:system-ui;background:#f4f6f8;padding:14px}.c{background:#fff;border-radius:18px;padding:18px;margin:12px 0}.p{font-size:29px;font-weight:800}.s{font-weight:800;padding:12px;background:#eef2f7;border-radius:11px;margin-top:12px}.ok{color:#067647}.bad{color:#b42318}</style><h2>자이나 코인원 감시봇</h2><small>WLD · KAIA / 자동주문 없음</small><div id=x></div><script>async function g(){let d=await(await fetch('/api',{cache:'no-store'})).json();x.innerHTML=Object.entries(d).map(([k,c])=>`<div class=c><b>${k}/KRW</b><div class=p>${c.price?Number(c.price).toLocaleString()+'원':'연결 대기'}</div><div class=${c.connected?'ok':'bad'}>${c.connected?'🟢 실시간 연결':'🔴 연결 대기'}</div><small>평단 ${Number(c.avg).toLocaleString()}원 · 최근고점 ${Number(c.peak||0).toLocaleString()}원 · 체결강도 ${Number(c.volume_power||0).toFixed(1)}</small><div class=s>${c.signal}</div></div>`).join('')}setInterval(g,2000);g()</script>'''
@app.route('/')
def home():return render_template_string(HTML)
@app.route('/health')
def health():return 'OK',200
@app.route('/api')
def api():
    with LOCK:return jsonify({s:{**STATE[s],**COINS[s]} for s in COINS})

# Gunicorn imports server:app, so workers must start at import time.
threading.Thread(target=coinone_loop,daemon=True,name='coinone-monitor').start(); log('[App] Coinone monitor thread started')
threading.Thread(target=telegram_loop,daemon=True,name='telegram-poller').start(); log('[App] Telegram polling thread started')

if __name__=='__main__': app.run(host='0.0.0.0',port=int(os.getenv('PORT','10000')))
