import os, json, time, threading, requests, websocket
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)
COINS = {
    'WLD': {'avg': 452.0, 'qty': 192495},
    'KAIA': {'avg': 35.0, 'qty': 1131289},
}
STATE = {k: {'price': 0.0, 'peak': 0.0, 'signal': '대기'} for k in COINS}
LAST_ALERT = {k: '' for k in COINS}
TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '')
CHAT = os.getenv('TELEGRAM_CHAT_ID', '')

def tg(method, payload=None):
    if not TOKEN:
        return None
    try:
        return requests.post(f'https://api.telegram.org/bot{TOKEN}/{method}', json=payload or {}, timeout=10).json()
    except Exception:
        return None

def get_chat():
    global CHAT
    if CHAT or not TOKEN:
        return
    r = tg('getUpdates', {'limit': 10, 'timeout': 1})
    if r and r.get('ok'):
        for u in reversed(r.get('result', [])):
            m = u.get('message') or u.get('edited_message')
            if m and m.get('chat', {}).get('id'):
                CHAT = str(m['chat']['id'])
                return

def alert(text):
    get_chat()
    if CHAT:
        tg('sendMessage', {'chat_id': CHAT, 'text': text})

def decide(k):
    c, x = COINS[k], STATE[k]
    if not x['price'] or not x['peak']:
        return '연결 대기'
    gain = (x['price'] / c['avg'] - 1) * 100
    dd = (x['price'] / x['peak'] - 1) * 100
    if dd <= -20: return '🛑 재매수 중지 · 급락 점검'
    if dd <= -15: return '🔵 3차 재매수 후보'
    if dd <= -10: return '🔵 2차 재매수 후보'
    if dd <= -5: return '🔵 1차 재매수 후보'
    if gain >= 500: return '🟠 5배 구간 · 과열 확인'
    if gain >= 400: return '🟠 4배 구간 · 추세 확인'
    if gain >= 300: return '🟡 3배 구간 · 추가 익절 검토'
    if gain >= 200: return '🟡 2배 구간 · 2차 익절 검토'
    if gain >= 100: return '🟢 1배 구간 · 1차 익절 검토'
    return '⚪ 홀딩 / 관찰'

def on_open(ws):
    for k in COINS:
        topics = [
            ('TICKER', {'quote_currency':'KRW','target_currency':k}),
            ('TRADE', {'quote_currency':'KRW','target_currency':k}),
            ('ORDERBOOK', {'quote_currency':'KRW','target_currency':k}),
            ('CHART', {'quote_currency':'KRW','target_currency':k,'interval':'1m'}),
            ('CHART', {'quote_currency':'KRW','target_currency':k,'interval':'5m'}),
            ('CHART', {'quote_currency':'KRW','target_currency':k,'interval':'15m'}),
        ]
        for ch, topic in topics:
            ws.send(json.dumps({'request_type':'SUBSCRIBE','channel':ch,'topic':topic}))

def on_message(ws, msg):
    try:
        o = json.loads(msg)
    except Exception:
        return
    if o.get('response_type') != 'DATA':
        return
    d = o.get('data', {})
    k = d.get('target_currency')
    if k not in COINS:
        return
    if o.get('channel') == 'TICKER' and d.get('last') is not None:
        x = STATE[k]
        x['price'] = float(d['last'])
        x['peak'] = max(x['peak'], x['price'])
        new = decide(k)
        old = x['signal']
        x['signal'] = new
        if new != old and new != LAST_ALERT[k] and any(q in new for q in ('후보','익절','중지')):
            LAST_ALERT[k] = new
            gain = (x['price']/COINS[k]['avg']-1)*100
            dd = (x['price']/x['peak']-1)*100
            alert(f'【자이나 코인봇】 {k}/KRW\n현재가: {x["price"]:,.4f}원\n평단대비: {gain:+.2f}%\n고점대비: {dd:+.2f}%\n신호: {new}\n\n※ 자동주문 없음')

def ws_loop():
    while True:
        try:
            w = websocket.WebSocketApp('wss://stream.coinone.co.kr', on_open=on_open, on_message=on_message)
            w.run_forever(ping_interval=20, ping_timeout=10)
        except Exception:
            time.sleep(3)

HTML = '''<!doctype html><meta name="viewport" content="width=device-width,initial-scale=1"><title>자이나 코인봇</title><style>body{font-family:system-ui;background:#f4f6f8;padding:12px}.c{background:#fff;border-radius:15px;padding:16px;margin:10px 0}.p{font-size:28px;font-weight:800}.s{font-weight:800;padding:11px;border-radius:10px;background:#eef2f7;margin-top:10px}</style><h2>자이나 코인원 감시봇</h2><small>WLD · KAIA / 자동주문 없음</small><div id="x"></div><script>async function g(){let d=await(await fetch('/api')).json();document.getElementById('x').innerHTML=Object.entries(d).map(([k,c])=>`<div class=c><b>${k}/KRW</b><div class=p>${c.price?Number(c.price).toLocaleString()+'원':'연결 대기'}</div><small>평단 ${c.avg.toLocaleString()}원 · 최근고점 ${Number(c.peak||0).toLocaleString()}원</small><div class=s>${c.signal}</div></div>`).join('')}setInterval(g,1000);g()</script>'''

@app.route('/')
def home(): return render_template_string(HTML)
@app.route('/health')
def health(): return 'OK', 200
@app.route('/api')
def api(): return jsonify({k:{**STATE[k],**COINS[k]} for k in COINS})

if __name__ == '__main__':
    threading.Thread(target=ws_loop, daemon=True).start()
    app.run(host='0.0.0.0', port=int(os.getenv('PORT','10000')))
