import time, requests, os, sys, datetime, tqdm
from tqdm.asyncio import tqdm as tqdm_async

LOGGED = False
IPYNB = hasattr(sys, 'ps1') or 'ipykernel' in sys.modules
DEBUGGER = any('pydevd' in mod for mod in sys.modules)
INTERACTIVE = IPYNB or DEBUGGER

NOTIFY = False if INTERACTIVE else True
def ntoogle(state:bool|None=None):
  global NOTIFY
  if state is not None: NOTIFY = state
  else: NOTIFY = not NOTIFY

N0 = 0.0
def notify(*message, sep=" "):
  global N0, NOTIFY
  t = time.time() - N0
  N0 = time.time()
  if not NOTIFY: return
  message = sep.join(message)
  D = dict(data=message.encode(encoding='utf-8'))
  requests.post( "https://ntfy.sh/uprp_dev", **D )
  print("notify", message, flush=True)

t0 = time.time()
def log( *anything ):
  global t0, LOGGED
  t = time.time() - t0
  t0 = time.time()
  if not LOGGED:
    LOGGED = True
    print(datetime.datetime.now(), *anything)
  elif t < 0.01:
    print(f"{t*1000:.1f}ms", *anything, flush=True)
  else:
    t = '{:02}:{:02}'.format(int(t // 60), int(t % 60))
    print(f"{t}", *anything, flush=True)

TQDMINTERVAL = dict() if INTERACTIVE else dict(mininterval=60, maxinterval=3600)
TQDMBAR = "{elapsed:>4} {desc} {n_fmt} {bar} {percentage:3.0f}%  {total_fmt} {remaining} {postfix}"
def progress(*args, asyncio=False, **kwargs):
  if asyncio:
    return tqdm_async(*args, bar_format=TQDMBAR, **TQDMINTERVAL, **kwargs)
  return tqdm.tqdm(*args, bar_format=TQDMBAR, **TQDMINTERVAL, **kwargs)

tqdm.tqdm.pandas(desc="🐼", bar_format=TQDMBAR, **TQDMINTERVAL)