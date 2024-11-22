import time, requests, os, sys

IPYNB = hasattr(sys, 'ps1') or 'ipykernel' in sys.modules

NOTIFY = False if IPYNB else True
def ntoogle(state:bool|None=None):
  global NOTIFY
  if state is not None: NOTIFY = state
  else: NOTIFY = not NOTIFY

with open(os.path.dirname(os.path.abspath(__file__))+"/ntfy", "r") as f: 
  NTFY = f.read().strip()

N0 = 0.0
def notify(*message, sep=" "):
  global N0, NOTIFY
  t = time.time() - N0
  N0 = time.time()
  if t < 1.0: return
  if not NOTIFY: return
  message = sep.join(message)
  message =  f"{str(os.path.abspath(__file__))}:{sep}{message}"
  D = dict(data=message.encode(encoding='utf-8'))
  requests.post( f"https://ntfy.sh/{NTFY}", **D )
  print("notify", message)

t0 = time.time()
def log( *anything ):
  global t0
  t = time.time() - t0
  t0 = time.time()
  if t < 0.01:
    print(f"{t*1000:.4f}ms", *anything)
  else:
    print(f"{t:.4f}s ", *anything)