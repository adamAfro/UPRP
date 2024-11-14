import sys
ipynb = hasattr(sys, 'ps1') or 'ipykernel' in sys.modules
if not ipynb:
  if (len(sys.argv) >= 2) and (sys.argv[1] == "nohup"):
    import os, json, subprocess
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    subprocess.Popen(f'nohup python {__file__} > profile.log 2>&1 &', shell=True)
    exit()

from pandas import DataFrame
from tqdm import tqdm
import os, json, xmltodict

class Profile:
  def __init__(self, Y:dict|None=None):
    self.Y = Y if Y is not None else {}

  def recursive(self, d:dict|list, path0:str='/'):
    rep = set()
    for k, V in d.items():
      for v in V if isinstance(V, list) else [V]:
        path = path0+k+'/'
        if path not in self.Y.keys(): 
          self.Y[path] = dict()
          self.Y[path]['short'] = path.replace('/', '   ')[-32:]
          self.Y[path]['dict'] = False
          self.Y[path]['None'] = False
          self.Y[path]['int'] = False
          self.Y[path]['float'] = False
          self.Y[path]['str'] = False
          self.Y[path]['repeat'] = False
          self.Y[path]['attr'] = "@" in path.split('/')[-2]

        if k in rep:
          self.Y[path]['repeat'] = True
        else: rep.add(k)
        
        if isinstance(v, dict):
          self.Y[path]['dict'] = True
          for k0, v0 in v.items(): self.recursive(v, path)
          continue

        if v is None:
          self.Y[path]['None'] = True
          continue
        
        if isinstance(v, int):
          self.Y[path]['int'] = True
          continue
        
        if isinstance(v, float):
          self.Y[path]['float'] = True
          continue

        v = v.strip()
        if (not v.startswith('0')) or v.replace(" ", "").startswith('0.'):
          try: 
            self.Y[path]['int'] = True
            continue
          except: pass
          try: 
            self.Y[path]['float'] = True
            continue
          except: pass

        self.Y[path]['str'] = True

    return self
  
  def to_csv(self, path:str, emoji=True):
    Y = DataFrame(self.Y).T.reset_index()
    I = [c for c in Y.columns if c not in ["short", "index"]]
    Y['short'] = Y['short'].str.pad(Y['short'].str.len().max(), side='left')
    if emoji:
      Y[I] = Y[I].apply(lambda x: x.replace({True: '✅', False: '❌'}))
      Y[I] = Y[I].apply(lambda x: x.str.pad(5, side='left'))
    Y[["short", *[c for c in Y.columns if c not in ["short", "index"]], "index"]]\
      .to_csv(path, index=False)

def xmlbundleload(f0):
  with open(f0) as f: F = f.read()
  F = F.replace("&", "?").split('<?xml version="1.0" encoding="UTF-8"?>')
  F = [d for d0 in F for d in d0.split('<?xml version="1.0"?>')]
  F = ['<?xml version="1.0" encoding="UTF-8"?>\n'+d.strip() for d in F if d.strip()]
  return F

def progress(x, **params):
  global ipynb
  if ipynb: return tqdm(x, **params)
  else:
    import time
    t0 = time.time()
    def g(x):
      for i, item in enumerate(x):
        t = time.time() - t0
        eta = t / (i + 1) * (len(x) - (i + 1))
        print(f"{i + 1}/{len(x)} t {t:.2f}s ETA {eta:.2f}s", end='\r')
        yield item
    return g(x)

lens = Profile()
lensF = [f for f in os.listdir('api.lens.org/res') if f.endswith(".json")]
for f0 in progress(lensF, desc="🌍"):
  with open(f'api.lens.org/res/{f0}') as f: D = json.load(f)['data']
  for d in D:
    try: lens.recursive(d)
    except: print(f"💥{f0}")
lens.to_csv("api.lens.org/profile.csv")

US = Profile()
USF = [f for f in os.listdir('bulkdata.uspto.gov') if f.endswith(".xml")]
for i, B in enumerate(USF):
  for f in progress(xmlbundleload(f'bulkdata.uspto.gov/{B}'), desc="🇺🇸",
                    postfix={"i": i, "n": len(USF)}):
    try: US.recursive(xmltodict.parse(f))
    except: print(f"💥{B}")
US.to_csv("api.lens.org/profile.csv")

UPRP = Profile()
UPRPF = [f for f in os.listdir('api.uprp.gov.pl/raw') if f.endswith(".xml")]
for f0 in progress(UPRPF, desc="🇵🇱"):
  with open(f"api.uprp.gov.pl/raw/{f0}") as x: f = x.read()
  try: UPRP.recursive(xmltodict.parse(f))
  except: print(f"💥{f0}")
UPRP.to_csv("api.uprp.gov.pl/profile.csv")