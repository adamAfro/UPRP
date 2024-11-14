from pandas import DataFrame
from tqdm import tqdm as progress
import os, xmltodict

def xmlbundleload(f0):
  with open(f0) as f: F = f.read()
  F = F.replace("&", "?").split('<?xml version="1.0" encoding="UTF-8"?>')
  F = [d for d0 in F for d in d0.split('<?xml version="1.0"?>')]
  F = ['<?xml version="1.0" encoding="UTF-8"?>\n'+d.strip() for d in F if d.strip()]
  return F

F = [f for f0 in progress([f for f in os.listdir() if f.endswith(".xml")], desc="📂")
       for f in xmlbundleload(f0)]

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

P = Profile()
for f in progress(F, desc="🔬"):
  P.recursive(xmltodict.parse(f))

Y = DataFrame(P.Y).T.reset_index()
I = [c for c in Y.columns if c not in ["short", "index"]]
Y[I] = Y[I].apply(lambda x: x.replace({True: '✅', False: '❌'}))

Y['short'] = Y['short'].str.pad(Y['short'].str.len().max(), side='left')
Y[I] = Y[I].apply(lambda x: x.str.pad(5, side='left'))

Y[["short", *[c for c in Y.columns if c not in ["short", "index"]], "index"]]\
  .to_csv("profile.csv", index=False)