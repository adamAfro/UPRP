import os, json, xmltodict
from pandas import DataFrame
from uuid import uuid1 as unique
from .log import progress

def xmlbundleload(f0):
  with open(f0) as f: F = f.read()
  F = F.replace("&", "?").split('<?xml version="1.0" encoding="UTF-8"?>')
  F = [d for d0 in F for d in d0.split('<?xml version="1.0"?>')]
  F = ['<?xml version="1.0" encoding="UTF-8"?>\n'+d.strip() for d in F if d.strip()]
  return F

class Profiler:

  """
  Zwraca profil słownika jako ścieżki do wartości,
  wskazując wszystko co kiedykolwiek było wartością w
  jakiejkolwiek liście jako oddzielną encję.
  """

  #TODO: ustalanie separatora ścieżki innego niż "/"
  #TODO: zbieranie wyników do dict Y, żeby wywalić pandas

  def __init__(self, raw:dict|None=None, exclude=None):
    self.Q = raw if raw is not None else {}
    self.E = exclude if exclude is not None else []
    self.Y = []

  def update(self, d:dict|list, path0:str='/'):
    rep = set()
    for k, V in d.items():
      path = path0+k+'/'
      if any(k in path for k in self.E): continue
      for v in V if isinstance(V, list) else [V]:
        if path not in self.Q.keys():
          self.Q[path] = dict()
          self.Q[path]['repeat'] = False
        if k in rep: self.Q[path]['repeat'] = True
        else: rep.add(k)
        if isinstance(v, dict): self.update(v, path)
        elif v is None: self.Q[path]['None'] = True
    return self

  def apply(self, d:dict|list, path0:str='/', y:dict|None=None, U=None):
    if (y is None) or self.Q[path0]["repeat"]:
      y0 = y if y is not None else None
      i = str(unique())
      if U is None: U = i
      y = { "id": i, "path": path0, "doc": U }
      if y0 is not None: y[ "&" + y0['path'] ] = y0['id']
      self.Y.append(y)
    for k, V in d.items():
      path = path0+k+'/'
      if any(k in path for k in self.E): continue
      for i, v in enumerate(V if isinstance(V, list) else [V]):
        if isinstance(v, dict): self.apply(v, path, y, U)
        else: y[path] = v
    return self

  def JSONl(self, dir:str, listname:str):
    F = [os.path.join(dir, f) for f in os.listdir(dir) if f.lower().endswith('.json')]
    for f0 in progress(F, desc=dir):
      with open(f0) as f: D = json.load(f)[listname]
      for d in D: self.update(d, dir)
    for i, f0 in progress(enumerate(F), desc=dir, total=len(F)):
      with open(f0) as f: D = json.load(f)[listname]
      for d in D: self.apply(d, dir)
    return self

  def JSON(self, dir:str):
    F = [os.path.join(dir, f) for f in os.listdir(dir) if f.lower().endswith('.json')]
    for f0 in progress(F, desc=dir):
      with open(f0) as f: d = json.load(f)
      self.update(d, dir)
    for i, f0 in progress(enumerate(F), desc=dir, total=len(F)):
      with open(f0) as f: d = json.load(f)
      self.apply(d, dir)
    return self

  def XML(self, dir:str):
    F = [os.path.join(dir, f) for f in os.listdir(dir) if f.lower().endswith('.xml')]
    for f0 in progress(F, desc=dir):
      with open(f0) as f: d = f.read()
      self.update(xmltodict.parse(d), dir)
    for i, f0 in progress(enumerate(F), desc=dir, total=len(F)):
      with open(f0) as f: d = f.read()
      self.apply(xmltodict.parse(d), dir)
    return self
  
  def XMLb(self, dir:str):
    F = [os.path.join(dir, f) for f in os.listdir(dir) if f.lower().endswith('.xml')]
    for f0 in progress(F, desc=dir):
      B = xmlbundleload(f0)
      for b in B: self.update(xmltodict.parse(b), dir)
    for i, f0 in progress(enumerate(F), desc=dir, total=len(F)):
      B = xmlbundleload(f0)
      for b in B: self.apply(xmltodict.parse(b), dir)
    return self

  def dataframes(self):
    return { n: X.drop(columns=['path']).dropna(axis=1, how='all')
                for n, X in DataFrame(self.Y).groupby('path') }