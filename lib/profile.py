import os, json, xmltodict, microdata
from pandas import DataFrame
from .log import progress

class Profiler:

  """
  Zwraca profil słownika jako ścieżki do wartości,
  wskazując wszystko co kiedykolwiek było wartością w
  jakiejkolwiek liście jako oddzielną encję.
  """

  #TODO: ustalanie separatora ścieżki innego niż "/"
  #TODO: zbieranie wyników do dict Y, żeby wywalić pandas

  def __init__(self, raw:dict|None=None, exclude=None, only=None):
    self.Q = raw if raw is not None else {}
    self.E = exclude if exclude is not None else []
    self.O = only if only is not None else []
    self.Y = []
    self.i = 0

  def id(self):
    self.i += 1
    return str(self.i)

  def isexcluded(self, path:str):
    if self.O and not any(k in path or path in k for k in self.O):
      return True
    if any(k in path for k in self.E): return True
    return False

  def update(self, d:dict|list, path0:str='/'):
    rep = set()
    for k, V in d.items():
      path = path0+k+'/'
      if self.isexcluded(path): continue
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
      i = self.id()
      if U is None: U = i
      y = { "id": i, "path": path0, "doc": U }
      if y0 is not None: y[ "&" + y0['path'] ] = y0['id']
      self.Y.append(y)
    for k, V in d.items():
      path = path0+k+'/'
      if self.isexcluded(path): continue
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

  def HTMLmicrodata(self, dir:str):
    F = [os.path.join(dir, f) for f in os.listdir(dir) if f.lower().endswith('.html')]
    for f0 in progress(F, desc=dir):
      with open(f0) as f: d = f.read()
      self.update({ "root": [u.json_dict() for u in microdata.get_items(d)] }, dir)
    for i, f0 in progress(enumerate(F), desc=dir, total=len(F)):
      with open(f0) as f: d = f.read()
      self.apply({ "root": [u.json_dict() for u in microdata.get_items(d)] }, dir)
    return self

  def dataframes(self):
    return { n: X.drop(columns=['path']).dropna(axis=1, how='all')
                for n, X in DataFrame(self.Y).groupby('path') }