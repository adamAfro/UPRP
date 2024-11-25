import os, json, xmltodict, networkx, re
from pandas import DataFrame
from tqdm import tqdm as progress
from uuid import uuid1 as unique

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

class Pathalias:

  """
  Na podstawie grafu tworzy informacyjne i unikalne aliasy ścieżek:

  - każdy wierzchołek to `(fragment ścieżki, [indeksy do ścieżek])`,
  - krawędź mówi o tym, że jeden wierzchołek jest poprzednikiem drugiego,
    dla przynajmniej jednej ścieżki.
  """

  def __init__(self, paths:list[str], maxlen:int=3, sep:str="/", norm:callable=None):
    self.sep = sep
    self.paths = paths
    self.norm = norm if norm is not None else lambda x: x
    self.maxlen = maxlen  #maksymalna długość aliasu
    self.short = dict()   #najkrótsze aliasy od ścieżek
    self.length = dict()   #długości i ścieżki jakie zawierają
    self.graph = None     #graf fragmentów ścieżek
    self._graph()
    self._short()

  def _graph(self):

    s = self.sep
    P0 = [(q, p.strip(s).split(s)) for q, p in enumerate(self.paths)]
    G = networkx.DiGraph()

    for q, p0 in P0:

      P = [(i, s.join(p0[:i+1])) for i in range(len(p0))]

      for i, p in P:
        if p not in G.nodes:
          G.add_node(p, chunk=p.split(s)[-1], paths=[])
        G.nodes[p]['paths'].append(q)
      for i in range(1, len(P)):
        G.add_edge(P[i-1][1], P[i][1])

    L, L0 = {}, [(q, len(p)) for q, p in P0]
    for q, l in L0:
      if l not in L.keys():
        L[l] = []
      L[l].append(q)

    self.length = { l: L[l] for l in sorted(L.keys()) }
    self.graph = G

  @property
  def unique(self):
    """
    Najkrótsze możliwe fragmenty ścieżki z liczbowym sufiksem,
    jeśli się powtarzają
    """
    A = self.short
    r = dict()    #odwrotne map. aliasów
    for k0, k in A.items() + [('id', 'id'), ('doc', 'doc')]:
      r[k] = r[k] + [k0] if k in r.keys() else [k0]

    u = dict()    #unikalne aliasy
    for k, K0 in r.items():
      if len(K0) == 1: continue
      for i0, i in enumerate(K0):
        u[i] = self.norm(f"{k}{self.sep}{i0+1}")

    return { **A, **u }

  def _short(self):

    """
    Najkrótsze możliwe fragmenty ścieżki niedłuższe niż `maxlen`
    Dążą do unikalności ale jej nie gwarantują, patrz `unique`.
    """

    G = self.graph
    L = self.length
    M = self.maxlen

    for l, Q in L.items():
      for q in Q:
        A0 = self.short.values()
        B = self._branch(q)
        N = [G.nodes[b] for b in B]
        p = self.norm(N[-1]["chunk"])
        N = sorted(N[:-1], key=lambda x: len(x['paths']))
        for i in range(0, min( len(N), M ) - 1):
          if (p not in A0) and (p not in ['id', 'doc']): break
          p = self.norm(f"{N[i]['chunk']}{self.sep}" + p)
        self.short[self.paths[q]] = p

  def _branch(self, pathq:int):

    G = self.graph
    N = [n for n, D in G.nodes(data=True) if pathq in D['paths']]
    G0 = G.subgraph(N)
    l = [n for n in N if G0.in_degree(n) == 0][0]
    r = [n for n in N if G0.out_degree(n) == 0][0]
    y = list(networkx.all_simple_paths(G0, source=l, target=r))[0]

    return y

def simplify(frames:dict[DataFrame], norm=lambda x:x) -> dict:
  """
  Uproszcza nazwy kolumn w ramkach danych zachowując spójność,
  szczególnie dla `"id"` i nazw poprzedzonych `"&"` - relacje.
  """

  def colfilter(c): return (c != "id") and (not c.startswith('&'))

  H = frames
  K0 = [h for h in H.keys()]
  K = Pathalias(K0, norm=norm).unique
  K = { k0: norm(k) for k0, k in K.items() }

  Q = dict()
  for h, X in H.items():
    U0 = [c for c in X.columns if colfilter(c)]
    U = { k0: norm(k) for k0, k in Pathalias(U0, norm=norm).unique.items() }
    R = [c for c in X.columns if c.startswith("&")]

    U['id'] = 'id'
    for r in R: U[r] = "&" + K[r[1:]]

    Q[h] = U

  return { "frames": K, "columns": Q }