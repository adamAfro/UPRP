from pandas import DataFrame
import json, xmltodict, re, networkx as nx
from tqdm import tqdm as progress
from uuid import uuid1 as unique
import os, re, pickle

class Profile:

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

  def apply(self, d:dict|list, path0:str='/', y:dict|None=None):
    if (y is None) or self.Q[path0]["repeat"]:
      y0 = y if y is not None else None
      y = { "id": str(unique()), "path": path0 }
      if y0 is not None: y[ "&" + y0['path'] ] = y0['id']
      self.Y.append(y)
    for k, V in d.items():
      path = path0+k+'/'
      if any(k in path for k in self.E): continue
      for i, v in enumerate(V if isinstance(V, list) else [V]):
        if isinstance(v, dict): self.apply(v, path, y)
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
      B = Profile.xmlbundleload(f0)
      for b in B: self.update(xmltodict.parse(b), dir)
    for i, f0 in progress(enumerate(F), desc=dir, total=len(F)):
      B = Profile.xmlbundleload(f0)
      for b in B: self.apply(xmltodict.parse(b), dir)
    return self

  @staticmethod
  def xmlbundleload(f0):
    with open(f0) as f: F = f.read()
    F = F.replace("&", "?").split('<?xml version="1.0" encoding="UTF-8"?>')
    F = [d for d0 in F for d in d0.split('<?xml version="1.0"?>')]
    F = ['<?xml version="1.0" encoding="UTF-8"?>\n'+d.strip() for d in F if d.strip()]
    return F

  def dataframes(self):
    return { n: X.drop(columns=['path']).dropna(axis=1, how='all')
                for n, X in DataFrame(self.Y).groupby('path') }

def branches(r:str, E):
  G = nx.DiGraph()
  for n0, n in E: G.add_edge(n0, n)
  y = []
  for l in [node for node in G.nodes if G.in_degree(node) == 0]:
    paths = list(nx.all_simple_paths(G, source=l, target=r))
    y.extend(paths)
  return y

class Mermaid:
  def entity(n, Q):
    return f'"{n}"' + '{\n'+ '\n'.join([f'  raw {q.strip("/").split("/")[-1]} "{q}"' for q in Q]) +" }\n"
  def relation(m, s):
    return f'"{s}"' + " ||--o{ " + f'"{m}" : "{s}"\n'
  def retype(x:str):return x.replace("  raw id ", "  key id ")\
                            .replace("  raw @", "  attr ")\
                            .replace("  raw #", "  val ")
  def schema(frames, mdfile, rootname):
    H, o, R = frames, mdfile, rootname
    E = [ (n, r[1:]) for n, X in H.items() for r in X.columns if r.startswith('&') ]
    Y0 = [Mermaid.entity(R, H[R])]
    for b in branches(R, E):
      Y0 += [""]
      for i, n in enumerate(b[:-1]):
        Y0[-1] += Mermaid.entity(n, H[n])
      for i, n in enumerate(b[:-1]):
        Y0[-1] += Mermaid.relation(b[i], b[i+1])

    with open(o, 'r') as f: M = f.read()
    r = re.compile(r'<!-- gen:profile.py -->.*?<!-- end:profile.py -->', re.DOTALL)
    Y0 = ['\n```mermaid\nerDiagram\n\n' + Mermaid.retype(m) + '\n```\n' for m in Y0]
    y = '\n'.join(Y0)
    M = re.sub(r, f'<!-- gen:profile.py -->\n{y}\n<!-- end:profile.py -->', M)
    with open(o, 'w', encoding='utf-8') as f: f.write(M)

pyalex = Profile(exclude=["abstract_inverted_index", "updated_date", "created_date"])\
        .JSON("api.openalex.org/res/").dataframes()
with open("api.openalex.org/data.pkl", 'wb') as f: pickle.dump(pyalex, f)
Mermaid.schema(pyalex, "api.openalex.org/readme.md", "api.openalex.org/res/")

lens = Profile().JSONl("api.lens.org/res/", listname="data").dataframes()
with open("api.lens.org/data.pkl", 'wb') as f: pickle.dump(lens, f)
Mermaid.schema(lens, "api.lens.org/readme.md", "api.lens.org/res/")

uprp = Profile().XML("api.uprp.gov.pl/raw/").dataframes()
with open("api.uprp.gov.pl/data.pkl", 'wb') as f: pickle.dump(uprp, f)
Mermaid.schema(uprp, "api.uprp.gov.pl/readme.md", "api.uprp.gov.pl/raw/")