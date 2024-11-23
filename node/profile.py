import re, networkx as nx
import sys, os, re, pickle

DIR = os.path.dirname(__file__)
ROOT = os.path.abspath(os.path.join(DIR, '..'))
sys.path.append(ROOT) # dodanie lib
os.chdir(DIR) # zmiana katalogu dla procesów

from lib.log import log, notify
from lib.profile import Profiler

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

log("✨"); notify("✨")

pyalex = Profiler(exclude=["abstract_inverted_index", "updated_date", "created_date"])\
        .JSON("api.openalex.org/res/").dataframes()
with open("api.openalex.org/data.pkl", 'wb') as f: pickle.dump(pyalex, f)
Mermaid.schema(pyalex, "api.openalex.org/readme.md", "api.openalex.org/res/")
log("📑", pyalex.keys())
notify("📑 pyalex ✅")

lens = Profiler().JSONl("api.lens.org/res/", listname="data").dataframes()
with open("api.lens.org/data.pkl", 'wb') as f: pickle.dump(lens, f)
Mermaid.schema(lens, "api.lens.org/readme.md", "api.lens.org/res/")
log("📑", lens.keys())
notify("📑 lens ✅")

uprp = Profiler().XML("api.uprp.gov.pl/raw/").dataframes()
with open("api.uprp.gov.pl/data.pkl", 'wb') as f: pickle.dump(uprp, f)
Mermaid.schema(uprp, "api.uprp.gov.pl/readme.md", "api.uprp.gov.pl/raw/")
log("📑", uprp.keys())
notify("📑 uprp ✅")