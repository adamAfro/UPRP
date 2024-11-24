from whoosh.fields import Schema, TEXT, ID, DATETIME, STORED
from whoosh.filedb.filestore import FileStorage
from pandas import DataFrame, isna, Series, concat
import re, pickle, yaml, os, sys, networkx as nx

DIR = os.path.dirname(__file__)
ROOT = os.path.abspath(os.path.join(DIR, '..'))
sys.path.append(ROOT) # dodanie lib
os.chdir(DIR) # zmiana katalogu dla procesów

from lib.log import log, notify, progress

K0 = ['doc_number', 'text', 'kind', 'lang', 
      'jurisdiction', 'country', 'residence', 
      'address', 'city', 'state', 'title', 'name']

def pivot(frames:dict[str, DataFrame],
          colfilter=lambda v: any(k0 in v.name for k0 in K0),
          how=dict(ignore_index=False, var_name='col', value_name='value'),
          table=dict(index='doc', columns=['frame', 'col'], values='value')):

  H0 = frames
  Q = colfilter
  H = dict()
  Y = []

  for h, X in H0.items():
    X = X.set_index("doc")
    d = [k for k in X.columns if not Q( X[k] )]
    H[h] = X.drop(columns=d)

  for h, X in progress(H.items(), total=len(H), desc="🔬"):
    X = X.melt( **how ).dropna(subset=['value']).reset_index()
    X['frame'] = h
    Y.append(X.set_index(['doc', 'frame', 'col']))

  T = concat([X for X in Y if X.shape[0] > 0])\
     .pivot_table( **table, aggfunc=set )

  return T

log("✨")

U = pickle.load(open("api.uprp.gov.pl/data.pkl", "rb"))
U = { h: X for h, X in U.items() if h not in ["comment", "citation",
                                              "priority_claim",
                                              "gazette_reference",
                                              "publication_reference",
                                              "classification_ipcr"] }; log("📂", U.keys())

#func
T = pivot(U).drop(columns=("raw", "dates_of_public_availability_doc_number")); log("📦")
T = T.progress_map(lambda s: ' '.join(s) if isinstance(s, set) else s).replace("-", None)
T.columns = [':'.join(c) for c in T.columns]; log("🔠")

F = FileStorage("index"); log("🗂", F)
S = Schema(doc=STORED, **{k: TEXT() for k in T.columns} ); log("📑", S)
I = F.create_index(S, indexname="api.uprp.gov.pl"); log("🔖", I)
W = I.writer()

ne = 0
for i, x in progress(T.iterrows(), total=len(T), desc="💾"):
  try:
    d = x.to_dict()
    d = { k: v for k, v in d.items() if not isna(v) }
    W.add_document(doc=i, **d )
  except Exception as E:
    log("💥", d, E)
    if ne > 100: raise E
    else: ne += 1
    W.commit()
    W = I.writer()

W.commit()