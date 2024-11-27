import re, sys, os, re, pickle, yaml
from pandas import DataFrame, to_datetime
from tqdm import tqdm as progress
from lib.log import log, notify
from lib.profile import Profiler
from lib.alias import simplify

def norm(x:str):
  x = re.sub(r'[^\w\.\-/\_]|\d', '', x)
  x = re.sub(r'\W+', '_', x)
  return x

def finalize(frames, dirname=""):

  H = frames

  for h, X in progress(H.items(), desc="📅", total=len(H)):
    for k in X.columns:
      X: DataFrame
      H[h] = X = X.convert_dtypes()
      if "date" in k:
        NA0 = X[k].isna().sum() / X.shape[0]
        D = to_datetime(X[k], errors='coerce', dayfirst=False)
        NA = D.isna().sum() / X.shape[0]
        if NA - NA0 < 0.50: X[k] = D

  qH = simplify(H, norm=norm)
  with open(f"{dirname}/alias.inv.yaml", 'w') as f:
    yaml.dump(qH, f, indent=2)

  H = { qH['frames'][h0]: X.set_index("id").rename(columns=qH['columns'][h0]) 
        for h0, X in H.items() }
  with open(f"{dirname}/data.pkl", 'wb') as f:
    pickle.dump(H, f)

  qH['columns'] = { qH['frames'][h]: { v: k for k, v in Q.items() } 
                    for h, Q in qH['columns'].items() }
  qH['frames'] = { v: k for k, v in qH['frames'].items() }
  with open(f"{dirname}/alias.yaml", 'w') as f:
    yaml.dump(qH, f, indent=2)

  qA = qH['columns']
  qA = { h: { k: None} for h, K in qA.items() for k in K.keys() }
  with open(f"{dirname}/assignement.null.yaml", 'w') as f:
    yaml.dump(qA, f, indent=2)

try:

  log("✨"); notify("✨")

  AE = ["abstract_inverted_index", "updated_date", "created_date"]
  A = Profiler(exclude=AE).JSON("api.openalex.org/res/").dataframes()
  finalize(A, "api.openalex.org")
  log("📑", A.keys()); notify("📑 pyalex ✅")

  L = Profiler().JSONl("api.lens.org/res/", listname="data").dataframes()
  finalize(L, "api.lens.org")
  log("📑", L.keys()); notify("📑 lens ✅")

  U = Profiler().XML("api.uprp.gov.pl/raw/").dataframes()
  finalize(U, "api.uprp.gov.pl")
  log("📑", U.keys()); notify("📑 uprp ✅")

except Exception as e:
  log("💥", e); notify("❌")