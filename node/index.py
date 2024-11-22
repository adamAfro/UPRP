from whoosh.fields import Schema, TEXT, ID, DATETIME, STORED
from whoosh.filedb.filestore import FileStorage
from pandas import DataFrame, isna
from tqdm import tqdm as progress; progress.pandas()
import re, pickle, datetime, os, sys

DIR = os.path.dirname(__file__)
ROOT = os.path.abspath(os.path.join(DIR, '..'))
sys.path.append(ROOT) # dodanie lib
os.chdir(DIR) # zmiana katalogu dla procesów

from lib.log import log, notify

def pklload(path:str):
  with open(path, 'rb') as f: return pickle.load(f)

def norm(path:str):
  y = path
  y = re.sub(r"[^\w/]", "", y)
  y = re.sub(r"/", "_", y)
  return y

def datealike(k:str): return any(q in k for q in ["date"])
def valdate(k:str):
  try: 
    datetime.datetime.fromisoformat(k)
    return k
  except: return None

def typeguess(path:str):

  k0 = path

  if k0 == "id": return STORED()
  k = k0.strip("_").split("_")[-1]
  k1 = None
  if k in ["value", "text"]:
    k1 = k
    k = k0.strip("_").split("_")[-2]

  if datealike(k):
    if k1 == "text": return TEXT()
    return DATETIME()
  
  if any(q in k for q in ["docnumber"]):
    return TEXT()
  
  if any(q in k for q in ["kind"]):
    return TEXT(field_boost=0.1)
  
  if any(q in k for q in ["lang", "jurisdiction", "country", "residence"]):
    return TEXT(field_boost=0.2)
  
  if any(q in k for q in ["address", "city", "state"]):
    return TEXT(field_boost=1.0)

  if any(q in k for q in ["title"]):
    return TEXT(field_boost=1.0)

  if any(q in k for q in ["name"]):
    return TEXT(field_boost=1.5)

  return None

def scheme(entity:DataFrame) -> Schema:

  X = entity

  T = { k: typeguess(k) for k in X.columns }
  T = { k: t for k, t in T.items() if t is not None }

  return Schema( **T )

def idxpkl(path:str, storage:FileStorage):

  f0 = path
  F = storage

  H = pklload(f0); log("💾", f0)

  l = 0
  for k, X in H.items():
    X.columns = [norm(c) for c in X.columns]
    idxframe(X, norm(k), F)
    l += 1
    notify(f"📑 {l}/{len(H)} ✅ ({k})")

def idxframe(frame:DataFrame, name:str, storage:FileStorage):

  k = name
  F = storage
  S = scheme(frame); log("📑", S)

  X = frame[S.names()]
  I = F.create_index(S, indexname=k); log("📁", k)

  ne = 0
  Q = I.writer()
  m = dict(desc="📄", postfix={'index':k})
  for i, x in progress(X.iterrows(), total=len(X), **m):
    try:
      i = x.to_dict()
      d = { k: valdate(v) for k, v in i.items() if datealike(k) }
      d = { k: v for k, v in d.items() if (v is not None) and (not isna(v)) }
      d = { k: v for k, v in d.items() if len(re.sub("\W", "", v)) > 0 }
      Q.add_document( **d )
    except Exception as E:
      log("💥", d, E)
      if ne > 100: raise E
      else: ne += 1
      Q.commit()
      Q = I.writer()

  Q.commit()

try:
  log("✨"); notify("✨")

  F = FileStorage("index"); log("📂", F)

  idxpkl("api.uprp.gov.pl/data.pkl", F)
  idxpkl("api.openalex.org/data.pkl", F)
  idxpkl("api.lens.org/data.pkl", F)

except Exception as E:
  log("❌", E)
  notify("❌")
  exit()

notify("✅")