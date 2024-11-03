from pandas import read_csv, DataFrame
from tqdm import tqdm as progress; progress.pandas()
import os, json, re

def extract(x:str, q:str):
  "-> list[(sliced x, start index, q-match-bool]"
  import re
  M0 = list(re.finditer(q, x, flags=re.IGNORECASE))
  I = [i for m in M0 for i in m.span()]
  M = [i for i in I if any((i == m.start()) for m in M0)]
  if len(I) == 0: return [(x, 0, False)]
  if I[0] != 0: I = [0] + I
  I = I + [len(x)]
  Y = [x[I[i]:I[i+1]] for i in range(len(I)-1)]
  Y = [(Y[i], I[i], I[i] in M) for i in range(len(Y))]
  return Y

L0, R0 = r"(?<=\d\s|.\d)", r"(?=\s*\d)"
Q = dict()

Q["URL"] = r'http[s]?://(?:\w|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'

with open(os.path.join('month.json'), 'r') as f: month = json.load(f)
month = '|'.join([ m for M in month['PL'] + month['EN'] for m in M ])
Q["monthLR"] = rf"{L0}(?:{month}){R0}"
Q["monthL"] = rf"\b(?:{month}){R0}"
Q["monthR"] = rf"{L0}(?:{month})\b"

sep = [r'\-', r'/', r'\\']
Q['preseriesL'] = rf"\b[^\W\d]+\s*[{''.join(sep)}]{R0}"

alnum = r"(?:[^\W\d/\-.&\^'\\]+\d|\d+[^\W\d/\-.&\^'\\])\w*"
Q["alnumLR"] = rf"{L0}{alnum}{R0}"
Q["alnumL"] = rf"{L0}{alnum}\b"
Q["alnumR"] = rf"\b{alnum}{R0}"
Q["alnum0"] = rf"\b{alnum}\b"

abbr = r"[^\W\d]{1,4}\.?"
Q["abbrLR"] = rf"{L0}{abbr}{R0}"
Q["abbrL"] = rf"\b{abbr}{R0}"
Q["abbrR"] = rf"{L0}{abbr}\b"
sep += [r'\.'] + sep + [r'\s']
Q['series'] = "(?:" + '|'.join([rf"(?:\d+\s*{s}+\s*)+" for s in sep]) + ")\s*\d+"
Q['altseries'] = rf"(?:\d+\s*[{''.join(sep)}]+\s*)+\s*\d+"

Q['num'] = r"\b\d+\b"

K = Q.keys()
def extractk(x):
  global K
  X = [{ "type": None, "start": 0, **x.to_dict() }]
  for k in K:
    Y = [(x, extract(x['text'], Q[k])) if x['type'] is None
         else (x, [(x['text'], 0, False)]) for x in X]
    X = [{ **x, "text": t, "start": i + x['start'], "type": k if m else x['type'] } 
         for x, y in Y for t, i, m in y]
  return X

def extract0(x):
  global K
  X = [{ "start": 0, **x.to_dict(), **{ k: False for k in K } }]
  for k in K:
    Y = [(x, extract(x['text'], Q[k])) if not any(x[k] for k in K) 
         else (x, [(x['text'], x['start'], False)]) for x in X]
    X = [{ **x, "text": t, "start": i + x['start'], k: m } 
         for x, y in Y for t, i, m in y]
  return X

X0 = read_csv('../docs.csv').reset_index()#.sample(50000, random_state=0)
X0 = X0.rename(columns={'docs':'text', 'index':'docs'})[['docs', 'text']]
X0 = DataFrame(X0.progress_apply(extractk, axis=1).explode().tolist())
X0['end'] = X0['start'] + X0['text'].str.len() - 1
X0 = X0[['docs', 'start', 'end', 'text', 'type']].rename(columns={'type':'type0'})
X0.insert(3, "type", X0['type0'].apply( lambda x: None if x is None else 
                                      "abbr" if x.startswith('abbr') else
                                      "alnum" if 'num' in x else
                                      "month" if x.startswith('month') else 
                                      "series" if "series" in x else x))
X0.query('text.str.strip() != ""', inplace=True)
b = X0['type'].isna() | (X0['type'] == 'URL')
X, Y = X0[b], X0[~b].copy()

Y['pattern'] = Y['text'].apply(lambda x: re.sub(r'\d+', lambda m: str(len(m.group())), x))
Y.query('type.isin(["alnum", "num", "series"])')