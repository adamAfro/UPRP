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

brace = '|'.join([rf"\{a}\w+\{b}" for a,b in ['()','{}','[]', '""', '<>']])
brace = rf"(?:{brace})"
Q["braceX"] = rf"{L0}{brace}{R0}"
Q["braceL"] = rf"(?<!\w){brace}{R0}"
Q["braceR"] = rf"{L0}{brace}(?!\w)"

with open(os.path.join('month.json'), 'r') as f: month = json.load(f)
month = '|'.join([ m for M in month['PL'] + month['EN'] for m in M ])
Q["monthX"] = rf"{L0}(?:{month}){R0}"
Q["monthR"] = rf"{L0}(?:{month})(?!\w)"
Q["monthL"] = rf"(?<!\w)(?:{month}){R0}"

sep = [r'\-', r'/', r'\\']
Q['alseriesL'] = rf"(?<!\w)\w{{,3}}[^\W\d]\.?\s*[{''.join(sep)}]{R0}"
Q['alseriesR'] = rf"{L0}[{''.join(sep)}]\s*[^\W\d]\w{{,3}}\.?(?!\w)"
Q['braceseriesL'] = rf"(?<!\w){brace}\s*[{''.join(sep)}]{R0}"
Q['braceseriesR'] = rf"{L0}[{''.join(sep)}]\s*{brace}(?!\w)"

abbr = r"[^\W\d]{1,4}\.?"
Q["abbrX"] = rf"{L0}{abbr}{R0}"
Q["abbrL"] = rf"(?<!\w){abbr}{R0}"
Q["abbrR"] = rf"{L0}{abbr}(?!\w)"

sep += [r'\.'] + sep + [r'\s']
Q['series0'] = "(?:" + '|'.join([rf"(?:\d+\s*{s}+\s*)+" for s in sep]) + ")\s*\d+"
Q['seriesU'] = rf"(?:\d+\s*[{''.join(sep)}]+\s*)+\s*\d+"

alnum = r"(?:[^\W\d]+\d|\d+[^\W\d])\w*"
Q["alnum"] = rf"(?<!\w){alnum}(?!\w)"

Q['num'] = r"(?<!\w)\d+(?!\w)"

K = Q.keys()
def extractk(x):
  global K
  X = [{ "type": None, "start": 0, **x }]
  for k in K:
    Y = [(x, extract(x['text'], Q[k])) if x['type'] is None
         else (x, [(x['text'], 0, False)]) for x in X]
    X = [{ **x, "text": t, "start": i + x['start'], "type": k if m else x['type'] } 
         for x, y in Y for t, i, m in y]
  return X

m0 = ['num', 'series', 'series0', 'alnum', 'altseries']
mR = [k for k in K if k.endswith('L') or k.endswith('X') if k != "URL"]
mL = [k for k in K if k.endswith('R') or k.endswith('X') if k != "URL"]
d0 = 3
def kmerge(X):
  global K, m0, mL, mR, d0
  for x0 in X:
    x0['typeL'] = x0['typeR'] = x0['type']
    x0['end'] = x0['start'] + len(x0['text']) - 1
  X = [x for x in X if x['text'].strip() != ""]
  i, n = 0, len(X)
  while (i < n):
    #torghtmerge
    x0 = X[i]
    while (x0['typeR'] is not None) and (i+1 <  n) and (X[i+1]['typeL'] is not None):
      x = X[i+1]
      d = x['start'] - x0['end']
      if not ((d < d0) and (x0['typeR'] in mR) and (x['typeL'] in m0)): break
      x0['text'] = x0['text'] + (d-1)*' ' + x['text']
      x0['end'] = x['end']
      x0['typeR'] = x['typeR']
      X.pop(i+1)
      n -= 1
    #toleftmerge
    while (x0['typeL'] is not None) and (i-1 >= 0) and (X[i-1]['typeR'] is not None):
      x = X[i-1]
      d = x0['start'] - x['end']
      if not ((d < d0) and (x0['typeL'] in mL) and (x['typeR'] in m0)): break
      x0['text'] = x['text'] + (d-1)*' ' + x0['text']
      x0['start'] = x['start']
      x0['typeL'] = x['typeL']
      X.pop(i-1)
      n -= 1
      i -= 1
    i += 1

  i, n = 0, len(X)
  while (i < n):
    x0 = X[i]
    while (x0['typeR'] is not None) and (i+1 <  n) and (X[i+1]['typeL'] is not None):
      x = X[i+1]
      d = x['start'] - x0['end']
      if not ((d < d0) and (x0['typeR'] in m0) and (x['typeL'] in m0)): break
      x0['text'] = x0['text'] + d*' ' + x['text']
      x0['end'] = x['end']
      x0['typeR'] = x['typeR']
      X.pop(i+1)
      n -= 1
    i += 1
  return X

X = read_csv('../docs.csv').reset_index().sample(12000, random_state=0)
X = X.rename(columns={'docs':'text', 'index':'docs'})[['docs', 'text']]
f = lambda x: kmerge(extractk(x.to_dict()))
X = DataFrame(X.progress_apply(f, axis=1).explode().tolist())
X['end'] = X['start'] + X['text'].str.len() - 1