from pandas import read_csv, DataFrame
from tqdm import tqdm as progress; progress.pandas()
import os, json, re

with open(os.path.join('month.json'), 'r') as f: Mo = json.load(f)
mo = '|'.join([ m for M in Mo['PL'] + Mo['EN'] for m in M ])

Q = re.compile('(?:' + '|'.join(rf'(?P<{k}>{q})' for k, q in {
"month":  rf"(?:(?<=\d\s|.\d)(?:{mo})(?!\w)|(?<!\w)(?:{mo})(?=\s*\d))",
"alnum":  r"(?<!\w)(?:[^\W\d]+\d|\d+[^\W\d])\w*(?!\w)",
"series": "(?:" + '|'.join([rf"(?:\d+\s*{s}+\s*)+" for s in ['\.', '\-', '/', '\\', '\s']]) + ")\s*\d+",
"num":    r"(?<!\w)\d+(?!\w)",
"braced": '|'.join([rf"\{a}\w{1,4}\{b}" for a,b in ['()','{}','[]', '""', '<>']]),
"abbr":   r"(?<!\w)[^\W\d]{1,4}\.?(?!\w)",
"space":  r"[\s\-\/\\\.]+",

}.items()) + ')')

K0 = ['alnum', 'num', 'series']
def expr(x:str):
  import re
  global Q, K0
  I = re.finditer(Q, x)
  S = [(m.group(), m.start(), m.end(), m.groupdict()) for m in I]
  if not S: return []
  S = [(x, a, b, any(m[k] is not None for k in K0)) for x, a, b, m in S]
  S.sort(key=lambda x: x[1])
  M = [(S[0])]
  for x, a, b, m in S[1:]:
    x0, a0, b0, m0 = M[-1]
    if a > b0: M.append((x, a, b, m))
    else:
      M[-1] = (x0 + x, a0, max(b0, b), m0 or m)
  return M

X = read_csv('../docs.csv').reset_index()
X = X.rename(columns={'docs':'text', 'index':'docs'})[['docs', 'text']]

qURL=r'(http[s]?://(?:\w|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+)'
X['text'] = X['text'].progress_apply(lambda x:
  re.sub(qURL, lambda m: ' '*len(m.group()), x))

f = lambda x: [(x['docs'], *c) for c in expr(x['text'])]
U = DataFrame(X.progress_apply(f, axis=1).explode().tolist(),
               columns=['docs', 'text', 'start', 'end', 'numerical'])\
               .dropna().query('numerical==True')

def date(d:str|None, m:str|None, y:str):
  from datetime import date as f
  import re
  
  d0, m0 = d is None, m is None
  if d0: d = 1
  elif m0: return None
  if m0: m = 1
  d = int(re.sub(r"[\D\s]", "", str(d)))
  m = int(re.sub(r"[\D\s]", "", str(m)))
  y = re.sub(r"[\D\s]", "", y)
  if len(y) == 2: 
    y = int(f"20{y}" if y[0] in "012" else f"19{y}")
  elif len(y) == 4: y = int(y)
  elif len(y) == 1: y = int(f"200{y}")
  else: return None
  try:
    D = f(y, m, d)
    if D.year > 2024: return None
    return (None if d0 else D.day, None if m0 else D.month, D.year)
  except ValueError: return None

def datenumQ( s = r"[\W\s]{1,3}",
             qd = r"(?:3[01]|[012]?\d)",
            qY4 = r"(?:20[012]\d|19\d\d)",
            qNd = r"(?:3[2-9]|[4-9]\d)"): return [
  rf"(?<!\d)(?P<A>{qd})?{s}(?P<B>{qd}){s}(?P<Y>{qY4}|{qNd}|{qd})(?!\d)",
  rf"(?<!\d)(?P<Y>{qY4}|{qNd}){s}(?P<A>{qd}){s}(?P<B>{qd})?(?!\d)",
  rf"(?<!\d)(?P<Y>{qY4})(?!\d)",
]

Qy = datenumQ()
def datenum(x:str):
  import re
  M0 = [(y.span(), y.group(), y.groupdict()) for q in Qy
        for y in re.finditer(q, x, flags=re.IGNORECASE)]
  V = [(r,x,date(d.get('A', None), d.get('B', None), d.get('Y'))) for r,x,d in M0] +\
      [(r,x,date(d.get('B', None), d.get('A', None), d.get('Y'))) for r,x,d in M0]
  return [(a,b,x,*D) for (a,b),x,D in V if D]

fD = lambda x: [(x['docs'], x['start'], x['end'], *c) for c in datenum(x['text'])]
D = DataFrame([y for u in U.progress_apply(fD, axis=1) if u for y in u],
              columns=['docs', 'start', 'end', 'numstart', 'numend', 'text', 'day', 'month', 'year'])\
              .convert_dtypes().drop_duplicates(subset=['docs', 'day', 'month', 'year'])

def monthQ(qY4 = r"(?:20[012]\d|19\d\d)"):
  qL = rf"({qY4}|\d\d?[\D\s]{{1,3}})?"*2
  qR = rf"([\D\s]{{1,3}}{qY4}|\d\d?)?"*2
  Q = zip(range(1, 12+1), Mo['PL'], Mo['EN'])
  Q = [(i, '|'.join(a+b)) for i, a, b in Q]
  Q = [(i, rf"{qL}({q}){qR}") for i, q in Q]
  return Q

Qm = monthQ()
def month(x:str):
  import re
  M = []
  for m0, (i0, z), (L0, L, _, R0, R) in [(m0, m.span(), m.groups()) 
    for m0, q in Qm for m in re.finditer(q, x, flags=re.IGNORECASE)]:
    l0 = len(L0) if L0 is not None else 0
    l = len(L) if L is not None else 0
    r0 = len(R0) if R0 is not None else 0
    r = len(R) if R is not None else 0
    if L0 is not None:
      if L is not None:
        M.extend((D, i0, z - r0 - r)
          for D in [date(L0, m0, L), date(L, m0, L0)]
          if D is not None)
        D0 = date(None, m0, y=L)
        if D0 is not None: 
          M.append((D0, i0 + l0, z - r0 - r))
        if R0 is not None:
          M.extend((D, i0 + l0, z - r)
            for D in [date(R0, m0, L), date(L, m0, R0)]
            if D is not None)
      else:
        D0 = date(None, m0, y=L0)
        if D0 is not None:
          M.append((D0, i0, z - r0 - r))
        if R0 is not None:
          M.extend((D, i0, z - r)
            for D in [date(L0, m0, R0), date(R0, m0, L0)]
            if D is not None)

    if R0 is not None:
      D0 = date(None, m0, y=R0)
      if D0 is not None:
        M.append((D0, i0 + l0 + l, z - r))
      if R is not None:
        M.extend((D, i0 + l0 + l, z)
          for D in [date(R0, m0, R), date(R, m0, R0)]
          if D is not None)

  return [(i0, z, *D) for D, i0, z in M]

fMo = lambda x: [(x['docs'], x['start'], x['end'], *c) for c in month(x['text'])]
M = DataFrame([y for u in U.progress_apply(fMo, axis=1) if u for y in u],
              columns=['docs', 'start', 'end', 'numstart', 'numend', 'day', 'month', 'year'])\
              .convert_dtypes().drop_duplicates(subset=['docs', 'day', 'month', 'year'])