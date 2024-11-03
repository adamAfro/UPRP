from pandas import read_csv, DataFrame
from tqdm import tqdm as progress; progress.pandas()
import os, json, warnings

def extract(x:str, target:str, v=None):
  "-> list[(sliced N, slice start, is-target-bool, T sliced like N if present)]"
  import re
  F = list(re.finditer(target, x))
  I = [i for m in F for i in m.span()]
  Q = [i for i in I if any((i == m.start()) for m in F)]
  if len(I) == 0: 
    if v is None: return [(x, 0, False)]
    else: return [(x, 0, False, v)]
  if I[0] != 0: I = [0] + I
  I = I + [len(x)]
  Y = [x[I[i]:I[i+1]] for i in range(len(I)-1)]
  Y = [(Y[i], I[i], I[i] in Q) for i in range(len(Y))]
  if v is not None: Y = [(*Y[i], v[I[i]:I[i+1]]) for i in range(len(I)-1)]
  return Y

def patregex( code = r'\b[a-z]{1,3}\.?(\s[a-z]{1,2}\.?)?', cforce = True,
              num = r'nume?r?|nr\.?',
              pat = r'(?<![a-z])(pate?n?t?|p)[\W]{0,2}',
              digits = r'((\d{3,}\s*\d{2,})|(\d{2,}\s*){3,})(\s*\d{2,})?',
              pre = r'(?<!(?<=\d)r|\d|\s)\s*',
              suf = r'(\s*[a-z]\d?(?!\w)\s?)?(?!\s*(doi|https\s*doi))' ):

  alpha = rf"({num}|({code})|{pat}){{1,3}}"
  if cforce:
    alpha = rf"({num}|{pat}){{1,2}}"
    alpha = rf"({alpha})?{code}({alpha})?"
  return rf"{pre}{alpha}\s*{digits}{suf}"

def dateregex(y=4, spaced=True, daymonth=True, ygreedy=False):
  y = r"(20[012]\d|19\d{2})" if y == 4 else r"\d{2}"
  if ygreedy: y = rf"{y}(\s*r(?!\w))\.?"
  if spaced: y = rf"{y}(\s*r(?!\w))?\.?"
  D = y
  if daymonth:
    d = r"[0123]?\d{1}"
    m = r"[01]?\d{1}"
    dm = rf"({d}\s+{m}|{m}\s+{d})"
    D = rf"({y}\s+{dm}|{dm}\s+{y})"

  R = rf"(?<!\d){D}(?!\d)"
  if not spaced: R.replace("\\s+", "\\s*")
  return R

with open(os.path.join('month.json'), 'r') as f: month = json.load(f)

Q = dict(
  
  etal = r'\b(et\s*al|i\s*in{1,2}i?)\s*\.?\b',
  pub = r"(wyd(anie)?|vol(ume)?|publ?(ikacja)?|iss(ue)?)\.?\s*(\d\s*)+",
  pgnum = r'\b((?<!(?<=p)l|\s)\s*p{1,2}|\s*s(tr)?)\.?\s*\d{1,5}\s+\d{1,5}\b',

  DOI = r"(https\s*)?\s*doi\.?\s*[\d\s\.]+",

  code = patregex(cforce=False),
  EP = patregex(r'ep\.?\s*[^\Wp]?\s*\w?'),
  PL = patregex(r'pl\.?\s*[^\Wp]?\s*\w?'),

  code56 = patregex(cforce=False, digits = r'\d?\d{2}\s*\d{3}'),
  EP56 = patregex(r'ep\.?\s*[^\Wp]?\s*\w?', digits = r'\d?\d{2}\s*\d{3}'),
  PL56 = patregex(r'pl\.?\s*[^\Wp]?\s*\w?', digits = r'\d?\d{2}\s*\d{3}'),

  Lmonth = rf"\d+\s*({'|'.join([ m for M in month['PL'] + month['EN'] for m in M ])})\s*\d*",
  Rmonth = rf"\s*\d*({'|'.join([ m for M in month['PL'] + month['EN'] for m in M ])})\s*\d+",
  fullmonth = rf"({'|'.join([ M[0] for M in month['PL'] + month['EN'] ] + [M[1] for M in month['PL']])})",
  yearnum = dateregex(4, daymonth=False),
  datealt = dateregex(2, ygreedy=True),
  datenumy = dateregex(4, ygreedy=True),
  datenum = dateregex(4),
  datealt0 = dateregex(2, spaced=False),
  datenum0 = dateregex(4, spaced=False),
  
  supgroup = '|'.join([rf'\{a}.*[\{{\}}\[\]\(\)\"].*\{b}' for a,b in ['()', '[]', '{}', '""']]),
  group = '|'.join([rf'\{a}.{{3,}}\{b}' for a,b in ['()', '[]', '{}', '""']]),
  sep = r'[,;:\(\)\[\]\{\}\'"]',
  alnum = r'\b([a-z]+\d+\w*|\d+[a-z]\w*)\b',
  URL = r'http[s]?://(?:\w|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+',
)

K = Q.keys()
def Fgen(k:str, G=[k for k in K if k not in ['supgroup', 'group']]):
  return (lambda t0, N0, a0, M0: [(t0, N0, a0, M0)] if any(M0[k] for k in G) 
          else [(t,  N,  a0+a, { **M0, k: m }) for N, a, m, t in extract(N0, Q[k], t0)])
def txtFgen(k:str, G=[k for k in K if k not in ['supgroup', 'group']]):
  return (lambda t0, N0, a0, M0: [(t0, N0, a0, M0)] if any(M0[k] for k in G) 
          else [(t,  N,  a0+a, { **M0, k: m }) for t, a, m, N in extract(t0, Q[k], N0)])
F = [

  lambda t0, N0, a0, M0: [(t,  N,  a0+a, { **M0, 'supgroup': m, 'supgroup-i': i }) 
    for i, (t, a, m, N) in enumerate(extract(t0, Q['supgroup'], N0))],

  lambda t0, N0, a0, M0: [(t,  N,  a0+a, { **M0, 'group': m, 'group-i': i }) 
    for i, (t, a, m, N) in enumerate(extract(t0, Q['group'], N0))],

  txtFgen('URL'),
  Fgen('pub'),
  Fgen('datenumy'), Fgen('datenum'),
  Fgen('DOI'),
  Fgen('EP'), Fgen('PL'), Fgen('code'),
  Fgen('Lmonth'), Fgen('Rmonth'),
  Fgen('EP56'), Fgen('PL56'), Fgen('code56'),

  Fgen('pgnum'), Fgen('etal'),
  Fgen('datealt'), Fgen('yearnum'), Fgen('fullmonth'),
  Fgen('alnum'),
  txtFgen('sep'),
]

def chain(t:str, N:str, a:int=0):
  global F, K
  "F[i](t, N, a, M) -> list[(t, N, a, M)]"
  Y = [(t, N, a, { k:False for k in Q.keys() })]
  for f in F:
    Y = [y for t, N, a, M in Y for y in f(t, N, a, M)]
  return Y

X = read_csv('../docs.csv')
X = X.reset_index().rename(columns={'docs':'text', 'index':'docs'})
X = X[['docs', 'text']]

X['start'] = 0
X['norm'] = X['text'].str.replace(r'[^\w\.]', " ", regex=True)
X['norm'] = X['norm'].str.replace(r'(\d)(\.)', lambda m: m.group(1) + " "*len(m.group(2)), regex=True)
X['norm'] = X['norm'].str.lower()

X = X.progress_apply(lambda x: [{ "docs": x['docs'], "text": t, "norm": N, "start": a, **M } 
                                for t, N, a, M in chain(x['text'], x['norm'])], axis=1)
X = DataFrame(X.explode().tolist())
X.insert(4, "end", X['start'] + X['text'].str.len() - 1)
X = X[(X['start'] <= X['end'])&(~X['sep'])&(X['text'].str.strip() != '')]

with warnings.catch_warnings():
  warnings.simplefilter("ignore", UserWarning)

  for k0 in ['EP', 'PL', 'code', 'pub']:
    for k in ['EP56', 'PL56', 'code56', 'pgnum', 'yearnum', 'datealt0', 'datenum0', "DOI"]:
      X.loc[X[k0], k] = X.loc[X[k0], 'norm'].str.contains(Q[k], regex=True)
  for k0 in ['Lmonth', 'Rmonth']:
    X.loc[X[k0], "fullmonth"] = X.loc[X[k0], 'norm'].str.contains(Q["fullmonth"], regex=True)

print("X.shape", X.shape)
print("docs", (DataFrame(X.groupby('docs').any()[K].sum(axis=0)).T/X['docs'].nunique()).round(2), sep='\n')
M0 = DataFrame(None, index=K, columns=K)
for i, k0 in enumerate(M0.index):
  for k in M0.columns[:i+1]:
    if k != k0: M0.loc[k, k0] = ""
    y = X.loc[X[k0], k].sum()
    r = round(y / X.shape[0] * 100, 2)
    M0.loc[k0, k] = f"{r:.2f}" if r > 0.0 else '0+  ' if y > 0 else '0   '
print("frac. X[x]&X[y]", M0, sep='\n')

X['lstrip'] = X['text'].str.replace(r'^[^\w\(\)\[\]"\.]+', "", regex=True)
X['start'] = X['start'] + X['text'].str.len() - X['lstrip'].str.len()
X['text'] = X['lstrip']
X = X.drop(columns=['lstrip'])

X['rstrip'] = X['text'].str.replace(r'[^\w\(\)\[\]"\.]+$', "", regex=True)
X['end'] = X['end'] - X['text'].str.len() + X['rstrip'].str.len()
X['text'] = X['rstrip']
X = X.drop(columns=['rstrip'])

X.drop(columns=["norm"]).to_csv('docs.chunks.csv', index=False)

from nbclient import NotebookClient
import nbformat
with open("insight.py", "r") as f: x = f.read()

J = nbformat.v4.new_notebook()
J.cells = [nbformat.v4.new_code_cell(c) 
           for c in x.split("#komórka")]

NotebookClient(J).execute()
nbformat.write(J, "insight.ipynb")