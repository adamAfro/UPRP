from pandas import read_csv, DataFrame
from tqdm import tqdm as progress; progress.pandas()

class Sentence:

  def split(x:str,  open = ['[', '(', '{'],
                    close = [']', ')', '}', ',', ';', ':'],
                    shift = ['"']):
    i, y, Y = 0, '', []
    for i in range(len(x)):
      if x[i] in open:
        if len(y) > 0: Y.append(y)
        y = x[i]
      elif x[i] in close:
        Y.append(y + x[i])
        y = ''
      elif x[i] in shift:
        if (len(y) > 0) and (y[0] != x[i]): 
          Y.append(y); y = x[i]
        else: Y.append(y + x[i]); y = ''
      else: y += x[i]
    if len(y) > 0: Y.append(y)
    return Y
  
  def extract(N:str, target:str, T=None):
    import re
    F = list(re.finditer(target, N))
    I = [i for m in F for i in m.span()]
    Q = [i for i in I if any((i == m.start()) for m in F)]
    if len(I) == 0: 
      if T is None: return [(N, False)]
      else: return [(N, T, False)]
    if I[0] != 0: I = [0] + I
    if I[-1]+1 != len(N): I = I + [len(N)]
    Y = [N[I[i]:I[i+1]] for i in range(len(I)-1)]
    Y = [(Y[i], I[i] in Q) for i in range(len(Y))]
    if T is not None:
      Y = [(N[I[i]:I[i+1]], T[I[i]:I[i+1]], Y[i][1]) for i in range(len(I)-1)]
    return Y

class Entity:
  import os, json
  def patregex( code = r'\b[a-z]{1,3}\.?(\s[a-z]{1,2}\.?)?', cforce = True,
                num = r'nume?r?|nr\.?',
                pat = r'(?<![a-z])(pate?n?t?|p)[\W]{0,2}',
                digits = r'((\d{3,}\s*\d{2,})|(\d{2,}\s*){3,})\s*(\d{2,}\s*)?',
                pre = r'(?<!(?<=\d)r|\d|\s)\s*',
                suf = r'(\w\d?\s*)?(?!\s*\d)(?!\s*doi)(?!\s*https\s*doi)' ):

    alpha = rf"({num}|({code})|{pat}){{1,3}}"
    if cforce:
      alpha = rf"({num}|{pat}){{1,2}}"
      alpha = rf"({alpha})?{code}({alpha})?"
    return rf"{pre}{alpha}\s*{digits}{suf}"

  with open(os.path.join('month.json'), 'r') as f: month = json.load(f)
  datenum = [ r"\d{4}\s+\d{1,2}\s+\d{1,2}",
              r"\d{1,2}\s+\d{1,2}\s+\d{4}",
              r"\d{1,2}\s+\d{1,2}\s+\d{2}",
              r"\d{2}\s+\d{1,2}\s+\d{1,2}" ]

Q = dict(
  etal = r'\b(et\s*al\.?|i\s*in{1,2}i?\.?)\b',

  pub = r"(wyd?a?n?i?e?|vol|publ?i?k?a?c?j?a?)\.?\s*(\d\s*)+",
  
  pgnum = r'\b((?<!(?<=p)l|\s)\s*p{1,2}|s(tr)?)\.?\s*\d{1,5}\s+\d{1,5}\b',

  code = Entity.patregex(cforce=False),
  code56 = Entity.patregex(cforce=False,
                            digits = r'\d?\d{2}\s*\d{3}'),
  EP56 = Entity.patregex(r'ep\.?\s*[^\Wp]?\s*\w?',
                  digits = r'\d?\d{2}\s*\d{3}'),
  PL56 = Entity.patregex(r'pl\.?\s*[^\Wp]?\s*\w?',
                  digits = r'\d?\d{2}\s*\d{3}'),

  Lmonth = rf"\d+\s*({'|'.join([ m for M in Entity.month['PL'] + Entity.month['EN'] for m in M ])})\s*\d*",
  Rmonth = rf"\s*\d*({'|'.join([ m for M in Entity.month['PL'] + Entity.month['EN'] for m in M ])})\s*\d+",
  fullmonth = rf"({'|'.join([ M[0] for M in Entity.month['PL'] + Entity.month['EN'] ] + [M[1] for M in Entity.month['PL']])})",
  yearnum = r"(?<!\d|\s)\s*(20[012]\d|19\d{2})\s*(?!\s\d)",
  datenum = rf"{'|'.join(Entity.datenum)}"
)

def extract(x, k, C, ignore=[]):
  if len(ignore) > 0:
    if any((hasattr(x, i) and (x[i]) == True) for i in ignore):
      return [(x['docs'], x['text'], x['norm'], False, *x[C])]

  return [(x['docs'], T, N, m, *x[C])
    for N, T, m in Sentence.extract(x['norm'], Q[k], x['text'])]

X = read_csv('../docs.csv')
X = X.reset_index().rename(columns={'docs':'text', 'index':'docs'})
X = X[['docs', 'text']]

X['norm'] = X['text'].str.replace(r'[^\w\.]', " ", regex=True)
X['norm'] = X['norm'].str.replace(r'(\d)(\.)', lambda m: m.group(1) + " "*len(m.group(2)), regex=True)
X['norm'] = X['norm'].str.lower()

def extraction(k:str, ignore=[]):
  global X
  C = X.columns[~X.columns.isin(["docs", "text", "norm"])]
  X = X.progress_apply(lambda x: extract(x, k, C, ignore), axis=1)
  C = ["docs", "text", "norm", k, *C]
  X = DataFrame(X.explode().tolist(), columns=C)
  X = X.dropna(subset=['text'])

for k in ['code', 'datenum', 'Lmonth', 'Rmonth']:
  extraction(k, ignore=Q.keys())
  print(k, X[k].sum())

for k in ['EP56', 'PL56', 'code56', 'pgnum', 'pub']: 
  extraction(k, ignore=Q.keys())
  print(k, X[k].sum())

for k in ['etal', 'yearnum', 'fullmonth']: 
  extraction(k, ignore=Q.keys())
  print(k, X[k].sum())

for k in ['EP56', 'PL56', 'code56', 'pgnum', 'pub']:
  X.loc[X['code'], k] = X.loc[X['code'], 'norm'].str.contains(k, regex=True)
  print(k, X[k].sum())

C = X.columns[~X.columns.isin(["docs", "text", "norm"])]
X = X.progress_apply(lambda x:
  [(x['docs'], x['text'], *x[C])] if x[Q.keys()].any() else 
  [(x['docs'], y, *x[C]) for y in Sentence.split(x['text'])], axis=1)
C = ["docs", "text", *C]
X = DataFrame(X.explode().tolist(), columns=C)
X = X.dropna(subset=['text'])
X.to_csv('docs.chunks.csv')