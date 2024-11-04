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

X = read_csv('../docs.csv').reset_index().sample(10000)
X = X.rename(columns={'docs':'text', 'index':'docs'})[['docs', 'text']]

qURL=r'(http[s]?://(?:\w|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+)'
X['text'] = X['text'].progress_apply(lambda x:
  re.sub(qURL, lambda m: ' '*len(m.group()), x))

f = lambda x: [(x['docs'], *c) for c in expr(x['text'])]
U = DataFrame(X.progress_apply(f, axis=1).explode().tolist(),
               columns=['docs', 'text', 'start', 'end', 'numerical'])\
               .dropna().query('numerical==True')