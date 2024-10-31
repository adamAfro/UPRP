from entities import Q, Sentence
from pandas import read_csv, DataFrame
from tqdm import tqdm as progress; progress.pandas()

def pattern(x:str):
  import re

  protocol = r'(\bhttps?://|www\.)'
  x = re.sub(protocol, "//", x)
  alpha = r'([^\W\d]+)'
  x = re.sub(alpha, lambda m: "C" if len(m.group(1)) > 4 else
                              "B" if len(m.group(1)) > 1 else "A", x)
  x = x.replace('B.', "D").replace('A.', "D")
  
  x = re.sub(r'\d+', lambda m: str(len(m.group(0))), x)
  
  webstart = r'//\S+'
  x = re.sub(webstart, "U", x)

  x = re.sub(r'[\W\s]', " ", x)
  x = re.sub(r'\s{2,}', " ", x)
  
  return x.strip()

N = read_csv('../docs.csv')
N = N.reset_index().rename(columns={'docs':'text', 'index':'docs'})
N = N[['docs', 'text']]

N3 = N.progress_apply(lambda x: [(3, x['docs'], y, p) for y, p in Sentence.nsub(x['text'], 3)], axis=1)
N5 = N.progress_apply(lambda x: [(5, x['docs'], y, p) for y, p in Sentence.nsub(x['text'], 5)], axis=1)
N7 = N.progress_apply(lambda x: [(7, x['docs'], y, p) for y, p in Sentence.nsub(x['text'], 7)], axis=1)

N = DataFrame([*N3.explode().tolist(),
               *N5.explode().tolist(),
               *N7.explode().tolist()],
               columns=['n', 'docs', 'text', 'start'])
N['start'] = (N['start']*100).astype(int)
N['norm'] = N['text'].str.replace(r'[^\w\.]', " ", regex=True)
N['norm'] = N['norm'].str.replace(r'(\d)(\.)', lambda m: m.group(1) + " "*len(m.group(2)), regex=True)
N['norm'] = N['norm'].str.lower()

N['pattern'] = N['norm'].progress_apply(pattern)
N = N.drop_duplicates(subset=['docs', 'pattern'])

N.to_csv('357-substr.pattern.csv', index=False)
reload = False
if reload:
  N = read_csv('357-substr.pattern.csv')

def match(k): return matchre(Q[k], k)
def matchre(q, k):
  import warnings
  with warnings.catch_warnings():#regexcatch
    warnings.simplefilter("ignore", UserWarning)
    global N
    print(q)
    print(k, end=' ')
    N[k] = N['norm'].str.contains(q, regex=True)
    n = N[N[k]]['docs'].nunique()
    r = N[N[k]]['docs'].nunique()/N['docs'].nunique()
    print(n, f'{round(r*100, 2)}%')

for k, q in reversed(Q.items()): matchre(q, k)
N.to_csv('357-substr.regex.csv', index=False)
reload = False
if reload:
  N = read_csv('357-substr.regex.csv')