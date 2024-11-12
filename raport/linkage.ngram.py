from pandas import read_csv, DataFrame, Series, concat
from tqdm import tqdm as progress; progress.pandas()

def mkngram(x:list, n:int): return [x[i:i+n] for i in range(len(x)-n+1)]
def norm(X:Series): 
  return X.str.replace('[\W\d\_]+', ' ', regex=True)\
          .str.replace('(?<!\w)\w{1,2}(?!\w)', ' ', regex=True)\
          .str.replace('\s+', ' ', regex=True).str.upper()

X = read_csv('docs.csv').reset_index()\
   .rename(columns={'docs':'text', 'index':'docs'})[['docs', 'text']]
X['norm'] = X['text'].pipe(norm)
X['n'] = X['norm'].str.split().apply(len)

E = []
for k, f0 in [("city", "../meta/data/cities.csv"),
              ("names", "../meta/data/names.csv"),
              ("orgs", "../meta/data/org.csv")]:

  T = read_csv(f0).dropna(subset=['name'])
  T = T[["name", "country", "number"]]\
    .drop_duplicates(subset=['country', 'number'])
  T['norm'] = T['name'].pipe(norm)
  T['n'] = T['norm'].str.split().apply(len)
  
  N = X

  n0 = min(T['n'].max(), 16)
  p = progress(reversed(range(2, n0+1)), total=n0-1, desc=k)
  for n in p:
    T0 = T.query('@n <= n')
    N0 = N.query('@n <= n')
    p.set_postfix(N=N0.shape[0], T=T0.shape[0], n=n)
    if T0.empty or N0.empty: continue

    N0 = N0.apply(lambda x: [{"docs": x["docs"], "link": n/x['n'], **dict(enumerate(N)) }
        for N in mkngram(x["norm"].split(), n)], axis=1)

    T0 = T.apply(lambda t: [{ **t, **dict(enumerate(N)), "mlink": n/t['n'] }
        for N in mkngram(t["norm"].split(), n)], axis=1)
    if T0.empty or N0.empty: continue

    N0 = DataFrame(N0.explode().dropna().tolist()).set_index([*range(n)])
    T0 = DataFrame(T0.explode().dropna().tolist()).set_index([*range(n)])

    E0 = N0.join(T0, how='inner').reset_index(drop=True)\
      .drop(columns=['norm', 'name'])
    E0['n'], E0['name'] = n, k
    E.append(E0)

    N = N.query("~(docs.isin(@E0['docs'])&(n + 1 > @n))")

E = concat(E)
E.to_csv('linkage.ngram.csv', index=False)