from pandas import read_csv, merge, concat
from tqdm import tqdm as progress

byidL = dict(left_index=True, right_index=True, how="left")

class M: #Meta
  
  W = concat([read_csv('../meta/names.chunks.csv', dtype=str),
              read_csv('../meta/assignment.chunks.csv', dtype=str),
              read_csv('../meta/cities.chunks.csv', dtype=str),
              read_csv('../meta/titles.chunks.csv', dtype=str)])

  W = W[W['name'].str.len() > 3].drop_duplicates()
  W = W.set_index("name")

  n = W.index.value_counts()
  K = W[W.index.isin(n[n <= 50].index)]
  C = W[W.index.isin(n[n >  50].index)]

class R: #Recog

  W = read_csv('extract/docs.chunks.csv', dtype=str)
  W = W[(W['name'] == "word")&(W['content'].str.strip().str.len() > 3)]
  W = W.drop(columns=['content', 'name', 'raport']).dropna()

  W = W.set_index('normal').sort_index()
  n = W.value_counts('docs')
  
  K = W[W.index.isin(M.K.index)]
  C = W[W.index.isin(M.C.index)]

K = merge(R.K, M.K, **byidL).dropna().reset_index().drop_duplicates()
K = K.value_counts(subset=["P", "docs"]).reset_index()
K = K.rename(columns={'count': 'keys'})

C = concat([merge(R.C, C, **byidL)
            for C in progress([M.C[b:b +int(1000)]
            for b in range(0, len(M.C), int(1000))])])
C = C.dropna().reset_index().drop_duplicates()
C = C.value_counts(subset=["P", "docs"]).reset_index()
C = C.rename(columns={'count': 'common'})
C['words'] = R.n[C['docs']].values

Y = merge(K, C[((C['common'] / C['words']) > 0.2)&(C['common'] > 1)], 
          on=["P", "docs"], how="outer")
Y["keys"] = Y["keys"].fillna(0).astype(int)
Y["common"] = Y["common"].fillna(0).astype(int)
Y['words'] = R.n[Y['docs']].values
Y['words'] = Y['words'].astype(int)
Y.to_csv('linkage.keys.csv', index=False)