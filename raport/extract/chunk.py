from entities import Q, Sentence
from pandas import read_csv, DataFrame
from tqdm import tqdm as progress; progress.pandas()

X = read_csv('../docs.csv')
X = X.reset_index().rename(columns={'docs':'text', 'index':'docs'})
X = X[['docs', 'text']]
X['norm'] = X['text'].str.replace(r'[^\w\.]', " ", regex=True)

Y = dict()
for k, q in reversed(Q.items()):
  print(k)
  U = X.progress_apply(lambda x: [(x['docs'], n, y, q)
    for n, y, q in Sentence.extract(x['norm'], q, x['text'])], axis=1)
  U = DataFrame(U.explode().tolist(), columns=['docs', 'norm', 'text', k])
  U = U[ U['text'].str.len() > 0]
  X = U[~U[k]].drop(columns=[k])
  U = U[ U[k]].drop(columns=[k])
  print('n', U.shape[0], end='\n\n')
  Y[k] = U