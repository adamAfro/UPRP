import pickle, yaml, os, re
from pandas import DataFrame, concat, to_datetime
from lib.log import log, notify, progress
from multiprocessing import Pool

def deleg(n:int): return min(n, os.cpu_count())

def melt(frame:DataFrame, dataname:str, framename:str):
  "`X:pandas.DataFrame; X.pipe(melt, 'generated', 'values')`"

  X = frame

  Y = X.reset_index(drop=False)\
      .melt(id_vars='doc', var_name='col')\
      .assign(dataset=dataname, frame=framename)\
      .dropna(subset=['value'])

  return Y[['dataset', 'frame', 'col', 'doc', 'value']]

class Replicator:

  def __init__(self, call:callable, *args, **kwargs):
    self.call = call
    self.args = args
    self.kwargs = kwargs

  def __call__(self, frame:DataFrame, column=None):

    "Zwraca ramkę kopi każdej z serii po zastosowaniu funkcji na rzędach"

    X = frame
    k = column

    if column is None: raise ValueError("not impl.")
    else: Y = X.dropna(subset=[k]).apply(lambda x: 
      [{ **x, k: y } for y in self.call(x[k], *self.args, **self.kwargs)], axis=1)

    Y = Y.explode().dropna().pipe(lambda S: DataFrame.from_records(S.tolist()))

    return Y

def posngram(x:str|float, n:int, repl:str='_', prefix=True, suffix=True):
  p = 1 if prefix else 0
  s = 1 if suffix else 0

  if not isinstance(x, str): return []
  if len(x) < n: return []
  return [ (p*i)*repl + x[i:i+n] + (s*max(0,(len(x)-3-i)))*repl 
    for i in range(len(x)-n+1) ]

def upwordy(x):
  if not isinstance(x, str): return []
  return re.sub("[^\w\s]|\d", "", x.upper()).split(' ')

def batch(frame: DataFrame, batches:int, call:callable, *args) -> DataFrame:

    X0 = frame
    b0 = batches
    n = X0.shape[0]
    b = n // b0 + (n % b0 > 0)
    B = [X0.iloc[i*b:(i+1)*b] for i in range(b0)]
    Y = []

    with Pool(processes=b0) as pool:
      Y = list(pool.starmap(call, [(X, *args) for X in B]))

    return concat(Y)

def batchjoin(batched:DataFrame, batches:int, target:DataFrame, *args) -> DataFrame:

  Q0 = batched
  X0 = target
  b0 = batches
  n = X0.shape[0]
  b = n // b0 + (n % b0 > 0)
  B = [X0.iloc[i*b:(i+1)*b] for i in range(b0)]
  Y = []

  with Pool(processes=b0) as pool:
    Y = list(pool.starmap(Q0.join, [(X, *args) for X in B]))

  return concat(Y)

class Loader:

  def __init__(self):

    Q: dict[ str, DataFrame ]
    with open('queries.pkl', 'rb') as f: Q = pickle.load(f)
    Q['dates'] = Q['dates'][['entry', 'year', 'month', 'day']].dropna()
    Q['codes'] = Q['codes'][['entry', 'number']]\
    .assign(number=lambda x: x['number'].str.replace(r"\D", "", regex=True))\
    .dropna()

    A = { 'api.lens.org': None, 'api.openalex.org': None, 'api.uprp.gov.pl': None }
    A: dict[str, dict[str, dict[str, str]]]
    for z in A:
      with open(f'{z}/assignement.yaml', 'r') as f:
        A[z] = yaml.load(f, Loader=yaml.FullLoader)

    Z = { z: None for z in A.keys() }
    Z: dict[ str, dict[ str, DataFrame ] ]
    for z in Z.keys():
      with open(f'{z}/data.pkl', 'rb') as f:
        Z[z] = pickle.load(f)
        for k, X in Z[z].items():
          X.set_index('doc', inplace=True)

    self.queries = Q
    self.assignments = A
    self.data = Z

  def _assigned(self, target:str):
    return ((z, h, k)
      for z, H in self.assignments.items()
      for h, X in H.items()
      for k, v in X.items()
      if v == target)

  def __call__(self, target:str):
    return concat((self.data[z][h][k].pipe(melt, z, h) for z, h, k in self._assigned(target)))

try:

  log('✨')
  notify(__file__, '✨')

  L = Loader()
  log('📂')

  Q = L.queries

  P:dict[str, DataFrame] = dict()
  P['dates'] = L('date')\
  .eval('value = @to_datetime(value, errors="coerce")')\
  .assign(year=lambda x: x['value'].dt.year)\
  .assign(month=lambda x: x['value'].dt.month)\
  .assign(day=lambda x: x['value'].dt.day)\
  .drop(columns=['value'])

  P['codes'] = L('number')\
  .assign(value=lambda x: x['value'].str.replace(r"\D", "", regex=True))\
  .drop_duplicates(subset=['value', 'doc'])

  K = ['target', 'dataset', 'frame', 'col']

  M = {

    'codes': Q['codes'].set_index('number')\
    .join(P['codes'].set_index('value'), how='inner')\
    .drop_duplicates(subset=['entry', 'doc'])\
    .assign(target="number")\
    .reset_index(drop=False)\
    .set_index(['doc', 'entry']),

    'dates': Q['dates'].set_index(['year', 'month', 'day'])\
    .join(P['dates'].set_index(['year', 'month', 'day']), how='inner')\
    .drop_duplicates(subset=['entry', 'doc'])\
    .assign(target="date")\
    .reset_index(drop=False)\
    .set_index(['doc', 'entry']),
  }

  log('🔗', 'strict', M['codes'].shape, M['dates'].shape)

  D = M['dates'].groupby(['year', 'month', 'day'])

  P['codes-posngrams'] = P['codes']\
  .pipe(batch, deleg(48), Replicator(posngram, n=3, suffix=False), 'value')\
  .drop_duplicates()\
  .set_index("doc")

  Q['codes-posngrams'] = Q['codes']\
  .pipe(Replicator(posngram, n=3, suffix=False), 'number')\
  .drop_duplicates()\
  .set_index("entry")

  k = 'codes-posngrams'
  I0 = M['codes']\
  .reset_index()\
  .set_index(['doc', 'entry', 'dataset', 'frame', 'col'])[[]]
  np = 0
  with progress(D, desc='🔗 num pos-3-gram') as D:
    for d, g in D:
      D.postfix = f'{d} {np}'

      q0 = g.index.get_level_values('entry')
      q0 = q0[q0.isin(Q[k].index)]
      q = Q[k].loc[q0].reset_index().set_index('number')
      if q.shape[0] == 0: continue

      p0 = g.index.get_level_values('doc')
      p0 = p0[p0.isin(P[k].index)]
      p = P[k].loc[p0].reset_index().set_index('value')
      if p.shape[0] == 0: continue

      l = q.join(p, how='inner')\
      .assign(target='number-posngram')\
      .set_index(['doc', 'entry', 'dataset', 'frame', 'col'])

      l = l.loc[l.index.difference(l.join(I0, how='inner').index)]
      if l.shape[0] == 0: continue
      np += l.shape[0]
      M[f'{k}{d}'] = l.reset_index(drop=False)

  P['cities'] = L('city')\
  .pipe(batch, deleg(16), Replicator(upwordy), 'value')\
  .drop_duplicates(subset=['value', 'doc'])\
  .query('value.str.len() > 2')\
  .pipe(batch, deleg(24), Replicator(posngram, n=3), 'value')\
  .drop_duplicates(subset=['value', 'doc'])\
  .set_index('doc')

  P['names'] = L('name')\
  .pipe(batch, deleg(24), Replicator(upwordy), 'value')\
  .drop_duplicates(subset=['value', 'doc'])\
  .query('value.str.len() > 2')\
  .pipe(batch, deleg(24), Replicator(posngram, n=3), 'value')\
  .drop_duplicates(subset=['value', 'doc'])\
  .set_index('doc')

  P['titles'] = L('title')\
  .pipe(batch, deleg(24), Replicator(upwordy), 'value')\
  .drop_duplicates(subset=['value', 'doc'])\
  .query('value.str.len() > 2')\
  .pipe(batch, deleg(24), Replicator(posngram, n=3), 'value')\
  .drop_duplicates(subset=['value', 'doc'])\
  .set_index('doc')

  Q['words'] = Q['raw']\
  .pipe(Replicator(upwordy), 'query')\
  .drop_duplicates()\
  .query('query.str.len() > 2')\
  .pipe(Replicator(posngram, n=3), 'query')\
  .drop_duplicates()\
  .set_index('entry')

  nw = 0
  with progress(D, desc='🔗 word pos-3-gram') as D:
    for d, g in D:
      D.postfix = f'{d} {nw}'
      for k in ['cities', 'names', 'titles']:

        q0 = g.index.get_level_values('entry')
        q0 = q0[q0.isin(Q['words'].index)]
        q = Q['words'].loc[q0].reset_index().set_index('query')
        if q.shape[0] == 0: continue

        p0 = g.index.get_level_values('doc')
        p0 = p0[p0.isin(P[k].index)]
        p = P[k].loc[p0].reset_index().set_index('value')
        if p.shape[0] == 0: continue

        l = q.join(p, how='inner')\
        .assign(target='word')\
        .set_index(['doc', 'entry', 'dataset', 'frame', 'col'])

        log(q.shape, p.shape, l.shape)

        if l.shape[0] == 0: continue
        nw += l.shape[0]
        M[f'{k}{d}'] = l.reset_index(drop=False)

  with open('matches.pkl', 'wb') as f:
    M = concat([X.reset_index() for X in M.values()], ignore_index=True)\
    .pivot_table(index=['entry', 'doc'], columns=K, aggfunc='size', fill_value=0)\
    .fillna(0)\
    .convert_dtypes()\
    .loc[:, lambda v: v.sum() > 0]

    pickle.dump(M, f)

    log('📂', 'matches.pkl', M.shape)

except Exception as e:
  log('❌', e)
  notify("❌")