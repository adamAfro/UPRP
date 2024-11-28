import pickle, yaml, os
from pandas import DataFrame, concat, to_datetime
from lib.log import log, notify
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

def ngramstr(x:str|float, n:int):
  if not isinstance(x, str): return []
  if len(x) < n: return []
  return [ x[i:i+n] for i in range(len(x)-n+1) ]

def ngram(frame:DataFrame, column:str, length:int):

  X = frame
  n = length
  k = column

  Y = X.apply(lambda x: [{ **x, k: g } for g in ngramstr(x[k], n)], axis=1)\
     .explode().dropna().pipe(lambda S: DataFrame.from_records(S.tolist()))

  return Y

def typostr(x:str, repl:str):
  "zwraca n-kopii, gdzie kolejne litery są zamienione na `repl`"
  return [ f"{x[:i]}{repl}{x[i+1:]}" for i in range(len(x)) ]

def typo(frame:DataFrame, column:str, repl:str):

  X = frame
  r = repl
  k = column

  Y = X.apply(lambda x: [{ **x, k: g } for g in typostr(x[k], r)], axis=1)\
     .explode().pipe(lambda S: DataFrame.from_records(S.tolist()))

  return Y

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

try:

  log('✨')
  notify(__file__, '✨')

  Q0: dict[ str, DataFrame ]
  with open('queries.pkl', 'rb') as f:
    Q0 = pickle.load(f)

  log('📂', 'queries.pkl')

  Z = { 'api.lens.org': None, 'api.openalex.org': None, 'api.uprp.gov.pl': None }
  Z: dict[ str, dict[ str, DataFrame ] ]
  for z in Z.keys():
    with open(f'{z}/data.pkl', 'rb') as f:
      Z[z] = pickle.load(f)
      for k, X in Z[z].items():
        X.set_index('doc', inplace=True)

    log('📂', f'{z}/data.pkl')

  A = { z: None for z in Z.keys() }
  A: dict[str, dict[str, dict[str, str]]]
  def assigned(target:str):
    return ((z, h, k)
      for z, H in A.items()
      for h, X in H.items()
      for k, v in X.items()
      if v == target)

  for z in A:
    with open(f'{z}/assignement.yaml', 'r') as f:
      A[z] = yaml.load(f, Loader=yaml.FullLoader)
      log('🗒', f'{z}/assignement.yaml')

  L, K0 = None, ['target', 'dataset', 'frame', 'col']
  PQ = Q0['codes'][['entry', 'number']]\
      .assign(number=lambda x: x['number'].str.replace(r"\D", "", regex=True))\
      .query('number.str.len() >= 5')\
      .set_index('number')
  QD = Q0['dates'][['entry', 'year', 'month', 'day']]\
      .dropna().set_index(['year', 'month', 'day'])

  P = concat((Z[z][h][k].pipe(melt, z, h) for z, h, k in assigned('number')))\
      .assign(value=lambda x: x['value'].str.replace(r"\D", "", regex=True))\
      .drop_duplicates(subset=['value', 'doc'])\
      .query('value.str.len() >= 5')\
      .set_index('value')
  L = PQ.join(P, how='inner')\
     .drop_duplicates(subset=['entry', 'doc'])\
     .assign(target="number")\
     .pivot_table(index=['entry', 'doc'], columns=K0, aggfunc='size', fill_value=0)
  log('🔢')

  D = concat((Z[z][h][k].pipe(melt, z, h) for z, h, k in assigned('number')))\
      .eval('value = @to_datetime(value, errors="coerce")')\
      .assign(year=lambda x: x['value'].dt.year)\
      .assign(month=lambda x: x['value'].dt.month)\
      .assign(day=lambda x: x['value'].dt.day)\
      .drop(columns=['value'])\
      .set_index(['year', 'month', 'day'])
  L = QD.join(D, how='inner')\
     .drop_duplicates(subset=['entry', 'doc'])\
     .assign(target="date")\
     .pivot_table(index=['entry', 'doc'], columns=K0, aggfunc='size', fill_value=0)\
     .pipe(lambda X: concat([X, L], axis=0))
  log('📅')

  N = concat((Z[z][h][k].pipe(melt, z, h) for z, h, k in assigned('name')))
  log('🪪')

  T = concat((Z[z][h][k].pipe(melt, z, h) for z, h, k in assigned('title')))
  log('📜')


  with open('matches.pkl', 'wb') as f:
    pickle.dump(L, f)
    log('💾', 'matches.pkl', os.path.getsize('matches.pkl'))


except Exception as e:
  log('❌', e)
  notify("❌")