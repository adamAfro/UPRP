import pickle, yaml, os
from pandas import DataFrame, concat, to_datetime, Series
from lib.log import log, notify
from numpy import ndarray

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

def typogramstr(x:str, repl:str):
  "zwraca n-kopii, gdzie kolejne litery są zamienione na `repl`"
  return [ f"{x[:i]}{repl}{x[i+1:]}" for i in range(len(x)) ]

def ngram(frame:DataFrame, column:str, length:int, typo:str|None=None):

  X = frame
  n = length
  k = column

  Y = X.apply(lambda x: [{ **x, k: g } for g in ngramstr(x[k], n)], axis=1)\
     .explode().dropna().pipe(lambda S: DataFrame.from_records(S.tolist()))

  if typo is None: return Y

  t = typo

  Y = Y.apply(lambda x: [{ **x, k: g } for g in typogramstr(x[k], t)], axis=1)\
     .explode().pipe(lambda S: DataFrame.from_records(S.tolist()))

  return Y

try:

  log('✨')
  notify(__file__, '✨')

  Q0: dict[ str, DataFrame ]
  with open('queries.pkl', 'rb') as f:
    Q0 = pickle.load(f)

  log('🥒', 'queries.pkl')

  Z = { 'api.lens.org': None, 'api.openalex.org': None, 'api.uprp.gov.pl': None }
  Z: dict[ str, dict[ str, DataFrame ] ]
  for z in Z.keys():
    with open(f'{z}/data.pkl', 'rb') as f:
      Z[z] = pickle.load(f)
      for k, X in Z[z].items():
        X.set_index('doc', inplace=True)

    log('🥒', f'{z}/data.pkl')

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


  P0 = concat([Z[z][h][k].pipe(melt, z, h) for z, h, k in assigned('number')])\
     .set_index('value')
  log('🔖', 'number', P0.shape[0])

  QP0 = Q0['codes'][['entry', 'number']]\
      .assign(number=lambda x: x['number'].str.replace(r"\D", "", regex=True))\
      .set_index('number')
  log('👇', 'number', QP0.shape[0])

  p0 = QP0.join(P0, how='inner').drop_duplicates(subset=['entry', 'doc'])
  log('🔎', 'number', p0.shape[0])


  P5 = P0.reset_index(drop=False).pipe(ngram, 'value', 5)\
      .set_index("value").drop_duplicates()
  log('🔖', 'number 5-gram', P5.shape[0])

  QP5 = QP0.reset_index(drop=False).pipe(ngram, 'number', 5)\
      .set_index("number").drop_duplicates()
  log('👇', 'number 5-gram', QP5.shape[0])

  p5 = QP5.join(P5, how='inner').drop_duplicates(subset=['entry', 'doc'])
  log('🔎', 'number 5-gram', p5.shape[0])


  P5t = P0.reset_index(drop=False).pipe(ngram, 'value', 5, typo='_')\
      .set_index("value").drop_duplicates()
  log('🔖', 'number 5-gram-typo', P5t.shape[0])

  QP5t = QP0.reset_index(drop=False).pipe(ngram, 'number', 5, typo='_')\
      .set_index("number").drop_duplicates()
  log('👇', 'number 5-gram-typo', QP5t.shape[0])

  p5t = QP5t.join(P5t, how='inner').drop_duplicates(subset=['entry', 'doc'])
  log('🔎', 'number 5-gram', p5t.shape[0])


  D0 = concat([Z[z][h][k].pipe(melt, z, h) 
              for z, H in A.items() for h, X in H.items() for k, v in X.items() 
              if v == 'date']).set_index('value')
  log('🔖', 'date', D0.shape[0])

  D0 = concat([Z[z][h][k].pipe(melt, z, h) for z, h, k in assigned('date')])\
     .eval('value = @to_datetime(value, errors="coerce")')\
     .assign(year=lambda x: x['value'].dt.year)\
     .assign(month=lambda x: x['value'].dt.month)\
     .assign(day=lambda x: x['value'].dt.day)\
     .drop(columns=['value'])\
     .set_index(['year', 'month', 'day'])
  QD0 = Q0['dates'][['entry', 'year', 'month', 'day']]\
      .dropna().set_index(['year', 'month', 'day'])
  log('👇', 'date', QD0.shape[0])

  d0 = QD0.join(D0, how='inner').drop_duplicates(subset=['entry', 'doc'])
  log('🔎', 'date', d0.shape[0])


  with open('matches.pkl', 'wb') as f:
    pickle.dump({
      'number': p0,
      'number_5_gram': p5,
      'number_5_gram_typo': p5t,
      'date': d0 }, f)
    log('🥒', 'matches.pkl', os.path.getsize('matches.pkl'))


except Exception as e:
  log('❌', e)
  notify("❌")