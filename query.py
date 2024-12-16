import pandas, pickle, sys, random, os
from lib.log import log, notify, progress
from lib.repo import Storage, Searcher
try:

  r, b = False, 128
  for A in sys.argv[1:]:
    A = A.lower().strip()
    if A == 'reindex': r = True
    if A.startswith('batch='):
      try: b = int(A[6:])
      except: pass

  log(f'✨ id={os.getpid()} batch={b} reindex={r}')
  notify('🔴')

  S = Searcher()
  if r:

    M = [(k, L.melt(k)) for L in progress([
      Storage.Within("api.uprp.gov.pl"),
      Storage.Within("api.lens.org"),
      Storage.Within("api.openalex.org")
    ], desc='📂') for k in ['date', 'number', 'name', 'city', 'title']]

    M = [(k0, pandas.concat([X for k, X in M if k == k0 if not X.empty]))
         for k0 in progress(['date', 'number', 'name', 'city', 'title'], desc='🧱')]
    log('📏', { k0: X.shape[0] for k0, X in M })

    random.shuffle(M)

    S.add(progress(M, desc='📑'))
    with open(f'searcher.cu.pkl', 'wb') as f:
      pickle.dump(S.dump(), f)
      log('💾')

  else:
    with open(f'searcher.cu.pkl', 'rb') as f:
      S.load(pickle.load(f))
      log('📂')

  notify('🟡')

  Q0 = pandas.read_csv('raport.uprp.gov.pl.csv').reset_index()\
  .rename(columns={'docs':'query', 'index':'entry'})[['entry', 'query']]\
  .drop_duplicates(subset=['query'], keep='first')\
  .set_index('entry')['query']
  Q = [(i, q) for i, q in Q0.items()]

  log('🗳')

  notify('🟢')
  n = len(Q)//b
  B = enumerate(range(0, len(Q), b))
  Y = []

  for i0, i in progress(B, desc='🔍', total=n+1):

    Y += [S.search(Q[i:i+b], limit=5)]

    k = f'matches?b={b if b != 1.0 else ""}&k={i0+1}&n={n}.pkl'
    k0 = f'matches?b={b if b != 1.0 else ""}&k={i0}&n={n}.pkl'
    with open(k, 'wb') as f: pickle.dump(Y, f)
    if os.path.exists(k0): os.remove(k0)
    log('💾')

  log('✅')
  notify('🏁')

except Exception as e:
  log('❌', e)
  notify("❌")
  raise e.with_traceback(e.__traceback__)