import pandas, pickle, traceback, sys, random
from lib.log import log, notify, progress
from lib.repo import Storage, Searcher

r, U = False, 1.0
for A in sys.argv[1:]:
  A = A.lower().strip()

  if A == 'reindex': r = True
  if A.startswith('frac='):
    try: U = float(A[5:])
    except: pass

try:

  log('✨', U, r)
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
    M = [(k, X.iloc[(i*250_000):((i+1)*250_000)])
         for k, X in progress(M, desc='🔨')
         for i in range(X.shape[0] // 250_000 + 1)]

    random.shuffle(M)

    S.add(progress(M, desc='📑'))
    with open('searcher.pkl', 'wb') as f:
      pickle.dump(S.dump(), f)
      log('💾')

  else:
    with open('searcher.pkl', 'rb') as f:
      S.load(pickle.load(f))
      log('📂')

  notify('🟡')

  Q = pandas.read_csv('raport.uprp.gov.pl.csv').reset_index()\
  .rename(columns={'docs':'query', 'index':'entry'})[['entry', 'query']]\
  .drop_duplicates(subset=['query'], keep='first')
  if U != 1.0: Q = Q.sample(frac=U, random_state=0)
  log('🗳', U)

  notify('🟢')
  Y = S.multisearch(progress(Q.itertuples(index=False), desc='🔎', total=Q.shape[0]))
  with open(f'matches{U if U != 1.0 else ""}.pkl', 'wb') as f:
    pickle.dump(Y, f)

  log('✅')
  notify('🏁')

except Exception as e:
  log('❌', e)
  log(traceback.format_exc())
  notify("❌")