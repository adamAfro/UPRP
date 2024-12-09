import pandas, pickle, traceback, sys
from lib.log import log, notify, progress
from lib.repo import Loader, Searcher

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

    M = [(k, L.melt(k)) for L in [
      Loader.Within("api.uprp.gov.pl"),
      Loader.Within("api.lens.org"),
      Loader.Within("api.openalex.org")
    ] for k in ['date', 'number', 'name', 'city', 'title']]

    S.iterload(progress(M, desc='📑'))
    with open('searcher.pkl', 'wb') as f:
      pickle.dump(S.snapshot, f)
      log('💾')

  else:
    with open('searcher.pkl', 'rb') as f:
      S.snapload(pickle.load(f))
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