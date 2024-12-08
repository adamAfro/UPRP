import pandas, pickle, os
from lib.log import log, notify, progress
from lib.repo import Loader, Searcher

try:

  log('✨')
  notify('🔴')

  S = Searcher()
  S.load(Loader.Within("api.uprp.gov.pl"))
  S.load(Loader.Within("api.lens.org"))
  S.load(Loader.Within("api.openalex.org"))
  log('📑')
  notify('🟡')

  Q = pandas.read_csv('raport.uprp.gov.pl.csv').reset_index()\
  .rename(columns={'docs':'query', 'index':'entry'})[['entry', 'query']]\
  .query('~ query.duplicated(keep="first")')
  log('🗳')

  notify('🟢')
  Y = S.multisearch(progress(Q.itertuples(index=False), desc='🔎', total=Q.shape[0]))
  with open(f'matches{U if U != 1.0 else ""}.pkl', 'wb') as f:
    pickle.dump(Y, f)

  log('✅')
  notify('🏁')

except Exception as e:
  log('❌', e)
  notify("❌")