import pandas, pickle
from lib.log import log, notify
from lib.repo import Loader, Searcher
from lib.parallel import listed

try:

  log('✨')
  notify('🔴')

  S = Searcher()
  S.load(Loader.Within("api.uprp.gov.pl"))
  S.load(Loader.Within("api.lens.org"))
  # S.load(Loader.Within("api.openalex.org"))
  log('📑')
  notify('🟡')

  Q = pandas.read_csv('raport.uprp.gov.pl.csv').reset_index()\
  .rename(columns={'docs':'query', 'index':'entry'})[['entry', 'query']]\
  .query('~ query.duplicated(keep="first")')
  log('🗳')

  notify('🟢')
  y = listed(Q['query'].tolist(), S.search, tqdmkwargs=dict(desc='🔎'))
  with open('matches.pkl', 'wb') as f:
    pickle.dump(y, f)

  log('✅')
  notify('🏁')

except Exception as e:
  log('❌', e)
  notify("❌")