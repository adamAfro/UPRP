import pandas, yaml, matplotlib.pyplot as plt

from lib.storage import Storage
from lib.geo import closest
from lib.flow import Flow
from util import strnorm
from util import data as D
from profiling import flow as f0
from geoloc import flow as fg

from matplotlib.ticker import MaxNLocator

@Flow.From()
def code(storage:Storage, assignpath:str):

  S = storage
  with open(assignpath, 'r') as f:
    S.assignement = yaml.load(f, Loader=yaml.FullLoader)

  K = ['frame', 'doc', 'id']
  A = S.melt('number-application').set_index(K)['value'].rename('number')
  B = S.melt('country-application').set_index(K)['value'].rename('country')
  Y = A.to_frame().join(B)

  Z = S.melt('application').set_index(K)['value'].rename('whole')
  Z = Z.str.replace(r'\W+', '', regex=True).str.upper().str.strip()
  Z = Z.str.extract(r'(?P<country>\D+)(?P<number>\d+)')

  Q = [X for X in [Y, Z] if not X.empty]
  if not Q: return pandas.DataFrame()
  Y = pandas.concat(Q, axis=0)
  Y = Y.reset_index().set_index('doc')[['country', 'number']]

  assert { 'country', 'number' }.issubset(Y.columns)
  assert { 'doc' }.issubset(Y.index.names)
  return Y

@Flow.From()
def event(storage:Storage, assignpath:str, codes:pandas.DataFrame):

  S = storage
  with open(assignpath, 'r') as f:
    S.assignement = yaml.load(f, Loader=yaml.FullLoader)

  X = pandas.concat([S.melt(f'date-{k}')[['doc', 'value', 'assignement']] 
                    for k in ['fill', 'application', 'exhibition', 'grant', 
                              'nogrant', 'decision', 'regional', 'priority', 'publication']])

  X['assignement'] = X['assignement'].str.split('-').str[1]
  X['value'] = pandas.to_datetime(X['value'], 
                                  errors='coerce', 
                                  format='mixed', 
                                  dayfirst=False)

  X = X.dropna(subset=['value'])
  X = X.drop_duplicates(subset=['doc', 'value', 'assignement'])
  X = X.set_index('doc')
  X = X.sort_values(by='value')

  X['year'] = X['value'].dt.year.astype(str)
  X['month'] = X['value'].dt.month.astype(str)
  X['day'] = X['value'].dt.day.astype(str)
  X['delay'] = (X['value'] - X['value'].min()).dt.days.astype(int)
  X = X.drop(columns='value')

  O = ['exhibition', 'priority', 'regional', 'fill', 'application', 'nogrant', 'grant', 'decision', 'publication']
  X['event'] = pandas.Categorical(X['assignement'], ordered=True, categories=O)
  X = X.drop(columns='assignement')

  X = X.join(codes, how='left')

  assert { 'event', 'year', 'month', 'day', 'delay', 'country', 'number' }.issubset(X.columns)
  assert { 'doc' }.issubset(X.index.names)
  return X

@Flow.From()
def classify(storage:Storage, assignpath:str, codes:pandas.DataFrame, extended=False):

  "Zwraca ramkę z klasyfikacjami."

  H = storage
  a = assignpath

  with open(a, 'r') as f:
    H.assignement = yaml.load(f, Loader=yaml.FullLoader)

  K = ['IPC', 'IPCR', 'CPC', 'NPC']
  K0 = ['section', 'class']
  if extended: K0 = K0 + ['subclass', 'group', 'subgroup']

  U = [H.melt(k).reset_index() for k in K]
  U = [m for m in U if not m.empty]
  if not U: return pandas.DataFrame()
  C = pandas.concat(U)

  C['value'] = C['value'].str.replace(r'\s+', ' ', regex=True)
  C['section'] = C['value'].str.extract(r'^(\w)') 
  C['class'] = C['value'].str.extract(r'^\w\s?(\d+)')
  if extended:

    raise NotImplementedError("fixit")
    C['subclass'] = C['value'].str.extract(r'^\w\s?\d+\s?(\w)')
    C['group'] = C['value'].str.extract(r'^\w\s?\d+\s?\w\s?(\d+)')
    C['subgroup'] = C['value'].str.extract(r'^\w\s?\d+\s?\w\s?\d+\s?/\s?(\d+)')

  C = C[['id', 'doc', 'assignement'] + K0]

  F = pandas.concat([H.melt(f'{k}-{k0}').reset_index() for k in K for k0 in K0])
  F = F.rename(columns={'assignement': 'classification'})
  F['assignement'] = F['classification'].str.split('-').str[0]
  P = F.pivot_table(index=['id', 'doc', 'assignement'], columns='classification', values='value', aggfunc='first').reset_index()
  P.columns = [k.split('-')[1] if '-' in k else k for k in P.columns]

  if (not C.empty) and (not P.empty):
    Y = pandas.concat([C, P], axis=0)
  elif not C.empty:
    Y = C
  elif not P.empty:
    Y = P
  else:
    return pandas.DataFrame()

  Y = pandas.concat([C, P], axis=0).drop(columns='id')
  Y.columns = ['doc', 'classification'] + K0
  Y = Y.set_index(['doc'])

  Y = Y.join(codes, how='left')

  assert { 'classification', 'section', 'class', 'country', 'number' }.issubset(Y.columns)
  assert { 'doc' }.issubset(Y.index.names)
  return Y

@Flow.From()
def geoloc(storage:Storage, 
           assignpath:str, 
           geodata:pandas.DataFrame, 
           codes:pandas.DataFrame,
           NAstr = ['bd', '~']):

  S = storage
  with open(assignpath, 'r') as f:
    S.assignement = yaml.load(f, Loader=yaml.FullLoader)

  C = S.melt('city').drop(columns=['repo', 'col', 'frame', 'assignement'])
  C = C[ ~ C['value'].isin(NAstr) ]
  C = C.drop_duplicates(subset=['doc', 'value'])
  C = C.set_index('doc')
  C = C['value'].str.split(',').explode()
  C = C.str.split(';').explode()

 #Wyciąganie z zapisów kodów pocztowych
  C = C.str.extractall(r'((?:[^\d\W]|\s)+)')[0].rename('value').dropna()

  C = C.reset_index().drop(columns='match')
  C['value'] = C['value'].apply(strnorm, dropinter=True, dropdigit=True)
  C = C.set_index('value')

  L = geodata
  L = L[ L['type'] == 'miasto' ].copy()
  L['city'] = L['city'].apply(strnorm, dropinter=True, dropdigit=True)

  Y = C.join(L.set_index('city'), how='inner').reset_index()
  Y = Y.dropna(subset=['lat','lon']).dropna(axis=1)
  Y = Y[['doc', 'value', 'lat', 'lon']]
  Y = Y.rename(columns={'value': 'city'})
  Y = Y.drop_duplicates()

  Y['lat'] = pandas.to_numeric(Y['lat'])
  Y['lon'] = pandas.to_numeric(Y['lon'])
  Y = closest(Y, 'doc', 'city', 'lat', 'lon')

  Y = Y.set_index('doc')
  Y = Y.join(codes, how='left')

  assert { 'city', 'lat', 'lon', 'loceval', 'country', 'number' }.issubset(Y.columns)
  assert { 'doc' }.issubset(Y.index.names)
  return Y

class Eval:

  def eventplot(events:pandas.DataFrame):

    X = events

    assert { 'delay', 'event' }.issubset(X.columns)
    assert { 'doc' }.issubset(X.index.names)

    d = X['year'].str.zfill(2)+'-'+X['month'].str.zfill(2)

    M = X.groupby([d, 'event']).size().unstack(fill_value=0).sort_index()
    f, A = plt.subplots(len(M.columns), constrained_layout=True, sharex=True, sharey=True)
    for i, k in enumerate(M.columns):
      M[k].plot.bar(xlabel='miesiąc', ax=A[i], ylabel=k, rot=0)
      A[i].xaxis.set_major_locator(MaxNLocator(integer=True, prune='both'))
    A[0].set_title('Liczba wydarzen w zależności od miesiąca')

  def eventnplot(X:pandas.DataFrame):

    assert 'event' in X.columns
    assert { 'doc' }.issubset(X.index.names)

    n = X.groupby(level=['doc'])['event'].value_counts().unstack(fill_value=0)
    n = n[ n.sum(axis=1) > 0 ]
    n.sort_values(by=[k for k in X['event'].dtype.categories], ascending=False)

    f, ax = plt.subplot_mosaic([['R', str(i)] for i in range(len(n.columns))], gridspec_kw={'width_ratios': [3, 1]})
    X.value_counts('event')[[k for k in X['assignement'].dtype.categories[::-1]]]\
     .plot.barh(title='Liczba wydarzeń w zależności od typu',
                xlabel='liczba', ax=ax['R']);

    for i, k in enumerate(n.columns):
      n[k].value_counts().sort_index().plot.pie(ax=ax[str(i)])
      ax[str(i)].set_ylabel(k)

    ax['0'].set_title("Liczba patentów\n z wydarzeniem typu")
    ax[str(len(n.columns)-1)].set_xlabel('ilość wydarzeń');

    return f

  def clsfnplot(X:pandas.DataFrame):

    assert { 'section', 'classification' }.issubset(X.columns)
    assert { 'doc' }.issubset(X.index.names)

    G = X.groupby(['section', 'classification']).size().unstack(fill_value=0)

    f, ax = plt.subplots(3, constrained_layout=True, sharex=True, sharey=True)
    G.plot.bar(title='Liczność sekcji patentowych w poszczególnych klasyfikacjach',
               xlabel='sekcja', stacked=False, subplots=True, legend=False, ax=ax);

    return f

flow = { k: dict() for k in D.keys() }

for h in flow.keys():

  flow[h]['patent-code'] = code(f0[h]['profiling'], assignpath=D[h]+'/assignement.yaml').map(D[h]+'/code.pkl')

  flow[h]['patent-geoloc'] = geoloc(f0[h]['profiling'], 
                                         assignpath=D[h]+'/assignement.yaml', 
                                         codes=flow[h]['patent-code'], 
                                         geodata=fg['Misc']['geodata'],).map(D[h]+'/geoloc.pkl')

  flow[h]['patent-event'] = event(f0[h]['profiling'], 
                                       assignpath=D[h]+'/assignement.yaml',
                                       codes=flow[h]['patent-code']).map(D[h]+'/event.pkl')

  flow[h]['patent-classify'] = classify(f0[h]['profiling'],
                                        assignpath=D[h]+'/assignement.yaml',
                                        codes=flow[h]['patent-code']).map(D[h]+'/classify.pkl')

for h in flow.keys():
  flow[h]['patentify'] = Flow(callback=lambda *x: x, args=[flow[h][k] for k in flow[h].keys()])