import pandas, yaml, tqdm, altair as Plot
from lib.storage import Storage
from lib.query import Query
from lib.flow import Flow

@Flow.From()
def Indexing(storage:Storage, assignpath:str):

  """
  Indeksowanie danych z profili, jest wymagane do przeprowadzenia
  wyszukiwania w optymalny komputacyjnie sposób.

  Indeksowanie to etap po profilowaniu, który fragmentuje dane na
  ustalone typy: ciągy cyfrowe, daty, słowa kluczowe, n-gramy słów i ciągów.
  W zależności od typu, ilości powtórzeń w danych i ich długości posiadaja
  inne punktacje, które mogą być dalej wykorzystane w procesie wyszukiwania.

  ***

  Indeksowanie korzysta z wcześniej przypisanych ról do określenia tego
  w jaki sposób przetwarzać dane.

  Wartości danych to indeksy i są opisane przez ich źródło, tj.:
  repozytorium, ramkę, kolumnę i rolę. Takie przypisanie zapewnia
  klarowność wyszukiwania i możliwość określenia poziomu dopasowania.
  """

  from lib.index import Exact, Words, Digital, Ngrams, Date

  S = storage
  a = assignpath

  with open(a, 'r') as f:
    S.assignement = yaml.load(f, Loader=yaml.FullLoader)

  P0, P = Digital(), Ngrams(n=3, suffix=False)
  p = S.melt('number')
  p = P0.add(p)
  p['assignement'] = 'partial-number'
  p = P.add(p)

  D0 = Date()
  d = S.melt('date')
  d = D0.add(d)

  W0, W = Words(), Ngrams(n=3, suffix=False)

  for k in ['name', 'title', 'city']:

    w = S.melt(k)
    w = W0.add(w)
    w['assignement'] = f'partial-{k}'
    w = W.add(w)

  return P0, P, D0, W0, W

@Flow.From()
def Qdentify(qpath:str, storage:Storage, docsframe:str):

  """
  Dopasowanie zapytań do dokumentów na podstawie nazwy pliku.

  Rozpoznawanie zapytań odbywa się w zupełnie innym kontekście i
  nie zwraca dla zapytań informacji o tym skąd pochodzą.
  Identyfikacja korzysta z nazw plików i metadanych samych zapytań
  to dopasowania ich do odpowiednich danych w zbiorze.
  """

  Q = pandas.read_csv(qpath)
  D = storage.data[docsframe]

  assert 'P' in Q.columns
  assert 'filename' in D.columns

  D['P'] = D['filename'].str.extract(r'(\d{6}).*\.xml')
  Q['P'] = Q['P'].astype(str)
  Q = Q.set_index('P')
  D = D.reset_index().set_index('P')

  Y = Q.join(D['doc'], how='left').reset_index()
  if Y['doc'].isna().any(): raise ValueError()

  Y = Y.drop(columns=['P'])

  return Y

@Flow.From()
def Parsing(searches: pandas.Series):

  """
  Parsowanie zapytań to proces wyciągania z tekstów
  ciągów przypominających daty i numery patentowe.

  Proces polega na wstępnym podzieleniu całego napisu na
  części spełniające określone wyrażenia regularne. Później,
  te są łączone na podstawie tego czy w ich pobliżu są oczekiwane
  ciągi takie jak ciągi liczbowe albo skrótowce takie jak "PL".
  """

  Q = searches
  Y, P = [], []

  for i, q0 in Q.iterrows():

    q = Query.Parse(q0['query'])

    P.extend([{ 'entrydoc': q0['doc'], 'entry': i, **v } for v in q.codes])

    Y.extend([{ 'entrydoc': q0['doc'], 'entry': i, 'value': v, 'kind': 'number' } for v in q.numbers])
    Y.extend([{ 'entrydoc': q0['doc'], 'entry': i, 'value': v, 'kind': 'date' } for v in q.dates])
    Y.extend([{ 'entrydoc': q0['doc'], 'entry': i, 'value': v, 'kind': 'year' } for v in q.years])
    Y.extend([{ 'entrydoc': q0['doc'], 'entry': i, 'value': v, 'kind': 'word' } for v in q.words])

  return pandas.DataFrame(Y).set_index('entry'),\
         pandas.DataFrame(P).set_index('entry')

class Search:

  "Klasa z metodami pomocniczymi dla wyszukiwania."

  Levels = pandas.CategoricalDtype([
    "weakest", "dated", "partial",
    "supported", "partial-supported",
    "exact", "dated-supported",
    "partial-dated", "partial-dated-supported",
    "exact-supported",
    "exact-dated", "exact-dated-supported"
  ], ordered=True)

  def score(matches):

    "Zwraca ramkę z kolumnami 'score' i 'level' i indeksem 'doc' i 'entry'."

    import cudf

    X: cudf.DataFrame = matches
    S = cudf.DataFrame(index=X.index)

    for k0 in ['number', 'date', 'partial-number']:
      K = [k for k in X.columns if k[3] == k0]
      if not K: continue
      S[k0] = X.loc[:, K].max(axis=1)

    for k0 in ['name', 'city', 'title']:
      K = [k for k in X.columns if k[3] == k0]
      if not K: continue
      S[k0] = X.loc[:, K].sum(axis=1)

    S = S.reindex(columns=['number', 'date', 'partial-number', 'name', 'title', 'city'], fill_value=0)

    L = cudf.DataFrame(index=S.index)
    L['exact'] = S['number'] >= 1
    L['partial'] = S['partial-number'] > 0
    L['dated'] = S['date'] >= 1
    L['supported'] = S['title'] + S['name'] + S['city'] + \
      (S['title'] * S['name'] > 1.) + \
      (S['name']  * S['city'] > 1.) + \
      (S['title'] * S['city'] > 1.) >= 3.

    L0 = Search.Levels.categories
    L['level'] = L0[0]
    for c in L0[1:]:
      try: q = L[c.split('-')].all(axis=1)
      except KeyError: continue
      R = L['level'].where(~ q, c)
      L['level'] = R

    Z = cudf.DataFrame({ 'score': S.sum(axis=1), 
                         'level': L['level'].astype(cudf.CategoricalDtype.from_pandas(Search.Levels)) }, 
                        index=S.index)

    Y = Z.reset_index().sort_values(['level', 'score'])
    Y = Y.groupby('entry').tail(1).set_index(['doc', 'entry'])

    return Y

@Flow.From()
def Narrow(queries:pandas.Series, indexes:tuple, pbatch:int=2**14, ngram=True):

  """
  Wyszukiwanie ograniczone do połączeń kodami patentowymi.

  Wyszukiwanie w zależności od parametrów korzysta z dopasowania
  kodami patentowymi albo ich częściami. Później w grafie takich
  połączeń szuka dodatkowych dowodów na istnienie połączenia:
  wspólnych kluczy (np. imion i nazw miast) oraz dat.
  """

  import cudf

  Q0, _ = queries
  P0, P, D0, W0, W = indexes

  QP = Q0.query('kind == "number"')

  mP0 = P0.match(QP['value'], 'max').reset_index()
  mP0['entry'] = mP0['entry'].astype('int64')

  b = pbatch#parial
  if b is not None:

    mP = cudf.concat([P.match(QP.iloc[i:i+b]['value'], 'max', 0.751, ownermatch=mP0)
      for i in tqdm.tqdm(range(0, QP.shape[0], b))]).reset_index()

    m0P = cudf.concat([mP0, mP]).set_index('entry')

  else:
    m0P = mP0.set_index('entry')

  Q = m0P.join(cudf.from_pandas(Q0.astype(str)))\
  [['doc', 'value', 'kind']].to_pandas()#indeksy wymagają inputu pandas

  D0 = D0.extend('doc')
  mD0 = D0.match(Q[Q['kind'] == 'date'][['value', 'doc']], 'max').reset_index()
  if not mD0.empty: mD0['entry'] = mD0['entry'].astype('int64')

  M = cudf.concat([m0P.reset_index(), mD0]).pivot_table(
    index=['doc', 'entry'],
    columns=['repo', 'frame', 'col', 'assignement'],
    values='score', aggfunc='max') if not mP0.empty else cudf.DataFrame()

  W0 = W0.extend('doc')
  mW0 = W0.match(Q[Q['kind'] == 'word'][['value', 'doc']], 'sum').reset_index()
  mW0['entry'] = mW0['entry'].astype('int64')

  if ngram:
    W = W.extend('doc')
    mW = W.match(Q[Q['kind'] == 'word'][['value', 'doc']], 'sum', ownermatch=mW0)\
    .reset_index()

    mW0W = cudf.concat([mW0, mW])

  else:
    mW0W = mW0

  Ms = mW0W.pivot_table(
    index=['doc', 'entry'],
    columns=['repo', 'frame', 'col', 'assignement'],
    values='score',
    aggfunc='sum') if not mW0.empty else cudf.DataFrame()

  if not M.empty and not Ms.empty:
    J = M.join(Ms).fillna(0.0)
  elif not M.empty:
    J = M.fillna(0.0)
  elif not Ms.empty:
    J = Ms.fillna(0.0)
  else:
    return cudf.DataFrame()

  L = Search.score(J)
  L.columns = cudf.MultiIndex.from_tuples([('', '', '', 'score'), ('', '', '', 'level')])

  Y = J[J.index.isin(L.index)].join(L)
  Y = Y[Y[('', '', '', 'level')] >= "partial-supported"]

  Y = Y.reset_index().set_index(('entry', '', '', ''))

  E = Q0['entrydoc'].reset_index().drop_duplicates().set_index('entry')
  E = cudf.Series.from_pandas(E['entrydoc'])

  Y = Y.join(E, how='left')
  Y = Y.set_index(['entrydoc', ('doc', '', '', '')], append=True)
  Y.index.names = ['entry', 'entrydoc', 'doc']
  Y = Y.sort_index()
  Y.columns = cudf.MultiIndex.from_tuples(Y.columns)
  Y = Y[Y.columns[::-1]]

  return Y.to_pandas()

@Flow.From()
def Family(queries:pandas.Series, matches:pandas.DataFrame, storage:Storage, assignpath:str):

  "Podmienia kody w zapytaniach na te znalezione w rodzinie patentowej."

  Q, _ = queries
  M = matches
  S = storage

  with open(assignpath, 'r') as f:
    S.assignement = yaml.load(f, Loader=yaml.FullLoader)

  A = S.melt('family-number').reset_index()[['id', 'doc', 'value']]
  B = S.melt('family-jurisdiction').reset_index()[['id', 'doc', 'value']]\
        .rename(columns={'value': 'jurisdiction'})

  U = S.melt('family')[['doc', 'value']]
  U['jurisdiction'] = U['value'].str.extract(r'^(\D+)')
  U['value'] = U['value'].str.extract(r'(\d+)')

  C = pandas.DataFrame()
  if (not A.empty):
    C = A.merge(B, on=['id', 'doc']).drop(columns=['id'])

  C = pandas.concat([X for X in [C, U] if not X.empty]).set_index('doc')
  C = C.query('jurisdiction == "PL"').drop(columns='jurisdiction')

  M = M.index.to_frame().reset_index(drop=True).drop_duplicates()
  P = M.set_index('doc').join(C['value'], how='inner')
  P = P.set_index('entry').rename(columns={"doc_number": "value"})
  P['kind'] = 'number'

  Q = Q[ Q.index.isin(P.index) ].query('kind != "number"')

  Z = []
  for i, q0 in P['value'].items():
    Z.extend([{ 'entry': i, **v } for v in Query.Parse("PL"+q0).codes])

  Z = pandas.DataFrame(Z).set_index('entry')

  return pandas.concat([Q, P]).sort_index(), Z



@Flow.From()
def Drop(queries:pandas.Series, matches:list[pandas.DataFrame]):

  "Usuwa z wyników zapytań te, które już zostały dopasowane w zadowalający sposób."

  Q, P = queries
  M = matches
  K = [('entry', '', '', ''), ('doc', '', '', ''),
        ('', '', '', 'level'), ('', '', '', 'score')]

  Y = []

  for m in M:
    y = m.reset_index()[K]
    y.columns = ['entry', 'doc', 'level', 'score']
    Y.append(y)

  Y = pandas.concat(Y, axis=0, ignore_index=True)
  assert Y['level'].dtype == 'category'
  Y = Y.sort_values(['level', 'score'])
  Y['level'] = Y['level'].astype(Search.Levels)
  Y = Y[Y['level'] >= "partial-dated-supported"]

  p0 = P.index.astype(str).unique()
  p = p0[p0.isin(Y['entry'])]

  q0 = Q.index.astype(str).unique()
  q = q0[q0.isin(Y['entry'])]

  return Q[ ~ Q.index.isin(q)], P[ ~ P.index.isin(p) ]

@Flow.From()
def Preview(path:str,
            profile:dict[str, pandas.DataFrame],
            matches:pandas.DataFrame,
            queries:pandas.Series, n0=24, n=16):

  """
  Podgląd wyników przetwarzania jako plik tekstowy zawartych tabel,
  gdy podane są wyniki `matches` i `queries` to wyświetla również
  wyniki zapytań i ich dopasowania; jeśli nie to przykładowe obserwacje.
  """

  with pandas.option_context('display.max_columns', None,
                              'display.max_rows', n0,
                              'display.expand_frame_repr', False):

    H = profile
    Y = H.str()

    M = matches.sample(n)
    Q, _ = queries

    M = M[M.index.get_level_values('entry').isin(Q.index.values)]
    M = M.sample(min(M.shape[0], n))

    for i, m in M.iterrows():
      Y += str(Q.loc[ i[M.index.names.index('entry')] ].T) + \
      '\n\n' + str(m.to_frame().T) + '\n\n' + \
      H.strdocs([ i[M.index.names.index('doc')] ])

    with open(path, 'w') as f: f.write(Y)


@Flow.From()
def result(R: dict[str, pandas.DataFrame]):

  for k in R.keys():
    R[k] = R[k][[('', '', '', 'level'), ('', '', '', 'score')]]
    R[k].columns = ['level', 'score']
    R[k]['source'] = k

  Y = pandas.concat(R.values(), axis=0)

  return Y

@Flow.From()
def edges(X:pandas.DataFrame):

  Y = pandas.DataFrame({'to': X.index.get_level_values('entrydoc'),
                        'from': X.index.get_level_values('doc')})
  return Y

from profiling import flow as f0
from util import data as D

flow = { k: dict() for k in D.keys() }

linkback = Qdentify(qpath='raport.uprp.gov.pl.csv', 
                    storage=f0['UPRP']['profiling'], 
                    docsframe='raw').map(D['UPRP']+'/identify.pkl')

queries = Parsing(linkback).map('queries.pkl')

for k, p in D.items():

  flow[k]['index'] = Indexing(f0[k]['profiling'], assignpath=p+'/assignement.yaml').map(p+'/indexes.pkl')

  flow[k]['narrow'] = Narrow(queries, flow[k]['index'], pbatch=2**14).map(p+'/narrow.pkl')

flow['UPRP']['narrow'] = Narrow(queries, flow['UPRP']['index'], pbatch=2**13).map(D['UPRP']+'/narrow.pkl')

flow['USPG']['narrow'] = Narrow(queries, flow['USPG']['index'], pbatch=2**14).map(D['USPG']+'/narrow.pkl')

flow['USPA']['narrow'] = Narrow(queries, flow['USPA']['index'], pbatch=2**14).map(D['USPA']+'/narrow.pkl')

flow['Lens']['narrow'] = Narrow(queries, flow['Lens']['index'], pbatch=2**12).map(D["Lens"]+'/narrow.pkl')

drop0 = Drop(queries, [flow['UPRP']['narrow'], flow['Lens']['narrow']]).map('alien.base.pkl')

for k, p in D.items():

  flow[k]['drop'] = Drop(queries, [flow[k]['narrow']]).map(p+'/alien.pkl')

  flow[k]['preview'] = Preview(f"{p}/profile.txt", f0[k]['profiling'], flow[k]['narrow'], queries)

flow['Google']['narrow'] = Narrow(drop0, flow['Google']['index'], pbatch=2**10).map(D["Google"]+'/narrow.pkl')

for k0 in ['Lens', 'Google']:

  k = f'UPRP-{k0}'
  flow[k] = dict()
  p = f'{D["UPRP"]}/{D[k0]}'
  D[k] = p

  flow[k] = dict()

  flow[k]['query'] = Family(queries=queries, matches=flow[k0]['narrow'], storage=f0[k0]['profiling'], assignpath=D[k0]+'/assignement.yaml').map(p+'/family.pkl')

  flow[k]['narrow'] = Narrow(queries=flow[k]['query'], indexes=flow['UPRP']['index'], pbatch=None, ngram=False).map(D[k]+'/narrow.pkl')

drop = Drop(queries, [flow[k]['narrow'] for k in D.keys()]).map('alien.pkl')


results = result({ h: F for h, S in flow.items() for k, F in S.items() if k == 'narrow' })

valid = edges(flow['UPRP']['narrow'])

plots = dict()

plots['F-results'] = Flow(args=[linkback, results], callback=lambda Q, Y:

  pandas.concat([pandas.DataFrame({'count': [Q.shape[0]], 'source': 'UPRP', 'kind': 'Cytowania' }),
                 Y.value_counts('source').reset_index().assign(kind='Wyniki')])\

    .pipe(Plot.Chart)\
    .mark_bar()\
    .encode(Plot.Y('kind:N').title(None),
            Plot.X('sum(count)').title('Ilość wyników w poszczególnych źródłach'), 
            Plot.Color('source:N').title('Źródło')))

for k, F in plots.items():
  F.name = k
  F.map(f'fig/rprt/{k}.png')