import sys, pandas, yaml, json, re, os, asyncio, aiohttp, unicodedata, zipfile
import xml.etree.ElementTree as ET
from lib.log import notify, log, progress
from lib.storage import Storage
from lib.query import Query
from lib.step import trail, Trace, Step, walk
from lib.profile import Profiler
from lib.alias import simplify
from lib.geo import closest
from pyproj import Transformer

@trail(Step)
def Profiling(dir:str, kind:str, assignpath:str, aliaspath:str,  profargs:dict={}):

  """
  Profilowanie danych, z różnych źródeł, do relacyjnych ramek danych.

  Profilowanie składa się z 2 etapów parsowania danych, 
  etapu normalizacji nazw oraz etapu manualnego przypisywania ról
  dla kolumn w wytworzonych ramkach danych.

  Dane wejściowe to heterogeniczna struktura 
  zagnieżdżonych obiektów z parametrami, które mogą być zarówno
  obiektami, listami obiektów oraz wartościami skalarnymi.

  To jak zagnieżdżony jest obiekt jest tutaj nazwane ścieżką,
  przykładowo dla obiektu `"krzesła"` w stukturze 
  `{ "dom": "pokój": { "krzesła": 3 } }`, ścieżka to "dom/pokój/krzesła".

  ***

  Heterogeniczność odnosi się do kilku faktów na temat danych:

  - istnienie parametru dla danej obserwacji nie jest gwarantowane;
  - typ wartości może różnić się po między obserwacjami mimo identycznej ścieżki;
  - to samo rzeczywiste zjawisko (np. autorstwo patentu) może być reprezentowane 
  w różny sposób: z różnymi parametrami, o różnych ścieżkach.

  Różnice w danych wynikają z różnic w wersjach schematu odpowiedniego 
  dla danego okresu, albo z braków danych.

  ***

  **Parsowanie danych** polega na odczytaniu zawartości zadanych plików,
  zgodnie z ich formatowaniem. Dane przetworzone na strukturę słowników
  są dalej analizowane pod kątem posiadania list podobnych obiektów.

  Kolejnym etapem jest tworzenie struktury homogenicznejz możliwymi brakami.
  Obiekty w listach są przetwarzane na oddzielne encje w oddzielnej strukturze,
  z relacją do obiektu w którym się znajdują. Wszystkie pozostałe wartości są
  przypisywane bezpośrednio do obiektu, w którym się znajdują niezależnie od
  poziomu zagnieżdżenia.

  W obu etapach nazwami danych i encji są ich ścieżki. Są one mało czytelne,
  z powodu ich długości dlatego wymagają normalizacji.

  ***

  **Normalizacja nazw** polega na przypisaniu krótkich, czytelnych nazw dla
  ścieżek w danych. Ścieżki zostają podzielone na pojedyncze fragmenty,
  czyli kolejne nazwy obiektów w których się znajdują. Z takich ciągów
  tworzony jest graf drzewa. W iteracjach po wierzchołkach wyciągane są
  nazwy w sposób, który zapewnia ich krótkość i unikalność. Jeśli pierwsza
  nazwa nie jest unikalna dodawany jest kolejny wierzchołek od końca, aż
  zapewni to unikalność. Jeśli cały proces nie odniesie sukcesu dodawane
  są liczby, aby zapewnić unikalność.

  ***

  **Przypisanie ról** polega na ręcznym przypisaniu nazw kolumnom w ramkach.
  Te nazwy są używane na dalszych etapach wyciągania danych.
  """

  P = Profiler( **profargs )
  
  kind = kind.upper()
  assert kind in ['JSON', 'JSONL', 'XML', 'HTMLMICRODATA']

  if kind == 'XML':
    P.XML(dir)
  elif kind == 'HTMLMICRODATA':
    P.HTMLmicrodata(dir)
  elif kind == 'JSON':
    P.JSON(dir)
  elif kind == 'JSONL':
    P.JSONl(dir, listname="data")

  H = P.dataframes()

  def pathnorm(x:str):
    x = re.sub(r'[^\w\.\-/\_]|\d', '', x)
    x = re.sub(r'\W+', '_', x)
    return x

  L = simplify(H, norm=pathnorm)
  H = { L['frames'][h0]: X.set_index(["id", "doc"])\
        .rename(columns=L['columns'][h0]) for h0, X in H.items() if not X.empty }

  L['columns'] = { L['frames'][h]: { v: k for k, v in Q.items() }  
                    for h, Q in L['columns'].items() }
  L['frames'] = { v: k for k, v in L['frames'].items() }
  with open(aliaspath, 'w') as f:
    yaml.dump(L, f, indent=2)#do wglądu

  A = { h: { k: None for k in V.keys() } for h, V in L['columns'].items() }
  with open(assignpath, 'w') as f:
    yaml.dump(A, f, indent=2)#do ręcznej edycji

  return Storage(dir, H)

@trail(Step)
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

@trail(Step)
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

@trail(Step)
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

@trail(Step)
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
      for i in progress(range(0, QP.shape[0], b))]).reset_index()

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

@trail(Step)
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



@trail(Step)
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

@trail(Trace)
def Preview(path:str,
            profile:dict[str, pandas.DataFrame],
            matches:pandas.DataFrame|None = None,
            queries:pandas.Series|None = None, n0=24, n=16):

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

    if matches is None:

      D = H.docs.sample(n).reset_index(drop=True).values
      Y += H.strdocs(D)

      with open(path, 'w') as f: f.write(Y)

      return

    M = matches.sample(n)
    Q, _ = queries

    M = M[M.index.get_level_values('entry').isin(Q.index.values)]
    M = M.sample(min(M.shape[0], n))

    for i, m in M.iterrows():
      Y += str(Q.loc[ i[M.index.names.index('entry')] ].T) + \
      '\n\n' + str(m.to_frame().T) + '\n\n' + \
      H.strdocs([ i[M.index.names.index('doc')] ])

    with open(path, 'w') as f: f.write(Y)

@trail(Trace)
def Fetch(queries:pandas.Series, URL:str, outdir:str):

  "Pobieranie pełnych stron HTML z wynikami wyszukiwania."

  async def scrap(P:list):

    t = 1
    H = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
    async with aiohttp.ClientSession(headers=H) as S:
      for i, p in P.iterrows():
        j, d = p['country'].upper(), ''.join(re.findall(r'\d+', p['number']))
        o = f"{outdir}/{j}{d}.html"
        if os.path.exists(o): continue
        x = f"{URL}/{j}{d}"
        async with S.get(x) as y0:
          y = await y0.text()
          await asyncio.sleep(t)
          with open(o, "w") as f: f.write(y)

  _, P = queries
  asyncio.run(scrap(P))

def txtnorm(input_str):
  nfkd_form = unicodedata.normalize('NFKD', input_str)
  return ''.join([c for c in nfkd_form if not unicodedata.combining(c)])

@trail(Step)
def LoadOSM(path:str):

  with open(path) as f:
    X = json.load(f)

  Y = []
  for H in X['features']:
    try: k = H['properties']['name']
    except KeyError: continue
    c = H['geometry']['coordinates']
    Y.append({'name': re.sub(r'\W+', ' ', k).upper().strip(), 
              'lat': c[1], 'lon': c[0]})

  Y = pandas.DataFrame(Y)

  Y['norm'] = Y['name'].apply(txtnorm)

  return Y

@trail(Step)
def Personify(storage:Storage, assignpath:str):

  """
  Zwraca ramkę z danymi osobowymi.

  Identyfikuje imiona osób na podstawie słownika imion o ile
  każde słowo w nazwie dłusze niż 2 znaki jest w słowniku.

  Działa tylko na wewnętrnzym zbiorze - do porpawnia
  """

  S = storage
  with open(assignpath, 'r') as f:
    S.assignement = yaml.load(f, Loader=yaml.FullLoader)

  K0 = ['name', 'fname', 'lname', 'city']
  P = pandas.DataFrame()

  for h in ['assignee', 'applicant', 'inventor']:

    K = [f'{k0}-{h}' for k0 in K0]
    p = pandas.concat([S.melt(f'{k}').reset_index() for k in K])
    p = p[['id', 'doc', 'value', 'assignement']]
    p['value'] = p['value'].str.upper().str.replace(r'\W+', ' ', regex=True).str.strip()
    p = p.pivot_table(index=['id', 'doc'], columns='assignement',
                      values='value', aggfunc='first')

    p.columns = [k.split('-')[0] for k in p.columns]
    p['role'] = h

    P = pandas.concat([P, p]) if not P.empty else p

  if P.empty:
    return pandas.DataFrame(), pandas.DataFrame()

  if 'fname' not in P.columns:
    return pandas.DataFrame(), P.reset_index().drop(columns=['id']).set_index(['doc', 'name'])

  N = pandas.concat([
    P['fname'].str.split(' ').explode().dropna().drop_duplicates()\
    .to_frame().assign(assignement='fname').rename(columns={'fname': 'word'}),
    P['lname'].str.split(' ').explode().dropna().drop_duplicates()\
    .to_frame().assign(assignement='lname').rename(columns={'lname': 'word'})
  ], ignore_index=True).drop_duplicates(subset='word').set_index('word')

  P['word'] = P['name'].str.replace(r'\b\w{,2}\b', '', regex=True)\
  .str.strip().dropna().apply(lambda x: ' '.join([y for y in set(x.split())]))

  P['N'] = P['word'].str.count(' ') + 1
  P['word'] = P['word'].str.split(' ')

  # ustalenie że nazwy stają się imieniem i nazwiskiem o ile
  # wszystkie (każde!) słowa w nazwie są w bazie słów imion i nazwisk
  Z = P.explode('word').dropna(subset='word')\
  .reset_index().set_index('word').join(N).dropna(subset='assignement')
  G = Z.groupby(['id', 'doc']).agg(n=('N', 'size'), N=('N', 'first'))\
  .reset_index().query('n == N')

  Z = Z.reset_index().set_index(['id', 'doc'])\
  .join(G[['id', 'doc']].set_index(['id', 'doc']), how='right')
  Z = Z.reset_index() # jeśli jest za dużo imion
  Z = Z.drop_duplicates(subset=['id', 'doc', 'assignement'])
  Z = Z.set_index(['id', 'doc'])

  fN = Z.query('assignement == "fname"')['word']
  lN = Z.query('assignement == "lname"')['word']
  P['fname'] = P['fname'].combine_first(fN)
  P['lname'] = P['lname'].combine_first(lN)

  #TODO: gdy imie jest w tabeli przed nazwiskiem może to sprawić
  # że nazwisko nie zostanie rozpoznane, mimo że słownik wskazuje inaczej

  P = P.reset_index().drop(columns=['word', 'N', 'id'])

  A = P.dropna(subset=['fname', 'lname']).drop(columns=['name'])\
  .drop_duplicates(subset=['doc', 'fname', 'lname'])\
  .set_index(['doc', 'fname', 'lname'])

  B = P.dropna(subset=['name']).drop(columns=['fname', 'lname'])\
  .drop_duplicates(subset=['doc', 'name'])\
  .set_index(['doc', 'name'])

  return A, B

@trail(Step)
def Geoloc(storage:Storage, geodata:pandas.DataFrame, assignpath:str):

  """
  Dopasowanie patentu do punktów geograficznych `(lat, lon)`.

  Uwagi:

  1. patent ma wiele lokalizacji (inaczej punktów powiązanych);
  2. nazwy mogą być zduplikowane; do zaimplementowania: estymacja 
  poprawnego miasta przez minimalizację średniej odległości 
  do pt.ów powiązanych.
  """

  S = storage
  with open(assignpath, 'r') as f:
    S.assignement = yaml.load(f, Loader=yaml.FullLoader)

  C = S.melt('city').drop(columns=['repo', 'id', 'col', 'frame', 'assignement'])
  C = C.drop_duplicates(subset=['doc', 'value'])
  C = C.set_index('doc')
  C = C['value'].str.split(',').explode()
  C = C.str.split(';').explode()
  C = C.str.upper().str.replace(r'\W+', ' ', regex=True)
  C = C.str.extractall(r'((?:[^\d\W]|\s)+)')[0].rename('value').dropna()
  C = C.str.upper().str.strip()
  C = C.reset_index().drop(columns='match')
  C = C.set_index('value')

  L = geodata
  L = L[ L['type'] == 'miasto' ].copy()
  L['name'] = L['name'].str.upper()\
                    .str.replace(r'\W+', ' ', regex=True)\
                    .str.strip()

  L = L.set_index('name')

  J = C.join(L, how='inner')
  J = J.reset_index().dropna(subset=['lat','lon']).dropna(axis=1)

  J = J[['doc', 'value', 'gmina', 'powiat', 'województwo', 'lat', 'lon']]
  J.columns = ['doc', 'name', 'gmina', 'powiat', 'województwo', 'lat', 'lon']
  J = J.drop_duplicates(subset=['doc', 'name', 'lat', 'lon'])

  J['lat'] = pandas.to_numeric(J['lat'])
  J['lon'] = pandas.to_numeric(J['lon'])
  Y = closest(J, 'doc', 'name', 'lat', 'lon')

  C = C[ ~ C.index.isin(Y['name']) ]
  C.index = C.index.to_series().apply(txtnorm).values
  L.index = L.index.to_series().apply(txtnorm).values

  J = C.join(L, how='inner')
  J = J.reset_index().dropna(subset=['lat','lon']).dropna(axis=1)

  J = J[['doc', 'index', 'gmina', 'powiat', 'województwo', 'lat', 'lon']]
                #^WTF: z jakiegoś powodu nie 'value'

  J.columns = ['doc', 'name', 'gmina', 'powiat', 'województwo', 'lat', 'lon']
  J = J.drop_duplicates(subset=['doc', 'name', 'lat', 'lon'])

  J['lat'] = pandas.to_numeric(J['lat'])
  J['lon'] = pandas.to_numeric(J['lon'])
  Y = pandas.concat([Y, closest(J, 'doc', 'name', 'lat', 'lon')])

  return Y

@trail(Step)
def Timeloc(storage:Storage, assignpath:str):

  "Wybiera najwcześniejsze daty dla każdego patentu"

  S = storage
  with open(assignpath, 'r') as f:
    S.assignement = yaml.load(f, Loader=yaml.FullLoader)

  C = pandas.concat([S.melt(f'date-{k}')[['doc', 'value', 'assignement']] 
                     for k in ['fill', 'application', 'exhibition', 'grant', 
                               'nogrant', 'decision', 'regional', 'priority', 'publication']])

  C['assignement'] = C['assignement'].str.split('-').str[1]
  C['value'] = pandas.to_datetime(C['value'], 
                                  errors='coerce', 
                                  format='mixed', 
                                  dayfirst=False)

  C = C.dropna(subset=['value'])
  C = C.drop_duplicates(subset=['doc', 'value', 'assignement'])
  C = C.set_index('doc')
  C = C.sort_values(by='value')

  C['year'] = C['value'].dt.year.astype(str)
  C['month'] = C['value'].dt.month.astype(str)
  C['day'] = C['value'].dt.day.astype(str)
  C['delay'] = (C['value'] - C['value'].min()).dt.days.astype(int)
  C = C.drop(columns='value')

  return C

@trail(Step)
def Classify(storage:Storage, assignpath:str):

  "Zwraca ramkę z klasyfikacjami."

  H = storage
  a = assignpath

  with open(a, 'r') as f:
    H.assignement = yaml.load(f, Loader=yaml.FullLoader)

  K = ['IPC', 'IPCR', 'CPC', 'NPC']
  K0 = ['section', 'class', 'subclass', 'group', 'subgroup']

  U = [H.melt(k).reset_index() for k in K]
  U = [m for m in U if not m.empty]
  if not U: return pandas.DataFrame()
  C = pandas.concat(U)

  C['value'] = C['value'].str.replace(r'\s+', ' ', regex=True)
  C['section'] = C['value'].str.extract(r'^(\w)') 
  C['class'] = C['value'].str.extract(r'^\w\s?(\d+)')
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

  Y = pandas.concat([C, P], axis=0)
  Y.columns = ['id', 'doc', 'classification'] + K0
  Y = Y.set_index(['id', 'doc'])

  return Y

@trail(Step)
def Pull(storage:Storage, assignpath:str, geodata:pandas.DataFrame, workdir:str):

  "Wyciąga dane zgodnie z przypisanymi rolami."

  os.makedirs(workdir, exist_ok=True)

  G = Geoloc(storage, geodata, assignpath, outpath=f'{workdir}/geo.pkl')
  T = Timeloc(storage, assignpath, outpath=f'{workdir}/time.pkl')
  C = Classify(storage, assignpath, outpath=f'{workdir}/clsf.pkl')
  P = Personify(storage, assignpath, outpath=f'{workdir}/people.pkl')

  return G(), T(), C(), P()

@trail(Trace)
def Bundle(dir:str,

           matches:   dict[str, pandas.DataFrame],
           pull:      dict[str, tuple[pandas.DataFrame]],

                                                         ):

  "Zapisuje połączone wyniki przetwarzania do plików CSV."

  M0, U = matches, pull
  G0 = { k: v[0] for k, v in U.items() }
  T0 = { k: v[1] for k, v in U.items() }
  C0 = { k: v[2] for k, v in U.items() }
  P0A = { k: v[3][0] for k, v in U.items() }
  P0B = { k: v[3][1] for k, v in U.items() }

  #reindex
  for M in M0.values():
    if M.empty: continue
    M.columns = ['::'.join(map(str, k)).strip('::') for k in M.columns.values]

  for G in G0.values():
    if G.empty: continue
    G.set_index('doc', inplace=True)

  for C in C0.values(): 
    if C.empty: continue
    C.reset_index(inplace=True)
    C.set_index('doc', inplace=True)
    C.drop(columns='id', inplace=True)

  for T in T0.values():
    if T.empty: continue
    pass

  #merge
  for k in M0.keys():

    M = M0[k]
    M['docrepo'] = k
    M.set_index('docrepo', append=True, inplace=True)

    try:
      G, T, C, PA, PB = G0[k], T0[k], C0[k], P0A[k], P0B[k]
      for X in [G, T, C, PA, PB]:
        X['docrepo'] = k
        X.set_index('docrepo', append=True, inplace=True)
    except KeyError: continue

  M0 = [X for X in M0.values() if not X.empty]
  G0 = [X for X in G0.values() if not X.empty]
  T0 = [X for X in T0.values() if not X.empty]
  C0 = [X for X in C0.values() if not X.empty]
  P0A = [X for X in P0A.values() if not X.empty]
  P0B = [X for X in P0B.values() if not X.empty]

  M0 = pandas.concat(M0, axis=0)
  G0 = pandas.concat(G0, axis=0)
  T0 = pandas.concat(T0, axis=0)
  C0 = pandas.concat(C0, axis=0)
  P0A = pandas.concat(P0A, axis=0)
  P0B = pandas.concat(P0B, axis=0)

  M0.to_csv(f'{dir}/pat:pat-raport-ocr.csv')
  G0.to_csv(f'{dir}/spatial:pat.csv')
  T0.to_csv(f'{dir}/event:pat.csv')
  C0.to_csv(f'{dir}/classification:pat.csv')
  P0A.to_csv(f'{dir}/people:pat-signed.csv')
  P0B.to_csv(f'{dir}/people:pat-named.csv')

  with zipfile.ZipFile(f'{dir}/dist.zip', 'w') as z:
    z.write(f'{dir}/pat:pat-raport-ocr.csv', 'pat:pat-raport-ocr.csv')
    z.write(f'{dir}/spatial:pat.csv', 'spatial:pat.csv')
    z.write(f'{dir}/event:pat.csv', 'event:pat.csv')
    z.write(f'{dir}/classification:pat.csv', 'classification:pat.csv')
    z.write(f'{dir}/people:pat-signed.csv', 'people:pat-signed.csv')
    z.write(f'{dir}/people:pat-named.csv', 'people:pat-named.csv')

@trail(Step)
def GMLParse(path:str):

    tree = ET.parse(path)
    root = tree.getroot()

    N = { 'ms': 'http://mapserver.gis.umn.edu/mapserver',
          'gml': 'http://www.opengis.net/gml/3.2',
          'wfs': 'http://www.opengis.net/wfs/2.0' }

    Y = []
    for M in root.findall('wfs:member', N):

      E = {}

      for e in M.find('ms:M1_UrzedoweNazwyMiejscowosci', N):
        E[e.tag.split('}')[1]] = e.text

      P = M.find('ms:M1_UrzedoweNazwyMiejscowosci/ms:msGeometry/gml:Point', N)
      U = P.find('gml:pos', N)
      if U is None: continue
      E['latitude'], E['longitude'] = U.text.split()
      E['srsName'] = P.attrib['srsName']

      Y.append(E)

    L = pandas.DataFrame(Y)
    T = Transformer.from_crs('EPSG:2180', 'EPSG:4326', always_xy=True)
    L['longitude'], L['latitude'] = zip(*L.apply(lambda r: T.transform(r['latitude'], r['longitude']), axis=1))

    L = L[[ 'RODZAJOBIEKTU', 'NAZWAGLOWNA', 'GMINA', 'POWIAT', 'WOJEWODZTWO', 'latitude', 'longitude' ]]
    L.columns = ['type', 'name', 'gmina', 'powiat', 'województwo', 'lat', 'lon']

    return L

@trail(Step)
def GeoXLSXload(path:str):

    L = pandas.read_excel(path, engine='openpyxl')
    L.columns = [re.sub(r'\s+', ' ', c).lower() for c in L.columns]
    L = L[[ 'rodzaj', 'nazwa miejscowości', 'powiat (miasto na prawach powiatu)', 'gmina', 'województwo', 'latitude', 'longitude' ]]
    L.columns = ['type', 'name', 'gmina', 'powiat', 'województwo', 'lat', 'lon']

    return L

try:

  D = { 'UPRP': 'api.uprp.gov.pl',
        'Lens': 'api.lens.org',
        # 'Open': 'api.openalex.org',
        'USPG': 'developer.uspto.gov/grant',
        'USPA': 'developer.uspto.gov/application',
        'Google': 'patents.google.com' }

  f = { k: dict() for k in D.keys() }

  f['Geoportal'] = dict()
  f['Geoportal']['parse'] = GMLParse(path='geoportal.gov.pl/wfs/name.gml', 
                                     outpath='geoportal.gov.pl/wfs/name.pkl')

  f['Misc'] = dict()
  f['Misc']['geodata'] = GeoXLSXload(path='prom/df_adresses_with_coordinates.xlsx', 
                                     outpath='prom/df_adresses_with_coordinates.pkl')

  f['UPRP']['profile'] = Profiling(D['UPRP']+'/raw/', kind='XML',
                                   assignpath=D['UPRP']+'/assignement.null.yaml', 
                                   aliaspath=D['UPRP']+'/alias.yaml',
                                   outpath=D['UPRP']+'/storage.pkl', skipable=True)

  f['Lens']['profile'] = Profiling(D['Lens']+'/res/', kind='JSONL',
                                   assignpath=D['Lens']+'/assignement.null.yaml', 
                                   aliaspath=D['Lens']+'/alias.yaml',
                                   outpath=D['Lens']+'/storage.pkl', skipable=True)

  # f['Open']['profile'] = Profiling(D['Open']+'/raw/', kind='JSON',
  #                                  profargs=dict(excluded=["abstract_inverted_index", "updated_date", "created_date"]),
  #                                  assignpath=D['Open']+'/assignement.null.yaml', 
  #                                  aliaspath=D['Open']+'/alias.yaml',
  #                                  outpath=D['Open']+'/storage.pkl')

  f['USPG']['profile'] = Profiling(D['USPG']+'/raw/', kind='XML',
                                   profargs=dict(only=['developer.uspto.gov/grant/raw/us-patent-grant/us-bibliographic-data-grant/']),
                                   assignpath=D['USPG']+'/assignement.null.yaml',
                                   aliaspath=D['USPG']+'/alias.yaml',
                                   outpath=D['USPG']+'/storage.pkl', skipable=True)

  f['USPA']['profile'] = Profiling(D['USPA']+'/raw/', kind='XML',
                                   profargs=dict(only=['developer.uspto.gov/application/raw/us-patent-application/us-bibliographic-data-application/']),
                                   assignpath=D['USPA']+'/assignement.null.yaml', 
                                   aliaspath=D['USPA']+'/alias.yaml',
                                   outpath=D['USPA']+'/storage.pkl', skipable=True)

  f['Google']['profile'] = Profiling(D['Google']+'/web/', kind='HTMLmicrodata',
                                     assignpath=D['Google']+'/assignement.null.yaml', 
                                     aliaspath=D['Google']+'/alias.yaml',
                                     outpath=D['Google']+'/storage.pkl', skipable=True)

  f['UPRP']['identify'] = Qdentify(qpath='raport.uprp.gov.pl.csv',
                                   storage=f['UPRP']['profile'], docsframe='raw',
                                   outpath=D['UPRP']+'/identify.pkl')

  f['All'] = dict()

  f['All']['query'] = Parsing(f['UPRP']['identify'], outpath='queries.pkl')

  for k, p in D.items():

    f[k]['index'] = Indexing(f[k]['profile'], assignpath=p+'/assignement.yaml',
                             outpath=p+'/indexes.pkl', skipable=True)

    f[k]['narrow'] = Narrow(f['All']['query'], f[k]['index'],
                            pbatch=2**14, outpath=p+'/narrow.pkl', skipable=True)

    f[k]['pull'] = Pull(f[k]['profile'], assignpath=p+'/assignement.yaml', 
                        geodata=f['Misc']['geodata'],
                        outpath=p+'/pull.pkl', workdir=p+'/bundle')

  f['UPRP']['narrow'] = Narrow(f['All']['query'], 
                               f['UPRP']['index'], pbatch=2**13, 
                               outpath=D['UPRP']+'/narrow.pkl', skipable=True)

  f['USPG']['narrow'] = Narrow(f['All']['query'], 
                               f['USPG']['index'], pbatch=2**14,
                                outpath=D['USPG']+'/narrow.pkl', skipable=True)

  f['USPA']['narrow'] = Narrow(f['All']['query'], 
                               f['USPA']['index'], pbatch=2**14, 
                               outpath=D['USPA']+'/narrow.pkl', skipable=True)

  f['Lens']['narrow'] = Narrow(f['All']['query'], 
                               f['Lens']['index'], pbatch=2**12, 
                               outpath=D["Lens"]+'/narrow.pkl', skipable=True)

  f['Base'] = dict()
  f['Base']['drop'] = Drop(f['All']['query'], [f['UPRP']['narrow'], f['Lens']['narrow']],
                           outpath='alien.base')

  for k, p in D.items():

    f[k]['drop'] = Drop(f['All']['query'], [f[k]['narrow']],
                        outpath=p+'/alien')

    f[k]['preview0'] = Preview(f"{p}/profile.txt", f[k]['profile'])
    f[k]['preview'] = Preview(f"{p}/profile.txt", f[k]['profile'], 
                              f[k]['narrow'], f['All']['query'])

  D['Google'] = 'patents.google.com'

  f['Google']['narrow'] = Narrow(f['Base']['drop'], 
                                 f['Google']['index'], pbatch=2**10, 
                                 outpath=D["Google"]+'/narrow.pkl', skipable=True)

  for k0 in ['Lens', 'Google']:

    k = f'UPRP-{k0}'
    f[k] = dict()
    p = f'{D["UPRP"]}/{D[k0]}'
    D[k] = p

    f[k] = dict()
    f[k]['query'] = Family(queries=f['All']['query'], 
                                   matches=f[k0]['narrow'], 
                                   storage=f[k0]['profile'],  
                                   assignpath=D[k0]+'/assignement.yaml',
                                   outpath=p+'/family.pkl')

    f[k]['narrow'] = Narrow(queries=f[k]['query'],
                            indexes=f['UPRP']['index'],
                            outpath=D[k]+'/narrow.pkl',
                            pbatch=None, ngram=False, skipable=True)

    f[k]['pull'] = Pull(f['UPRP']['profile'], assignpath=D['UPRP']+'/assignement.yaml', 
                        geodata=f['Geoportal']['parse'],
                        workdir=p+'/bundle',
                        outpath=p+'/pull.pkl')

  f['All']['drop'] = Drop(f['All']['query'], [f[k]['narrow'] for k in D.keys()], 
                          outpath='alien')


  f['Google']['fetch'] = Fetch(f['All']['drop'], 'https://patents.google.com/patent',
                              outdir=D['Google']+'/web')

  f['All']['bundle'] = Bundle('bundle',
                              matches={ k: f[k]['narrow'] for k in D.keys() },
                              pull={ k: f[k]['pull'] for k in D.keys() if '-' not in k })

  if sys.argv[1] == 'emigrate':

    if os.path.exists('migraton.zip'):
      os.remove('migraton.zip')
    with zipfile.ZipFile(f'migraton.zip', 'w') as z:

      for k in ['UPRP', 'Lens', 'USPG', 'USPA', 'Google']:

        z.write(f"{D[k]}/narrow.pkl")
        z.write(f"{D[k]}/storage.pkl")
        z.write(f"{D[k]}/assignement.yaml")
        z.write(f"{D[k]}/alias.yaml")

      for k in ['Lens', 'Google']:

        z.write(f"{D['UPRP']}/{D[k]}/narrow.pkl")

    log('📥', f'{(os.path.getsize("migraton.zip")/ (1024 ** 3)):.2} GB')

    exit()

  if sys.argv[1] == 'imigrate':

    import zipfile

    with zipfile.ZipFile(f'migraton.zip', 'r') as z:

      z.extractall()

    exit()

  if sys.argv[1] == 'restart':

    for k in f.keys():
      print(k)
      for h in f[k].keys():
        print(k, h)
        if not isinstance(f[k][h], Step): continue
        f[k][h].restart()

    exit()

  if sys.argv[1] == 'print':

    for k in f.keys():
      for h in f[k].keys():
        f[k][h].name = f"{k}.{h}"

    def traceprint(x:Trace, indent=0):
      print(' '*indent + str(x))
      for s in x.steps:
        walk(s, lambda a: traceprint(a, indent+1))

    for k in f.keys():
      for h in f[k].keys():
        if not isinstance(f[k][h], Trace): continue
        walk(f[k][h], traceprint)

    exit()

  if sys.argv[1] == 'docsgen':

    from lib.notebook import gen

    n = '\n'

    D = set()
    L = set()
    for k in f.keys():
      for h in f[k].keys():
        f[k][h].name = f"{k}.{h}"

    def traceprint(x:Trace, target=None):
      global Y
      if target:
        L.add(f'{x.name}["{str(x).replace(": ", ":<br>")}"]')
        L.add(f'{x.name} --> {target.name}')
        L.add(f'click {x.name} "#{x.__class__.__name__.lower().replace(".", "-")}"')
        D.add((x.__class__.__name__, x.__class__.__doc__))
      for s in x.steps:
        walk(s, lambda a: traceprint(a, x))

    u = f['All']['bundle']
    walk(u, traceprint)
    L.add(f'{u.name}["{u}"]')
    L.add(f'click {u.name} "#{u.__class__.__name__.lower().replace(".", "-")}"')

    with open('bundle/readme.md') as f: S = f.read()

    D = [(k, re.sub(r'\n\s+', r'\n', d)) for k, d in D if d] #unindent
    D = [f"{k}\n{'-'*len(k)}\n\n{d}\n" for k, d in D]
    D = [re.sub(r'\*{3}\n', r'\n\n', d) for d in D]

    V = f'```\npython {sys.version}\npandas {pandas.__version__}\n```\n\n\n'

    Y = f"```mermaid\ngraph LR\n{n.join(list(L))}\n```"
    gen('insight.py', 'raport.ipynb', [S], [V, Y] + D)

    exit()

  E = []
  for a in sys.argv[1:]:

    k, h = a.split('.')
    f0 = f[k][h]

    log('🚀', os.getpid(), ' '.join(sys.argv))

    notify(a)

    f0.endpoint()

    notify("✅")

except Exception as e:

  notify("❌")

  raise e.with_traceback(e.__traceback__)