import sys, pandas, cudf, matplotlib.pyplot as pyplot
import yaml, re, os, asyncio, aiohttp, unicodedata
import xml.etree.ElementTree as ET
from lib.log import notify, log, progress
from lib.storage import Storage
from lib.query import Query
from lib.step import trail, Trace, Step
from lib.profile import Profiler
from lib.alias import simplify
from lib.index import Exact, Words, Digital, Ngrams, Date
from lib.geo import closest

@trail(Step)
def Profiling(dir:str, kind:str, assignpath:str, aliaspath:str,  profargs:dict={}):

  "Profilowanie danych z różnych źródeł do ramek danych relacyjnych."

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
def Indexing(storage:Storage, assignpath:str) -> tuple[Digital, Ngrams, Exact, Words, Ngrams]:

  "Indeksowanie danych z profili."

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

  "Dopasowanie zapytań do dokumentów na podstawie kolumny P."

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

  Y['entry'] = Y['doc']
  Y = Y.drop(columns=['P', 'doc'])

  return Y

@trail(Step)
def Parsing(searches: pandas.Series):

  Q = searches.set_index('entry')['query']
  Q.index = Q.index.astype('str')

  Y, P = [], []

  for i, q0 in Q.items():

    q = Query.Parse(q0)

    P.extend([{ 'entry': i, **v } for v in q.codes])

    Y.extend([{ 'entry': i, 'value': v, 'kind': 'number' } for v in q.numbers])
    Y.extend([{ 'entry': i, 'value': v, 'kind': 'date' } for v in q.dates])
    Y.extend([{ 'entry': i, 'value': v, 'kind': 'year' } for v in q.years])
    Y.extend([{ 'entry': i, 'value': v, 'kind': 'word' } for v in q.words])

  return pandas.DataFrame(Y).set_index('entry'),\
         pandas.DataFrame(P).set_index('entry')

class Search:

  "Klasa z metodami pomocniczymi dla wyszukiwania."

  Levels = cudf.CategoricalDtype([
    "weakest", "dated", "partial",
    "supported", "partial-supported",
    "exact", "dated-supported",
    "partial-dated", "partial-dated-supported",
    "exact-supported",
    "exact-dated", "exact-dated-supported"
  ], ordered=True)

  def score(matches:cudf.DataFrame):

    "Zwraca ramkę z kolumnami 'score' i 'level' i indeksem 'doc' i 'entry'."

    X = matches
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

    L0 = Search.Levels.categories.to_pandas()
    L['level'] = L0[0]
    for c in L0[1:]:
      try: q = L[c.split('-')].all(axis=1)
      except KeyError: continue
      R = L['level'].where(~ q, c)
      L['level'] = R

    Z = cudf.DataFrame({ 'score': S.sum(axis=1), 'level': L['level'].astype(Search.Levels) }, index=S.index)

    Y = Z.reset_index().sort_values(['level', 'score'])
    Y = Y.groupby('entry').tail(1).set_index(['doc', 'entry'])

    return Y

  def insight(matches:pandas.DataFrame):

    M = matches
    K = [('', '', '', 'level'), ('', '', '', 'score')] +\
        [k for r in ['api.uprp.gov.pl', 'api.lens.org', 'api.openalex.org']
          for k0 in ['date', 'number', 'name', 'city', 'title']
          for k in M.columns if (k[0] == r) and (k[3] == k0)]

    M = M[K].sort_values(by=[('', '', '', 'level'),
                             ('', '', '', 'score')], ascending=False)

    Y = M.reset_index()[[('entry', '', '', ''), 
                         ('doc', '', '', ''),
                         ('', '', '', 'level'),
                         ('', '', '', 'score')]]

    Y.columns = ['entry', 'doc', 'level', 'score']
    Y = Y.drop_duplicates(subset='entry')

    F, A = pyplot.subplots(1, 2, figsize=(14, 4))

    (Y['level'] >= 'exact-dated')\
    .replace({True: 'dokładne', False: 'niedokładne'})\
    .value_counts()\
    .plot.pie(title=f'Dokładność dopasowania n={Y.shape[0]}', 
              ylabel='', xlabel='', colors=['y', 'g'], autopct='%1.1f%%', ax=A[0]);

    Y.value_counts('level').sort_index()\
    .plot.barh(title='Rozkład poziomów dopasowania', ylabel='', xlabel='', ax=A[1],
               color=[k for k in 'rrrryyyyyggg']);

    def draw(path:str):
      F.savefig(path, format='png')
      pyplot.close()

    return draw

@trail(Step)
def Narrow(queries:pandas.Series, indexes:tuple[Digital, Ngrams, Exact, Words, Ngrams], batch:int=2**14):

  "Wyszukiwanie ograniczone (patrz: kod) oparte o kody, daty i słowa kluczowe."

  Q, _ = queries
  P0, P, D0, W0, W = indexes

  QP = Q.query('kind == "number"')

  mP0 = P0.match(QP['value'], 'max').reset_index()

  b = batch
  mP = cudf.concat([P.match(QP.iloc[i:i+b]['value'], 'max', 0.6, ownermatch=mP0)
    for i in progress(range(0, QP.shape[0], b))]).reset_index()

  m0P = cudf.concat([mP0, mP]).set_index('entry')

  Q = m0P.join(cudf.from_pandas(Q.astype(str)))\
  [['doc', 'value', 'kind']].to_pandas()

  D0 = D0.extend('doc')
  mD0 = D0.match(Q[Q['kind'] == 'date'][['value', 'doc']], 'max')\
  .reset_index()

  M = cudf.concat([m0P.reset_index(), mD0]).pivot_table(
    index=['doc', 'entry'],
    columns=['repo', 'frame', 'col', 'assignement'],
    values='score', aggfunc='max') if not mP0.empty else cudf.DataFrame()

  W0 = W0.extend('doc')
  mW0 = W0.match(Q[Q['kind'] == 'word'][['value', 'doc']], 'sum')\
  .reset_index()

  W = W.extend('doc')
  mW = W.match(Q[Q['kind'] == 'word'][['value', 'doc']], 'sum', ownermatch=mW0)\
  .reset_index()

  Ms = cudf.concat([mW0, mW]).pivot_table(
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

  #TODO odzyskać insight

  return Y

@trail(Step)
def Drop(queries:pandas.Series, matches:list[pandas.DataFrame]):

  Q, P = queries
  M = matches
  K = [('entry', '', '', ''), ('doc', '', '', ''),
        ('', '', '', 'level'), ('', '', '', 'score')]

  Y = []

  for m in M:
    y = m.reset_index()[K]
    y.columns = ['entry', 'doc', 'level', 'score']
    Y.append(y)

  Y = cudf.concat(Y, axis=0, ignore_index=True)
  assert Y['level'].dtype == 'category'
  Y = Y.sort_values(['level', 'score'])
  Y['level'] = Y['level'].astype(Search.Levels)
  Y = Y[Y['level'] >= "partial-dated-supported"]

  p0 = P.index.astype(str).unique()
  p = p0[p0.isin(Y['entry'].values_host)]

  q0 = Q.index.astype(str).unique()
  q = q0[q0.isin(Y['entry'].values_host)]

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

    M = matches.to_pandas().sample(n)
    Q, _ = queries

    M = M[M.index.get_level_values(1).isin(Q.index.values)]
    M = M.sample(min(M.shape[0], n))

    for i, m in M.iterrows():
      Y += str(Q.loc[ i[1] ]) + '\n\n' + str(m.to_frame().T) + '\n\n' + H.strdocs([ i[0] ])

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
  L = L[ L['RODZAJOBIEKTU'] == 'miasto' ].copy()
  L['NAZWAGLOWNA'] = L['NAZWAGLOWNA'].str.upper()\
                    .str.replace(r'\W+', ' ', regex=True)\
                    .str.strip()

  L = L.set_index('NAZWAGLOWNA')

  J = C.join(L, how='inner')
  J = J.reset_index().dropna(axis=1)

  J = J[[ 'doc', 'value', 'GMINA', 'POWIAT', 'WOJEWODZTWO', 'latitude', 'longitude', 'srsName' ]]
  J.columns = ['doc', 'name', 'gmina', 'powiat', 'województwo', 'lat', 'lon', 'srs']
  J = J.drop_duplicates(subset=['doc', 'name', 'lat', 'lon'])

  J['lat'] = pandas.to_numeric(J['lat'])
  J['lon'] = pandas.to_numeric(J['lon'])
  Y = closest(J, 'doc', 'name', 'lat', 'lon', 'EPSG:2180')

  def plremove(text):#ASCII
    x = unicodedata.normalize('NFD', text)
    return ''.join([c for c in x if unicodedata.category(c) != 'Mn'])
  C = C[ ~ C.index.isin(Y['name']) ]
  C.index = C.index.to_series().apply(plremove).values
  L.index = L.index.to_series().apply(plremove).values

  J = C.join(L, how='inner')
  J = J.reset_index().dropna(axis=1)

  J = J[[ 'doc', 'index', 'GMINA', 'POWIAT', 'WOJEWODZTWO', 'latitude', 'longitude', 'srsName' ]]
                  #^WTF: z jakiegoś powodu nie 'value'

  J.columns = ['doc', 'name', 'gmina', 'powiat', 'województwo', 'lat', 'lon', 'srs']
  J = J.drop_duplicates(subset=['doc', 'name', 'lat', 'lon'])

  J['lat'] = pandas.to_numeric(J['lat'])
  J['lon'] = pandas.to_numeric(J['lon'])
  Y = pandas.concat([Y, closest(J, 'doc', 'name', 'lat', 'lon', 'EPSG:2180')])

  return Y

@trail(Step)
def Timeloc(storage:Storage, assignpath:str):

  "Wybiera daty z patentów"

  S = storage
  with open(assignpath, 'r') as f:
    S.assignement = yaml.load(f, Loader=yaml.FullLoader)

  C = S.melt('date')[['doc', 'value']]
  C['value'] = pandas.to_datetime(C['value'], 
                                  errors='coerce', 
                                  format='mixed', 
                                  dayfirst=False)

  C = C.drop_duplicates(subset=['doc', 'value'])
  C = C.set_index('doc')

  return C

@trail(Step)
def Classify(storage:Storage, assignpath:str):

  """
  Zwraca ramkę z klasyfikacjami, przyporządkowanie może być szczegółowe,
  np. IPC-section albo ogólne IPC.
  """

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
def Bundle(matches:dict[str, cudf.DataFrame],
           storages:dict[str, Storage],
           geo: dict[str, pandas.DataFrame], 
           time:dict[str, pandas.DataFrame], 
           clsf:dict[str, pandas.DataFrame]):

  U0, S, G, T, C = matches, storages, geo, time, clsf

  for h, M in U0.items():
    M[('', '', '', 'repo')] = h
  M = pandas.concat([M.reset_index().to_pandas() for M in U0.values()], axis=0)
  M = M.sort_values([('', '', '', 'level'), ('', '', '', 'score')])
  M = M.drop_duplicates(subset=[('entry', '', '', '')], keep='first')
  M = M.set_index([('entry', '', '', '')])

  return M

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

    return pandas.DataFrame(Y)

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

  f['UPRP']['profile'] = Profiling(D['UPRP']+'/raw/', kind='XML',
                                   assignpath=D['UPRP']+'/assignement.null.yaml', 
                                   aliaspath=D['UPRP']+'/alias.yaml',
                                   outpath=D['UPRP']+'/storage.pkl')

  f['Lens']['profile'] = Profiling(D['Lens']+'/res/', kind='JSONL',
                                   assignpath=D['Lens']+'/assignement.null.yaml', 
                                   aliaspath=D['Lens']+'/alias.yaml',
                                   outpath=D['Lens']+'/storage.pkl')

  # f['Open']['profile'] = Profiling(D['Open']+'/raw/', kind='JSON',
  #                                  profargs=dict(excluded=["abstract_inverted_index", "updated_date", "created_date"]),
  #                                  assignpath=D['Open']+'/assignement.null.yaml', 
  #                                  aliaspath=D['Open']+'/alias.yaml',
  #                                  outpath=D['Open']+'/storage.pkl')

  f['USPG']['profile'] = Profiling(D['USPG']+'/raw/', kind='XML',
                                   profargs=dict(only=['developer.uspto.gov/grant/raw/us-patent-grant/us-bibliographic-data-grant/']),
                                   assignpath=D['USPG']+'/assignement.null.yaml',
                                   aliaspath=D['USPG']+'/alias.yaml',
                                   outpath=D['USPG']+'/storage.pkl')

  f['USPA']['profile'] = Profiling(D['USPA']+'/raw/', kind='XML',
                                   profargs=dict(only=['developer.uspto.gov/application/raw/us-patent-application/us-bibliographic-data-application/']),
                                   assignpath=D['USPA']+'/assignement.null.yaml', 
                                   aliaspath=D['USPA']+'/alias.yaml',
                                   outpath=D['USPA']+'/storage.pkl')

  f['Google']['profile'] = Profiling(D['Google']+'/web/', kind='HTMLmicrodata',
                                     assignpath=D['Google']+'/assignement.null.yaml', 
                                     aliaspath=D['Google']+'/alias.yaml',
                                     outpath=D['Google']+'/storage.pkl')

  f['UPRP']['identify'] = Qdentify(qpath='raport.uprp.gov.pl.csv',
                                   storage=f['UPRP']['profile'], docsframe='raw',
                                   outpath=D['UPRP']+'/identify.pkl')

  f['All'] = dict()

  f['All']['query'] = Parsing(f['UPRP']['identify'], outpath='queries.pkl')

  for k, p in D.items():

    f[k]['index'] = Indexing(f[k]['profile'], assignpath=p+'/assignement.yaml',
                             outpath=p+'/indexes.pkl', skipable=True)

    f[k]['narrow'] = Narrow(f['All']['query'], f[k]['index'],
                            batch=2**14, outpath=p+'/narrow.pkl')

    f[k]['drop'] = Drop(f['All']['query'], [f[k]['narrow']],
                        outpath=p+'/alien', skipable=False)

    f[k]['preview0'] = Preview(f"{p}/profile.txt", f[k]['profile'])
    f[k]['preview'] = Preview(f"{p}/profile.txt", f[k]['profile'], 
                              f[k]['narrow'], f['All']['query'])

    f[k]['geoloc'] = Geoloc(f[k]['profile'], assignpath=p+'/assignement.yaml', 
                            geodata=f['Geoportal']['parse'],
                            outpath=p+'/geoloc.pkl', skipable=True)

    f[k]['timeloc'] = Timeloc(f[k]['profile'], assignpath=p+'/assignement.yaml',
                              outpath=p+'/timeloc.pkl', skipable=True)

    f[k]['classify'] = Classify(storage=f[k]['profile'], 
                                assignpath=p+'/assignement.yaml',
                                outpath=p+'/classification.pkl')

  f['UPRP']['narrow'] = Narrow(f['All']['query'], 
                               f['UPRP']['index'], batch=2**14, 
                               outpath=D['UPRP']+'/narrow.pkl')

  f['USPG']['narrow'] = Narrow(f['All']['query'], 
                               f['USPG']['index'], batch=2**14,
                                outpath=D['USPG']+'/narrow.pkl')

  f['USPA']['narrow'] = Narrow(f['All']['query'], 
                               f['USPA']['index'], batch=2**14, 
                               outpath=D['USPA']+'/narrow.pkl')

  f['Lens']['narrow'] = Narrow(f['All']['query'], 
                               f['Lens']['index'], batch=2**12, 
                               outpath=D["Lens"]+'/narrow.pkl')

  f['Base'] = dict()
  f['Base']['drop'] = Drop(f['All']['query'], [f['UPRP']['narrow'], f['Lens']['narrow']],
                           outpath='alien.base', skipable=False)

  f['Google']['narrow'] = Narrow(f['Base']['drop'], 
                                 f['Google']['index'], batch=2**10, 
                                 outpath=D["Google"]+'/narrow.pkl')

  f['All']['drop'] = Drop(f['All']['query'], [f[k]['narrow'] for k in D.keys()], 
                          outpath='alien', skipable=False)


  D['Google'] = 'patents.google.com'
  f['Google']['fetch'] = Fetch(f['All']['drop'], 'https://patents.google.com/patent',
                              outdir=D['Google']+'/web', )

  f['All']['bundle'] = Bundle(matches={ k: f[k]['narrow'] for k in D.keys() },
                              storages={ k: f[k]['profile'] for k in D.keys() },
                              geo={ k: f[k]['geoloc'] for k in D.keys() },
                              time={ k: f[k]['timeloc'] for k in D.keys() },
                              clsf={ k: f[k]['classify'] for k in D.keys() },
                              outpath='bundle.pkl')

  if sys.argv[1] == 'restart':

    for k in f.keys():
      print(k)
      for h in f[k].keys():
        print(k, h)
        if not isinstance(f[k][h], Step): continue
        f[k][h].restart()

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