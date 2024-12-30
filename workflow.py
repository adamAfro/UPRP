import sys, pandas, cudf, matplotlib.pyplot as pyplot,\
      yaml, re, os, asyncio, aiohttp

from lib.log import notify, log, progress
from lib.storage import Storage
from lib.query import Query
from lib.step import Ghost, Step
from lib.profile import Profiler
from lib.alias import simplify
from lib.index import Exact, Words, Digital, Ngrams, Date
from fake_useragent import UserAgent

class Profiling(Step):

  def __init__(self, dir:str, kind:str,
               assignpath:str, aliaspath:str, 
               profargs:dict={}, *args, **kwargs):

    assert kind.upper() in ['JSON', 'JSONL', 'XML', 'HTMLMICRODATA']

    super().__init__(*args, **kwargs)
    self.dir: str = dir
    self.kind: str = kind.upper()
    self.profargs: list[str] = profargs
    self.assignpath: str = assignpath
    self.aliaspath: str = aliaspath

  def run(self):

    P = Profiler( **self.profargs )

    if self.kind == 'XML':
      P.XML(self.dir)
    elif self.kind == 'HTMLMICRODATA':
      P.HTMLmicrodata(self.dir)
    elif self.kind == 'JSON':
      P.JSON(self.dir)
    elif self.kind == 'JSONL':
      P.JSONl(self.dir, listname="data")

    H = P.dataframes()

    L = simplify(H, norm=Profiling.pathnorm)
    H = { L['frames'][h0]: X.set_index(["id", "doc"])\
         .rename(columns=L['columns'][h0]) for h0, X in H.items() if not X.empty }

    L['columns'] = { L['frames'][h]: { v: k for k, v in Q.items() }  
                     for h, Q in L['columns'].items() }
    L['frames'] = { v: k for k, v in L['frames'].items() }
    with open(self.aliaspath, 'w') as f:
      yaml.dump(L, f, indent=2)#do wglądu

    A = { h: { k: None for k in V.keys() } for h, V in L['columns'].items() }
    with open(self.assignpath, 'w') as f:
      yaml.dump(A, f, indent=2)#do ręcznej edycji

    return Storage(self.dir, H)

  @staticmethod
  def pathnorm(x:str):
    x = re.sub(r'[^\w\.\-/\_]|\d', '', x)
    x = re.sub(r'\W+', '_', x)
    return x

class Indexing(Step):

  def __init__(self, storage:dict[str, pandas.DataFrame], 
               assignpath:str, *args, **kwargs):

    super().__init__(*args, **kwargs)
    self.storage: dict[str, pandas.DataFrame] = storage
    self.assignpath: str = assignpath

  def run(self) -> tuple[Digital, Ngrams, Exact, Words, Ngrams]:

    S = self.storage
    a = self.assignpath

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

class Parsing(Step):

  def __init__(self, searches:pandas.Series, *args, **kwargs):

    super().__init__(*args, **kwargs)

    self.searches: pandas.Series = searches

  def run(self):

    Q = self.searches
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

class Search(Step):

  Levels = cudf.CategoricalDtype([
    "weakest", "dated", "partial",
    "supported", "partial-supported",
    "exact", "dated-supported",
    "partial-dated", "partial-dated-supported",
    "exact-supported",
    "exact-dated", "exact-dated-supported"
  ], ordered=True)

  def __init__(self,
               queries:pandas.Series,
               indexes:tuple[Exact],
               batch:int,
               *args, **kwargs):

    super().__init__(*args, **kwargs)

    self.queries: pandas.Series = queries
    self.indexes: tuple[Exact] = indexes
    self.batch: int = batch

  def score(self, matches:cudf.DataFrame):

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

  def insight(self, matches:pandas.DataFrame):

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

    F.savefig(self.outpath+'.png', format='png')
    pyplot.close()

class Narrow(Search):

  def run(self):

    Q, _ = self.queries
    P0, P, D0, W0, W = self.indexes

    QP = Q.query('kind == "number"')

    mP0 = P0.match(QP['value'], 'max').reset_index()

    b = self.batch
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
      L = M.join(Ms).fillna(0.0).pipe(self.score)
    elif not M.empty:
      L = M.fillna(0.0).pipe(self.score)
    elif not Ms.empty:
      L = Ms.fillna(0.0).pipe(self.score)
    else:
      L = cudf.DataFrame()

    L.columns = cudf.MultiIndex.from_tuples([('', '', '', 'score'), ('', '', '', 'level')])

    Y = M[M.index.isin(L.index)].join(L)
    Y = Y[Y[('', '', '', 'level')] >= "partial-supported"]

    self.insight(Y.to_pandas())

    return Y

class Drop(Step):

  def __init__(self, queries:pandas.Series, matches:list[pandas.DataFrame], *args, **kwargs):

    super().__init__(*args, **kwargs)

    self.queries: pandas.Series = queries
    self.matches: pandas.DataFrame = matches

  def run(self):

    Q, P = self.queries
    M = self.matches
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
    Y = Y[Y['level'] >= "exact-dated"]

    p0 = P.index.astype(str).unique()
    p = p0[p0.isin(Y['entry'].values_host)]

    q0 = Q.index.astype(str).unique()
    q = q0[q0.isin(Y['entry'].values_host)]

    return Q[ ~ Q.index.isin(q)], P[ ~ P.index.isin(p) ]

class Preview(Ghost):

  def __init__(self, path:str,
               profile:dict[str, pandas.DataFrame],
               matches:pandas.DataFrame|None = None,
               queries:pandas.Series|None = None,
               *args, **kwargs):

    super().__init__(*args, **kwargs)

    self.path: str = path
    self.storage: dict[str, pandas.DataFrame] = profile
    self.matches: pandas.DataFrame|None = matches
    self.queries: pandas.Series|None = queries

  def run(self, n0=24, n=16):
    with pandas.option_context('display.max_columns', None,
                               'display.max_rows', n0,
                               'display.expand_frame_repr', False):

      H = self.storage
      Y = H.str()

      if self.matches is None:

        D = H.docs.sample(n).reset_index(drop=True).values
        Y += H.strdocs(D)

        with open(self.path, 'w') as f: f.write(Y)

        return

      M = self.matches.sample(n)
      Q, _ = self.queries

      M = M[M.index.get_level_values(1).isin(Q.index.values)]
      M = M.sample(min(M.shape[0], n))

      for i, m in M.iterrows():
        Y += Q.loc[ i[1] ] + '\n\n' + str(m.to_frame().T) + '\n\n' + H.strdocs([ i[0] ])

      with open(self.path, 'w') as f: f.write(Y)

class Fetch(Ghost):

  def __init__(self, queries:pandas.Series, URL:str, outdir:str, *args, **kwargs):

    super().__init__(*args, **kwargs)

    self.URL: str = URL
    self.outdir: str = outdir
    self.queries: pandas.Series = queries

  def run(self):

    async def scrap(P:list):

      t = 1
      H = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
      async with aiohttp.ClientSession(headers=H) as S:
        for i, p in P.iterrows():
          j, d = p['country'].upper(), ''.join(re.findall(r'\d+', p['number']))
          o = f"{self.outdir}/{j}{d}.html"
          if os.path.exists(o): continue
          x = f"{self.URL}/{j}{d}"
          async with S.get(x) as y0:
            y = await y0.text()
            await asyncio.sleep(t)
            with open(o, "w") as f: f.write(y)

    _, P = self.queries
    asyncio.run(scrap(P))

try:

  Q = pandas.read_csv('raport.uprp.gov.pl.csv').set_index('entry')['query']
  Q = Q.drop_duplicates()
  Q.index = Q.index.astype('str')

  D = { 'UPRP': 'api.uprp.gov.pl',
        'Lens': 'api.lens.org',
        # 'Open': 'api.openalex.org',
        'USPG': 'developer.uspto.gov/grant',
        'USPA': 'developer.uspto.gov/application' }

  f = { k: dict() for k in D.keys() }

  f['All'] = dict()
  f['All']['query'] = Parsing(Q, outpath='queries.pkl')

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

  for k, p in D.items():

    f[k]['index'] = Indexing(f[k]['profile'], assignpath=p+'/assignement.yaml',
                             outpath=p+'/indexes.pkl', skipable=True)

    f[k]['narrow'] = Narrow(f['All']['query'], f[k]['index'],
                            batch=2**14, outpath=p+'/narrow.pkl')

    f[k]['drop'] = Drop(f['All']['query'], f[k]['narrow'],
                        outpath=p+'/alien', skipable=False)

    f[k]['preview0'] = Preview(f"{p}/profile.txt", f[k]['profile'])
    f[k]['preview'] = Preview(f"{p}/profile.txt", f[k]['profile'], 
                              f[k]['narrow'], f['All']['query'])

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

  f['All']['drop'] = Drop(f['All']['query'], [f[k]['narrow'] for k in D.keys()], 
                          outpath='alien', skipable=False)


  D['Google'] = 'patents.google.com'
  f['Google'] = dict()
  f['Google']['fetch'] = Fetch(f['All']['drop'], 'https://patents.google.com/patent',
                              outdir=D['Google']+'/web', )

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