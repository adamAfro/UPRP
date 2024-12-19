import sys, pandas, matplotlib.pyplot as pyplot, yaml, re, os
from lib.log import notify, log, progress
from lib.repo import Storage, Searcher
from lib.step import Ghost, Step
from lib.profile import Profiler
from lib.alias import simplify

class Profiling(Step):

  def __init__(self, dir:str, kind:str,
               assignpath:str, aliaspath:str, 
               profargs:dict={}, *args, **kwargs):

    assert kind.upper() in ['JSON', 'JSONL', 'XML']

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

  def run(self):

    S = self.storage
    a = self.assignpath

    with open(a, 'r') as f:
      S.assignement = yaml.load(f, Loader=yaml.FullLoader)

    K = ['date', 'number', 'name', 'city', 'title']
    M = [(k, S.melt(k)) for k in K]
    M = [(k, S) for k, S in M if not S.empty]

    Y = Searcher().add(progress(M, desc=f'📑'))

    return Y

class Searching(Step):

  def __init__(self, queries:pandas.Series, searcher:Searcher, batch=128,
               search=dict(), *args, **kwargs):

    super().__init__(*args, **kwargs)

    self.queries: pandas.Series = queries
    self.searcher: Searcher = searcher

    self.batch: int = batch

    self.search = dict()
    self.search['limit'] = search.get('limit', 3)
    self.search['narrow'] = search.get('narrow', True)

  def run(self):

    Q = self.queries
    S = self.searcher
    Q = [(i, q) for i, q in Q.items()]
    b = int(self.batch)
    n = len(Q)//b
    B = enumerate(range(0, len(Q), b))
    Y0 = None

    for i0, i in progress(B, desc=f'🔍', total=n+1):

      Y = S.search(Q[i:i+b], **self.search)
      if Y.empty: continue
      Y0 = pandas.concat([Y0, Y]) if Y0 is not None else Y
      self.dumpprog(Y0, 100*(i0+1)//n)

    assert Y0 is not None
    return Y0

class Patmatchdrop(Step):

  def __init__(self, queries:pandas.Series, matches:pandas.DataFrame, *args, **kwargs):

    super().__init__(*args, **kwargs)

    self.queries: pandas.Series = queries
    self.matches: pandas.DataFrame = matches

  def run(self):

    Q = self.queries
    M = self.matches
    K = [('entry', '', '', ''), ('doc', '', '', ''),
         ('', '', '', 'level'), ('', '', '', 'score')]

    Y = M.reset_index()[K]
    Y.columns = ['entry', 'doc', 'level', 'score']
    Y = Y.sort_values(by=['level', 'score'], ascending=False)\
    .drop_duplicates(subset='entry')

    assert Y['level'].dtype == 'category'
    Y = Y[Y['level'] >= "exact-dated"]
    I = set(Q.index.values) & set(Y['entry'].values)

    return Q.drop(I)

class Insight(Ghost):

  def __init__(self, matches:pandas.DataFrame, figpath:str, *args, **kwargs):

    super().__init__(*args, **kwargs)

    self.figpath: str = figpath
    self.matches: pandas.DataFrame = matches

  def run(self):

    M = self.matches
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
    .plot.barh(title='Rozkład poziomów dopasowania', ylabel='', xlabel='',
              color=[k for k in reversed('gggyyyyyyrrrrrrrrb')], ax=A[1]);

    F.savefig(self.figpath, format='png')

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
      Q = self.queries

      M = M[M.index.get_level_values(1).isin(Q.index.values)]
      M = M.sample(min(M.shape[0], n))

      for i, m in M.iterrows():
        Y += Q.loc[ i[1] ] + '\n\n' + str(m.to_frame().T) + '\n\n' + H.strdocs([ i[0] ])

      with open(self.path, 'w') as f: f.write(Y)

try:

  Q = pandas.read_csv('raport.uprp.gov.pl.csv').set_index('entry')['query']
  Q = Q.drop_duplicates()
  Q.index = Q.index.astype('str')

  D = { 'UPRP': 'api.uprp.gov.pl',
        'Lens': 'api.lens.org',
        'Open': 'api.openalex.org',
        'USPG': 'developer.uspto.gov/grant',
        'USPA': 'developer.uspto.gov/application' }

  f = { k: dict() for k in D.keys() }

  f['UPRP']['profile'] = Profiling(D['UPRP']+'/raw/', kind='XML',
                                   assignpath=D['UPRP']+'/assignement.null.yaml', 
                                   aliaspath=D['UPRP']+'/alias.yaml',
                                   outpath=D['UPRP']+'/storage.pkl')

  f['Lens']['profile'] = Profiling(D['Lens']+'/res/', kind='JSONL',
                                   assignpath=D['Lens']+'/assignement.null.yaml', 
                                   aliaspath=D['Lens']+'/alias.yaml',
                                   outpath=D['Lens']+'/storage.pkl')

  f['Open']['profile'] = Profiling(D['Open']+'/raw/', kind='JSON',
                                   profargs=dict(excluded=["abstract_inverted_index", "updated_date", "created_date"]),
                                   assignpath=D['Open']+'/assignement.null.yaml', 
                                   aliaspath=D['Open']+'/alias.yaml',
                                   outpath=D['Open']+'/storage.pkl')

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
                             outpath=p+'/searcher.pkl')

    f[k]['narrow'] = Searching(Q, f[k]['index'], search=dict(narrow=True),
                               outpath=p+'/matches.narrow.pkl')

    f[k]['ndrop'] = Patmatchdrop(Q, f[k]['narrow'], 
                                 outpath=p+'/alien.s.csv', skipable=False)

    f[k]['insight'] = Insight(f[k]['narrow'], p+'/insight.png')

    f[k]['preview0'] = Preview(f"{p}/profile.txt", f[k]['profile'])
    f[k]['preview'] = Preview(f"{p}/profile.txt", f[k]['profile'], f[k]['narrow'], Q)

  f['UPRP']['narrow'] = Searching(Q, f['UPRP']['index'], 1024, search=dict(narrow=True),
                                  outpath=D['UPRP']+'/matches.narrow.pkl')

  # f['Lens']['match'] = Searching(f['UPRP']['drop'], f['Lens']['index'],
  #                                outpath=D["Lens"]+'/matches.pkl')

  E = []
  for a in sys.argv[1:]:
    try:

      k, h = a.split('.')
      f0 = f[k][h]

      log('🚀', os.getpid(), ' '.join(sys.argv))

      notify(a)

      f0.endpoint()

      notify("✅")

    except Exception as e:

      E.append(e)

      notify("❌")

  if E: raise ExceptionGroup("❌", E)

except Exception as e:

  raise e.with_traceback(e.__traceback__)