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

    Y = Searcher().add(progress(M, desc=f'📑 {self.dir}'))

    return Y

class Searching(Step):

  def __init__(self, queries:pandas.Series, searcher:Searcher, *args, **kwargs):

    super().__init__(*args, **kwargs)

    self.queries: pandas.Series = queries
    self.searcher: Searcher = searcher
    self.lazy.update(['queries', 'searcher'])

  def run(self, batch=128):

    Q = self.queries
    S = self.searcher
    Q = [(i, q) for i, q in Q.items()]
    b = batch
    n = len(Q)//b
    B = enumerate(range(0, len(Q), b))
    Y0 = None

    for i0, i in progress(B, desc=f'🔍', total=n+1):

      Y = S.search(Q[i:i+b], limit=5)
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
    self.lazy.update(['queries', 'matches'])

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
    self.lazy.update(['matches'])

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

    f[k]['match'] = Searching(Q, f[k]['index'], outpath=p+'/matches.pkl')

    f[k]['insight'] = Insight(f[k]['match'], p+'/insight.png')

    f[k]['preview0'] = Preview(f"{p}/profile.txt", f[k]['profile'])
    f[k]['preview'] = Preview(f"{p}/profile.txt", f[k]['profile'], f[k]['match'], Q)

    f[k]['drop'] = Patmatchdrop(Q, f[k]['match'], outpath=p+'/alien.s.csv', skipable=False)

  f['Lens']['match'] = Searching(f['UPRP']['drop'], f['Lens']['index'],
                                 outpath=D["Lens"]+'/matches.pkl')

  if len(sys.argv) == 2:
    a, b, *C = sys.argv[1].split(" ")
  else:
    a, b = sys.argv[1], sys.argv[2]
    C = sys.argv[3:] if len(sys.argv) > 3 else []

  y = f[a][b]
  log('🚀', os.getpid())
  notify(' '.join(sys.argv))
  y.endpoint(*C)

  notify("✅")

except Exception as e:

  notify("❌")

  raise e.with_traceback(e.__traceback__)

finally:

  log('🏁', os.getpid())