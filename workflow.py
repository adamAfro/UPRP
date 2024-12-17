import sys, pandas, matplotlib.pyplot as pyplot
from lib.log import notify, log, progress
from lib.repo import Storage, Searcher
from lib.step import Ghost, Step

class Indexing(Step):

  def __init__(self, dir:str, *args, **kwargs):

    super().__init__(*args, **kwargs)
    self.dir: str = dir

  def run(self):

    Y = Searcher()
    X = Storage.Within(self.dir)
    K = ['date', 'number', 'name', 'city', 'title']
    M = [(k, X.melt(k)) for k in K]
    M = [(k, X) for k, X in M if not X.empty]
    Y.add(progress(M, desc=f'📑 {self.dir}'))

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
  Q.index = Q.index.astype('str')

  iUPRP = Indexing('api.uprp.gov.pl', outpath='api.uprp.gov.pl/searcher.pkl')
  qUPRP = Searching(Q, iUPRP, outpath='api.uprp.gov.pl/matches.pkl')
  dUPRP = Patmatchdrop(Q, qUPRP, outpath='api.uprp.gov.pl/alien.csv')
  oUPRP = Insight(qUPRP, figpath='api.uprp.gov.pl/insight.png')

  iLens = Indexing('api.lens.org', outpath='api.lens.org/searcher.pkl')
  qLens = Searching(dUPRP, iLens, outpath='api.lens.org/matches.pkl')
  dLens = Patmatchdrop(dUPRP, qLens, outpath='api.lens.org/alien.csv')
  oLens = Insight(qLens, figpath='api.lens.org/insight.png')

  iOpen = Indexing('api.openalex.org', outpath='api.openalex.org/searcher.pkl')
  qOpen = Searching(dLens, iOpen, outpath='api.openalex.org/matches.pkl')
  dOpen = Patmatchdrop(dLens, qOpen, outpath='api.openalex.org/alien.csv')
  oOpen = Insight(qOpen, figpath='api.openalex.org/insight.png')

  steps = {
    'iUPRP': iUPRP,
    'iLens': iLens,
    'iOpen': iOpen,
    'qUPRP': qUPRP,
    'qLens': qLens,
    'qOpen': qOpen,
    'oUPRP': oUPRP,
    'oLens': oLens,
    'oOpen': oOpen,
  }

  for a in sys.argv[1:]:
    steps[a].output()

except Exception as e:
  log('❌', e)
  notify("❌")
  raise e.with_traceback(e.__traceback__)