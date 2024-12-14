from .index import Base as Exact, Digital, Words, Ngrams
import pickle, yaml, re, warnings
from datetime import date
from pandas import DataFrame, MultiIndex, Series, concat, to_datetime, CategoricalDtype
from lib.expr import Marker
from lib.datestr import num as datenum, month
from lib.datestr import MREGEX

class Loader:

  "klasa do wczytywania danych z repozytorium"

  def __init__(self, name:str,
               data:dict[str, DataFrame],
               assignment:dict[str, dict[str, dict[str, str]]]):

    self.name = name
    self.data = data
    self.assignment = assignment

    self.docs = concat([X.index.get_level_values('doc').to_series()
      for X in self.data.values()]).drop_duplicates()\
      .reset_index(drop=True)

  def getdocs(self, docs:list):

    Y = dict()
    for h, X in self.data.items():
      try: Y[h] = X.loc[ X.index.get_level_values('doc').isin(docs) ]
      except KeyError: continue
    return Y

  def Within(path:str, name:str|None=None):

    "wczytuje dane z katalogu (pliki `data.pkl` i `assignement.yaml`)"

    f0 = path
    k = name if name is not None else f0.split('/')[-1]

    A: dict[str, dict[str, str]] = dict()
    with open(f'{f0}/assignement.yaml', 'r') as f:
      A = yaml.load(f, Loader=yaml.FullLoader)

    H: dict[str, DataFrame] = dict()
    with open(f'{f0}/data.pkl', 'rb') as f:
      H = pickle.load(f)

    for h, X in H.items():
      X.set_index(['doc', X.index], inplace=True)

    return Loader(k, H, A)

  def melt(self, name:str):

    """
    dane w formacie długim dla przyporządkowanych kolumn, 
    kolumny: `'repo', 'frame', 'col', 'assignement', 'doc', 'value'`

    Uwaga
    -----

    Rozw. tymczasowe (patrz: `search`) zakłada unikalność 
    dokumentów kolumny `doc` po między repozytoriami,

    - przykładowy konfilkt: 2 repozytoria mają identyfikatory iterowane 
    od `0:n1` oraz od `0:n2`, wtedy znajdy `0:min(n1,n2)` są dwuznaczne.
    """

    a = name
    H0 = [self.data[h][k].to_frame().pipe(Loader._melt, self.name, h) for h, k in self._assigned(a)]
    if not H0: return DataFrame(columns=['repo', 'frame', 'col', 'assignement', 'doc', 'id', 'value'])

    H = concat(H0)
    H['assignement'] = a

    if a == 'date':
      H['value'] = to_datetime(H['value'], errors='coerce').dt.date

    return H

  @staticmethod
  def _melt(frame:DataFrame, repo:str, name:str):

    "funk. wewn. do tworzenia ramki danych w formacie długim"

    X = frame

    Y = X.reset_index(drop=False)\
    .melt(id_vars=['doc', 'id'], var_name='col')\
    .assign(repo=repo, frame=name)\
    .dropna(subset=['value'])

    return Y[['repo', 'frame', 'col', 'doc', 'id', 'value']]

  def _assigned(self, target:str):

    "zwraca przyporządkowane kolumny dla danego celu"

    return ((h, k)
      for h, X in self.assignment.items()
      for k, v in X.items()
      if v == target)

class Searcher:

  "klasa do wyszukiwania danych w repozytorium"

  BUG = []

  URLalike=r'(?:http[s]?://(?:\w|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+)'
  codealike, marktarget = ['alnum', 'num', 'series'], {
    "month":  MREGEX,
    "alnum":  r"(?<!\w)(?:[^\W\d]+\d|\d+[^\W\d])\w*(?!\w)",
    "series": r"(?:" + r'|'.join([rf"(?:\d+\s*{s}+\s*)+" for s in [r'\.', r'\-', r'/', r'\\', r'\s']]) + r")\s*\d+",
    "num":    r"(?<!\w)\d+(?!\w)", "space":  r"[\s\-\/\\\.]+",
    "braced": '|'.join([rf"\{a}\w{{1,4}}\{b}" for a,b in ['()','{}','[]', '""', '<>']]),
    "abbr":   r"(?<!\w)[^\W\d]{1,4}\.?(?!\w)",
  }

  patentalike = r'(?P<country>(?<![^\W])[a-zA-Z]{2}(?![^\W\d]))' + \
                r'(?P<prefix>(?:[^\W\d]|[\.\s]){,5})?' + \
                r'(?P<number>(?:\d\W?\s?){5,})(?!\d)' + \
                r'(?P<suffix>[\W\s]{,3}[^\w\s]*[0123abuABUXY][^\w\s\)\}\]]*[0123a-zA-Z]?[^\w\s]*)?'

  codemarker = Marker(marktarget, codealike)

  def __init__(self):

    self.indexes = {
      'dates': Exact(),
      'years': Exact(factor=0.5),
      'numbers': Digital(),
      'numprefix': Ngrams(n=3, suffix=False),
      'words':  Words(case='upper', factor=0.4),
      'ngrams': Ngrams(n=3, suffix=False, factor=0.3),
    }

    for b in Searcher.BUG: warnings.warn(b)

  def dump(self):
    return { h: V.dump() for h, V in self.indexes.items() }

  def load(self, dump:dict):
    for h, V in self.indexes.items(): V.load(*dump[h])

  Levels = CategoricalDtype(reversed([
    "exact-dated-supported",
    "exact-yearly-supported",
    "exact-dated",
    "exact-yearly",
    "exact-supported",
    "partial-dated-supported",
    "partial-yearly-supported",
    "partial-dated",
    "dated-supported",
    "exact",
    "partial-yearly",
    "partial-supported",
    "yearly-supported",
    "supported",
    "partial",
    "dated",
    "yearly",
    ""
  ]), ordered=True)

  @staticmethod
  def levelcal(assignements:Series):

    Q = assignements

    N = ('exact' if Q['number'] >= 1 else 'partial' if Q['number'] > 0 else None)
    D = 'dated' if Q['date'] >= 1 else 'yearly' if Q['date'] > 0 else None
    X = 'supported' if Q['title'] + Q['name'] + Q['city'] >= 1 else None

    return '-'.join((x for x in [N, D, X] if x is not None))

  @staticmethod
  def levelscore(results: DataFrame, limit=100,
                 stacked=['date', 'city', 'name', 'title']):

    S0 = stacked
    X: DataFrame = results.copy().T

    s = X.index.get_level_values('assignement').isin(S0)
    S = X.loc[ s ].groupby('assignement').agg('sum').T
    U = X.loc[~s ].groupby('assignement').agg('max').T

    A = concat([S, U], axis=1).fillna(0)\
    .reindex(columns=['number', 'date', 'city', 'name', 'title'], fill_value=0)\

    L = A.apply(Searcher.levelcal, axis=1).astype(Searcher.Levels)
    Y = DataFrame({ 'score': A.sum(axis=1),
                    'level': L.astype(Searcher.Levels) }, index=A.index)

    Y = Y.sort_values(['score', 'level'])
    n = min(limit, Y.shape[0])

    return Y.tail(n)

  @staticmethod
  def basescore(count: DataFrame, stacked=['date', 'city', 'name', 'title']):

    X = count.copy()

    s = X['assignement'].isin(stacked)
    S = X.loc[ s ].pivot_table(index='doc',
      columns=['repo', 'frame', 'col', 'assignement'],
      values='score', aggfunc='sum')

    U = X.loc[~s ].pivot_table(index='doc',
      columns=['repo', 'frame', 'col', 'assignement'],
      values='score', aggfunc='max')

    Y = S.combine_first(U)

    return Y.fillna(0)

  def add(self, idxmelted:list[tuple[str, DataFrame]]):

    for h, X in idxmelted:

      if h == 'date':

        D = to_datetime(X['value'], errors='coerce')

        X.loc[:, 'value'] = D.dt.date
        self.indexes['dates'].add(X.copy(), reindex=False)

        X.loc[:, 'value'] = D.dt.year
        self.indexes['years'].add(X.copy(), reindex=False)

      elif h == 'number':

        X = self.indexes['numbers'].add(X.copy(), cumulative=False, reindex=False)

        self.indexes['numprefix'].add(X.copy(), cumulative=False, reindex=False)

      elif h in ['title', 'name', 'city']:

        X = self.indexes['words'].add(X.copy(), reindex=False)

        self.indexes['ngrams'].add(X.copy(), reindex=False)

    self.indexes['dates'].reindex()
    self.indexes['years'].reindex()
    self.indexes['numbers'].reindex()
    self.indexes['numprefix'].reindex()
    self.indexes['words'].reindex()
    self.indexes['ngrams'].reindex()

  def multisearch(self, idxqueries:list[tuple]):
    Q = idxqueries
    Y0 = []
    for i, q in Q:
      y = self.search(q)
      y['entry'] = i
      y = y.reset_index()\
      .rename(columns={ 'index': 'doc' })\
      .set_index(['entry', 'doc'])
      Y0.append(y)
    Y = concat(Y0)
    u = Y.select_dtypes(include='number').columns
    Y[u] = Y[u].fillna(0)
    Y.columns = Y.columns.set_names(['repo', 'frame', 'col', 'assignement'])
    return Y

  def search(self, query:str):

    q = query
    q = re.sub(Searcher.URLalike, '', q)

    W = [w for w in re.sub(r"[\W\d\s]+", " ", q).strip().upper().split(' ') if len(w) >= 3]

    X = [(x) for x, _, _, m in self.codemarker.union(q) if m == True]
    P = [m.groupdict() for v in X for m in re.finditer(Searcher.patentalike, v)]
    P = [re.sub(r'\D', '', d['number']) for d in P]
    D0 = [(y, m, d) for x in X for _, _, x, d, m, y in datenum(x)] + \
         [(y, m, d) for x in X for _, _, x, d, m, y in month(x)]
    D = [date(y, m, d) for y, m, d in D0 if y is not None and
                                            m is not None and
                                            d is not None]

    M = [
          self.indexes['dates'].match(D),
          self.indexes['years'].match(list(set([d.year for d in D]))),
          self.indexes['numbers'].match(P, aggregation='max'),
          self.indexes['numprefix'].match(P, aggregation='max'),
          self.indexes['words'].match(W),
          self.indexes['ngrams'].match(W),
                                          ]
    M = [U for U in M if not U.empty]
    for U in M: U.name = 'score'
    M = [U.reset_index() for U in M]
    if not M: return DataFrame()

    Y0 = concat(M).pipe(Searcher.basescore)
    L = Y0.pipe(Searcher.levelscore)
    Y = Y0.query('doc in @L.index')

    L.columns = MultiIndex.from_tuples([('', '', '', 'score'), ('', '', '', 'level')])

    return Y.join(L)