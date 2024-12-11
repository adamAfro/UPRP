from .index import Base as Exact, Digital, Words, Ngrams
import pickle, yaml, re
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

    self.unique = concat([X.index.to_series()
      for X in self.data.values()]).drop_duplicates()\
      .reset_index(drop=True)

  def get(self, docs:list):

    Y = dict()
    for h, X in self.data.items():
      try: Y[h] = X.loc[docs, :]
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
      X.set_index('doc', inplace=True)

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
    if not H0: return DataFrame(columns=['repo', 'frame', 'col', 'assignement', 'doc', 'value'])

    H = concat(H0)
    H['assignement'] = a

    if a == 'date':
      H['value'] = to_datetime(H['value'], errors='coerce')

    return H

  @staticmethod
  def _melt(frame:DataFrame, repo:str, name:str):

    "funk. wewn. do tworzenia ramki danych w formacie długim"

    X = frame

    Y = X.reset_index(drop=False)\
    .melt(id_vars='doc', var_name='col')\
    .assign(repo=repo, frame=name)\
    .dropna(subset=['value'])

    return Y[['repo', 'frame', 'col', 'doc', 'value']]

  def _assigned(self, target:str):

    "zwraca przyporządkowane kolumny dla danego celu"

    return ((h, k)
      for h, X in self.assignment.items()
      for k, v in X.items()
      if v == target)

class Searcher:

  "klasa do wyszukiwania danych w repozytorium"

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
      'dates': Exact('value'),
      'numbers': Digital('value'),
      'numprefix': Ngrams('value', n=3, suffix=False),
      'words':  Words('value', case='upper'),
      'ngrams': Ngrams('value', n=3),
    }

  def dump(self):
    return { h: V.dump() for h, V in self.indexes.items() }

  def load(self, dump:dict):
    for h, V in self.indexes.items(): V.load(*dump[h])

  Levels = CategoricalDtype(reversed([
    "exact-dated-supported",
    "exact-dated",
    "exact-supported",
    "partial-dated-supported",
    "partial-dated",
    "dated-supported",
    "exact",
    "partial-supported",
    "supported",
    "partial",
    "dated",
    ""
  ]), ordered=True)

  @staticmethod
  def levelcal(assignements:Series):

    Q = assignements

    N = ('exact' if Q['number'] >= 1 else 'partial' if Q['number'] > 0 else None)
    D = 'dated' if Q['date'] >= 1 else None
    X = 'supported' if Q['title'] + Q['name'] + Q['city'] >= 1 else None

    return '-'.join((x for x in [N, D, X] if x is not None))

  @staticmethod
  def levelscore(results: DataFrame, limit=100):

    X: DataFrame = results.copy()

    A = X.groupby(['doc', 'assignement'])
    A = A.agg({'count': 'max'}).unstack(fill_value=0)
    A.columns = A.columns.droplevel(0)
    A = A.reindex(columns=['number', 'date', 'city', 'name', 'title'], fill_value=0)
    L = A.apply(Searcher.levelcal, axis=1).astype(Searcher.Levels)
    L.name = 'level'

    for d, w in [('date', 10), ('number', 10), ('city', 2), ('name', 2), ('title', 1)]:
      I = (X['assignement'] == d)
      X.loc[I, 'count'] = X.loc[I, 'count'] * w

    Y = X.groupby('doc').agg({'count': 'sum'})['count'].to_frame()
    Y = Y.join(L).sort_values(['count', 'level'])
    Y['level'] = Y['level'].astype(Searcher.Levels)

    n = min(limit, Y.shape[0])

    return Y.tail(n)

  def add(self, idxmelted:list[tuple[str, DataFrame]]):

    for h, X in idxmelted:
      if h == 'date':
        X['value'] = to_datetime(X['value'], errors='coerce').dt.date
        self.indexes['dates'].add(X, reindex=False)
      elif h == 'number':
        X = self.indexes['numbers'].add(X, reindex=False)
        X = self.indexes['numprefix'].add(X, reindex=False)
      elif h in ['title', 'name', 'city']:
        X = self.indexes['words'].add(X, reindex=False)
        X = self.indexes['ngrams'].add(X, reindex=False)

    self.indexes['dates'].reindex()
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
          self.indexes['numbers'].match(P, aggregation='max'),
          self.indexes['numprefix'].match(P, aggregation='max'),
          self.indexes['words'].match(W),
          self.indexes['ngrams'].match(W),
                                          ]
    M = [U for U in M if not U.empty]
    for U in M: U.name = 'count'
    M = [U.reset_index() for U in M]
    if not M: return DataFrame()
    Y0 = concat(M)
    s: Series = Searcher.levelscore(Y0)
    Y = Y0[Y0['doc'].isin(s.index) ]\
    .pivot_table(index=['doc'], columns=['repo', 'frame', 'col', 'assignement'], 
                 aggfunc='sum', values='count', fill_value=0)
    s.columns = MultiIndex.from_tuples([('', '', '', 'score'), ('', '', '', 'level')])

    return Y.join(s)