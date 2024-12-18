from .index import Base as Exact, Digital, Words, Ngrams
import pickle, yaml, re, warnings
from datetime import datetime
from pandas import DataFrame, MultiIndex, Series, concat, to_datetime, CategoricalDtype
from lib.expr import Marker
from lib.datestr import num as datenum, month
from lib.datestr import MREGEX
import cudf

class Storage:

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

    return Storage(k, H, A)

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
    H0 = [self.data[h][k].to_frame().pipe(Storage._melt, self.name, h) for h, k in self._assigned(a)]
    if not H0: return DataFrame(columns=['repo', 'frame', 'col', 'assignement', 'doc', 'id', 'value'])

    H = concat(H0)
    H['assignement'] = a

    return H

  @staticmethod
  def _melt(frame:DataFrame, repo:str, name:str):

    "funk. wewn. do tworzenia ramki danych w formacie długim"

    X = frame
    K = ['repo', 'frame', 'col', 'doc', 'id', 'value']
    X.columns = [k if k not in K else k+'*' for k in X.columns]

    Y = X.reset_index(drop=False)\
    .melt(id_vars=['doc', 'id'], var_name='col')\
    .assign(repo=repo, frame=name)\
    .dropna(subset=['value'])

    return Y[K]

  def _assigned(self, target:str):

    "zwraca przyporządkowane kolumny dla danego celu"

    return ((h, k)
      for h, X in self.assignment.items()
      for k, v in X.items()
      if v == target)

class Query:

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

  def __init__(self, words:list[str], codes:list[str], dates:list[int], years:list[int]):

    self.words = words
    self.codes = codes
    self.dates = dates
    self.years = years

  def Parse(query:str):

    q = re.sub(Query.URLalike, '', query)

    W = [w for w in re.sub(r"[\W\d\s]+", " ", q).strip().upper().split(' ') if len(w) >= 3]

    X = [(x) for x, _, _, m in Query.codemarker.union(q) if m == True]
    P = [m.groupdict() for v in X for m in re.finditer(Query.patentalike, v)]
    P = [re.sub(r'\D', '', d['number']) for d in P]
    D0 = [(y, m, d) for x in X for _, _, x, d, m, y in datenum(x)] + \
         [(y, m, d) for x in X for _, _, x, d, m, y in month(x)]
    D = [datetime(y, m, d).strftime('%Y-%m-%d')
         for y, m, d in D0 if y is not None and m is not None and d is not None]

    return Query(W, P, D, [y for y in set([y for y,m,d in D0])])

  def wordmelt(self, entry) -> list[dict]:
    return [{ 'entry': str(entry), 'value': v } for v in self.words]

  def nummelt(self, entry) -> list[dict]:
    return [{ 'entry': str(entry), 'value': v } for v in self.codes]

  def datemelt(self, entry) -> list[dict]:
    return [{ 'entry': str(entry), 'value': v } for v in self.dates]

  def yearmelt(self, entry) -> list[dict]:
    return [{ 'entry': str(entry), 'value': v } for v in self.years]

class Searcher:

  def __init__(self, indexes: dict[str, Exact] = {
    'dates': Exact(),
    'years': Exact(factor=0.5),
    'numbers': Digital(),
    'numprefix': Ngrams(n=3, suffix=False),
    'words':  Words(case='upper', factor=1),
    'ngrams': Ngrams(n=3, suffix=False, factor=1)
  }): self.indexes = indexes

  def dump(self):
    return { h: V.dump() for h, V in self.indexes.items() }

  def load(self, dump:dict):
    for h, V in self.indexes.items(): V.load(*dump[h])

  Levels = CategoricalDtype([l for l in reversed([
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
  ])], ordered=True)

  @staticmethod
  def levelcal(assignements:Series):

    A = assignements

    Q = cudf.DataFrame(index=A.index)
    Q['exact'] = A['number'] >= 1
    Q['partial'] = A['number'] > 0
    Q['dated'] = A['date'] >= 1
    Q['yearly'] = A['date'] > 0
    Q['supported'] = A['title'] + A['name'] + A['city'] >= 1

    Q['level'] = Searcher.Levels.categories[0]
    for c in Searcher.Levels.categories[1:]:
      q = Q[c.split('-')].all(axis=1)
      R = Q['level'].where(~ q, c)
      Q['level'] = R

    return Q['level']

  @staticmethod
  def levelscore(base: DataFrame, limit:int, 
                 aggdict={ 'number': 'max',
                           'date': 'sum',
                           'city': 'sum',
                           'name': 'sum',
                           'title': 'sum' }):
    A = aggdict
    X: DataFrame = base
    S0 = cudf.DataFrame(index=X.index)

    for k0, a in A.items():
      G = X.loc[:, [k for k in X.columns if k[3] == k0]]
      V = G.sum(axis=1) if a == 'sum' else G.max(axis=1)
      S0[k0] = V

    S = S0.reindex(columns=A.keys(), fill_value=0)

    Z = cudf.DataFrame({ 'score': S.sum(axis=1),
                         'level': Searcher.levelcal(S).astype(Searcher.Levels) }, 
                          index=S.index)

    Y = Z.reset_index().sort_values(['level', 'score'])
    Y = Y.groupby('entry').tail(limit).set_index(['doc', 'entry'])

    return Y

  @staticmethod
  def basescore(count: DataFrame, stacked=['date', 'city', 'name', 'title']):

    X = count.copy()

    s = X['assignement'].isin(stacked)
    S = X.loc[ s ].pivot_table(index=['doc', 'entry'],
      columns=['repo', 'frame', 'col', 'assignement'],
      values='score', aggfunc='sum')

    if s.all():
      S[('', '', '', 'number')] = 0
      return S

    U = X.loc[~s ].pivot_table(index=['doc', 'entry'],
      columns=['repo', 'frame', 'col', 'assignement'],
      values='score', aggfunc='max')

    Y = S.join(U).fillna(0.0)

    return Y

  def add(self, idxmelted:list[tuple[str, DataFrame]]):

    for h, X in idxmelted:

      if h == 'date':

        D = to_datetime(X['value'], errors='coerce')

        X['value'] = D.dt.strftime('%Y-%m-%d')
        self.indexes['dates'].add(X, reindex=False)

        X['value'] = D.dt.year
        X['value'] = X['value'].astype('int')
        self.indexes['years'].add(X, reindex=False)

      elif h == 'number':

        X = self.indexes['numbers'].add(X, cumulative=False, reindex=False)

        self.indexes['numprefix'].add(X, cumulative=False, reindex=False)

      elif h in ['title', 'name', 'city']:

        X = self.indexes['words'].add(X, reindex=False)

        self.indexes['ngrams'].add(X, reindex=False)

    for k in ['dates', 'years', 'numbers', 'numprefix', 'words', 'ngrams']:
      self.indexes[k].reindex()

    return self

  def search(self, queries:list[tuple], limit:int):

    Q = [(i, Query.Parse(q)) for i, q in queries]

    try: W = DataFrame([y for i, q in Q if q.words for y in q.wordmelt(i)]).set_index('entry')['value']
    except: W = DataFrame()
    try: N = DataFrame([y for i, q in Q if q.codes for y in q.nummelt(i)]).set_index('entry')['value']
    except: N = DataFrame()
    try: D = DataFrame([y for i, q in Q if q.dates for y in q.datemelt(i)]).set_index('entry')['value']
    except: D = DataFrame()
    try: U = DataFrame([y for i, q in Q if q.years for y in q.yearmelt(i)]).set_index('entry')['value']
    except: U = DataFrame()

    X = [(D, 'dates', 'sum'),
         (U, 'years', 'sum'),
         (N, 'numbers', 'max'),
         (N, 'numprefix', 'max'),
         (W, 'words', 'sum'),
         (W, 'ngrams', 'sum')]

    M = [self.indexes[k].match(q, aggregation=A)
         for q, k, A in X if not q.empty]

    M = [U for U in M if not U.empty]
    for U in M: U.name = 'score'

    M = [U.reset_index() for U in M]
    if not M: return cudf.DataFrame()

    S = cudf.concat(M).pipe(Searcher.basescore)
    S.columns = S.columns.set_names(['repo', 'frame', 'col', 'assignement'])
    L = S.pipe(Searcher.levelscore, limit=limit)
    L.columns = MultiIndex.from_tuples([('', '', '', 'score'),
                                        ('', '', '', 'level')])

    Y = S[S.index.isin(L.index)].join(L)
    Y.columns = Y.columns.set_names(['repo', 'frame', 'col', 'assignement'])

    u = Y.select_dtypes(include='number').columns
    Y[u] = Y[u].fillna(0)

    return Y.round(3).to_pandas()