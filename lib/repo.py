import pickle, yaml, re, os, time
from multiprocessing import Pool
from pandas import DataFrame, Index, Series, concat, to_datetime
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
    kolumny: `'repo', 'frame', 'col', 'doc', 'value'`

    Uwaga
    -----

    Rozw. tymczasowe (patrz: `search`) zakłada unikalność 
    dokumentów kolumny `doc` po między repozytoriami,

    - przykładowy konfilkt: 2 repozytoria mają identyfikatory iterowane 
    od `0:n1` oraz od `0:n2`, wtedy znajdy `0:min(n1,n2)` są dwuznaczne.
    """

    a = name
    H0 = [self.data[h][k].to_frame().pipe(Loader._melt, self.name, h) for h, k in self._assigned(a)]
    if not H0: return DataFrame(columns=['repo', 'frame', 'col', 'doc', 'value'])

    H = concat(H0)

    if a == 'date': 
      H = H.eval('value = @to_datetime(value, errors="coerce")')\
      .assign(year=lambda x: x['value'].dt.year)\
      .assign(month=lambda x: x['value'].dt.month)\
      .assign(day=lambda x: x['value'].dt.day)\
      .drop(columns=['value'])
    elif a == 'number': 
      H = H.assign(value=lambda x: x['value'].str.replace(r"\D", "", regex=True))\
      .drop_duplicates(subset=['value', 'doc'])

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

  def __init__(self, limit:int=10):

    self.limit = limit

    self.data: dict[str, DataFrame] = { k: DataFrame() for k in ['date', 'number', 'title', 'name', 'city'] }
    self.ngramdata: dict[str, DataFrame] = { k: DataFrame() for k in ['number', 'title', 'name', 'city'] }

    self.index = dict[str, set]()
    self.ngramindex = dict[str, set]()

  @staticmethod
  def basic_score(results:DataFrame):
    return results.value_counts('doc')

  def load(self, loader:Loader):

    L = loader

    self.add(L.melt('date'), 'date')
    self.add(L.melt('number'), 'number')
    for h in ['title', 'name', 'city']:
      self.add(L.melt(h), h)

    H = ['number', 'title', 'name', 'city']
    for h in H + ['date']:
      self.index[h] = set(self.data[h].index)
    for h in H:
      self.ngramindex[h] = set(self.ngramdata[h].index)

  def add(self, frame:DataFrame, name:str):

    k = name

    if k == 'date': return self.adddt(frame, name)
    if k == 'number': return self.addnum(frame, name)

    X = frame

    if X.empty: return

    X['value'] = X['value'].astype('str')\
    .str.replace(r"[\W\d\s]+", " ", regex=True)\
    .str.upper()

    X = X.pipe(Word().pandas, 'value')\
    .query('value.str.len() >= 3')

    D = self.data
    D[k] = concat([D[k].reset_index(), X]) if not D[k].empty else X.copy()
    D[k] = D[k].set_index('value').sort_index()

    X = Ngram(3, prefix=True, suffix=True).pandas(X, 'value')

    N = self.ngramdata
    N[k] = concat([N[k].reset_index(), X]) if not N[k].empty else X
    N[k] = N[k].set_index('value').sort_index()

    return

  def adddt(self, frame:DataFrame, name='date'):

    k = name
    X = frame
    D = self.data

    if X.empty: return

    if D[k].empty: D[k] = X
    else: D[k] = concat([D[k].reset_index(), X])
    D[k] = D[k].set_index(['year', 'month', 'day']).sort_index()

  def addnum(self, frame:DataFrame, name='number'):

    k = name
    X = frame

    if X.empty: return

    X['value'] = X['value'].astype('str')\
    .str.replace(r"\D", "", regex=True)

    D = self.data
    D[k] = concat([D[k].reset_index(), X]) if not D[k].empty else X.copy()
    D[k] = D[k].set_index('value').sort_index()

    X = Ngram(3, prefix=True, suffix=False).pandas(X, 'value')

    N = self.ngramdata
    N[k] = concat([N[k].reset_index(), X]) if not N[k].empty else X
    N[k] = N[k].set_index('value').sort_index()

  def multisearch(self, idxqueries:list[tuple]):
    Q = idxqueries
    Y = [(i, self.search(q)) for i, q in Q]
    Y = concat([y for i, y in Y], keys=[k for i, y in Y for k in [i]*len(y)])
    Y = Y.fillna(0)
    return Y

  def search(self, query:str):

    q = query
    q = re.sub(Searcher.URLalike, '', q)

    W = [w for w in re.sub(r"[\W\d\s]+", " ", q).strip().upper().split(' ') if len(w) >= 3]

    X = [(x) for x, _, _, m in self.codemarker.union(q) if m == True]
    P = [m.groupdict() for v in X for m in re.finditer(Searcher.patentalike, v)]
    P = [re.sub(r'\D', '', d['number']) for d in P]
    D = [(y, m, d) for x in X for _, _, x, d, m, y in datenum(x)] + \
        [(y, m, d) for x in X for _, _, x, d, m, y in month(x)]

    M = [
          self.match('date', D),
          self.match('number', P),
          self.match('title', W),
          self.match('name', W),
          self.match('city', W),
          self.ngrammatch('number', Ngram(3, prefix=True, suffix=False).flat(P)),
          self.ngrammatch('title', Ngram(3, prefix=True, suffix=True).flat(W)),
          self.ngrammatch('name', Ngram(3, prefix=True, suffix=True).flat(W)),
          self.ngrammatch('city', Ngram(3, prefix=True, suffix=True).flat(W)),
                                     ]
    M = [U for U in M if not U.empty]
    if not M: return DataFrame()
    Y0 = concat(M)

    s = Searcher.basic_score(Y0)
    s = s[s > 0]
    if s.empty: return DataFrame()
    I = s.sort_values().tail(self.limit).index
    Y = Y0[Y0['doc'].isin(I) ]\
    .pivot_table(index=['doc'], columns=['repo', 'frame', 'col'], 
                 aggfunc='sum', values='ratio', fill_value=0)

    return Y

  def match(self, kind:str, data:list):
    X = set(data)
    I = list(self.index[kind] & X)
    Y = self.data[kind].loc[I]
    Y['ratio'] = 1
    return Y

  def ngrammatch(self, kind:str, data:list):
    X = set(data)
    I = list(self.ngramindex[kind] & X)
    Y = self.ngramdata[kind].loc[I]
    if Y.empty: return Y
    Y = Y.groupby(['repo', 'frame', 'col', 'doc'])\
    .agg({'ratio': 'sum'}).reset_index()
    return Y

class Polyproc:

  """
  Stosuje wiele procesorów do zmniejszenia czasu wykonywania zadania.
  Implementacja stosuje estymację na podstawie średniego czasu wykonania
  próbki, próbuje dostosować liczbę procesorów do optymalnego czasu `time`.
  Optymalny czas nie powinien być minimalny, bo wiąże się z uruchamianiem 
  procesów co nakłada czasochłonne działania systemu.
  """

  def __init__(self, optimal=5, sample=10000, limit=None):
    self.optimal = optimal
    self.sample = sample
    if limit is None:
      self.limit = os.cpu_count()

  def calc(self, frame:DataFrame, column:str, callname:str):

    "Oblicza `N`-procesorów do osiągnięcia `time` npdst. `sample`-próbek"

    f = self.__getattribute__(callname)

    X = frame
    k = column
    n0 = X.shape[0]
    n = min(self.sample, n0)
    S = X.sample(n)
    y = self.optimal
    N0 = self.limit

    t0 = time.time()
    f(S, k)
    t = time.time() - t0

    m = t/n
    N = min(int(n0*m/y), N0)

    return N

  def batch(self, frame:DataFrame, column:str, callname:str, batches:int) -> DataFrame:

    f = self.__getattribute__(callname)

    X0 = frame
    N = batches
    n0 = X0.shape[0]
    n = n0 // N + (n0 % N > 0)
    B = [X0.iloc[i*n:(i+1)*n] for i in range(N)]
    Y = []
    with Pool(processes=N) as pool:
      Y = list(pool.starmap(f, [(X, column) for X in B]))

    return concat(Y)


  def pandas(self, frame:DataFrame, column:str):

    "Procesuje ramkę danych `frame` w kolumnie `column`"

    N = self.calc(frame, column, '_pandas')
    if N < 2: return self._pandas(frame, column)
    return self.batch(frame, column, '_pandas', N)


class Word(Polyproc):

  def _pandas(self, frame:DataFrame, column:str):

    X = frame
    k = column

    def wordrepl(x):
      return [{ **x, k: y } for y in x[k].split(' ')]

    Y = X.dropna(subset=[k])\
    .apply(wordrepl, axis=1).explode().dropna()\
    .pipe(lambda S: DataFrame.from_records(S.tolist()))

    return Y

class Ngram(Polyproc):

  def __init__(self, n:int,
               prefix:bool=True, suffix:bool=True,
               repl:str='_', strprocargs:dict={}):

    super().__init__( **strprocargs )

    self.n = n
    self.prefix = prefix
    self.suffix = suffix
    self.repl = repl

  def _string(self, x:str):

    p = 1 if self.prefix else 0
    s = 1 if self.suffix else 0
    r = self.repl
    n = self.n

    return [(p*i)*r + x[i:i+n] + (s*max(0,(len(x)-3-i)))*r
      for i in range(len(x)-n+1) ]

  def flat(self, x:list[str]):
    return [k for y in x for k in self._string(y)]

  def _pandas(self, frame:DataFrame, column:str):

    X = frame
    k = column

    def gramrepl(x):
      return [{ **x, k: y, 'ratio': self.n/len(x[k]) } for y in self._string(x[k])]

    Y = X.dropna(subset=[k])\
    .apply(gramrepl, axis=1).explode().dropna()\
    .pipe(lambda S: DataFrame.from_records(S.tolist()))

    return Y