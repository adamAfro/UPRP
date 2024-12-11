from pandas import concat, DataFrame, Series
import os, time, multiprocessing

class Multiprocessor:

  """
  Stosuje wiele procesorów do zmniejszenia czasu wykonywania zadania.
  Implementacja stosuje estymację na podstawie średniego czasu wykonania
  próbki, próbuje dostosować liczbę procesorów do optymalnego czasu `time`.
  Optymalny czas nie powinien być minimalny, bo wiąże się z uruchamianiem 
  procesów co nakłada czasochłonne działania systemu.

  ```py
  class Y(Multiprocessor):
    def _batched(self, frag:DataFrame) -> DataFrame:
      return f(frag)
  ```
  """

  def __init__(self, name:str, timing=5, tsample=10000, CPUlimit=128, forcecalc=True):

    self.name = name
    self.timing = timing
    self.tsample = tsample
    self.CPUlimit = CPUlimit if CPUlimit is not None else os.cpu_count()

    self.forcecalc = forcecalc
    self.tmean = None

  def _calctmean(self, frame:DataFrame):

    "Oblicza `N`-procesorów do osiągnięcia `timing` npdst. `tsample`-próbek"

    X = frame
    n0 = X.shape[0]
    n = min(self.tsample, n0)
    S = X.sample(n)

    t0 = time.time()
    self._batched(S)
    t = time.time() - t0

    return t/n

  def _batched(self, frame:DataFrame): return frame
  def _batch(self, frame:DataFrame, batches:int) -> DataFrame:

    X0 = frame
    N = batches
    n0 = X0.shape[0]
    n = n0 // N + (n0 % N > 0)
    B = [X0.iloc[i*n:(i+1)*n] for i in range(N)]
    Y = []
    with multiprocessing.Pool(processes=N) as U:
      Y = list(U.starmap(self._batched, [(X,) for X in B]))

    return concat(Y)

  def batch(self, frame:DataFrame):

    if (self.tmean is None) or self.forcecalc:
      self.tmean = self._calctmean(frame)

    X = frame
    n = X.shape[0]
    t = self.timing
    m = self.tmean
    N0 = self.CPUlimit
    N = min(int(n*m/t), N0)

    if N < 2: return self._batched(X)
    return self._batch(X, N)



class Base(Multiprocessor):

  "`Base('searched-values').add(X)`"

  def __init__(self, name:str, *args, **kwargs):

    super().__init__(name, *args, **kwargs)

    self.name = name
    self.indexed = DataFrame()
    self.indices = set()

  def match(self, values:list, minscore:int=1, aggregation='size') -> Series:

    A = aggregation

    m = minscore
    I0 = self.indices
    Y0 = self.indexed

    X = values
    I = list(I0 & set(X))

    M = Y0.loc[I]
    if M.empty: return Series()

    K = [k for k in M.columns]
    N = M.groupby(K).agg(A)
    N = N[N >= m]

    return N

  @staticmethod
  def matchhier(values:list, hier:dict[tuple, dict]):
    """
    ogranicza liczbę wyników do unikalnych wartości 
    po między indeksami do tych samych źródeł

    ```py
    matchhier(q, {
      (slices, {}): {
        (sufixngrams, { 'minscore': 0.5, 'aggregation': 'sum' }),
        (prefixngrams, { 'minscore': 0.5, 'aggregation': 'sum' })
      }
    })
    ```
    """
    raise NotImplementedError('wymaga dodatkowej identyfikacji w `Base.indexed`')
    raise NotImplementedError('wymaga funkcji do wstępnego przetwarzania zapytań w `Base`')

  def _prep(self, frame:DataFrame): return frame
  def add(self, frame:DataFrame, reindex=True):

    if frame.empty: return frame
    k = self.name
    L = self.indexed
    I = self.indices
    X = self._prep(frame)

    I = I.union( set(X[k]) )
    self.indices = I

    if L.index.name is not None: L = L.reset_index()
    L = X if L.empty else concat([L, X])
    self.indexed = L

    if reindex:
      self.reindex()

    return X

  def reindex(self):
    self.indexed = self.indexed.set_index(self.name).sort_index()

  def dump(self):
    return self.name, self.indexed, self.indices

  def load(self, name:str, indexed:DataFrame, indices:set):
    self.name = name
    self.indexed = indexed
    self.indices = indices



class Digital(Base):

  def _prep(self, frame):

    X = frame
    X[self.name] = X[self.name].astype('str')\
    .str.replace(r"\D", "", regex=True)

    return X



class Slices(Base):

  def __init__(self, *args, sep:str=' ', **kwargs):

    super().__init__(*args, **kwargs)
    self.sep = sep

  def _batched(self, frame:DataFrame):

    X = frame
    k = self.name

    def wordrepl(x):
      return [{ **x, k: y } for y in x[k].split(self.sep)]

    Y = X.dropna(subset=[k])\
    .apply(wordrepl, axis=1).explode().dropna()\
    .pipe(lambda S: DataFrame.from_records(S.tolist()))

    return Y



class Words(Slices):

  def __init__(self, *args, case=None, minl:int=1, **kwargs):

    super().__init__(*args, **kwargs)
    self.case = case
    self.minl = minl

  def _prep(self, frame:DataFrame):

    k = self.name
    c = self.case
    m = self.minl
    X = frame

    X[k] = X[k].astype('str').str.replace(r"[\W\d\s]+", " ", regex=True)
    if c == 'lower': X[k] = X[k].str.lower()
    elif c == 'upper': X[k] = X[k].str.upper()

    X = self.batch(X)
    X = X.loc[X[k].str.len() >= m]

    return X



class Ngrams(Base):

  """
  ```py
  X: DataFrame
  I0 = Words('value').add(X)
  I = Ngrams('value').extend(I0)
  ```
  """

  def __init__(self, *args, n:int,
               prefix:bool=True, suffix:bool=True,
               repl:str='_',
               shared:str='n',
               orderkey:str='i',
               **kwargs):

    super().__init__(*args, **kwargs)
    self.n = n
    self.prefix = prefix
    self.suffix = suffix
    self.repl = repl
    self.scorename = shared
    self.orderkey = orderkey

  def match(self, values:list[str], minscore:float=0.5, aggregation:str='sum', 
            minshare:float=0.5) -> Series:

    A = aggregation
    m2 = minscore
    m = minshare
    k0 = self.scorename

    X = self.flat(values)
    X.name = self.orderkey

    I0 = self.indices
    Y0 = self.indexed

    I = list(I0 & set(X.index))

    M = Y0.loc[I]
    if M.empty: return Series()

    K = [k for k in M.columns if k != k0]+[X.name]
    N = M.join(X).groupby(K).sum()[k0]
    M2 = N[N >= m].reset_index().drop(columns=[X.name])
    K2 = [k for k in M2.columns if k != k0]
    N2 = M2.groupby(K2).agg(A)[k0]
    N2 = N2[N2 >= m2]

    return N2

  def flat(self, values:list[str]):
    X = values
    if len(X) == 0: return Series()
    I, Y = zip(*[(i, y) for i, x in enumerate(X) 
                 for y in self._string(x)])

    return Series(I, index=Y)

  def _prep(self, frame:DataFrame):
    return self.batch(frame)

  def _batched(self, frame: DataFrame):

    X = frame
    k = self.name
    h = self.scorename
    n = self.n

    def gramrepl(x):
      return [{ **x, k: y, h: 1/(len(x[k])-n+1) } for y in self._string(x[k])]

    Y = X.dropna(subset=[k])\
    .apply(gramrepl, axis=1).explode().dropna()\
    .pipe(lambda S: DataFrame.from_records(S.tolist()))

    return Y

  def _string(self, x:str):

    p = 1 if self.prefix else 0
    s = 1 if self.suffix else 0
    r = self.repl
    n = self.n

    return [(p*i)*r + x[i:i+n] + (s*max(0,(len(x)-n-i)))*r
      for i in range(len(x)-n+1) ]