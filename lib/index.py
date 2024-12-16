from cudf import concat, DataFrame
import os, time, multiprocessing
import pandas

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

    self.value = name
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



class Base:

  "`Base('searched-values').add(X)`"

  def __init__(self, factor:float=1.0, value:str='value', score:str='score', *args, **kwargs):


    self.factor = factor

    self.value = value
    self.score = score
    self.indexed = DataFrame()

  @staticmethod
  def idxinv(X:pandas.Series):
    v = X.name
    Y = X.reset_index().set_index(v)
    return DataFrame.from_pandas(Y)

  def match(self, keys:pandas.Series, minscore:float=0.5, aggregation:str='sum'):

    A = aggregation
    X = self.idxinv(keys)
    M = self.indexed.join(X)

    if M.empty: return DataFrame()

    h = self.score
    K = [k for k in M.columns if k != h]
    Y = M.groupby(K).agg(A)

    m = minscore
    return Y.query(f"{h} >= {m}")

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

  def _prep(self, frame:DataFrame):
    return DataFrame.from_pandas(frame)

  def add(self, frame:pandas.DataFrame, reindex=True, cumulative=True):

    if frame.empty: return frame
    L = self.indexed
    X = self._prep(frame)

    if self.score in X.columns:

      X = X.drop(columns=[self.score])

    if cumulative:

      X = X.value_counts() * self.factor
      X = X.reset_index(name=self.score)

    else:

      X = X.drop_duplicates()
      X[self.score] = self.factor

    if L.index.name is not None: L = L.reset_index()
    L = X if L.empty else concat([L, X])
    self.indexed = L

    if reindex:
      self.reindex()

    return X.to_pandas()

  def reindex(self):
    if self.indexed.empty: return
    self.indexed = self.indexed.set_index(self.value).sort_index()

  def dump(self):
    return self.value, self.indexed

  def load(self, name:str, indexed:DataFrame):
    self.value = name
    self.indexed = indexed



class Digital(Base):

  def _prep(self, frame):

    X = frame
    X[self.value] = X[self.value].astype('str')\
    .str.replace(r"\D", "", regex=True)

    return DataFrame.from_pandas(X)



class Slices(Base):

  def __init__(self, *args, sep:str=' ', **kwargs):

    super().__init__(*args, **kwargs)
    self.sep = sep

  def _prep(self, frame:pandas.DataFrame):

    X = frame
    k = self.value

    def wordrepl(x):
      return [{ **x, k: y } for y in x[k].split(self.sep)]

    Y = X.dropna(subset=[k])\
    .apply(wordrepl, axis=1).explode().dropna()\
    .pipe(lambda S: pandas.DataFrame.from_records(S.tolist()))

    return DataFrame.from_pandas(Y)



class Words(Slices):

  def __init__(self, *args, case=None, minl:int=1, **kwargs):

    super().__init__(*args, **kwargs)
    self.case = case
    self.minl = minl

  def _prep(self, frame:pandas.DataFrame):

    k = self.value
    c = self.case
    m = self.minl
    X = frame

    X[k] = X[k].astype('str').str.replace(r"[\W\d\s]+", " ", regex=True)
    if c == 'lower': X[k] = X[k].str.lower()
    elif c == 'upper': X[k] = X[k].str.upper()

    Y = super()._prep(X)
    Y = Y.loc[Y[k].str.len() >= m]

    return DataFrame.from_pandas(Y)



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
               score:str='score',
               weight:str='weight',
               owner:str='owner',
               **kwargs):

    super().__init__(*args, score=score, **kwargs)
    self.n = n
    self.prefix = prefix
    self.suffix = suffix
    self.repl = repl
    self.weight = weight
    self.owner = owner

  def match(self, keys:pandas.Series, minscore:float=0.5, aggregation:str='sum', minshare:float=0.5):

    A = aggregation
    v = self.value
    w = self.weight
    X = self._prep(keys.reset_index()).drop(columns=[w])\
    .drop_duplicates().set_index(v)
    M = self.indexed.join(X)

    if M.empty: return DataFrame()

    H0 = [self.weight, self.score]
    K0 = [k for k in M.columns if k not in H0]
    Y0 = M.groupby(K0).sum()

    h0 = self.weight
    m0 = minshare
    Y0 = Y0.query(f"{h0} >= {m0}")

    if Y0.empty: return DataFrame()

    Y1 = Y0.reset_index()
    h = self.score
    Y1[h] = Y1[h] * Y1[w] if A == 'sum' else Y1[w]

    H = [self.owner, self.weight, self.score]
    K = [k for k in Y1.columns if k not in H]
    Y2 = Y1[K+[h]].groupby(K).agg(A)

    m = minscore
    return Y2.query(f"{h} >= {m}")

  def _prep(self, frame: pandas.DataFrame):

    X = frame
    k = self.value
    h = self.weight
    n = self.n

    def gramrepl(x):
      return [{ **x, k: y, h: 1/(len(x[k])-n+1) } for y in self._string(x[k])]

    Y = X.dropna(subset=[k])\
    .apply(gramrepl, axis=1).explode().dropna()\
    .pipe(lambda S: pandas.DataFrame.from_records(S.tolist()))

    return DataFrame.from_pandas(Y)

  def _string(self, x:str):

    p = 1 if self.prefix else 0
    s = 1 if self.suffix else 0
    r = self.repl
    n = self.n

    return [(p*i)*r + x[i:i+n] + (s*max(0,(len(x)-n-i)))*r
      for i in range(len(x)-n+1) ]