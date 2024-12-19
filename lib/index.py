import pandas, cudf
import os, time, multiprocessing
from copy import copy as shallow#HACK01

class Multiprocessor:

  """
  Stosuje wiele procesorów do zmniejszenia czasu wykonywania zadania.
  Implementacja stosuje estymację na podstawie średniego czasu wykonania
  próbki, próbuje dostosować liczbę procesorów do optymalnego czasu `time`.
  Optymalny czas nie powinien być minimalny, bo wiąże się z uruchamianiem 
  procesów co nakłada czasochłonne działania systemu.

  ```py
  class Y(Multiprocessor):
    def _prep(self, frag:DataFrame) -> DataFrame:
      return f(frag)
  ```
  """

  def __init__(self, timing=5, tsample=10000, CPUlimit=100, forcecalc=True):

    self.timing = timing
    self.tsample = tsample
    self.CPUlimit = CPUlimit if CPUlimit is not None else os.cpu_count()

    self.forcecalc = forcecalc
    self.tmean = None

  def _calctmean(self, frame:pandas.DataFrame):

    "Oblicza `N`-procesorów do osiągnięcia `timing` npdst. `tsample`-próbek"

    X = frame
    n0 = X.shape[0]
    n = min(self.tsample, n0)
    S = X.sample(n)

    t0 = time.time()
    self._prep(S)
    t = time.time() - t0

    return t/n

  def prep(self, frame:pandas.DataFrame, batches:int|None=1) -> cudf.DataFrame:

    N = batches
    if N is None:

      if (self.tmean is None) or self.forcecalc:
        self.tmean = self._calctmean(frame)

      X = frame
      n = X.shape[0]
      t = self.timing
      m = self.tmean
      N0 = self.CPUlimit
      N = min(int(n*m/t), N0)

    if N < 2:
      return cudf.DataFrame.from_pandas(self._prep(frame))

    return cudf.concat([cudf.DataFrame.from_pandas(X)
                        for X in self._bprep(X, N) if not X.empty])

  def _bprep(self, frame:pandas.DataFrame, batches:int) -> list[pandas.DataFrame]:

    X0 = frame
    N = batches
    n0 = X0.shape[0]
    n = n0 // N + (n0 % N > 0)
    B = [X0.iloc[i*n:(i+1)*n] for i in range(N)]

    Y = []
    L = [(X,) for X in B]
    Z = shallow(self)
    Z.indexed = None#HACK01
    f = Z._prep

    with multiprocessing.Pool(processes=N) as U:
      Y = [y for y in U.starmap(f, L)]

    return Y

  def _prep(self, frame:pandas.DataFrame) -> pandas.DataFrame:
    return frame



class Base(Multiprocessor):

  "`Base('searched-values').add(X)`"

  def __init__(self, factor:float=1.0, value:str='value', score:str='score', *args, **kwargs):

    super().__init__(*args, **kwargs)

    self.factor = factor

    self.value = value
    self.score = score
    self.indexed = cudf.DataFrame()

  @staticmethod
  def idxinv(X:pandas.Series):
    v = X.name
    Y = X.reset_index().set_index(v)
    return cudf.DataFrame.from_pandas(Y)

  def match(self, keys:pandas.Series, minscore:float=0.5, aggregation:str='sum'):

    if self.indexed.empty: return cudf.DataFrame()

    A = aggregation
    X = self.idxinv(keys)
    M = self.indexed.join(X)

    if M.empty: return cudf.DataFrame()

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

  def add(self, frame:pandas.DataFrame, reindex=True, cumulative=True):

    if frame.empty: return frame
    X = self.prep(frame, batches=None)

    if (self.score in X.columns) and cumulative:

      K = [k for k in X.columns if k != self.score]
      X = X.groupby(K).sum().reset_index()

    elif cumulative:

      X = X.value_counts()
      X = X.reset_index(name=self.score)

    else:

      X = X.drop_duplicates()
      X[self.score] = 1

    L = self.indexed
    if L.index.name is not None: L = L.reset_index()
    self.indexed = X if L.empty else cudf.concat([L, X])

    if reindex:
      self.reindex()

    return X.to_pandas()

  def reindex(self):
    if self.indexed.empty: return
    self.indexed = self.indexed.set_index(self.value).sort_index()

  def dump(self):
    return self.indexed

  def load(self, name:str, indexed:cudf.DataFrame):
    self.value = name
    self.indexed = indexed



class Digital(Base):

  def _prep(self, frame):

    X = frame
    X[self.value] = X[self.value].astype('str')\
    .str.replace(r"\D", "", regex=True)

    return X



class Slices(Base):

  def __init__(self, *args, sep:str=' ', **kwargs):

    super().__init__(*args, **kwargs)
    self.sep = sep

  def _prep(self, frame:pandas.DataFrame):

    X = frame
    k = self.value

    S = X.dropna(subset=[k])\
    .apply(self.wordrepl, axis=1).explode().dropna()
    Y = pandas.DataFrame.from_records(S.tolist())

    return Y

  def wordrepl(self, x):

    k = self.value
    w = self.score

    return [{ **x, k: y, w: len(y)/len(x[k]) } for y in x[k].split(self.sep)]



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

    return Y



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

  def match(self, keys:pandas.Series, minscore:float=0.5, aggregation:str='sum', minshare:float=0.5, ownermatch=None):

    if self.indexed.empty: return cudf.DataFrame()

    A = aggregation
    v = self.value
    w = self.weight
    X = self.prep(keys.reset_index()).drop(columns=[w])\
    .drop_duplicates().set_index(v)
    M = self.indexed.join(X)

    if M.empty: return cudf.DataFrame()

    h0 = self.weight
    K0 = [k for k in M.columns if k != h0]
    Y0 = M.groupby(K0).sum().clip(upper=1)

    m0 = minshare
    Y0 = Y0.query(f"{w} >= {m0}")

    if Y0.empty: return cudf.DataFrame()

    Y1 = Y0.reset_index()
    h = self.score
    Y1[h] = Y1[h] * Y1[w]
    Y1 = Y1.drop(columns=[w])

    if (ownermatch is not None) and (not ownermatch.empty):

      O = ownermatch.drop(columns=[self.score])
      D = Y1.reset_index().set_index(O.columns.tolist())
      O = O.set_index(O.columns.tolist())
      D = D.join(O, how='inner')
      Y1 = Y1.drop(D['index'].values, axis=0)
      if Y1.empty: return cudf.DataFrame()

    K2 = [k for k in Y1.columns if k != self.score]
    Y2 = Y1[K2+[h]].groupby(K2).agg(A).reset_index()

    K3 = [k for k in K2 if k != self.owner]
    Y3 = Y2.groupby(K3).agg('max')

    m = minscore
    return Y3.query(f"{h} >= {m}")

  def _prep(self, frame: pandas.DataFrame):

    X = frame
    k = self.value

    S = X.dropna(subset=[k])\
    .apply(self.gramrepl, axis=1).explode().dropna()
    Y = pandas.DataFrame.from_records(S.tolist())

    return Y

  def gramrepl(self, x):

    k = self.value
    h = self.weight
    n = self.n

    return [{ **x, k: y, h: 1/(len(x[k])-n+1) } for y in self._string(x[k])]

  def _string(self, x:str):

    p = 1 if self.prefix else 0
    s = 1 if self.suffix else 0
    r = self.repl
    n = self.n

    return [(p*i)*r + x[i:i+n] + (s*max(0,(len(x)-n-i)))*r
      for i in range(len(x)-n+1) ]

#HACK01: przechowywanie ustawień funkcji i danych może i jest wygodne
# ale gdy używa się wielu procesów to one kopiuję dane, co jest
# szczególnie problematyczne gdy używa się CUDA, bo karta graficzna się buntuje.
# Rozwiązanie kopiuje cały obiekt (płytko) i usuwa z siebie referencje do
# danych, więc nie ma problemu z ich kopiowaniem.
# Właściwe rozwiązanie wymaga przepisania funkcji `_prep`, żeby nie używały `self`,
# wtedy będą mogły być swobodnie przekazywane do wielu procesów.