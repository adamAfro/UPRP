import sys, os, pytest
from pandas import Series, DataFrame, date_range, to_datetime
from numpy.random import randint, choice, seed
from numpy import datetime64

DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.join(DIR, '..')
sys.path.append(ROOT)
from lib.repo import Loader, Searcher
from lib.log import log, progress

def randinsert(x:str, char:str, min:int, max:int):
  n = randint(min, max) if min != max else min
  for i in range(n):
    k = randint(0, len(x))
    x = x[:k] + char + x[k:]
  return x

def anystr(charset:str, min:int=5, max:int=64) -> str:
  n = randint(min, max) if min != max else min
  x = ''.join(choice(list(charset), n))
  if len(x) > 16: x = x[:3] + randinsert(x[:3], ' ', 2, 4)
  return x

UNIQ = 0
def unique():
  global UNIQ
  UNIQ += 1
  return f'U{UNIQ}'

def profmockup(entities:int, alphabet:str, digits:str, dates:tuple[int, int]):

  seed(42)

  a = alphabet
  d = digits

  N = entities
  I = [unique() for _ in range(N)]
  D0 = date_range(f'{dates[0]}-01-01', f'{dates[1]}-12-31', freq='D')

  nA = randint(1, N + randint(1, N))
  nB = randint(1, N + randint(1, N))
  nC = randint(1, N + randint(1, N))

  H = {

    'A': DataFrame({'doc': [choice(I) for _ in range(nA)],
                    'a-date': choice(D0, nA),
                    'a-number': [anystr(d, 5, 16) for _ in range(nA)],
                    'a-title': [anystr(a) for _ in range(nA)] }).set_index('doc'),

    'B': DataFrame({'doc': [choice(I) for _ in range(nB)],
                    'b-name': [anystr(a) for _ in range(nB)],
                    'b-city': [anystr(a) for _ in range(nB)] }).set_index('doc'),

    'C': DataFrame({'doc': [choice(I) for _ in range(nC)],
                    'c-number': [anystr(d, 5, 16) for _ in range(nC)],
                    'c-city': [anystr(a) for _ in range(nC)] }).set_index('doc'),
  }

  A = {
    'A': { 'a-date': 'date', 'a-number': 'number', 'a-title': 'title' },
    'B': { 'b-name': 'name', 'b-city': 'city' },
    'C': { 'c-number': 'number', 'c-city': 'city' },
  }

  return H, A

def mockup(entities:int):

  S = Searcher()

  n0 = entities
  M0 = [('X', 'abcABC', '123', (1900, 1949)),
        ('XY', 'abcefgABCEFG', '123456', (1900, 1999)),
        ('Y', 'efgEFG', '456', (1950, 1999)),
        ('Z', 'hijHIJ', '789', (2000, 2030))]

  n = n0 // len(M0)

  M = { k: Loader('mockup', *profmockup(n, a, d, y)) for k, a, d, y in M0 }
  S.add([(h, L.melt(h)) for h0, L in M.items()
         for h in ['date', 'number', 'name', 'city', 'title']])

  return S, M

@pytest.fixture(scope="module")
def init(entities=100):
  return mockup(entities)

def test_initialization(init: tuple[Searcher, dict[str, Loader]]):
  S, _ = init
  assert not S.indexes['dates'].indexed.empty
  assert not S.indexes['numbers'].indexed.empty
  assert not S.indexes['numprefix'].indexed.empty
  assert not S.indexes['words'].indexed.empty
  assert not S.indexes['ngrams'].indexed.empty

def genqueries(loader:Loader, nmax=1, dating=False):

  L = loader
  n = min(nmax, L.unique.shape[0])
  I = L.unique.sample(n)
  Y = []

  for i in I.values:

    Q0 = L.get([i])
    Q = [v if not k.endswith('number') else "PL"+v
          for X in Q0.values()
          for k in X.columns for v in X[k].values ]

    if dating:
      raise NotImplementedError('parser nie odróżnia niektórych dat od kodów, np.: PL 222222 2000-12-12')
      D = [to_datetime(v).strftime("%Y-%m-%d") for v in Q if isinstance(v, datetime64)]

    Q = [v for v in Q if not isinstance(v, datetime64)]
    Q = [v for V in Q for v in V.split(' ') if len(v) > 3]

    q0 = randint(3, min(10, len(Q))) if len(Q) > 3 else len(Q)
    Y.append(' '.join(choice(Q, q0, replace=False)))

  G = Series(Y, index=I.values)
  G = G[G != '']
  return G

def test_search(init: tuple[Searcher, dict[str, Loader]], maxsearches=100):

  S, M = init
  n0 = maxsearches
  n = n0 // len(M)

  for k0, K in [('X', ['X', 'XY']),
                ('XY',['X', 'Y', 'XY']),
                ('Y', ['Y', 'XY']),
                ('Z', ['Z'])]:

    Q = genqueries(M[k0], n)
    for i, q in progress(Q.items(), desc=f'🔎 {k0}', total=Q.shape[0]):

      y = S.search(q)
      assert not y.empty, 'no matches'
      assert i in y.index, 'not found'

      for j in y.index:

        for k in [k for k in M.keys() if k not in K]:
          assert j not in M[k].unique.values, 'wrong domain'

        assert any(j in M[k].unique.values for k in K), 'out of domain'

      TODO = True
      if TODO: continue

      for k in [k for k in y.columns if k[3] == "number"]:
        assert y[k].max() <= 1.0, f'counted multiple numbers: {y.loc[y[k].idxmax()]}'