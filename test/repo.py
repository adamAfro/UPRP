import sys, os, unittest, cProfile, re
from pandas import Series, DataFrame, date_range, to_datetime
from numpy.random import randint, choice, seed
from numpy import datetime64

seed(42)

DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.join(DIR, '..')
sys.path.append(ROOT)
from lib.repo import Storage, Searcher
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
def unique(fix:str):
  global UNIQ
  UNIQ += 1
  return f'{fix}{UNIQ}'

def profmockup(entities:int, alphabet:str, digits:str, dates:tuple[int, int]):

  a = alphabet
  d = digits

  N = entities
  I = [unique('D') for _ in range(N)]
  D0 = date_range(f'{dates[0]}-01-01', f'{dates[1]}-12-31', freq='D')

  nA = randint(1, N + randint(1, N))
  nB = randint(1, N + randint(1, N))
  nC = randint(1, N + randint(1, N))

  H = {

    'A': DataFrame({'id': [unique('I') for _ in range(nA)],
                    'doc': [choice(I) for _ in range(nA)],
                    'a-date': choice(D0, nA),
                    'a-number': [anystr(d, 5, 16) for _ in range(nA)],
                    'a-title': [anystr(a) for _ in range(nA)] }).set_index(['id', 'doc']),

    'B': DataFrame({'id': [unique('I') for _ in range(nB)],
                    'doc': [choice(I) for _ in range(nB)],
                    'b-name': [anystr(a) for _ in range(nB)],
                    'b-city': [anystr(a) for _ in range(nB)] }).set_index(['id', 'doc']),

    'C': DataFrame({'id': [unique('I') for _ in range(nC)],
                    'doc': [choice(I) for _ in range(nC)],
                    'c-number': [anystr(d, 5, 16) for _ in range(nC)],
                    'c-city': [anystr(a) for _ in range(nC)] }).set_index(['id', 'doc']),
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

  M = { k: Storage('mockup', *profmockup(n, a, d, y)) for k, a, d, y in M0 }
  S.add([(h, L.melt(h)) for h0, L in M.items()
         for h in ['date', 'number', 'name', 'city', 'title']])

  return S, M

def genqueries(loader:Storage):

  L = loader
  n = L.docs.shape[0]
  I = L.docs.sample(n)
  Y = []

  for i in I.values:

    Q0 = L.getdocs([i])
    Q = [v if not k.endswith('number') else "PL"+v
          for X in Q0.values()
          for k in X.columns for v in X[k].values ]

    D = ["<DATED>"+to_datetime(v).strftime("%Y-%m-%d")+"</DATED>"
         for v in Q if isinstance(v, datetime64)]
    Q = [v for v in Q if not isinstance(v, datetime64)]
    Q = [v for V in Q for v in V.split(' ') if len(v) > 3]
    Q = Q + D

    q0 = randint(3, min(10, len(Q))) if len(Q) > 3 else len(Q)
    Y.append(' '.join(choice(Q, q0, replace=False)))

  G = Series(Y, index=I.values)
  G = G[G != '']

  return G

class TestSearch(unittest.TestCase):

  @classmethod
  def setUpClass(cls):

    S, M = mockup(entities=1000)

    cls.searcher = S
    cls.mockup = M
    cls.queries: dict[str, Series] = { k0: genqueries(M[k0]) for k0 in M.keys() }

  def assertion(self, results:DataFrame, index, domain:list[str], top=None):

    M = self.mockup

    y = results
    i = index
    K = domain

    self.assertFalse(y.empty, 'no matches')
    self.assertIn(i, y.index, 'not found')

    y = y.sort_values([('', '', '', 'level'), ('', '', '', 'score')], ascending=False)
    for j in y.index:

      for k in [k for k in M.keys() if k not in K]:
          self.assertNotIn(j, M[k].docs.values, 'wrong domain')

      self.assertTrue(any(j in M[k].docs.values for k in K), 'out of domain')

    for k in [k for k in y.columns if k[3] == "number"]:
      self.assertLessEqual(y[k].max(), 1.0, f'counted multiple numbers:\n{y.loc[y[k].idxmax()]}')

    if top is not None:
      self.assertIn(i, y.head(top).index, f'not a top result')

  def test_search(self):

    S = self.searcher
    K0 = { 'X': ['X', 'XY'],
           'XY': ['X', 'Y', 'XY'],
           'Y': ['Y', 'XY'],
           'Z': ['Z'] }

    H = DataFrame([{ 'doc': i, 'query': q, 'domain': K0[k] }
                  for k, Q in self.queries.items()
                  for i, q in Q.items()]).set_index('doc')

    Q = [(i, q) for i, q in H['query'].reset_index(drop=True).items()]
    Y0 = S.search(Q, limit=100)
    Y = Y0.reset_index().set_index(('doc', '', '', ''))

    for i, h in H.iterrows():
      with self.subTest(index=i, query=h['query'], domain=h['domain']):
        try: self.assertion(Y.loc[[i]], i, h['domain'])
        except KeyError: self.fail(f'no match for {i}')

class AssertionOnlyTestResult(unittest.TextTestResult):
  def addError(self, test, err):
    exc_type, exc_value, exc_traceback = err
    if exc_type is AssertionError:
      self.failures.append((test, self._exc_info_to_string(err, test)))
    else:
      try: raise err
      except: raise Exception(exc_type, exc_value).with_traceback(exc_traceback)

  def addSubTest(self, test, subtest, err):
    if err is not None:
      exc_type, exc_value, exc_traceback = err
      if exc_type is AssertionError:
        self.failures.append((subtest, self._exc_info_to_string(err, test)))
      else:
        try: raise err
        except: raise Exception(exc_type, exc_value).with_traceback(exc_traceback)

class AssertionOnlyTestRunner(unittest.TextTestRunner):
  def _makeResult(self):
    return AssertionOnlyTestResult(self.stream, self.descriptions, self.verbosity)


T = unittest.TestSuite()
T.addTest(TestSearch('test_search'))

with cProfile.Profile() as pr:
  AssertionOnlyTestRunner().run(T)

X = DataFrame(pr.getstats(),
  columns=['func', 'ncalls', 'ccalls', 'tottime', 'cumtime', 'callers'])

X['file'] = X['func'].apply(lambda x: re.search(r'file "([^"]+)"', str(x)))
X['file'] = X['file'].apply(lambda x: x.group(1) if x else None)
X['local'] = X['file'].apply(lambda x: x.replace(ROOT, '') if x else None)

X['line'] = X['func'].apply(lambda x: re.search(r'line (\d+)', str(x)))
X['line'] = X['line'].apply(lambda x: x.group(1) if x else None)

X['code'] = X['func'].apply(lambda x: re.search(r'<code object \<?(\w+)\>?', str(x)))
X['code'] = X['code'].apply(lambda x: x.group(1) if x else None)

X['percall'] = X['tottime'] / X['ncalls']

K = ['percall', 'local', 'line', 'code', 'tottime']
X = X[K+ [k for k in X.columns if k not in K]]
X = X.sort_values('tottime', ascending=False)
X.head(24)