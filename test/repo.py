import sys, os, pytest
from pandas import DataFrame, date_range
from numpy.random import randint, choice, seed
from uuid import uuid1

DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.join(DIR, '..')
sys.path.append(ROOT)
from lib.repo import Loader, Searcher

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

def quantmockup(entities:int, alphabet:str, digits:str):

  seed(42)

  a = alphabet
  d = digits

  N = entities
  I = [str(uuid1()) for _ in range(N)]
  D0 = date_range('1900-01-01', '2010-12-31', freq='D')

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

@pytest.fixture(scope="module")
def searcher_loader(entities = 1000):

  H, A = quantmockup(entities, 'abcABC', '123')

  S = Searcher()
  L = Loader('mockup', H, A)
  S.load(L)
  return S, H

def test_initialization(searcher_loader: tuple[Searcher, dict[str, DataFrame]]):
  S, H = searcher_loader
  assert not S.data['date'].empty
  assert not S.data['number'].empty
  assert not S.data['title'].empty
  assert not S.data['name'].empty
  assert not S.data['city'].empty

def test_quant_ngram_search(searcher_loader: tuple[Searcher, dict[str, DataFrame]], searches=1000):
  S, H = searcher_loader
  n = searches
  for _ in range(n):
    h = choice([h for h in (H.keys())])
    k = choice([k for k in H[h].columns if k != 'doc' and 'date' not in k])
    q = H[h][k].sample().values[0].split(' ')[0][:3]
    if 'number' in k: q = 'PL'+q+'123' #min
    r = S.search(q)
    assert not r.empty

def test_quant_search(searcher_loader: tuple[Searcher, dict[str, DataFrame]], searches=1000):
  S, H = searcher_loader
  n = searches
  for _ in range(n):
    h = choice([h for h in (H.keys())])
    k = choice([k for k in H[h].columns if k != 'doc' and 'date' not in k])
    q = H[h][k].sample().values[0]
    if 'number' in k: q = 'PL'+q
    r = S.search(q)
    assert not r.empty

def test_quant_number_search(searcher_loader: tuple[Searcher, dict[str, DataFrame]], searches=1000):
  S, H = searcher_loader
  n = searches
  for _ in range(n):
    h = choice(['A', 'C'])
    k = choice([k for k in H[h].columns if k != 'doc' and k.endswith('number')])
    q = H[h][k].sample().values[0]
    r = S.search('PL'+q)
    assert not r.empty

def test_quant_external_search(searcher_loader: tuple[Searcher, dict[str, DataFrame]], searches=1000):
  S, H = searcher_loader
  n = searches
  for _ in range(n):
    r = S.search(anystr(charset=' XYZ'))
    assert r.empty