import sys, os
from pandas import DataFrame, date_range
from numpy.random import randint, choice, seed
from uuid import uuid1

DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.join(DIR, '..')
sys.path.append(ROOT)
from lib.repo import Loader, Searcher

seed(42)

def randstr(min:int=5, max:int=64, alphabet:str=' 123abcABC') -> str:
  n = randint(min, max) if min != max else min
  return ''.join(choice(list(alphabet), n))

def randnum(min:int=5, max:int=10):
  return randstr(min, max, alphabet='123')

def mockup(entities:int):

  N = entities
  I = [str(uuid1()) for _ in range(N)]
  D0 = date_range('1900-01-01', '2021-12-31', freq='D')

  nA = randint(1, N + randint(1, N))
  nB = randint(1, N + randint(1, N))
  nC = randint(1, N + randint(1, N))

  H = {

    'A': DataFrame({'doc': [choice(I) for _ in range(nA)],
                    'a-date': choice(D0, nA),
                    'a-number': [randnum() for _ in range(nA)],
                    'a-title': [randstr() for _ in range(nA)] }).set_index('doc'),

    'B': DataFrame({'doc': [choice(I) for _ in range(nB)],
                    'b-name': [randstr() for _ in range(nB)],
                    'b-city': [randstr() for _ in range(nB)] }).set_index('doc'),

    'C': DataFrame({'doc': [choice(I) for _ in range(nC)],
                    'c-number': [randnum() for _ in range(nC)],
                    'c-city': [randstr() for _ in range(nC)] }).set_index('doc'),
  }

  A = {
    'A': { 'a-date': 'date', 'a-number': 'number', 'a-title': 'title' },
    'B': { 'b-name': 'name', 'b-city': 'city' },
    'C': { 'c-number': 'number', 'c-city': 'city' },
  }

  return H, A

def test_searcher(entities=1000, searches=1000):

  n0 = entities
  n = searches

  H, A = mockup(entities=n0)

  S = Searcher()
  L = Loader('mockup', H, A)

  S.load(L)
  assert not S.data['date'].empty
  assert not S.data['number'].empty
  assert not S.data['title'].empty
  assert not S.data['name'].empty
  assert not S.data['city'].empty

  for _ in range(n):
    h = choice([h for h in (H.keys() if not Searcher.TODO.Keysearch else ['A', 'C'])])
    k = choice([k for k in H[h].columns if k != 'doc' and k.endswith('number')])
    q = H[h][k].sample().values[0]
    r = S.search('PL'+q)
    assert r is not None

  for _ in range(n):
    r = S.search(randstr(alphabet=' XYZ'))
    assert r is None