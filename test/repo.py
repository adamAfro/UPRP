import sys, os, pickle, yaml
from pandas import DataFrame, date_range
from numpy.random import randint, choice

DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.join(DIR, '..')
sys.path.append(ROOT)
from lib.repo import Loader, Searcher

def data(repo:str):

  f = f"{ROOT}/{repo}/data.test.pkl"
  a = f"{ROOT}/{repo}/assignement.yaml"

  with open(f, 'rb') as f: H = pickle.load(f)
  for h, X in H.items(): X.set_index('doc', inplace=True)
  with open(a, 'r') as f:
    A = yaml.load(f, Loader=yaml.FullLoader)

  return Loader(repo, H, A)

def test_adding_uprp():

  S = Searcher()
  S.load(data('api.uprp.gov.pl'))

  assert S.data['date'] is not None
  assert S.data['number'] is not None
  assert S.data['title'] is not None
  assert S.data['name'] is not None
  assert S.data['city'] is not None

def test_adding_lens():

  S = Searcher()
  S.load(data('api.lens.org'))

  assert S.data['date'] is not None
  assert S.data['number'] is not None
  assert S.data['title'] is not None

def test_adding_both():

  S = Searcher()
  S.load(data('api.uprp.gov.pl'))
  S.load(data('api.lens.org'))

  assert S.data['date'] is not None
  assert S.data['number'] is not None
  assert S.data['title'] is not None
  assert S.data['name'] is not None
  assert S.data['city'] is not None