import pandas, cProfile, re, os
from repo import init, test_initialization, \
  test_quant_external_search, test_quant_ngram_search, \
  test_quant_number_search, test_quant_search

DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.join(DIR, '..')
ROOT = os.path.abspath(ROOT)

with cProfile.Profile() as pr:
  inited = init(1000)
  test_initialization(inited)
  test_quant_external_search(inited, 10_000)
  test_quant_ngram_search(inited, 10_000)
  test_quant_number_search(inited, 10_000)
  test_quant_search(inited, 10_000)

X = pandas.DataFrame(pr.getstats(),
  columns=['func', 'ncalls', 'ccalls', 'tottime', 'cumtime', 'callers'])

X['file'] = X['func'].apply(lambda x: re.search(r'file "([^"]+)"', str(x)))
X['file'] = X['file'].apply(lambda x: x.group(1) if x else None)
X['local'] = X['file'].apply(lambda x: x.replace(ROOT, '') if x else None)

X['line'] = X['func'].apply(lambda x: re.search(r'line (\d+)', str(x)))
X['line'] = X['line'].apply(lambda x: x.group(1) if x else None)

X['code'] = X['func'].apply(lambda x: re.search(r'<code object \<?(\w+)\>?', str(x)))
X['code'] = X['code'].apply(lambda x: x.group(1) if x else None)

X['percall'] = X['tottime'] / X['ncalls']*1000

K = ['percall', 'local', 'line', 'code', 'tottime']
X = X[K+ [k for k in X.columns if k not in K]]
X = X.sort_values('tottime', ascending=False)
X.head(24)