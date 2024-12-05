from repo import test_search_success
import pandas, cProfile, re, os

DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.join(DIR, '..')
ROOT = os.path.abspath(ROOT)

with cProfile.Profile() as pr:
  test_search_success(10_000, 10_000)

X = pandas.DataFrame(pr.getstats(),
  columns=['func', 'ncalls', 'ccalls', 'tottime', 'cumtime', 'callers'])

X['file'] = X['func'].apply(lambda x: re.search(r'file "([^"]+)"', str(x)))
X['file'] = X['file'].apply(lambda x: x.group(1) if x else None)
X['local'] = X['file'].apply(lambda x: x.replace(ROOT, '') if x else None)

X['line'] = X['func'].apply(lambda x: re.search(r'line (\d+)', str(x)))
X['line'] = X['line'].apply(lambda x: x.group(1) if x else None)

X['code'] = X['func'].apply(lambda x: re.search(r'<code object \<?(\w+)\>?', str(x)))
X['code'] = X['code'].apply(lambda x: x.group(1) if x else None)

K = ['local', 'line', 'code', 'tottime']
X = X[K+ [k for k in X.columns if k not in K]]
X = X.sort_values('tottime', ascending=False)