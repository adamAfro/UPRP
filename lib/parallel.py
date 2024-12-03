import multiprocessing
from lib.log import progress

def listed(data, worker: callable, tqdmkwargs=dict(), chunksize=10):
  X = data
  n = len(X)
  f = worker
  n0 = chunksize

  with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as u:
    Y = list(progress(u.imap(f, X, chunksize=n0), total=n, **tqdmkwargs))
  return Y