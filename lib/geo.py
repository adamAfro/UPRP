import pandas
from geopy.distance import geodesic
from .log import *

def closest(namedgeo: pandas.DataFrame,
          group: str, name: str, lat: str, lon: str):

  """
  W każdej z grup usuwa powtarzające się nazwy miejscowości wybierając
  kombinacje o najniższej sumie odległości między pozostałymi.
  """

  def f(G): return _closest(G, name, lat, lon)

  Y = namedgeo.groupby(group)\
     .progress_apply(f).reset_index(drop=True)

  return Y


def _closest(namedgeo: pandas.DataFrame,
             name: str, lat: str, lon: str, evalkey:str='loceval'):

  """
  Usuwa powtarzające się nazwy miejscowości wybierając kombinacje
  o najniższej sumie odległości między pozostałymi.
  """

  G = namedgeo.copy()

  G[evalkey] = 'unique'
  N = G[name].value_counts()
  if N.max() == 1: return G

  G[evalkey] = 'proximity'
  U = G[ G[name].isin(N[ N == 1 ].index) ] #uniq.
  D = G[ G[name].isin(N[ N >  1 ].index) ] #dupl.

  if U.empty and (D[name].nunique() == 1):

    G[evalkey] = 'ambiguous'
    return D.sample(1)  #rand.

  I = [ D[ D[name] == k ].index for k in D[name].unique() ]

  C = combgen(I)
  C = [c + U.index.tolist() for c in C]

  Y = G.loc[C[0]] 
  m = distmx(Y, lat, lon).sum().sum()

  for c in C[1:]:

    X = G.loc[c]
    t = distmx(X, lat, lon).sum().sum()
    if t > m: continue
    Y, m = X, t

  return Y

def distmx(geo:pandas.DataFrame, lat:str, lon:str):

  n = len(geo)
  M = pandas.DataFrame(index=geo.index, columns=geo.index)

  for i in range(n):
    for j in range(i, n):

      A = (geo.iloc[i][lon], geo.iloc[i][lat])
      B = (geo.iloc[j][lon], geo.iloc[j][lat])

      d = geodesic(A, B).kilometers
      M.iloc[i, j] = d
      M.iloc[j, i] = d

  return M

def combgen(choices:list[list], Y0=[], i=0):

  if i == len(choices): return [Y0]

  return [c for I in choices[i] for c in combgen(choices, Y0 + [I], i + 1) ]