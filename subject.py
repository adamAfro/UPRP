import pandas
from lib.flow import Flow
import plot

@Flow.From()
def affilgeo(registry:pandas.DataFrame):

  import tqdm
  tqdm.tqdm.pandas()

  X = registry
  assert { 'id' }.issubset(X.index.names) and X.index.is_unique
  assert { 'doc', 'lat', 'lon', 'organisation' }.issubset(X.columns)

  geoset = lambda g: set(tuple(x) for x in g[['lat', 'lon']].dropna().values)\
                                  if g['lat'].count() > 0 else frozenset()
  geoassign = lambda g: g.assign(geoaffil=[geoset(g)]*g.shape[0])
  Y = X.reset_index().groupby('doc').progress_apply(geoassign).set_index('id')

  assert { 'doc', 'geoaffil' }.issubset(Y.columns)
  assert { 'id' }.issubset(Y.index.names) and  Y.index.is_unique
  return Y

@Flow.From()
def affilnames(registry:pandas.DataFrame):

  import tqdm
  tqdm.tqdm.pandas()

  assert { 'id' }.issubset(registry.index.names)
  assert { 'doc', 'lat', 'lon', 'organisation' }.issubset(registry.columns)

  U = registry

 #org'split
  o = U['organisation'].fillna(False)
  X = U.loc[ o[ o == False].index ]
  O = U.loc[ o[ o == True ].index ]

 #nameset'mk
  X[['firstnames', 'lastnames', 'value']] = \
    X[['firstnames', 'lastnames', 'value']].fillna('').astype(str)
  X['nameset'] = (X['firstnames'] + ' ' + X['value'] + ' ' + X['lastnames'])
  X['nameset'] = X['nameset'].str.strip().str.split()
  X['nameset'] = X['nameset'].apply(lambda x: frozenset([x[0], x[-1]]) if x else frozenset())
  O['nameset'] = O['value'].apply(lambda x: frozenset({x}))
  Y = pandas.concat([X, O])

 #affilating
  nameset = lambda g: set(g['nameset'].dropna().values)
  nameassign = lambda g: g.assign(nameaffil=[nameset(g)]*g.shape[0])
  Y = Y.reset_index().groupby('doc').progress_apply(nameassign).set_index('id')

  assert { 'doc', 'nameaffil' }.issubset(Y.columns)
  assert { 'id' }.issubset(Y.index.names)
  assert Y.index.is_unique
  return Y

@Flow.From('calculating similarity')
def simcalc(affilated:pandas.DataFrame):

  import tqdm

  X = affilated

  k0 = 'nameset'
  k1 = 'firstnames'
  k2 = 'lastnames'
  kn = 'value'
  i = 'id'

  c = 'clsf-'
  kG = ['lat', 'lon']

  kC = [k for k in X.columns if k.startswith(c)]

  class kX: G='geoaffil'; N='nameaffil'; C='clsfmatch'; D='clsfdiff'
  class kY: G='geoaffil'; N='nameaffil'; C='clsfmatch'; D='clsfdiff'; M='geomatch'

  X[[k1, k2, kn]] = X[[k1, k2, kn]].fillna('').astype(str)
  X[k0] = (X[k1] + ' ' + X[kn] + ' ' + X[k2]).str.strip().str.split()
  X[k0] = X[k0].apply(lambda x: frozenset([x[0], x[-1]]) if x else None)

  X = X[ X['organisation'].fillna(False) == False ]
  z = X['nameset'].apply(len) < 2
  if z.sum() > 0:
    print(Warning(f'(X.nameset.apply(len) < 2).sum() == {z.sum()}'))

 #Nameset
  X = X.reset_index().set_index(k0)
  Y = X.join(X, lsuffix='L', rsuffix='R')
  Y = Y.query(f'{i}L != {i}R')
  Y['min'] = Y[[i+'L', i+'R']].min(axis=1)
  Y['max'] = Y[[i+'L', i+'R']].max(axis=1)
  Y = Y.drop_duplicates(['min', 'max']).drop(['min', 'max'], axis=1)
  Y = Y.set_index([i+'L', i+'R'])
  X = X.set_index(i)

 #Geoloc
  Y[kY.M] = sum([Y[k+'L'] == Y[k+'R'] for k in kG]) == 2

 #Classification
  Y[kY.C] = sum([(~Y[k+'L'].isna()) & (Y[k+'L'] != 0) & (Y[k+'L'] == Y[k+'R']) for k in kC])
  Y[kY.D] = sum([(~Y[k+'L'].isna()) & (Y[k+'L'] != 0) & (Y[k+'L'] != Y[k+'R']) for k in kC])

  Y[kY.N] = 0
  Y[kY.G] = 0

  for (i, j) in tqdm.tqdm(Y.index):

    if not ((X[kX.N].loc[i] is None) or (X[kX.N].loc[j] is None)):
      Y.loc[(i,j), kY.N] = len(X[kX.N].loc[i].union(X[kX.N].loc[j]))

    if not ((X[kX.G].loc[i] is None) or (X[kX.G].loc[j] is None)):
      Y.loc[(i,j), kY.G] = len(X[kX.G].loc[i].union(X[kX.G].loc[j]))

  S = Y[[kY.N, kY.G, kY.C, kY.D, kY.M]]
  S = S.sort_values([kY.N, kY.G, kY.C, kY.D, kY.M])

  i = 'id'
  assert { kY.N, kY.G, kY.C, kY.D, kY.M }.issubset(S.columns)
  assert { i+'L', i+'R' }.issubset(S.index.names)
  return S

@Flow.From()
def identify(sim:pandas.DataFrame, all:pandas.DataFrame):

  import networkx as nx

  S = sim
  G = S[ S['geomatch'] == True ].drop('geomatch', axis=1)
  Z = S[ S['geomatch'] == False ].drop('geomatch', axis=1)

  I = pandas.concat([G.query('~((clsfdiff > 0) & (clsfmatch == 0))')\
                      .query('(nameaffil > 0) | (geoaffil > 0)'),
                      Z.query('(clsfdiff == 0) | (clsfmatch > 0)')\
                      .query('nameaffil > 2') ])

  Y = all

  G = nx.Graph()
  G.add_nodes_from(list(set(Y.index)))
  G.add_edges_from([tuple(e) for e in I.index])

  X = pandas.DataFrame({'id': list(nx.connected_components(G))})
  X.index.name = 'entity'
  X = X.explode('id').reset_index().set_index('id')

  if 'entity' in Y.columns:
    Y = Y.drop(columns='entity')

  Y = Y.join(X)

  return Y

@Flow.From()
def fillgeo(entities:pandas.DataFrame, group:str, loceval:str):

  import tqdm

  assert group in entities.columns

  E = entities
  G = E.groupby(group)

  for g, G in tqdm.tqdm(G, total=G.ngroups):

    n = G[['lat', 'lon']].value_counts()
    if n.empty: continue

    m = n.idxmax()
    E.loc[G.index, 'loceval'] = loceval
    E.loc[G.index, 'lat'] = G['lat'].fillna(m[0])
    E.loc[G.index, 'lon'] = G['lon'].fillna(m[1])

  return E

@Flow.From()
def fillgeobpr(entities:pandas.DataFrame, group:str, loceval:str):

  X = entities
  n = X['loceval'].isna().sum()

  while(n > 0):

    if d == 0: break
    print(-d, "geo NA")

    affilG = affilgeo(X)
    affilN = affilnames(affilG)
    sim = simcalc(affilN)
    identified = identify(sim=sim, all=X)
    geofilled = fillgeo(identified, group=group, loceval=loceval)

    X = geofilled()

    d = n - X['loceval'].isna().sum()
    n = n - d

  return X

from registry import flow as f0

affilG = affilgeo(f0['registry']['2013']).map('subject/affilate-geo.pkl')
affilN = affilnames(affilG).map('subject/affilate.pkl')

sim = simcalc(affilN).map('subject/sim.pkl')

simplot = sim.trigger()
simplot.trigger(lambda X: plot.n(X.reset_index(drop=True), group='geomatch', categories=5))\
       .map('subject/similarities-geo.png')
simplot.trigger(lambda X: plot.n(X.reset_index(drop=True), group='nameaffil', xbinstart=5, xbin=3, xtick=8, categories=5))\
       .map('subject/similarities-nameaffil.png')

identities = identify(sim=sim, all=affilN).map('subject/entity.pkl')

geofilled = fillgeo(entities=identities, group='entity', loceval='identity')
geofilledBPR = fillgeobpr(geofilled, group='entity', loceval='identity-BPR')
docgeofilled = fillgeo(geofilledBPR, group='doc', loceval='document')

flow = { 'subject': { 'fillgeo0': geofilled,
                      'fillgeo': docgeofilled,
                      'identify': identities,
                      'simcalc': sim, 
                      'simplot': simplot,
                      'affilate': affilN } }