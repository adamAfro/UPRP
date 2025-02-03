import pandas, geopandas as gpd, altair as Plot
from lib.flow import Flow
import gloc

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
    E.loc[G.index, 'loceval'] = E.loc[G.index, 'loceval'].fillna(loceval)
    E.loc[G.index, 'lat'] = G['lat'].fillna(m[0])
    E.loc[G.index, 'lon'] = G['lon'].fillna(m[1])

  return E

@Flow.From()
def ptregion(X:gpd.GeoDataFrame, R:gpd.GeoDataFrame, idname:str):

  Y = gpd.sjoin(X, R[['geometry', 'gid']], how='left', predicate='within')
  Y = gpd.GeoDataFrame(Y, geometry='geometry').drop(columns=['index_right'])
  Y = Y.rename(columns={ 'gid': idname })

  return Y

from registry import flow as f0

affilG = affilgeo(f0['registry']['2013']).map('cache/affilate-geo.pkl')
affilN = affilnames(affilG).map('cache/affilate.pkl')

sim = simcalc(affilN).map('cache/sim.pkl')

identities = identify(sim=sim, all=f0['registry']['2013']).map('cache/entity.pkl')

geofilled0 = fillgeo(entities=identities, group='entity', loceval='identity')
geofilled = fillgeo(entities=geofilled0, group='doc', loceval='document').map('cache/filled.pkl')

mapped0 = Flow('make gpd', lambda X: gpd.GeoDataFrame(X.reset_index().assign(year=X['grant'].dt.year), 
                                                      geometry=gpd.points_from_xy(X.lon, X.lat, crs='EPSG:4326')), 
                                                      args=[geofilled])
mappedw = ptregion(mapped0, gloc.region[1], 'wgid')
mappedp = ptregion(mappedw, gloc.region[2], 'pgid').map('cache/mapped.pkl')
mapped = mappedp

flow = { 'subject': { 'map': mapped,
                      'fillgeo': geofilled,
                      'identify': identities,
                      'simcalc': sim,
                      'affilate': affilN } }

plots = dict()

plots[f'F-geoloc-eval-clsf'] = Flow(args=[mapped], callback=lambda X:

  Plot.Chart(X[['IPC', 'loceval']]\
              .explode('IPC')\
              .replace({'unique': 'jednoznaczna',
                        'proximity': 'najlbiższa innym',
                        'document': 'npdst. współautorów',
                        'identity': 'npdst. tożsamości' })\
              .value_counts(['IPC', 'loceval']).reset_index())

      .mark_bar().encode( Plot.Y(f'count:Q').title(None),
                          Plot.Color('loceval:N')\
                              .title('Metoda geolokalizacji')\
                              .legend(orient='bottom', columns=1),
                          Plot.X('IPC:N').title('Klasyfikacja')))

plots[f'F-geoloc-eval'] = Flow(args=[mapped], callback=lambda X:

  Plot.Chart(X.assign(year=X['grant'].dt.year.astype(int))\
              .replace({'unique': 'jednoznaczna',
                        'proximity': 'najlbiższa innym',
                        'document': 'npdst. współautorów',
                        'identity': 'npdst. tożsamości' })\
             .value_counts(['year', 'loceval']).reset_index())

      .mark_bar().encode( Plot.X('year:O').title('Rok'),
                          Plot.Y('count:Q').title(None),
                          Plot.Color('loceval:N')\
                              .title('Metoda geolokalizacji')\
                              .legend(orient='bottom', columns=2)))


for k, F in plots.items():
  F.name = k
  F.map(f'fig/subj/{k}.png')