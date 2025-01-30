import pandas, lib, geopandas as gpd
from lib.flow import Flow
import altair as Plot
import geoloc

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

from registry import flow as f0

affilG = affilgeo(f0['registry']['2013']).map('subject/affilate-geo.pkl')
affilN = affilnames(affilG).map('subject/affilate.pkl')

sim = simcalc(affilN).map('subject/sim.pkl')

identities = identify(sim=sim, all=f0['registry']['2013']).map('subject/entity.pkl')

geofilled0 = fillgeo(entities=identities, group='entity', loceval='identity')
geofilled = fillgeo(entities=geofilled0, group='doc', loceval='document').map('subject/filled.pkl')

clusters = geofilled.trigger(lambda *X: lib.geo.cluster(X[0], 'kmeans', coords=['lat', 'lon'], innerperc=True,
                                                        keys=[f'clsf-{k}' for k in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']], k=4))

geolocated = Flow('make gpd', lambda X: gpd.GeoDataFrame(X.assign(year=X['grant'].dt.year), 
                                                         geometry=gpd.points_from_xy(X.lon, X.lat, crs='EPSG:4326')), 
                                                         args=[geofilled])

Woj = geoloc.ptregion(geolocated, geoloc.Woj)
Pow = geoloc.ptregion(geolocated, geoloc.Pow)

def applyclust(clu:Flow):

  F = []
  for g, G in clu().groupby('cluster'):
    f = geoloc.statunit(G, geoloc.flow['Misc']['dist'], coords=['lat', 'lon'], rads=[100])\
              .map(f'subject/clusters/stats-{g}.pkl')

    F.append(f)

  return Flow(callback=lambda *X: pandas.concat(X), args=F)

geostatunit = geoloc.statunit(geofilled, geoloc.flow['Misc']['dist'], coords=['lat', 'lon'], 
                     rads=[20, 50, 100]).map('subject/geostats.pkl')

flow = { 'subject': { 'fillgeo': geofilled,
                      'cluster': clusters,
                      'identify': identities,
                      'simcalc': sim, 
                      'affilate': affilN,
                      'geostatunit': geostatunit,
                      'cluststats': applyclust(clusters) } }

f = flow

f['subj-plot'] = dict()

f['subj-plot'][f'F-geoloc-eval-clsf'] = Flow(args=[geostatunit], callback=lambda X:

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

f['subj-plot'][f'F-geoloc-eval'] = Flow(args=[geostatunit], callback=lambda X:

  Plot.Chart(X.assign(year=X['grant'].dt.year)\
              .replace({'unique': 'jednoznaczna',
                        'proximity': 'najlbiższa innym',
                        'document': 'npdst. współautorów',
                        'identity': 'npdst. tożsamości' })\
             .value_counts(['year', 'loceval']).reset_index())

      .mark_bar().encode( Plot.X('year:T').title('Rok'),
                          Plot.Y(f'count:Q').title(None),
                          Plot.Color('loceval:N')\
                              .title('Metoda geolokalizacji')\
                              .legend(orient='bottom', columns=2)))

f['subj-plot'][f'map'] = Flow(args=[geostatunit, geoloc.Woj], callback=lambda X, G:

  Plot.Chart(G).mark_geoshape(stroke='black', fill=None) + \

  Plot.Chart(X.value_counts(['lat', 'lon'])\
              .reset_index())\

      .mark_circle().encode(longitude='lon:Q', latitude='lat:Q', 
                            color=Plot.Color(f'count:Q').scale(scheme='goldgreen'),
                            size=Plot.Size('count').title('Ilość pkt.')).project('mercator'))

f['subj-plot'][f'map-13-22'] = Flow(args=[geostatunit, geoloc.Woj], callback=lambda X, G:(

  lambda X, G=G:

    Plot.hconcat(

        Plot.vconcat(

            Plot.Chart(G).mark_geoshape(stroke='black', fill=None) + \

            X   .query('year == 2013')\
                .pipe(Plot.Chart, title=str(2013))\
                .mark_circle(color='black')\
                .encode(longitude='lon:Q', latitude='lat:Q', 
                        size=Plot.Size('count').title('Ilość')), *[

            Plot.Chart(G).mark_geoshape(stroke='black', fill=None) + \

            X   .assign(diff=X.sort_values('year').groupby(['lat', 'lon'])['count'].diff().fillna(0))\
                .query('year == @y')\
                .pipe(Plot.Chart, title=str(y)).mark_circle()\
                .encode(longitude='lon:Q', latitude='lat:Q', 
                        size=Plot.Size('count').title('Ilość'),
                        color=Plot.Color(f'diff:Q')\
                                  .scale(scheme='redyellowgreen')\
                                  .title('Zmiana r.r.'))

          for y in range(2014, 2017+1)]),

        Plot.vconcat(*[

            Plot.Chart(G).mark_geoshape(stroke='black', fill=None) + \

            X   .assign(diff=X.sort_values('year').groupby(['lat', 'lon'])['count'].diff().fillna(0))\
                .query('year == @y')\
                .pipe(Plot.Chart, title=str(y)).mark_circle()\
                .encode(longitude='lon:Q', latitude='lat:Q', 
                        size=Plot.Size('count').title('Ilość'),
                        color=Plot.Color(f'diff:Q')\
                                  .scale(scheme='redyellowgreen')\
                                  .title('Zmiana r.r.'))

          for y in range(2018, 2022+1)]),
      )
    )( X.assign(year=X['grant'].dt.year)\
        .value_counts(['lat', 'lon', 'year'])\
        .unstack(fill_value=0).stack().rename('count').reset_index()) )

f['subj-plot'][f'T-meandist'] = Flow(args=[geostatunit], callback=lambda X: (

  lambda P:

    P.mark_rect(width=50).encode(Plot.Color('value:Q').title('Wartość').scale(scheme='orangered')) + \
    P.mark_text(baseline='middle').encode(Plot.Text('value:Q', format=".2f"))

)(pandas.concat([ X[f'meandist{r}'].describe().drop('count').reset_index() for r in ['', '50', '100'] ])\
        .melt(id_vars='index').dropna()\
        .replace({ 'meandist': 'śr. odl.', 'meandist50': 'śr. odl. do 50km', 'meandist100': 'śr. odl. do 100km',  })\
        .replace({ 'mean': 'średnia', 'std': 'odch. std.', 'min': '0%', 'max': '100%' })\
        .pipe(Plot.Chart)\
        .encode(Plot.Y('variable:O').title(None),
                Plot.X('index:O').title(None).scale(padding=20))
))

for r in ['', '50', '100']:

  f['subj-plot'][f'F-meandist{r}'] = Flow(args=[geostatunit], callback=lambda X, r=r:

    Plot.Chart(X).mark_bar()\
        .encode(Plot.X(f'meandist{r}:Q').bin().title('Średni dystans'),
                Plot.Y('count()').title(None)))

  f['subj-plot'][f'map-meandist{r}'] = Flow(args=[geostatunit, geoloc.Pow], callback=lambda X, G, r=r: (
    lambda X, G=G, r=r:

    gpd .sjoin(gpd.GeoDataFrame(X, geometry=gpd.points_from_xy(X.lon, X.lat, crs=G.crs)), G, 
                how='right', predicate="within")\
        .groupby('geometry')[f'meandist{r}'].mean().reset_index()\
        .pipe(gpd.GeoDataFrame, geometry='geometry', crs=G.crs)\
        .pipe(Plot.Chart).mark_geoshape(stroke='black')\
        .encode(color=Plot.Color(f'meandist{r}:Q')\
                          .scale(scheme='blueorange')\
                          .title('Śr. d. w pow.')).project('mercator')

  )(X.value_counts(['lat', 'lon', f'meandist{r}']).reset_index()))

for k, F in f['subj-plot'].items():
  F.name = k
  F.map(f'fig/{k}.subj.png')