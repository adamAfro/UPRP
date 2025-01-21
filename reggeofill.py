import pandas, matplotlib.pyplot as plt
from lib.flow import Flow, ImgFlow
from config import Colr, Cmap
import geopandas as gpd
import geoplot as gplt

@Flow.From()
def Geoloc(entities:pandas.DataFrame, group:str, loceval:str):

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

@ImgFlow.From()
def Evalplot(X):

  f, A = plt.subplots(2, 2, figsize=(12, 12))

  X.loc[X['lat'].isna(), 'loceval'] = 'brak'
  X['loceval'] = X['loceval'].replace({ 'proximity': 'najbliższe\n z duplikatów', 
                                        'unique': 'unikalne' }).fillna('uzupełnione')
  X['loceval'].value_counts().sort_values(ascending=False)\
              .plot.bar(ax=A[0,0], color=[Colr.warning, Colr.attention, Colr.good, Colr.good], 
                        title='Rozkład typu geolokalizacji', xlabel='', rot=0)

  X = X.dropna(subset=['lat', 'lon'])

  P = gpd.GeoDataFrame(X, geometry=gpd.points_from_xy(X['lon'], X['lat']))
  G = P.groupby(['lat', 'lon', 'city']).size().rename('count').reset_index()
  G = gpd.GeoDataFrame(G, geometry=gpd.points_from_xy(G['lon'], G['lat']))
  G = G.sort_values('count', ascending=False).head(10)

  gplt.kdeplot(P, fill=True, cmap=Cmap.neutral, ax=A[0,1])
  A[0,1].set_title('Wszystkie geolokalizacje osób\nzwiązanych z patentami')
  for x, y, label in zip(G.geometry.x, G.geometry.y, G['city']):
    A[0,1].annotate(label, xy=(x, y), xytext=(3, 3), 
                  textcoords="offset points", fontsize=8, color='black')

  X2 = X.query('~ loceval.isna()')
  P = gpd.GeoDataFrame(X2, geometry=gpd.points_from_xy(X2['lon'], X2['lat']))
  G = P.groupby(['lat', 'lon', 'city']).size().rename('count').reset_index()
  G = gpd.GeoDataFrame(G, geometry=gpd.points_from_xy(G['lon'], G['lat']))
  G = G.sort_values('count', ascending=False).head(10)

  gplt.kdeplot(P, fill=True, cmap=Cmap.good, ax=A[1,0])
  A[1,0].set_title('Geolokalizacje wynikające\nbezpośrednio z rejestrów')
  for x, y, label in zip(G.geometry.x, G.geometry.y, G['city']):
    A[1,0].annotate(label, xy=(x, y), xytext=(3, 3), 
                    textcoords="offset points", fontsize=8, color='black')

  X0 = X.query(' loceval.isna()')
  P = gpd.GeoDataFrame(X0, geometry=gpd.points_from_xy(X0['lon'], X0['lat']))
  G = P.groupby(['lat', 'lon', 'city']).size().rename('count').reset_index()
  G = gpd.GeoDataFrame(G, geometry=gpd.points_from_xy(G['lon'], G['lat']))
  G = G.sort_values('count', ascending=False).head(10)

  gplt.kdeplot(P, fill=True, cmap=Cmap.attention, ax=A[1,1])
  A[1,1].set_title('Geolokalizacje uzupełnione na podstawie\npodobieństwa i współautorstwa')
  for x, y, label in zip(G.geometry.x, G.geometry.y, G['city']):
    A[1,1].annotate(label, xy=(x, y), xytext=(3, 3), 
                    textcoords="offset points", fontsize=8, color='black')

  return f

from registers import flow as f0

flow = dict()
d0 = 'registry/geofill'

E = Geoloc(f0['entity'], group='entity', loceval='identity').map(f'{d0}/entity-geoloc.pkl')
D = Geoloc(E, group='doc', loceval='affilation').map(f'{d0}/entity-geoloc-doc.pkl')

@Flow.From()
def Geobackprop(entities:pandas.DataFrame):

  X = entities
  n = X['lat'].isna().sum()

  while(n > 0):

    X = E.callback(X, group='entity', loceval='identity')
    d = n - X['lat'].isna().sum()
    n = n - d

    if d == 0: break
    print(-d, "geo NA")

    A = flow['affilate'].callback(X)

    S00 = flow['simcalc-00'].callback(A, qcount='count  < 10')
    S10 = flow['simcalc-10'].callback(A, qcount='count >= 10 & count  < 50')
    S50 = flow['simcalc-50'].callback(A, qcount='count >= 50')
    S = flow['simcalc'].callback([S00, S10, S50])

    X = flow['entity'].callback(S, X)

  return X

P = Geobackprop(flow['entity']).map(f'{d0}/geobackprop.pkl')

DP = Geoloc(P, group='doc', loceval=f'{d0}/geobackprop-doc.pkl')

flow['geoloc-eval'] = Evalplot(D).map('docs/insight-img/registers/geoloc-eval.png')