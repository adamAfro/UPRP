import pandas, yaml, numpy
from lib.storage import Storage
from lib.name import mapnames, classify
from lib.flow import Flow, ImgFlow
import matplotlib.pyplot as plt
from config import Colr, Cmap
import geopandas as gpd
import geoplot as gplt
import geoplot.crs as gcrs
import seaborn as sns

@Flow.From()
def Nameclsf(asnstores:dict[Storage, str],
             assignements = ['names', 'firstnames', 'lastnames', 'ambignames'],
             typeassign='type-name'):

  Y = pandas.DataFrame()

  for assignpath, S in asnstores.items():

    with open(assignpath, 'r') as f:
      S.assignement = yaml.load(f, Loader=yaml.FullLoader)

    for h in ['assignee', 'applicant', 'inventor']:

      K = [f'{k0}-{h}' for k0 in assignements]

      X = pandas.concat([S.melt(f'{k}') for k in K]).set_index(['doc', 'id'])
      X = X[['value', 'assignement']]
      X['assignement'] = X['assignement'].str.split('-').str[0]

      T = S.melt(typeassign).set_index(['doc', 'id'])['value'].rename('type')
      if not T.empty: X = X.join(T, on=['doc', 'id'], how='left')

      Y = pandas.concat([Y, X]) if not Y.empty else X

  return mapnames(Y.reset_index(drop=True), 
                  orgqueries=['type.str.upper() == "LEGAL"'],
                  orgkeysubstr=['&', 'INTERNAZIO', 'INTERNATIO', 'INC.', 'ING.', 'SP. Z O. O.', 'S.P.A.'],
                  orgkeywords=[x for X in [ 'THE', 'INDIVIDUAL', 'CORP',
                                            'COMPANY PRZEDSIEBIORSTWO FUNDACJA INSTYTUT INSTITUTE',
                                            'HOSPITAL SZPITAL',
                                            'SZKOLA',
                                            'COMPANY LTD SPOLKA LIMITED GMBH ZAKLAD PPHU',
                                            'KOPALNIA SPOLDZIELNIA FABRYKA',
                                            'ENTERPRISE TECHNOLOGY',
                                            'LLC CORPORATION INC',
                                            'MIASTO GMINA URZAD',
                                            'GOVERNMENT RZAD',
                                            'AKTIENGESELLSCHAFT KOMMANDITGESELLSCHAFT',
                                            'UNIWERSYTET UNIVERSITY AKADEMIA ACADEMY',
                                            'POLITECHNIKA'] for x in X.split()])

@Flow.From()
def Textual(storage:Storage, assignpath:str, nameset:pandas.DataFrame,
            assignements = ['firstnames', 'lastnames'],
            assignentities = ['names', 'ambignames'],
            cityassign='city'):

  S = storage
  with open(assignpath, 'r') as f:
    S.assignement = yaml.load(f, Loader=yaml.FullLoader)

  Y = pandas.DataFrame()

  C = S.melt(cityassign).set_index(['doc','id'])['value'].rename('city')

  i = 0
  for h in ['assignee', 'applicant', 'inventor']:

    X0 = pandas.concat([S.melt(f'{k}') for k in [f'{k0}-{h}' for k0 in assignements]])
    if not X0.empty:
      X0 = X0.set_index(['doc', 'id']).join(C, how='left').reset_index()
      X0['id'] = X0.groupby(['doc', 'id']).ngroup() + i
      i = X0['id'].max() + 1

    X = pandas.concat([S.melt(f'{k}') for k in [f'{k0}-{h}' for k0 in assignentities]])
    if not X.empty:
      X = X.set_index(['doc', 'id']).join(C, how='left').reset_index()
      X['id'] = X.groupby(['doc', 'id', 'frame', 'col']).ngroup() + i
      i = X['id'].max() + 1

    if 'city' not in X.columns: X['city'] = None
    if 'city' not in X0.columns: X0['city'] = None

    X = pandas.concat([X0[['doc', 'id', 'value', 'city']], 
                       X[['doc', 'id', 'value', 'city']]])

    X = X.set_index(['doc', 'id'])

    Y = pandas.concat([Y, X]) if not Y.empty else X

  if Y.empty: pandas.DataFrame()
  Y = Y.reset_index().drop_duplicates(subset=['doc', 'value']).set_index(['doc', 'id'])
  
  P, O = classify(Y, nameset)
  try: O = O.drop(columns=['role', 'value'])#LEGACY
  except KeyError: pass

  P = P.reset_index().drop(columns='id')
  P.index = numpy.arange(P.shape[0])#WORKAROUND01
  P.index.name = 'id'
  assert { 'norm', 'firstnames', 'lastnames', 'city', 'unclear', 'doc' }.issubset(P.columns)
  assert { 'id' }.issubset(P.index.names)

  O = O.reset_index().drop(columns='id')
  O.index = numpy.arange(P.shape[0]+1, P.shape[0]+1+O.shape[0])#WORKAROUND01
  O.index.name = 'id'
  assert { 'norm', 'doc' }.issubset(O.columns)
  assert { 'id' }.issubset(O.index.names)

  P = P.drop_duplicates(['doc', 'norm'])#WTF nie powinno być

  return P, O

@Flow.From()
def Spacetime(textual:pandas.DataFrame, 
              geoloc:pandas.DataFrame, 
              event:pandas.DataFrame, 
              clsf:pandas.DataFrame):

  X, O = textual

  T = event
  X = X.reset_index().set_index('doc')\
       .join(T.groupby('doc')['delay'].min(), how='left').reset_index()

  G = geoloc.set_index('city', append=True)
  X = X.set_index(['doc', 'city']).join(G, how='left').reset_index()

  X = X.set_index('doc')
  C = clsf
  C['clsf'] = C['section']
  C = pandas.get_dummies(C[['clsf']], prefix_sep='-')
  C = C.groupby('doc').sum()
  X = X.join(C, how='left')

  X = X.reset_index().set_index('id')

  assert { 'doc', 'lat', 'lon', 'delay' }.issubset(X.columns)
  assert any([ c.startswith('clsf-') for c in X.columns ])
  assert { 'id' }.issubset(X.index.names)

  return X


@Flow.From('getting affilations')
def Affilate(registry:pandas.DataFrame):

  import tqdm

  assert { 'id' }.issubset(registry.index.names)
  assert { 'doc', 'lat', 'lon' }.issubset(registry.columns)

  X = registry

  kd = 'doc'
  KG = ['lat', 'lon']

  G = X.groupby(kd)

  X['nameaffil'] = [set() for _ in range(X.shape[0])]
  X['geoaffil'] = [set() for _ in range(X.shape[0])]

  for d, g in tqdm.tqdm(G, total=G.ngroups):
    if g.shape[0] > 1:
      for i in g.index:
        X.at[i, 'nameaffil'].update(g.index.values)
        X.at[i, 'geoaffil'].update(tuple(q) for q in g.loc[g.index != i, KG].dropna().values)

  X['nameaffil'] = X['nameaffil'].apply(lambda x: x if len(x) > 0 else None)
  X['geoaffil'] = X['geoaffil'].apply(lambda x: x if len(x) > 0 else None)
  
  assert { 'nameaffil', 'geoaffil' }.issubset(X.columns)
  assert { 'id' }.issubset(X.index.names)
  return X

@Flow.From('calculating similarity')
def Simcalc(affilated:pandas.DataFrame, qcount:str):

  import tqdm

  X = affilated

  k0 = 'nameset'
  k1 = 'firstnames'
  k2 = 'lastnames'
  kn = 'norm'
  i = 'id'

  c = 'clsf-'
  kG = ['lat', 'lon']

  kC = [k for k in X.columns if k.startswith(c)]
  
  class kX: G='geoaffil'; N='nameaffil'; C='clsfmatch'; D='clsfdiff'
  class kY: G='geoaffil'; N='nameaffil'; C='clsfmatch'; D='clsfdiff'; M='geomatch'

  X[[k1, k2, kn]] = X[[k1, k2, kn]].fillna('').astype(str)
  X[k0] = (X[k1] + ' ' + X[kn] + ' ' + X[k2]).str.strip().str.split()
  X[k0] = X[k0].apply(lambda x: frozenset([x[0], x[-1]]) if x else None)

  X = X[ X[k0].isin(X.value_counts(k0).to_frame().query(qcount).index) ]

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
def Entity(sim:pandas.DataFrame, all:pandas.DataFrame):

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

class Selfloc:

  @Flow.From()
  def fill(entities:pandas.DataFrame, group:str, loceval:str):

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
  def evalplot(X):

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

    X0 = X.query('~ loceval.isna()')
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

  @ImgFlow.From()
  def evalplot3D(X):

    X = X.dropna(subset=['lat', 'lon', 'delay'])

    f, A0 = plt.subplots(figsize=(15, 15))
    A0.axis('off')

   #normalizacja
    X['delay'] = X['delay'] - X['delay'].min()

    A = [f.add_subplot(221, projection='3d'),
         f.add_subplot(222, projection='3d'),
         f.add_subplot(223),
         f.add_subplot(224, projection='3d')]

    Selfloc.scatter3D(A[0], X, Colr.neutral)
    A[0].set_title('Geolokalizacje osób związane z patentami')

    Selfloc.scatter3D(A[1], X.query('~ loceval.isna()'), Colr.good)
    A[1].set_title('Geolokalizacje oparte o dane z rejestrów')

    Selfloc.scatter3D(A[3], X.query('loceval.isna()'), Colr.attention)
    A[3].set_title('Geolokalizacje wynikające z uzupełniania braków')

    X['loceval'] = X['loceval'].replace({ 'proximity': 'z blikości', 
                                          'unique': 'unikalne' }).fillna('uzupełnione')

    X['loceval'].value_counts().sort_values(ascending=False)\
                .plot.barh(ax=A[2], color=[Colr.attention, Colr.good, Colr.good], 
                          title='Ocena geolokalizacji', ylabel='')

    return f

  def scatter3D(ax, X, color):

   #nakładające się
    P = X.groupby(['delay', 'lat', 'lon']).size().rename('count')\
         .reset_index()

    ax.scatter(P['lon'], P['lat'], P['delay'], s=P['count']/10, c=color)

    ax.set_xlabel('Długość')
    ax.set_ylabel('Szerokość')
    ax.set_zlabel('Opóźnieninie (dni)')

@Flow.From()
def Merge(affilated:dict[str, pandas.DataFrame]):

  for k, X in affilated.items():
    X['repo'] = k  

  Y = pandas.concat(affilated.values())

  return Y

from config import data as D
from profiling import flow as f0
from patent import flow as fP

flow = { k: dict() for k in D.keys() }

flow['nameread'] = Nameclsf({ D['UPRP']+'/assignement.yaml': f0['UPRP']['profiling'],
                              D['Lens']+'/assignement.yaml': f0['Lens']['profiling'],
                              D['Google']+'/assignement.yaml': f0['Google']['profiling'] }).map('names.pkl')

flow['registry-textual'] = Textual(f0['UPRP']['profiling'], 
                                           assignpath=D['UPRP']+'/assignement.yaml', 
                                           nameset=flow['nameread']).map(D['UPRP']+'/textual.pkl')

flow['registry-spacetime'] = Spacetime(flow['registry-textual'], 
                                       fP['UPRP']['patent-geoloc'], 
                                       fP['UPRP']['patent-event'], 
                                       fP['UPRP']['patent-classify']).map('spacetime.pkl')

flow['affilate'] = Affilate(flow['registry-spacetime']).map('affilate.pkl')

flow['simcalc-00'] = Simcalc(flow['affilate'], qcount='count  < 10')
flow['simcalc-10'] = Simcalc(flow['affilate'], qcount='count >= 10 & count  < 50')
flow['simcalc-50'] = Simcalc(flow['affilate'], qcount='count >= 50')
flow['simcalc'] = Flow('Simcalc', lambda x: pandas.concat(x), 
                        args=[flow['simcalc-00'], 
                              flow['simcalc-10'],
                              flow['simcalc-50']]).map('sim.pkl')

flow['entity'] = Entity(sim=flow['simcalc'], all=flow['registry-spacetime']).map('entity.pkl')

flow['self-entity-geoloc'] = Selfloc.fill(flow['entity'], group='entity', loceval='identity')

@Flow.From()
def Geobackprop(entities:pandas.DataFrame):

  X = entities
  n = X['lat'].isna().sum()

  while(n > 0):

    X = flow['self-entity-geoloc'].callback(X, group='entity', loceval='identity')
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

flow['geobackprop'] = Geobackprop(flow['entity']).map('geobackprop.pkl')

flow['self-doc-geoloc'] = Selfloc.fill(flow['geobackprop'], group='doc', loceval='affilation')

flow['geoloc-eval'] = Selfloc.evalplot(flow['self-doc-geoloc']).map('docs/insight-img/registers/geoloc-eval.png')