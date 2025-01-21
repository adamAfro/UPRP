import pandas, yaml, numpy
from lib.storage import Storage
from lib.name import mapnames
from lib.flow import Flow
import matplotlib.pyplot as plt
from util import strnorm

pandas.set_option('future.no_silent_downcasting', True)

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
def Pull(storage:Storage, assignpath:str,
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
    X[h] = True

    Y = pandas.concat([Y, X]) if not Y.empty else X

 #Roles
  Y[['assignee', 'applicant', 'inventor']] = Y[['assignee', 'applicant', 'inventor']].fillna(False)
  Y = Y.reset_index().groupby(['doc', 'id', 'city'], dropna=False)\
       .agg({'assignee': 'max', 'applicant': 'max', 'inventor': 'max', 
             'value': ' '.join }).reset_index()

 #Normalize
  Y['city'] = Y['city'].apply(strnorm, dropinter=True, dropdigit=True)
  Y['value'] = Y['value'].apply(strnorm, dropinter=False, dropdigit=False)

 #Traktowanie powtórzeń wynikających z nazw miast jako inne rejestry
  Y = Y.drop_duplicates(['doc', 'value', 'city'])

 #Concat'val
  Y['id'] = numpy.arange(Y.shape[0])
  Y = Y.set_index('id').drop_duplicates()
  assert { 'id' }.issubset(Y.index.names) and Y.index.is_unique
  assert { 'doc', 'value', 'city', 
           'assignee', 'applicant', 'inventor' }.issubset(Y.columns)

  return Y

@Flow.From()
def Textual(pulled:pandas.DataFrame, nameset:pandas.DataFrame):

  X = pulled
  M = nameset

  assert { 'id' }.issubset(X.index.names) and X.index.is_unique
  assert X.duplicated(['doc', 'value', 'city']).sum() == 0
  assert { 'doc', 'value', 'city', 
           'assignee', 'applicant', 'inventor' }.issubset(X.columns)

 #exact
  X = X.reset_index()
  E = X.set_index('value').join(M, how='inner')
  E = E.reset_index().set_index(['doc', 'id', 'city'])
  X = X.set_index(['doc', 'id', 'city']).drop(E.index)

 #split
  X['value'] = X['value'].apply(strnorm, dropinter=True, dropdigit=True)
  X['nword'] = X['value'].str.count(' ') + 1
  X['value'] = X['value'].str.split(' ')

 #word
  W = X.reset_index().explode('value')
  W = W.set_index('value').join(M[ M.isin(['firstname', 'lastname', 'ambigname']) ], how='inner')
  nW = W.groupby(['doc', 'id', 'city']).agg({'nword': 'first', 'role': 'count'})
  W = nW[ nW['nword'] == nW['role'] ][[]].join(W.reset_index().set_index(['doc', 'id', 'city']), how='inner')
  X = X.drop('nword', axis=1).drop(W.index)
  X['value'] = X['value'].apply(' '.join)
  W = W.drop('nword', axis=1)

  Y = pandas.concat([E, W]).reset_index().drop_duplicates(['doc', 'city', 'value'])

 #org
  O = Y[ Y['role'] == 'orgname' ]
  O = O.set_index('id').drop('role', axis=1)
  O['organisation'] = True

  assert { 'id' }.issubset(O.index.names)
  assert { 'organisation'  }.issubset(O.columns)

 #people
  agg = lambda X: X.agg({'value':' '.join, **{k:'max' for k in ['assignee', 'inventor', 'applicant'] }})
  P  = Y[ Y['role'] !=   'orgname' ].groupby(['doc', 'id', 'city']).pipe(agg)
  Nf = Y[ Y['role'] == 'firstname' ].groupby(['doc', 'id', 'city']).pipe(agg)['value'].rename('firstnames')
  Nl = Y[ Y['role'] ==  'lastname' ].groupby(['doc', 'id', 'city']).pipe(agg)['value'].rename('lastnames')
  P = P.join(Nf, how='left').join(Nl, how='left').reset_index().set_index('id')
  P['organisation'] = False

  assert { 'id' }.issubset(P.index.names)
  assert { 'organisation', 'firstnames', 'lastnames' }.issubset(P.columns)

  X = X.reset_index().set_index('id')
  X = X.loc[X.index.difference(P.index).difference(O.index)]
  Y = pandas.concat([P, O, X])

  assert { 'id' }.issubset(Y.index.names) and Y.index.is_unique
  assert { 'doc', 'value', 'firstnames', 'lastnames', 'city', 
           'organisation', 'assignee', 'applicant', 'inventor' }.issubset(Y.columns)

  return Y

@Flow.From()
def Spacetime(textual:pandas.DataFrame, 
              geoloc:pandas.DataFrame, 
              event:pandas.DataFrame, 
              clsf:pandas.DataFrame):

  X = textual

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

  assert { 'doc', 'lat', 'lon', 'delay', 'organisation' }.issubset(X.columns)
  assert any([ c.startswith('clsf-') for c in X.columns ])
  assert { 'id' }.issubset(X.index.names)

  return X

@Flow.From()
def Affilategeo(registry:pandas.DataFrame):

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
def Affilatenames(registry:pandas.DataFrame):

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
  X[['firstnames', 'lastnames', 'norm']] = \
    X[['firstnames', 'lastnames', 'norm']].fillna('').astype(str)
  X['nameset'] = (X['firstnames'] + ' ' + X['norm'] + ' ' + X['lastnames'])
  X['nameset'] = X['nameset'].str.strip().str.split()
  X['nameset'] = X['nameset'].apply(lambda x: frozenset([x[0], x[-1]]) if x else frozenset())
  O['nameset'] = O['norm'].apply(lambda x: frozenset({x}))
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

  z = X.nameset.apply(len) < 2
  if z.sum() > 0: Warning('(X.nameset.apply(len) < 2).sum() > 0')
  X = X[ z ]

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

class Entity:

  @Flow.From()
  def arrange(sim:pandas.DataFrame, all:pandas.DataFrame):

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

  def plot(entities:pandas.DataFrame):

    X = entities

    assert { 'id' }.issubset(X.index.names)
    assert { 'doc', 'lat', 'lon', 'organisation', 'entity' }.issubset(X.columns)

    f, A = plt.subplots(2, 1, tight_layout=True, gridspec_kw={'height_ratios': [1, 2]})

    nE = X.value_counts('entity').value_counts().sort_index()
    nE1 = nE.groupby(lambda x: x if x <= 1 else '1+').sum()
    nE1.plot.pie(title='Liczba rejestrów na 1 podmiot', 
                ax=A[0], ylabel='', autopct='%1.1f%%')
    nE = nE[nE.index != 1].groupby(lambda x: x if x <= 10 else '10+'
                                               if x <= 99 else '99+').sum()
    nE.plot.bar(title='Liczba rejestrów na 1 podmiot\ngdy podmiot występuje wielokrotnie', 
                ax=A[1], xlabel='ilość')

    return f

from util import data as D
from profiling import flow as f0
from patent import flow as fP

N0 = Nameclsf({ D['UPRP']+'/assignement.yaml':   f0['UPRP']['profiling'],
                D['Lens']+'/assignement.yaml':   f0['Lens']['profiling'],
                D['Google']+'/assignement.yaml': f0['Google']['profiling'] }).map('registry/names.pkl')

X = Pull(f0['UPRP']['profiling'], assignpath=D['UPRP']+'/assignement.yaml').map('registry/pulled.pkl')

N = Textual(X, nameset=N0).map('registry/textual.pkl')

GT = Spacetime(N, fP['UPRP']['patent-geoloc'], 
                          fP['UPRP']['patent-event'], 
                          fP['UPRP']['patent-classify']).map('registry/spacetime.pkl')

aG = Affilategeo(GT).map('registry/affilate-geo.pkl')
aN = Affilatenames(aG).map('registry/affilate.pkl')

S000 = Simcalc(aN, qcount='count  < 100').map('registry/sim-000.pkl')
S100 = Simcalc(aN, qcount='count >= 100').map('registry/sim-100.pkl')
S = Flow('Simcalc merge', lambda *x: pandas.concat(x), args=[S000, S100]).map('registry/sim.pkl')

E = Entity.arrange(sim=S, all=GT).map('registry/entity.pkl')

flow = { 'entity': E, 'rpull': X }