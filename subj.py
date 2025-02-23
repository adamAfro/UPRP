r"""
\section{Identyfikacja osób i uzupełnianie braków geolokalizacji}
"""

#lib
from lib.flow import Flow
import lib.flow, gloc, rgst

#calc
import pandas, geopandas as gpd
from pandas import DataFrame as DF
from geopandas import GeoDataFrame as GDF

#plot
import altair as Plot

@lib.flow.map('cache/subj/affilate-geo.pkl')
@lib.flow.init(rgst.selected)
def affilgeo(registers:pandas.DataFrame):

  r"""
  \D{defi}{Podobieństwo afiliacyjno-geolokalizacyjne $\tilde\gamma$}
  { Ilość identycznych geolokalizacji dla dwóch osób: $p_i$ oraz $p_j$:

  $$
  \tilde\gamma(p_i, p_j) = | \tilde G_i \cap \tilde G_j | \ge 0,
  $$

  gdzie $\tilde G_i$ to zbiór geolokalizacji osób w relacji współautorstwa z $p_i$.}
  """

  import tqdm
  tqdm.tqdm.pandas()

  X = registers
  assert { 'id' }.issubset(X.index.names) and X.index.is_unique
  assert { 'doc', 'lat', 'lon', 'organisation' }.issubset(X.columns)

  geoset = lambda g: set(tuple(x) for x in g[['lat', 'lon']].dropna().values)\
                                  if g['lat'].count() > 0 else frozenset()
  geoassign = lambda g: g.assign(geoaffil=[geoset(g)]*g.shape[0])
  Y = X.reset_index().groupby('doc').progress_apply(geoassign).set_index('id')

  assert { 'doc', 'geoaffil' }.issubset(Y.columns)
  assert { 'id' }.issubset(Y.index.names) and  Y.index.is_unique
  return Y

@lib.flow.map('cache/subj/affilate.pkl')
@lib.flow.init(affilgeo)
def affilnames(registry:pandas.DataFrame):

  r"""
  \D{defi}{Główna para imiennicza $\hat N_k$}{Zbiór 2-elementowy pierwszego 
  i ostatniego słowa ciągu imion i nazwiska. Założenie jest takie, że
  najistotniejesze imię albo nazwisko (część nazwiska wieloczłonowego) 
  znajduje się na początku, a koniec utożsamiany jest z nazwiskiem; 
  w skrajnej sytuacji może to być mało znaczące drugie imię. 
  Przyjęte jest, że są to przypadki bardzo rzadkie.}

  \D{defi}{Zbiór afiliacyjno-nazewniczy $k$-osoby $\tilde N_k$}{Zbiór imion, 
  nazwisk oraz słów, które mogą być imieniem lub nazwą; zawiera
  wyżej wymienione elementy, którymi identyfiują się osoby będące
  współautorami patentów razem z $k$-osobą.
  $$\tilde N_k \subset N_0$$}

  \D{defi}{Podobieństwo afiliacyjno-nazewnicze $\tilde \varphi$}{Dotyczy wpisów,
  które są związane z patentami badanej pary wpisów $w_i, w_j$.
  Zbiór $N_i$ jest zbiorem zbiorów głównych par imienniczych wpisów
  dotyczących patentu zawierającego wpis $w_i$. Analogicznie jest
  dla $N_j$:

  $$
  \tilde \varphi(p_i, p_j) = | \tilde N_i \cap \tilde N_j | \ge 0,\quad
  \tilde N_i = \{ \hat N_k \mid w_k \in W_i \land k \ne i \}
  $$}
  """

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

@lib.flow.map('cache/subj/sim.pkl')
@lib.flow.init(affilnames)
def simcalc(affilated:pandas.DataFrame):

  r"""
  \D{defi}{Zgodność nazewnicza}{występuje pod warunkiem, że
  suma zbiorów ich głównych par imienniczych jest im równa:
  $$
  \varphi(w_i, w_j) = \begin{cases}
    1 & \text{jeśli } \hat N_i = (\hat N_i \cap \hat N_j) = \hat N_j\\
    0 & \text{w przeciwnym przypadku}
  \end{cases}
  $$}

  \D{defi}{Zgodność geolokalizacyjna}{świadczy o tym, czy dwie osoby
  mają identyczne geolokalizacje:
  $$
  \gamma(p_i, p_j) = \begin{cases}
    1 & \text{jeśli } G_i = G_j\\
    0 & \text{w przeciwnym przypadku}
  \end{cases}
  $$

  gdzie $G_i$ to zbiór geolokalizacji przypisanej danej osobie.}

  \begin{uwaga}
  Wpisy zawierają nazwy miejscowości zameldowania, jednak wyszukwianie za ich
  pomocą odbywa się używając geolokalizacji patentowej.
  W związku z tym, mimo identycznej nazwy miejscowości może nie dojść do
  zgodności geolokalizacyjnej.
  \end{uwaga}

  \D{defi}{Zgodność klasyfikacyjna $\eta$}{Ilość identycznych sekcji klasyfikacji,
  w których znajdują się aplikacje patentowe dwóch osób: $p_i$ oraz $p_j$:
  $$\eta(p_i, p_j) = | C_i \cap C_j | \ge 0,$$
  gdzie $C_i$ to zbiór sekcji klasyfikacji dla osoby $p_i$.}

  \D{defi}{Niezgodność klasyfikacyjna $\hat\eta$}{Ilość różnych sekcji klasyfikacji,
  w których znajdują się aplikacje patentowe dwóch osób: $p_i$ oraz $p_j$:
  $$\hat\eta(p_i, p_j) = | C_i \setminus C_j | \ge 0,$$
  gdzie $C_i$ to zbiór sekcji klasyfikacji dla osoby $p_i$.}



  \subsubsection{Przebieg wyszukiwania}

  \begin{uwaga}
  Przyjmujemy uproszczenie: zakładamy, że dana osoba w różnych patentach 
  jest podpisana w jednolity sposób.
  \end{uwaga}

  Pierwszym etapem wyszukiwania jest zawężenie zakresu wyłącznie do relacji
  spełniających warunek $\varphi(p_i, p_j) = 1$.
  W efekcie otrzymujemy zbiór $W_0$:

  $$W_0 = \{ (p_i, p_j)\mid \varphi(p_i, p_j) = 1 \}$$

  Kolejnym etapem jest wyznaczenie wartości zgodności klasyfikacji oraz 
  podobieństw afiliacyjnych.

  $$W_1 = \{ ( \eta(p_i, p_j), \tilde \gamma(p_i, p_j), \tilde \varphi(p_i, p_j) ) \mid (p_i, p_j) \in W_0 \}$$
  """

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

@lib.flow.map('cache/subj/entity.pkl')
@lib.flow.init(simcalc, rgst.selected)
def identify(sim:pandas.DataFrame, all:pandas.DataFrame):

  r"""
  Zbiór $W_1$ dzielimy na dwa podzbiory zgodnie z wartościami zgodności lokalizacyjnej:

  $$
  W_2 = \{ ( w \mid w \in W_1, \gamma(p_i, p_j) = 0 \}\qquad 
  W_3 = \{ ( w \mid w \in W_1, \gamma(p_i, p_j) = 1 \}
  $$

  Oba zbiory rozważamy jako oddzielne przypadki ze względu na ich
  gruntownie różną naturę. Przyjmując pewne stałe jako wartości
  graniczne dla zgodności klasyfikacji oraz podobieństw afiliacyjnych
  możemy podjąć decyzję o zgodności dwóch wpisów pod względem
  opisywania jednej osoby.

  Dla $W_2$:

  $$\neg \big(\hat\eta > 0 \land \eta = 0\big) \land \big(\tilde\varphi(w_i, w_j) > 0 \lor \tilde\gamma(p_i, p_j) > 0\big)$$

  Dla $W_3$:

  $$\neg \big(\hat\eta > 0 \land \eta = 0\big) \land \big(\tilde\varphi(w_i, w_j \big) > 2$$

  Na podstawie grafu tych zgodności
  identyfikujemy spójne składowe. 
  Każda składowa jest identyfikuje osobę,
  gdzie każdy wierzchołek wynika
  z procesu patentowania w jakim brała udział.
  """

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

@lib.flow.placeholder()
def fillgeo(identified:pandas.DataFrame, group:str, loceval:str):

  """
  \subsection{Uzupełnianie braków geolokalizacji}

  Pierwszym etapem uzupełniania braków geolokalizacji jest
  przypisanie brakujących miast na podstawie identyfikacji:
  jesli osoba $a$ ma przypisane miasto $x$ w związku z patentem
  $p_1$, to w przypadku braku miasta w związku z patentem $p_2$
  przypisujemy miasto $x$. Gdy miast jest kilka, wybierane jest
  najczęściej występujące.
  W przypadkach w których to nie poskutkowało przypisaniem miasta,
  przypisujemy miasto na podstawie najczęściej występującego miasta
  w ramach osób pracujących nad danym patentem.
  """

  import tqdm

  assert group in identified.columns

  E = identified
  G = E.groupby(group)

  for g, G in tqdm.tqdm(G, total=G.ngroups):

    n = G[['lat', 'lon']].value_counts()
    if n.empty: continue

    m = n.idxmax()
    E.loc[G.index, 'loceval'] = E.loc[G.index, 'loceval'].fillna(loceval)
    E.loc[G.index, 'lat'] = G['lat'].fillna(m[0])
    E.loc[G.index, 'lon'] = G['lon'].fillna(m[1])

  return E

geofilled0 = fillgeo(identified=identify, group='entity', loceval='identity')
geofilled = fillgeo(identified=geofilled0, group='doc', loceval='document').map('cache/subj/filled.pkl')

@lib.flow.map('cache/subj/mapped.pkl')
@lib.flow.init(geofilled, gloc.region[1], gloc.region[2])
def mapped(filled:DF, woj:GDF, pow:GDF):

  X = filled

  G = GDF(X.reset_index(), geometry=gpd.points_from_xy(X.lon, X.lat, crs='EPSG:4326'))

  G = gpd.sjoin(G, woj[['geometry', 'gid']], how='left', predicate='within')
  G = GDF(G, geometry='geometry').drop(columns=['index_right'])
  G = G.rename(columns={ 'gid': 'wgid' })

  G = gpd.sjoin(G, pow[['geometry', 'gid']], how='left', predicate='within')
  G = GDF(G, geometry='geometry').drop(columns=['index_right'])
  G = G.rename(columns={ 'gid': 'pgid' })

  return G