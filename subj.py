r"""
\section{Identyfikacja osób}

Głownym problemen danych jest niejednoznaczność w kontekście identyfikacji osób.
W danych patentowych, osoby rozróżnia się za pomocą imienia, nazwiska
oraz nazwy miejscowości. Jak wiadomo wiele osób może mieć te same imię i nazwisko,
także w jednym miejscu. Jest to duże ograniczenie wynikające z samego zbioru danych.
Należy także wspomnieć o drobnych niespójnościach danych w zapisie imion i nazwisk
(\cref{def:drobne-niespójności}) --- występowanie diaktryk i akcentów w zapisie
nie jest gwarantowane, a jednocześnie nie jest wykluczone.

Kolejną niejednoznacznością jest podobne zjawisko dla nazw miejscowości.
W Polsce jest wiele miejscowości o identycznych nazwach, a rejestry nie oferują
nic po za samą nazwą. Tutaj także występuje problem z diaktrykami i akcentami.

Ponadto występują też 2 inne problemy. Pierwszym jest 
niespójność fragmentacji danych (\cref{def:niespójność-fragmentacji}).
W przypadku tabeli z danymi osobowymi wynalazców są do dyspozycji ich
imiona i nazwiska. W przypadku pozostałych osób związanych z patentem
są to najczęściej ciągi imion i nazwisk. Nie jest jednak gwarantowane,
że dotyczą one osób fizycznych. Drugim problemem jest niespójność 
typów danych (\cref{def:niespójność-typów}). Część danych oznaczonych
jako imiona dotyczy nazw firm lub instytucji. Oznaczenie tego faktu
istnieje tylko w niektórych przypadkach, dużo częściej jest to pominięte.

\begin{uwaga}
Dane w zbiorach uwzględniają różną szczegółowość w zapisie imion i nazwisk.
Niektóre zawierają drugie imię, niektóre wyłącznie literę drugiego imienia.
Przypadków jest wiele. Poniższe podejście pomija tę ambiwalencję.
\end{uwaga}
"""

#lib
from lib.flow import Flow
import gloc

#calc
import pandas, geopandas as gpd

#plot
import altair as Plot

@Flow.From()
def affilgeo(registry:pandas.DataFrame):

  """
  \begin{defi}\label{defi:podobieństwo-af-geo}
  Podobieństwo afiliacyjno-geolokalizacyjne $\tilde\gamma$ --- ilość identycznych geolokalizacji
  dla dwóch osób: $p_i$ oraz $p_j$:

  $$
  \tilde\gamma(p_i, p_j) = | \tilde G_i \cap \tilde G_j | \ge 0,
  $$

  gdzie $\tilde G_i$ to zbiór geolokalizacji osób w relacji współautorstwa z $p_i$.
  \end{defi}
  """

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

  r"""
  \begin{defi}
  Główna para imiennicza $\hat N_k$ --- zbiór 2-elementowy pierwszego 
  i ostatniego słowa ciągu imienniczego
  \end{defi}

  \begin{defi}
  Zbiór afiliacyjno-nazewniczy $k$-osoby $\tilde N_k$ - zbiór imion, 
  nazwisk oraz słów, które mogą być imieniem lub nazwą; zawiera
  wyżej wymienione elementy, którymi identyfiują się osoby będące
  współautorami patentów razem z $k$-osobą.
  $$\tilde N_k \subset N_0$$
  \end{defi}

  \begin{defi}\label{defi:podobieństwo-af-nazw}
  Podobieństwo afiliacyjno-nazewnicze $\tilde \varphi$ --- dotyczy wpisów,
  które są związane z patentami badanej pary wpisów $w_i, w_j$.
  Zbiór $N_i$ jest zbiorem zbiorów głównych par imienniczych wpisów
  dotyczących patentu zawierającego wpis $w_i$. Analogicznie jest
  dla $N_j$:

  $$
  \tilde \varphi(p_i, p_j) = | \tilde N_i \cap \tilde N_j | \ge 0,\quad
  \tilde N_i = \{ \hat N_k \mid w_k \in W_i \land k \ne i \}
  $$
  \end{defi}
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

@Flow.From('calculating similarity')
def simcalc(affilated:pandas.DataFrame):

  r"""
  \begin{defi}\label{defi:zgodność-nazw}
  Zgodność nazewnicza 2 wpisów $w_i,w_j$ występuje pod warunkiem, że
  elementy suma zbiorów ich głównych par imienniczych jest im równa:
  $$
  \varphi(w_i, w_j) = \begin{cases}
    1 & \text{jeśli } \hat N_i = (\hat N_i \cap \hat N_j) = \hat N_j\\
    0 & \text{w przeciwnym przypadku}
  \end{cases}
  $$
  \end{defi}

  \begin{defi}\label{defi:zgodność-geolokalizacyjna}
  Zgodność geolokalizacyjna:
  $$
  \gamma(p_i, p_j) = \begin{cases}
    1 & \text{jeśli } G_i = G_j\\
    0 & \text{w przeciwnym przypadku}
  \end{cases}
  $$
  gdzie $G_i$ to zbiór geolokalizacji przypisanej danej osobie.
  \end{defi}

  \begin{uwaga}
  Wpisy zawierają nazwy miejscowości zameldowania, jednak wyszukwianie za ich
  pomocą odbywa się po geolokalizacji patentowej.
  W związku z tym, mimo identycznej nazwy miejscowości może nie dojść do
  zgodności geolokalizacyjnej.
  \end{uwaga}

  Kolejnym determinantem jest klasyfikacja. \Cref{wniosek:klasyfikacje-deter-1}
  pokazuje, że klasyfikacje nie są dobrym determinantem jeśli opierać by się wyłącznie 
  na nich. Warto jednak zauważyć, że klasyfikacje mogą być dobrym uzupełnieniem
  dla pozostałych determinantów.

  \begin{defi}\label{defi:zgodność-clsf}
  Zgodność klasyfikacyjna $\eta$ --- ilość identycznych sekcji klasyfikacji,
  w których znajdują się aplikacje patentowe dwóch osób: $p_i$ oraz $p_j$:
  $$\eta(p_i, p_j) = | C_i \cap C_j | \ge 0,$$
  gdzie $C_i$ to zbiór sekcji klasyfikacji dla osoby $p_i$.
  \end{defi}

  \begin{uwaga}
  Zgodność $\varphi$ (\cref{defi:zgodność-nazw}) oraz podobieństwo $\tilde \varphi$ 
  (\cref{defi:podobieństwo-af-nazw}) pomija rozróżnienie na imiona i nazwiska.
  \end{uwaga}



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

@Flow.From()
def identify(sim:pandas.DataFrame, all:pandas.DataFrame):

  """
  Zbiór $W_1$ dzielimy na dwa podzbiory zgodnie z wartościami zgodności lokalizacyjnej:

  $$
  W_2 = \{ ( w \mid w \in W_1, \gamma(p_i, p_j) = 0 \}\qquad 
  W_3 = \{ ( w \mid w \in W_1, \gamma(p_i, p_j) = 1 \}
  $$

  \begin{uwaga}
  Zgodność lokalizacyjną można zastąpić miarą odległości geograficznej,
  jednak na potrzeby uproszczenia jest to wartość binarna.
  \end{uwaga}

  Oba zbiory rozważamy jako oddzielne przypadki ze względu na ich
  gruntownie różną naturę. Przyjmując pewne stałe jako wartości
  graniczne dla zgodności klasyfikacji oraz podobieństw afiliacyjnych
  możemy podjąć decyzję o zgodności dwóch wpisów pod względem
  opisywania jednej osoby. Na podstawie grafu tych zgodności
  identyfikujemy spójne składowe. Każda taka część grafu to
  zbiór wpisów, które opisują tę samą osobę.
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

@Flow.From()
def fillgeo(entities:pandas.DataFrame, group:str, loceval:str):

  """
  \subsection{Uzupełnianie braków geolokalizacji za pomocą innych danych}

  Brak danych dotyczących położenia osób związanych z danym patentem jest
  istotnym problemem w analizie dyfuzji przestrzennej. Pominięcie obserwacji
  z powodu braku danych może prowadzić do błędnych wniosków. 
  Determinacja położenia osób za pomocą innych danych patentowych jest
  więc kluczowa.

  W danych można wyróżnić 3 przypadki dostępności geolokalizacji.

  \begin{przyp}\label{przyp:brak-geo-0}
  Geolokalizacja jest dostępna dla każdej osoby związanej z patentem.
  \end{przyp}

  \begin{przyp}\label{przyp:brak-geo-n}
  Geolokalizacja jest dostępna dla części osób zwiazanych z patentem.
  \end{przyp}

  \begin{przyp}\label{przyp:brak-geo-N}
  Geolokalizacja nie jest dostępna dla żadnej osoby związanej z patentem.
  \end{przyp}

  W przypadku \ref{przyp:brak-geo-0} nie ma potrzeby uzupełniania danych.
  W przypadkach \ref{przyp:brak-geo-n} i \ref{przyp:brak-geo-N} 
  rozważamy sytuację po identyfiakcji za pomocą podobieństw. 
  Jeśli dany wpis osoby jest przypisane osobie, 
  która w innych wpisach ma geolokalizacje, to jest ona uzupełniana
  najczęściej powtarzającą sie geolokalizacją.

  Efektem jest zmiana części przypadków z \ref{przyp:brak-geo-N} na
  \ref{przyp:brak-geo-n}, potencjalnie także na \ref{przyp:brak-geo-0}.
  W takiej sytuacji należy powtórzyć czynność wyszukiwania podobieństw ---
  dane zostały uzupełnione o geolokalizację, więc potencjalnie można
  oczekiwać znalezienia nowych relacji identyczności.
  Te 2 kroki należy powtarzać dopóki dają efekt. Ostatecznie
  przypadki, które pozostały przypadkami \ref{przyp:brak-geo-N}
  nie mogą zostać uzupełnione wiarygodnymi metodami.
  W przypadkach \ref{przyp:brak-geo-n} można zastosować wyróżnianie
  najczęściej powtarzanego miejsca związania pośród innych osób i przyjąć
  je za geolokalizację osób bez niej.
  """

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

from rgst import flow as f0

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

      .mark_bar().encode( Plot.X(f'count:Q').title(None),
                          Plot.Color('loceval:N')\
                              .title('Metoda geolokalizacji')\
                              .legend(orient='bottom', columns=1),
                          Plot.Y('IPC:N').title('Klasyfikacja')))

plots[f'F-geoloc-eval'] = Flow(args=[mapped], callback=lambda X:

  Plot.Chart(X.assign(year=X['grant'].dt.year.astype(int))\
              .replace({'unique': 'jednoznaczna',
                        'proximity': 'najlbiższa innym',
                        'document': 'npdst. współautorów',
                        'identity': 'npdst. tożsamości' })\
             .value_counts(['year', 'loceval']).reset_index())

      .mark_bar().encode( Plot.Y('year:O').title('Rok'),
                          Plot.X('count:Q').title(None),
                          Plot.Color('loceval:N')\
                              .title('Metoda geolokalizacji')\
                              .legend(orient='bottom', columns=2)))


for k, F in plots.items():
  F.name = k
  F.map(f'fig/subj/{k}.png')