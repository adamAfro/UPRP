import pandas, numpy, geopandas as gpd, altair as Plot, altair as Pt, networkx as nx
from lib.flow import Flow
import lib.flow, gloc, endo, raport as rprt
from util import A4

@Flow.From()
def graph(edgdocs:pandas.DataFrame, 
          edgaffil:pandas.DataFrame,
          dist:pandas.DataFrame):

  """
    \subsubsection
  {Tworzenie grafu na podstawie raportów o stanie techniki}

  Graf $G$ jest grafem skierowanym o krawędziach $E$ i węzłach
  będących osobami pełniącymi role patentowe.

  Jest stworzony na podstawie raportów o stanie techniki
  składa się z 2 rodzajów węzłów: patentów i osób pełniących 
  role patentowe.

  Pierwszym etapem jest utworzenie krawędzi $E_r$ między dokumentami.
  Każdy raport dotyczy jednego patentu, a może odwoływać się 
  do wielu innych. Odwołanie umieszczone w raporcie do innego
  patentu traktujemy jako krawędź w grafie skierowanym $G_r$.
  Kierunek grafu jest zgodny z przepływem informacji, co znaczy,
  że patent którego dotyczy raport jest węzłem końcowym,
  a wszystkie patenty wymienione w raporcie są węzłami początkowymi.

  Drugim etapem jest utworzenie krawędzi $E_x$ między osobami,
  a dokumentami. Najpierw krawędzie skierowane są tworzone 
  w kierunku dokumentów, które były wymieniane w raportach.
  Następnie krawędzie $E_y$ z patentów będących przedmiotami 
  raportów są tworzone w kierunku osób, które są ich autorami.

  Ostatecznie krawędzie $E$ grafu powstają jeśli istnieje ściezka
  między 2 wierzchołkami reprezentującymi osoby. Zgodnie z kierunkiem
  grafu, te krawędzie są skierowane, a ich zwrot reprezentuje kierunek
  przepływu wiedzy.
  """

  edgdocs = rprt.valid()
  edgaffil = endo.data()
  dist = gloc.dist()

  A = edgaffil.reset_index()

  D = edgdocs
  D['edge'] = range(len(D))

  X = A.set_index('doc').join(D.set_index('from'), how='inner')
  Y = A.set_index('doc').join(D.set_index('to'), how='inner')
  E = X.set_index('edge').join(Y.set_index('edge'), rsuffix='Y', how='inner').reset_index()

  E = E.set_index(['lat', 'lon', 'latY', 'lonY'])

  d = dist.reset_index().melt(id_vars=[('lat', ''), ('lon', '')], value_name='distance')
  d.columns = ['lat', 'lon', 'latY', 'lonY', 'distance']
  d = d.set_index(['lat', 'lon', 'latY', 'lonY'])
  E = E.join(d).reset_index()

  E['closeness'] = 1 / (E['distance']+1)
  E['Gdelay'] = (E['grantY'] - E['grant']).dt.days.astype(int)
  E['Adelay'] = (E['applicationY'] - E['application']).dt.days.astype(int)

  E = E[E['Adelay'] > 0]

 #Jaccard
  JX = E.apply(lambda x: frozenset([l for l in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H'] if x[f'clsf-{l}'] > 0]), axis=1)
  JY = E.apply(lambda x: frozenset([l for l in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H'] if x[f'clsf-{l}Y'] > 0]), axis=1)
  E['Jaccard'] = JX.to_frame().join(JY.rename(1)).apply(lambda x: len(x[0] & x[1]) / len(x[0] | x[1]), axis=1)

  return E

@Flow.From()
def findcomp(nodes:pandas.DataFrame, edges:pandas.DataFrame):

  N = nodes
  E = edges

  N = N.set_index('id')

  G = nx.Graph()
  G.add_nodes_from(N.index)
  G.add_weighted_edges_from(E[['id', 'idY', 'distance']].values)

 #komponenty
  C = pandas.Series(nx.connected_components(G)).explode()
  C = C.reset_index().set_index(0)['index'].rename('comp')
  N = N.join(C)

  return N

@Flow.From()
def statcomp(nodes:pandas.DataFrame, edges:pandas.DataFrame):

  N = nodes
  E = edges

  C = N.value_counts('comp').rename('nodes').to_frame()
  C['meandegree'] = N.groupby('comp')['degree'].mean()
  C['meancloseness'] = N.groupby('comp')['closeness'].mean()
  C['meandist'] = E.set_index('id').join(N['comp']).groupby('comp')['distance'].mean()
  C['Gdelay'] = E.set_index('id').join(N['comp']).groupby('comp')['Gdelay'].mean()
  C['Adelay'] = E.set_index('id').join(N['comp']).groupby('comp')['Adelay'].mean()

  return C

@Flow.From()
def aggppl(nodes:pandas.DataFrame, edges:pandas.DataFrame):

  X = nodes
  E = edges

 #klasyfikacja per patent
  for l in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']:
    X[f'Npat{l}'] = (X[f'clsf-{l}'] > 0).astype(int)

  X['gloc'] = X[['lat', 'lon']].apply(tuple, axis=1)
  X = X.groupby('entity')\
       .agg({ 'gloc': pandas.Series.mode,

              **{ f'Npat{l}': 'sum' for l in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H'] } })

  X['lat'] = X['gloc'].apply(lambda x: x[0])
  X['lon'] = X['gloc'].apply(lambda x: x[1])

  G = nx.Graph()
  G.add_nodes_from(X.index)
  G.add_weighted_edges_from(E[['entity', 'entityY', 'distance']].values)

  X['degree'] = [G.degree(n) for n in X.index]
  X['closeness'] = [1 / (G.degree(n)+1) for n in X.index]

 #entropia: sumarycznie p(x) log(p(x))
  T = X[['NpatA', 'NpatB', 'NpatC', 'NpatD', 'NpatE', 'NpatF', 'NpatG', 'NpatH']]\
        .apply(lambda x: x/x.sum())\
        .map(lambda x: - x*numpy.log(x) if x > 0 else 0)\
        .sum(axis=1)

  X['entropy'] = T
  X = X.drop(columns=['gloc'])

  return X

@lib.flow.make()
def GIDassign(X:pandas.DataFrame):

  """
  \textbf{Przyporządkowanie unikalnych identyfikatorów do wierzchołków grafu} ---
  działanie optynalizacyjne, które ogranicza liczbę operacji na danych.
  """

  A = X[['lat', 'lon']].drop_duplicates()
  B = X[['latY', 'lonY']].drop_duplicates()
  B = B.rename(columns={'latY': 'lat', 'lonY': 'lon'})
  I = pandas.concat([A, B]).drop_duplicates()
  I['gid'] = range(0, len(I))
  I = I.set_index(['lat', 'lon'])['gid']

  X = X.merge(I, left_on=['lat', 'lon'], right_index=True, how='left')

  I = I.rename('gidY')
  X = X.merge(I, left_on=['latY', 'lonY'], right_index=True, how='left')

  return X

edges0 = graph(rprt.valid, endo.data, gloc.dist).map('cache/edges.pkl')
edges = GIDassign(edges0)
nodes0 = findcomp(endo.data, edges)
comps = statcomp(nodes0, edges).map('cache/compstat.pkl')
nodes = aggppl(endo.data, edges)

plots = dict()

plots['F-nodes'] = lib.flow.forward(nodes, lambda X:

  X[['degree']]\
    .pipe(Pt.Chart).mark_bar()\
    .properties(width=0.4*A4.W, height=0.2*A4.H)
    .encode(Pt.X('degree:Q')\
              .title('Stopień węzła')\
              .bin(step=10),
            Pt.Y('count(degree)')\
              .scale(type='log')\
              .title('Ilość węzłów o danym stopniu (skala logarytmiczna)')) &\

  X[['entropy']]\
    .pipe(Pt.Chart).mark_bar()\
    .properties(width=0.4*A4.W, height=0.1*A4.H)
    .encode(Pt.X('entropy:Q').bin(step=0.01)\
              .title('Poziom entropii sekcji klasyfikacji'),
            Pt.Y('count(entropy)')\
              .scale(type='log')\
              .title('(Skala logarytmiczna)')) &\

  X[['closeness']]\
    .pipe(Pt.Chart).mark_bar()\
    .properties(width=0.4*A4.W, height=0.1*A4.H)
    .encode(Pt.X('closeness:Q').bin(step=0.01)\
              .title('Bliskość w grafie'),
            Pt.Y('count(closeness)')\
              .scale(type='log')\
              .title('(Skala logarytmiczna)'))

)

plots['F-edges'] = lib.flow.forward(edges, lambda X:(

  lambda X:

    ( X .pipe(Pt.Chart).mark_circle()\
        .properties(width=0.05*A4.W, height=0.05*A4.H)\
        .encode(Pt.Y('idgeo:N').title(None),
                Pt.Size('count(idgeo)').legend(None)) |\
      X .pipe(Pt.Chart).mark_text()\
        .properties(width=0.05*A4.W, height=0.05*A4.H)\
        .encode(Pt.Y('idgeo:N').axis(None),
                Pt.Text('count(idgeo)')) ) &\

    X .query('distance > 0')\
      .pipe(Pt.Chart).mark_bar()\
      .properties(width=0.4*A4.W, height=0.1*A4.H)\
      .encode(Pt.X('distance:Q')\
                .title('Niezerowy dystans [km]')\
                .bin(step=10),
              Pt.Y('count(distance)')\
                .title(None)) &\

    X .pipe(Pt.Chart).mark_bar()\
      .properties(width=0.4*A4.W, height=0.1*A4.H)\
      .encode(Pt.X('Adelay:Q')\
                .title('Okres między składaniem aplikacji patentowych [dni]')\
                .bin(step=100),
              Pt.Y('count(Adelay)')\
                .title(None)) &\

   ( X .pipe(Pt.Chart).mark_circle()\
      .properties(width=0.4*A4.W, height=0.025*A4.H)\
      .encode(Pt.Size('count(Jaccard)').title(None),
              Pt.X('Jaccard:Q')\
                .title(['Liczność krawędzi o danym', 
                        'indeksie Jaccarda dla klasyfikacji'])) +\

    X .pipe(Pt.Chart).mark_text(yOffset=-0.01*A4.H)\
      .properties(width=0.4*A4.W, height=0.025*A4.H)\
      .encode(Pt.X('Jaccard:Q'),
              Pt.Text('count(Jaccard)')) )

) (X [['distance', 'Jaccard', 'Adelay']]\
      .eval('idgeo = distance > 0')\
      .replace({'idgeo': {True: 'd > 0', 
                          False: '0-wy dystans'}})) )

plots[f'F-rprt-comp'] = Flow.Forward([comps], lambda X:
(
  ( lambda S:
    Pt.Chart(S).mark_circle()\
      .properties(width=0.05*A4.W, height=0.05*A4.H)\
      .encode(Pt.Y('multi:N').title(None),
              Pt.Size('count(multi)').legend(None)) |\
    Pt.Chart(S).mark_text()\
      .properties(width=0.05*A4.W, height=0.05*A4.H)\
      .encode(Pt.Y('multi:N').axis(None),
              Pt.Text('count(multi)'))\

  )(X .eval('multi = nodes > 1')\
      .replace({'multi': {True: 'n-węzłów',
                          False: '1 węzeł'}})) |\

  ( lambda S:
    Pt.Chart(S).mark_circle()\
      .properties(width=0.05*A4.W, height=0.05*A4.H)\
      .encode(Pt.Y('idgeo:N').title(None),
              Pt.Size('count(idgeo)').legend(None)) |\
    Pt.Chart(S).mark_text()\
      .properties(width=0.05*A4.W, height=0.05*A4.H)\
      .encode(Pt.Y('idgeo:N').axis(None),
              Pt.Text('count(idgeo)'))\

  )(X .eval('idgeo = meandist > 0')\
      .replace({'idgeo': {True: 'd > 0', 
                          False: '0-wy dystans'}}))
) &\

  X .query('nodes > 1')\
    .pipe(Pt.Chart).mark_bar()\
    .properties(width=0.4*A4.W, height=0.1*A4.H)\
    .encode(Pt.X('nodes:Q').title('Liczba węzłów (n >1)').bin(step=5),
            Pt.Y('count(nodes)').title(None)) &\

  X .query('nodes > 1')\
    .pipe(Pt.Chart).mark_bar()\
    .properties(width=0.4*A4.W, height=0.1*A4.H)\
    .encode(Pt.X('meandegree:Q').title('Średni stopień węzła (n >1)').bin(step=3),
            Pt.Y('count(meandegree)').title(None)) &\

  X .query('nodes > 1')\
    .pipe(Pt.Chart).mark_bar()\
    .properties(width=0.4*A4.W, height=0.1*A4.H)\
    .encode(Pt.X('Adelay:Q').title('Dni między składaniem aplikacji').bin(step=100),
            Pt.Y('count(Adelay)').title(None)) &\

  X .query('nodes > 1')\
    .pipe(Pt.Chart).mark_bar()\
    .properties(width=0.4*A4.W, height=0.1*A4.H)\
    .encode(Pt.X('Gdelay:Q').title('Dni między ochroną patentową').bin(step=100),
            Pt.Y('count(Gdelay)').title(None)) &\

  X .query('meandist > 0')\
    .pipe(Pt.Chart).mark_bar()\
    .properties(width=0.4*A4.W, height=0.1*A4.H)\
    .encode(Pt.X('meandist:Q').title('Średni dystans krawędzi (d > 0)').bin(step=10),
            Pt.Y('count(meandist)').title(None)) )

plots[f'M-rprt-dist'] = Flow(args=[edges, gloc.region[1]], callback=lambda X, G: (

  lambda X, G=G:

    Plot.vconcat(

      Plot.Chart(G).mark_geoshape(stroke='black', fill=None) + \

      X .query(f'distance == 0')\
        .pipe(Plot.Chart).mark_circle(fill='green')\
        .properties(width=0.5*A4.W, height=0.3*A4.H)\
        .encode(Plot.Latitude('lat'), 
                Plot.Longitude('lon'),
                Plot.Size('count()')\
                    .legend(orient='top')\
                    .title('Ilość połączeń w tej samej geolokalizacji')), *[

      Plot.Chart(G).mark_geoshape(stroke='black', fill=None) + \

      X .query(f'{start} < distance <= {end}')\
        .pipe(Plot.Chart, title=f'{start} - {end} km').mark_rule(opacity=0.1)\
        .properties(width=0.5*A4.W, height=0.3*A4.H)\
        .encode(Plot.Color('distance:Q')\
                    .title('Dystans [km]')\
                    .scale(scheme='greens', reverse=True)\
                    .legend(orient='bottom'),
                Plot.Latitude('lat'),
                Plot.Longitude('lon'),
                Plot.Latitude2('latY'),
                Plot.Longitude2('lonY')).project('mercator')

        for start, end in [(0, 100), (100, 200)]]) | \

    Plot.vconcat(*[

      Plot.Chart(G).mark_geoshape(stroke='black', fill=None) + \

      X .query(f'{start} < distance <= {end}')\
        .pipe(Plot.Chart, title=f'{start} - {end} km').mark_rule(opacity=0.1)\
        .properties(width=0.5*A4.W, height=0.3*A4.H)\
        .encode(Plot.Color('distance:Q')\
                    .title('Dystans [km]')\
                    .scale(scheme='greens', reverse=True)\
                    .legend(orient='bottom'),
                Plot.Latitude('lat'),
                Plot.Longitude('lon'),
                Plot.Latitude2('latY'),
                Plot.Longitude2('lonY')).project('mercator')

        for start, end in [(200, 300), (300, 450), (450, 800)]])

  )(X[['lat', 'lon', 'latY', 'lonY', 'distance', 'closeness']]))

for k, F in plots.items():
  F.name = k
  F.map(f'fig/grph/{k}.png')