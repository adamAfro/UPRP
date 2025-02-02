import pandas, numpy, geopandas as gpd, altair as Plot, altair as Pt
from lib.flow import Flow
import gloc, endo, raport as rprt
from util import A4

@Flow.From()
def graph(edgdocs:pandas.DataFrame, 
          edgaffil:pandas.DataFrame,
          dist:pandas.DataFrame):

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
  E = E.join(d)

  E['closeness'] = 1 / (E['distance']+1)

  return E

@Flow.From()
def findcomp(nodes:pandas.DataFrame, edges:pandas.DataFrame):

  import networkx as nx

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

  N['degree'] = pandas.DataFrame(nx.degree(G)).set_index(0)[1]
  N['closeness'] = pandas.Series(nx.closeness_centrality(G, distance='weight'))

  return N

@Flow.From()
def statcomp(nodes:pandas.DataFrame, edges:pandas.DataFrame):

  N = nodes
  E = edges

  C = N.value_counts('comp').rename('nodes').to_frame()
  C['meandegree'] = N.groupby('comp')['degree'].mean()
  C['meancloseness'] = N.groupby('comp')['closeness'].mean()
  C['meandist'] = E.set_index('id').join(N['comp']).groupby('comp')['distance'].mean()

  return C

edges = graph(rprt.valid, endo.data, gloc.dist)
nodes = findcomp(endo.data, edges)
comps = statcomp(nodes, edges).map('cache/compstat.pkl')

plots = dict()

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

  X .query('meandist > 0')\
    .pipe(Pt.Chart).mark_bar()\
    .properties(width=0.4*A4.W, height=0.1*A4.H)\
    .encode(Pt.X('meandist:Q').title('Średni dystans krawędzi (d >1)').bin(step=10),
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

  )(X.reset_index()[['lat', 'lon', 'latY', 'lonY', 'distance', 'closeness']]))

for k, F in plots.items():
  F.name = k
  F.map(f'fig/grph/{k}.png')