r"""
\section{Dyfuzja wiedzy innowacyjnej w powiatach}
"""

#lib
import lib.flow, grph, gloc

#calc
from pandas import DataFrame as DF, Series as Se
from geopandas import GeoDataFrame as GDF
from scipy.cluster.hierarchy import linkage, fcluster

#plot
import altair as Pt
from util import A4

@lib.flow.map('fig/difp/M-x.pdf')
@lib.flow.init(grph.network[0], gloc.region[2], gloc.region[0])
def xmap(edges:DF, regions:GDF, borders:GDF):

  r"""
  \newpage
  \chart{fig/difp/M-x.pdf}
  { Mapa powiatów z zaznaczoną ilością osób, które zostały cytowane }

  Lokalne generatory wiedzy 
    można wskazać 
    szczególnie 
      w powiecie Warszawskim oraz 
      Wrocławskim. 
  Patenty z tych regionów 
    są szczególnie licznie cytowane
      w stosunku do patentów z pozostałych regionów.
  Jest to zjasiwko 
    relatywnie 
    stałe w czasie.
  Można zaznaczyć, że rok 2012 
    był szczególny --- 
      patenty z tych regionów
        są najczęściej cytowane
          w badanym okresie.
  Co ciekawe 
    nawet bardziej niż w 2011.
  """

  E0 = edges
  R = regions
  B = borders

 #ograniczenie danych
  E0 = E0.query('(year >= 2011) & (year < 2020)')
  R.geometry = R.geometry.simplify(0.02, preserve_topology=True)
  B.geometry = B.geometry.simplify(0.02, preserve_topology=True)

 #grupowanie krawędzi
  gE = E0.groupby(['year', 'pgid'])
  E = gE.size().rename('size').reset_index()
  E = E.set_index('pgid').join(R.set_index('gid')).reset_index()
  E = GDF(E, geometry='geometry')
  E = E.sort_values(['size'], ascending=False)
  E['large'] = E['size'] > 1000

 #segregowanie
  K = E['year'].unique()
  S = Se([E.query(f'year == {k}').drop(columns='year') for k in K], index=K)

 #wymiary
  C = ['white']+[x for c in ['grey', 'yellow', 'orange', 'red', 'violet', 'blue', 'darkblue'] for x in [c,c]]
  c = Pt.Color('size', type='quantitative')
  c = c.scale(range=C, domainMin=0)
  c = c.title('Ilość osób').legend(orient='bottom')

  xL = Pt.Latitude('lat', type='quantitative')
  yL = Pt.Longitude('lon', type='quantitative')
  tL = Pt.Text('name', type='nominal')

 #wykresy
  m0 = Pt.Chart(B).mark_geoshape(fill=None, stroke='black', strokeWidth=0.001*A4.W).project('mercator')
  M = S.map(lambda m: Pt.Chart(m).mark_geoshape().encode(c).project('mercator')).sort_index()
  M = M.map(lambda m: (m0 + m).properties(width=0.5*A4.W, height=0.5*A4.W))
  M = M.reset_index().apply(lambda m: m[0].properties(title=f'Rok {m["index"]}'), axis=1)
  m = Pt.concat(*M.values.tolist(), columns=3, spacing=0)

  return m

@lib.flow.map('fig/difp/M-y.pdf')
@lib.flow.init(grph.network[0], gloc.region[2], gloc.region[0])
def ymap(edges:DF, regions:GDF, borders:GDF):

  r"""
  \newpage
  \chart{fig/difp/M-y.pdf}
  { Mapa powiatów z zaznaczoną ilością osób, które cytowały }

  Synteza wiedzy innowacyjnej następuje,
    tak jak ma to miejsce w przypadku generowania jej,
      szczególnie w Warszawie oraz Wrocławiu.
  Tutaj jednak taże Katowice i Kraków 
    są okresowo bardziej aktywne
      --- lata 2015/16.
  """

  E0 = edges
  R = regions
  B = borders

 #ograniczenie danych
  E0 = E0.query('(yearY > 2011) & (yearY <= 2020)')
  R.geometry = R.geometry.simplify(0.02, preserve_topology=True)
  B.geometry = B.geometry.simplify(0.02, preserve_topology=True)

 #grupowanie krawędzi
  gE = E0.groupby(['yearY', 'pgidY'])
  E = gE.size().rename('size').reset_index()
  E = E.set_index('pgidY').join(R.set_index('gid')).reset_index()
  E = GDF(E, geometry='geometry')
  E = E.sort_values(['size'], ascending=False)

 #segregowanie
  K = E['yearY'].unique()
  S = Se([E.query(f'yearY == {k}').drop(columns='yearY') for k in K], index=K)

 #wymiary
  C = ['white']+[x for c in ['grey', 'yellow', 'orange', 'red', 'violet', 'blue', 'darkblue'] for x in [c,c]]
  c = Pt.Color('size', type='quantitative')
  c = c.scale(range=C, domainMin=0)
  c = c.title('Ilość osób').legend(orient='bottom')

 #wykresy
  m0 = Pt.Chart(B).mark_geoshape(fill=None, stroke='black', strokeWidth=0.001*A4.W).project('mercator')
  M = S.map(lambda m: Pt.Chart(m).mark_geoshape().encode(c).project('mercator')).sort_index()
  M = M.map(lambda m: (m0 + m).properties(width=0.5*A4.W, height=0.5*A4.W))
  M = M.reset_index().apply(lambda m: m[0].properties(title=f'Rok {m["index"]}'), axis=1)
  m = Pt.concat(*M.values.tolist(), columns=3, spacing=0)

  return m

@lib.flow.map('fig/difp/F.pdf')
@lib.flow.init(grph.network[0], grph.network[1], gloc.region[2])
def citekind(edges:DF, nodes:DF, regions:DF):

  r"""
  \newpage
  \chart{fig/difp/F.pdf}
  { Cytowania patentów w zależności od ich pochodzenia --- poziom powiatowy }
  """

  N0 = nodes
  E0 = edges
  R0 = regions

 #zakres dyfuzji
  E0['internal'] = E0['pgid'] == E0['pgidY']
  EI = E0.query('  internal')
  EE = E0.query('~ internal')

 #wpływ patentów
  N0['cytowany wewn.'] = N0['doc'].isin(EI['to'])
  N0['cytowany zewn.'] = N0['doc'].isin(EE['to'])
  N0['generator'] = N0.apply(lambda x: ' i '.join([k for k in ['cytowany wewn.', 'cytowany zewn.'] if x[k]]), axis=1)
  N0['generator'] = N0['generator'].replace({'cytowany wewn. i cytowany zewn.': 'cytowany w.&z.'})
  N0['generator'] = N0['generator'].replace({'': 'nie cytowane'})

  N0['cytujący wewn.'] = N0['doc'].isin(EI['from'])
  N0['cytujący zewn.'] = N0['doc'].isin(EE['from'])
  N0['synthesis'] = N0.apply(lambda x: ' i '.join([k for k in ['cytujący wewn.', 'cytujący zewn.'] if x[k]]), axis=1)
  N0['synthesis'] = N0['synthesis'].replace({'cytujący wewn. i cytujący zewn.': 'cytujący w.&z.'})
  N0['synthesis'] = N0['synthesis'].replace({'': 'nie cytujące'})

 #ograniczenie danych
  E0 = E0[['pgid', 'year', 'pgidY', 'yearY', 'to', 'from', 'internal']].copy()
  N0 = N0[['pgid', 'year', 'doc', 'generator', 'synthesis']].copy()
  N0 = N0.query('year >= 2011')

 #regionalizacja
  R0 = R0[['gid', 'name']]
  E0 = E0.set_index('pgid').join(R0.set_index('gid')).reset_index()
  E0 = E0.set_index('pgidY').join(R0.set_index('gid').add_suffix('Y')).reset_index()
  N0 = N0.set_index('pgid').join(R0.set_index('gid')).reset_index()

 #grupowanie (...) ilościowe
  gN = N0.groupby(['pgid', 'name', 'year', 'generator', 'synthesis'])
  N = gN.size().rename('size').reset_index()

  s = N.groupby('pgid')['size'].sum().rename('sum').reset_index()
  s['cluster'] = fcluster(linkage(s[['sum']], method='ward', metric='euclidean'), 2, criterion='maxclust')
  N = N.set_index('pgid').join(s.set_index('pgid')['cluster']).reset_index()

 #zgrupowanie mniejszych
  U = N.query('cluster == 1').groupby(['year', 'generator', 'synthesis'])['size'].sum().reset_index()
  U['name'] = 'pozostałe'
 #zgrupowanie szczegółowe większych
  Z = N.query('cluster == 2').drop(columns=['cluster', 'pgid'])

 #wymiary
  x = Pt.X('year', type='ordinal')
  x = x.axis(values=[2011, 2020])
  y = Pt.Y('size', type='quantitative')
  y = y.axis(values=[0,1, 5, 10, 20, 50, 100, 200, 500, 1000, 2000])
  f = Pt.Column('synthesis', type='nominal')
  c = Pt.Color('generator', type='nominal')
  c = c.legend(orient='bottom')
  c = c.scale(range=['black', 'red', 'blue', 'grey'])
  n = Pt.Row('name', type='nominal')

 #etykiety
  Z['name'] = Z['name'].str.split(' ').apply(lambda x: x[1])
  for a in [x, y, f, c, n]: a.title=None 

  c = c.title('Pochodzenie cytowanych patentów')

 #wykres zgrupowania
  u0 = Pt.Chart(U.query('synthesis == "nie cytujące"')).mark_bar().encode(x, y, c, f, n)
  u0 = u0.properties(width=0.1*A4.W, height=0.04*A4.H)

  u = Pt.Chart(U.query('synthesis != "nie cytujące"')).mark_bar().encode(x, y, c, f, n)
  u = u.properties(width=0.1*A4.W, height=0.04*A4.H)

 #wykres szczegółowy
  p0 = Pt.Chart(Z.query('synthesis == "nie cytujące"')).mark_bar().encode(x, y, c, f, n)
  p0 = p0.properties(width=0.1*A4.W, height=0.04*A4.H)

  p = Pt.Chart(Z.query('synthesis != "nie cytujące"')).mark_bar().encode(x, y, c, f, n)
  p = p.properties(width=0.1*A4.W, height=0.04*A4.H)

  return (p0 | p) & (u0 | u)