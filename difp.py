r"""
\section{Dyfuzja wiedzy innowacyjnej w powiatach}
"""

#lib
import lib.flow, grph, gloc

#calc
from pandas import DataFrame as DF, Series as Se
from geopandas import GeoDataFrame as GDF

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
  c = Pt.Color('size', type='quantitative')
  c = c.scale(range=['white', 'grey', 'red', 'blue'], domainMin=0)
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
  c = Pt.Color('size', type='quantitative')
  c = c.scale(range=['white', 'grey', 'red', 'blue'], domainMin=0)
  c = c.title('Ilość osób').legend(orient='bottom')

 #wykresy
  m0 = Pt.Chart(B).mark_geoshape(fill=None, stroke='black', strokeWidth=0.001*A4.W).project('mercator')
  M = S.map(lambda m: Pt.Chart(m).mark_geoshape().encode(c).project('mercator')).sort_index()
  M = M.map(lambda m: (m0 + m).properties(width=0.5*A4.W, height=0.5*A4.W))
  M = M.reset_index().apply(lambda m: m[0].properties(title=f'Rok {m["index"]}'), axis=1)
  m = Pt.concat(*M.values.tolist(), columns=3, spacing=0)

  return m