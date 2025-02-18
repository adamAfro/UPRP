r"""
\section{Dyfuzja wiedzy innowacyjnej}

Dyfuzja wiedzy jest procesem zachodzącym między osobami.
Cytowania patentowe są symptomem tego procesu.
Dla uproszczenia, nazywamy je
\emph{przepływem}.
Dotyczy on transferu wiedzy 
między osobami pełniącymi rolę patentowe
w jednym z patentów powiązanych cytowaniem.
Każde cytowanie jest więc relacją 
między dwoma osobami.
Relacja ta nie jest symetrzyczna:
jedna jest osobą cytująca, 
a druga cytowaną.
"""

#lib
import lib.flow, grph, gloc

#calc
import pandas, math
from pandas import DataFrame as DF, Series as Se
from geopandas import GeoDataFrame as GDF

#plot
import altair as Pt
from util import A4

@lib.flow.map('fig/difu/M-x-points.pdf')
@lib.flow.init(grph.network[0], gloc.region[2], gloc.region[0])
def xpoints(edges:DF, regions:GDF, borders:GDF):

  r"""
  \newpage
  \chart{fig/difu/M-x-points.pdf}
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

@lib.flow.map('fig/difu/M-y-points.pdf')
@lib.flow.init(grph.network[0], gloc.region[2], gloc.region[0])
def ypoints(edges:DF, regions:GDF, borders:GDF):

  r"""
  \newpage
  \chart{fig/difu/M-y-points.pdf}
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

@lib.flow.map(('fig/difu/F-woj.pdf', 'tbl/difu/F-woj.tex'))
@lib.flow.init(grph.network[0], grph.network[1], gloc.region[1])
def viswoj(edges:DF, nodes:DF, regions:DF):

  r"""
  \newpage
  \begin{multicols}{2}
  \chart{fig/difu/F-woj.pdf}
  { Wykres cytowań zawartych w patentach z uwzględnieniem
    rodzaju pochodzenia cytowanych i cytujących patentów }

  \columnbreak

  Wykresy po prawej prezetnują cytowania w województwach.
  \TODO{opis}

  \end{multicols}

  \tbl{tbl/difu/F-woj.tex}
  { Tabela korelacji ilości patentów cytowanych względem liczności }
  """

  N0 = nodes
  E0 = edges
  R0 = regions

 #zakres dyfuzji
  E0['internal'] = E0['wgid'] == E0['wgidY']
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
  E0 = E0[['wgid', 'year', 'wgidY', 'yearY', 'to', 'from', 'internal']].copy()
  N0 = N0[['wgid', 'year', 'doc', 'generator', 'synthesis']].copy()
  N0 = N0.query('year >= 2011')

 #regionalizacja
  R0 = R0[['gid', 'name']]
  E0 = E0.set_index('wgid').join(R0.set_index('gid')).reset_index()
  E0 = E0.set_index('wgidY').join(R0.set_index('gid').add_suffix('Y')).reset_index()
  N0 = N0.set_index('wgid').join(R0.set_index('gid')).reset_index()

 #grupowanie (...) regionalne
  gN = N0.groupby(['name', 'year', 'generator', 'synthesis'])
  N = gN.size().rename('size').reset_index()

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
  N['name'] = N['name'].str[:4]+'.'
  for a in [x, y, f, c, n]: a.title=None 

  c = c.title('Pochodzenie cytowanych patentów')

 #wykres
  p0 = Pt.Chart(N.query('synthesis == "nie cytujące"')).mark_bar().encode(x, y, c, f, n)
  p0 = p0.properties(width=0.1*A4.W, height=0.04*A4.H)

  p = Pt.Chart(N.query('synthesis != "nie cytujące"')).mark_bar().encode(x, y, c, f, n)
  p = p.properties(width=0.1*A4.W, height=0.04*A4.H)

 #korelacja
  n = N.groupby(['name', 'year'])['size'].sum()

  X = N.pivot_table('size', ['name', 'year'], ['generator'], 'sum', fill_value=0)
  X = X.drop(columns='nie cytowane').sum(axis=1)

  Y = N.pivot_table('size', ['name', 'year'], ['synthesis'], 'sum', fill_value=0)
  Y = Y.drop(columns='nie cytujące').sum(axis=1)

  r = Se([X.corr(Y), X.corr(n), Y.corr(n)], index=['$v_x, v_y$', 
                                                   '$v_x, v$',
                                                   '$v_y, v$'])
  r.index.name = '$a, b$'
  r = r.rename('$corr(a, b)$ (Pearson)').to_frame()

  return p0 | p, r.to_latex()

@lib.flow.map('fig/difu/F-mxtrwoj.pdf')
@lib.flow.init(grph.network[0], gloc.region[1])
def mxtrwoj(edges:DF, regions:DF):

  r"""
  \subsection{Cytowania w raportach o stanie techniki pomiędzy województwami}

  \chart{fig/difu/F-mxtrwoj.pdf}
  { Macierz cytowań w raportach o stanie techniki z uwzględnieniem lat. }

  Rząd wykresu wskazuje
  na województwo osoby cytowanej, 
  a kolumna, województwo osoby cytującej.
  \TODO{przykład}

  W dużej większości nie występuje zjawisko cytowań osób
  z innych województw, jednak są liczne wyjątki, 
  kolejno --- od góry:

  \TODO{opisać. popr. po zmianach}
  """

 #dane
  E0 = edges
  R0 = regions

 #regionalizacja
  R0 = R0[['gid', 'name']]
  E0 = E0.set_index('wgid').join(R0.set_index('gid')).reset_index()
  E0 = E0.set_index('wgidY').join(R0.set_index('gid').add_suffix('Y')).reset_index()

 #grupowanie (...) regionalne
  gE = E0.groupby(['name', 'nameY', 'yearY'])

 #krawędzie regionalne
  E = gE.size().rename('size').to_frame()
  E = E.join(gE.agg({'distance': 'mean'}))
  E = E.reset_index()
  E.loc[E['name'] == E['nameY'], 'size'] = 0

 #segregacja na regiony
  K = list( set(E['nameY'].unique().tolist() + E['name'].unique().tolist()) )
  S = DF({ k0: [E.query(f'(name == "{k0}") & (nameY == "{k}")') for k in K] for k0 in K }, index=K)
  S = S.map(lambda X: X if not X.empty else None)

 #sortowanie
  o = S.map(lambda X: X['size'].sum() if X is not None else 0)
  o = o.sum().sort_values(ascending=False).index
  S = S.loc[o, o]

 #wymiary
  x = Pt.X('yearY', type='ordinal')
  y = Pt.Y('size', type='quantitative')
  y = y.axis(values=[0, 500], labels=False)
  c = Pt.Color('distance', type='quantitative')
  c = c.scale(range=['green', 'blue', 'red', 'black'], domain=(0, 500))

 #opisy wymiarów
  x = x.axis(None)
  y = y.title(None)
  c = c.legend(orient='bottom').title('Średnia odległość')

 #wykresy słupkowe
  F = S.map(lambda X: Pt.Chart(X).mark_bar().encode(x, y, c) if X is not None 
                 else Pt.Chart(X).mark_bar(fill=None))

 #teksty podpisujące województwa
  T = [Pt.Chart().mark_text().encode(text=Pt.datum(k[:4]+'.')) for k in S.keys()]
  for i, x in enumerate(F.index): F.iloc[i,i] = T[i]

 #dodanie etykiet osi do skrajnych
  y0 = Pt.Y('size', type='quantitative').title(None).axis(values=[0, 500])
  F.iloc[+1:, 0] = F.iloc[+1:, 0].apply(lambda p: p.encode(y0))
  x0 = Pt.X('yearY', type='ordinal').title(None).title(None).axis(values=[2011, 2022])
  F.iloc[-1,:-1] = F.iloc[-1,:-1].apply(lambda p: p.encode(x0))

 #łączenie
  F = F.map(lambda p: p.properties(width=0.05*A4.W, height=0.04*A4.W))
  p = Pt.concat(*[z for v in F.values.tolist() for z in v], columns=16, spacing=0)
  p = p.resolve_scale(x='shared', y='shared')

  return p