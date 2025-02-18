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


@lib.flow.placeholder()
def ncited(edges:DF, by:list[str], coords:list[str], region:GDF, width:float, extent:dict|None=None, 
           embed=False, compose=True):

  r"""
  \subsection{Regiony, z których pochodzi najwięcej cytowanych prac}

  \chart{fig/difu/M-ncited.pdf}{Polska \TODO{opisać}}

  \chart{fig/difu/M-ncited-lesser.pdf}{Małopolska \TODO{opisać}}

  \todo{pozostałe wykresy}
  """

  E = edges

  assert all( c in E.columns for c in by ), f'all( c in {E.columns} for c in {by} )'
  assert all( c in E.columns for c in coords ), f'all( c in {E.columns} for c in {coords} )'
  if embed: assert len(by) == 1, by

  G = E.groupby(by+coords)
  N = G.size().rename('size').reset_index()
  N0 = N.drop(columns=by).groupby(coords).sum().reset_index()
  B = N[by].value_counts().index

  if embed:

    N = N.pivot_table(index=coords, columns=by, values='size', fill_value=0).reset_index()
    N = N.melt(id_vars=coords, var_name=by[0], value_name='size')
    N = N.merge(region.set_index('gid'), left_on=coords, right_index=True)
                     #^ TODO indeks już powinien taki być
    N = GDF(N, geometry='geometry')

    N0 = N0.merge(region.set_index('gid'), left_on=coords, right_index=True)
    N0 = GDF(N0, geometry='geometry')

  d = (0, N0['size'].max())

  def mapplot(X):

    f = Pt.Chart(X.sort_values(by=['size'], ascending=False))

    if embed:

      if compose:
        f = f.encode( Pt.Color('size', type='quantitative')\
                        .legend(orient='bottom')\
                        .title(None)\
                        .scale(range=['black', 'red', 'green'], domain=d))
      else:
        f = f.encode( Pt.Color('size', type='quantitative')\
                        .legend(orient='bottom')\
                        .title(None)\
                        .scale(range=['black', 'red', 'green']))

      return f.mark_geoshape(stroke=None)

    f = f.mark_circle(size=10, clip=True)
    f = f.encode(Pt.Color('size', type='quantitative')\
                   .legend(orient='bottom', values=[10, 100, 200, 500, 1000, 2000, 5000])\
                   .title('Ilość patentów, które zostały cytowane')\
                   .scale(range=['green', 'red', 'black'], domain=d))
    f = f.encode(Pt.Latitude(coords[0], type='quantitative'))
    f = f.encode(Pt.Longitude(coords[1], type='quantitative'))
    f = f.encode(Pt.Size('size', type='quantitative'))

    return Pt.Chart(region).mark_geoshape(fill=None, stroke='black') + f

  w = width
  w0 = int(1/w)
  M = [N[(N[by] == b).all(axis=1)] for b in B]
  M = [mapplot(X) for X in M]
  M = [m.properties(title=', '.join([str(k) for k in b])) for m, b in zip(M, B)]
  M+= [mapplot(N0).properties(title='Łącznie')]
  M = [m.properties(width=w*A4.W, height=w*A4.W) for m in M]

  if extent is not None:
    M = [m.project(type='mercator', scale=extent['scale'], center=extent['center']) for m in M]
  else:
    M = [m.project('mercator') for m in M]

  if not compose:
    return tuple([N]+M)

  M = [ [m for m in M[i*w0:(i+1)*w0]] for i in range(math.ceil(len(M)/w0)) ]
  M = [Pt.hconcat(*m) for m in M]
  M = Pt.vconcat(*M)
  M = M.resolve_scale(color='shared')

  return N, M

@lib.flow.map('fig/difu/F-mxtrwoj.pdf')
@lib.flow.init(grph.network[0], grph.network[1], gloc.region[1])
def mxtrwoj(edges:DF, nodes:DF, regions:DF):

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
  N0 = nodes
  E0 = edges
  R0 = regions

 #regionalizacja
  R0 = R0[['gid', 'name']]
  E0 = E0.set_index('wgid').join(R0.set_index('gid')).reset_index()
  E0 = E0.set_index('wgidY').join(R0.set_index('gid').add_suffix('Y')).reset_index()
  N0 = N0.set_index('wgid').join(R0.set_index('gid')).reset_index()

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

ptPL=ncited(edges=grph.network[0], 
            by=['year'], coords=['lat', 'lon'], 
            region=gloc.region[0], width=0.33).map((None, 'fig/difu/M-ncited.pdf')),

ptLsrPL=ncited(edges=grph.network[0], by=['year'], 
              coords=['lat', 'lon'], region=gloc.region[1], width=0.33, 
              extent=dict(scale=2500, center=[17.0, 51.0])).map((None, 'fig/difu/M-ncited-lesser.pdf')),

wojPL=ncited(edges=grph.network[0], 
             by=['year'], coords=['wgid'], embed=True,
             region=gloc.region[1], width=0.33).map((None, 'fig/difu/M-ncited-woj.pdf')),

powPL=ncited(edges=grph.network[0], compose=False,
             by=['year'], coords=['pgid'], embed=True,
             region=gloc.region[2], width=0.33).map((None, *[f'fig/difu/M-ncited-pow-{k}.pdf' for k in range(13,20+1)], f'fig/difu/M-ncited-pow.pdf')),

powLsrPL=ncited(edges=grph.network[0], compose=False,
                by=['year'], coords=['pgid'], embed=True,
                region=gloc.region[2], width=0.33,
                extent=dict(scale=2000, center=[18.0, 50.5])).map((None, *[f'fig/difu/M-ncited-lesser-pow-{k}.pdf' for k in range(13,20+1)], f'fig/difu/M-ncited-lesser-pow.pdf'))