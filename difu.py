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
  Przykładowo: pierwszy rząd, 7 kolumna (d.3)
  pokazuje osób w województwie dolnośląskim,
  które cytują osoby z województwa opolskiego.

  W dużej większości nie występuje zjawisko cytowań osób
  z innych województw, jednak są liczne wyjątki, 
  kolejno --- od góry:

  \TODO{sprawdz. popr. po zmianach}
  \begin{itemize}
  \item W woj. dolnośląskim w roku 2015 nastąpił wzrost zainteresowania
        w stosunku do łódzkiego (d.1), jego szybkie zakończenie dało początek
        jeszcze większemu zainteresowaniu woj. opolskim (d.3).
        Także woj. śląskie wykazuje jednostajny zwiększający się 
        wpływ na dolnośląskie w ograniczonym okresie 2013-15 (d.2).
        Należy tutaj wspomnieć, że wszystkie te 3 województwa 
        są relatywnie blisko.

  \item Województwo mazowieckie jest jest w znaczącej relacji z małopolskim.
        Wykres słupkowy pozakuje znaczący spadek zainteresowania tymi patentami 
        w roku 2017 (mz.1), przy wzroście
        szczególnie dla województwa dolnośląskiego (mz.2) 
        Widać też sporadyczny ale znaczący wpływ patentów z woj. śląskiego i łódzkiego.
  \item \TODO{mp.1, mp.2}
  \item \TODO{o.1, o.2}
  \item \TODO{ł.1, ł.2, ł.3, ł.4}
  \item \TODO{ś.1, ś.2, ś.3, ś.4}
  """

  N = nodes
  E = edges
  R = regions

  R = R[['gid', 'name']]

  E = E.set_index('wgid').join(R.set_index('gid')).reset_index()
  E = E.set_index('wgidY').join(R.set_index('gid').add_suffix('Y')).reset_index()
  N = N.set_index('wgid').join(R.set_index('gid')).reset_index()

  G = E.groupby(['name', 'nameY', 'yearY'])
  C = G.size().rename('size').to_frame()
  C = C.join(G.agg({'distance': 'mean'}))
  C = C.reset_index()
  C.loc[C['name'] == C['nameY'], 'size'] = 0

  def bar(X):

    if X.empty: return Pt.Chart().mark_bar(fill=None)

    p = Pt.Chart(X).mark_bar()
    p = p.properties(width=0.05*A4.W, height=0.04*A4.W)
    p = p.encode(Pt.Color('distance', type='quantitative')
                   .legend(orient='bottom')
                   .title('Średnia odległość')
                   .scale(range=['green', 'blue', 'red', 'black']))
    p = p.encode(Pt.X('yearY', type='ordinal').axis(None))
    p = p.encode(Pt.Y('size', type='quantitative')
                   .title(None).axis(values=[0, 500], labels=False))

    return p

  Z = { k0: [C.query(f'(name == "{k0}") & (nameY == "{k}")') for k in C['nameY'].unique()] for k0 in C['name'].unique() }

  L = [Pt.Chart().mark_text().encode(text=Pt.datum(k[:4]+'.')) for k in Z.keys()]
  L = [l.properties(width=0.05*A4.W, height=0.04*A4.W) for l in L]

  F = DF({ k0: [bar(z) for z in v] for k0, v in Z.items() }, index=Z.keys())
  for i, x in enumerate(F.index): F.iloc[i,i] = L[i] 
  F.iloc[1:, 0] = F.iloc[1:, 0].apply(lambda p: p.encode(Pt.Y('size', type='quantitative')
                                                           .title(None).axis(values=[0, 500])))
  F.iloc[-1,:-1] = F.iloc[-1,:-1].apply(lambda p: p.encode(Pt.X('yearY', type='nominal')
                                                             .title(None).axis(values=[2011, 2022])))

  def arrow(i, j, year, desc, dir='left'):
    l = Pt.Chart().encode(x=Pt.datum(year, type='nominal'))
    a = Pt.Chart().encode(x=Pt.datum(year, type='nominal'))
    if dir == 'left':
      a = a.mark_point(shape='arrow', y=0, angle=+135, color='black', xOffset=-4)
      l = l.mark_text(text=desc, y=0, color='black', fontSize=8, xOffset=-10, yOffset=-1, align='right')
    else:
      a = a.mark_point(shape='arrow', y=0, angle=360-135, color='black', xOffset=+3)
      l = l.mark_text(text=desc, y=0, color='black', fontSize=8, xOffset=+10, yOffset=-1, align='left')

    F.iloc[i, j] = F.iloc[i, j] + a + l

  arrow(0,13, 2016, '(d.1)')
  arrow(0,14, 2016, '(d.2)')
  arrow(0, 6, 2016, '(d.3)')
  arrow(4, 5, 2016, '(mz.1)', 'right')
  arrow(4, 0, 2017, '(mz.2)')
  arrow(5, 4, 2016, '(mp.1)')
  arrow(5, 9, 2017, '(mp.2)')
  arrow(6,14, 2013, '(o.1)')
  arrow(6,11, 2017, '(o.2)')

  arrow(13,15, 2016, '(ł.4)')
  arrow(13, 4, 2016, '(ł.1)')
  arrow(13, 5, 2016, '(ł.2)')
  arrow(13, 6, 2016, '(ł.3)')

  arrow(14, 0, 2016, '(ś.1)')
  arrow(14, 2, 2014, '(ś.2)')
  arrow(14, 4, 2016, '(ś.3)')
  arrow(14, 5, 2016, '(ś.4)')

  P = Pt.concat(*[z for v in F.values.tolist() for z in v], columns=16, spacing=0)
  P = P.resolve_scale(x='shared', y='shared')

  return P

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