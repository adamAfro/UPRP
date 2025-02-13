r"""
\section{Dyfuzja wiedzy innowacyjnej}
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


@lib.flow.make()
def ncited(edges:DF, by:list[str], coords:list[str], borders:GDF, width:float, extent:dict|None=None):

  r"""
  \subsection{Regiony, z których pochodzi najwięcej cytowanych prac}

  \chart{fig/difu/M-ncited.pdf}{Polska \TODO{opisać}}

  \chart{fig/difu/M-ncited-lesser.pdf}{Małopolska \TODO{opisać}}
  """

  E = edges
  E = E.query('year >= 2013')#TODO rm

  assert all( c in E.columns for c in by ), f'all( c in {E.columns} for c in {by} )'
  assert all( c in E.columns for c in coords ), f'all( c in {E.columns} for c in {coords} )'

  G = E.groupby(by+coords)
  N = G.size().rename('size').reset_index()
  N0 = N.drop(columns=by).groupby(coords).sum().reset_index()
  B = N[by].value_counts().index

  def ptplot(X):

    f = Pt.Chart(X.sort_values(by=['size'], ascending=False))
    f = f.mark_circle(size=10, clip=True)
    f = f.encode(Pt.Latitude(coords[0], type='quantitative'))
    f = f.encode(Pt.Longitude(coords[1], type='quantitative'))
    f = f.encode(Pt.Size('size', type='quantitative'))
    f = f.encode(Pt.Color('size', type='quantitative')\
                   .legend(orient='bottom', values=[10, 100, 200, 500, 1000, 2000, 5000])\
                   .title('Ilość patentów, które zostały cytowane')\
                   .scale(range=['green', 'red', 'black']))
    return f

  w = width
  w0 = int(1/w)
  M0 = Pt.Chart(borders).mark_geoshape(fill=None, stroke='black')
  M = [N[(N[by] == b).all(axis=1)] for b in B]
  M = [M0 + ptplot(X) for X in M]
  M = [m.properties(title=', '.join([str(k) for k in b])) for m, b in zip(M, B)]
  M+= [(M0 + ptplot(N0)).properties(title='Łącznie')]
  M = [m.properties(width=w*A4.W, height=w*A4.W) for m in M]

  if extent is not None:
    M = [m.project(type='mercator', scale=extent['scale'], center=extent['center']) for m in M]
  else:
    M = [m.project('mercator') for m in M]


  M = [ [m for m in M[i*w0:(i+1)*w0]] for i in range(math.ceil(len(M)/w0)) ]
  M = [Pt.hconcat(*m) for m in M]
  M = Pt.vconcat(*M)
  M = M.resolve_scale(color='shared')

  return N, M

FLOW = dict(
            ptPL=ncited(edges=grph.web[0], 
                        by=['year'], coords=['lat', 'lon'], 
                        borders=gloc.region[0], width=0.33).map((None, 'fig/difu/M-ncited.pdf')),

            ptLsrPL=ncited(edges=grph.web[0], by=['year'], 
                           coords=['lat', 'lon'], borders=gloc.region[1], width=0.33, 
                           extent=dict(scale=2500, center=[17.0, 51.0])).map((None, 'fig/difu/M-ncited-lesser.pdf'))
           )