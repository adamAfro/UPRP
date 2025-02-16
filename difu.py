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
  E = E.query('year >= 2013')#TODO rm

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

@lib.flow.map('fig/difu/F-wmx.pdf')
@lib.flow.init(grph.network[0], grph.network[1], gloc.region[1])
def mxtransfer(edges:DF, nodes:DF, regions:DF):

  N = nodes
  E = edges
  R = regions

  R = R[['gid', 'name']]

  E = E.set_index('wgid').join(R.set_index('gid')).reset_index()
  E = E.set_index('wgidY').join(R.set_index('gid').add_suffix('Y')).reset_index()
  N = N.set_index('wgid').join(R.set_index('gid')).reset_index()

  B = N.groupby('name').size().rename('base').to_frame()

  G = E.groupby(['name', 'nameY'])
  C = G.size().rename('size').to_frame().reset_index()
  C = C.set_index('name').join(B).reset_index()
  C = C.query('name != nameY')
  C['ratio'] = C['size'] / C['base']

  M = Pt.Chart(C).mark_circle()
  M = M.encode(Pt.X('name', type='nominal').title('Województwo źródłowe'))
  M = M.encode(Pt.Y('nameY', type='nominal').title('Województwo docelowe'))
  M = M.encode(Pt.Size('size', type='quantitative').title('Ilość relacji'))
  M = M.encode(Pt.Color('ratio', type='quantitative')
                 .title('Stosunek~do liczby~patentów'.split('~'))
                 .scale(range=['red', 'green', 'black']))

  return M

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