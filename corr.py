#local
import lib.flow, endo, gloc

#plot
import altair as Pt
from util import A4

#calc
import pandas, numpy, geopandas as gpd
import libpysal as sal, esda as sale

@lib.flow.Flow.From()
def Moran(X:pandas.DataFrame, R:gpd.GeoDataFrame, counted: list, by:str):

  X[counted] = X[counted].map(lambda x: 1 if x > 0 else 0)
  X = X[counted + [by]]
  X['total'] = 1 
  K = ['total']+counted

  X = X .set_index(by)\
        .join(R.set_index('gid'), how='right')\
        .groupby(by)\
        .agg(dict(geometry='first', **{ k: 'sum' for k in K }))\
        .pipe(gpd.GeoDataFrame, geometry='geometry')

  w = sal.weights.Queen.from_dataframe(X)
  w.transform = 'r'

  S = pandas.DataFrame(index=K)
  S['global'] = [sale.Moran(X[k], w, permutations=1000) for k in K]
  S['I'] = S['global'].map(lambda x: x.I)
  S['p'] = S['global'].map(lambda x: x.p_sim)
  S = S.drop(columns='global').reset_index()

  return S

pAC = Moran(endo.data, gloc.region[2], [f'clsf-{l}' for l in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']], 'pgid')

plots = dict()

plots['T-Moran'] = lib.flow.forward(pAC, lambda X:(

  lambda X:
    X .eval('z = p < 0.05').replace({True: 'p < 0.05', False: ''})\
      .pipe(Pt.Chart)\
      .properties(width=.03*A4.W, height=0.15*A4.H)\
      .mark_circle()\
      .encode(Pt.Y('index').title(None),
              Pt.Size('I').legend(None),
              Pt.Color('z').title(None)\
                .scale(range=['#0f0', '#f00'])) |\

    X .pipe(Pt.Chart, title='I')\
      .properties(width=.03*A4.W, height=0.15*A4.H)\
      .mark_text()\
      .encode(Pt.Y('index').axis(None),
              Pt.Text('I', format='.2f')) |\

    X .pipe(Pt.Chart, title='p')\
      .properties(width=.03*A4.W, height=0.15*A4.H)\
      .mark_text()\
      .encode(Pt.Y('index').axis(None),
              Pt.Text('p', format='.2f'))

)(X.replace(dict(total='Ogólnie', **{ f'clsf-{k}': k for k in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H'] }))))

for k, F in plots.items():
  F.name = k
  F.map(f'fig/corr/{k}.png')