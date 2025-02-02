import pandas, numpy, altair as Plot, geopandas as gpd
from lib.flow import Flow
import gloc, subject

@Flow.From()
def load(path:str, varname:str, geo:pandas.DataFrame, names={
  'Kod': 'gid',
  'Wartosc': 'value',
  'Rok': 'year'
}):

  K = { **names, varname: 'variable' }

  X = pandas.read_excel(path, sheet_name='DANE')

  X = X.replace('-', numpy.nan)
  X = X.rename(columns=K)
  X = X[K.values()]

  X['gid'] = (X['gid'] // 100000).astype(str)
  X['gid'] = X['gid'].str.zfill(2)
  X = X.set_index('gid').join(geo.set_index('gid'), how='inner')
  X = X.reset_index()

  X = gpd.GeoDataFrame(X, geometry='geometry')

  return X

@Flow.From()
def plot(G:gpd.GeoDataFrame, var:str):

  G = G[G['variable'] == var].drop(columns=['variable'])
  G = G.groupby(['geometry', 'year']).sum()\
       .unstack(fill_value=0.).stack().reset_index()
  G = gpd.GeoDataFrame(G, geometry='geometry')

  p = Plot.hconcat(

    Plot.vconcat(

        G .query('year == 2013')\
          .pipe(Plot.Chart, title=str(2013))\
          .mark_geoshape()\
          .encode(Plot.Color(f'value:Q')),

      *[G .assign(diff=G.sort_values('year').groupby(['geometry'])['value'].diff().fillna(0))\
          .query('year == @y')\
          .pipe(Plot.Chart, title=str(y)).mark_geoshape()\
          .encode(color=Plot.Color(f'diff:Q')\
                            .scale(scheme='redyellowgreen')\
                            .title('Zmiana r.r.'))

      for y in range(2014, 2017+1)]),

    Plot.vconcat(*[

        G .assign(diff=G.sort_values('year').groupby(['geometry'])['value'].diff().fillna(0))\
          .query('year == @y')\
          .pipe(Plot.Chart, title=str(y)).mark_geoshape()\
          .encode(color=Plot.Color(f'diff:Q')\
                            .scale(scheme='redyellowgreen')\
                            .title('Zmiana r.r.'))

      for y in range(2018, 2022+1)]),
    )

  return p

@Flow.From()
def align(X:gpd.GeoDataFrame, G:gpd.GeoDataFrame, begin=2013):

  y0 = G['year'].min()

  X = X.value_counts(['gid', 'year']).reset_index()
  Y = G.pivot_table(index=['gid', 'year'], values='value', columns=['variable'])

  m = { k0: f'K{i}' for i, k0 in enumerate(Y.columns) }
  Y = Y.rename(columns=m)

  for k in Y.columns:
    for i in range(1, begin - y0 + 1):
      Y[f'{k}R{i}'] = Y[k].groupby(level=0).shift(i)

  Y = Y.rename(columns={ k: f'{k}R0' for k in m.values() })
  Y = Y.join(X.set_index(['gid', 'year']))
  Y.attrs['mapping'] = { k: k0 for k0, k in m.items() }

  return Y

@Flow.From()
def delaycorr(aligned:pandas.DataFrame, endo:str, method='pearson'):

  from scipy.stats import pearsonr

  A = aligned

  Y = []
  for k in A.columns:

    if k == endo: continue
    y = (~A[k].isna()) & (~A[endo].isna())

    if method == 'pearson':
      r, p = pearsonr(A.loc[y, k], A.loc[y, endo])
    else: raise NotImplementedError()

    y = y[y].index.get_level_values('year')
    Y.append({ 'column': k, 'corr': r, 'p': p, 'method': method,
               'begin': y.min(), 'end': y.max() })

  Y = pandas.DataFrame(Y)
  Y['column'] = Y['column'].str.split('R')
  Y['key'] = Y['column'].apply(lambda l: A.attrs['mapping'][l[0]])
  Y['delay'] = Y['column'].apply(lambda l: l[1])
  Y = Y.drop(columns=['column'])

  return Y

inn = load(path='GUS/BDL/inn-06-22.xlsx', varname='Wskaźniki', geo=gloc.region[1])

innplot = Flow('inn-GUS-plot', lambda: [
  plot(inn, 'udział nakładów na działalność innowacyjną w przedsiębiorstwach w nakładach krajowych').map('GUS/BDL/inn-exp-ent-country.png')(),
  plot(inn, 'średni udział przedsiębiorstw innowacyjnych w ogólnej liczbie przedsiębiorstw').map('GUS/BDL/inn-exp-ent.png')(),
  plot(inn, 'nakłady na działalność innowacyjną w przedsiębiorstwach w relacji do PKB').map('GUS/BDL/inn-exp-pkb.png')(),
])

inncorr = delaycorr( align(subject.Woj, inn), 'count', 'pearson' )
inncorrplot = Flow('plot inn corr', args=[inncorr], callback=lambda X:
  Plot.Chart(X.assign(key=X['key'].apply(lambda x: [' '.join(x.split()[i:i+4]) for i in range(0, len(x.split()), 4)]))).mark_bar()\
      .encode(Plot.X('delay:O').title('Opóźnienie roczne'), 
              Plot.Column('key:O').title(''),
              Plot.Y('corr:Q').title('Wsp. korelacji Pearsona'), 
              Plot.Color('p:Q').title('P-wartość')))