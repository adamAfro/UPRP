import pandas, numpy, altair as Plot, geopandas as gpd
from lib.flow import Flow
import geoloc

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

inn = load(path='GUS/BDL/wsk-dz-innow.xlsx', varname='Wskaźniki', geo=geoloc.Woj)
X = inn()

X['variable'].value_counts()


innplot = Flow('inn-GUS-plot', lambda: [
  plot(inn, 'udział nakładów na działalność innowacyjną w przedsiębiorstwach w nakładach krajowych').map('GUS/BDL/inn-exp-ent-country.png')(),
  plot(inn, 'średni udział przedsiębiorstw innowacyjnych w ogólnej liczbie przedsiębiorstw').map('GUS/BDL/inn-exp-ent.png')(),
  plot(inn, 'nakłady na działalność innowacyjną w przedsiębiorstwach w relacji do PKB').map('GUS/BDL/inn-exp-pkb.png')(),
])