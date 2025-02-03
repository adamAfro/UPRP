import pandas, numpy, geopandas as gpd, altair as Plot
from lib.flow import Flow
import gloc, subject, lib.timeseries

@Flow.From()
def meandist(geo:pandas.DataFrame, dist:pandas.DataFrame, coords:list[str], rads=[], filtr=None, symbol=''):

  X = geo
  X = X.dropna(subset=coords)

  D = dist
  Q = X if filtr is None else X.loc[filtr]
  I0 = [g for g in D.columns if g in X[coords].values]
  I = [g for g in D.columns if g in Q[coords].values]
  D = D.loc[I0, I].sort_index()
  N = Q.value_counts(subset=coords).sort_index()

  X = X.set_index(coords)
  M = int(numpy.ceil(D.max().max()))
  for r in rads+[M]:

    R = D.copy()

   #poprawka na liczności
    L = (R <= r).astype(int)
    L = L.apply(lambda v: v.rename(None)*N, axis=1)
    L = L.sort_index()

   #ważenie przez liczność
    R = R * L
    R = R.sum(axis=1)
    L = L.sum(axis=1)
    R = R[R > 0]
    L = L[L > 0]

   #średnia dla danej liczności
    R = R/L
    if r == M: r = ''
    X = X.join(R.rename(f'meandist{symbol}{r}').astype(float))

  X = X.reset_index()

  return X

data = subject.mapped
data = meandist(data, gloc.dist, coords=['lat', 'lon'], rads=[50, 100])
for k in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']:
  data = meandist(data, gloc.dist, coords=['lat', 'lon'], rads=[50, 100], 
                  filtr=lambda X, k=k: X[f'clsf-{k}'] > 0, symbol=k)

data = data.map('cache/final.pkl')

plots = dict()

plots[f'F-pat-n-woj'] = Flow(args=[data, gloc.region[1]], callback=lambda X, G:

  X[['grant', 'wgid']]\
    .set_index('wgid').join(G.set_index('gid'))
    .pipe(Plot.Chart).mark_bar().properties(width=100, height=100)\
    .encode(Plot.X('year(grant)').title('Rok'),
            Plot.Y('count()').title(None),
            Plot.Facet('name:N', columns=4).title(None)))

plots[f'F-pat-n-woj-Q'] = Flow(args=[data, gloc.region[1]], callback=lambda X, G:

  X[['grant', 'wgid']]\
    .set_index('wgid').join(G.set_index('gid'))
    .pipe(Plot.Chart).mark_bar().properties(width=100, height=100)\
    .encode(Plot.X('quarter(grant)').title('Kwartał'),
            Plot.Y('count()').title(None),
            Plot.Facet('name:N', columns=4).title(None)))

plots[f'F-pat-n-woj-mo'] = Flow(args=[data, gloc.region[1]], callback=lambda X, G:

  X[['grant', 'wgid']]\
    .set_index('wgid').join(G.set_index('gid'))
    .pipe(Plot.Chart).mark_bar().properties(width=100, height=100)\
    .encode(Plot.X('month(grant)').title('Miesiąc').axis(format='%m'),
            Plot.Y('count()').title(None),
            Plot.Facet('name:N', columns=4).title(None)))

plots[f'F-pat-n-clsf'] = Flow(args=[data], callback=lambda X:

  Plot.vconcat(*[

    X[X[f'clsf-{k}'] > 0][['grant']]
      .pipe(Plot.Chart).mark_bar().properties(width=400, height=100)\
      .encode(Plot.X('year(grant)').title(None),
              Plot.Y('count()').title(f'Ilość pat. w kl. {k}'))

    for k in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']]))

plots[f'M'] = Flow(args=[data, gloc.region[1]], callback=lambda X, G:

  Plot.Chart(G).mark_geoshape(stroke='black', fill=None) + \

  Plot.Chart(X.value_counts(['lat', 'lon'])\
              .reset_index())\

      .mark_circle().encode(longitude='lon:Q', latitude='lat:Q', 
                            color=Plot.Color(f'count:Q').scale(scheme='goldgreen'),
                            size=Plot.Size('count').title('Ilość pkt.')).project('mercator'))

@Flow.From()
def histogram(X:pandas.DataFrame, k:str, step=None, title=None): return (\

  X[[k]].pipe(Plot.Chart).mark_bar()\
    .encode(Plot.Y(f'{k}:Q').bin(**(dict(step=step) if step else {})).title(None),
            Plot.X('count()').title(title)) + \

  X[k].astype(float).describe().loc[['25%', '50%', '75%', 'mean', 'min', 'max']]\
    .rename({ 'mean': 'średnia', 'max': 'maks.', 'min': 'min.' }).reset_index()\
    .pipe(Plot.Chart).mark_rule()\
    .encode(Plot.Y(f'{k}:Q'),
            Plot.Color('index:N')\
                .legend(orient='right')\
                .title('Statystyka')\
                .scale(scheme='category10')) + \

  pandas.DataFrame({ 'mean': [X[k].mean()], 'std': [X[k].std()], 
                     'index': 'odchylenie' })\
    .eval('y1 = mean - (std / 2)').eval('y2 = mean + (std / 2)')
    .pipe(Plot.Chart).mark_rule()\
    .encode(Plot.Y('y1:Q'), Plot.Y2('y2:Q'),
            Plot.Color('index:N'))

).properties(width=100)

@Flow.From()
def qseasonplot(X:pandas.DataFrame, k:str): return (

  lambda X:

    X .assign(season=X[k].dt.quarter%4+1)#grouper#fix
      .groupby('season')['quarter'].sum().reset_index()\
      .pipe(Plot.Chart, height=100).mark_circle()\
      .encode(Plot.Y('season:O').title('Sumarycznie'),
              Plot.Size('quarter:Q')\
                  .title('Ilość patentów')\
                  .legend(None)) | \

    (X.pipe(Plot.Chart, height=100).mark_bar()\
      .encode(Plot.X('grant:T').title('Kolejno').axis(format='%Y'),
              Plot.Y('quarter:Q').title(None)) | histogram(X, 'quarter')()).resolve_scale(y='shared')

)(X[[k]].groupby(pandas.Grouper(key=k, freq='QE', label='left'))\
   .size().rename('quarter').reset_index())

@Flow.From()
def mseasonplot(X:pandas.DataFrame, k:str): return (

  lambda X:

    X .assign(season=X[k].dt.month%12+1)#grouper#fix
      .groupby('season')['month'].sum().reset_index()\
      .pipe(Plot.Chart, height=200).mark_circle()\
      .encode(Plot.Y('season:O').title('Sumarycznie'),
              Plot.Size('month:Q')\
                  .title('Ilość patentów')\
                  .legend(None)) | \

    (X.pipe(Plot.Chart, height=200).mark_rule()\
      .encode(Plot.X('grant:T').title('Kolejno').axis(format='%Y'),
              Plot.Y('month:Q').title(None)) | histogram(X, 'month')()).resolve_scale(y='shared')

)(X[[k]].groupby(pandas.Grouper(key=k, freq='M', label='left'))\
   .size().rename('month').reset_index())

plots['F-Q'] = qseasonplot(data, 'grant')
plots['F-mo'] = mseasonplot(data, 'grant')


plots['T-statio-woj'] = Flow(args=[data, gloc.region[1]], callback=lambda X, G: (

  lambda S:

    S .pipe(Plot.Chart).mark_point()\
      .encode(Plot.X('name:N').title(None),
              Plot.Y('test:N').title(None),
              Plot.Color('z:N').title('p-wartość'),
              Plot.Size('p:Q')) + \

    S .query('p < 0.01')\
      .pipe(Plot.Chart).mark_point(shape='stroke')\
      .encode(Plot.X('name:N').title(None),
              Plot.Y('test:N').title(None),
              Plot.Color('z:N').title('p-wartość'))

)(lib .timeseries.stationary(X, 'wgid', 'grant').set_index('wgid')\
      .join(G[['name', 'gid']].set_index('gid'))\
      .eval('name = name.fillna("Polska")')\
      .eval('z=p<0.05').replace({ True: 'p < 0.05', False: 'p ≥ 0.05' })))

dtplots = dict()
for r, R in { 'woj': gloc.region[1], 'pow': gloc.region[2] }.items():

  G = Flow(args=[data, R], callback=lambda X, G: 
    X .assign(year=X['grant'].dt.year)\
      [['year', 'wgid']].set_index('wgid').join(G.set_index('gid'))\
      .groupby(['wgid', 'year']).agg({ 'geometry':'first', 'year':'count' })\
      .unstack(fill_value=0).stack().rename(columns={'year': 'count'}).reset_index())

  dtplots[f'M-{r}-13'] = Flow(args=[data, G], callback=lambda X, G:

    G .query(f'year == 2013')\
      .pipe(gpd.GeoDataFrame, geometry='geometry')\
      .pipe(Plot.Chart).mark_geoshape(stroke=None)\
      .encode(Plot.Color('count:Q')\
                  .legend(orient='left')\
                  .scale(scheme='goldgreen')\
                  .title('Ilość patentów w regionie')) + \

    X .assign(year=X['grant'].dt.year).query('year == 2013')\
      .value_counts(['lat', 'lon']).reset_index()\
      .pipe(Plot.Chart)\
      .mark_circle(color='black')\
      .encode(Plot.Longitude('lon:Q'),
              Plot.Latitude('lat:Q'),
              Plot.Size('count:Q')\
                  .title('Ilość patentów w punkcie')\
                  .legend(orient='right')))

  P = Flow(args=[data], callback=lambda X:
    X .assign(year=X['grant'].dt.year)\
      .value_counts(['lat', 'lon', 'year'])\
      .reset_index())

  for y in range(2014, 2022+1):

    dtplots[f'M-{r}-dt-{str(y)[2:]}'] = Flow(args=[P, G], callback=lambda X, G, y=y:(

      G .assign(diff=G.sort_values('year').groupby(['wgid'])['count'].diff().fillna(0))\
        .query(f'year == {y}')\
        .pipe(gpd.GeoDataFrame, geometry='geometry')\
        .pipe(Plot.Chart).mark_geoshape(stroke=None)\
        .encode(Plot.Color('diff:Q', legend=(dict(orient='left') if y == 2014 else None))\
                    .scale(scheme='redyellowgreen', domain=[-1000, 1000])\
                    .title('Zmiana r.r. w regionie')) + \

      X .assign(diff=X.sort_values('year').groupby(['lat', 'lon'])['count'].diff().fillna(0))\
        .query(f'year == {y}')\
        .eval('shape=diff>0')\
        .replace({ False: 'ujemna', True: 'dodatnia' })
        .eval('diff=abs(diff)')\
        .pipe(Plot.Chart)\
        .mark_point(color='black', shape='triangle-down')\
        .encode(Plot.Longitude('lon:Q'),
                Plot.Latitude('lat:Q'),
                Plot.Color('shape:N', legend=({} if y == 2014 else None))\
                    .title('Kierunek zmiany')\
                    .scale(range=['darkgreen', 'darkred']),
                Plot.Shape('shape:N', legend=({} if y == 2014 else None))\
                    .title('Kierunek zmiany')\
                    .scale(range=['triangle-up', 'triangle-down']),
                Plot.Size('diff:Q', legend=({} if y == 2014 else None))\
                    .scale(domain=[0, 1000])\
                    .title('Zmiana r.r. w punkcie')))\

        .resolve_legend(color='independent') )

for r in ['', '50', '100']:

  plots[f'M-meandist{r}'] = Flow(args=[data, gloc.region[2]], callback=lambda X, G, r=r: (

    lambda X, G=G, r=r:

    gpd .sjoin(gpd.GeoDataFrame(X, geometry=gpd.points_from_xy(X.lon, X.lat, crs=G.crs)), G, 
                how='right', predicate="within")\
        .groupby('geometry')[f'meandist{r}'].mean().reset_index()\
        .pipe(gpd.GeoDataFrame, geometry='geometry', crs=G.crs)\
        .pipe(Plot.Chart).mark_geoshape(stroke='black')\
        .encode(color=Plot.Color(f'meandist{r}:Q')\
                          .scale(scheme='blueorange')\
                          .title('Śr. d. w pow.')).project('mercator')

  )(X.value_counts(['lat', 'lon', f'meandist{r}']).reset_index()) | Plot.hconcat(*([
    histogram(data, f'meandist{k}{r}', title=k)() for k in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']
  ]+[histogram(data, f'meandist{r}')()])))

for k, F in plots.items():
  F.name = k
  F.map(f'fig/endo/{k}.png')

for k, F in dtplots.items():
  F.name = k
  F.map(f'fig/endo/{k}.png')

plots['M-woj'] = Flow(args=[F for k, F in dtplots.items() if 'M-woj' in k], callback=lambda *X: None)
plots['M-pow'] = Flow(args=[F for k, F in dtplots.items() if 'M-pow' in k], callback=lambda *X: None)