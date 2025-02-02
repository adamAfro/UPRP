import pandas, numpy, geopandas as gpd, altair as Plot
from lib.flow import Flow
import geoloc, subject, raport, lib.timeseries

@Flow.From()
def statunit(geo:pandas.DataFrame, dist:pandas.DataFrame, coords:list[str], rads=[]):

  X = geo
  D = dist

  X = X.dropna(subset=coords)

  I = [g for g in D.columns if g in X[coords].values]
  D = D.loc[I, I].sort_index()

  N = X.value_counts(subset=coords).sort_index()
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

   #średnia dla danej liczności
    R = R.sum(axis=1) / L.sum(axis=1)
    if r == M: r = ''
    X = X.join(R.rename(f'meandist{r}').astype(float))

  return X

@Flow.From()
def graph(edgdocs:pandas.DataFrame, 
          edgaffil:pandas.DataFrame,
          dist:pandas.DataFrame):

  A = edgaffil.reset_index()

  D = edgdocs
  D['edge'] = range(len(D))

  X = A.set_index('doc').join(D.set_index('from'), how='inner')
  Y = A.set_index('doc').join(D.set_index('to'), how='inner')
  E = X.set_index('edge').join(Y.set_index('edge'), rsuffix='Y', how='inner').reset_index()

  E = E.set_index(['lat', 'lon', 'latY', 'lonY'])

  d = dist.reset_index().melt(id_vars=[('lat', ''), ('lon', '')], value_name='distance')
  d.columns = ['lat', 'lon', 'latY', 'lonY', 'distance']
  d = d.set_index(['lat', 'lon', 'latY', 'lonY'])
  E = E.join(d)

  E['closeness'] = 1 / (E['distance']+1)

  return E

data = subject.mapped
data = statunit(data, geoloc.dist, coords=['lat', 'lon'], rads=[20, 50, 100])
data = data.map('endo/final.pkl')

rprtgraph = graph(raport.valid, data, geoloc.dist)

debug = { 'rprtgraph': rprtgraph }

plots = dict()

plots[f'F-pat-n-woj'] = Flow(args=[data, geoloc.region[1]], callback=lambda X, G:

  X[['grant', 'wgid']]\
    .set_index('wgid').join(G.set_index('gid'))
    .pipe(Plot.Chart).mark_bar().properties(width=100, height=100)\
    .encode(Plot.X('year(grant)').title('Rok'),
            Plot.Y('count()').title(None),
            Plot.Facet('name:N', columns=4).title(None)))

plots[f'F-pat-n-woj-Q'] = Flow(args=[data, geoloc.region[1]], callback=lambda X, G:

  X[['grant', 'wgid']]\
    .set_index('wgid').join(G.set_index('gid'))
    .pipe(Plot.Chart).mark_bar().properties(width=100, height=100)\
    .encode(Plot.X('quarter(grant)').title('Kwartał'),
            Plot.Y('count()').title(None),
            Plot.Facet('name:N', columns=4).title(None)))

plots[f'F-pat-n-woj-mo'] = Flow(args=[data, geoloc.region[1]], callback=lambda X, G:

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

plots[f'M'] = Flow(args=[data, geoloc.region[1]], callback=lambda X, G:

  Plot.Chart(G).mark_geoshape(stroke='black', fill=None) + \

  Plot.Chart(X.value_counts(['lat', 'lon'])\
              .reset_index())\

      .mark_circle().encode(longitude='lon:Q', latitude='lat:Q', 
                            color=Plot.Color(f'count:Q').scale(scheme='goldgreen'),
                            size=Plot.Size('count').title('Ilość pkt.')).project('mercator'))

@Flow.From()
def histogram(X:pandas.DataFrame, k:str, step=None): return (\

  X[[k]].pipe(Plot.Chart).mark_bar()\
    .encode(Plot.Y(f'{k}:Q').bin(**(dict(step=step) if step else {})).title(None),
            Plot.X('count()').title(None)) + \

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


plots['T-statio-woj'] = Flow(args=[data, geoloc.region[1]], callback=lambda X, G: (

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
for r, R in { 'woj': geoloc.region[1], 'pow': geoloc.region[2] }.items():

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

  plots[f'F-meandist{r}'] = histogram(data, f'meandist{r}')

  plots[f'M-meandist{r}'] = Flow(args=[data, geoloc.region[2]], callback=lambda X, G, r=r: (
    lambda X, G=G, r=r:

    gpd .sjoin(gpd.GeoDataFrame(X, geometry=gpd.points_from_xy(X.lon, X.lat, crs=G.crs)), G, 
                how='right', predicate="within")\
        .groupby('geometry')[f'meandist{r}'].mean().reset_index()\
        .pipe(gpd.GeoDataFrame, geometry='geometry', crs=G.crs)\
        .pipe(Plot.Chart).mark_geoshape(stroke='black')\
        .encode(color=Plot.Color(f'meandist{r}:Q')\
                          .scale(scheme='blueorange')\
                          .title('Śr. d. w pow.')).project('mercator')

  )(X.value_counts(['lat', 'lon', f'meandist{r}']).reset_index()))

plots[f'M-rprtdist'] = Flow(args=[rprtgraph, geoloc.region[1]], callback=lambda X, G: (

  lambda X, G=G:

    Plot.vconcat(

      Plot.Chart(G).mark_geoshape(stroke='black', fill=None) + \

      X .query(f'distance == 0')\
        .pipe(Plot.Chart)\
        .mark_circle(fill='green')\
        .encode(Plot.Latitude('lat'), 
                Plot.Longitude('lon'),
                Plot.Size('count()')\
                    .legend(orient='top')\
                    .title('Ilość połączeń w tej samej geolokalizacji')), *[

      Plot.Chart(G).mark_geoshape(stroke='black', fill=None) + \

      X .query(f'{start} < distance <= {end}')\
        .pipe(Plot.Chart, title=f'{start} - {end} km')\
        .mark_rule(opacity=0.1)\
        .encode(Plot.Color('distance:Q')\
                    .title('Dystans [km]')\
                    .scale(scheme='greens', reverse=True)\
                    .legend(orient='bottom'),
                Plot.Latitude('lat'),
                Plot.Longitude('lon'),
                Plot.Latitude2('latY'),
                Plot.Longitude2('lonY')).project('mercator')

        for start, end in [(0, 100), (100, 200)]]) | \

    Plot.vconcat(*[

      Plot.Chart(G).mark_geoshape(stroke='black', fill=None) + \

      X .query(f'{start} < distance <= {end}')\
        .pipe(Plot.Chart, title=f'{start} - {end} km')\
        .mark_rule(opacity=0.1)\
        .encode(Plot.Color('distance:Q')\
                    .title('Dystans [km]')\
                    .scale(scheme='greens', reverse=True)\
                    .legend(orient='bottom'),
                Plot.Latitude('lat'),
                Plot.Longitude('lon'),
                Plot.Latitude2('latY'),
                Plot.Longitude2('lonY')).project('mercator')

        for start, end in [(200, 300), (300, 450), (450, 800)]])

  )(X.reset_index()[['lat', 'lon', 'latY', 'lonY', 'distance', 'closeness']]))


plots['F-rprt-meandist'] = histogram(rprtgraph, 'distance', step=10)


for k, F in plots.items():
  F.name = k
  F.map(f'fig/endo/{k}.png')

for k, F in dtplots.items():
  F.name = k
  F.map(f'fig/endo/{k}.png')

plots['M-woj'] = Flow(args=[F for k, F in dtplots.items() if 'M-woj' in k], callback=lambda *X: None)
plots['M-pow'] = Flow(args=[F for k, F in dtplots.items() if 'M-pow' in k], callback=lambda *X: None)