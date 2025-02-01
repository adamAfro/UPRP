import pandas, numpy, geopandas as gpd, altair as Plot
from lib.flow import Flow
import geoloc, subject, raport

@Flow.From()
def cluster(geo:pandas.DataFrame, method:str, coords:list[str], keys=[], k:int=2, innerperc=False):

  from sklearn.cluster import KMeans
  from sklearn.preprocessing import StandardScaler

  X = geo

  N = X.groupby(coords)[keys].sum().reset_index()

  if innerperc:
    N[keys] = N[keys].apply(lambda x: x/x.sum(), axis=1)

  K0 = ['__xrad__', '__yrad__']
  K = K0 + keys
  N[K0] = numpy.radians(N[coords])
  N[K] = StandardScaler().fit_transform(N[K])

  if method == 'kmeans':
    Y = KMeans(n_clusters=k, random_state=0).fit(N[K])
  else:
    raise NotImplementedError()

  N[method] = Y.labels_

  N = N.set_index(coords)[method]
  X = X.reset_index().set_index(coords).join(N)
  X[method] = X[method].astype(str)

  return X

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
data = cluster(data, 'kmeans', coords=['lat', 'lon'], k=5, innerperc=True,
               keys=[f'clsf-{k}' for k in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']])

rprtgraph = graph(raport.valid, data, geoloc.dist)

data = data.map('endo/final.pkl')

debug = { 'rprtgraph': rprtgraph }

plots = dict()

plots[f'F-pat-n-woj'] = Flow(args=[data, geoloc.region[1]], callback=lambda X, G:

  X[['grant', 'wgid']]\
    .set_index('wgid').join(G.set_index('gid'))
    .pipe(Plot.Chart).mark_bar()\
    .encode(Plot.X('year(grant)').title('Rok przyznania ochrony'),
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

plots[f'M-cluster'] = Flow(args=[data, geoloc.region[1]], callback=lambda X, G:

  Plot.Chart(G).mark_geoshape(stroke='black', fill=None) + \

  Plot.Chart(X.value_counts(['lat', 'lon', 'kmeans']).reset_index()).mark_circle()\
      .encode(longitude='lon:Q', latitude='lat:Q',
              color=Plot.Color('kmeans:N').title('Nr. kl. k-średnich').scale(scheme='category10'),
              size=Plot.Size('count:Q').title('Ilość pkt.')).project('mercator'))

plots[f'T-cluster-clsf'] = Flow(args=[data], callback=lambda X: (

  lambda P:

    P.mark_rect(width=50).encode(Plot.Color('value:Q').title('Udział').scale(scheme='orangered')) + \
    P.mark_text(baseline='middle').encode(Plot.Text('value:Q', format=".2f"))

)( X[[f'clsf-{k}' for k in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H'] ]]\
    .apply(lambda x: x/x.sum(), axis=1).assign(kmeans=X['kmeans'])\
    .groupby('kmeans').mean().reset_index()\
    .melt(id_vars='kmeans')\
    .pipe(Plot.Chart)\
    .encode(Plot.Y('variable:N').title(None),
            Plot.X('kmeans:N').title(None).scale(padding=20))
))

plots[f'T-cluster-meandist'] = Flow(args=[data], callback=lambda X: (

  lambda P:

    P.mark_rect(width=50).encode(Plot.Color('value:Q').title('Wartość').scale(scheme='orangered')) + \
    P.mark_text(baseline='middle').encode(Plot.Text('value:Q', format=".2f"))

)( X[['kmeans']+[f'meandist{r}' for r in ['', '50', '100'] ]]\
    .groupby('kmeans').mean().reset_index()\
    .melt(id_vars='kmeans')\
    .pipe(Plot.Chart)\
    .encode(Plot.Y('variable:N').title(None),
            Plot.X('kmeans:N').title(None).scale(padding=20))
))

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

plots[f'T-meandist'] = Flow(args=[data], callback=lambda X: (

  lambda P:

    P.mark_rect(width=50).encode(Plot.Color('value:Q').title('Wartość').scale(scheme='orangered')) + \
    P.mark_text(baseline='middle').encode(Plot.Text('value:Q', format=".2f"))

)(pandas.concat([ X[f'meandist{r}'].describe().drop('count').reset_index() for r in ['', '50', '100'] ])\
        .melt(id_vars='index').dropna()\
        .replace({ 'meandist': 'śr. odl.', 'meandist50': 'śr. odl. do 50km', 'meandist100': 'śr. odl. do 100km',  })\
        .replace({ 'mean': 'średnia', 'std': 'odch. std.', 'min': '0%', 'max': '100%' })\
        .pipe(Plot.Chart)\
        .encode(Plot.Y('variable:O').title(None),
                Plot.X('index:O').title(None).scale(padding=20))
))

for r in ['', '50', '100']:

  plots[f'F-meandist{r}'] = Flow(args=[data], callback=lambda X, r=r:

    X[f'meandist{r}'].to_frame()\
      .pipe(Plot.Chart).mark_bar()\
      .encode(Plot.X(f'meandist{r}').bin().title('Średni dystans [km]'),
              Plot.Y('count()').title(None)))

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


for k, F in plots.items():
  F.name = k
  F.map(f'fig/endo/{k}.png')

for k, F in dtplots.items():
  F.name = k
  F.map(f'fig/endo/{k}.png')

plots['M-woj'] = Flow(args=[F for k, F in dtplots.items() if 'M-woj' in k], callback=lambda *X: None)
plots['M-pow'] = Flow(args=[F for k, F in dtplots.items() if 'M-pow' in k], callback=lambda *X: None)