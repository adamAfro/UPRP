import pandas, geopandas as gpd, altair as Plot
from lib.flow import Flow
import geoloc, subject

data = subject.mapped
data = geoloc.statunit(data, geoloc.dist, coords=['lat', 'lon'], rads=[20, 50, 100]).map('subject/geostats.pkl')

plots = dict()

plots[f'F-geoloc-eval-clsf'] = Flow(args=[data], callback=lambda X:

  Plot.Chart(X[['IPC', 'loceval']]\
              .explode('IPC')\
              .replace({'unique': 'jednoznaczna',
                        'proximity': 'najlbiższa innym',
                        'document': 'npdst. współautorów',
                        'identity': 'npdst. tożsamości' })\
              .value_counts(['IPC', 'loceval']).reset_index())

      .mark_bar().encode( Plot.Y(f'count:Q').title(None),
                          Plot.Color('loceval:N')\
                              .title('Metoda geolokalizacji')\
                              .legend(orient='bottom', columns=1),
                          Plot.X('IPC:N').title('Klasyfikacja')))

plots[f'F-geoloc-eval'] = Flow(args=[data], callback=lambda X:

  Plot.Chart(X.assign(year=X['grant'].dt.year)\
              .replace({'unique': 'jednoznaczna',
                        'proximity': 'najlbiższa innym',
                        'document': 'npdst. współautorów',
                        'identity': 'npdst. tożsamości' })\
             .value_counts(['year', 'loceval']).reset_index())

      .mark_bar().encode( Plot.X('year:T').title('Rok'),
                          Plot.Y(f'count:Q').title(None),
                          Plot.Color('loceval:N')\
                              .title('Metoda geolokalizacji')\
                              .legend(orient='bottom', columns=2)))

plots[f'map'] = Flow(args=[data, geoloc.region[1]], callback=lambda X, G:

  Plot.Chart(G).mark_geoshape(stroke='black', fill=None) + \

  Plot.Chart(X.value_counts(['lat', 'lon'])\
              .reset_index())\

      .mark_circle().encode(longitude='lon:Q', latitude='lat:Q', 
                            color=Plot.Color(f'count:Q').scale(scheme='goldgreen'),
                            size=Plot.Size('count').title('Ilość pkt.')).project('mercator'))

plots[f'map-13-22'] = Flow(args=[data, geoloc.region[1]], callback=lambda X, G:(

  lambda X, G=G:

    Plot.hconcat(

        Plot.vconcat(

            Plot.Chart(G).mark_geoshape(stroke='black', fill=None) + \

            X   .query('year == 2013')\
                .pipe(Plot.Chart, title=str(2013))\
                .mark_circle(color='black')\
                .encode(longitude='lon:Q', latitude='lat:Q', 
                        size=Plot.Size('count').title('Ilość')), *[

            Plot.Chart(G).mark_geoshape(stroke='black', fill=None) + \

            X   .assign(diff=X.sort_values('year').groupby(['lat', 'lon'])['count'].diff().fillna(0))\
                .query('year == @y')\
                .pipe(Plot.Chart, title=str(y)).mark_circle()\
                .encode(longitude='lon:Q', latitude='lat:Q', 
                        size=Plot.Size('count').title('Ilość'),
                        color=Plot.Color(f'diff:Q')\
                                  .scale(scheme='redyellowgreen')\
                                  .title('Zmiana r.r.'))

          for y in range(2014, 2017+1)]),

        Plot.vconcat(*[

            Plot.Chart(G).mark_geoshape(stroke='black', fill=None) + \

            X   .assign(diff=X.sort_values('year').groupby(['lat', 'lon'])['count'].diff().fillna(0))\
                .query('year == @y')\
                .pipe(Plot.Chart, title=str(y)).mark_circle()\
                .encode(longitude='lon:Q', latitude='lat:Q', 
                        size=Plot.Size('count').title('Ilość'),
                        color=Plot.Color(f'diff:Q')\
                                  .scale(scheme='redyellowgreen')\
                                  .title('Zmiana r.r.'))

          for y in range(2018, 2022+1)]),
      )
    )( X.assign(year=X['grant'].dt.year)\
        .value_counts(['lat', 'lon', 'year'])\
        .unstack(fill_value=0).stack().rename('count').reset_index()) )

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

    Plot.Chart(X).mark_bar()\
        .encode(Plot.X(f'meandist{r}:Q').bin().title('Średni dystans'),
                Plot.Y('count()').title(None)))

  plots[f'map-meandist{r}'] = Flow(args=[data, geoloc.region[2]], callback=lambda X, G, r=r: (
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

for k, F in plots.items():
  F.name = k
  F.map(f'fig/{k}.subj.png')