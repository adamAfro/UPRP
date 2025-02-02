import pandas, numpy, geopandas as gpd, altair as Plot
import lib.flow, endo, gloc as gloc

@lib.flow.Flow.From()
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

data = cluster(endo.data, 'kmeans', coords=['lat', 'lon'], k=5, innerperc=True,
               keys=[f'clsf-{k}' for k in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']])

plots = dict()

plots[f'M-kmeans'] = lib.flow.Flow(args=[data, gloc.region[1]], callback=lambda X, G:

  Plot.Chart(G).mark_geoshape(stroke='black', fill=None) + \

  Plot.Chart(X.value_counts(['lat', 'lon', 'kmeans']).reset_index()).mark_circle()\
      .encode(longitude='lon:Q', latitude='lat:Q',
              color=Plot.Color('kmeans:N').title('Nr. kl. k-średnich').scale(scheme='category10'),
              size=Plot.Size('count:Q').title('Ilość pkt.')).project('mercator'))

plots[f'T-kmeans-clsf'] = lib.flow.Flow(args=[data], callback=lambda X: (

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

plots[f'T-kmeans-meandist'] = lib.flow.Flow(args=[data], callback=lambda X: (

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


for k, F in plots.items():
  F.name = k
  F.map(f'fig/clst/{k}.png')