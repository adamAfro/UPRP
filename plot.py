import pandas, matplotlib.pyplot as plt
import geopandas as gpd, geoplot as gplt
from matplotlib.ticker import MaxNLocator

plt.rcParams['axes.spines.top'] = False
plt.rcParams['axes.spines.right'] = False
plt.style.use('grayscale')

class Colr:

  neutral = 'blue'
  good = 'green'
  mid = 'yellow'
  attention = 'orange'
  warning = 'red'

class Cmap:

  neutral = 'Blues'
  good = 'Greens'
  mid = 'YlOrBr'
  attention = 'Oranges'
  warning = 'Reds'
  visible = 'plasma'

class Annot:

  def bar(ax, nbarfix=2, rbarfix=0.02, fixh = 12):
    H = []
    R = ax.get_ylim()[1] - ax.get_ylim()[0]
    for p in ax.patches:
      h = p.get_height()
      z = -1 if h > 0.5*R else 1
      if h == 0: continue
      T = (0, z*fixh)
      for h0 in H:
        if abs(h - h0) < rbarfix*R:
          T = (0+T[0], fixh+T[1])
      H = [h] + H[:nbarfix]
      ax.annotate(f'{h}', (p.get_x() + p.get_width() / 2., h),
                  bbox=dict(boxstyle="round,pad=0.3", edgecolor='black', facecolor='white', alpha=0.7),
                  ha='center', va='center', xytext=T, textcoords='offset points',
                  arrowprops=dict(arrowstyle='-', color='black', shrinkA=0, shrinkB=0))

def squarealike(n):

  m = int(n**0.5)
  if m**2 == n: return m, m
  return m+1, m


def monthly(X:pandas.DataFrame, by:str, title='W danym miesiącu', x='miesiąc',
            year='year', month='month'):

  d = X[year].str.zfill(2)+'-'+X[month].str.zfill(2)

  M = X.groupby([d, by]).size().unstack(fill_value=0).sort_index()

  f, A = plt.subplots(len(M.columns), constrained_layout=True,
                      sharex=True, sharey=True, figsize=(8, 1+len(M.columns)))

  for i, k in enumerate(M.columns):
    M[k].plot.bar(xlabel='miesiąc', ax=A[i], ylabel=k, rot=0)
    A[i].xaxis.set_major_locator(MaxNLocator(integer=True, prune='both'))

  A[0].set_title(title)

  return f

def NA(X: pandas.DataFrame):

  Y = pandas.DataFrame({'Dane': X.notna().sum(), 'Braki': X.isna().sum()})

  f, A = plt.subplots(1, figsize=(8, 4), tight_layout=True)

  Y.plot(kind='bar', stacked=True, color=[Colr.good, Colr.warning], ax=A)
  A.set_ylabel('Liczba wartości')
  A.set_title('Braki danych w kolumnach')
  A.legend()

  return f

class Geodisp:

  def total(X:pandas.DataFrame, coords=['lat', 'lon'],
            scale=0.5, growth=25, label='city'):

    P = X.value_counts(coords).reset_index()
    P = gpd.GeoDataFrame(P, geometry=gpd.points_from_xy(P[coords[1]], P[coords[0]]))

    f, A = plt.subplots(1, figsize=(8, 8), tight_layout=True)
    gplt.pointplot(P, ax=A, extent=P.total_bounds, legend=True, legend_var='hue',
                   hue='count', cmap=Cmap.visible, scale='count', limits=(scale, scale*growth))

    if label:

      L = X[coords + [label]].value_counts().reset_index().head(10)
      L = gpd.GeoDataFrame(L, geometry=gpd.points_from_xy(L[coords[1]], L[coords[0]]))

      for x, y, k in zip(L.geometry.x, L.geometry.y, L[label]):
        A.annotate(k, xy=(x, y), xytext=(3, 3), textcoords="offset points", fontsize=8, color='black')

    return f

  def periods(X:pandas.DataFrame, coords=['lat', 'lon'],
              scale=0.5, growth=25, time='date', freq='12M'):

    from matplotlib.colors import Normalize

    X = gpd.GeoDataFrame(X, geometry=gpd.points_from_xy(X[coords[1]], X[coords[0]]))
    T = X.groupby(pandas.Grouper(key=time, freq=freq))
    T = [(g, G.value_counts(coords+['geometry']).reset_index()) for g, G in T]

    M0 = max([G['count'].max() for g, G in T])
    def maxscale(m, M):
      return lambda x: scale + scale*growth*x/M0

    r, c = squarealike(len(T))
    f, A = plt.subplots(r, c, figsize=(15, 15), tight_layout=True)
    A = A.flatten()

    for i, (g, G) in enumerate(T):

      G = G.dropna(subset=coords)
      P = gpd.GeoDataFrame(G, geometry=G['geometry'])

      legend = dict(legend=True, legend_var='hue') if i % c == c-1 else dict(legend=False)

      A[i].set_title('Od ' + g.strftime('%d.%m.%Y'))
      gplt.pointplot(P, ax=A[i], extent=X.total_bounds, **legend,
                     hue='count', cmap=Cmap.visible, norm = Normalize(vmin=0, vmax=M0),
                     scale='count', scale_func=maxscale)

    for i in range(len(T), len(A)):
      A[i].axis('off')

    return f