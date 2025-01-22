import pandas, matplotlib.pyplot as plt, numpy
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
  distinct = 'tab20'

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
  a, b = m, m
  while a*b < n: b += 1
  return a, b


def monthly(X:pandas.DataFrame, by:str, 
            title='W danym miesiącu', x='miesiąc',
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

def geodisp(X:pandas.DataFrame, coords=['lat', 'lon'], label=None,
            scale=0.5, growth=25, time=None, freq='12M', 
            color=None):

  from matplotlib.colors import Normalize

  X = gpd.GeoDataFrame(X, geometry=gpd.points_from_xy(X[coords[1]], X[coords[0]]))
  T = [(None, X)]

  if time:
    T = X.groupby(pandas.Grouper(key=time, freq=freq))

  K = [] if color is None else [color]
  T = [(g, G.value_counts(coords+['geometry']+K).reset_index()) for g, G in T if not G.empty]

  M0 = max([G['count'].max() for g, G in T])
  def maxscale(m, M):
    return lambda x: scale + scale*growth*x/M0

  r, c = squarealike(len(T))
  f, A = plt.subplots(r, c, figsize=(16, 16), tight_layout=True)
  A = A.flatten() if isinstance(A, numpy.ndarray) else [A]

  if color is None:
    C = dict(hue='count', cmap=Cmap.visible, 
             norm=Normalize(vmin=0, vmax=M0))
  else:
    C = dict(hue=color, cmap=Cmap.distinct)

  if label:
    E = X[coords + [label]].value_counts().reset_index().head(10)
    E = gpd.GeoDataFrame(E, geometry=gpd.points_from_xy(E[coords[1]], E[coords[0]]))

  for i, (g, G) in enumerate(T):

    G = G.dropna(subset=coords)
    if G.empty: 
      A[i].axis('off')
      continue

    P = gpd.GeoDataFrame(G, geometry=G['geometry'])

    L = dict(legend=False)
    if color is not None:
      L = dict(legend=True, legend_var='hue')
      L['legend_kwargs'] = dict(bbox_to_anchor=(1.05, 1), 
                                loc='upper right', fontsize=8)
    elif i % c == c-1:
      L = dict(legend=True, legend_var='hue')
      L['legend_kwargs'] = dict(shrink=0.5)

    gplt.pointplot(P, ax=A[i], extent=X.total_bounds, **L, **C,
                    scale='count', scale_func=maxscale)

    if label:
      for x, y, k in zip(E.geometry.x, E.geometry.y, E[label]):
        A[i].annotate(k, xy=(x, y), xytext=(3, 3), textcoords="offset points", 
                      fontsize=8, color='black')


    if g:
      if time: A[i].set_title(g.strftime('%d.%m.%Y' + ' - ' + freq))

  for i in range(len(T), len(A)):
    A[i].axis('off')

  return f