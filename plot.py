import pandas, matplotlib.pyplot as plt, numpy
import geopandas as gpd, geoplot as gplt, geoplot.crs as gcrs
from matplotlib.ticker import MaxNLocator
import matplotlib.colors as mcolors
from matplotlib.colors import Normalize

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

  @staticmethod
  def NA(x: str, n: int):
    cmap = plt.get_cmap(x, n)
    colors = [cmap(i) for i in range(cmap.N)]
    return mcolors.ListedColormap(colors+[(0, 0, 0, 1)])

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

def syntdata():

  dates = pandas.date_range(start='2000-01-01', end='2022-12-31', freq='D')
  categories = ['A','A','A', 'B', 'B', 'B', 'C', 'D']
  data = pandas.DataFrame({ 'date': dates,
                            'value1': numpy.random.choice(categories, len(dates)),
                            'value2': numpy.random.choice(categories, len(dates)),
                            'value3': numpy.random.rand(len(dates)) })

  for col in ['value1', 'value2', 'value3']:
    data.loc[data.sample(frac=0.2).index, col] = numpy.nan

  return data

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

def intbins(v: pandas.Series, n: int, start=None):

  M = v.max()
  m = start if start else v.min()

 #range
  r = (M - m) / n
  R = [int(m+i*r) for i in range(n+1)]
  R = [(R[i]+1*(i != 0), R[i+1]) for i in range(len(R)-1)]

  def find(x):
    for i, (a, b) in enumerate(R):
      if a <= x <= b: return (a, b)
    return (x, x) #dla sortowania

  def strfy(x):
    if x is None: return 'b.d.'
    if x[0] == x[1]: return str(x[0])
    return f'{x[0]}-{x[1]}'

  y0 = v.apply(find)
  y = pandas.Categorical(y0.apply(strfy),
                         categories=[strfy(x) for x in sorted(y0.unique())], 
                         ordered=True)

  return y

def n(X:pandas.DataFrame, group=None,
      time=None, freq='12M',
      categories=12, xtick=12, 
      xbin=None, xbinstart=None):

  g0 = None
  if time: g0 = time
  if group: g0 = group

  K = [k for k in X.columns if k != g0]
  K0 = [k for k in K if X[k].nunique() <= categories]

  KL = [k for k in K if (k not in K0) and (X[k].dtype in ['O', 'str', 'object', 'o'])]

  for k in KL: X[k] = X[k].str.len()

  KZ = [k for k in K if k not in K0]

 #order
  o = {}

  for k in KZ:
    if X[k].quantile(0.5) < 1:
      X[k] = pandas.cut(X[k], bins=categories, include_lowest=True, right=False)
    else:
      X[k] = intbins(X[k], categories)

    o[k] = X[k].dtype.categories.tolist()

  if not g0: raise NotImplementedError()

  if group:
    if xbin: X[g0] = intbins(X[g0], xbin, xbinstart)
    X = X.groupby(g0)
    oX = [g for g in X.groups if g != 'b.d.']+['b.d.']

  if time: X = X.groupby(pandas.Grouper(key=g0, freq=freq))

  X = pandas.concat([X[k].value_counts(dropna=False).reset_index()\
            .rename(columns={k: 'value'}).assign(column=k) for k in K])

  T = X.groupby('column')
  f, A = plt.subplots(T.ngroups, constrained_layout=True, 
                      sharex=True, sharey=True,
                      figsize=(8, 1+2*T.ngroups))
  A = A.flatten() if isinstance(A, numpy.ndarray) else [A]

  for i, (v, V) in enumerate(T):

    if v in KL: v = f'{v} (ilość znaków)'

    if V.empty: continue

    V['value'] = V['value'].astype(str).fillna('b.d.')
    V = V.pivot(index=g0, columns='value', values='count').fillna(0)

   #sort
    V = V.reindex(oX)
    if v in KZ:
      V = V[[k for k in o[v] if k in V.columns] + [k for k in V.columns if k not in o[v]]]

    if time: V.index = V.index.date

    V.plot.bar(stacked=True, ax=A[i], xlabel='', rot=0, legend=True, 
               cmap=Cmap.NA(Cmap.distinct, V.shape[1]))

    A[i].legend(title=v, bbox_to_anchor=(1.05, 1.05), loc='upper left')

    if xtick: A[i].xaxis.set_major_locator(MaxNLocator(xtick))

  A[-1].set_xlabel(g0)

  return f

def ngeo(X:pandas.DataFrame, coords=['lat', 'lon'], label=None,
            scale=0.5, growth=25, time=None, freq='12M', 
            color=None, border=False, fill=None):

  w = gcrs.WebMercator()
  if border or fill:
    m = gpd.read_file('map/powiaty.shp').to_crs(epsg=4326)

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
  f, A = plt.subplots(r, c, figsize=(16, 16), tight_layout=True,
                      subplot_kw={'projection': w})
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

    if border:
      A[i] = gplt.polyplot(m, ax=A[i], projection=w, extent=m.total_bounds, 
                           edgecolor=Colr.neutral)

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

    gplt.pointplot(P, ax=A[i], extent=m.total_bounds, **L, **C,
                   scale='count', scale_func=maxscale, projection=w)


    if g:
      if time: A[i].set_title(g.strftime('%d.%m.%Y' + ' - ' + freq))

  for i in range(len(T), len(A)):
    A[i].axis('off')

  return f