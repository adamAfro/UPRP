import pandas, matplotlib.pyplot as plt, numpy
import geopandas as gpd, geoplot as gplt, geoplot.crs as gcrs
from matplotlib.ticker import MaxNLocator
import matplotlib.colors as mcolors
from matplotlib.colors import Normalize

plt.rcParams['axes.spines.top'] = False
plt.rcParams['axes.spines.right'] = False
plt.style.use('grayscale')

pow = gpd.read_file('map/powiaty.shp').to_crs(epsg=4326)
woj = gpd.read_file('map/wojewodztwa.shp').to_crs(epsg=4326)
pol = gpd.read_file('map/polska.shp').to_crs(epsg=4326)

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

def count(X:pandas.DataFrame, 
          stacked=True,
          group=None,
         time=None, freq='12M',
         categories=12, xtick=None, 
         xbin=None, xbinstart=None,
         NA='b.d.',
         trend=False,
         trendix:dict[str,pandas.Series]={},
         appendix:dict[str,pandas.Series]={}):

  g0 = None
  if time: g0 = time
  if group: g0 = group

  K = [k for k in X.columns if k != g0]
  K0 = [k for k in K if X[k].nunique() <= categories]

  KL = [k for k in K if (k not in K0) and (X[k].dtype in ['O', 'str', 'object', 'o'])]

  for k in KL: X[k] = X[k].str.len()

  KZ = [k for k in K if k not in K0]

  if len(K) == 0:
    X[''] = 1
    K = ['']

 #order
  o = {}

  for k in KZ:
    if X[k].quantile(0.5) < 1:
      X[k] = pandas.cut(X[k], bins=categories, include_lowest=True, right=False)
    else:
      X[k] = intbins(X[k], categories)

    X[k].cat.add_categories(NA)
    o[k] = X[k].dtype.categories.tolist()

  if not g0: raise NotImplementedError()

  X[K] = X[K].astype(str).fillna(NA)
  if group:
    if xbin: X[g0] = intbins(X[g0], xbin, xbinstart)
    X = X.groupby(g0)
    oX = [g for g in X.groups if g != NA]+[NA]

  if time: X = X.groupby(pandas.Grouper(key=g0, freq=freq))

  T = pandas.concat([X[k].value_counts().reset_index()\
            .rename(columns={k: '__legend__'})\
            .assign(__axes__=k) for k in K])\
            .set_index(['__legend__', g0])

  for k, A in appendix.items():
    A.index.names = ['__legend__', g0]
    T = pandas.concat([T, A.to_frame().assign(__axes__=k)])

  T = [(v, V['count']) for v, V in T.groupby('__axes__') if not V.empty]

  f, A = plt.subplots(len(T), constrained_layout=True, 
                      sharex=True, sharey=True,
                      figsize=(8, 1+2*len(T)))
  A = A.flatten() if isinstance(A, numpy.ndarray) else [A]

  for i, (v, V) in enumerate(T):

    if v in KL: v = f'{v} (ilość znaków)'

    if V.empty: continue

    V = V.reset_index()
    V['__legend__'] = V['__legend__'].replace('nan', NA)
    V['__legend__'] = V['__legend__'].astype(str).fillna(NA)
    V = V.pivot(index=g0, columns='__legend__', values='count').fillna(0)

   #sort
    if group: V = V.reindex(oX)
    if v in KZ:
      V = V[[k for k in o[v] if k in V.columns] + [k for k in V.columns if k not in o[v]]]
    elif (NA in V.columns) and V.columns[-1] != NA:
      V = V[[k for k in V.columns if k != NA]+[NA]]

    if time: V.index = V.index.date

    V.plot.bar(stacked=stacked, ax=A[i], xlabel='', rot=0, legend=True, 
               cmap=Cmap.NA(Cmap.distinct, V.shape[1]))

    if V.shape[1] > 1: 
      A[i].legend(title=v, bbox_to_anchor=(1.05, 1.05), loc='upper left')
    else:
      A[i].legend().remove()

    if trend:

      if v in trendix:
        for k in trendix[v].columns:
          V[k] = trendix[v][k]

      V = V.rename(columns={ '1': 'ogółem' })
      At = A[i].twinx()
      V = V/V.max()
      V.plot(ax=At, cmap=Cmap.NA(Cmap.distinct, V.shape[1]), legend=True, marker='o', linestyle='--')
      At.legend(title="Względny trend")

    if xtick: A[i].xaxis.set_major_locator(MaxNLocator(xtick))

  A[-1].set_xlabel(g0)

  return f

def map(X:pandas.DataFrame, coords=['lat', 'lon'], 
        label=None,
        point=None, growth=10,
        regions=None,
        group=None, time=None, freq='12M',
        color=None, border=False, kde=None):

  w = gcrs.WebMercator()

  X = gpd.GeoDataFrame(X, geometry=gpd.points_from_xy(X[coords[1]], X[coords[0]]), crs='EPSG:4326')

  if regions is not None:

    R = gpd.sjoin(X, regions, op='within')
    R = R.groupby(['index_right']+[k for k in[time, group] if k])
    R = R.size().reset_index().rename(columns={0: 'count'})
    R = regions.join(R.set_index('index_right'))
    R['count'] = R['count'].fillna(0).astype(int)

    if group:
      R = R.groupby(group)
    elif time:
      R = R.groupby(pandas.Grouper(key=time, freq=freq))
    else:
      R = [(None, R)]

    R0 = max([G['count'].max() for g, G in R])

    if color is None:

      CR = dict(hue='count', cmap=Cmap.visible, 
                norm=Normalize(vmin=0, vmax=R0))

    else:
      CR = dict(hue=color, cmap=Cmap.distinct)

  if group:
    T = X.groupby(group)
  elif time:
    T = X.groupby(pandas.Grouper(key=time, freq=freq))
  else:
    T = [(None, X)]

  K = [] if color is None else [color]
  T = [(g, G.value_counts(coords+['geometry']+K).reset_index()) for g, G in T if not G.empty]

  T0 = max([G['count'].max() for g, G in T])

  r, c = squarealike(len(T))
  f, A = plt.subplots(r, c, figsize=(16, 16),
                      subplot_kw={'projection': w})

  A = A.flatten() if isinstance(A, numpy.ndarray) else [A]

  if point:

    if color is None:

      C = dict(hue='count', cmap=Cmap.visible, 
              norm=Normalize(vmin=0, vmax=T0))

    else:
      C = dict(hue=color, cmap=Cmap.distinct)

  if label:
    E = X[coords + [label]].value_counts().reset_index().head(10)
    E = gpd.GeoDataFrame(E, geometry=gpd.points_from_xy(E[coords[1]], E[coords[0]]))

  for i, (g, G) in enumerate(T):

    if border:
      A[i] = gplt.polyplot(pow, ax=A[i], projection=w, extent=pol.total_bounds, 
                           edgecolor=Colr.neutral, linewidth=border)
    else:
      A[i] = gplt.polyplot(pol, ax=A[i], projection=w, extent=pol.total_bounds, 
                           edgecolor=Colr.neutral, linewidth=1)

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

    if kde:
      gplt.kdeplot(P, ax=A[i], extent=pol.total_bounds, projection=w, 
                   weights=P['color'] if color else P['count'], 
                   levels=kde, **L, cmap=Cmap.neutral)

    if point:
      def maxscale(m, M): return lambda x: point + point*growth*x/T0
      gplt.pointplot(P, ax=A[i], extent=pol.total_bounds, **L, **C,
                     scale='count', scale_func=maxscale, projection=w)


    if regions is not None:

      gplt.choropleth(R.get_group(g), ax=A[i], extent=pol.total_bounds, **L, **CR, projection=w)

    if g:
      if time: A[i].set_title(g.strftime('%d.%m.%Y' + ' - ' + freq))
      else: A[i].set_title(g)

  if point and (not color):

    f.subplots_adjust(bottom=0.1)
    sm = plt.cm.ScalarMappable(cmap=C['cmap'], norm=C['norm'])
    sm.set_array([])
    f.colorbar(sm, cax=f.add_axes([0.1, 0.05, 0.8, 0.01]), 
               orientation='horizontal')

  for i in range(len(T), len(A)):
    A[i].axis('off')

  return f



from lib.flow import Flow
from util import data as D
from patent import flow as fP
from registry import flow as fR
from subject import flow as fS
from external import flow as fE

flow = dict()

for h in D.keys():

  fP[h]['code'].trigger(count).map(D[h]+'/code.png')
  fP[h]['event'].trigger(lambda X: count(X[['event', 'date']], time='date')).map(D[h]+'/event.png')
  fP[h]['classify'].trigger(count).map(D[h]+'/classify.png')

spacetime = fR['registry']['spacetime'].trigger(lambda X: X.assign(location=X['city'].isna()))

spacetime.trigger(lambda X: count(X[['location', 'loceval', 'application']], time='application', xtick=5))\
         .map('registry/NA-loc-geo.png')

sim = fS['subject']['simcalc'].trigger()
sim.trigger(lambda X: count(X.reset_index(drop=True), group='geomatch', categories=5))\
   .map('subject/Y-sim-geo.png')
sim.trigger(lambda X: count(X.reset_index(drop=True), group='nameaffil', categories=8, xbin=3, xbinstart=5))\
   .map('subject/Y-sim-affil.png')

def strrole(x, K:list[str]):
  y = '-'.join([k for k in K if x[k] == 1])
  return y if y else None
roles = fS['subject']['fillgeo'].trigger(lambda X: X.assign(role=X.apply(strrole, K=['assignee', 'inventor', 'applicant', 'organisation'], axis=1)))

roles.trigger(lambda X: count(X[['role', 'loceval']].reset_index(drop=True), group='role'))\
     .map('subject/NA-geo-role.png')

# roles.trigger(lambda X: map(X, point=1, kde=16)).map('subject/map.png')
# roles.trigger(lambda X: map(X, point=1, kde=8, time='firstdate')).map('subject/map-periods-first.png')
# roles.trigger(lambda X: map(X, point=1, kde=8, time='application')).map('subject/map-periods.png')

IPC = roles.trigger(lambda *X: X[0].explode('IPC')[['loceval', 'lat', 'lon', 'IPC', 'application']]\
                                   .rename(columns={ 'application': 'date', 'IPC': 'section' }))

# IPC.trigger(lambda X: count(X[['loceval', 'section']], group='section'))\
#    .map('subject/NA-IPC-geo.png')
# IPC.trigger(lambda X: map(X, point=1, color='section', time='date'))\
#    .map('subject/map-IPC.png')

IPC.trigger(lambda *X: map(X[0], regions=pow, group='section')).map(f'subject/map-IPC.png')

for k in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']:
  IPC.trigger(lambda *X, k=k: map(X[0][X[0]['section'] == k], time='date', regions=pow)).map(f'subject/map-IPC-{k}-periods.png')

for k in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']:
  IPC.trigger(lambda *X, k=k: count(X[0][X[0]['section'] == k][['loceval', 'date']], time='date')).map(f'subject/NA-IPC-loc-{k}.png')


compGUS = Flow(callback=lambda *X: count(X[1].assign(year=X[1]['application'].dt.year).groupby('doc')['year'].min().reset_index().assign(src='UPRP')[['src', 'year']],
                                         appendix={'src': X[0].assign(src='GUS').set_index(['src', 'year'])['count']},
                                         group='year', stacked=False, trend=True), 
                                         args=[fE['GUS']['UPRP'], fS['subject']['fillgeo']])
compGUS.map('GUS/comprasion.png')


all = Flow(callback=lambda *x: x, args=[spacetime, IPC, sim, roles])

flow = { 'plot': { 'all': all,
                   'spacetime': spacetime,
                   'IPC': IPC,
                   'sim': sim,
                   'roles': roles
                   } }