from lib.flow import Flow

@Flow.From('read data bundle')
def read():

  from pandas import read_csv, MultiIndex
  from pandas import to_datetime, Categorical

  data = [(read_csv(f'bundle/{f0}.csv', dtype='str'), f0) for f0 in 
          ['classification:pat', 'event:pat', 
           'pat:pat-raport-ocr', 'pat', 
           'people:pat-named', 
           'people:pat-signed', 
           'spatial:pat']]

  for X, f0 in data:
    X.attrs['name'] = f0

  C, T, R, L, N, S, G = [X.set_index(['doc', 'docrepo']) for X, f0 in data]

  G['lat'] = G['lat'].astype(float)
  G['lon'] = G['lon'].astype(float)

  T['delay'] = T['delay'].astype(int)
  T['date'] = to_datetime(T['year'] + '-' + T['month'] + '-' + T['day'])
  T['assignement'] = Categorical(T['assignement'], ordered=True,
                                    categories=['exhibition', 'priority', 'regional', 'fill', 'application', 'nogrant', 'grant', 'decision', 'publication'])

  H = [C, T, R, N, S, G]
  A = [X.attrs for X in H]
  H = [X.join(L) for X in H]
  for X, a in zip(H, A): X.attrs = a
  C, T, R, N, S, G = H

  R.columns = MultiIndex.from_tuples([c[:4] for c in 
              (R.columns + '::::::').str.split('::')])

  return C, T, L, R, N, S, G

@Flow.From('join tables to form person')
def personify(bframes):

  from pandas import get_dummies, NA

  C, T, L, R, N, S, G = bframes

  Y = S.copy()

  C = C[[ 'classification', 'section' ]].reset_index()
  C = C.drop_duplicates().set_index(['doc', 'docrepo'])

  C = C.query('classification == "IPC"').drop('classification', axis=1)
  C = C.pipe(get_dummies, prefix='', prefix_sep='')\
      .reset_index().groupby(['doc', 'docrepo']).sum()

  Y = Y.join(C, how='left')

  T = T.reset_index().groupby(['doc', 'docrepo'])['delay'].min()
  Y = Y.join(T, how='left')

  G = G[['name', 'lat', 'lon']]
  G = G.rename(columns={'name': 'city'})
  G = G.set_index('city', append=True)

  Y = Y.set_index('city', append=True)
  Y = Y.join(G, how='left')

  Y = Y.replace({'': NA})
  Y.attrs['name'] = 'person'

  return Y

import pandas as pd, numpy as np
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

class fsize: 
  width = 8; height = 8
  wide = (8, 4)
  high = (8, 10)

plt.rcParams['figure.figsize'] = [fsize.width, fsize.height]

def event(X:pd.DataFrame, period:int, nperiod:int, periodkey='__period__'):

  assert { 'delay', 'assignement' }.issubset(X.columns)

  X[periodkey] = np.floor(X['delay']/(period*nperiod))*(period*nperiod)
  X[periodkey] = X[periodkey].astype(int)
  Tg = X.groupby([periodkey, 'assignement']).size().unstack(fill_value=0)
  X = X.drop(columns=periodkey)

  f, ax = plt.subplots(9, figsize=fsize.high, constrained_layout=True, sharex=True, sharey=True)
  Tg.plot.bar(title=f'Wydarzenia dotyczące patentów na dany $T={period}\cdot{nperiod}$ dni okres od początku rejestrów', 
              legend=False, xlabel=f'dzień początku okresu', stacked=False, subplots=True, ax=ax)
  
  return f



def eventn(X:pd.DataFrame):

  assert 'assignement' in X.columns
  
  f, ax = plt.subplots(figsize=fsize.wide)
  X.value_counts('assignement').plot.barh(title='Ogólna liczba wydarzeń w zależności od typu',
                                          ylabel='liczba', ax=ax);

  return f



def eventpat(X:pd.DataFrame):

  assert 'assignement' in X.columns
  assert { 'doc', 'docrepo' }.issubset(X.index.names)

  n = X.groupby(level=['doc', 'docrepo'])['assignement'].value_counts().unstack(fill_value=0)
  n = n[ n.sum(axis=1) > 0 ]
  n.sort_values(by=[k for k in X['assignement'].dtype.categories], ascending=False)

  f, ax = plt.subplots(9, figsize=fsize.high, constrained_layout=True, sharex=True)
  for k, a in zip(n.columns, ax): n[k].value_counts().plot.barh(ax=a)
  ax[0].set_title("Liczba patentów o danej ilości wydarzeń powiązanych")
  ax[-1].set_xlabel('ilość wydarzeń powiązanych');

  return f



def eventsampl(X:pd.DataFrame, n:int):

  assert { 'assignement', 'country', 'number' }.issubset(X.columns)
  assert { 'doc', 'docrepo' }.issubset(X.index.names)

  I = X.index.to_series().sample(n, random_state=42).index
  G = X.loc[I].groupby(level=['doc', 'docrepo'])

  f, ax = plt.subplots(5, 4, sharey=True, figsize=fsize.high, tight_layout=True)
  f.suptitle('Wydarzenia i ich daty dla losowej próbki patentów')
  ax = ax.flatten()
  for i in range(n):
    g = G.get_group(I[i]).set_index('date')['assignement'].sort_index()
    (g.cat.codes+1).plot.line(ax=ax[i], marker='o')
    try:
      l = X.loc[I[i]]['country'].values[0] + X.loc[I[i]]['number'].values[0]
      ax[i].set_title(l)
    except: ax[i].set_title('?')
  for i in range(n):
    ax[i].set_yticklabels(X['assignement'].dtype.categories)
    ax[i].yaxis.set_major_locator(ticker.MaxNLocator(nbins=9))
    ax[i].xaxis.set_major_locator(ticker.MaxNLocator(nbins=3))

  return f



def clsf(X:pd.DataFrame):

  assert { 'section', 'classification' }.issubset(X.columns)
  assert { 'doc', 'docrepo' }.issubset(X.index.names)

  G = X.groupby(['section', 'classification']).size().unstack(fill_value=0)

  f, ax = plt.subplots(3, figsize=fsize.high, constrained_layout=True, sharex=True)
  G.plot.bar(title='Liczność sekcji patentowych w poszczególnych klasyfikacjach', sharex=True,
              xlabel='sekcja', stacked=False, subplots=True, legend=False, ax=ax);

  return f



def nclsf(X:pd.DataFrame):

  assert { 'section', 'classification' }.issubset(X.columns)
  assert { 'doc', 'docrepo' }.issubset(X.index.names)

  X = X.reset_index()
  n = pd.get_dummies(X[['doc', 'docrepo', 'classification']], columns=['classification'], 
                     prefix='', prefix_sep='').groupby(['doc', 'docrepo']).sum()
  f, ax = plt.subplots(figsize=fsize.wide)
  n.reset_index().set_index('doc').groupby('docrepo').sum()\
    .plot.bar(title='Ilość klasyfikacji o w zbiorach danych', ax=ax)

  return f



def npatclsf(X:pd.DataFrame):

  assert { 'section', 'classification' }.issubset(X.columns)
  assert { 'doc', 'docrepo' }.issubset(X.index.names)

  X = X.reset_index()
  n = pd.get_dummies(X[['doc', 'docrepo', 'classification']], columns=['classification'], 
                     prefix='', prefix_sep='').groupby(['doc', 'docrepo']).sum()

  n = (n > 0).astype(int)

  f, ax = plt.subplots(figsize=fsize.wide)
  n.reset_index().set_index('doc').groupby('docrepo').sum()\
    .plot.bar(title='Ilość patentów o poszczególnych klasyfikacjach w zbiorach danych', ax=ax)

  return f



def clsfsampl(X:pd.DataFrame, n:int):

  assert { 'section', 'classification' }.issubset(X.columns)
  assert { 'doc', 'docrepo' }.issubset(X.index.names)

  X = X.reset_index()
  I = X[['doc', 'docrepo']].sample(n, random_state=0)
  Y = X.loc[X['doc'].isin(I['doc'])].fillna('')
  Y = Y.sample(44, random_state=42).sort_index().set_index('doc')

  f, ax = plt.subplots(figsize=fsize.high, constrained_layout=True)
  ax.axis('off')
  f.suptitle('Losowe klasyfikacje patentów dla losowej próbki patentów')
  tab = ax.table(cellText=Y.values, 
                colLabels=Y.columns, 
                rowLabels=Y.index, loc='center')

  for k, u in tab.get_celld().items():
    u.set_facecolor('none')
    u.set_edgecolor('lightgray')

  return f



def geo(X:pd.DataFrame, M:gpd.GeoDataFrame):

  assert { 'lat', 'lon', 'name', 'województwo', 'loceval' }.issubset(X.columns)
  assert { 'doc', 'docrepo' }.issubset(X.index.names)

  f, ax = plt.subplot_mosaic([['A', 'C'], ['B', 'D']], 
                            gridspec_kw={'width_ratios': [1, 1]}, 
                            figsize=(12, 8))

  ax['A'].sharex(ax['B'])

  f.set_constrained_layout(True)

  X = gpd.GeoDataFrame(X, geometry=gpd.points_from_xy(X.lon, X.lat))
  X['name'].value_counts().head(16).plot.barh(title='Najczęściej występujące nazwy w rejestrach patentowych', 
                                            ax=ax['A'], ylabel='liczba');

  X['województwo'].value_counts().plot.barh(title='Liczba patentów w zależności od województwa',
                                          ax=ax['B'], ylabel='liczba');

  X['loceval'].value_counts().plot.pie(title='Sposób określenia geolokalizacji patentu na podstawie nazwy',
                                      ax=ax['C'], ylabel='', autopct='%1.1f%%', colors=['green', 'darkred']);

  M.plot(ax=ax['D'], color='lightgrey')
  ax['D'].set_xlim(14, 25); ax['D'].set_ylim(49, 55)
  X.plot(ax=ax['D'], markersize=5, alpha=0.1)
  ax['D'].set_title(f'Rozrzut geolokalizacji patentów ({X["name"].nunique()} punktów)')

  return f



def geosampl(X:pd.DataFrame, M:gpd.GeoDataFrame, n:int):

  assert { 'lat', 'lon', 'name', 'country', 'number' }.issubset(X.columns)
  assert { 'doc', 'docrepo' }.issubset(X.index.names)

  X = X.reset_index()
  I = X[['doc', 'docrepo']].sample(n, random_state=0)
  G = X.loc[X['doc'].isin(I['doc'])]
  G = gpd.GeoDataFrame(G, geometry=gpd.points_from_xy(G['lon'], G['lat']))

  f, ax = plt.subplots(5, 4, figsize=(fsize.width, fsize.width), constrained_layout=True)
  f.suptitle(f'Geolokalizacje osób powiązanych z losowymi patentami')
  ax = ax.flatten()
  for i in range(n):

    d = I['doc'].iloc[i]
    g = G.loc[G['doc'] == d]

    a = ax[i]
    a.axis('off')
    M.plot(ax=a, color='lightgrey')
    a.set_xlim(14, 25); a.set_ylim(49, 55)
    g.plot(ax=a, markersize=100)
    try: a.set_title(g['country'].values[0] + g['number'].values[0])
    except: a.set_title('?')

  return f



def name(H:list[pd.DataFrame]):

  assert { 'name' }.issubset(H[0].columns)
  assert { 'fname', 'lname', 'role' }.issubset(H[1].columns)
  assert { 'doc', 'docrepo' }.issubset(H[0].index.names)
  assert { 'doc', 'docrepo' }.issubset(H[1].index.names)

  f, ax = plt.subplot_mosaic([['A', 'B', 'C'], ['D', 'D', 'E']], figsize=(12, 12))
  f.set_constrained_layout(True)

  H[1] = H[1].reset_index().set_index(['doc', 'docrepo'])

  H[0]['role'].value_counts().add(H[1]['role'].value_counts(), fill_value=0)\
            .plot.barh(title='Role w pat.',
                       ax=ax['A'], color='orange', ylabel='liczba', xlabel='');

  H[1]['fname'].value_counts().head(16)\
            .plot.barh(title='Najcz. wyst. imiona',
                       ax=ax['B'], color='orange', ylabel='liczba');

  H[1]['lname'].value_counts().head(16)\
            .plot.barh(title='Najcz. wyst. nazwiska',
                       ax=ax['C'], color='orange', ylabel='liczba');

  H[0].assign(name=H[0]['name'].str[:10]+'...')['name'].value_counts().head(16)\
            .plot.barh(title='Najczęściej występujące nazwy w rejestrach patentowych',
                       ax=ax['D'], color='orange', ylabel='liczba');

  H[1].reset_index()['docrepo'].value_counts().add(H[0].reset_index()['docrepo'].value_counts(), fill_value=0)\
    .plot.barh(title='Liczba nazw w zależności\nod repozytorium',
               ax=ax['E'], color='orange', ylabel='liczba');

  return f



def person(personified:pd.DataFrame, q:str):

  from sklearn.preprocessing import LabelEncoder

  X = personified

  assert { 'lat', 'lon', 'delay', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'X' }.issubset(X.columns)

  G = X.dropna(subset=['lat', 'lon', 'delay'])

  uniqletters = lambda x: ','.join(sorted([k for k in set(''.join(x.values))]))

  Y = G.query(q)
  Y['section'] = Y[['A','B','C','D','E','F','G','H','X']]\
                  .apply(lambda r: ''.join(r.index[r == True]), axis=1)

  latdelG = Y.groupby(['lat', 'delay']).agg(size=('lat', 'count'), 
    section=('section', uniqletters)).reset_index()

  londelG = Y.groupby(['lon', 'delay']).agg(size=('lon', 'count'), 
    section=('section', uniqletters)).reset_index()

  latlonG = Y.groupby(['lat', 'lon']).agg(size=('lat', 'count'), 
    section=('section', uniqletters)).reset_index()

  E = LabelEncoder()
  E.fit(pd.concat(x['section'] for x in [Y, latdelG, londelG, latlonG]).drop_duplicates())
  for x in [Y, latdelG, londelG, latlonG]:
    x['section-num'] = x['section'].pipe(E.transform)

  f = plt.figure()
  ax = f.add_subplot(111, projection='3d')
  ax.view_init(elev=15, azim=-45)

  for i in range(len(Y)):
    ax.plot([Y['lat'].iloc[i], Y['lat'].iloc[i]], 
            [Y['lon'].iloc[i], Y['lon'].iloc[i]], 
            [0, Y['delay'].iloc[i]], color='blue', linestyle='dotted')

    ax.plot([Y['lat'].iloc[i], Y['lat'].iloc[i]], 
            [Y['lon'].max()+1, Y['lon'].iloc[i]], 
            [Y['delay'].iloc[i], Y['delay'].iloc[i]], 
            color='red', linestyle='dotted')

  ax.set_xlabel('Latitude')
  ax.set_ylabel('Longitude')
  ax.set_zlabel('Delay')

  grid = [['3d', '2d'], ['2d2', '2d3']]
  f, axs = plt.subplot_mosaic(grid, figsize=(12, 12))

  axs['3d'].axis('off')
  ax3d = f.add_subplot(2, 2, 1, projection='3d')
  
  for a, x, y, z, c, c0 in [ (ax3d, Y['lat'], Y['lon'], Y['delay'], Y['section-num'], Y['section'].unique()),
                      (axs['2d'], latdelG['lat'], latdelG['delay'], latdelG['size'], latdelG['section-num'], latdelG['section'].unique()),
                      (axs['2d3'], londelG['lon'], londelG['delay'], londelG['size'], londelG['section-num'], londelG['section'].unique()),
                      (axs['2d2'], latlonG['lat'], latlonG['lon'], latlonG['size'], latlonG['section-num'], latlonG['section'].unique()) ]:

    s = a.scatter(x, y, s=z*100, c=c) if z.name == 'size' else a.scatter(x, y, z, c=c)
    c = [s.cmap(s.norm(E.transform([k])[0])) for k in c0]
    h = [plt.Line2D([0], [0], marker='o', color='w', 
                    markerfacecolor=x, markersize=10) for x in c[:16]]
 
    if c0.shape[0] > 16:
      w = f'{c0.shape[0] - 16} innych\nkombinacji'
      c0 = c0[:16]
      c0 = list(c0) + [w]
      h.append(plt.Line2D([0], [0], marker='o', color='w', markerfacecolor='none', markersize=10))

    a.legend(h, c0, title="Section")

  ax3d.set_title('Wykres 3-wymiarowy')
  ax3d.view_init(elev=45, azim=-60)
  ax3d.set_xlabel('lat')
  ax3d.set_ylabel('lon')
  ax3d.set_zlabel('delay')

  axs['2d'].set_title('Szerokość geograficzna / opóźnienie')
  axs['2d'].set_xlabel('lat')
  axs['2d'].set_ylabel('delay')
  axs['2d'].yaxis.set_label_position("right")
  axs['2d'].yaxis.tick_right()

  axs['2d3'].set_title('Wysokość geograficzna / opóźnienie')
  axs['2d3'].set_xlabel('lon')
  axs['2d3'].set_ylabel('delay')
  axs['2d3'].yaxis.set_label_position("right")
  axs['2d3'].yaxis.tick_right()

  axs['2d2'].set_title('Szerokość geograficzna / wysokość geograficzna')
  axs['2d2'].set_xlabel('lat')
  axs['2d2'].set_ylabel('lon')
  axs['2d2'].yaxis.set_label_position("right")
  axs['2d2'].yaxis.tick_right()

  for i in range(len(Y)):

    ax3d.plot([Y['lat'].iloc[i], Y['lat'].iloc[i]], 
              [Y['lon'].iloc[i], Y['lon'].iloc[i]], 
              [0, Y['delay'].iloc[i]], color='blue', linestyle='dotted')

    ax3d.plot([Y['lat'].iloc[i], Y['lat'].iloc[i]], 
              [Y['lon'].max()+1, Y['lon'].iloc[i]], 
              [Y['delay'].iloc[i], Y['delay'].iloc[i]], 
              color='red', linestyle='dotted')

  plt.tight_layout()

  return f

@Flow.From()
def plot(bundle, personified):

  C, T, L, R, N, S, G = bundle
  U = personified

  MONDO = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
  def sign(X:pd.DataFrame, x:str=None):
    assert 'name' in X.attrs
    s = X.attrs['name']
    return f'fig/bundle/{s}-{x}.png' if x else f'fig/bundle/{s}.png'

  event(T, 365, 3).savefig(sign(T, 'period'))
  eventn(T).savefig(sign(T, 'n'))
  eventpat(T).savefig(sign(T, 'pat'))
  eventsampl(T, 20).savefig(sign(T, 'sample'))
  clsf(C).savefig(sign(C))
  nclsf(C).savefig(sign(C, 'n'))
  npatclsf(C).savefig(sign(C, 'n-pat'))
  clsfsampl(C, 20).savefig(sign(C, 'sample'))
  geo(G, MONDO).savefig(sign(G))
  geosampl(G, MONDO, 20).savefig(sign(G, 'sample'))
  name([N, S]).savefig(sign(N))
  person(U, q='fname == "PIOTR" and lname == "KOWALSKI"').savefig(sign(U, 'ex-1'))
  person(U, q='fname == "ANTONI" and lname == "LATUSZEK"').savefig(sign(U, 'ex-2'))
  person(U, q='fname == "STANISŁAW" & lname == "BEDNAREK"').savefig(sign(U, 'ex-3'))

import typing, pandas
import pandas as pd, geopandas as gpd

readb = read()
bpersonify: typing.Callable[[], pandas.DataFrame] = personify(readb)
bplot = plot(readb, bpersonify)