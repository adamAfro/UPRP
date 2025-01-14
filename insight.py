import pandas as pd, matplotlib.pyplot as plt, numpy as np, geopandas as gpd
import matplotlib.ticker as ticker

class fsize: 
  width = 8; height = 8
  wide = (8, 4)
  high = (8, 10)

plt.rcParams['figure.figsize'] = [fsize.width, fsize.height]

data = [(pd.read_csv(f'bundle/{f0}.csv', dtype='str'), f0) for f0 in 
        ['classification:pat', 
         'event:pat', 
         'pat:pat-raport-ocr', 'pat', 
         'people:pat-named', 
         'people:pat-signed', 
         'spatial:pat']]

for X, f0 in data:
  X.attrs['sign'] = lambda x=None: f'raport-fig/{f0}-{x}.png' if x else f'raport-fig/{f0}.png'

data = [X for X, f0 in data]

MONDO = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
C, T, R, L, N, S, G = [X.set_index(['doc', 'docrepo']) for X in data]

R.columns = pd.MultiIndex.from_tuples([c[:4] for c in 
             (R.columns + '::::::').str.split('::')])

G['lat'] = G['lat'].astype(float)
G['lon'] = G['lon'].astype(float)

T['delay'] = T['delay'].astype(int)
T['date'] = pd.to_datetime(T['year'] + '-' + T['month'] + '-' + T['day'])
T['assignement'] = pd.Categorical(T['assignement'], ordered=True,
																	categories=['exhibition', 'priority', 'regional', 'fill', 'application', 'nogrant', 'grant', 'decision', 'publication'])

def concat(C, T, L, S, G):

  Y = S.join(L, how='left').fillna('')

  C = C[[ 'classification', 'section' ]].reset_index()
  C = C.drop_duplicates().set_index(['doc', 'docrepo'])

  C = C.query('classification == "IPC"').drop('classification', axis=1)
  C = C.pipe(pd.get_dummies, prefix='', prefix_sep='')\
      .reset_index().groupby(['doc', 'docrepo']).sum()

  Y = Y.join(C, how='left')

  T = T.reset_index().groupby(['doc', 'docrepo'])['delay'].min()
  Y = Y.join(T, how='left')

  G = G[['name', 'lat', 'lon']]
  G = G.rename(columns={'name': 'city'})
  G = G.set_index('city', append=True)

  Y = Y.set_index('city', append=True)
  Y = Y.join(G, how='left')

  Y = Y.replace({'': pd.NA})
  
  return Y

U = concat(C, T, L, S, G)
U.attrs['sign'] = lambda x=None: f'raport-fig/person-{x}.png' if x else f'raport-fig/person.png'

# # # # #

def event(X:pd.DataFrame, period:int, nperiod:int, periodkey='__period__'):

  X[periodkey] = np.floor(X['delay']/(period*nperiod))*(period*nperiod)
  X[periodkey] = X[periodkey].astype(int)
  Tg = X.groupby([periodkey, 'assignement']).size().unstack(fill_value=0)
  X = X.drop(columns=periodkey)

  f, ax = plt.subplots(9, figsize=fsize.high, constrained_layout=True, sharex=True, sharey=True)
  Tg.plot.bar(title=f'Wydarzenia dotyczące patentów na dany $T={period}\cdot{nperiod}$ dni okres od początku rejestrów', 
              legend=False, xlabel=f'dzień początku okresu', stacked=False, subplots=True, ax=ax)
  
  return f

# # # # #

def eventn(X:pd.DataFrame):
  
  f, ax = plt.subplots(figsize=fsize.wide)
  X.value_counts('assignement').plot.barh(title='Ogólna liczba wydarzeń w zależności od typu',
                                          ylabel='liczba', ax=ax);

  return f

# # # # #

def eventpat(X:pd.DataFrame):

  n = X.groupby(level=['doc', 'docrepo'])['assignement'].value_counts().unstack(fill_value=0)
  n = n[ n.sum(axis=1) > 0 ]
  n.sort_values(by=[k for k in T['assignement'].dtype.categories], ascending=False)

  f, ax = plt.subplots(9, figsize=fsize.high, constrained_layout=True, sharex=True)
  for k, a in zip(n.columns, ax): n[k].value_counts().plot.barh(ax=a)
  ax[0].set_title("Liczba patentów o danej ilości wydarzeń powiązanych")
  ax[-1].set_xlabel('ilość wydarzeń powiązanych');

  return f

# # # # #

def eventsampl(X:pd.DataFrame, n:int):

  X = T.index.to_series().sample(n, random_state=42).index
  G = T.loc[X].groupby(level=['doc', 'docrepo'])

  f, ax = plt.subplots(5, 4, sharey=True, figsize=fsize.high, tight_layout=True)
  f.suptitle('Wydarzenia i ich daty dla losowej próbki patentów')
  ax = ax.flatten()
  for i in range(n):
    g = G.get_group(X[i]).set_index('date')['assignement'].sort_index()
    (g.cat.codes+1).plot.line(ax=ax[i], marker='o')
    try: ax[i].set_title(L.loc[X[i]]['country'] + L.loc[X[i]]['number'])
    except: ax[i].set_title('?')
  for i in range(n):
    ax[i].set_yticklabels(T['assignement'].dtype.categories)
    ax[i].yaxis.set_major_locator(ticker.MaxNLocator(nbins=9))
    ax[i].xaxis.set_major_locator(ticker.MaxNLocator(nbins=3))

  return f

# # # # #

def clsf(X:pd.DataFrame):

  G = X.groupby(['section', 'classification']).size().unstack(fill_value=0)

  f, ax = plt.subplots(3, figsize=fsize.high, constrained_layout=True, sharex=True)
  G.plot.bar(title='Liczność sekcji patentowych w poszczególnych klasyfikacjach', sharex=True,
              xlabel='sekcja', stacked=False, subplots=True, legend=False, ax=ax);

  return f

# # # # #

def nclsf(X:pd.DataFrame):

  X = X.reset_index()
  n = pd.get_dummies(X[['doc', 'docrepo', 'classification']], columns=['classification'], 
                     prefix='', prefix_sep='').groupby(['doc', 'docrepo']).sum()
  f, ax = plt.subplots(figsize=fsize.wide)
  n.reset_index().set_index('doc').groupby('docrepo').sum()\
    .plot.bar(title='Ilość klasyfikacji o w zbiorach danych', ax=ax)

  return f

# # # # #

def npatclsf(X:pd.DataFrame):

  X = X.reset_index()
  n = pd.get_dummies(X[['doc', 'docrepo', 'classification']], columns=['classification'], 
                     prefix='', prefix_sep='').groupby(['doc', 'docrepo']).sum()

  n = (n > 0).astype(int)

  f, ax = plt.subplots(figsize=fsize.wide)
  n.reset_index().set_index('doc').groupby('docrepo').sum()\
    .plot.bar(title='Ilość patentów o poszczególnych klasyfikacjach w zbiorach danych', ax=ax)

  return f

# # # # #

def clsfsampl(X:pd.DataFrame, n:int):

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

# # # # #

def geo(X:pd.DataFrame, M:gpd.GeoDataFrame):

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

# # # # #

def geosampl(X:pd.DataFrame, M:gpd.GeoDataFrame, n:int):

  I = X[['doc', 'docrepo']].sample(n, random_state=0)
  G = X.loc[X['doc'].isin(I['doc'])]

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
    g.query(f'doc == {d}').plot(ax=a, markersize=100)

  return f

# # # # #

def name(H:list[pd.DataFrame]):

  f, ax = plt.subplot_mosaic([['A', 'B', 'C'], ['D', 'D', 'E']], figsize=(12, 12))
  f.set_constrained_layout(True)

  H[0] = H[0].reset_index().set_index('doc')

  H[1]['role'].value_counts().add(H[0]['role'].value_counts(), fill_value=0)\
            .plot.barh(title='Role w pat.',
                       ax=ax['A'], color='orange', ylabel='liczba', xlabel='');

  H[0]['fname'].value_counts().head(16)\
            .plot.barh(title='Najcz. wyst. imiona',
                       ax=ax['B'], color='orange', ylabel='liczba');

  H[0]['lname'].value_counts().head(16)\
            .plot.barh(title='Najcz. wyst. nazwiska',
                       ax=ax['C'], color='orange', ylabel='liczba');

  H[1].assign(name=H[1]['name'].str[:10]+'...')['name'].value_counts().head(16)\
            .plot.barh(title='Najczęściej występujące nazwy w rejestrach patentowych',
                       ax=ax['D'], color='orange', ylabel='liczba');

  H[0]['docrepo'].value_counts().add(H[1]['docrepo'].value_counts(), fill_value=0)\
    .plot.barh(title='Liczba nazw w zależności\nod repozytorium',
               ax=ax['E'], color='orange', ylabel='liczba');

  return f

# # # # #

def person(X:pd.DataFrame, q:str):

  G = X.dropna(subset=['lat', 'lon', 'delay'])
  G.drop_duplicates(['fname', 'lname', 'flname', 'lat', 'lon'])\
    .value_counts(['fname', 'lname', 'flname'], dropna=False)

  Y = G.query(q)

  f = plt.figure()
  ax = f.add_subplot(111, projection='3d')
  ax.scatter(Y['lat'], Y['lon'], Y['delay'])
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
  ax3d.set_title('Wykres 3-wymiarowy')
  ax3d.scatter(Y['lat'], Y['lon'], Y['delay'])
  ax3d.view_init(elev=45, azim=-60)
  ax3d.set_xlabel('lat')
  ax3d.set_ylabel('lon')
  ax3d.set_zlabel('delay')

  ads = Y.groupby(['lat', 'delay']).size().reset_index()
  axs['2d'].scatter(ads['lat'], ads['delay'], s=ads[0]*100)
  axs['2d'].set_xlabel('lat')
  axs['2d'].set_ylabel('delay')
  axs['2d'].yaxis.set_label_position("right")
  axs['2d'].yaxis.tick_right()
  axs['2d'].set_title('Szerokość geograficzna / opóźnienie')

  ods = Y.groupby(['lon', 'delay']).size().reset_index()
  axs['2d3'].scatter(ods['lon'], ods['delay'], s=ods[0]*100)
  axs['2d3'].set_xlabel('lon')
  axs['2d3'].set_ylabel('delay')
  axs['2d3'].yaxis.set_label_position("right")
  axs['2d3'].yaxis.tick_right()
  axs['2d3'].set_title('Wysokość geograficzna / opóźnienie')

  aos = Y.groupby(['lat', 'lon']).size().reset_index()
  axs['2d2'].scatter(aos['lat'], aos['lon'], s=aos[0]*100)
  axs['2d2'].set_xlabel('lat')
  axs['2d2'].set_ylabel('lon')
  axs['2d2'].yaxis.set_label_position("right")
  axs['2d2'].yaxis.tick_right()
  axs['2d2'].set_title('Szerokość geograficzna / wysokość geograficzna')

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

# # # # #

event(T, 365, 3).savefig(T.attrs['sign']('period'))
eventn(T).savefig(T.attrs['sign']('n'))
eventpat(T).savefig(T.attrs['sign']('pat'))
eventsampl(T, 20).savefig(T.attrs['sign']('sample'))
clsf(X).savefig(X.attrs['sign']())
nclsf(X).savefig(X.attrs['sign']('n'))
npatclsf(X).savefig(X.attrs['sign']('n-pat'))
clsfsampl(X, 20).savefig(X.attrs['sign']('sample'))
geo(G, MONDO).savefig(G.attrs['sign']())
geosampl(G, MONDO, 20).savefig(G.attrs['sign']('sample'))
name([N, S]).savefig(N.attrs['sign']())
person(U).savefig(U.attrs['sign']('person'))