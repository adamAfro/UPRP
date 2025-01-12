import pandas as pd, matplotlib.pyplot as plt, numpy as np, geopandas as gpd
import matplotlib.ticker as ticker


class fsize: 
  width = 8; height = 8
  wide = (8, 4)
  high = (8, 10)

plt.rcParams['figure.figsize'] = [fsize.width, fsize.height]
bundledir = 'bundle'
figdir = 'raport-fig'



# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #



t0 = 365
t0f = 3

T = pd.read_csv(bundledir+'/event:pat.csv').set_index(['doc', 'docrepo'])

T['assignement'] = pd.Categorical(T['assignement'], ordered=True,
																	categories=['exhibition', 'priority', 'regional', 'fill', 'application', 'nogrant', 'grant', 'decision', 'publication'])


T['date'] = pd.to_datetime(T['year'].astype(str) + '-' + T['month'].astype(str) + '-' + T['day'].astype(str))
T['delay'] = np.floor(T['delay']/(t0*t0f))*(t0*t0f)
T['delay'] = T['delay'].astype(int)
Tg = T.groupby(['delay', 'assignement']).size().unstack(fill_value=0)


f, ax = plt.subplots(9, figsize=fsize.high, constrained_layout=True, sharex=True, sharey=True)
Tg.plot.bar(title=f'Wydarzenia dotyczące patentów na dany $T={t0}\cdot{t0f}$ dni okres od początku rejestrów', 
					  legend=False, xlabel=f'dzień początku okresu', stacked=False, subplots=True, ax=ax)

f.savefig(figdir+'/event.png')

f, ax = plt.subplots(figsize=fsize.wide)
T.value_counts('assignement').plot.barh(title='Ogólna liczba wydarzeń w zależności od typu',
                                       ylabel='liczba', ax=ax);

f.savefig(figdir+'/event-n.png')


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #



TaN = T.groupby(level=['doc', 'docrepo'])['assignement'].value_counts().unstack(fill_value=0)
TaN.sort_values(by=[k for k in T['assignement'].dtype.categories], ascending=False)

f, ax = plt.subplots(9, figsize=fsize.high, constrained_layout=True, sharex=True)
for k, a in zip(TaN.columns, ax): TaN[k].value_counts().plot.barh(ax=a)
ax[0].set_title("Liczba patentów o danej ilości wydarzeń powiązanych")
ax[-1].set_xlabel('ilość wydarzeń powiązanych');

f.savefig(figdir+'/event:pat-n.png')



# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #



n = 20
Ti = T.index.to_series().sample(n, random_state=42).index
Gt = T.loc[Ti].groupby(level=['doc', 'docrepo'])

f, ax = plt.subplots(5, 4, sharey=True, figsize=fsize.high, tight_layout=True)
f.suptitle('Wydarzenia i ich daty dla losowej próbki patentów')
ax = ax.flatten()
for i in range(n):
  g = Gt.get_group(Ti[i]).set_index('date')['assignement'].sort_index()
  (g.cat.codes+1).plot.line(ax=ax[i], marker='o')
for i in range(n):
  ax[i].set_yticklabels(T['assignement'].dtype.categories)
  ax[i].yaxis.set_major_locator(ticker.MaxNLocator(nbins=9))
  ax[i].xaxis.set_major_locator(ticker.MaxNLocator(nbins=3))

f.savefig(figdir+'/pat:event.png')



# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #



C = pd.read_csv(bundledir+'/classification:pat.csv', dtype=str)
Cg = C.groupby(['section', 'classification']).size().unstack(fill_value=0)

f, ax = plt.subplots(3, figsize=fsize.high, constrained_layout=True, sharex=True)
Cg.plot.bar(title='Liczność sekcji patentowych w poszczególnych klasyfikacjach', sharex=True,
            xlabel='sekcja', stacked=False, subplots=True, legend=False, ax=ax);

f.savefig(figdir+'/classification.png')



# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #



NC0 = pd.get_dummies(C[['doc', 'docrepo', 'classification']], columns=['classification'], 
                     prefix='', prefix_sep='').groupby(['doc', 'docrepo']).sum()
f, ax = plt.subplots(figsize=fsize.wide)
NC0.reset_index().set_index('doc').groupby('docrepo').sum()\
   .plot.bar(title='Ilość klasyfikacji o w zbiorach danych', ax=ax)

f.savefig(figdir+'/classification.png')


NC0 = (NC0 > 0).astype(int)

f, ax = plt.subplots(figsize=fsize.wide)
NC0.reset_index().set_index('doc').groupby('docrepo').sum()\
   .plot.bar(title='Ilość patentów o poszczególnych klasyfikacjach w zbiorach danych', ax=ax)

f.savefig(figdir+'/pat:classification-n.png')



# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #



n = 10
Ci = C[['doc', 'docrepo']].sample(n, random_state=0)
Cs = C.loc[C['doc'].isin(Ci['doc'])].fillna('')
Cs = Cs.sample(44, random_state=42).sort_index().set_index('doc')

f, ax = plt.subplots(figsize=fsize.high, constrained_layout=True)
ax.axis('off')
f.suptitle('Losowe klasyfikacje patentów dla losowej próbki patentów')
tab = ax.table(cellText=Cs.values, 
               colLabels=Cs.columns, 
               rowLabels=Cs.index, loc='center')

for k, u in tab.get_celld().items():
  u.set_facecolor('none')
  u.set_edgecolor('lightgray')

f.savefig(figdir+'/pat:classification.png')



# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #



mondo = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
f, ax = plt.subplot_mosaic([['A', 'C'], ['B', 'D']], 
                           gridspec_kw={'width_ratios': [1, 1]}, 
                           figsize=(12, 8))

ax['A'].sharex(ax['B'])

f.set_constrained_layout(True)

G = pd.read_csv(bundledir+'/spatial:pat.csv')
G = gpd.GeoDataFrame(G, geometry=gpd.points_from_xy(G.lon, G.lat))
G['name'].value_counts().head(16).plot.barh(title='Najczęściej występujące nazwy w rejestrach patentowych', 
                                           ax=ax['A'], ylabel='liczba');

G['województwo'].value_counts().plot.barh(title='Liczba patentów w zależności od województwa',
                                         ax=ax['B'], ylabel='liczba');

G['loceval'].value_counts().plot.pie(title='Sposób określenia geolokalizacji patentu na podstawie nazwy',
                                     ax=ax['C'], ylabel='', autopct='%1.1f%%', colors=['green', 'darkred']);

mondo.plot(ax=ax['D'], color='lightgrey')
ax['D'].set_xlim(14, 25); ax['D'].set_ylim(49, 55)
G.plot(ax=ax['D'], markersize=5, alpha=0.1)
ax['D'].set_title(f'Rozrzut geolokalizacji patentów ({G["name"].nunique()} punktów)')

f.savefig(figdir+'/spatial.png')



# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #



n = 20
Gi = G[['doc', 'docrepo']].sample(n, random_state=0)
Gg = G.loc[G['doc'].isin(Gi['doc'])]

f, ax = plt.subplots(5, 4, figsize=(fsize.width, fsize.width), constrained_layout=True)
f.suptitle(f'Geolokalizacje osób powiązanych z losowymi patentami')
ax = ax.flatten()
for i in range(n):

  d = Gi['doc'].iloc[i]
  g = Gg.loc[Gg['doc'] == d]

  a = ax[i]
  a.axis('off')
  mondo.plot(ax=a, color='lightgrey')
  a.set_xlim(14, 25); a.set_ylim(49, 55)
  g.query(f'doc == {d}').plot(ax=a, markersize=100)

f.savefig(figdir+'/pat:spatial.png')



# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #



f, ax = plt.subplot_mosaic([['A', 'B', 'C'], ['D', 'D', 'E']], figsize=(12, 12))
f.set_constrained_layout(True)

P0A = pd.read_csv(bundledir+'/people:pat-signed.csv', index_col=0)
P0A = P0A.reset_index().set_index('doc')

P0B = pd.read_csv(bundledir+'/people:pat-named.csv', index_col=0).reset_index().set_index('doc')

P0B['role'].value_counts().add(P0A['role'].value_counts(), fill_value=0)\
					.plot.barh(title='Role w pat.',
         	 				   ax=ax['A'], color='orange', ylabel='liczba', xlabel='');

P0A['fname'].value_counts().head(16)\
 					.plot.barh(title='Najcz. wyst. imiona',
										 ax=ax['B'], color='orange', ylabel='liczba');

P0A['lname'].value_counts().head(16)\
 					.plot.barh(title='Najcz. wyst. nazwiska',
										 ax=ax['C'], color='orange', ylabel='liczba');

P0B.assign(name=P0B['name'].str[:10]+'...')['name'].value_counts().head(16)\
					.plot.barh(title='Najczęściej występujące nazwy w rejestrach patentowych',
									 	 ax=ax['D'], color='orange', ylabel='liczba');

P0A['docrepo'].value_counts().add(P0B['docrepo'].value_counts(), fill_value=0)\
	.plot.barh(title='Liczba nazw w zależności\nod repozytorium',
						ax=ax['E'], color='orange', ylabel='liczba');

f.savefig(figdir+'/people.png')