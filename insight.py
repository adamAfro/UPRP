import pandas as pd, matplotlib.pyplot as plt, numpy as np
import matplotlib.ticker as ticker


class fsize: 
  width = 8; height = 8
  wide = (8, 4)
  high = (8, 10)

plt.rcParams['figure.figsize'] = [fsize.width, fsize.height]
bundledir = 'bundle'



# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #



t0 = 365
t0f = 3

T = pd.read_csv(bundledir+'/event:pat.csv').set_index(['doc', 'docrepo'])

T['assignement'] = pd.Categorical(T['assignement'], ordered=True,
																	categories=['exhibition', 'office', 'priority', 'regional', 'fill', 'application', 'decision', 'nogrant', 'grant'])#TODO publ fix


T['date'] = pd.to_datetime(T['year'].astype(str) + '-' + T['month'].astype(str) + '-' + T['day'].astype(str))
T['delay'] = np.floor(T['delay']/(t0*t0f))*(t0*t0f)
T['delay'] = T['delay'].astype(int)
Tg = T.groupby(['delay', 'assignement']).size().unstack(fill_value=0)


f, ax = plt.subplots(9, figsize=fsize.high, constrained_layout=True, sharex=True, sharey=True)
Tg.plot.bar(title=f'Wydarzenia dotyczące patentów na dany $T={t0}\cdot{t0f}$ dni okres od początku rejestrów', 
					  legend=False, xlabel=f'dzień początku okresu', stacked=False, subplots=True, ax=ax)

f, ax = plt.subplots(figsize=fsize.wide)
T.value_counts('assignement').plot.barh(title='Ogólna liczba wydarzeń w zależności od typu',
                                       ylabel='liczba', ax=ax);



# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #



TaN = T.groupby(level=['doc', 'docrepo'])['assignement'].value_counts().unstack(fill_value=0)
TaN.sort_values(by=[k for k in T['assignement'].dtype.categories], ascending=False)

f, ax = plt.subplots(9, figsize=fsize.high, constrained_layout=True, sharex=True)
for k, a in zip(TaN.columns, ax): TaN[k].value_counts().plot.barh(ax=a)
ax[0].set_title("Liczba patentów o danej ilości wydarzeń powiązanych")
ax[-1].set_xlabel('ilość wydarzeń powiązanych');



# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #



n = 20
Ti = T.index.to_series().sample(n, random_state=42).index
Gt = T.loc[Ti].groupby(level=['doc', 'docrepo'])

f, ax = plt.subplots(5, 4, sharey=True, figsize=fsize.high, tight_layout=True)
f.suptitle('Wydarzenia i ich daty dla losowej próbki patentów')
ax = ax.flatten()
for i in range(n):
  g = Gt.get_group(Ti[i]).set_index('date')['assignement'].sort_index()
  g.cat.codes.plot.line(ax=ax[i], marker='o')
for i in range(n):
  ax[i].set_yticklabels(T['assignement'].dtype.categories)
  ax[i].yaxis.set_major_locator(ticker.MaxNLocator(nbins=9))
  ax[i].xaxis.set_major_locator(ticker.MaxNLocator(nbins=3))



# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #



f, ax = plt.subplot_mosaic([[str(i), 'bar'] for i in range(3)], 
                           gridspec_kw={'width_ratios': [3, 1]}, 
                           figsize=(12, 8))

C = pd.read_csv(bundledir+'/classification:pat.csv')
Cg = C.groupby(['section', 'classification']).size().unstack(fill_value=0)
Cg.plot.bar(title='Liczność sekcji patentowych w poszczególnych klasyfikacjach', sharex=True, color='blue',
            xlabel='sekcja', stacked=False, subplots=True, legend=False, ax=[ax[str(i)] for i in range(3)]);

C.value_counts('classification').plot.bar(title='Ogólna liczba klasyfikacji\nw danym typie',
                                          ax=ax['bar'], ylabel='liczba', color='blue');



# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #



f, ax = plt.subplot_mosaic([['A', 'B'], ['C', 'D']], 
                           gridspec_kw={'width_ratios': [1, 1]}, 
                           figsize=(12, 8))

f.set_constrained_layout(True)

G = pd.read_csv(bundledir+'/spatial:pat.csv')
G['name'].value_counts().head(16).plot.barh(title='Najczęściej występujące nazwy\nw rejestrach patentowych', 
                                           ax=ax['A'], color='green', ylabel='liczba');

G['województwo'].value_counts().plot.barh(title='Liczba patentów w zależności\nod województwa',
                                         ax=ax['B'], color='green', ylabel='liczba');

G['loceval'].value_counts().plot.pie(title='Sposób określenia geolokalizacji patentu na podstawie nazwy',
                                     ax=ax['C'], ylabel='', autopct='%1.1f%%', colors=['green', 'darkred']);

G.plot.scatter(x='lat', y='lon', title=f'Rozkład geolokalizacji patentów ({G["name"].nunique()} punktów)',
               ax=ax['D'], color='green', s=1, alpha=.05, xlabel='szerokość', ylabel='długość');



# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #



f, ax = plt.subplot_mosaic([['A', 'B', 'C'], ['D', 'D', 'E'], ['G', 'G', 'F']], figsize=(12, 12))
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

pd.Series({ 'imie i nazwisko': P0A.shape[0], 'ogólna nazwa': P0B.shape[0] })\
	.plot.pie(title=f'Liczba osób\nw rejestrach patentowych\nw zależności\nod dostępności nazwiska (n={P0A.shape[0]+P0B.shape[0]})',
						ax=ax['E'], ylabel='', autopct='%1.1f%%', colors=['orange', 'darkred']);

pd.Series({ 
  'z lokacją': (~P0A['city'].isna()).sum() + (~P0B['city'].isna()).sum(), 
  'bez': P0A['city'].isna().sum() + P0B['city'].isna().sum() })\
	.plot.pie(title=f'Liczba osób ze wskazaną lokacją\ni bez niej (n={P0A.shape[0]+P0B.shape[0]})',
						ax=ax['F'], ylabel='', autopct='%1.1f%%', colors=['orange', 'darkred']);

P0A['docrepo'].value_counts().add(P0B['docrepo'].value_counts(), fill_value=0)\
	.plot.barh(title='Liczba osób\nw zależności od repozytorium',
						ax=ax['G'], color='orange', ylabel='liczba');