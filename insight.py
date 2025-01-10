import pandas as pd, matplotlib.pyplot as plt
bundledir = 'bundle'



# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #



f, ax = plt.subplot_mosaic([[str(i), 'bar'] for i in range(9)],
                           gridspec_kw={'width_ratios': [3, 1]}, 
                           figsize=(12, 8))

T = pd.read_csv(bundledir+'/event:pat.csv').set_index(['doc', 'docrepo'])

T['date'] = pd.to_datetime(T['year'].astype(str) + '-' + T['month'].astype(str) + '-' + T['day'].astype(str))
Tg = T.groupby(['delay', 'assignement']).size().unstack(fill_value=0)
Tg.plot.line(marker='o', linestyle='', markersize=1, alpha=.05, color='orange',
             title='Wydarzenia dotyczące patentów na dany dzień od początku rejestrów', sharex=True,
             xlabel='dzień', stacked=False, subplots=True, ax=[ax[str(i)] for i in range(9)]);

T.value_counts('assignement').plot.bar(title='Ogólna liczba wydarzeń\nw zależności od typu',
                                       ax=ax['bar'], ylabel='liczba', color='orange');



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