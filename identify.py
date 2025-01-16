from lib.flow import Flow
from pandas import DataFrame, concat
from bundle import bpersonify

@Flow.From('making base repo for searching')
def mkrepo(personified:DataFrame, sep=';'):

  X = personified

  k0 = 'nameset'
  ki = 'id'

  K0 = ['fname', 'lname', 'flname']
  KD = ['doc', 'docrepo']
  KG = ['lat', 'lon']
  KC = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'X']#IPC

  X[k0] = X[K0].fillna('').agg(sep.join, axis=1)\
               .str.strip(sep).str.replace(r':+', sep, regex=True)\
               .str.split(sep).dropna()\
               .apply(lambda x: sorted(x))\
               .apply(sep.join)

  X = X[[k0]+KG+KC].reset_index().drop('city', axis=1)
  X = X[ X.duplicated(k0, keep=False) ]

  #NA na początek do workaroundu
  X = X.sort_values([k for k in X.columns[::-1]], na_position='first')
  #WORKAROUND - duplikatów nie powinno być - wymagana poprawa w poprz. etap.
  X = X.drop_duplicates(KD+[k0], keep='last')
  assert X[KD+[k0]].duplicated().sum() == 0

  X.index.name = ki
  assert X.index.is_unique

  return X

@Flow.From('getting affilations')
def affilate(repo:DataFrame):

  import tqdm

  X = repo

  KD = ['doc', 'docrepo']
  KG = ['lat', 'lon']

  G = X.groupby(KD)

  Y = DataFrame(index=X.index, data={ 'nameaffil': [set() for i in range(X.shape[0])],
                                      'geoaffil': [set() for i in range(X.shape[0])] })

  for d, g in tqdm.tqdm(G, total=G.ngroups):

    for i, x in g.iterrows():

      if g.shape[0] == 1: continue
      else: g = g.drop(i)

      Y.loc[i, 'nameaffil'].update(g.index.values)
      Y.loc[i, 'geoaffil'].update(tuple(q) for q in g[KG].dropna().values)

  return Y

@Flow.From('calculating similarity')
def simcalc(repo:DataFrame, affilation:DataFrame, qcount:str):

  import tqdm

  N = repo
  A = affilation

  k0 = 'nameset'
  ki = 'id'
  km = 'match'
  kn = 'nameaffil'
  kg = 'geoaffil'
  kgm = 'geomatch'
  kcm = 'clsfmatch'
  kcd = 'clsfdiff'

  KG = ['lat', 'lon']
  KC = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'X']#IPC

  #BIAS #WORKAROUND #TODO: potrzebna optymalizacja, żeby usunąć, np. fragmetnacja
  N = N[ N[k0].isin(N.value_counts(k0).to_frame().query(qcount).index) ]

  N = N.reset_index().set_index(k0)
  Y = N[[ki]+KG+KC].join(N[[ki]+KG+KC].rename({ ki: km }, axis=1), rsuffix='R')

  Y = Y.query(f'{ki} != {km}')
  Y['min'] = Y[[ki, km]].min(axis=1)
  Y['max'] = Y[[ki, km]].max(axis=1)
  Y = Y.drop_duplicates(['min', 'max']).drop(['min', 'max'], axis=1)
  Y = Y.set_index([ki, km])

  #geo
  Y[kgm] = sum([Y[k] == Y[k+'R'] for k in KG]) == 2
  Y = Y.drop(KG+[k+'R' for k in KG], axis=1)

  #clsf
  Y[kcm] = sum([(~Y[k].isna()) & (Y[k] != 0) & (Y[k] == Y[k+'R']) for k in KC])
  Y[kcm].value_counts()

  Y[kcd] = sum([(~Y[k].isna()) & (Y[k] != 0) & (Y[k] != Y[k+'R']) for k in KC])
  Y[kcd].value_counts()

  Y = Y.drop(KC+[k+'R' for k in KC], axis=1)

  Y[kn] = 0
  Y[kg] = 0

  for (i, j) in tqdm.tqdm(Y.index):

    Y.loc[(i,j), kn] = len(A[kn].loc[i].union(A[kn].loc[j]))
    Y.loc[(i,j), kg] = len(A[kg].loc[i].union(A[kg].loc[j]))

  return Y



def simplot(sim:DataFrame):

  import matplotlib.pyplot as plt

  Z = sim

  f, ax = plt.subplots(2, 3, figsize=(15, 10), tight_layout=True)

  ax[1, 0].axis('off')

  if 'geomatch' in Z.columns:
    Z['geomatch'].replace({ True: "identyczne", False: "inne lub brak" }).value_counts()\
                .plot.pie(title='Dopasowania geolokalizacyjne\nw identycznych wpisach', ylabel='',
                          ax=ax[0, 0], autopct='%1.1f%%');
  else: 
    ax[0, 0].axis('off')

  Z.value_counts('geoaffil').sort_index()\
   .plot.bar(title='Ilość miejsc współautorów wspólnych\nw identycznych wpisach',
             ylabel='ilość miejsc współautorów', xlabel='ilość par identycznych podpisów',
             ax=ax[0, 1]);

  Z.value_counts(['clsfmatch', 'clsfdiff']).unstack().fillna(0)\
  .rename_axis('niezgodne\nsekcje klasyf.', axis=1).sort_index()\
  .plot.bar(title='Dopasowania i różnice klasyfikacyjne\nw identycznych wpisach',
            ylabel='', xlabel='ilość identycznych sekcji klasyfikacji', ax=ax[0, 2]);

  ZN = Z.value_counts('nameaffil').reset_index()

  (ZN.query('count > 1000').set_index('nameaffil').sort_index()/1000)\
    .plot.barh(title='Ilość współautorów wspólnych\nw identycznych wpisach', legend=False,
              ylabel='ilość współautorów (n > 1000)',
              xlabel='ilość par identycznych podpisów (w tysiącach)',
              ax=ax[1, 2])

  ZN.query('count < 1000')['count']\
    .plot.hist(title='Ilość współautorów wspólnych\nw identycznych wpisach',
              ylabel='ilość współautorów (n < 1000)', xlabel='ilość par identycznych podpisów',
              bins=12, ax=ax[1, 1]);

  return f

@Flow.From('select only valid results')
def pick(sim:DataFrame):

  S = sim
  G = S[ S['geomatch'] == True ].drop('geomatch', axis=1)
  Z = S[ S['geomatch'] == False ].drop('geomatch', axis=1)

  I = concat([G.query('~((clsfdiff > 0) & (clsfmatch == 0))')\
               .query('(nameaffil > 0) | (geoaffil > 0)'),
              Z.query('(clsfdiff == 0) | (clsfmatch > 0)')\
               .query('nameaffil > 2') ])

  return I.index.to_frame().reset_index(drop=True)

@Flow.From('identify authors of documents')
def identify(searchrepo:DataFrame, picked:DataFrame):

  import networkx as nx

  N = searchrepo
  E = picked

  G = nx.Graph()
  G.add_nodes_from(N.index)
  G.add_edges_from([tuple(e) for e in E.values])

  P = DataFrame({'registry': list(nx.connected_components(G))})
  P.index.name = 'person'

  L = N.join(P.explode('registry').reset_index().set_index('registry'))\
       .set_index(['doc', 'docrepo'])['person']

  P['example'] = P['registry'].apply(lambda x: next(iter(x)))
  P = P.reset_index().set_index('example').join(N['nameset'])\
       .set_index('person')['nameset']

  return P, L

searchrepo = mkrepo(bpersonify).map('computed/searchrepo.pkl')
affilation = affilate(searchrepo).map('computed/affilation.pkl')
sim00 = simcalc(searchrepo, affilation, 'count  < 20')
sim20 = simcalc(searchrepo, affilation, 'count >= 20')
sim = Flow(callback=lambda X: concat(X), args=[[sim00, sim20]]).map('computed/similarity.pkl')

@Flow.From()
def plot(similarity:DataFrame):

  S = similarity
  Z = S[ S['geomatch'] == False ].drop('geomatch', axis=1)
  G = S[ S['geomatch'] == True ].drop('geomatch', axis=1)

  simplot(S).savefig('fig/identify-similarity.png');
  simplot(G).savefig('fig/identify-similarity-geo.png');
  simplot(Z).savefig('fig/identify-similarity-nogeo.png');

splot = plot(sim)
authorship = identify(searchrepo, pick(sim))