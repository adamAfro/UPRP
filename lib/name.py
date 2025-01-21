import pandas
from util import strnorm

def mapnames(entries: pandas.DataFrame,

             v: str = 'value',
             a: str = 'assignement',
             y: str = 'role',

             a0:str = 'names',
             a1:str = 'firstnames',
             a2:str = 'lastnames',

             lO:str = 'orgname',
             l1:str = 'firstname',
             l2:str = 'lastname',
             lA:str = 'ambigname',

             orgqueries: list[str] = [],
             orgkeywords: list[str] = [],
             orgkeysubstr: list[str] = [],

             sep=' '):

  X = entries
  Y = pandas.DataFrame()

  Io0 = sum([X[v].str.contains(q) for q in orgkeysubstr])
  O0 = X[Io0 > 0][[v]]
  O0[y] = lO

  X[v] = X[v].apply(strnorm, dropinter=True, dropdigit=True)

  O0 = O0[O0[v].str.len() > 1]
  X = X.loc[ X.index.difference(O0.index) ]
  Y = pandas.concat([Y, O0])

  for q in orgqueries:

    V = entries.query(q)[[v]]
    V[y] = lO

    X = X.loc[ X.index.difference(V.index) ]
    Y = pandas.concat([Y, V])

  Io = X[v].apply(lambda y: any(q in y for q in orgkeywords))
  O = X[Io][[v]]
  O[y] = lO

  X = X.loc[ X.index.difference(O.index) ]
  Y = pandas.concat([Y, O])

  Nf = X[ X[a] == a1 ][v].str.split(sep).explode().dropna().drop_duplicates().to_frame().set_index(v)
  Nl = X[ X[a] == a2 ][v].str.split(sep).explode().dropna().drop_duplicates().to_frame().set_index(v)
  N0 = X[ X[a] == a0 ][v].str.split(sep).explode().dropna().drop_duplicates().to_frame().set_index(v)

  N0 = pandas.concat([N0, Nf.join(Nl, how='inner')]).drop_duplicates()

  Nf = Nf.drop(Nf.join(N0, how='inner').index, errors='ignore')
  Nl = Nl.drop(Nl.join(N0, how='inner').index, errors='ignore')

  N0[y] = lA
  Nf[y] = l1
  Nl[y] = l2

  X = X.loc[ X.index.difference(N0.index.union(Nf.index).union(Nl.index)) ]
  Y = pandas.concat([ Y, N0.reset_index(), Nf.reset_index(), Nl.reset_index() ])

  Y[y] = pandas.Categorical(Y[y], categories=[lO, l1, l2, lA], ordered=True)
  Y = Y.sort_values(y).drop_duplicates(v, keep='last').set_index(v)

  Y = Y[ Y.index != '' ] #WTF

  return Y[y]