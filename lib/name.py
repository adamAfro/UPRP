import pandas, unicodedata

def normalize(entries:pandas.Series):

  X = entries.copy()
  X = X.str.upper()
  X = X.str.replace(r'[\W\s]+', ' ', regex=True)
  X = X.str.replace(r'\b(\w{1})\b', '', regex=True)
  X = X.str.strip()
  X = X.apply(lambda x: ''.join([c for c in unicodedata.normalize('NFKD', x)
                                   if not unicodedata.combining(c)]))
  return X

def classify(entries:pandas.DataFrame, namesmap:pandas.DataFrame,

             I = ['doc', 'id'],

             v0: str = 'value',
             n:  str = 'nword',

             v: str = 'norm',
             x: str = 'role',

             lO:str = 'orgname',
             l1:str = 'firstname',
             l2:str = 'lastname',
             lA:str = 'ambigname',

             k1:str = 'firstnames',
             k2:str = 'lastnames',
             kU:str = 'unclear'):
  
  assert isinstance(namesmap, pandas.Series)

  X = entries
  M = namesmap

  X = X.reset_index().drop_duplicates([I[0], v0])

  X[v] = X[v0].pipe(normalize)

 #Exact
  E = X.reset_index().set_index(v).join(M, how='inner')
  E = E.reset_index().set_index(I)
  X = X.set_index(I).drop(E.index)

  X[n] = X[v].str.count(' ') + 1
  X[v] = X[v].str.split(' ')

 #Words
  W = X.drop(v0, axis=1).explode(v)
  W = W.reset_index().set_index(v).join(M[ M.isin([l1, l2, lA]) ], how='inner')
  nW = W.groupby(I).agg({n: 'first', x: 'count'})
  W = nW[ nW[n] == nW[x] ][[]].join(W.reset_index().set_index(I), how='inner')
  X = X.drop([v, n], axis=1).drop(W.index)
  W = W.drop(n, axis=1)

  Y = pandas.concat([E, W]).reset_index().drop_duplicates(I+[v])

 #Org
  O = Y[ Y[x] == lO ]
  O = O.set_index(I)[[v0, v, x]]
  O[kU] = False

 #People
  P = Y[ Y[x] != lO ].groupby(I).agg({v:' '.join})
  Nf = Y[ Y[x] == l1 ].groupby(I).agg({v:' '.join})[v].rename(k1)
  Nl = Y[ Y[x] == l2 ].groupby(I).agg({v:' '.join})[v].rename(k2)
  P = P.join(Nf, how='left').join(Nl, how='left')
  P[kU] = False

 #WORKAROUND: zawiera nazwy firm
  X[v] = X[v0].pipe(normalize)
  P = pandas.concat([X.drop(v0, axis=1), P]).sort_index()
  P[kU] = P[kU].fillna(True)

  return P, O

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

  O0[v] = O0[v].pipe(normalize)
  O0 = O0[O0[v].str.len() > 1]
  X = X.loc[ X.index.difference(O0.index) ]
  Y = pandas.concat([Y, O0])

  X[v] = X[v].pipe(normalize)

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