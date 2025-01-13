import pandas

def classify(entries: pandas.DataFrame, nameset:pandas.DataFrame,
             valuekey: str, kindkey: str, normkey:str,
             firstname: str,
             lastname: str,
             commonname: str,
             orgname: str,
             idkey:str,
             dockey:str,
             rolekey:str,
             evalkey:str,
             indexkey: str = 'index'):

  X = entries
  X = X.reset_index()
  X.index.name = indexkey
  X = X.set_index([idkey, dockey], append=True)

  wordkey = '__word__'

  N = pandas.concat([
    nameset.query(f'{kindkey} == "{k}"')[valuekey]\
           .str.split(' ').explode().dropna().drop_duplicates()\
           .to_frame().assign(kind=k).rename(columns={valuekey: wordkey}) for k in [commonname, firstname, lastname]
  ], ignore_index=True).drop_duplicates(subset=wordkey)

  X[evalkey] = None
  for k in [firstname, lastname, commonname]:
    if k in X.columns:
      X.loc[ ~X[k].isna(), evalkey ] = 'source'
    else:
      X[k] = None

  X.update(namefill(X, N, firstname, lastname, commonname, 
                    idkey, dockey, kindkey, normkey, valuekey, nchar=2, wordkey=wordkey))

  #TODO: gdy imie jest w tabeli przed nazwiskiem może to sprawić
  # że nazwisko nie zostanie rozpoznane, mimo że słownik wskazuje inaczej

  a = (~X[[firstname, lastname, commonname]].isna()).sum(axis=1) > 0
  A = X[a].reset_index().drop(columns=[valuekey])
  A[evalkey] = A[evalkey].fillna('nameset')
  A = A.drop_duplicates(subset=[dockey, firstname, lastname])

  B = X[~a].dropna(subset=valuekey).drop(columns=[firstname, lastname, commonname])
  B = B.reset_index().dropna(subset=valuekey)
  B = B.drop_duplicates(subset=[dockey, valuekey])

  B[normkey] = B[valuekey].str.replace(r'\W+', ' ', regex=True).str.strip()
  Z = nameset
  Z[normkey] = Z[valuekey].str.replace(r'\W+', ' ', regex=True).str.strip()

  O = Z.query(f'{kindkey}=="{orgname}"').set_index(normkey)[[kindkey]]
  O[evalkey] = 'nameset'
  if B[evalkey].count() != 0: raise NotImplementedError()
  B = B.drop(columns=[evalkey]).set_index(normkey).join(O, how='left')
  B[rolekey] = B[kindkey].fillna(B[rolekey])

  A = A.set_index([dockey, firstname, lastname, commonname])[[rolekey, evalkey]]
  B = B.drop_duplicates(subset=[dockey, valuekey]).dropna(subset=valuekey)\
       .set_index([dockey, valuekey])[[rolekey, evalkey]]

  return A, B

def namefill(entries:pandas.DataFrame, fill:pandas.DataFrame,
             firstname:str, lastname:str, commonname:str,
             idkey:str, dockey:str, kindkey:str,
             normkey:str, valuekey:str, nchar:int, indexkey:str='index', 
             n0key:str = '__N__', nkey:str = '__n__', wordkey:str = '__word__'):

  X = entries
  N = fill

  N[wordkey] = N[wordkey].str.replace(r'\W+', ' ', regex=True)
  N = N.set_index(wordkey)

  X[wordkey] = X[valuekey].str.replace(rf'\b\w{{,{nchar}}}\b', '', regex=True)
  X[wordkey] = X[wordkey].str.replace(r'\W+', ' ', regex=True).str.strip()
  X = X.dropna(subset=wordkey).copy()
  X[wordkey] = X[wordkey].apply(lambda x: ' '.join([y for y in set(x.split())]))

  X[n0key] = X[wordkey].str.count(' ') + 1
  X[wordkey] = X[wordkey].str.split(' ')

  Z = X[[n0key, wordkey]].explode(wordkey).dropna(subset=wordkey)
  Z = Z.reset_index().set_index(wordkey).join(N, how='inner')

  G = Z.groupby([indexkey, idkey, dockey]).agg({n0key: ['size', 'first']})
  G.columns = [nkey, n0key]
  G = G[ G[nkey] == G[n0key] ].reset_index()

  Z = Z.reset_index().set_index([indexkey, idkey, dockey])
  Z = Z.join(G[[indexkey, idkey, dockey]].set_index([indexkey, idkey, dockey]), how='right')
  Z = Z.pivot_table(index=[indexkey, idkey, dockey], 
                columns=kindkey, values=wordkey, 
                aggfunc=lambda x: ' '.join(x)).reset_index()

  Z = Z.set_index([indexkey, idkey, dockey])

  for k in [firstname, lastname, commonname]:
    X[k] = X[k].fillna(Z[k])

  X = X.reset_index().drop(columns=[wordkey, n0key])
  X = X.set_index([indexkey, idkey, dockey])

  return X

def distinguish(entries: pandas.DataFrame,
                valuekey: str, kindkey: str, normkey:str,
                firstname: str,
                lastname: str,
                commonname: str,
                orgname: str,
                orgqueries: list[str] = [],
                namequeries: list[str] = [],
                orgkeywords: list[str] = [],
                orgkeysubstr: list[str] = []):

  X = entries
  Y = pandas.DataFrame(columns=[valuekey, kindkey])

  for q in orgqueries:

    y = entries.query(q).assign(kind=orgname)
    Y = pandas.concat([Y, y[[valuekey, kindkey]]])
    X = X.loc[ X.index.difference(Y.index) ]

  Nf = X[firstname].dropna().str.split(' ').explode().dropna()
  Nf = pandas.concat([Nf.str.split(' ').explode().dropna()]).drop_duplicates()
  Nf = Nf[ Nf.str.len() > 2 ].rename(valuekey)

  Nl = X[lastname].dropna().str.split(' ').explode().dropna()
  Nl = pandas.concat([Nl.str.split(' ').explode().dropna()]).drop_duplicates()
  Nl = Nl[ Nl.str.len() > 2 ].rename(valuekey)

  N = pandas.concat([Nf, Nl])
  N = N[ N.duplicated(keep=False) ]
  N = N.drop_duplicates()
  Nf = Nf[ ~ Nf.isin(N) ]
  Nl = Nl[ ~ Nl.isin(N) ]

  Y = pandas.concat([ Y,
                      N.to_frame().assign(kind=commonname),
                      Nf.to_frame().assign(kind=firstname),
                      Nl.to_frame().assign(kind=lastname) ])

  for q in namequeries:

    y = X.query(q).assign(kind=commonname)[[valuekey, kindkey]]

    y[valuekey] = y[valuekey].str.replace(r'\W+', ' ', regex=True).str.strip().dropna()

    y[valuekey] = y[valuekey].str.split()
    y = y.explode(valuekey).dropna(subset=valuekey).drop_duplicates(subset=valuekey)

    Y = pandas.concat([Y, y])
    X = X.loc[ X.index.difference(Y.index) ]


  X[normkey] = X[valuekey].str.replace(r'\W+', ' ', regex=True).str.strip()

  X = X.drop(columns=[firstname, lastname]).dropna(subset=valuekey)
  X = namedrop(X, Y, normkey, valuekey, nchar=2)

  X[kindkey] = X[normkey].apply(lambda y: orgname if any(x in y for x in orgkeywords) else None)
  for k in orgkeysubstr:
    X.loc[ X[valuekey].str.contains(k), kindkey ] = orgname

  Y = pandas.concat([Y, X.query(f'{kindkey} == "{orgname}"')[[kindkey, valuekey, normkey]]\
                         .drop_duplicates(valuekey)])

  X = X.query(f'{kindkey} != "{orgname}"').drop(columns=kindkey)

  Y = Y.dropna(subset=valuekey)


  a = Y.query(f'{kindkey} == "{commonname}"')
  b = Y.query(f'{kindkey}.isin(["{firstname}", "{lastname}"])')
  c = a[valuekey].isin(b[valuekey])
  c = c[c].index

  Y = Y.drop(c).drop_duplicates(subset=valuekey)

  return Y

def namedrop(entries:pandas.DataFrame, drop:pandas.DataFrame,
             normkey:str, valuekey:str, nchar:int = 2, indexkey:str='index', 
             n0key:str = '__N__', nkey:str = '__n__', wordkey:str = '__word__'):

  X = entries
  D = drop

  D = D[ D[valuekey].str.len() > nchar ].set_index(valuekey)
  X[wordkey] = X[normkey].str.replace(rf'\b\w{{,{nchar}}}\b', '', regex=True)\
                         .str.strip().dropna()\
                         .apply(lambda x: ' '.join([y for y in set(x.split())]))

  X[n0key] = X[wordkey].str.count(' ') + 1

  X[wordkey] = X[wordkey].str.split(' ')
  Z = X[[n0key, wordkey]].explode(wordkey).dropna(subset=wordkey)

  Z = Z.reset_index().set_index(wordkey).join(D, how='inner')
  Z = Z.reset_index().drop_duplicates([indexkey, wordkey]).set_index(indexkey)

  G = Z.groupby(level=0).agg({n0key: ['size', 'first']})
  G.columns = [nkey, n0key]
  G = G[ G[nkey] == G[n0key] ][[]]

  X = X.loc[X.index.difference(Z.join(G, how='inner').index)]
  X = X.drop(columns=[n0key, wordkey])

  return X