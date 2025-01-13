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
             indexkey: str = 'index'):

  X = entries
  X = X.reset_index()
  X.index.name = indexkey
  X = X.set_index([idkey, dockey], append=True)

  wordkey = '__word__'
  n0key = '__N__'
  nkey = '__n__'

  N = pandas.concat([
    nameset.query(f'{kindkey} == "{k}"')[valuekey]\
           .str.split(' ').explode().dropna().drop_duplicates()\
           .to_frame().assign(kind=k).rename(columns={valuekey: wordkey}) for k in [commonname, firstname, lastname]
  ], ignore_index=True).drop_duplicates(subset=wordkey)

  N[wordkey] = N[wordkey].str.replace(r'\W+', ' ', regex=True)
  N = N.set_index(wordkey)
  X[wordkey] = X[valuekey].str.replace(r'\b\w{,2}\b', '', regex=True)\
                       .str.strip().dropna().str.replace(r'\W+', ' ', regex=True)\
                       .apply(lambda x: ' '.join([y for y in set(x.split())]))

  X[n0key] = X[wordkey].str.count(' ') + 1
  X[wordkey] = X[wordkey].str.split(' ')

  # ustalenie że nazwy stają się imieniem i nazwiskiem o ile
  # wszystkie (każde!) słowa w nazwie są w bazie słów imion i nazwisk
  Z = X.explode(wordkey).dropna(subset=wordkey)\
  .reset_index().set_index(wordkey).join(N).dropna(subset=kindkey)

  G = Z.groupby([indexkey, idkey, dockey]).agg({n0key: ['size', 'first']})
  G.columns = [nkey, n0key]
  G = G[ G[nkey] == G[n0key] ].reset_index()

  Z = Z.reset_index().set_index([indexkey, idkey, dockey])\
  .join(G[[indexkey, idkey, dockey]].set_index([indexkey, idkey, dockey]), how='right')
  Z = Z.reset_index() # jeśli jest za dużo imion
  Z = Z.drop_duplicates(subset=[indexkey, idkey, dockey, kindkey])
  Z = Z.set_index([indexkey, idkey, dockey])

  if firstname not in X.columns: X[firstname] = None
  X[firstname] = X[firstname].combine_first(Z.query(f'{kindkey} == "{firstname}"')[wordkey])
  X[firstname] = X[firstname].combine_first(Z.query(f'{kindkey} == "{commonname}"')[wordkey])

  if lastname not in X.columns: X[lastname] = None
  X[lastname] = X[lastname].combine_first(Z.query(f'{kindkey} == "{lastname}"')[wordkey])
  X[lastname] = X[lastname].combine_first(Z.query(f'{kindkey} == "{commonname}"')[wordkey])

  #TODO: gdy imie jest w tabeli przed nazwiskiem może to sprawić
  # że nazwisko nie zostanie rozpoznane, mimo że słownik wskazuje inaczej

  X = X.reset_index().drop(columns=[wordkey, n0key, idkey])

  A = X.dropna(subset=[firstname, lastname]).drop(columns=[valuekey])\
       .drop_duplicates(subset=[dockey, firstname, lastname])

  B = X.loc[X.index.difference(A.index)].dropna(subset=valuekey)\
       .drop(columns=[firstname, lastname])\
       .drop_duplicates(subset=[dockey, valuekey])

  B[normkey] = B[valuekey].str.replace(r'\W+', ' ', regex=True).str.strip()
  Z = nameset
  Z[normkey] = Z[valuekey].str.replace(r'\W+', ' ', regex=True).str.strip()

  B = B.set_index(normkey).join(Z.query(f'{kindkey}=="{orgname}"').set_index(normkey)[[kindkey]], how='left')
  B[rolekey] = B[kindkey].fillna(B[rolekey])

  A = A.drop_duplicates(subset=[firstname, lastname])\
       .set_index([dockey, firstname, lastname])[[rolekey]]
  B = B.drop_duplicates(subset=valuekey).dropna(subset=valuekey)\
       .set_index([dockey, valuekey])[[rolekey]]

  #TODO nie wszyscy inventorzy są kwalifikowani jako flname a powinni

  return A, B

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
  X = namefilter(X, Y, normkey, valuekey, nchar=2)

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

def namefilter(entries:pandas.DataFrame, drop:pandas.DataFrame,
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