import pandas, yaml, numpy
from lib.storage import Storage
from lib.name import mapnames
from lib.flow import Flow
from util import strnorm

pandas.set_option('future.no_silent_downcasting', True)

@Flow.From()
def Nameclsf(asnstores:dict[Storage, str],
             assignements = ['names', 'firstnames', 'lastnames', 'ambignames'],
             typeassign='type-name'):

  Y = pandas.DataFrame()

  for assignpath, S in asnstores.items():

    with open(assignpath, 'r') as f:
      S.assignement = yaml.load(f, Loader=yaml.FullLoader)

    for h in ['assignee', 'applicant', 'inventor']:

      K = [f'{k0}-{h}' for k0 in assignements]

      X = pandas.concat([S.melt(f'{k}') for k in K]).set_index(['doc', 'id'])
      X = X[['value', 'assignement']]
      X['assignement'] = X['assignement'].str.split('-').str[0]

      T = S.melt(typeassign).set_index(['doc', 'id'])['value'].rename('type')
      if not T.empty: X = X.join(T, on=['doc', 'id'], how='left')

      Y = pandas.concat([Y, X]) if not Y.empty else X

  return mapnames(Y.reset_index(drop=True), 
                  orgqueries=['type.str.upper() == "LEGAL"'],
                  orgkeysubstr=['&', 'INTERNAZIO', 'INTERNATIO', 'INC.', 'ING.', 'SP. Z O. O.', 'S.P.A.'],
                  orgkeywords=[x for X in [ 'THE', 'INDIVIDUAL', 'CORP',
                                            'COMPANY PRZEDSIEBIORSTWO FUNDACJA INSTYTUT INSTITUTE',
                                            'HOSPITAL SZPITAL',
                                            'SZKOLA',
                                            'COMPANY LTD SPOLKA LIMITED GMBH ZAKLAD PPHU',
                                            'KOPALNIA SPOLDZIELNIA FABRYKA',
                                            'ENTERPRISE TECHNOLOGY',
                                            'LLC CORPORATION INC',
                                            'MIASTO GMINA URZAD',
                                            'GOVERNMENT RZAD',
                                            'AKTIENGESELLSCHAFT KOMMANDITGESELLSCHAFT',
                                            'UNIWERSYTET UNIVERSITY AKADEMIA ACADEMY',
                                            'POLITECHNIKA'] for x in X.split()])

@Flow.From()
def Pull(storage:Storage, assignpath:str,
            assignements = ['firstnames', 'lastnames'],
            assignentities = ['names', 'ambignames'],
            cityassign='city'):

  S = storage
  with open(assignpath, 'r') as f:
    S.assignement = yaml.load(f, Loader=yaml.FullLoader)

  Y = pandas.DataFrame()

  C = S.melt(cityassign).set_index(['doc','id'])['value'].rename('city')

  i = 0
  for h in ['assignee', 'applicant', 'inventor']:

    X0 = pandas.concat([S.melt(f'{k}') for k in [f'{k0}-{h}' for k0 in assignements]])
    if not X0.empty:
      X0 = X0.set_index(['doc', 'id']).join(C, how='left').reset_index()
      X0['id'] = X0.groupby(['doc', 'id']).ngroup() + i
      i = X0['id'].max() + 1

    X = pandas.concat([S.melt(f'{k}') for k in [f'{k0}-{h}' for k0 in assignentities]])
    if not X.empty:
      X = X.set_index(['doc', 'id']).join(C, how='left').reset_index()
      X['id'] = X.groupby(['doc', 'id', 'frame', 'col']).ngroup() + i
      i = X['id'].max() + 1

    if 'city' not in X.columns: X['city'] = None
    if 'city' not in X0.columns: X0['city'] = None

    X = pandas.concat([X0[['doc', 'id', 'value', 'city']], 
                       X[['doc', 'id', 'value', 'city']]])

    X = X.set_index(['doc', 'id'])
    X[h] = True

    Y = pandas.concat([Y, X]) if not Y.empty else X

 #Roles
  Y[['assignee', 'applicant', 'inventor']] = Y[['assignee', 'applicant', 'inventor']].fillna(False)
  Y = Y.reset_index().groupby(['doc', 'id', 'city'], dropna=False)\
       .agg({'assignee': 'max', 'applicant': 'max', 'inventor': 'max', 
             'value': ' '.join }).reset_index()

 #Normalize
  Y['city'] = Y['city'].apply(strnorm, dropinter=True, dropdigit=True)
  Y['value'] = Y['value'].apply(strnorm, dropinter=False, dropdigit=False)

 #Traktowanie powtórzeń wynikających z nazw miast jako inne rejestry
  Y = Y.drop_duplicates(['doc', 'value', 'city'])

 #Concat'val
  Y['id'] = numpy.arange(Y.shape[0])
  Y = Y.set_index('id').drop_duplicates()
  assert { 'id' }.issubset(Y.index.names) and Y.index.is_unique
  assert { 'doc', 'value', 'city', 
           'assignee', 'applicant', 'inventor' }.issubset(Y.columns)

  return Y

@Flow.From()
def Textual(pulled:pandas.DataFrame, nameset:pandas.DataFrame):

  X = pulled
  M = nameset

  assert { 'id' }.issubset(X.index.names) and X.index.is_unique
  assert X.duplicated(['doc', 'value', 'city']).sum() == 0
  assert { 'doc', 'value', 'city', 
           'assignee', 'applicant', 'inventor' }.issubset(X.columns)

 #exact
  X = X.reset_index()
  E = X.set_index('value').join(M, how='inner')
  E = E.reset_index().set_index(['doc', 'id', 'city'])
  X = X.set_index(['doc', 'id', 'city']).drop(E.index)

 #split
  X['value'] = X['value'].apply(strnorm, dropinter=True, dropdigit=True)
  X['nword'] = X['value'].str.count(' ') + 1
  X['value'] = X['value'].str.split(' ')

 #word
  W = X.reset_index().explode('value')
  W = W.set_index('value').join(M[ M.isin(['firstname', 'lastname', 'ambigname']) ], how='inner')
  nW = W.groupby(['doc', 'id', 'city']).agg({'nword': 'first', 'role': 'count'})
  W = nW[ nW['nword'] == nW['role'] ][[]].join(W.reset_index().set_index(['doc', 'id', 'city']), how='inner')
  X = X.drop('nword', axis=1).drop(W.index)
  X['value'] = X['value'].apply(' '.join)
  W = W.drop('nword', axis=1)

  Y = pandas.concat([E, W]).reset_index().drop_duplicates(['doc', 'city', 'value'])

 #org
  O = Y[ Y['role'] == 'orgname' ]
  O = O.set_index('id').drop('role', axis=1)
  O['organisation'] = True

  assert { 'id' }.issubset(O.index.names)
  assert { 'organisation'  }.issubset(O.columns)

 #people
  agg = lambda X: X.agg({'value':' '.join, **{k:'max' for k in ['assignee', 'inventor', 'applicant'] }})
  P  = Y[ Y['role'] !=   'orgname' ].groupby(['doc', 'id', 'city']).pipe(agg)
  Nf = Y[ Y['role'] == 'firstname' ].groupby(['doc', 'id', 'city']).pipe(agg)['value'].rename('firstnames')
  Nl = Y[ Y['role'] ==  'lastname' ].groupby(['doc', 'id', 'city']).pipe(agg)['value'].rename('lastnames')
  P = P.join(Nf, how='left').join(Nl, how='left').reset_index().set_index('id')
  P['organisation'] = False

  assert { 'id' }.issubset(P.index.names)
  assert { 'organisation', 'firstnames', 'lastnames' }.issubset(P.columns)

  X = X.reset_index().set_index('id')
  X = X.loc[X.index.difference(P.index).difference(O.index)]
  Y = pandas.concat([P, O, X])

  assert { 'id' }.issubset(Y.index.names) and Y.index.is_unique
  assert { 'doc', 'value', 'firstnames', 'lastnames', 'city', 
           'organisation', 'assignee', 'applicant', 'inventor' }.issubset(Y.columns)

  return Y

@Flow.From()
def Spacetime(textual:pandas.DataFrame, 
              geoloc:pandas.DataFrame, 
              event:pandas.DataFrame, 
              clsf:pandas.DataFrame):

  X = textual

  T = event
  X = X.reset_index().set_index('doc')\
      .join(T.groupby('doc')['date'].min(), how='left').reset_index()
  X = X.rename(columns={'date': 'firstdate'})

  A = T[ T['event'] == 'application' ].reset_index().drop_duplicates(['doc', 'date'])
  X = X.set_index('doc')\
       .join(A.set_index('doc')['date'], how='left').reset_index()
  X = X.rename(columns={'date': 'application'})

  G = geoloc.set_index('city', append=True)
  X = X.set_index(['doc', 'city']).join(G, how='left').reset_index()

  X = X.set_index('doc')
  C0 = clsf
  C0['clsf'] = C0['section']
  C0 = pandas.get_dummies(C0[['clsf']], prefix_sep='-')
  C0 = C0.groupby('doc').sum()
  X = X.join(C0, how='left')

  for c in ['IPC', 'IPCR', 'CPC']:

    C = clsf[ clsf['classification'] == c ]
    C = C.reset_index().drop_duplicates(['doc', 'classification', 'section'])
    C = C.pivot_table(index='doc', columns='classification', 
                      values='section', aggfunc=list)
    X = X.join(C, how='left')

  X = X.reset_index().set_index('id')

  assert { 'doc', 'lat', 'lon', 'loceval', 'firstdate', 'application', 'organisation' }.issubset(X.columns)
  assert any([ c.startswith('clsf-') for c in X.columns ])
  assert { 'id' }.issubset(X.index.names)

  return X

from util import data as D
from profiling import flow as f0
from patent import flow as fP

FN0 = Nameclsf({ D['UPRP']+'/assignement.yaml':   f0['UPRP']['profiling'],
                D['Lens']+'/assignement.yaml':   f0['Lens']['profiling'],
                D['Google']+'/assignement.yaml': f0['Google']['profiling'] }).map('registry/names.pkl')

F0 = Pull(f0['UPRP']['profiling'], assignpath=D['UPRP']+'/assignement.yaml').map('registry/pulled.pkl')

FN = Textual(F0, nameset=FN0).map('registry/textual.pkl')

FGT = Spacetime(FN, fP['UPRP']['geoloc'], fP['UPRP']['event'], 
                  fP['UPRP']['classify']).map('registry/spacetime.pkl')

flow = { 'registry': {'2013': Flow(callback=lambda *X: X[0][ (X[0]['firstdate'] > '2013-01-01') | (X[0]['application'] > '2013-01-01') ], args=[FGT]),
                      'pull': F0, 
                      'spacetime':FGT } }