from pandas import read_csv, concat, merge
from tqdm import tqdm as process; process.pandas()
import time

X = read_csv('../docs.csv').reset_index()\
  .rename(columns={'docs':'text', 'index':'docs'})[['docs', 'text']]
D = concat([read_csv('date.num.csv'), 
            read_csv('date.month.csv')])

Dl = D.loc[ :, ['docs', 'day', 'month', 'year'] ]\
      .set_index(['day', 'month', 'year'])
Dl = Dl.join(Dl, ['day', 'month', 'year'], 'inner', 'L', 'R')
Dl = Dl.query('docsL != docsR')
Dl['docsL'], Dl['docsR'] = (Dl[['docsL', 'docsR']].min(axis=1),
                            Dl[['docsL', 'docsR']].max(axis=1))
Dl = Dl.reset_index(drop=False)
Dl = Dl.drop_duplicates(subset=['day', 'month', 'year', 'docsL', 'docsR'])
Dl = Dl.value_counts(subset=['docsL', 'docsR'])\
       .to_frame().rename(columns={'count': "date"})\
       .convert_dtypes({"date": 'int'})

P = concat([read_csv('patent.csv'),
            read_csv('patent.p-.csv')])
P['code'] = P['country'].apply(lambda x: [l.upper() for l in x if l.isalpha()]).str.join('') + \
            P['number'].apply(lambda x: [l for l in x if l.isdigit()]).str.join('')

Pl = P.loc[ :, ['docs', 'code'] ].set_index('code')
Pl = Pl.join(Pl, 'code', 'inner', 'L', 'R')
Pl = Pl.query('docsL != docsR')
Pl['docsL'], Pl['docsR'] = (Pl[['docsL', 'docsR']].min(axis=1),
                            Pl[['docsL', 'docsR']].max(axis=1))
Pl = Pl.reset_index(drop=False)
Pl = Pl.drop_duplicates(subset=['code', 'docsL', 'docsR'])
Pl = Pl.value_counts(subset=['docsL', 'docsR'])\
       .to_frame().rename(columns={'count': "code"})\
       .convert_dtypes({"code": 'int'})

E = read_csv('wikin.csv').query('`score-wikineural` > 0.5')\
  .rename(columns=lambda x: x.replace('wikineural-', ''))\
  .dropna().reset_index(drop=True).drop('score-wikineural', axis=1)\
  .query('text.str.len() > 2')

E.loc[E.query('text.str.lower().str.contains("politechnika")').index, ['ORG', 'LOC']] = True, False
E.loc[E.query('text.str.lower().str.contains("uniwersytet")').index, ['ORG', 'LOC']] = True, False
L = { 'PER': None, 'ORG': None, 'LOC': None }
for k in L.keys():
  print(k, end=' ')
  t0 = time.time()
  Lx = E.loc[ E[k], ['docs', 'text'] ].set_index('text')
  print(Lx.shape[0], end=' ')
  
  Lx = Lx.join(Lx, 'text', 'inner', 'L', 'R')
  Lx = Lx.query('docsL != docsR')
  Lx['docsL'], Lx['docsR'] = (Lx[['docsL', 'docsR']].min(axis=1),
                            Lx[['docsL', 'docsR']].max(axis=1))
  Lx = Lx.reset_index(drop=False)
  Lx = Lx.drop_duplicates(subset=['text', 'docsL', 'docsR'])
  L[k] = Lx.value_counts(subset=['docsL', 'docsR'])\
           .to_frame().rename(columns={'count': k})\
           .convert_dtypes({k: 'int'})
  print(f'{round(time.time() - t0, 3)}s {L[k].shape[0]} ✅')

L = concat(list(L.values()) + [Pl, Dl], axis=1).fillna(0)
L.sort_values(by=["date", 'code', 'PER', 'ORG', 'LOC'], ascending=False, inplace=True)

L['bool'] = (L['date'] > 0).astype(int) + \
            (L['code'] > 0).astype(int) + \
            (L['PER'] > 0).astype(int) + \
            (L['ORG'] > 0).astype(int) + \
            (L['LOC'] > 0).astype(int)

L = L[L['bool'] > 1].reset_index()
L = merge(L, X.add_prefix("L"), left_on='docsL', right_on='Ldocs')
L = merge(L, X.add_prefix("R"), left_on='docsR', right_on='Rdocs')
L['Ltext'] = L['Ltext'].str.replace(r'\W', '')
L['Rtext'] = L['Rtext'].str.replace(r'\W', '')
L = L[L.apply(lambda x: x['Ltext'] == x['Rtext'], axis=1)]
dL = L[['docsL', 'docsR']].rename(columns={'docsR':'docs', 
                                           'docsL': 'duplicate'})
X = X.merge(dL, on='docs', how='left')
X[['docs', 'duplicate']].to_csv('duplicated.csv', index=False)