from pandas import read_csv, DataFrame, merge, Series, concat
from tqdm import tqdm as progress; progress.pandas()

M0 = read_csv('../meta/union.csv', dtype=str)
M0['index'] = M0.index.astype(str)

IM4U = M0.query('(~number.isna()) & (number.str.len() > 6)')\
  .query("number.str.match(r'^(?:19\d\d|20[012]\d)')").index

IM4A = M0.query('(~applno.isna()) & (applno.str.len() > 6)')\
  .query("applno.str.match(r'^(?:19\d\d|20[012]\d)')").index

M4 = M0.loc[IM4U.union(IM4A)].copy()
M4.loc[IM4U, 'number'] = M4.loc[IM4U, 'number'].str[4:]
M4.loc[IM4A, 'applno'] = M4.loc[IM4A, 'applno'].str[4:]

M0 = concat([M0, M4])
M0['number'] = M0['number'].str.replace(r'^0+', '', regex=True)
M0['applno'] = M0['applno'].str.replace(r'^0+', '', regex=True)

R = merge(read_csv('extract/date.csv', dtype=str).set_index("docs")\
            .drop(columns=['text', 'start', 'end']),
          read_csv('extract/patent.csv', dtype=str).set_index("docs")\
            .drop(columns=['text', 'start', 'end']),
          left_index=True, right_index=True, how="outer")\
          .dropna(subset=['number']).reset_index(drop=False)

R['suffix'] = R['suffix'].str.replace(r'\W', '', regex=True)
R['number'] = R['number'].str.replace(r'^0+', '', regex=True)
R['applno'] = R['number']
R['applday'] = R['day']
R['applmonth'] = R['month']
R['applyear'] = R['year']

C = [['day', 'month', 'year', 'country', 'number', 'suffix'],
     ['applday', 'applmonth', 'applyear', 'country', 'applno', 'suffix'],

     ['day', 'month', 'year', 'country', 'number'],
     ['applday', 'applmonth', 'applyear', 'country', 'applno'],

     ['month', 'year', 'country', 'number', 'suffix'],
     ['applmonth', 'applyear', 'country', 'applno', 'suffix'],

     ['month', 'year', 'country', 'number'],
     ['applmonth', 'applyear', 'country', 'applno'],

     ['year', 'country', 'number', 'suffix'],
     ['applyear', 'country', 'applno', 'suffix'],

     ['year', 'country', 'number'],
     ['applyear', 'country', 'applno'],

     ['country', 'number', 'suffix'],
     ['country', 'applno', 'suffix'],

     ['country', 'number'],
     ['country', 'applno'],
  ]

Y = []
for g, c in progress(enumerate(C), total=len(C)):
  E = R.set_index(c).join(M0.set_index(c), how="inner", lsuffix="L", rsuffix="R")
  Y.extend([{ "loss": g, **e } for e in E[["docs", "index"]].to_dict(orient='records')])
Y = DataFrame(Y).sort_values(by="loss").drop_duplicates(subset=['docs', 'index'])

Series([R['docs'].nunique(), R['docs'].nunique() - Y['docs'].nunique(), Y['docs'].nunique()],
       ['cytowania z kodami', 'c.z k. bez krawędzi', 'c.z k. z kr.'])\
  .plot.barh(title="Cytowania, a krawędzie między metadanymi, a kodami pat.",
            color=['black', 'red', 'green']);

Y['loss'].value_counts().sort_index()\
  .plot.bar(title='Odgległość krawędzi między metadanymi,\na kodami pat. (mniejsze - lepsze)',
            color=['green'] * 8 + ['orange'] * 8, xlabel='odległość', ylabel='n-krawędzi')

Y = Y.merge(M0, on='index')
Y.to_csv('linkage.code.csv', index=False)