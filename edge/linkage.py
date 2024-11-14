from pandas import read_csv

N = read_csv("linkage.code.csv", dtype="str").set_index(['country', 'number', 'docs'])
G = read_csv("linkage.ngram.csv", dtype="str").set_index(['country', 'number', 'docs'])

N['loss'] = N['loss'].astype('int')
G['link'] = G['link'].astype('float')

P = G.pivot_table(index=['country', 'number', 'docs'], columns='name', values='link', aggfunc='sum')\
  .reset_index().set_index(['country', 'number', 'docs'])
N = N.join(P, how='left')
N['city'] = N['city'].fillna(0)
N['names'] = N['names'].fillna(0)
N['orgs'] = N['orgs'].fillna(0)

N.query('loss >= 6')[['city', 'names', 'orgs']].sum(axis=1)\
  .plot.hist(bins=24)

N['quality'] = N.apply(lambda x: 1/1 if x['loss'] < 6 else
                                 1/2 if x['city'] + x['names'] + x['orgs'] > 0.25 else
                                 1/4 if x['city'] > 0 or x['names'] > 0 or x['orgs'] > 0 else
                                 1/8, axis=1)

N['quality'].value_counts().sort_index().plot.bar()
N.reset_index(drop=False)[['docs', 'country', 'number', 'suffix', 'applno', 'quality']]\
 .drop_duplicates(subset=['country', 'docs', 'number'])\
 .to_csv("linkage.code.ngram-quality.csv", index=False)