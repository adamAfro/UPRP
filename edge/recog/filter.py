from pandas import read_csv
X = read_csv('docs.csv', dtype=str)
for normalized, Q in {
  "-": "−–—", ",": "‚、", ">": "›",
  "...": "…", "'": "`‘’′´", '"': "“”„", "+": "⁺",
  "A": "Ａ", "C": "Ｃ", "D": "Ｄ",
  "E": "Ｅ", "I": "Ｉ", "L": "Ｌ", "Y": "Ｙ" }.items():
  for q in Q: X["docs"] = X["docs"].str.replace(q, normalized)

X['docs'] = X['docs'].fillna('')
X['category'] = X['category'].fillna('')
X['claims'] = X['claims'].fillna('')

X['cnorm'] = (X['category'].str
  .extract(r'(\w+)', expand=False).str
  .upper().fillna(''))
X['dnorm'] = (X['docs'].str
  .lower().replace("\W+", " ", regex=True)
  .replace("\s+", " ", regex=True)).fillna('')

# sprawdzane ręcznie, czy rekordy w ogóle mogą być danymi
X['datable'] = ~X.index.isin([
                              3075,
                                     ])

# sprawdzane ręcznie właściwe rozpoznania OCR - zgodnie ze znakami
X['rvalid'] = X.index.isin([
                            172694, 
                            172695,
                                    ])

X['cvalid'] = X['cnorm'].apply(lambda x: all(c in 'AELOPTXYD' for c in x ))

X['dvalid'] = X['docs'].str.len() >= 7
X['dvalid'] = ~X['dnorm'].apply(lambda x: "dokumenty z poda" in x)&X['dvalid']
X['dvalid'] = ~X['dnorm'].apply(lambda x: ("dokument" in x) and len(x) < 10)&X['dvalid']

X = X.loc[(X['datable']&X['cvalid']&X['dvalid'])|X['rvalid']]
X[['P', 'page', 
   'category', 'docs', 'claims']].to_csv('../docs.csv', index=False)