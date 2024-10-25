from pandas import read_csv, DataFrame, merge, concat

def date(x:str):
  from numpy import nan as missing
  from itertools import product
  import re
  Q1 = re.findall(r'(?<!\d)(\d{1})(?!\d)', x)
  Q1 = [int(q) for q in Q1]
  Q2 = re.findall(r'(?<!\d)(\d{2})(?!\d)', x)
  Q2 = [int(q) for q in Q2]
  D = [q for q in Q1+Q2 if 0 < q < 32]  
  M = [month(x)]
  if (not M) or (M[0] is None):
    M = [d for d in D if 0 < d < 13]
  Q4 = (re.findall(r'(?<!\d)(\d{4})(?!\d)', x))
  Y = Q4 = [int(q) for q in Q4]  
  if not Y: Y = [q for q in Q2]
  Y = [y if y > 99
       else y + 1900 if y < 25
       else y + 2000 for y in Y]
  if D and M:
    n = dict()
    for m in M: n[m] = n.get(m, 0) + 1
    U = product(D, M)
    U = [(a,b) for (a,b) in U if (a != b) or (n[a] > 1) or (n[b] > 1)]
    return [{"d": d, "m": m, "y": y} for d, m in U for y in Y]
  if M: return [{"d": missing, "m": m, "y": y} for m in M for y in Y]
  return [{"d": missing, "m": missing, "y": y} for y in Y]  
def month(x:str):
  for i, M in enumerate(pl):
    if any((m in x) for m in M): return i+1
  for i, M in enumerate(en):
    if any((m in x) for m in M): return i+1
  return None
pl = [["styczeń", "stycznia", "stycz", "sty"],
      ["luty", "lutego", "lut"],
      ["marzec", "marca", "marz", "mar"],
      ["kwiecień", "kwietnia", "kwiec", "kwi", "kw"],
      ["maj", "maja"],
      ["czerwiec", "czerwca", "czerw"],
      ["lipiec", "lipca", "lip"],
      ["sierpień", "sierpnia", "sierp"],
      ["wrzesień", "września", "wrześ"],
      ["październik", "października", "paźdz"],
      ["listopad", "listopada", "list"],
      ["grudzień", "grud"]]
en = [["jan", "january"], 
      ["feb", "february"], 
      ["mar", "march"], 
      ["apr", "april"], 
      ["may"], 
      ["jun", "june"], 
      ["jul", "july"], 
      ["aug", "august"], 
      ["sep", "september"], 
      ["oct", "october"], 
      ["nov", "november"], 
      ["dec", "december"]]

class R:
  import re
  X = read_csv('recog/docs.csv', dtype=str)
  X['index'] = X.index.astype(str)
  X.rename(columns={"P": "raport"}, inplace=True)
  E = read_csv('extract/docs.chunks.csv', dtype=str)

  D = E[ E['name'] == "date" ].apply(axis=1, func=lambda r: 
    [{**d, "docs": r['docs'], "raport": r['raport'] } for d in date(r['content'])])
  D = DataFrame([d for l in D for d in l])
  D['y'] = D['y'].astype("Int64")
  D['m'] = D['m'].astype("Int64")
  D['d'] = D['d'].astype("Int64")

  P = E[E['name'] == "patent"].apply(axis=1, func=lambda r: 
    { "P": ''.join(re.findall(r"\d", r['content'])), "docs": r['docs'], "raport": r['raport'] })
  P = DataFrame(P.tolist())
  P['P'].apply(lambda x: x if len(x) >= 6 else "0"*(6-len(x)) + x)
  P = P[P['P'].str.len() == 6]

class M:
  P = read_csv('../meta/numbers.csv', dtype=str)
  P["P"] = P["P"].replace("-A", "")
  D = read_csv('../meta/dates.csv', dtype=str)
  D['y'] = D['y'].astype("Int64")
  D['m'] = D['m'].astype("Int64")
  D['d'] = D['d'].astype("Int64")

P = R.P[ R.P['P'].isin(M.P['P']) ].copy(); P['number'] = P['P']
N = merge(R.P.rename(columns={"P":"number"}), M.P.dropna(), "inner", on="number")
P = concat([P, N]).drop_duplicates()

D = merge(R.D, M.D, "inner", on=['d', 'm', 'y'])
Y = merge(P, D, "left", on=['docs', 'P', 'raport'])

K = read_csv("linkage.keys.csv", dtype=str)
Y = merge(Y, K, on=['docs', 'P'], how="left")

Y = Y.rename(columns={'docs': 'index', "type": "datetype"})

Y = merge(Y, R.X.drop(columns=['raport']), on='index', how="left")
Y = Y.drop_duplicates(subset=['P', 'index'], keep='first')
Y = Y[["raport", "page", "index",
        "P", "number", "docs",
        "category", "claims",
        "datetype", "d", "m", "y",
        "keys", "common", "words"]].convert_dtypes()

Y = Y.sort_values(["keys", "common", "y"], ascending=False)
Y = Y.drop_duplicates(subset=['index'])
Y = Y[(~Y['datetype'].isna())|(~Y['words'].isna())]
Y.to_csv('docs.linkage.csv')