class Search:
  
  def __init__(self, targets:list[str], tolerance:float):
    self.searches = [t.split() for t in targets if len(t) > 3]
    self.tolerance = tolerance
    self.keys = set([key for t in self.searches for key in t])
    
  def within(self, X:list[str], I:list[any], linebreak:callable):
    Y = []
    for i, x in zip(I, X):
      Y.append([Search.textscore(x, i, Q, linebreak, 1 - self.tolerance) 
                for Q in self.searches])      
    return Y

  def textscore(text:str, index:int, query:list[str], linebreak:callable, treshold:float):
    k = Search.score(text.split(), index, query, linebreak, treshold)
    y = k/len(query)
    return round(y, 1)

  def score(T:list[str], i:int, Q:list[str], linebreak:callable, h:float):
    y, d = 0, 0
    for w in T:
      if len(w) < 3: continue
      if not Search.fail(w, Q[y], h):
        d = 0
        y += 1
      else:
        d += 1
        if d > 1: return y
        continue
      if y == len(Q): return y
    if y == 0: return 0
    L = linebreak(i)
    if L is None: return y
    yr = max([Search.score(T, i, Q[y:], linebreak, h) for (T, i) in L])    
    return y + yr

  def fail(w:str, s:str, tres:float):
    if abs(len(w) - len(s)) > 0.5*tres*len(s): return True
    from difflib import SequenceMatcher as Matcher
    return Matcher(None, s, w).ratio() < tres

import pandas as pds
from tqdm import tqdm as prg

def linebreak(i, X, h=60):
  x = X.loc[i]
  B = X.loc[X['ytoplft'] >= x['ytoplft']]
  N = B.loc[B['ytoplft'] -  x['ytoplft'] < h].sort_values('ytoplft')
  if N.empty: return None
  return list(zip(N['text'].str.split(), N.index))
  
QH = ["dokument - z podaną identyfikacją",
      "kategoria dokumentu",
      "odniesienie do zastrzeżenia",
      "odniesienie do zastrz"]
QF = ["dalszy ciąg wykazu",
      "dokument podważający",
      "dokument stanowiący",
      "dokument wcześniejszy",
      "uwagi do zgłoszenia"]

Q = QH + QF
search = Search(Q, tolerance=0.3)
X = pds.read_csv('../labels/labeled.csv')
X['data'] = False
for u, U in prg(X.groupby('unit')):
  lbreak = lambda i: linebreak(i, U)
  Y = pds.DataFrame(search.within(U["text"], U.index, lbreak), 
                    index=U.index, columns=Q)
  H = U.loc[ (Y[QH] > 2/3).any(axis=1) ]
  F = U.loc[ (Y[QF] > 2/3).any(axis=1) ]
  lH = H[["ybtmlft", "ybtmrgt"]].max().max() if not H.empty else U["ytoplft"].min()
  lF = F[["ytoplft", "ytoprgt"]].min().min() if not F.empty else U["ybtmlft"].max()
  D = U.loc[ (U["ytoplft"] > lH) & (U["ybtmlft"] < lF) ]
  X.loc[D.index, 'data'] = True

acc = sum((X['data'] == (X['type'] != 0)))/X.shape[0]
print(f"Dokładność: {acc:.2f}%")

notinc = X['data'] < (X['type'] != 0)
print(f"Brakujące: {sum(notinc)} {sum(notinc)/X.shape[0]:.2f}%")

ovrest = X['data'] > (X['type'] != 0)
print(f"Na wyrost: {sum(ovrest)} {sum(ovrest)/X.shape[0]:.2f}%")

X['data'].astype(int).to_csv('table.lab.bin.csv', header=False, index=False)

X = pds.read_csv('paddle.csv')
X['data'] = False
for u, U in prg(X.groupby('unit')):
  try:
    lbreak = lambda i: linebreak(i, U)
    Y = pds.DataFrame(search.within(U["text"], U.index, lbreak), 
                      index=U.index, columns=Q)
    H = U.loc[ (Y[QH] > 0.65).any(axis=1) ]
    F = U.loc[ (Y[QF] > 0.65).any(axis=1) ]
    lH = H["ybtmlft"].max() if not H.empty else U["ytoplft"].min()
    lF = F["ytoplft"].min() if not F.empty else U["ybtmlft"].max()
    D = U.loc[ (U["ytoplft"] > lH) & (U["ybtmlft"] < lF) ]
    X.loc[D.index, 'data'] = True
  except Exception as e: print(e)

X['data'].astype(int).to_csv('table.bin.csv', header=False, index=False)