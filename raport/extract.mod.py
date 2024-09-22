import pandas

def matchkey(text:str, key:str, tol:float):
  key = key.lower()
  words = text.lower().split()
  from difflib import SequenceMatcher
  for word in words:
    if SequenceMatcher(None, key, word).ratio() > tol:
      return True
  return False

def Gfeat(G:pandas.DataFrame, searches:list[list[str]]):
  from pandas import DataFrame
  from numpy import sqrt, sign, inf
  X = DataFrame({'x': G[['xtoplft', 'xtoprgt', 'xbtmrgt', 'xbtmlft']].mean(axis=1),
                 'y': G[['ytoplft', 'ytoprgt', 'ybtmrgt', 'ybtmlft']].mean(axis=1)})
  X['width'] = (G['xtoprgt'] - G['xtoplft'] + G['xbtmrgt'] - G['xbtmlft']) / 2
  X['length'] = G['text'].apply(len)
  X['digits'] = G['text'].apply(lambda x: sum(c.isdigit() for c in x))
  X['words'] = G['text'].apply(lambda x: len(x.split()))
  keys = set([key for search in searches for key in search])
  for k in keys:
    X[f"#{k}"] = G['text'].apply(lambda x: matchkey(x, k, 0.5))
  for s in searches:
    X[f"#{s}"] = X[[f"#{k}" for k in s]].all(axis=1)
    for i, x in X[~X[f"#{s}"] & X[f"#{s[0]}"]].iterrows():
      column = (X['x'] - x['x']).abs() < x['width']/2
      below = X['y'] - x['y'] > 0
      N = X.loc[column & below].copy()
      N['diff'] = (N['x'] - x['x'])**2 + (N['y'] - x['y'])**2
      N = N.sort_values('diff').head(3)
      if N.empty: continue
      if N[f"#{s[1]}"].any(): X.loc[i, f"#{s}"] = True
    X[f"x&{s}"] = X[f"#{s}"].astype(float)#closeness
    X[f"y&{s}"] = X[f"#{s}"].astype(float)#closeness
    X[f"X&{s}"], X[f"Y&{s}"] = 0.0, 0.0
  X = X.drop(columns=[f"#{k}" for k in keys])
  for i, row in X.iterrows():
    for s in searches:
      if row[f"#{s}"]: continue
      mindist = inf
      for j, krow in X[ X[f"#{s}"] == True ].iterrows():
        if i == j: continue
        xd, yd = row['x'] - krow['x'], row['y'] - krow['y']
        dist = sqrt(xd**2 + yd**2)
        if dist >= mindist: continue
        else: mindist = dist
        X.loc[i, f"x&{s}"] = 1/(1 + abs(xd))
        X.loc[i, f"y&{s}"] = 1/(1 + abs(yd))
        X.loc[i, f"X&{s}"] = -1.0 if xd < 0 else 1.0
        X.loc[i, f"Y&{s}"] = -1.0 if yd < 0 else 1.0
  X = X.drop(columns=[f"#{s}" for s in searches])
  return X

def feat(df:pandas.DataFrame, searches:list[str]):
  from concurrent.futures import ProcessPoolExecutor, as_completed
  from pandas import concat
  from tqdm import tqdm
  result=[]
  with ProcessPoolExecutor() as exc:
    fus = [exc.submit(Gfeat, G, searches) for _, G in df.groupby('unit')]
    for fu in tqdm(as_completed(fus), desc="📏", total=len(fus)):
      result.append(fu.result())
  return concat(result, ignore_index=True)

def split(df:pandas.DataFrame, unit:str):
  from sklearn.model_selection import train_test_split
  train, test = train_test_split(df[unit].unique(), test_size=0.2)
  return df[df[unit].isin(train)], df[df[unit].isin(test)]

def neural(nneuro:list[int], drop=0.0):
  from keras.models import Sequential
  from keras.layers import Dense, Input, Dropout
  from keras.optimizers import Adam
  M = Sequential()
  M.add(Input(shape=(nneuro[0],)))
  for d in nneuro[1:]:
    M.add(Dense(d, activation='relu'))
    if drop > 0.0: M.add(Dropout(drop))
  M.add(Dense(1, activation='sigmoid'))
  M.compile(loss='binary_crossentropy', optimizer=Adam(0.001), metrics=['accuracy'])
  return M

searches = [["kategoria", "dokumentu"],
            ["dokumenty", "z", "podana", "identyfikacja"],
            ["odniesienie", "do", "zastrzeżenie"],
            ["podważający", "poziom"],
            ["sprawozdanie", "wykonał"]]

df = pandas.read_csv('labels/labeled.csv')
tr, ts = split(df, unit='unit')
Xtr, ytr = feat(tr, searches), tr['type'] != 0
Xts, yts = feat(ts, searches), ts['type'] != 0

from sklearn import preprocessing as prep
scl = prep.StandardScaler()
Xtr = scl.fit_transform(Xtr)
Xts = scl.transform(Xts)
import joblib
joblib.dump(scl, 'scaler.joblib')

model = neural([Xtr.shape[1], 512, 512, 256], drop=0.5)
his = model.fit(Xtr, ytr, validation_data=(Xts, yts),
                epochs=1000, batch_size=2000, verbose=2).history

model.save('extractor.keras')