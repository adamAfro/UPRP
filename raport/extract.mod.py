import os, pandas, sklearn.preprocessing as prep, joblib
os.chdir("/storage/home/ajakubczak/Dokumenty/UPRP/raport")

def matchkey(text:str, key:str, tol:float):
  key = key.lower()
  words = text.lower().split()
  from difflib import SequenceMatcher
  for word in words:
    if SequenceMatcher(None, key, word).ratio() > tol:
      return True
  return False

def Gfeat(G:pandas.DataFrame, keys:list[str]):
  from pandas import DataFrame
  from numpy import sqrt, sign, inf
  X = DataFrame({'x': G[['xtoplft', 'xtoprgt', 'xbtmrgt', 'xbtmlft']].mean(axis=1),
                 'y': G[['ytoplft', 'ytoprgt', 'ybtmrgt', 'ybtmlft']].mean(axis=1)})
  X['width'] = (G['xtoprgt'] - G['xtoplft'] + 
                G['xbtmrgt'] - G['xbtmlft']) / 2
  X['height'] = (G['ytoplft'] - G['ybtmlft'] +
                 G['ytoprgt'] - G['ybtmrgt']) / 2
  X['length'] = G['text'].apply(len)
  X['letters'] = G['text'].apply(lambda x: sum(c.isalpha() for c in x))
  X['digits'] = G['text'].apply(lambda x: sum(c.isdigit() for c in x))
  X['spaces'] = G['text'].apply(lambda x: sum(c.isspace() for c in x))
  X['puncts'] = G['text'].apply(lambda x: sum(c in '.,;:?!' for c in x))
  X['words'] = G['text'].apply(lambda x: len(x.split()))
  for key in keys:
    X[f"#{key}"] = G['text'].apply(lambda x: matchkey(x, key, 0.5))
    X[f"x&{key}"] = X[f"#{key}"].astype(float)#closeness
    X[f"y&{key}"] = X[f"#{key}"].astype(float)#closeness
  for i, row in X.iterrows():
    for key in keys:
      mindist = inf
      for j, krow in X[ X[f"#{key}"] == True ].iterrows():
        if i == j: continue
        xd, yd = row['x'] - krow['x'], row['y'] - krow['y']
        dist = sqrt(xd**2 + yd**2)
        if dist >= mindist: continue
        else: mindist = dist
        X.loc[i, f"x&{key}"] = sign(xd)/(1 + abs(xd))
        X.loc[i, f"y&{key}"] = sign(yd)/(1 + abs(yd))
  return X.drop(columns=[f"#{key}" for key in keys])

def feat(df:pandas.DataFrame, keys:list[str]):
  from concurrent.futures import ProcessPoolExecutor, as_completed
  from pandas import concat
  from tqdm import tqdm
  result=[]
  with ProcessPoolExecutor() as exc:
    fus = [exc.submit(Gfeat, G, keys) for _, G in df.groupby('unit')]
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
  M.compile(loss='binary_crossentropy', optimizer=Adam(0.0005), metrics=['accuracy'])
  return M

keys = ["kategoria", "dokument", "identyfikacja", "odniesienie", "zastrzeżenie",
        "podważający", "poziom", "sprawozdanie", "wykonał"]

df = pandas.read_csv('labels/labeled.csv')
tr, ts = split(df, unit='unit')
Xtr, ytr = feat(tr, keys), tr['type'] != 0
Xts, yts = feat(ts, keys), ts['type'] != 0

scl = prep.StandardScaler()
Xtr = scl.fit_transform(Xtr)
Xts = scl.transform(Xts)
joblib.dump(scl, 'scaler.joblib')

model = neural([Xtr.shape[1], 512, 512, 256], drop=0.0)
his = model.fit(Xtr, ytr, validation_data=(Xts, yts),
                epochs=100, batch_size=2000, verbose=2).history

model.save('extractor.keras')