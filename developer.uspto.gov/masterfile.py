import pickle
from pandas import read_csv, to_datetime

X = read_csv('historical_masterfile.csv')
D = [k for k in X.columns if k.endswith('_dt')]
X[D] = X[D].apply(to_datetime, format='%d%b%Y', errors='coerce')
X['jurisdiction'] = X['pubno'].str.extract(r'^(\D+)')
X['kind'] = X['pubno'].str.extract(r'\d(\D\w?)')
X['pubno'] = X['pubno'].str.extract(r'(\d+)')
X['appl_id'] = X['appl_id'].astype(str).str.extract(r'(\d+)\.')

with open('data.pkl', 'wb') as f:
  pickle.dump(X, f)