from pandas import read_csv, concat, to_datetime

Y = []
for f0, s0 in [("patents.csv", "api.uprp.gov.pl"),
              ("ext/api.lens.org/patents.csv", "api.lens.org"),
              ("ext/bulkdata.uspto.gov/patents.csv", "bulkdata.uspto.gov")]:

  X = read_csv(f0, dtype=str)
  X['source'] = s0

  X['publ'] = to_datetime(X['publ'], dayfirst=False, format='mixed', errors='coerce')
  X['year'] = X['publ'].dt.year
  X['month'] = X['publ'].dt.month
  X['day'] = X['publ'].dt.day

  X['appl'] = to_datetime(X['appl'], dayfirst=False, format='mixed', errors='coerce')
  X['applyear'] = X['appl'].dt.year
  X['applmonth'] = X['appl'].dt.month
  X['applday'] = X['appl'].dt.day
  X = X.drop(columns=['publ', 'appl'])

  Y.append(X)

Y = concat(Y).convert_dtypes(str)
Y.to_csv("union.csv", index=False)