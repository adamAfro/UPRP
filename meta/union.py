from pandas import read_csv, concat

L = read_csv('ext/api.lens.org/patents.csv', dtype=str)
L['source'] = 'api.lens.org'

L['publ'] = L['publ'].fillna('0000-00-00')
L['year'] = L['publ'].str[:4].astype(int).replace(0, None)
L['month'] = L['publ'].str[5:7].astype(int).replace(0, None) 
L['day'] = L['publ'].str[8:10].astype(int).replace(0, None)

L['appl'] = L['appl'].fillna('0000-00-00')
L['applyear'] = L['appl'].str[:4].astype(int).replace(0, None)
L['applmonth'] = L['appl'].str[5:7].astype(int).replace(0, None) 
L['applday'] = L['appl'].str[8:10].astype(int).replace(0, None)
L = L.drop(columns=['publ', 'appl'])

U = read_csv('ext/bulkdata.uspto.gov/patents.csv', dtype=str)
U['source'] = 'bulkdata.uspto.gov'

U['year'] = U['publ'].str[:4].astype(int)
U['month'] = U['publ'].str[4:6].astype(int) 
U['day'] = U['publ'].str[6:8].astype(int)

U['applyear'] = U['appl'].str[:4].astype(int)
U['applmonth'] = U['appl'].str[4:6].astype(int) 
U['applday'] = U['appl'].str[6:8].astype(int)
U = U.drop(columns=['publ', 'appl'])

# TODO: łączenie patentów polskich
concat([L, U]).to_csv('ext/patents.csv', index=False)