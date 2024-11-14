from pandas import read_csv, concat, to_datetime

P = concat([
  read_csv("api.lens.org/patents.csv", dtype=str)\
    .assign(source="api.lens.org"),
  read_csv("bulkdata.uspto.gov/patents.csv", dtype=str)\
    .assign(source="bulkdata.uspto.gov"),
  read_csv("api.uprp.gov.pl/patents.csv", dtype=str)\
    .assign(source="uprp.gov.pl") ])

P['publ'] = to_datetime(P['publ'], 
                        dayfirst=False, 
                        format='mixed', 
                        errors='coerce')
P['appl'] = to_datetime(P['appl'], 
                        dayfirst=False, 
                        format='mixed', 
                        errors='coerce')

P['year'] = P['publ'].dt.year.astype('Int64')
P['month'] = P['publ'].dt.month.astype('Int64')
P['day'] = P['publ'].dt.day.astype('Int64')
P['applyear'] = P['appl'].dt.year.astype('Int64')
P['applmonth'] = P['appl'].dt.month.astype('Int64')
P['applday'] = P['appl'].dt.day.astype('Int64')
P = P.drop(columns=['publ', 'appl'])

N = concat([read_csv("api.lens.org/names.csv", dtype=str)\
              .assign(source="api.lens.org"),
            read_csv("bulkdata.uspto.gov/names.csv", dtype=str)\
              .assign(source="bulkdata.uspto.gov"),
            read_csv("names.csv", dtype=str)\
              .assign(source="uprp.gov.pl")])
N['name'] = N['name'].fillna(N['first'].fillna("") + " " + N['last'].fillna(""))
N = N[["country", "number", "suffix", "name", "source"]]

O = concat([read_csv("bulkdata.uspto.gov/org.csv", dtype=str)\
              .assign(source="bulkdata.uspto.gov")\
              .rename(columns={"orgname": "name"}),
            read_csv("org.csv", dtype=str)\
              .assign(source="uprp.gov.pl")]).drop(columns=['namelang'])

G = read_csv("cities.csv", dtype=str).assign(source="uprp.gov.pl")

P.to_csv("patents.csv", index=False)
N.to_csv("names.csv", index=False)
O.to_csv("org.csv", index=False)
G.to_csv("cities.csv", index=False)