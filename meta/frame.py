from pandas import read_csv, merge, DataFrame, to_datetime, concat, NA

r = "frames/XML/root/pl-patent-document/"

A = read_csv(r+"bibliographic-data/assignees/assignee/df.csv", dtype=str)
A = merge(A, read_csv(r + "bibliographic-data/assignees/assignee/addressbook/df.csv", dtype=str), 
          left_on="ID", right_on="PID", how="left", suffixes=("-0", "-1"))
A = merge(A, read_csv(r + "bibliographic-data/assignees/assignee/addressbook/name/df.csv", dtype=str), 
          left_on="ID-1", right_on="PID", how="left", suffixes=("-1", "-2"))
A = DataFrame({ "P": A["P-0"], "name": A["$"].str.upper(), "lang": A["&lang"].str.upper() }).drop_duplicates()
A.to_csv("assignment.csv", index=False)

A0 = A[["P", "name"]].copy()
A0['name'] = A0['name'].str.replace(r"[^\w\.]", " ", regex=True)
A0 = A0.dropna().apply(lambda x: [(str(x["P"]), w) for w in x['name'].split()], axis=1)
A0 = DataFrame(A0.explode().tolist(), columns=["P", "name"]).drop_duplicates()
A0.replace("", NA).dropna().to_csv("assignment.chunks.csv", index=False)


G = read_csv(r+"bibliographic-data/assignees/assignee/addressbook/address/df.csv", dtype=str)
G = DataFrame({ "P": G["P"], "name": G["$city"] }).drop_duplicates()
G.to_csv("cities.csv", index=False)

G0 = G[["P", "name"]].copy()
G0['name'] = G0['name'].str.replace(r"[^\w\.]", " ", regex=True)
G0 = G0.dropna().apply(lambda x: [(str(x["P"]), w) for w in x['name'].split()], axis=1)
G0 = DataFrame(G0.explode().tolist(), columns=["P", "name"]).drop_duplicates()
G0["name"] = G0["name"].str.upper()
G0.replace("", NA).dropna().to_csv("cities.chunks.csv", index=False)


N = read_csv(r+"bibliographic-data/parties/inventors/inventor/df.csv")
N = merge(N, read_csv(r+"bibliographic-data/parties/inventors/inventor/addressbook/df.csv"),
          left_on="ID", right_on="PID", how="left", suffixes=("-0", "-1"))
N = merge(N, read_csv(r+"bibliographic-data/parties/inventors/inventor/addressbook/address/df.csv"),
          left_on="ID-1", right_on="PID", how="left", suffixes=("-1", "-2"))
N = DataFrame({"P": N["P"],
               "ID": N["ID-0"],
               "lang": N["&lang"].str.upper(),
               "first": N["$first-name"].str.upper(),
               "last": N["$last-name"].str.upper() })
if N["lang"].nunique() == 1: N = N.drop(columns=["lang"])
N = N.drop_duplicates(subset=["P", "ID", "first", "last"], keep="first").drop(columns=["ID"])
N.to_csv("names.csv", index=False)

N0 = concat([N[['P', 'first']].rename(columns={"first":"name"}), 
             N[['P', 'last']].rename(columns={"last":"name"})])
N0['name'] = N0['name'].str.replace(r"[^\w\.]", " ", regex=True)
N0 = N0.dropna().apply(lambda x: [(str(x["P"]), w) for w in x['name'].split()], axis=1)
N0 = DataFrame(N0.explode().tolist(), columns=["P", "name"]).drop_duplicates()
N0.replace("", NA).dropna().to_csv("names.chunks.csv", index=False)


D = []
for k, t, x in [('&date-publ', 'root-publication', r+"df.csv"),
                ('$date', 'application', r+"bibliographic-data/application-reference/document-id/df.csv"),
                ('$date', 'publication', r+"bibliographic-data/publication-reference/document-id/df.csv"),
                ('$date-publ', 'publication-alt', r+"bibliographic-data/publication-reference/document-id/df.csv"),
                ('$date', 'pre-grant', r+"bibliographic-data/dates-of-public-availability/unexamined-printed-without-grant/document-id/df.csv"),
                ("$date", "fill", r+"bibliographic-data/date-exhibition-filed/df.csv"),
                ("$date", "reg-fill", r+"bibliographic-data/pct-or-regional-filing-data/document-id/df.csv"),
                ("$date", "reg-publication", r+"bibliographic-data/pct-or-regional-publishing-data/document-id/df.csv")]:

  X = read_csv(x, dtype=str)
  X = DataFrame({ "P": X["P"], "date": X[k], "type": t })
  D.append(X)

D = concat(D).replace('~', NA).dropna()
D["date"] = to_datetime(D["date"], format="mixed", dayfirst=False)
D["y"] = D["date"].dt.year
D["m"] = D["date"].dt.month
D["d"] = D["date"].dt.day
D = D.drop(columns=["date"]).drop_duplicates(keep="first")
D.to_csv("dates.csv", index=False)


P = read_csv(r+"df.csv", dtype=str)
P = DataFrame({ "P": P["P"], "number": P["&doc-number"] })
                           # „Numer prawa wyłącznego” #UPRP
P.loc[P['P'] == P['number'], 'number'] = NA
P.to_csv("numbers.csv", index=False)


T = read_csv(r+"bibliographic-data/invention-title/df.csv", dtype=str)
T = DataFrame({ "P": T["P"], "lang": T["&lang"], "name": T["$"] })
T = T.dropna().drop_duplicates(subset=["P", "lang", "name"], keep="first")
T.to_csv("titles.csv", index=False)

T0 = T[["P", "name"]].copy()
T0['name'] = T0['name'].str.replace(r"[^\w\.]", " ", regex=True)
T0 = T0.dropna().apply(lambda x: [(str(x["P"]), w) for w in x['name'].split()], axis=1)
T0 = DataFrame(T0.explode().tolist(), columns=["P", "name"]).drop_duplicates()
T0["name"] = T0["name"].str.upper()
T0.replace("", NA).dropna().to_csv("titles.chunks.csv", index=False)