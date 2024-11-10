from pandas import read_csv, merge, DataFrame, to_datetime, concat, set_option as config
config('display.max_columns', None)

r = "frames/XML/root/pl-patent-document/"

S = read_csv(r+"bibliographic-data/assignees/assignee/df.csv", dtype=str)
S = merge(S, read_csv(r + "bibliographic-data/assignees/assignee/addressbook/df.csv", dtype=str), 
          left_on="ID", right_on="PID", how="left", suffixes=("-0", "-1"))
S = merge(S, read_csv(r + "bibliographic-data/assignees/assignee/addressbook/name/df.csv", dtype=str), 
          left_on="ID-1", right_on="PID", how="left", suffixes=("-1", "-2"))
S = DataFrame({ "P": S["P-0"], "name": S["$"].str.upper(), "lang": S["&lang"].str.upper() }).drop_duplicates()
S.to_csv("assignment.csv", index=False)

S0 = S[["P", "name"]].copy()
S0['name'] = S0['name'].str.replace(r"[^\w\.]", " ", regex=True)
S0 = S0.dropna().apply(lambda x: [(str(x["P"]), w) for w in x['name'].split()], axis=1)
S0 = DataFrame(S0.explode().tolist(), columns=["P", "name"]).drop_duplicates()
S0.replace("", None).dropna().to_csv("assignment.chunks.csv", index=False)


G = read_csv(r+"bibliographic-data/assignees/assignee/addressbook/address/df.csv", dtype=str)
G = DataFrame({ "P": G["P"], "name": G["$city"] }).drop_duplicates()
G.to_csv("cities.csv", index=False)

G0 = G[["P", "name"]].copy()
G0['name'] = G0['name'].str.replace(r"[^\w\.]", " ", regex=True)
G0 = G0.dropna().apply(lambda x: [(str(x["P"]), w) for w in x['name'].split()], axis=1)
G0 = DataFrame(G0.explode().tolist(), columns=["P", "name"]).drop_duplicates()
G0["name"] = G0["name"].str.upper()
G0.replace("", None).dropna().to_csv("cities.chunks.csv", index=False)


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
N0.replace("", None).dropna().to_csv("names.chunks.csv", index=False)


T = read_csv(r+"bibliographic-data/invention-title/df.csv", dtype=str)
T = DataFrame({ "P": T["P"], "lang": T["&lang"], "name": T["$"] })
T = T.dropna().drop_duplicates(subset=["P", "lang", "name"], keep="first")
T.to_csv("titles.csv", index=False)

T0 = T[["P", "name"]].copy()
T0['name'] = T0['name'].str.replace(r"[^\w\.]", " ", regex=True)
T0 = T0.dropna().apply(lambda x: [(str(x["P"]), w) for w in x['name'].split()], axis=1)
T0 = DataFrame(T0.explode().tolist(), columns=["P", "name"]).drop_duplicates()
T0["name"] = T0["name"].str.upper()
T0.replace("", None).dropna().to_csv("titles.chunks.csv", index=False)


X = read_csv(r+"df.csv", dtype=str).add_prefix("root")
B = read_csv(r+"bibliographic-data/df.csv", dtype=str).add_prefix("bibl")
X = X.merge(B, left_on="rootID", right_on="biblPID", how="left")

A0 = read_csv(r+"bibliographic-data/application-reference/df.csv", dtype=str).add_prefix("appl0")
X = X.merge(A0, left_on="rootID", right_on="appl0PID", how="left")

A = read_csv(r+"bibliographic-data/application-reference/document-id/df.csv", dtype=str).add_prefix("appl")
X = X.merge(A, left_on="appl0ID", right_on="applPID", how="left")

A = DataFrame({ 
  "P": X["rootP"],
  "appl": X["appl$date"],
  "applno": X["appl$doc-number"],
  "suffix": X["appl$kind"]
}).replace("~", None).set_index(["P", "suffix"])


X = read_csv(r+"df.csv", dtype=str).add_prefix("root")
B = read_csv(r+"bibliographic-data/df.csv", dtype=str).add_prefix("bibl")
X = X.merge(B, left_on="rootID", right_on="biblPID", how="left")

U0 = read_csv(r+"bibliographic-data/publication-reference/df.csv", dtype=str).add_prefix("publ0")
X = X.merge(U0, left_on="biblPID", right_on="publ0PID", how="left")

U = read_csv(r+"bibliographic-data/publication-reference/document-id/df.csv", dtype=str).add_prefix("publ")
X = X.merge(U, left_on="publ0ID", right_on="publPID", how="left")

U = DataFrame({
  "P": X["rootP"],
  "lang": X["root&lang"],
  "country": X["publ$country"].fillna(X["publ0$country"]).fillna(X["root&country"]),
  "publ": X["publ$date"].fillna(X["publ$date-publ"]).fillna(X["publ0$date"]).fillna(X["root&date-publ"]),
  "number": X["publ$doc-number"].fillna(X["publ0$doc-number"]).fillna(X["root&doc-number"]),
  "suffix": X["publ$kind"].fillna(X["publ0$kind"]).fillna(X["root&kind"]),
}).replace("~", None).set_index(["P", "suffix"])

U = U.drop(U.loc[U.reset_index().duplicated(subset=["P", "suffix", "publ"]).values].index)
U.loc[U.index.duplicated(keep=False)]

U.index.duplicated().sum()

P = U.merge(A, left_index=True, right_index=True, how="outer").reset_index()
P['lang'], P['country'] = P['lang'].str.upper(), P['country'].str.upper()
P['country'] = P['country'].fillna("PL")
P['lang'] = P['lang'].fillna("PL")

P['number'] = P['number'].fillna(P['P'])
P = P.drop(columns=["P"])

P.to_csv("patents.csv", index=False)