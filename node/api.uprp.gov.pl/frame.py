from pandas import read_csv, merge, DataFrame, concat
from pandas import set_option as config; config('display.max_columns', None)

r = "frames/XML/root/pl-patent-document/"


# Poszukiwanie dat aplikacji patentów z numerami
# ----------------------------------------------
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


# Poszukiwanie dat publikacji patentów z numerami
# -----------------------------------------------
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


# Łączenie dat aplikacji i publikacji i tytułów
# ---------------------------------------------
P = U.merge(A, left_index=True, right_index=True, how="outer").reset_index()
P['lang'], P['country'] = P['lang'].str.upper(), P['country'].str.upper()
P['country'] = P['country'].fillna("PL")
P['lang'] = P['lang'].fillna("PL")

P['number'] = P['number'].fillna(P['P'])
P = P.drop(columns=["P"])

T = read_csv(r+"bibliographic-data/invention-title/df.csv", dtype=str)
T = DataFrame({ "number": T["P"], "namelang": T["&lang"].str.upper(), "name": T["$"] })
T = T.dropna().drop_duplicates(subset=["number", "name"], keep="first")
T = T.pivot_table(index='number', columns='namelang', values='name', aggfunc='first')\
  .reset_index().add_suffix("title").rename(columns={"numbertitle": "number"})
T.columns.name = None
P = P.merge(T, on="number", how="left")

P[["ENtitle", "PLtitle", "country", "number", "applno", "suffix", "publ", "appl", "lang"]]\
  .to_csv("patents.csv", index=False)
P = P.drop(columns=["appl", "publ", "lang", "ENtitle", "PLtitle"])




# Imiona osób powiązanych
# -----------------------
N = read_csv(r+"bibliographic-data/parties/inventors/inventor/df.csv").add_prefix("inv")
N = merge(N, read_csv(r+"bibliographic-data/parties/inventors/inventor/addressbook/df.csv").add_prefix("addr"),
          left_on="invID", right_on="addrPID", how="left")
N = merge(N, read_csv(r+"bibliographic-data/parties/inventors/inventor/addressbook/address/df.csv").add_prefix("addr0"),
          left_on="addrID", right_on="addr0PID", how="left")
N = DataFrame({"number": N["invP"].astype(str),
               "namelang": N["addr&lang"].str.upper(),
               "nameloc": N["addr0$country"].str.upper(),
               "city": N["addr0$city"].str.upper(),
               "first": N["addr$first-name"].str.upper(),
               "last": N["addr$last-name"].str.upper() })
N = N.drop_duplicates(subset=[k for k in N.columns if k != 'namelang'], keep="first")
N = merge(P, N, on="number", how="inner")
N = N.replace("~", None).replace("BD", None)
N.to_csv("names.csv", index=False)


# Powiazania patentów z ogranizacjami i właścicielami
# ----------------------------------------------------
#
# TODO: usunąć imiona imona z nazw firm (?)
S = read_csv(r+"bibliographic-data/assignees/assignee/df.csv", dtype=str).add_prefix("asgn")
S = merge(S, read_csv(r + "bibliographic-data/assignees/assignee/addressbook/df.csv", dtype=str).add_prefix("addr"),
          left_on="asgnID", right_on="addrPID", how="left")
S = merge(S, read_csv(r + "bibliographic-data/assignees/assignee/addressbook/name/df.csv", dtype=str).add_prefix("name"),
          left_on="addrID", right_on="namePID", how="left")
S = DataFrame({ "number": S["asgnP"], "name": S["name$"].str.upper(), "namelang": S["addr&lang"].str.upper() }).drop_duplicates()
S = S.drop_duplicates(subset=["number", "name"])
S = merge(P, S, on="number", how="inner")
S[["country", "number", "applno", "suffix", "name", "namelang"]].to_csv("org.csv", index=False)


# Miasta
# ------
G = read_csv(r+"bibliographic-data/assignees/assignee/addressbook/address/df.csv", dtype=str)
G = DataFrame({ "number": G["P"], "name": G["$city"].str.upper(), "nameloc": G["$country"].str.upper() }).drop_duplicates()
G = merge(P, G, on="number", how="inner")
G[["country", "number", "applno", "suffix", "name", "nameloc"]].to_csv("cities.csv", index=False)