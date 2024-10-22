from pandas import read_csv, merge, DataFrame, to_datetime, concat, NA

r = "frames/XML/root/pl-patent-document/"

A = read_csv(r+"bibliographic-data/assignees/assignee/df.csv", dtype=str)
A = merge(A, read_csv(r + "bibliographic-data/assignees/assignee/addressbook/df.csv", dtype=str), 
          left_on="ID", right_on="PID", how="left", suffixes=("-0", "-1"))
A = merge(A, read_csv(r + "bibliographic-data/assignees/assignee/addressbook/name/df.csv", dtype=str), 
          left_on="ID-1", right_on="PID", how="left", suffixes=("-1", "-2"))
A = DataFrame({ "P": A["P-0"], "name": A["$"], "lang": A["&lang"].str.upper() }).drop_duplicates()
A.to_csv("assignment.csv", index=False)


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
T = DataFrame({ "P": T["P"], "lang": T["&lang"], "title": T["$"] })
T = T.dropna().drop_duplicates(subset=["P", "lang", "title"], keep="first")
T.to_csv("titles.csv", index=False)