
from pandas import read_csv, merge, DataFrame, to_datetime, concat
from numpy import nan

dward = dict(left_on="ID", right_on="PID", how="left", suffixes=("", "*"))
ddrop, drnme = dict(columns=["PID*", "ID"]), dict(columns={ "ID*": "ID" })

uward = dict(left_on="PID", right_on="ID", how="left", suffixes=("", "*"))
udrop = dict(columns=["PID", "ID*", "PID*"])

def assignment():
  
  R = "XML/root/pl-patent-document/bibliographic-data/assignees/assignee/"
  X = read_csv(R + "df.csv", dtype=str)
  X["XID"] = X["ID"]

  X = merge(X, **dward, right=read_csv(R + "addressbook/df.csv", dtype=str)).drop(**ddrop).rename(**drnme)
  X = merge(X, **dward, right=read_csv(R + "addressbook/name/df.csv", dtype=str)).drop(**ddrop).rename(**drnme)
  
  Y = DataFrame({ "P": X["P"], "name": X["$"] }).drop_duplicates()

  return Y

def names():

  R = "XML/root/pl-patent-document/bibliographic-data/parties/inventors/inventor/"
  X = read_csv(R + "df.csv", dtype=str)
  X["XID"] = X["ID"]

  X = merge(X, **dward, right=read_csv(R + "addressbook/df.csv", dtype=str)).drop(**ddrop).rename(**drnme)
  X = merge(X, **dward, right=read_csv(R + "addressbook/address/df.csv", dtype=str)).drop(**ddrop).rename(**drnme)
  
  Y = DataFrame({"P": X["P"], 
                 "ID": X["XID"],
                 "lang": X["&lang"].str.upper(),
                 "finitial": X["$first-name"].str[0].str.upper() + ".",
                 "first": X["$first-name"].str.upper(),
                 "last": X["$last-name"].str.upper(),
                 "linitial": X["$last-name"].str[0].str.upper() + "." })
  
  if Y["lang"].nunique() == 1: Y = Y.drop(columns=["lang"])
  return Y.drop_duplicates(subset=["P", "ID", "first", "last"], keep="first")

def dates():  

  R = "XML/root/pl-patent-document/"
  X = read_csv(R + "bibliographic-data/df.csv", dtype=str)
  X["XID"] = X["ID"]
  
  X = merge(X, **dward, right=read_csv(R + "bibliographic-data/publication-reference/df.csv", dtype=str)).drop(**ddrop).rename(**drnme)
  X = merge(X, **dward, right=read_csv(R + "bibliographic-data/publication-reference/document-id/df.csv", dtype=str)).drop(**ddrop).rename(**drnme)
  X = merge(X, **uward, right=read_csv(R + "df.csv", dtype=str)).drop(**udrop)
  
  L = [DataFrame({"P": X["P"], "ID": X["XID"], "date": X["&date-publ"] }).dropna(),
       DataFrame({"P": X["P"], "ID": X["XID"], "date": X["$date-publ"] }).dropna(),
       DataFrame({"P": X["P"], "ID": X["XID"], "date": X["$date"] }).dropna(),
       DataFrame({"P": X["P"], "ID": X["XID"], "date": X["$date*"] }).dropna()]

  Y = concat(L, axis=0).replace("~", nan).dropna()
  Y = Y.drop_duplicates(subset=["P", "ID", "date"], keep="first")

  Y["date"] = to_datetime(Y["date"], format="%Y%m%d")
  
  Y["y"] = Y["date"].dt.year
  Y["m"] = Y["date"].dt.month
  Y["d"] = Y["date"].dt.day

  return Y.drop(columns=["date", "ID"])

def titles():
  
  X = read_csv("XML/root/pl-patent-document/bibliographic-data/invention-title/df.csv", dtype=str)
  Y = DataFrame({ "P": X["P"], "lang": X["&lang"], "title": X["$"] })
  Y = Y.dropna()
  Y = Y.drop_duplicates(subset=["P", "lang", "title"], keep="first")
  return Y

names().to_csv("names.csv", index=False)
dates().to_csv("dates.csv", index=False)
titles().to_csv("titles.csv", index=False)
assignment().to_csv("assignment.csv", index=False)