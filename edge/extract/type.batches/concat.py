from pandas import read_csv, concat
import os

X = concat([read_csv(f0) for f0 in os.listdir("./") if f0.endswith(".csv")], axis=0)

q = {
  'category': 'To which category, does the reference references?\nExpected values are: patent, scientific, website, book\nFor example: { "value": "example-value" }',
  'authors': 'What authors are listed?\nExpected output type is a list  or empty list [] if there is not any, for example: { "value": ["example-1", "example-2"] } or { "value": [] }',
  'country': 'What country is it from? Refer to it by found short code.\nExpected output type is: str or "" if there is not any  with key "value"; for example: { "value": "example" }',
  'date': 'What is the date of publication?\nExpected output type is: str or "" if there is not any  with key "value"; for example: { "value": "example" }',
  'day': 'What is the day of the publication?\nExpected output type is: str or "" if there is not any  with key "value"; for example: { "value": "example" }',
  'month': 'What is the month of the publication?\nExpected output type is: str or "" if there is not any  with key "value"; for example: { "value": "example" }',
  'year': 'What is the year of the publication?\nExpected output type is: str or "" if there is not any  with key "value"; for example: { "value": "example" }',
  'journal': 'What journal is it from?\nExpected output type is: str or "" if there is not any  with key "value"; for example: { "value": "example" }',
  'number': 'What number does the patent have?\nExpected output type is: str or "" if there is not any  with key "value"; for example: { "value": "example" }',
  'title': 'What title does it have?\nExpected output type is: str or "" if there is not any  with key "value"; for example: { "value": "example" }',
}

X = X.rename(columns={ q: k for k, q in q.items()})
X[X.select_dtypes(object).columns] = X.select_dtypes(object).apply(lambda v: 
  v.fillna("").str.replace(".*💣.*", "", regex=True).replace("", None))

P = X.query('category == "patent"').dropna(how='all', axis=1)
P.count().plot.barh(title="Liczba niepustych wartości w kolumnach tabeli `patent`");
P.to_csv("../patent.llama32.csv", index=False)

S = X.query('category == "scientific"').dropna(how='all', axis=1)
S.count().plot.barh(title="Liczba niepustych wartości w kolumnach tabeli `scientific`");
S.to_csv("../scientific.llama32.csv", index=False)