#komórka
from pandas import read_csv, to_numeric
from matplotlib.pyplot import subplots, rcParams as plotsetup
from matplotlib.ticker import MaxNLocator
plotsetup['figure.figsize'] = [12, 12]
F = {}
X = read_csv('docs.chunks.csv')

X.select_dtypes(include=['bool']).assign(wszystkie=True)\
.groupby(X['docs']).any().sum(axis=0).sort_values(ascending=False)\
.plot.barh(title="Wyrażenia znalezione w cytowaniach (nie wykluczają się wzajemnie)",
           color=['black'] + ['orange'] * X.select_dtypes(include=['bool']).shape[1]);

#komórka
C = X.query(' and '.join([f"(not {k})" for k in ["DOI", "pub", "pgnum"]]) + \
            " and " + ' or '.join([k for k in ["code", "code56", "PL", "PL56", "EP", "EP56"]]))
F["C"] = subplots(3,1, sharex=True, tight_layout=True)

C['text'].str.extract(r"([^\W\d]+)")[0]\
.fillna("<brak>").value_counts().to_frame().query("count > 512")\
.plot.barh(title="Wyrażenia literowe znalezione w wyrażeniach kodo-podobnych",
  				ylabel='zestaw 1. (n>512)', xlabel='', legend=False, ax=F["C"][1][0]);

C['text'].str.extract(r"[\s\d\W]{5,}([^\W\d]+\d*)")[0]\
.fillna("").value_counts().to_frame().query("count > 380")\
.plot.barh(ylabel='zestaw 2. (n>380)', xlabel='', legend=False, ax=F["C"][1][1]);

C['text'].str.extract(r"([\s\d\W]{5,})")[0]\
.fillna("").apply(lambda x: sum(l.isdigit() for l in x))\
.value_counts().sort_index()\
.plot.barh(title="Liczba cyfr w wyrażeniach kodo-podobnych", 
          ylabel="pomiędzy zestawem 1. a 2.", xlabel='', ax=F["C"][1][2]);

#komórka
D = X.query("datenum or datenumy or datealt0 or datealt or Rmonth or Lmonth or fullmonth")

F["D"] = subplots(3,1, sharex=True, tight_layout=True)

D['text'].str.extract("(?<!\d)(\d{1})(?!\d)")[0].value_counts().sort_index()\
.plot.barh(title="Liczby w dato-podobnych wyrażeniach",
           ylabel="liczba jednocyfrowa", ax=F["D"][1][0]);

D['text'].str.extract("(?<!\d)(\d{2})(?!\d)")[0].value_counts().sort_index()\
.plot.barh(ylabel="liczba dwucyfrowa", ax=F["D"][1][1]);
F["D"][1][1].yaxis.set_major_locator(MaxNLocator(nbins=10))

D['text'].str.extract("(?<!\d)(\d{4})(?!\d)")[0].value_counts().sort_index()\
.plot.barh(ylabel="liczba czterocyfrowe", ax=F["D"][1][2]);
F["D"][1][2].yaxis.set_major_locator(MaxNLocator(nbins=10))


#komórka
