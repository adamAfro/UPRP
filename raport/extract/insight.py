#komórka
from pandas import read_csv
from matplotlib.pyplot import subplots, rcParams as plotsetup
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