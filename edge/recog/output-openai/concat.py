def ls(dir="batches"):
  from os import listdir, path
  D = [path.join(dir, d) for d in listdir(dir)]
  D = [d for d in D if path.isdir(d) if "random" not in d]
  Fs = [listdir(d) for d in D if path.isdir(d)]
  Fs = [path.join(D[i], Fs[i][j])
        for i in range(len(Fs))
        for j in range(len(Fs[i]))]
  return Fs

from read import CSV as read_csv
from tqdm import tqdm as progress
from pandas import concat
L = []
for f in progress(ls("batches")):
  X = read_csv(f)
  X["P"], X["page"] = f.split("/")[-1].split(".")[0:2]
  L.append(X)
X = concat(L)
X.to_csv("docs.csv", index=False)

I = [i for i in ls("../../img")
       if i.endswith(".jpg") and not i.endswith(".crop.jpg")]
I = [i.split("/")[-1].split(".")[:2] for i in I]


omited = [(P, p) for (P, p) in progress(I) if X.query("P == @P and page == @p").empty]

F = ls("../../img")
import shutil
for (P, p) in progress(omited):
  f = [f for f in F if f.endswith(f"{P}.{p}.jpg")][0]
  shutil.copyfile(f, "../../img/openai-omited/" + f.split("/")[-1])