def ls(dir="../docs"):
  from os import listdir, path
  D = [path.join(dir, d) for d in listdir(dir)]
  D = [d for d in D if path.isdir(d) if "random" not in d]
  Fs = [listdir(d) for d in D if path.isdir(d)]
  Fs = [path.join(D[i], Fs[i][j])
        for i in range(len(Fs))
        for j in range(len(Fs[i]))]
  return Fs

I = [f for f in ls("./") 
         if "opeanai" not in f and not f.endswith(".crop.jpg")]
p = set([f.split("/")[-1].split(".")[0] for f in I])
P = set([f.split("/")[-1].split(".")[0] for f in ls("../docs")])
for x in (P - p): print(x)