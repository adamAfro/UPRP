from pandas import read_csv as read
X = read('recog/paddle.csv')
X = X.loc[read('isdata.pred.csv', header=None).squeeze() > 0.5]

from os import path, listdir, makedirs as dir
D = [path.join("img", d) for d in listdir("img")]
Fs = [listdir(d) for d in D]
Fs = [path.join(D[i], Fs[i][j])
         for i in range(len(Fs))
         for j in range(len(Fs[i]))]

from PIL import Image
from tqdm import tqdm as progress
for u, U in progress(X.groupby('unit')):
  P, pg = U['P'].unique()[0], U['page'].unique()[0]
  lft = U[['xtoplft', 'xbtmlft']].min().values[0]
  rgt = U[['xtoprgt', 'xbtmrgt']].max().values[0]
  top = U[['xtoplft', 'xtoprgt']].min().values[0]
  btm = U[['xbtmlft', 'xbtmrgt']].max().values[0]
  f = next(f for f in Fs if f.endswith(f"{P}.{pg}.jpg"))
  img = Image.open(f).crop((lft, top, rgt, btm))
  f = f.replace("img", "img/crop")
  dir(path.dirname(f), exist_ok=True)
  img.save(f, 'JPEG')