from pandas import read_csv as read
import glob, os
X = read('../recog/paddle.csv')
B = read('../recog/table.bin.csv', header=None).squeeze()
X = X.loc[B == True]

from os import path, listdir, makedirs as dir
D = [d for d in listdir("./") if path.isdir(d)]
Fs = [listdir(d) for d in D if path.isdir(d)]
Fs = [path.join(D[i], Fs[i][j])
         for i in range(len(Fs))
         for j in range(len(Fs[i]))]

from PIL import Image
from tqdm import tqdm as progress
rm = glob.glob('**/*.crop.jpg', recursive=True)
for f in progress(rm): os.remove(f)

for u, U in progress(X.groupby('unit')):
  try:
    P, pg = U['P'].unique()[0], U['page'].unique()[0]
    lft = U[['xtoplft', 'xbtmlft']].min().min()
    rgt = U[['xtoprgt', 'xbtmrgt']].max().max()
    top = U[['ytoplft', 'ytoprgt']].min().min()
    btm = U[['ybtmlft', 'ybtmrgt']].max().max()
    h, w = btm - top, rgt - lft
    if h < 10 or w < 10: continue
    f = next(f for f in Fs if f.endswith(f"{P}.{pg}.jpg"))
    img = Image.open(f).crop((lft, top, rgt, btm))
    img = img.resize((int(img.width//2.4), int(img.height//2.4)), 
                     Image.Resampling.LANCZOS)
    f = f.replace(".jpg", ".crop.jpg")
    dir(path.dirname(f), exist_ok=True)
    img.save(f, 'JPEG')
  except Exception as e: print(e)