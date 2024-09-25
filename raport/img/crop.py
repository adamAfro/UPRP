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

def crop(G):
  try:
    u, U = G
    a = (U['ybtmrgt'] - U['ytoplft']).mean()
    P, pg = U['P'].unique()[0], U['page'].unique()[0]
    top = U[['ytoplft', 'ytoprgt']].min().min()
    btm = U[['ybtmlft', 'ybtmrgt']].max().max()
    f = next(f for f in Fs if f.endswith(f"{P}.{pg}.jpg"))
    I = Image.open(f)
    lft, rgt = 0, I.width
    h, w = btm - top, rgt - lft
    if h < 10 or w < 10: return
    I = I.crop((lft, top, rgt, btm))
    I = I.resize((min(I.width, int(15*I.width//a)), 
                  min(I.height, int(15*I.height//a))), 
                  Image.Resampling.LANCZOS)
    f = f.replace(".jpg", ".crop.jpg")
    os.makedirs(os.path.dirname(f), exist_ok=True)
    I.save(f, 'JPEG')
  except Exception as e: print(e)
  
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm as progress
G = list(X.groupby('unit'))
with ProcessPoolExecutor() as executor:
    list(progress(executor.map(crop, G), total=len(G)))