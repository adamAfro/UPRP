from os import path, listdir, makedirs as dir
D = [d for d in listdir("./") if path.isdir(d)]
Fs = [listdir(d) for d in D if path.isdir(d)]
Fs = [path.join(D[i], Fs[i][j])
      for i in range(len(Fs))
      for j in range(len(Fs[i])) if Fs[i][j].endswith(".csv")]

def image(f:str): return "../../img/" + f.replace(".csv", ".jpg")


def wrap(t, w):
  L = []
  while len(t) > w:
    s = t.rfind(' ', 0, w)
    if s == -1: s = w
    L.append(t[:s])
    t = t[s:].strip()
  L.append(t)
  return '\n'.join(L)


def mktest(i, p, o):
  from PIL import Image, ImageDraw, ImageFont
  I = Image.open(i)
  with open(p, "r", encoding="utf-8") as f: T = f.read()
  T = wrap(T, 100)

  w, h = I.size
  pF = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
  sF = 30
  F = ImageFont.truetype(pF, sF)
  
  draw = ImageDraw.Draw(Image.new('RGB', (w, 1), (255, 255, 255)))
  B = draw.textbbox((0, 0), T, font=F)
  tw, th = B[2] - B[0], B[3] - B[1]
  
  IY = Image.new('RGB', (w, h + th + 20), (255, 255, 255))
  
  draw = ImageDraw.Draw(IY)
  draw.text((10, 10), T, fill=(0, 0, 0), font=F)
  
  IY.paste(I, (0, th + 20))
  
  IY.save(o)

from tqdm import tqdm as progress
for f in progress(Fs):
  try:
    p = image(f)
    o = "test/" + f.replace(".csv", ".test.jpg")
    dir(path.dirname(o), exist_ok=True)
    mktest(p, f, o)
  except Exception as e: print(f, e)