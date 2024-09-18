import os, pdf2image, tqdm
os.chdir("/home/adam/Projekty/UPRP/raport")

dir = "docs"
dirs = [os.path.join(dir, dirname) for dirname in os.listdir(dir)]
paths = [os.listdir(D) for D in dirs]
paths = [os.path.join(dirs[i], paths[i][j])
         for i in range(len(paths))
         for j in range(len(paths[i]))]

for P in tqdm.tqdm(paths):
  try:
    pages = pdf2image.convert_from_path(P)
    name = os.path.basename(os.path.splitext(P)[0])
    dirname = os.path.basename(os.path.dirname(P))
    for i, page in enumerate(pages):
      D = os.path.join("img", dirname)
      os.makedirs(D, exist_ok=True)
      out = os.path.join(D, f"{name}.{i}.jpg")
      page.save(out, 'JPEG')
  except Exception as e: print(e)