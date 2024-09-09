import os; os.chdir("/home/adam/Projekty/UPRP/raport/img")

def get_num(fid: str) -> str:
  num = fid
  if "/" in num: num = num.split("/")[-1]
  if "." in num: num = num.split(".")[0]
  return num

folders = os.listdir("../docs")
paths = [os.path.join("../docs", folder) for folder in folders]
paths = [os.path.join(path, file) for path in paths for file in os.listdir(path)]

def PDF2img(path: str, out: str) -> None:
  from pdf2image import convert_from_path
  import PIL
  imgs = convert_from_path(path)
  for i, img in enumerate(imgs):
    if i > 2: break
    img = img.resize((512, int(512 * img.height / img.width)), PIL.Image.LANCZOS)
    img.save(f"{out}-{i}.jpg", "JPEG")

from tqdm import tqdm
for path in tqdm(paths[:1000]):
  out = os.path.join("output", get_num(path))
  PDF2img(path, out)