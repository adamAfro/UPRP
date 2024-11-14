from os import path, listdir
Fs = [path.join("./batches/random128.jsonl", f)
      for f in listdir("./batches/random128.jsonl")]

ID = [path.join("../../img", d) for d in listdir("../../img")]
ID = [d for d in ID if path.isdir(d)]
IFs = [listdir(d) for d in ID]
IFs = [path.join(ID[i], IFs[i][j])
      for i in range(len(IFs))
      for j in range(len(IFs[i]))]

def image(f:str): 
  P, pg = f.split("/")[-1].split(".")[:2]
  return next((f for f in IFs if f.endswith(f"{P}.{pg}.jpg")), None)

def mkcode(f:str): return f"""
from read import CSV
CSV("{f}")""".strip()

def mkimg(f:str): return f"""
![📄]({image(f).replace('.jpg', '.crop.jpg')})
![📄]({image(f)})""".strip()

import nbformat as nbf
from nbconvert.preprocessors import ExecutePreprocessor
n = nbf.v4.new_notebook()
C = []
for f in Fs:
  code_cell = nbf.v4.new_code_cell(mkcode(f))
  markdown_cell = nbf.v4.new_markdown_cell(mkimg(f))
  C.extend([code_cell, markdown_cell])
n['cells'] = C

ep = ExecutePreprocessor(timeout=600, kernel_name='python3')
ep.preprocess(n, {'metadata': {'path': './'}})

with open("test.ipynb", "w", encoding="utf-8") as f:
  nbf.write(n, f)