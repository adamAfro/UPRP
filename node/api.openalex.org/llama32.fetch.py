from pyalex import Works, concurrent # https://github.com/adamAfro/pyalex
from pandas import read_csv
from tqdm import tqdm as progress; progress.pandas()
import re, json

X = read_csv("../../edge/extract/scientific.llama32.csv", dtype=str)
T = X['title'].dropna().values
T = [re.sub(r'[\W\d\.]', ' ', t) for t in T]

f = lambda x: Works().search_filter(title=x)
Y = await concurrent(f, T, frequency=10)
Y = [y for v in Y for y in v if y]

for y in Y:
  with open(f"res/{y['id'].split('/')[-1]}.json", "w") as f:
    f.write(json.dumps(y))