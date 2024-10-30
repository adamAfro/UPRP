from entities import Q, Sentence, month
from pandas import read_csv, DataFrame
from tqdm import tqdm as progress; progress.pandas()
import warnings

def pattern(x:str):
  import re

  protocol = r'(\bhttps?://|www\.)'
  x = re.sub(protocol, "//", x)
  alpha = r'([^\W\d]+)'
  x = re.sub(alpha, lambda m: "C" if len(m.group(1)) > 8 else
                              "B" if len(m.group(1)) > 3 else
                              "A" if len(m.group(1)) > 1 else "L", x)
  x = x.replace('L.', "I").replace('A.', "I")
  
  x = re.sub(r'\d+', lambda m: str(len(m.group(0))), x)
  
  webstart = r'//\S+'
  x = re.sub(webstart, "U", x)

  x = re.sub(r'[\W\s]', " ", x)
  x = re.sub(r'\s{2,}', " ", x)
  
  return x.strip()

N = read_csv('../docs.csv')
N = N.reset_index().rename(columns={'docs':'text', 'index':'docs'})
N = N[['docs', 'text']]

N3 = N.progress_apply(lambda x: [(x['docs'], y, 3) for y in Sentence.nsub(x['text'], 3)], axis=1)
N5 = N.progress_apply(lambda x: [(x['docs'], y, 5) for y in Sentence.nsub(x['text'], 5)], axis=1)
N7 = N.progress_apply(lambda x: [(x['docs'], y, 7) for y in Sentence.nsub(x['text'], 7)], axis=1)

N = DataFrame([*N3.explode().tolist(),
               *N5.explode().tolist(),
               *N7.explode().tolist()],
               columns=['docs', 'text', 'n'])
N['norm'] = N['text'].str.replace(r'[^\w\.]', " ", regex=True)
N['norm'] = N['norm'].str.replace(r'(\d)(\.)', lambda m: m.group(1) + " "*len(m.group(2)), regex=True)

N['pattern'] = N['norm'].progress_apply(pattern)
N = N.drop_duplicates(subset=['docs', 'pattern'])
with warnings.catch_warnings():#regexcatch
  warnings.simplefilter("ignore", UserWarning)
  for k, v in Q.items():
    print(k, end=' ')
    N[k] = N['norm'].str.contains(v, regex=True)
    print('✅')