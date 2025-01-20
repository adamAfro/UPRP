import sys, os; sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
import pickle, pandas

with open('../../api.uprp.gov.pl/storage.pkl', 'rb') as file:
  S = pickle.load(file)

n = 3
k = 7
Y = ''

def render(y, appendix=''):

  s = '\\'
  Y = ''
  Y += s+"begin{table}[H]"+s+"centering"
  Y += y
  Y += s+"caption{Losowe obserwacje zbioru "+\
       s+"texttt{"+h+"}"+\
       s+"label{tab:losowe_"+h+"}" + appendix + "}"
  Y += s+"end{table}"
  
  return Y

for h, X in S.data.items():

  X:pandas.DataFrame
  X = X.fillna('-')
  X.columns = X.columns.map(lambda x: x.replace('_', ' '))
  X.columns = X.columns.map(lambda x: x.replace('&', '\&'))
  X.columns = X.columns.map(lambda x: x.replace('$', '\$'))
  X.columns = X.columns.map(lambda x: x[:k]+'...' if len(x) > k+5 else x)

  S = X.sample(n, random_state=42)
  
  S = S.map(lambda x: x.replace('_', ' '))
  S = S.map(lambda x: x.replace('&', '\&'))
  S = S.map(lambda x: x.replace('$', '\$'))
  S = S.map(lambda x: x[:k]+'...' if len(x) > k+5 else x)  
  S = S.sort_index().reset_index().T
  S.columns.name = 'Losowa obs. nr.'

  h = h.replace('_', ' ')

  if S.shape[0] < 30:
    Y += render(S.to_latex())
  else:
    for i in range(0, S.shape[0], 30):
      Y += render(S.iloc[i:i+30].to_latex(), ' (część '+str(i//30+1)+'.)')

with open('input.tex', 'w') as file:
  file.write(Y)