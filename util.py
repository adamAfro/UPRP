data = { 'UPRP': 'api.uprp.gov.pl',
         'Lens': 'api.lens.org',
         'USPG': 'developer.uspto.gov/grant',
         'USPA': 'developer.uspto.gov/application',
         'Google': 'patents.google.com' }

class A4:
  DPI = 150
  W = DPI/2.54*(21.0 - 2.6 - 3.0)
  H = DPI/2.54*(29.7 - 2.6 - 3.0)

def strnorm(x, dropinter:bool, dropdigit:bool):

  import unicodedata, re

  if not isinstance(x, str): return None
  x = x.upper()
  n = unicodedata.normalize('NFKD', x)
  y = ''.join([c for c in n if not unicodedata.combining(c)])

  for a, b in {'Ą': 'A', 'Ć': 'C', 'Ę': 'E', 
               'Ł': 'L', 'Ń': 'N', 'Ó': 'O', 
               'Ś': 'S', 'Ź': 'Z', 'Ż': 'Z'}.items():

      y = y.replace(a, b)

  if dropdigit:
    y = re.sub(r'[\s\d]+', ' ', y)

  if dropinter:
    y = re.sub(r'[\s\W]+', ' ', y).strip()

  return y