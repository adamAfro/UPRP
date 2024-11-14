import os, pandas
os.chdir("/storage/home/ajakubczak/Dokumenty/UPRP/raport/labels")

df = pandas.concat([pandas.read_csv(f'./output/{f}') 
                    for f in os.listdir('./output') 
                    if f.endswith('.csv')])

df['id'] = df.index
df['page'] = df['page']
df['text'] = df['text']
df['xtoplft'] = df['x0']
df['ytoplft'] = df['y0']
df['xtoprgt'] = df['x1']
df['ytoprgt'] = df['y1']
df['xbtmrgt'] = df['x2']
df['ybtmrgt'] = df['y2']
df['xbtmlft'] = df['x3']
df['ybtmlft'] = df['y3']
df['P'] = df.apply(lambda x:
                          x['file'].split('/')[-1].split('.')[0], 
                          axis=1)
df['unit'] = (df['P'].astype(str) + ':' + df['page'].astype(str)).factorize()[0] + 1

df['group'] = df['group'].factorize()[0] + 1
df['type'] = df['type'].apply(lambda x: 1 if x == 'docs' else 
                                        2 if x == 'category' else 
                                        3 if x == 'claim' else 
                                        0)

df = df[['P', 'page', 'unit', 'text',
        'xtoplft', 'ytoplft',
        'xtoprgt', 'ytoprgt',
        'xbtmrgt', 'ybtmrgt',
        'xbtmlft', 'ybtmlft', 'group', 'type']]

df.to_csv('labeled.csv', index=False)