import os, pandas
os.chdir("/storage/home/ajakubczak/Dokumenty/UPRP/raport/recog")

dirs = [os.path.join('./output', D) for D in os.listdir('./output')]
paths = [os.listdir(D) for D in dirs if os.path.isdir(D)]
paths = [os.path.join(dirs[i], paths[i][j])
         for i in range(len(paths))
         for j in range(len(paths[i]))]

df = pandas.concat([pandas.read_csv(f) for f in paths])

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

df = df[['P', 'page', 'unit', 'text',
        'xtoplft', 'ytoplft',
        'xtoprgt', 'ytoprgt',
        'xbtmrgt', 'ybtmrgt',
        'xbtmlft', 'ybtmlft']]

df.to_csv('paddle.csv', index=False)