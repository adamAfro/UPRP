def read_xml_multiindex(dir, max_files=None):

    """Przykład:
    ```
    patents = read_xml_multiindex(dir="data").astype(str)
    patents.to_csv("df.csv", index=False)
    # patents = read_csv("df.csv", dtype=str)
    patents.columns = wrap_cols(patents.columns)
    patents = patents.dropna(axis=1, thresh=2)

    from pandas import option_context as df_context
    with df_context('display.max_rows', None, 'display.max_columns', None):
        display(patents.describe().T.drop(columns=['top', 'freq']))
    ```
    """

    from tqdm import tqdm
    from pandas import DataFrame

    files = list_files(dir)
    if max_files: files = files[:max_files]

    data = []
    with tqdm(total=len(files)) as pbar:
        for path in files:
            with open(f"{dir}/{path}") as file:
                d = parse_xml(file.read())
                d['id'] = path[:-4]
                try: data.append(d)
                except: pass
            pbar.update(1)

    df = DataFrame(data).sort_index(axis=1)

    for column in df.columns:
        nacount = df[column].isna().sum()
        all_single = len(df) == nacount + sum([(isinstance(x, list) and len(x) == 1) for x in df[column]])
        if all_single:
            df[column] = df[column].apply(lambda x: x[0] if isinstance(x, list) else x)
    return df

def wrap_cols(columns):

    from pandas import MultiIndex, Series
    
    levels_n = max(columns.str.count('/')) + 1
    index = Series(columns).str.split('/')
    index = index.apply(lambda x: (x + [''] * levels_n)[:levels_n])

    return MultiIndex.from_tuples(index)

def traverse_tree(node, path, end_nodes):
    new_path = f"{path}/{node.tag}" if path else node.tag
    if not list(node):
        if new_path in end_nodes:
            end_nodes[new_path].append(node.text)
        else:
            end_nodes[new_path] = [node.text]
    else:
        for child in node:
            traverse_tree(child, new_path, end_nodes)

def parse_xml(xml):
    import xml.etree.ElementTree as ET
    root = ET.fromstring(xml)
    end_nodes = {}
    traverse_tree(root, "", end_nodes)
    
    return end_nodes

def list_files(directory):
    import os
    return os.listdir(directory)