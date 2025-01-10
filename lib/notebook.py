import nbformat as nbf, re
from nbconvert.preprocessors import ExecutePreprocessor

def gen(input_file, output_file, before=None, after=None):

    with open(input_file, 'r') as file:
        content = file.read()

    cells = re.split(r'(#\s){3,}', content)
    cells = [cell.strip() for cell in cells]
    cells = [cell for cell in cells if cell and cell != '#']

    nb = nbf.v4.new_notebook()
    nb.cells = [nbf.v4.new_code_cell(cell) for cell in cells if cell.strip()]

    if before:
        for md_string in before:
            nb.cells.insert(0, nbf.v4.new_markdown_cell(md_string))

    if after:
        for md_string in after:
            nb.cells.append(nbf.v4.new_markdown_cell(md_string))

    with open(output_file, 'w') as f:
        nbf.write(nb, f)

    ep = ExecutePreprocessor(timeout=600, kernel_name='python3')
    with open(output_file) as f:
        nb = nbf.read(f, as_version=4)
        ep.preprocess(nb, {'metadata': {'path': './'}})

    with open(output_file, 'w') as f:
        nbf.write(nb, f)