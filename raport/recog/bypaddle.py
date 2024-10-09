def dir(pdf_dir:str, ocr_dir:str, file_limit:int=None, paddle_args:dict={}, preprocessor:callable=None):

    """
    Przykład użycia w notatniku:
    ```
    import paddle
    if paddle.is_compiled_with_cuda():
        paddle.set_device('gpu:0')
        print("GPU ✅")
    else:
        print("coś jest nie tak z GPU")

    dir('../docs+', 'output+')
    ```
    """

    from tqdm import tqdm as progress_bar

    from pandas import DataFrame
    import numpy as np, os

    from paddleocr import PaddleOCR
    from pdf2image import convert_from_path

    if 'lang' not in paddle_args: paddle_args['lang'] = 'pl'
    if 'ocr_version' not in paddle_args: paddle_args['ocr_version'] = 'PP-OCRv4'

    ocr = PaddleOCR(show_log = False, use_gpu=True, **paddle_args)
    def process_page(page):

        img = np.array(page)

        if preprocessor is not None: img = preprocessor(img)
        
        result = ocr.ocr(img, cls=False)[0]
        boxes = [line[0] for line in result]
        texts = [line[1][0] for line in result]

        return boxes, texts

    def process_doc(path):

        pages = convert_from_path(path)
        text_boxes = []
        for page_i, page in enumerate(pages):

            result = process_page(page)
            result = [{
                'file': path, 'page': page_i, 'text': text, 
                ** { f"x{i}": int(x) for i, [x, _] in enumerate(box) },
                ** { f"y{i}": int(y) for i, [_, y] in enumerate(box) }
            } for box, text in zip(*result)]

            text_boxes.extend(result)

        DataFrame(text_boxes).to_csv(os.path.join(ocr_dir, os.path.basename(path)) + '.csv', index=False)

    def get_paths(pdf_dir, ocr_dir, file_limit):

        os.makedirs(ocr_dir, exist_ok=True)
        paths = [os.path.join(pdf_dir, path) for path in os.listdir(pdf_dir)]
        if file_limit is not None: 
            paths = paths[:file_limit]
            print(f'ograniczono do {file_limit} plików')

        length_all = len(paths)
        paths = [p for p in paths if not os.path.exists(os.path.join(ocr_dir, os.path.basename(p)) + '.csv')]
        print(f'pominięto {length_all - len(paths)} plików, które już zostały przetworzone')

        return paths

    paths, errors = get_paths(pdf_dir, ocr_dir, file_limit), []
    with progress_bar(total=len(paths)) as progress:

        for path in paths:

            progress.set_description(path)
            try: process_doc(path)
            except Exception as e: errors.append(e)
            if len(errors): progress.set_postfix({
                'błędy': len(errors),
                'ostatni': str(errors[-1])[:24] if errors else 'brak'
            })

            progress.update(1)

    from json import dump
    with open(f"{ocr_dir}/errors.json", 'w') as f:
        dump([str(e) for e in errors], f, indent=4)