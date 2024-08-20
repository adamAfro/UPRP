import http.server, os

path_sample:str
path_save:str
dir_output = 'output'

def get_dir_PDF() -> str: pass
def get_dir_OCR() -> str: pass

class Server(http.server.SimpleHTTPRequestHandler):

    def do_GET(self):

        import re

        if self.is_forbidden_file():
            self.send_response(403)
            self.end_headers()
            self.wfile.write(b'Access forbidden')
            return
        
        csv_match = re.match(r'/(\d+)\.pdf.csv$', self.path)
        if csv_match is not None:
            self.send_CSV_JSON(csv_match)
            return

        pdf_match = re.match(r'/(\d+)\.pdf$', self.path)
        if pdf_match is not None:
            self.send_PDF_ims(pdf_match)
            return
        
        super().do_GET()

    def do_POST(self):

        if self.path == '/save':
            self.save()
            return
        
        if self.path == '/load':
            self.load()
            return
        
        if self.path == '/sample':
            self.get_sample()
            return
        
        
        super().do_POST()

    def load(self):

        from pandas import read_csv

        try:
            df = read_csv(path_save)
            data = df.to_json(orient='records')
        except:
            data = '[]'

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(data.encode())

    def save(self):

        import json
        from pandas import DataFrame

        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        df = DataFrame(json.loads(post_data.decode()))
        df.to_csv(path_save, index=False)
        
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        response = {'status': 'success', 'data': post_data.decode()}
        self.wfile.write(json.dumps(response).encode())

    def get_sample(self):

        import json

        with open(path_sample, 'r') as f:
            data = json.load(f)
        
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())

    def is_forbidden_file(self):
        requested_path = self.translate_path(self.path)
        return os.path.basename(requested_path) == os.path.basename(__file__)
    
    def send_PDF_ims(self, pdf_match):
        from pdf2image import convert_from_path
        from io import BytesIO
        import base64

        pdf_path = get_dir_PDF(pdf_match.group(0)[1:])
        if not os.path.exists(pdf_path):
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b'File not found')
            return

        try:
            images = convert_from_path(pdf_path)
            boundary = '----WebKitFormBoundary7MA4YWxkTrZu0gW'
            self.send_response(200)
            self.send_header('Content-type', f'multipart/x-mixed-replace; boundary={boundary}')
            self.end_headers()

            for image in images:
                img_byte_arr = BytesIO()
                image.save(img_byte_arr, format='JPEG')
                img_data = img_byte_arr.getvalue()

                base64_data = base64.b64encode(img_data).decode('utf-8')

                self.wfile.write(f'--{boundary}\r\n'.encode())
                self.wfile.write(b'Content-Type: image/jpeg\r\n')
                self.wfile.write(f'Content-Length: {len(base64_data)}\r\n'.encode())
                self.wfile.write(b'\r\n')
                self.wfile.write(base64_data.encode())
                self.wfile.write(b'\r\n')

            self.wfile.write(f'--{boundary}--\r\n'.encode())

        except Exception as e:
            self.send_response(500)
            self.end_headers()
            self.wfile.write(b'Error processing PDF')

    def send_CSV_JSON(self, csv_match):

        import csv, json

        csv_file = get_dir_OCR(csv_match.group(0)[1:])
        if not os.path.exists(csv_file):
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b'File not found')
        
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            json_data = json.dumps(rows)
            self.wfile.write(json_data.encode('utf-8'))

if __name__ == "__main__":
    port = 8001
    server_address = ('', port)
    httpd = http.server.HTTPServer(server_address, Server)
    print(f"Serwer HTTP działa na porcie http://localhost:{port}")
    httpd.serve_forever()