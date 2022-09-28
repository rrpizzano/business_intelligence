from posixpath import basename
from turtle import clear
import zipfile
import os
from pathlib import Path

base_path = Path(__file__).resolve().parent

with zipfile.ZipFile(f'{base_path}/zipped_CSVs.zip', mode='w') as z:
    for file in os.listdir(f'{base_path}/temp'):
        if os.path.splitext(file)[1] == '.csv':
            z.write(f'{base_path}/temp/{file}', basename(f'{file}'))

    if os.stat(f'{base_path}/zipped_CSVs.zip').st_size == 0:
        os.remove(f'{base_path}/zipped_CSVs.zip')
