import os
import shutil
from pathlib import Path
import time

base_path = Path(__file__).resolve().parent

temp_folder = os.makedirs('{}/temporal_folder_2'.format(base_path))
time.sleep(5)
shutil.rmtree('{}/temporal_folder_2'.format(base_path))
