import os
from zipfile import ZipFile

def extrai_dados_carga():

    path_carga = 'data/raw/carga'
    path_output = 'data/bronze/carga'
    years = [2017, 2018, 2019]

    for year in years:

        zip_path_carga = os.path.join(path_carga, f'{year}Carga.zip')

        with ZipFile(zip_path_carga, 'r') as zip:
            zip.extractall(path_output)
        


