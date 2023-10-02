import os
from zipfile import ZipFile

def extrai_dados_atracacao():

    path_atracacao = 'data/raw/atracacao'
    path_output = 'data/bronze/atracacao'
    years = [2021, 2022, 2023]

    for year in years:

        zip_path_atracacao = os.path.join(path_atracacao, f'{year}Atracacao.zip')

        with ZipFile(zip_path_atracacao, 'r') as zip:
            zip.extractall(path_output)
        


