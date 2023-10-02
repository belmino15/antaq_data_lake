import requests
import os

from functions.utils import safe_open_wb

def captura_dados_carga():

    years = [2021, 2022, 2023]
    path_atracacao = "data/raw/carga"

    for year in years:
        url = f"https://web3.antaq.gov.br/ea/txt/{year}Carga.zip"
        response = requests.get(url)

        path_fileName = os.path.join(path_atracacao, f'{year}Carga.zip')

        with safe_open_wb(path_fileName) as arquivo:
            arquivo.write(response.content)