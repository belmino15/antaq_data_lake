import requests
import os

from functions.utils import safe_open_wb

def captura_dados_atracacao():

    years = [2017, 2018, 2019]
    path_atracacao = "data/raw/atracacao"

    for year in years:
        url = f"https://web3.antaq.gov.br/ea/txt/{year}Atracacao.zip"
        response = requests.get(url)

        path_fileName = os.path.join(path_atracacao, f'{year}Atracacao.zip')

        with safe_open_wb(path_fileName) as arquivo:
            arquivo.write(response.content)