import pandas as pd
import glob
import os

def transforma_dados_atracacao():

    silver_path = 'data/silver/atracacao.txt'

    arquivos = glob.glob('data/bronze/atracacao/*Atracacao.txt')

    dataframes = []
    for arquivo in arquivos:
        df = pd.read_csv(arquivo, delimiter=';')
        dataframes.append(df)

    df_concatenado = pd.concat(dataframes, ignore_index=True)

    selected_columns = [
        'IDAtracacao', 'CDTUP', 'IDBerco', 'Berço', 'Porto Atracação',
        'Apelido Instalação Portuária', 'Complexo Portuário',
        'Tipo da Autoridade Portuária', 'Data Atracação', 'Data Chegada',
        'Data Desatracação', 'Data Início Operação', 'Data Término Operação',
        'Ano', 'Mes', 'Tipo de Operação'
    ]

    rename_columns_list = {
        'Ano': 'Ano da data de início da operação',
        'Mes': 'Mês da data de início da operação'
    }

    df_concatenado = df_concatenado[selected_columns].rename(columns=rename_columns_list)

    os.makedirs(os.path.dirname(silver_path), exist_ok=True)

    df_concatenado.to_csv(silver_path, sep=';', index=False)

