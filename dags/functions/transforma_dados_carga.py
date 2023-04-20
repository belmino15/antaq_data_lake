import pandas as pd
import glob
import os

def transforma_dados_carga():

    silver_path = 'data/silver/carga.txt'

    arquivos = glob.glob('data/bronze/carga/*Carga.txt')

    dataframes = []
    for arquivo in arquivos:
        df = pd.read_csv(arquivo, delimiter=';')
        dataframes.append(df)

    df_concatenado = pd.concat(dataframes, ignore_index=True)

    selected_columns = [
        'IDCarga', 'IDAtracacao', 'Origem', 'Destino', 'CDMercadoria',
        'Tipo Operação da Carga', 'Carga Geral Acondicionamento',
        'ConteinerEstado', 'Tipo Navegação', 'FlagAutorizacao', 'FlagCabotagem',
        'FlagCabotagemMovimentacao', 'FlagConteinerTamanho'
    ]

    df_concatenado = df_concatenado[selected_columns]

    os.makedirs(os.path.dirname(silver_path), exist_ok=True)

    df_concatenado.to_csv(silver_path, sep=';', index=False)
