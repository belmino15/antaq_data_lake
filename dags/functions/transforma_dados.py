import pandas as pd
import glob
import os

def transforma_dados():

    # Carrega dados Atracacao
    silver_path_atracacao = 'data/silver/atracacao.txt'

    arquivos = glob.glob('data/bronze/atracacao/*Atracacao.txt')

    dataframes = []
    for arquivo in arquivos:
        df = pd.read_csv(arquivo, delimiter=';')
        dataframes.append(df)
        del df

    df_atracacao = pd.concat(dataframes, ignore_index=True)
    del dataframes, arquivos, arquivo

    # Transforma dados Atracacao
    df_atracacao['Data Atracação'] = pd.to_datetime(df_atracacao['Data Atracação'], format= "%d/%m/%Y %H:%M:%S")
    df_atracacao['Data Chegada'] = pd.to_datetime(df_atracacao['Data Chegada'], format= "%d/%m/%Y %H:%M:%S")
    df_atracacao['Data Desatracação'] = pd.to_datetime(df_atracacao['Data Desatracação'], format= "%d/%m/%Y %H:%M:%S")
    df_atracacao['Data Início Operação'] = pd.to_datetime(df_atracacao['Data Início Operação'], format= "%d/%m/%Y %H:%M:%S")
    df_atracacao['Data Término Operação'] = pd.to_datetime(df_atracacao['Data Término Operação'], format= "%d/%m/%Y %H:%M:%S")

    ## TEsperaAtracacao em Minutos
    df_atracacao['TEsperaAtracacao'] = df_atracacao['Data Atracação'] - df_atracacao['Data Chegada']
    df_atracacao['TEsperaAtracacao'] = df_atracacao['TEsperaAtracacao'].dt.total_seconds()/60

    ## TesperaInicioOp em Minutos
    df_atracacao['TesperaInicioOp'] = df_atracacao['Data Início Operação'] - df_atracacao['Data Chegada']
    df_atracacao['TesperaInicioOp'] = df_atracacao['TesperaInicioOp'].dt.total_seconds()/60

    ## TOperacao em Minutos
    df_atracacao['TOperacao'] = df_atracacao['Data Término Operação'] - df_atracacao['Data Início Operação']
    df_atracacao['TOperacao'] = df_atracacao['TOperacao'].dt.total_seconds()/60

    ## TEsperaDesatracacao em Minutos
    df_atracacao['TEsperaDesatracacao'] = df_atracacao['Data Desatracação'] - df_atracacao['Data Término Operação']
    df_atracacao['TEsperaDesatracacao'] = df_atracacao['TEsperaDesatracacao'].dt.total_seconds()/60

    ## TAtracado em Minutos
    df_atracacao['TAtracado'] = df_atracacao['Data Término Operação'] - df_atracacao['Data Atracação']
    df_atracacao['TAtracado'] = df_atracacao['TAtracado'].dt.total_seconds()/60

    ## TEstadia em Minutos
    df_atracacao['TEstadia'] = df_atracacao['Data Desatracação'] - df_atracacao['Data Atracação']
    df_atracacao['TEstadia'] = df_atracacao['TEstadia'].dt.total_seconds()/60

    # Renomeia colunas Atracacao
    rename_columns_list = {
        'Ano': 'Ano da data de início da operação',
        'Mes': 'Mês da data de início da operação'
    }

    df_atracacao = df_atracacao.rename(columns=rename_columns_list)

    os.makedirs(os.path.dirname(silver_path_atracacao), exist_ok=True)

    # Salva dados Atracacao
    df_atracacao.to_csv(silver_path_atracacao, sep=';', index=False)
    del silver_path_atracacao

    # Prepara dados Atracacao
    df_atracacao = df_atracacao[['IDAtracacao', 'Ano da data de início da operação', 'Mês da data de início da operação']]

    # Carrega dados Carga
    silver_path_carga = 'data/silver/carga.txt'

    arquivos = glob.glob('data/bronze/carga/*Carga.txt')

    dataframes = []
    for arquivo in arquivos:
        df = pd.read_csv(arquivo, delimiter=';')
        dataframes.append(df)
        del df

    df_carga = pd.concat(dataframes, ignore_index=True)
    del dataframes, arquivos, arquivo

    # Transforma dados Carga
    df_carga = df_carga.join(df_atracacao, how='left', on='IDAtracacao', rsuffix="_right").drop(columns=['IDAtracacao_right'])
    del df_atracacao

    rename_columns_list = {
        'Ano da data de início da operação': 'Ano da data de início da operação da atracacao',
        'Mês da data de início da operação': 'Mês da data de início da operação da atracacao'
    }

    df_carga = df_carga.rename(columns=rename_columns_list)

    # Salva dados Carga
    os.makedirs(os.path.dirname(silver_path_carga), exist_ok=True)

    df_carga.to_csv(silver_path_carga, sep=';', index=False)