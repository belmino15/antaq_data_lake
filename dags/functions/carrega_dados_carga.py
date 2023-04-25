import pandas as pd
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

def carrega_dados_carga():

    # Ler o arquivo do texto e criar um DataFrame
    df = pd.read_csv('data/silver/carga.txt', delimiter=';')
    df = df.where(df.notnull(), None)

    df = df.head(300000)

    # Inserir os dados na tabela

    mssql_hook = MsSqlHook(mssql_conn_id='mssql_connection')
    
    conn = mssql_hook.get_conn()

    with conn.cursor() as cursor:
        cursor.execute("""
        if exists (select * from sysobjects where name='carga' and xtype='U')
        drop table carga;
        CREATE TABLE carga (
            IDCarga INT,
            IDAtracacao INT,
            Origem VARCHAR(255),
            Destino VARCHAR(255),
            CDMercadoria VARCHAR(255),
            TipoOperacaoCarga VARCHAR(255),
            CargaGeralAcondicionamento VARCHAR(255),
            ConteinerEstado VARCHAR(255),
            TipoNavegacao VARCHAR(255),
            FlagAutorizacao VARCHAR(255),
            FlagCabotagem INT,
            FlagCabotagemMovimentacao INT,
            FlagConteinerTamanho VARCHAR(255),
            Ano_da_data_de_inicio_da_operacao_atracacao INT,
            Mes_da_data_de_inicio_da_operacao_atracacao VARCHAR(50),
        );
        """)
    
    with conn.cursor() as cursor:
        i = 0
        quant_linhas = 10000
        while i < df.shape[0]:
            tup = list(df.iloc[i:i+quant_linhas, :].itertuples(index=False))
            cursor.executemany("""
            INSERT INTO carga (
                IDCarga,
                IDAtracacao,
                Origem,
                Destino,
                CDMercadoria,
                TipoOperacaoCarga,
                CargaGeralAcondicionamento,
                ConteinerEstado,
                TipoNavegacao,
                FlagAutorizacao,
                FlagCabotagem,
                FlagCabotagemMovimentacao,
                FlagConteinerTamanho,
                Ano_da_data_de_inicio_da_operacao_atracacao,
                Mes_da_data_de_inicio_da_operacao_atracacao
                ) VALUES (%d, %s, %s, %s, %s, %s, %s, %s, %s, %s, %d, %d, %s, %d, %s)
            """, tup)
            i+=quant_linhas

            print(i)

    conn.commit()
    cursor.close()

