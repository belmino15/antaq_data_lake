import pandas as pd
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

def carrega_dados_carga():

    # Ler o arquivo do texto e criar um DataFrame
    df = pd.read_csv('data/silver/carga.txt', delimiter=';')
    df = df.where(df.notnull(), None)

    df = df.head(100000)

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
            FlagConteinerTamanho VARCHAR(255)
        );
        """)
    
    with conn.cursor() as cursor:
        for row in df.itertuples(index=False):
            cursor.execute("""
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
                FlagConteinerTamanho
                )
                VALUES(%d, %s, %s, %s, %s, %s, %s, %s, %s, %s, %d, %d, %s);
                """, row)

    conn.commit()
    cursor.close()

