import pandas as pd
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

def carrega_dados_atracacao():

    # Ler o arquivo do texto e criar um DataFrame
    df = pd.read_csv('data/silver/atracacao.txt', delimiter=';')
    df = df.astype(object).where(pd.notnull(df),None)

    df = df.head(300000)

    # Inserir os dados na tabela

    mssql_hook = MsSqlHook(mssql_conn_id='mssql_connection')
    
    conn = mssql_hook.get_conn()

    with conn.cursor() as cursor:
        cursor.execute("""
        if exists (select * from sysobjects where name='atracacao' and xtype='U')
        drop table atracacao;
        CREATE TABLE atracacao (
            IDAtracacao INT,
            CDTUP VARCHAR(50),
            IDBerco VARCHAR(50),
            Berço VARCHAR(50),
            Porto_Atracacao VARCHAR(100),
            Apelido_Instalacao_Portuaria VARCHAR(50),
            Complexo_Portuario VARCHAR(50),
            Tipo_da_Autoridade_Portuaria VARCHAR(50),
            Data_Atracacao VARCHAR(50),
            Data_Chegada VARCHAR(50),
            Data_Desatracacao VARCHAR(50),
            Data_Inicio_Operacao VARCHAR(50),
            Data_Termino_Operacao VARCHAR(50),
            Ano_da_data_de_inicio_da_operacao INT,
            Mes_da_data_de_inicio_da_operacao VARCHAR(50),
            Tipo_de_Operacao VARCHAR(50),
            Tipo_Navegação_Atracação VARCHAR(50),
            Nacionalidade_Armador INT,
            FlagMCOperacaoAtracacao INT,
            Terminal VARCHAR(100),
            Municipio VARCHAR(50),
            UF VARCHAR(50),
            SGUF VARCHAR(50),
            Regiao_Geografica VARCHAR(50),
            Num_Capitania VARCHAR(50),
            Num_IMO INT,
            TEsperaAtracacao INT,
            TesperaInicioOp INT,
            TOperacao INT,
            TEsperaDesatracacao INT,
            TAtracado INT,
            TEstadia INT
        );
        """)
    
    with conn.cursor() as cursor:
        i = 0
        quant_linhas = 10000
        while i < df.shape[0]:
            tup = list(df.iloc[i:i+quant_linhas, :].itertuples(index=False))
            cursor.executemany("""
            INSERT INTO atracacao (
                IDAtracacao,
                CDTUP,
                IDBerco,
                Berço,
                Porto_Atracacao,
                Apelido_Instalacao_Portuaria,
                Complexo_Portuario,
                Tipo_da_Autoridade_Portuaria,
                Data_Atracacao,
                Data_Chegada,
                Data_Desatracacao,
                Data_Inicio_Operacao,
                Data_Termino_Operacao,
                Ano_da_data_de_inicio_da_operacao,
                Mes_da_data_de_inicio_da_operacao,
                Tipo_de_Operacao,
                Tipo_Navegação_Atracação,
                Nacionalidade_Armador,
                FlagMCOperacaoAtracacao,
                Terminal,
                Municipio,
                UF,
                SGUF,
                Regiao_Geografica,
                Num_Capitania,
                Num_IMO,
                TEsperaAtracacao,
                TesperaInicioOp,
                TOperacao,
                TEsperaDesatracacao,
                TAtracado,
                TEstadia
                ) VALUES (%d, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %d, %s, %s, %s, %d, %d, %s, %s, %s, %s, %s, %s, %d, %d, %d, %d, %d, %d, %d)
            """, tup)
            i+=quant_linhas

            print(i)

    conn.commit()
    cursor.close()

