with dados_ceara as (
	SELECT
		SGUF as Localidade,
		COUNT(IDAtracacao) as NumAtracacoes,
		avg(TEsperaAtracacao) as TempMedioEsperaAtracacao,
		avg(TAtracado) as TempMedioAtracado,
		Mes_da_data_de_inicio_da_operacao as Mes,
		Ano_da_data_de_inicio_da_operacao as Ano
    FROM
    	atracacao
    WHERE
    	SGUF = 'CE'
    GROUP by
    	SGUF,
    	Mes_da_data_de_inicio_da_operacao,
    	Ano_da_data_de_inicio_da_operacao
),
dados_nordeste as (
	SELECT
		Regiao_Geografica as Localidade,
		COUNT(IDAtracacao) as NumAtracacoes,
		avg(TEsperaAtracacao) as TempMedioEsperaAtracacao,
		avg(TAtracado) as TempMedioAtracado,
		Mes_da_data_de_inicio_da_operacao as Mes,
		Ano_da_data_de_inicio_da_operacao as Ano
    FROM
    	atracacao
    WHERE
    	Regiao_Geografica = 'Nordeste'
    GROUP by
    	Regiao_Geografica,
    	Mes_da_data_de_inicio_da_operacao,
    	Ano_da_data_de_inicio_da_operacao
),
dados_brasil as (
	SELECT
		'Brasil' as Localidade,
		COUNT(IDAtracacao) as NumAtracacoes,
		avg(TEsperaAtracacao) as TempMedioEsperaAtracacao,
		avg(TAtracado) as TempMedioAtracado,
		Mes_da_data_de_inicio_da_operacao as Mes,
		Ano_da_data_de_inicio_da_operacao as Ano
    FROM
    	atracacao
    WHERE
    	Regiao_Geografica in (
    			'Nordeste', 'Norte', 'Sul', 'Sudeste', 'Centro-Oeste'
    		)
    GROUP BY
    	Mes_da_data_de_inicio_da_operacao,
    	Ano_da_data_de_inicio_da_operacao
)
SELECT * FROM dados_ceara
UNION ALL
SELECT * FROM dados_nordeste
UNION ALL
SELECT * FROM dados_brasil
ORDER BY localidade, ano, mes;