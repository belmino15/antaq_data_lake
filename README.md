# antaq_data_lake

## PROVA PRÁTICA - ESPECIALISTA EM ENGENHARIA DE DADOS

As respostas do questionário encontram-se em `questoes/questoes.md`.

## Sobre o projeto

Esse repositório cria uma pipeline de Dados que Captura dados do Antaq e disponibiliza em um Banco de dados SQL.

- Os dados do Antaq podem ser visualizados em:
    -  https://web3.antaq.gov.br/ea/sense/index.html#pt
    - https://web3.antaq.gov.br/ea/sense/download.html#pt
- Para criar a infraestrutura utilize o comando `docker-compose up -d --build`
- A connection com o Banco de Dados deve ser configurada assim:
![connection_sql_server](questoes/connection_sql_server.png)
- O envio de emails é realizado pelo serviço MailTrap.

## Outros projetos

Nesse repositório também foi desenvolvida uma query de acesso aos dados, um script de configuração de um servidor Kubernetes rodando Airflow e o CICD de entrega das DAGs separando produção e homologação. Os arquivos são respectivamente `numero_atracacao.sql`, `script_airflow_kube.sh` e `.github/workflows/main.yml`. Detalhes de cada arquivos podem ser encontrados nas resposta do questionário.
