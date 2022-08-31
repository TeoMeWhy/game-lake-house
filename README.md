# Game Lake House

Projeto realizado durante lives na Twitch no canal [Téo Me Why](https://www.twitch.tv/teomewhy).

## Índice

- [1. Objetivo](#1-objetivo)
- [2. Tecnologia & Arquitetura](#2-tecnologia--arquitetura)
- [3. Etapas](#3-etapas)
    - [3.1 Coleta de dados](#31-coleta-de-dados)
    - [3.2 Ingestão para Bronze](#32-ingestão-para-bronze)
    - [3.3 Tratamentos para Silver](#33-tratamentos-para-silver)
- [4. Como você pode nos apoiar?](#4-como-você-pode-nos-apoiar)

## 1. Objetivo

Criação de uma datalake com dados de _esports_, como: Dota2, League of Legends, Valorant, Counter Strike, etc.

Segue algumas possibilidades de aplicações com os dados coletados e armazenados:
- Análise de performance dos players, times, herois, campeões, mapas, etc;
- Clusterização de Players;
- Modelos preditivos para desfecho de partida;
- Modelos para predição em campeonatos;
- Sugestão de *trades* entre times;

## 2. Tecnologia & Arquitetura

<img src="https://i.ibb.co/mGXqXRC/Game-Lake-House-Principal.png" alt="Game-Lake-House-Principal" width="650">

Os componentes utilizados em nossa arquitetura para elaboração do projeto foram:

- S3 Bucket para armazenamento dos dados em todas camadas:
    - RAW: Onde o dado bruto é pousado assim que coletado;
    - Bronze: Dado tratado e pronto para ser consumido;
    - Silver: Dado tratado e pode ser consumido de forma facilitada
    - Gold: Dado disponibilizado a partir de análises e modelos, podendo ser consumido por ferramentas de BI ou DataViz.

- Databricks foi o ecosistema escolhido para se trabalhar com processamento distribuído, fazendo uso de Apache Spark e Delta Lake para processamento e armazenamento dos dados. Com essa abordagem, o provisionamento do ambiente de desenvolvimento, bem como cluster de processametno é feito de forma facilitada, abstraindo algumas complexidades. Dentre as facilidades deste ambiente, temos uma fácil integração com Git / GitHub / GitLab, etc, bem como um simples *scheduler* para orquestrar nossos *jobs* na plataforma.

- Redash será nossa ferramenta de Data Visualization, uma vez que pertence ao Databricks, sendo gratuito e de fácil conexão com clusters Apache Spark.

## 3. Etapas

### 3.1 Coleta de dados

O primeiro passo é realizar as coletas de dados pela API do provedor do jogo. No caso do Dota2, utilizamos o [opendota.com](https://www.opendota.com/). Para isso, com ajuda do Python e a biblioteca **requests**, realizamos requisições à API e salvamos os dados no datalake com o Apache Spark em Delta.

Especificamente para o Dota2, utilizamos dois *end-points*:
- [https://api.opendota.com/api/proMatches](https://api.opendota.com/api/proMatches) para coletar o histórico de todas as partidas profissionais.
- [https://api.opendota.com/api/matches/{match_id}](https://api.opendota.com/api/matches/{match_id}) para coletar os detalhes de cada match_id

Vale dizer que os dados brutos dos detalhes das partidas são salvos em *.json*, uma vez que possuem uma estrutura extramente complexa para serem salvos em *.parquet*. Os scripts criados para esta etapa estão disponiveis em `raw/`.

### 3.2 Ingestão para Bronze

Com os dados devidamente coletados, podemos organiza-los no nosso datalake e dar início às consultas. O primeiro passo é definir um *schema* dos dados a serem ingeridos, isto é, a partir dos dados brutos em **raw**, definimos um schema das informações que gostaríamos de converter. A partir deste *schema*, podemos realizar a ingestão e persistir os dados em formato *.parquet*.

Você pode encontrar estes scripts em `bronze/`.

### 3.3 Tratamentos para Silver

Agora que temos uma forma fácil e otimizada para consumir os dados, podemos criar novas visões de dados, isto é, visões mais analíticas e sumarizadas que ajudarão na criação de análises e modelos preditivos, como as ***feature store***.

Realizamos a criação de um script de *template* para realizar as ingestões em Delta a partir de ***queries SQL***, onde o mesmo se encontra em `silver/template_ingestion.py`.

## 4. Como você pode nos apoiar

Fazemos lives todas terças e quintas na Twitch, então uma maneira totalmente gratuita de nos apoiar é aprender conosco participando das lives.

Caso tenha interesse em nos apoiar financeiramente, você pode:

- Ser Sub na Twitch, com uma assinatura mensal de nosso [canal aqui](https://www.twitch.tv/teomewhy).

- Ser um apoiador [(Sponsor) no GitHub](https://github.com/sponsors/TeoMeWhy), com apoio mensal ou apenas uma vez aqui.

Em ambos tipos de apoio, você poderá ter acesso ao nosso ambiente do Databricks.

Para que você possa acompanhar os valores arrecadados e nossas dispesas, temos [essa planilha aberta](https://docs.google.com/spreadsheets/d/1V5e4aIJTLh1k7kFn_wj5Bn_7_9hDCml1eNcXdK6NhU8/edit?usp=sharing) e atualizada mensalmente.