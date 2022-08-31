# Game Lake House

Projeto realizado durante lives na Twitch no canal [Téo Me Why](https://www.twitch.tv/teomewhy).

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

### 3. Etapas

#### 3.1 Coleta de dados

O primeiro passo é realizar as coletas de dados pela API do provedor do jogo. No caso do Dota2, utilizamos o [opendota.com](https://www.opendota.com/). Para isso, com ajuda do Python e a biblioteca **requests**, realizamos requisições à API e salvamos os dados no datalake com o Apache Spark em Delta.

Especificamente para o Dota2, utilizamos dois *end-points*:
- [https://api.opendota.com/api/proMatches](https://api.opendota.com/api/proMatches) para coletar o histórico de todas as partidas profissionais.
- [https://api.opendota.com/api/matches/{match_id}](https://api.opendota.com/api/matches/{match_id}) para coletar os detalhes de cada match_id

Tais scripts podem ser encontrados em `/raw`

