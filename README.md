# Trampar de Lakehouse
## Sobre

A pedido do [codista](https://bsky.app/profile/codista.trampardecasa.com.br), criamos um datalake/lakehouse analítico para acompanhar a operação do site [Trampar de Casa](https://www.trampardecasa.com.br/).

A ideia é criar todas as camadas de dados para que se possa realizar as devidas análises, dashboards e até futuros modelos de Machine Learning e Inteligência Artificial.

Todo desenvolvimento foi realizado em live e está disponível para [membros do YouTube](https://www.youtube.com/playlist?list=PLvlkVRRKOYFSG6ORablElWKH-G2K3H9Bn) e [Subs da Twitch](https://www.twitch.tv/collections/SOtxLvG_9Rcfbw).

<img src="https://i.ibb.co/8cmj2dY/trampar-de-casa-drawio.png" alt="trampar-de-casa-drawio">

## Etapas
### Carga em Raw
Com o devido acesso ao bucket Raw, a aplicação do Trampar de Casa enviar um foto do banco de dados em formato `.csv`. Esses dados são uma foto atual do banco representando seu comportamento no momento da extração.

Este processo ocorre diariamente.

### Camada em Bronze
O dado de Raw é consumido a partir de um `Volume` no Databricks, em uma `external location` referente ao bucket raw.

Para realizar a leitura dos dados, utilizamos o Apache Spark. Para a primeira carga, realizamos o processamento `full-load`, isto é, leitura de todos os arquivos do bucket corresponde à tabela ser processada.

Pensando no volume crescente dos dados, para as cargas posteriores, lemos os dados utilizando `AutoLoader` com `streaming` do Apache Spark. Desta maneira, lemos apenas os dados novos entre os processos.

O dado é salvo em formato `delta` com `upsert` das diferenças encontradas entre as ingestões.

### Camada Silver
Com base em regras de negócios e pensando em uma mdoelagem de dados mais analítica, criamos essa camada a partir de consultas SQL, ainda utilizando o Apache Spark.

As cargas nesta camada são `full-load`, ou seja, leitura completa das tabelas em bronze. Ainda pensamos em transformar este processo em streaming com o uso de CDF (Change Data Feed).

### Camada Gold
Nesta camada criamos nosso cubos analíticos com base na necessidade de visualização por parte de negócio.

As cargas também são realizadas em `full-load` com muito SQL e Apache Spark.

### Orquestração

Toda orquestração é realizada com `Databricks Workflows`, sem às 7AM UTC-3.

<img src="https://i.ibb.co/k5XhXxh/image.png" alt="databricks workflows tasks">

A execução, o repositório do projeto é clonado e executado na `branch main`.

## Sobre autor

[Téo Calvo](https://bsky.app/profile/teomewhy.org) é estatístico, educador popular e adora compartilhar conhecimento sobre dados e tecnologia.

Sua missão é transformar vidas por meio do ensino, gerando conteúdo em seu canal [Téo Me Why](https://youtube.com/@teomewhy) e muito mais.

[Conheça mais sobre mim aqui](https://teomewhy.org).

