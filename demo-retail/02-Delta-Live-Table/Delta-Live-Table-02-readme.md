# Demo de engenharia de dados em Delta Live Tables
Esta demonstração abrange um pipeline de dados de uma loja de varejo em uma estrutura de ingestão de arquivos JSON e CSV para tabelas Bronze, tratamento e refinamento para tabelas Silver e, finalmente, a combinação destas em tabelas Gold. 

Todo o pipeline utiliza as Delta Live Tables, que simplifica o pipeline de dados e facilita o processo de monitoramento e garantia de qualidade de suas tabelas.

## Como acompanhar esta demo
---
1. Execute o notebook `02.0-Delta-Live-Table-Preparation` utilizando seu cluster interativo. Este notebook será responsável por:
   1. Descompactar os arquivos de dados brutos que vão emular a chegada de arquivos em seu bucket de ingestão
   2. Gerar os parâmetros necessários para a criação do seu Workflow Delta Live Tables
2. Crie um Workflow de Delta Live Tables inserindo os parâmetros gerados no notebook `02.0-Delta-Live-Table-Preparation`.
    - [Tutorial para criação de workflow Delta Live Tables](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-quickstart.html)
3. Em seu workflow, configure:
    - Product edition: Advanced
    - Pipeline name: `<exportado pelo Notebook 02.0-Delta-Live-Table-Preparation>`
    - Notebook libraries: `<exportado pelo Notebook 02.0-Delta-Live-Table-Preparation>`
    - Configuration
      - Inserir dois conjuntos `chave:valor`:
        - `json_directory` : `<exportado pelo Notebook 02.0-Delta-Live-Table-Preparation>`
        - `csv_directory` : `<exportado pelo Notebook 02.0-Delta-Live-Table-Preparation>`
    - Target: `<exportado pelo Notebook 02.0-Delta-Live-Table-Preparation>`
    - Storage location: `<exportado pelo Notebook 02.0-Delta-Live-Table-Preparation>`
    - Pipeline mode: `Triggered`
    - Autopilot options:
      - Enable auto scaling
      - Min clusters = 0
      - Max clusters = 1
    - Use Photon Acceleration: Sim
4. Execute seu Workflow.

## Acões após finalizar seus estudos 
---
- Após finalizar seus estudos, retorne ao notebook `02.0-Delta-Live-Table-Preparation` e altere o widget `Reset all data` no topo da página para `True` e execute novamente a célula 3 do caderno para limpar as tabelas e arquivos de dados brutos descompactados.