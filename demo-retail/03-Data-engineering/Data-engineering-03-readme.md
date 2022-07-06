# Demo de engenharia de dados em fluxo convencional
Esta demonstração abrange um pipeline de dados de uma loja de varejo em uma estrutura de ingestão de arquivos JSON e CSV para tabelas Bronze, tratamento e refinamento para tabelas Silver e, finalmente, a combinação destas em tabelas Gold.

## Como acompanhar esta demo
---
Você pode seguir esta demo de duas formas:
- Executando o Notebook Databricks `03.1-Delta-Pipeline-Ingestion-full` em seu cluster interativo
- Criando um Workflow em seu workspace Databricks utilizando o mesmo notebook como tarefa deste workflow e executando-o

## Acões após finalizar seus estudos 
---
- Após finalizar a execução do notebook, altere o widget `Reset all data` no topo da página para `True` e execute novamente a célula 3 para limpar as tabelas e arquivos de dados brutos descompactados.