## 📊 Enriquecimento e Análise de CNPJs

Este projeto tem como objetivo consultar dados de CNPJs na API pública da ReceitaWS, enriquecer essas informações, salvar os dados em um arquivo .csv e, por fim, realizar uma análise da frequência das datas de abertura das empresas.

### ✅ Funcionalidades

🔍 Leitura de uma lista de CNPJs a partir de um arquivo CSV.

🌐 Consulta de dados na API da ReceitaWS.

🧹 Tratamento e limpeza de dados (remoção de caracteres, extração de códigos).

💾 Salvamento dos dados enriquecidos em um novo CSV.

📈 Análise da data de abertura mais comum entre as empresas.

🏆 Classificação de todas as empresas com base na frequência da data de abertura, utilizando SQL com SQLite em memória.

### 🛠 Tecnologias usadas

Python 3.11.9

Biblioteca requests

Módulo csv e sqlite3 da biblioteca padrão do Python

### 📌 Observações

A API gratuita da ReceitaWS permite apenas 3 requisições por minuto. Por isso, foi implementado um sleep(20) após cada consulta.

O projeto usa SQLite em memória para simular uma análise relacional (sem precisar instalar banco externo).

Aprendi muito durante esse projeto teste, fica o agradecimento a equipe da APPMAX.

Não Utilizei PySpark por estar dando alguns conflitos que não consegui resolver com meu computador (windows)
