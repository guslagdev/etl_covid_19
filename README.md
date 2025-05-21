# 🦠 COVID-19 Data Pipeline

Este projeto tem como objetivo processar, transformar e analisar dados públicos de vacinação contra a COVID-19, utilizando **PySpark no ambiente Databricks**. A solução segue a arquitetura **Medallion** (Bronze → Silver → Gold) e responde a três perguntas-chave do desafio.

---

## 🚀 Objetivos Atendidos

✅ **1. Qual país usou mais tipos de vacinas?**  
✅ **2. Top 10 países com mais vacinações por mês e ano**  
✅ **3. Top 10 países anuais por vacinação, incluindo os tipos de vacinas utilizadas, ordenando por diversidade e volume de vacinação**

---

## 🧱 Estrutura dos notebooks

| Notebook           | Descrição |
|--------------------|-----------|
| `lib_functions.py` | Funções utilitárias, parâmetros e configuração do Spark |
| `01.Stage.py`      | Ingestão dos dados crus (CSV e JSON) e criação da camada **bronze** |
| `02.Transformation.py` | Transformação dos dados e criação das tabelas **silver** |
| `03.Gold.py`       | Agregações e rankings para geração da tabela **gold** |

---

## 💾 Fontes de dados

- [`locations.csv`](https://github.com/owid/covid-19-data/blob/master/public/data/vaccinations/locations.csv)  
- [`vaccinations.json`](https://github.com/owid/covid-19-data/blob/master/public/data/vaccinations/vaccinations.json)  

---

## 🛠 Tecnologias usadas

- Apache Spark (PySpark)
- Databricks Community Edition
- Delta Lake (armazenamento em camadas)
- SQL + funções de janela para rankings

---

## ✅ Como executar

1. Importe os notebooks no Databricks
2. Execute os notebooks na ordem: `01.Stage`, `02.Transformation`, `03.Gold`
3. Consulte os blocos de validação (comandos SQL) para ver os resultados

---

ℹ️ Observações

⚠️ **Atenção**:
O arquivo `lib_functions.py` **não precisa ser executado diretamente**, pois já é importado nos demais notebooks com `%run ./lib_functions`.  
No entanto, **ele precisa estar no mesmo diretório** dos notebooks para que a execução funcione corretamente.

---

Desenvolvido por: Gustavo Lagares.