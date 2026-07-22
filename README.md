# metrics-materializer
Questa repo contiene il materiale che è stato creato per l'applicazione di materializzazione delle metriche

Per il primo avvio inserisci il tuo token personale in databricks.cfg che deve essere taggato su databricks con SQL e cluster.

Esempio in `databricks.cfg`:

[DEFAULT]
host = https://<workspace>.azuredatabricks.net/
token = dapiXXXXXXXXXXXXXXXX

In alternativa puoi usare variabili ambiente:

- DATABRICKS_HOST
- DATABRICKS_TOKEN
- DATABRICKS_WAREHOUSE_ID

Nota: l'app ora prova in questo ordine:

1. variabili ambiente / config applicativa
2. file locale `databricks.cfg` nella root del progetto
3. meccanismo di default del Databricks SDK