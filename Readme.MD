# What is this

Small project to learn about Data Engineering. It fetches from a Postgres database on Supabase, compares last identifier in a Databricks table. If Databricks identifier is smaller than Postgres identifier, or is null - then insert differential values. After the insert to Databricks, the data in Databricks is aggregated down to a JSON file and put to an S3 directory which a front end application reads for metrics. 
