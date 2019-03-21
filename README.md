# stackoverflow_etl
Sample Apache Airflow DAG to do the orchestration of etl tasks


Problem
-------

Domain: Stackoverflow
create report where we need the usernames of stackoverflow posts with top scores

Data source
	* users - postgres
	* posts - MySQL

* Extract the data in one place as JSON (GCS)
* Move to a staging area (working area) (Bigquery)
* Transform the data to a format from which users can easily generate the report
* Load into a datasource (Bigquery)

* Generate the report
