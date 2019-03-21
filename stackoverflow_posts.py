from airflow import DAG
from airflow.contrib.operators import gcs_to_bq
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator
from airflow.contrib.operators.mysql_to_gcs import MySqlToGoogleCloudStorageOperator
from datetime import datetime, timedelta

gcp_connection_id = "google_cloud_default"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 03, 21)
}

def query_to_join_posts_with_users():
    return 'SELECT p.owner_user_id as user_id, u.display_name as user_name, p.score as post_score, p.title as post_title FROM `demo_temp.stackoverflow_posts` as p join `demo_temp.stackoverflow_users` as u on p.owner_user_id = u.id'

with DAG('stackoverflow',
         default_args=default_args,
         schedule_interval="*/10 * * * *") as dag:

    extract_posts_data = MySqlToGoogleCloudStorageOperator(
        task_id='extract_posts_data',
        sql='SELECT * FROM stackoverflow_posts',
        export_format='JSON',
        bucket='demo-sample',
        filename='so_posts.json')

    extract_users_data = PostgresToGoogleCloudStorageOperator(
            task_id='extract_users_data',
            sql='SELECT * FROM stackoverflow_users',
            bucket='demo-sample',
            filename='so_users.json')

    move_users_data_to_staging = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
            task_id='move_users_data_to_staging',
            bucket='demo-sample',
            source_objects=['so_users.json'],
            write_disposition='WRITE_TRUNCATE',
            schema_fields=[{'name': 'id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
                           {'name': 'display_name', 'type': 'STRING', 'mode': 'NULLABLE'},
                           {'name': 'age', 'type':	'STRING', 'mode': 'NULLABLE'},
                           {'name': 'reputation', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                           {'name': 'up_votes', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                           {'name': 'down_votes', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                           {'name': 'views', 'type': 'INTEGER', 'mode': 'NULLABLE'}],
            skip_leading_rows=1,
            ignore_unknown_values=True,
            allow_jagged_rows=True,
            create_disposition='CREATE_IF_NEEDED',
            source_format='NEWLINE_DELIMITED_JSON',
            retries=2,
            retry_delay=timedelta(seconds=15),
            destination_project_dataset_table='demo_temp.stackoverflow_users',
            dag=dag)

    move_posts_data_to_staging = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
                task_id='move_posts_data_to_staging',
                bucket='demo-sample',
                source_objects=['so_posts.json'],
                write_disposition='WRITE_TRUNCATE',
                schema_fields=[
                               {'name': 'id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                               {'name': 'title', 'type': 'STRING', 'mode': 'NULLABLE'},
                               {'name': 'accepted_answer_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                               {'name': 'answer_count', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                               {'name': 'comment_count', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                               {'name': 'favorite_count', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                               {'name': 'last_editor_display_name', 'type': 'STRING', 'mode': 'NULLABLE'},
                               {'name': 'last_editor_user_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                               {'name': 'owner_display_name', 'type': 'STRING', 'mode': 'NULLABLE'},
                               {'name': 'owner_user_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                               {'name': 'parent_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                               {'name': 'post_type_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                               {'name': 'score', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                               {'name': 'view_count', 'type': 'INTEGER', 'mode': 'NULLABLE'}
                               ],
                skip_leading_rows=1,
                ignore_unknown_values=True,
                allow_jagged_rows=True,
                create_disposition='CREATE_IF_NEEDED',
                source_format='NEWLINE_DELIMITED_JSON',
                destination_project_dataset_table='demo_temp.stackoverflow_posts',
                dag=dag)

    transform_and_load = BigQueryOperator(
              task_id='transform_and_load',
              dag=dag,
              bql=query_to_join_posts_with_users(),
              destination_dataset_table='demo.stackoverflow_data',
              use_legacy_sql=False,
              write_disposition='WRITE_TRUNCATE',
              bigquery_conn_id=gcp_connection_id)

    extract_posts_data >> extract_users_data >> move_users_data_to_staging >> move_posts_data_to_staging >> transform_and_load
