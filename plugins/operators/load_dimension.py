from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    sql_template= "INSERT INTO {destination_table} {songplay_table_insert} "
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 destination_table="",
                 sql_query="",
                 mode="overwrite",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.destination_table = destination_table
        self.sql_query = sql_query
        self.mode = mode

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.mode == "overwrite":
            self.log.info("Clearing data from destination Redshift table {}".format(self.destination_table))
            redshift.run("DELETE FROM {}".format(self.destination_table))
 
        
        facts_sql = LoadDimensionOperator.sql_template.format(
            destination_table=self.destination_table,
            songplay_table_insert=self.sql_query
        )
        self.log.info("Loading dimension data to table {}".format(self.destination_table))
        redshift.run(facts_sql)
        self.log.info("Loading dimension data to table {} completed".format(self.destination_table))
