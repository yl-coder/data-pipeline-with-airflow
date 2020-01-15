from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    facts_sql_template= "INSERT INTO {destination_table} {songplay_table_insert} "

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 destination_table="",
                 songplay_table_insert="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id;
        self.destination_table = destination_table;
        self.songplay_table_insert = songplay_table_insert;

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        facts_sql = LoadFactOperator.facts_sql_template.format(
            destination_table=self.destination_table,
            songplay_table_insert=self.songplay_table_insert
        )
        self.log.info("Loading fact data to table {}".format(self.destination_table))
        redshift.run(facts_sql)
        self.log.info("Loading fact data to table {} Completed".format(self.destination_table))
