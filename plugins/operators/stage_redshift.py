from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql = """
        COPY {table}
        FROM '{s3_bucket}'
        with credentials
        'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
        format as json {copy_options}
    """
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 copy_options="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.aws_credentials_id = aws_credentials_id
        self.copy_options = copy_options

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table {}".format(self.table))
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            table = self.table,
            s3_bucket = self.s3_bucket,
            access_key = credentials.access_key,
            secret_key = credentials.secret_key,
            copy_options = self.copy_options
        )
        redshift.run(formatted_sql)
        self.log.info("Copying data from S3 to Redshift Completed")




