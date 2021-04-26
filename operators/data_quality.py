from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

#DataQuality Opeator to validate the records loaded into dimension & fact tables
class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_connection_id,
                 target_table,
                 validate_column,
                 *args, **kwargs):
        '''
            DataQualityOperator init constructor

            arguments:
                redshift_conn_id -- redshift connection id 
                target_table -- target table where data validation need to happen
                validate_column - column in which validation need to be applied
        '''
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_connection_id = redshift_connection_id
        self.target_table = target_table
        self.validate_column = validate_column 

    def execute(self, context):
        '''
            execute gets called when DataQualityOperator is invoked
        '''
        self.log.info('Inside DataQualityOperator --->>>>')
        redshift = PostgresHook(self.redshift_connection_id)
        
        #Validation 1 - Check if table contains records
        for table in self.target_table:
            self.log.info(f"Executing record count validation for table : {table}")
            data_valid_sql = f"SELECT COUNT(*) FROM {table}"
            result = redshift.get_records(data_valid_sql)
            if len(result) < 1 or len(result[0]) < 1:
                raise ValueError(f"Data quality check failed: {table} select returned no results")
            num_records = result[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed: {table} contain 0 rows")
            self.log.info(f"Record count check passed and table {table} contains # {num_records}")
        
        #Validation 2 - Check selected field contains any null values, incase of null throws an error 
        for index, table in enumerate(self.target_table):
            self.log.info(f"Executing column null validation for table:{table} and column:{self.validate_column[index]}")
            data_valid_sql_1 = f"select count(1) from {table} where {self.validate_column[index]} is null"
            print(data_valid_sql_1)
            records = redshift.get_records(data_valid_sql_1)
            print("records value {}".format(records[0][0]))
            if records and records[0][0]> 0 :
                raise ValueError(f"Data quality check failed. {table} contains {len(records)} null records in column {self.validate_column[index]}")
            self.log.info(f"Null validation check passed for table:{table} and column:{self.validate_column[index]}")
            
        self.log.info("Executed Data quality check for all tables")