from airflow import DAG
# from datetime import datetime, timedelta, date
import datetime
from airflow.utils.dates import days_ago
from dateutil.relativedelta import relativedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.email_operator import EmailOperator
# from airflow.operators.sql import SQLCheckOperator
from airflow.operators.check_operator import CheckOperator
from airflow.hooks.mysql_hook import MySqlHook
# from airflow.hooks.oracle_hook import OracleHook
# from airflow.models import BaseOperator
# from airflow.utils.decorators import apply_defaults

from contextlib import closing
from tempfile import NamedTemporaryFile
from typing import Optional

import MySQLdb
import unicodecsv as csv

from airflow.models import BaseOperator
# from airflow.providers.mysql.hooks.mysql import MySqlHook
# from airflow.providers.vertica.hooks.vertica import VerticaHook
from airflow.utils.decorators import apply_defaults

import sql_statements
default_args = {
    'owner': 'MD.Shafiqul Islam',
    "start_date": datetime.datetime(2021, 8, 8),
    'retries': 1,
    'retry_delay': datetime.timedelta(seconds=5)
}

with DAG(
    'prism_aqm_dwh_etl_geo_total_data_extractor',
    default_args=default_args,
    schedule_interval=None,
    template_searchpath=['/usr/local/airflow/sql_files'],
    catchup=False
) as dag:

    class MySqlToMySqlOperator(BaseOperator):
        """
        Moves data from MySQL to MySQL.

        :param sql: SQL query to execute against the MySQL source database. (templated)
        :type sql: str
        :param mysql_source_conn_id: source MySQL source connection
        :type mysql_source_conn_id: str
        :param mysql_destination_table: target MySQL table, use dot notation to target a
            specific database. (templated)
        :type mysql_destination_table: str
        :param mysql_destination_conn_id: Reference to :ref:`mysql connection id <howto/connection:mysql>`.
        :type mysql_destination_conn_id: str
        :param mysql_destination_preoperator: sql statement to run against MySQL destination prior to
            import, typically use to truncate of delete in place of the data
            coming in, allowing the task to be idempotent (running the task
            twice won't double load data). (templated)
        :type mysql_destination_preoperator: str
        :param mysql_destination_postoperator: sql statement to run against MySQL destination after the
            import, typically used to move data from staging to production
            and issue cleanup commands. (templated)
        :type mysql_destination_postoperator: str
        :param bulk_load: flag to use bulk_load option.  This loads MySQL destination directly
            from a tab-delimited text file using the LOAD DATA LOCAL INFILE command.
            This option requires an extra connection parameter for the
            destination MySQL connection: {'local_infile': true}.
        :type bulk_load: bool
        """

        template_fields = ('sql', 'mysql_destination_table', 'mysql_destination_preoperator', 'mysql_destination_postoperator')

        template_ext = ('.sql',)

        template_fields_renderers = {
            "mysql_destination_preoperator": "sql",
            "mysql_destination_postoperator": "sql",

        }
        ui_color = '#a0e08c'


        @apply_defaults
        def __init__(
            self,
            sql: str,
            mysql_destination_table: str,
            mysql_source_conn_id: str = 'mysql_source_default',
            mysql_destination_conn_id: str = 'mysql_destination_default',
            mysql_destination_preoperator: Optional[str] = None,
            mysql_destination_postoperator: Optional[str] = None,
            bulk_load: bool = False,
            *args,
            **kwargs,
        ) -> None:
            super().__init__(*args, **kwargs)
            self.sql = sql
            self.mysql_destination_table = mysql_destination_table
            self.mysql_destination_conn_id = mysql_destination_conn_id
            self.mysql_destination_preoperator = mysql_destination_preoperator
            self.mysql_destination_postoperator = mysql_destination_postoperator
            self.mysql_source_conn_id = mysql_source_conn_id
            self.bulk_load = bulk_load

        def execute(self, context):
            mysql_source = MySqlHook(mysql_conn_id=self.mysql_source_conn_id)
            print('line_113', mysql_source.get_conn())
            mysql_destination = MySqlHook(mysql_conn_id=self.mysql_destination_conn_id)

            mysql_source_conn = mysql_source.get_conn()
            mysql_source_conn_cursor = mysql_source_conn.cursor()
            mysql_source_conn.set_character_set('utf8')
            mysql_source_conn_cursor.execute('SET NAMES utf8;')
            mysql_source_conn_cursor.execute('SET CHARACTER SET utf8;')
            mysql_source_conn_cursor.execute('SET character_set_connection=utf8;')

            mysql_destination_conn = mysql_destination.get_conn()
            mysql_destination_conn_cursor = mysql_destination_conn.cursor()
            mysql_destination_conn.set_character_set('utf8')
            mysql_destination_conn_cursor.execute('SET NAMES utf8;')
            mysql_destination_conn_cursor.execute('SET CHARACTER SET utf8;')
            mysql_destination_conn_cursor.execute('SET character_set_connection=utf8;')


            tmpfile = None
            result = None

            selected_columns = []

            count = 0
            with closing(mysql_source_conn) as conn:
                with closing(conn.cursor()) as cursor:
                    cursor.execute(self.sql)
                    selected_columns = [d[0] for d in cursor.description]
                    selected_columns = [column.replace('group','`group`') for column in selected_columns]

                    self.log.info(cursor.description)
                    self.log.info(selected_columns)

                    if self.bulk_load:
                        with NamedTemporaryFile("wb") as tmpfile:
                            self.log.info("Selecting rows from MySql to local file %s...", tmpfile.name)
                            self.log.info(self.sql)

                            csv_writer = csv.writer(tmpfile, delimiter='\t', encoding='utf-8')
                            result = cursor.fetchall()
                            for row in result:
                                csv_writer.writerow(row)
                                count += 1

                            tmpfile.flush()
                    else:
                        self.log.info("Selecting rows from MySQL source...")
                        self.log.info(self.sql)

                        result = cursor.fetchall()
                        # self.log.info(result)
                        count = len(result)

                    self.log.info("Selected rows from MySql source %s", count)

            if self.mysql_destination_preoperator:
                self.log.info("Running MySQL destination preoperator...")
                mysql_destination.run(self.mysql_destination_preoperator)

            try:
                if self.bulk_load:
                    self.log.info("Bulk inserting rows into MySQL destination...")
                    with closing(mysql_destination.get_conn()) as conn:
                        with closing(conn.cursor()) as cursor:
                            cursor.execute(
                                "LOAD DATA LOCAL INFILE '%s' INTO "
                                "TABLE %s LINES TERMINATED BY '\r\n' (%s)" %
                                (tmpfile.name,
                                self.mysql_destination_table,
                                ", ".join(selected_columns))
                            )
                            conn.commit()
                    tmpfile.close()
                else:
                    self.log.info("Inserting rows into MySQL destination...")
                    mysql_destination.insert_rows(table=self.mysql_destination_table, rows=result, target_fields=selected_columns)
                self.log.info("Inserted rows into MySQL destination %s", count)
            except (MySQLdb.Error, MySQLdb.Warning):  # pylint: disable=no-member
                self.log.info("Inserted rows into MySQL destinataion 0")
                self.log.info(mysql_destination_conn_cursor._executed)
                raise

            if self.mysql_destination_postoperator:
                self.log.info("Running MySQL destination postoperator...")
                mysql_destination.run(self.mysql_destination_postoperator)

            self.log.info("Done")

    class GeoDistanceByOutletTableToGeoDistanceByOutletFactTableMySqlOperator(BaseOperator):
        """
        Moves data from MySQL to MySQL.

        :param sql: SQL query to execute against the MySQL source database. (templated)
        :type sql: str
        :param mysql_source_conn_id: source MySQL source connection
        :type mysql_source_conn_id: str
        :param mysql_destination_table: target MySQL table, use dot notation to target a
            specific database. (templated)
        :type mysql_destination_table: str
        :param mysql_destination_conn_id: Reference to :ref:`mysql connection id <howto/connection:mysql>`.
        :type mysql_destination_conn_id: str
        :param mysql_destination_preoperator: sql statement to run against MySQL destination prior to
            import, typically use to truncate of delete in place of the data
            coming in, allowing the task to be idempotent (running the task
            twice won't double load data). (templated)
        :type mysql_destination_preoperator: str
        :param mysql_destination_postoperator: sql statement to run against MySQL destination after the
            import, typically used to move data from staging to production
            and issue cleanup commands. (templated)
        :type mysql_destination_postoperator: str
        :param bulk_load: flag to use bulk_load option.  This loads MySQL destination directly
            from a tab-delimited text file using the LOAD DATA LOCAL INFILE command.
            This option requires an extra connection parameter for the
            destination MySQL connection: {'local_infile': true}.
        :type bulk_load: bool
        """

        # template_fields = ('sql', 'mysql_destination_table', 'mysql_destination_preoperator', 'mysql_destination_postoperator')

        # template_ext = ('.sql',)

        # template_fields_renderers = {
        #     "mysql_destination_preoperator": "sql",
        #     "mysql_destination_postoperator": "sql",
        #
        # }
        ui_color = '#a0e08c'


        @apply_defaults
        def __init__(
            self,
            # sql: str,
            starting_date,
            ending_date,
            # start_month,
            # end_month,
            mysql_destination_table: str,
            mysql_source_conn_id: str = 'mysql_source_default',
            mysql_destination_conn_id: str = 'mysql_destination_default',
            mysql_destination_preoperator: Optional[str] = None,
            mysql_destination_postoperator: Optional[str] = None,
            bulk_load: bool = False,
            *args,
            **kwargs,
        ) -> None:
            super().__init__(*args, **kwargs)
            # self.sql = sql
            self.starting_date = starting_date
            self.ending_date = ending_date
            # self.start_month = start_month
            # self.end_month = end_month
            self.mysql_destination_table = mysql_destination_table
            self.mysql_destination_conn_id = mysql_destination_conn_id
            self.mysql_destination_preoperator = mysql_destination_preoperator
            self.mysql_destination_postoperator = mysql_destination_postoperator
            self.mysql_source_conn_id = mysql_source_conn_id
            self.bulk_load = bulk_load

        def execute(self, context):
            mysql_source = MySqlHook(mysql_conn_id=self.mysql_source_conn_id)
            print('line_113', mysql_source.get_conn())
            mysql_destination = MySqlHook(mysql_conn_id=self.mysql_destination_conn_id)
            mysql_source_conn = mysql_source.get_conn()
            mysql_source_conn_cursor = mysql_source_conn.cursor()
            mysql_source_conn.set_character_set('utf8')
            mysql_source_conn_cursor.execute('SET NAMES utf8;')
            mysql_source_conn_cursor.execute('SET CHARACTER SET utf8;')
            mysql_source_conn_cursor.execute('SET character_set_connection=utf8;')

            mysql_destination_conn = mysql_destination.get_conn()
            mysql_destination_conn_cursor = mysql_destination_conn.cursor()
            mysql_destination_conn.set_character_set('utf8')
            mysql_destination_conn_cursor.execute('SET NAMES utf8;')
            mysql_destination_conn_cursor.execute('SET CHARACTER SET utf8;')
            mysql_destination_conn_cursor.execute('SET character_set_connection=utf8;')


            days_list = []

            delta = self.ending_date - self.starting_date       # as timedelta

            for i in range(delta.days + 1):
                day = self.starting_date + datetime.timedelta(days=i)
                print(day)
                days_list.append(datetime.datetime.strftime(day, '%Y-%m-%d'))

            tmpfile = None
            result = None

            selected_columns = []

            count = 0
            for day in days_list:
                with closing(mysql_source.get_conn()) as conn:
                    with closing(conn.cursor()) as cursor:
                        # cursor.execute('set global max_allowed_packet=67108864')
                        cursor.execute("select * from geo_distance_by_outlet where date between '"+day+"' and '"+day+"'")
                        # self.log.info(cursor.execute())
                        # cursor.execute(self.sql)
                        # print(cursor.description)
                        print(day)
                        # print(type(cursor))
                        selected_columns = [d[0] for d in cursor.description]
                        selected_columns = [column.replace('explain','`explain`') for column in selected_columns]
                        self.log.info(cursor.description)
                        self.log.info(selected_columns)

                        if self.bulk_load:
                            with NamedTemporaryFile("wb") as tmpfile:
                                self.log.info("Selecting rows from MySql to local file %s...", tmpfile.name)
                                self.log.info("select mtime, rtslug, route_id, rtlid, skid, volume, value, date, outlets, visited, channel, ioq, sales_time from query_manager_table where date between '"+day+"' and '"+day+"'")

                                # self.log.info("select mtime, rtslug, rtlid, skid, volume, value, date, outlets, visited, ioq, sales_time from query_manager_table where month(date) = month("+month+") and year(date) = year("+month+")")
                                # self.log.info(self.sql)

                                csv_writer = csv.writer(tmpfile, delimiter='\t', encoding='utf-8')
                                result = cursor.fetchall()
                                # print(result)
                                for row in result:
                                    # print(row)
                                    csv_writer.writerow(row)
                                    count += 1

                                tmpfile.flush()
                        else:
                            self.log.info("Selecting rows from MySQL source...")
                            self.log.info("select mtime, rtslug, route_id, rtlid, skid, volume, value, date, outlets, visited, channel, ioq, sales_time from query_manager_table where date between '"+day+"' and '"+day+"'")

                                # self.log.info("select mtime, rtslug, rtlid, skid, volume, value, date, outlets, visited, ioq, sales_time from query_manager_table where month(date) = month("+month+") and year(date) = year("+month+")")
                                # self.log.info(self.sql)

                            result = cursor.fetchall()
                            count = len(result)

                        self.log.info("Selected rows from MySql source %s", count)

                if self.mysql_destination_preoperator:
                    self.log.info("Running MySQL destination preoperator...")
                    mysql_destination.run(self.mysql_destination_preoperator)

                try:
                    if self.bulk_load:
                        self.log.info("Bulk inserting rows into MySQL destination...")
                        with closing(mysql_destination.get_conn()) as conn:
                            with closing(conn.cursor()) as cursor:
                                cursor.execute(
                                    "LOAD DATA LOCAL INFILE '%s' INTO "
                                    "TABLE %s LINES TERMINATED BY '\r\n' (%s)" %
                                    (tmpfile.name,
                                    self.mysql_destination_table,
                                    ", ".join(selected_columns))
                                    )
                                conn.commit()
                        tmpfile.close()
                    else:
                        self.log.info("Inserting rows into MySQL destination...")
                        mysql_destination.insert_rows(table=self.mysql_destination_table, rows=result, target_fields=selected_columns)
                    self.log.info("Inserted rows into MySQL destination %s", count)
                except (MySQLdb.Error, MySQLdb.Warning):  # pylint: disable=no-member
                    self.log.info("Inserted rows into MySQL destinataion 0")
                    self.log.info(mysql_destination_conn_cursor._executed)
                    raise

                if self.mysql_destination_postoperator:
                    self.log.info("Running MySQL destination postoperator...")
                    mysql_destination.run(self.mysql_destination_postoperator)

                self.log.info("Done")


    class GeoDistanceBySectionTableToGeoDistanceBySectionFactTableMySqlOperator(BaseOperator):
        """
        Moves data from MySQL to MySQL.

        :param sql: SQL query to execute against the MySQL source database. (templated)
        :type sql: str
        :param mysql_source_conn_id: source MySQL source connection
        :type mysql_source_conn_id: str
        :param mysql_destination_table: target MySQL table, use dot notation to target a
            specific database. (templated)
        :type mysql_destination_table: str
        :param mysql_destination_conn_id: Reference to :ref:`mysql connection id <howto/connection:mysql>`.
        :type mysql_destination_conn_id: str
        :param mysql_destination_preoperator: sql statement to run against MySQL destination prior to
            import, typically use to truncate of delete in place of the data
            coming in, allowing the task to be idempotent (running the task
            twice won't double load data). (templated)
        :type mysql_destination_preoperator: str
        :param mysql_destination_postoperator: sql statement to run against MySQL destination after the
            import, typically used to move data from staging to production
            and issue cleanup commands. (templated)
        :type mysql_destination_postoperator: str
        :param bulk_load: flag to use bulk_load option.  This loads MySQL destination directly
            from a tab-delimited text file using the LOAD DATA LOCAL INFILE command.
            This option requires an extra connection parameter for the
            destination MySQL connection: {'local_infile': true}.
        :type bulk_load: bool
        """

        # template_fields = ('sql', 'mysql_destination_table', 'mysql_destination_preoperator', 'mysql_destination_postoperator')

        # template_ext = ('.sql',)

        # template_fields_renderers = {
        #     "mysql_destination_preoperator": "sql",
        #     "mysql_destination_postoperator": "sql",
        #
        # }
        ui_color = '#a0e08c'


        @apply_defaults
        def __init__(
            self,
            # sql: str,
            starting_date,
            ending_date,
            # start_month,
            # end_month,
            mysql_destination_table: str,
            mysql_source_conn_id: str = 'mysql_source_default',
            mysql_destination_conn_id: str = 'mysql_destination_default',
            mysql_destination_preoperator: Optional[str] = None,
            mysql_destination_postoperator: Optional[str] = None,
            bulk_load: bool = False,
            *args,
            **kwargs,
        ) -> None:
            super().__init__(*args, **kwargs)
            # self.sql = sql
            self.starting_date = starting_date
            self.ending_date = ending_date
            # self.start_month = start_month
            # self.end_month = end_month
            self.mysql_destination_table = mysql_destination_table
            self.mysql_destination_conn_id = mysql_destination_conn_id
            self.mysql_destination_preoperator = mysql_destination_preoperator
            self.mysql_destination_postoperator = mysql_destination_postoperator
            self.mysql_source_conn_id = mysql_source_conn_id
            self.bulk_load = bulk_load

        def execute(self, context):
            mysql_source = MySqlHook(mysql_conn_id=self.mysql_source_conn_id)
            print('line_113', mysql_source.get_conn())
            mysql_destination = MySqlHook(mysql_conn_id=self.mysql_destination_conn_id)
            mysql_source_conn = mysql_source.get_conn()
            mysql_source_conn_cursor = mysql_source_conn.cursor()
            mysql_source_conn.set_character_set('utf8')
            mysql_source_conn_cursor.execute('SET NAMES utf8;')
            mysql_source_conn_cursor.execute('SET CHARACTER SET utf8;')
            mysql_source_conn_cursor.execute('SET character_set_connection=utf8;')

            mysql_destination_conn = mysql_destination.get_conn()
            mysql_destination_conn_cursor = mysql_destination_conn.cursor()
            mysql_destination_conn.set_character_set('utf8')
            mysql_destination_conn_cursor.execute('SET NAMES utf8;')
            mysql_destination_conn_cursor.execute('SET CHARACTER SET utf8;')
            mysql_destination_conn_cursor.execute('SET character_set_connection=utf8;')


            days_list = []

            delta = self.ending_date - self.starting_date       # as timedelta

            for i in range(delta.days + 1):
                day = self.starting_date + datetime.timedelta(days=i)
                print(day)
                days_list.append(datetime.datetime.strftime(day, '%Y-%m-%d'))

            tmpfile = None
            result = None

            selected_columns = []

            count = 0
            for day in days_list:
                with closing(mysql_source.get_conn()) as conn:
                    with closing(conn.cursor()) as cursor:
                        # cursor.execute('set global max_allowed_packet=67108864')
                        cursor.execute("select * from geo_distance_by_section where date between '"+day+"' and '"+day+"'")
                        # self.log.info(cursor.execute())
                        # cursor.execute(self.sql)
                        # print(cursor.description)
                        print(day)
                        # print(type(cursor))
                        selected_columns = [d[0] for d in cursor.description]
                        selected_columns = [column.replace('explain','`explain`') for column in selected_columns]
                        self.log.info(cursor.description)
                        self.log.info(selected_columns)

                        if self.bulk_load:
                            with NamedTemporaryFile("wb") as tmpfile:
                                self.log.info("Selecting rows from MySql to local file %s...", tmpfile.name)
                                self.log.info("select mtime, rtslug, route_id, rtlid, skid, volume, value, date, outlets, visited, channel, ioq, sales_time from query_manager_table where date between '"+day+"' and '"+day+"'")

                                # self.log.info("select mtime, rtslug, rtlid, skid, volume, value, date, outlets, visited, ioq, sales_time from query_manager_table where month(date) = month("+month+") and year(date) = year("+month+")")
                                # self.log.info(self.sql)

                                csv_writer = csv.writer(tmpfile, delimiter='\t', encoding='utf-8')
                                result = cursor.fetchall()
                                # print(result)
                                for row in result:
                                    # print(row)
                                    csv_writer.writerow(row)
                                    count += 1

                                tmpfile.flush()
                        else:
                            self.log.info("Selecting rows from MySQL source...")
                            self.log.info("select mtime, rtslug, route_id, rtlid, skid, volume, value, date, outlets, visited, channel, ioq, sales_time from query_manager_table where date between '"+day+"' and '"+day+"'")

                                # self.log.info("select mtime, rtslug, rtlid, skid, volume, value, date, outlets, visited, ioq, sales_time from query_manager_table where month(date) = month("+month+") and year(date) = year("+month+")")
                                # self.log.info(self.sql)

                            result = cursor.fetchall()
                            count = len(result)

                        self.log.info("Selected rows from MySql source %s", count)

                if self.mysql_destination_preoperator:
                    self.log.info("Running MySQL destination preoperator...")
                    mysql_destination.run(self.mysql_destination_preoperator)

                try:
                    if self.bulk_load:
                        self.log.info("Bulk inserting rows into MySQL destination...")
                        with closing(mysql_destination.get_conn()) as conn:
                            with closing(conn.cursor()) as cursor:
                                cursor.execute(
                                    "LOAD DATA LOCAL INFILE '%s' INTO "
                                    "TABLE %s LINES TERMINATED BY '\r\n' (%s)" %
                                    (tmpfile.name,
                                    self.mysql_destination_table,
                                    ", ".join(selected_columns))
                                    )
                                conn.commit()
                        tmpfile.close()
                    else:
                        self.log.info("Inserting rows into MySQL destination...")
                        mysql_destination.insert_rows(table=self.mysql_destination_table, rows=result, target_fields=selected_columns)
                    self.log.info("Inserted rows into MySQL destination %s", count)
                except (MySQLdb.Error, MySQLdb.Warning):  # pylint: disable=no-member
                    self.log.info("Inserted rows into MySQL destinataion 0")
                    self.log.info(mysql_destination_conn_cursor._executed)
                    raise

                if self.mysql_destination_postoperator:
                    self.log.info("Running MySQL destination postoperator...")
                    mysql_destination.run(self.mysql_destination_postoperator)

                self.log.info("Done")

    start_operator = DummyOperator(task_id='Begin_execution')

    geo_distance_by_outlet_table_to_geo_distance_by_outlet_fact_table = GeoDistanceByOutletTableToGeoDistanceByOutletFactTableMySqlOperator(
                        task_id = 'geo_distance_by_outlet_table_to_geo_distance_by_outlet_fact_table',
                        starting_date = datetime.date(2021, 7, 1),
                        ending_date = datetime.date(2021, 8, 9),
                        mysql_source_conn_id = "geo_bat",
                        mysql_destination_table = "geo_distance_by_outlet_fact",
                        mysql_destination_conn_id = "pass_aqm_dwh_database",
                        mysql_destination_preoperator = sql_statements.CREATE_geo_distance_by_outlet_fact_TABLE_IF_NOT_EXISTS,
                        bulk_load = False
    )

    geo_distance_by_section_table_to_geo_distance_by_section_fact_table = GeoDistanceBySectionTableToGeoDistanceBySectionFactTableMySqlOperator(
                        task_id = 'geo_distance_by_section_table_to_geo_distance_by_section_fact_table',
                        starting_date = datetime.date(2021, 1, 1),
                        ending_date = datetime.date(2021, 8, 9),
                        mysql_source_conn_id = "geo_bat",
                        mysql_destination_table = "geo_distance_by_section_fact",
                        mysql_destination_conn_id = "pass_aqm_dwh_database",
                        mysql_destination_preoperator = sql_statements.CREATE_geo_distance_by_section_fact_TABLE_IF_NOT_EXISTS,
                        bulk_load = False
    )

    run_quality_checks_geo_distance_by_outlet_fact = CheckOperator(
                       task_id = 'run_data_quality_checks_geo_distance_by_outlet_fact',
                       sql = "select count(*) from geo_distance_by_outlet_fact",
                       conn_id = "pass_aqm_dwh_database",
    )

    run_quality_checks_geo_distance_by_section_fact = CheckOperator(
                       task_id = 'run_data_quality_checks_geo_distance_by_section_fact',
                       sql = "select count(*) from geo_distance_by_section_fact",
                       conn_id = "pass_aqm_dwh_database",
    )

    send_success_email = EmailOperator(
                task_id='send_email',
                to='shafiqul.islam@apsissolutions.com',
                subject='Daily ETL process for PRISM SALES AQM DWH',
                html_content=""" <h1>Congratulations! Daily ETL for PRISM AQM DWH process has been completed.</h1> """,
    )

    start_operator >> geo_distance_by_outlet_table_to_geo_distance_by_outlet_fact_table
    geo_distance_by_outlet_table_to_geo_distance_by_outlet_fact_table >> geo_distance_by_section_table_to_geo_distance_by_section_fact_table
    geo_distance_by_section_table_to_geo_distance_by_section_fact_table >> run_quality_checks_geo_distance_by_outlet_fact
    run_quality_checks_geo_distance_by_outlet_fact >> run_quality_checks_geo_distance_by_section_fact
    run_quality_checks_geo_distance_by_section_fact >> send_success_email
