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
import tempfile
# from tempfile import NamedTemporaryFile
from typing import Optional

import MySQLdb
import unicodecsv as csv

from airflow.models import BaseOperator
# from airflow.providers.mysql.hooks.mysql import MySqlHook
# from airflow.providers.vertica.hooks.vertica import VerticaHook
from airflow.utils.decorators import apply_defaults

import sql_statements
import sql_statements_mariadb_column_store

# from datacleaner import data_cleaner

yesterday_date = datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(1), '%Y-%m-%d')
# yesterday_date = datetime.datetime.strftime(datetime.datetime(2022, 3, 29), '%Y-%m-%d')

# current_month = datetime.strftime(datetime.now(), '%Y-%m-%d')
# previous_month = datetime.strftime(datetime.now() - timedelta(30), '%Y-%m-%d')


default_args = {
    'owner': 'MD.Shafiqul Islam',
    'start_date': datetime.datetime(2022, 4, 5),
    'retries': 1,
    'retry_delay': datetime.timedelta(seconds=5)
}

with DAG(
    'prism_aqm_dwh_etl_daily_data_extractor_mariadb_column_store_part_2',
    default_args=default_args,
    schedule_interval='0 2 * * *',
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

            # tmpfile = None
            tmpfile = tempfile.NamedTemporaryFile()

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
                        with open (tmpfile.name, "wb") as f:
                        # with NamedTemporaryFile("wb") as tmpfile:
                            self.log.info("Selecting rows from MySql to local file %s...", tmpfile.name)
                            self.log.info(self.sql)

                            csv_writer = csv.writer(f, delimiter='\t', encoding='utf-8')
                            result = cursor.fetchall()
                            for row in result:
                                csv_writer.writerow(row)
                                count += 1

                            f.flush()
                            # tmpfile.flush()
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
                                "LOAD DATA LOCAL INFILE '%s' IGNORE INTO "
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
            date,
            # ending_date,
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
            self.date = date
            # self.ending_date = ending_date
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


            # days_list = []
            #
            # delta = self.ending_date - self.starting_date       # as timedelta
            #
            # for i in range(delta.days + 1):
            #     day = self.starting_date + datetime.timedelta(days=i)
            #     print(day)
            #     days_list.append(datetime.datetime.strftime(day, '%Y-%m-%d'))

            # tmpfile = None
            tmpfile = tempfile.NamedTemporaryFile()

            result = None

            selected_columns = []

            count = 0
            with closing(mysql_source.get_conn()) as conn:
                with closing(conn.cursor()) as cursor:
                    # cursor.execute('set global max_allowed_packet=67108864')
                    cursor.execute("select * from geo_distance_by_outlet where date between '"+self.date+"' and '"+self.date+"'")
                    # self.log.info(cursor.execute())
                    # cursor.execute(self.sql)
                    # print(cursor.description)
                    self.log.info(self.date)
                    # print(type(cursor))
                    selected_columns = [d[0] for d in cursor.description]
                    selected_columns = [column.replace('explain','`explain`') for column in selected_columns]
                    self.log.info(cursor.description)
                    self.log.info(selected_columns)

                    if self.bulk_load:
                        with open (tmpfile.name, "wb") as f:
                        # with NamedTemporaryFile("wb") as tmpfile:
                            self.log.info("Selecting rows from MySql to local file %s...", tmpfile.name)
                            # self.log.info("select mtime, rtslug, route_id, rtlid, skid, volume, value, date, outlets, visited, channel, ioq, sales_time from query_manager_table where date between '"+day+"' and '"+day+"'")

                            # self.log.info("select mtime, rtslug, rtlid, skid, volume, value, date, outlets, visited, ioq, sales_time from query_manager_table where month(date) = month("+month+") and year(date) = year("+month+")")
                            # self.log.info(self.sql)

                            csv_writer = csv.writer(f, delimiter='\t', encoding='utf-8')
                            result = cursor.fetchall()
                            # print(result)
                            for row in result:
                                # print(row)
                                csv_writer.writerow(row)
                                count += 1

                            f.flush()
                            # tmpfile.flush()
                    else:
                        self.log.info("Selecting rows from MySQL source...")
                        # self.log.info("select mtime, rtslug, route_id, rtlid, skid, volume, value, date, outlets, visited, channel, ioq, sales_time from query_manager_table where date between '"+day+"' and '"+day+"'")

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
                                "LOAD DATA LOCAL INFILE '%s' IGNORE INTO "
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
            date,
            # ending_date,
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
            self.date = date
            # self.ending_date = ending_date
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


            # days_list = []
            #
            # delta = self.ending_date - self.starting_date       # as timedelta
            #
            # for i in range(delta.days + 1):
            #     day = self.starting_date + datetime.timedelta(days=i)
            #     print(day)
            #     days_list.append(datetime.datetime.strftime(day, '%Y-%m-%d'))

            # tmpfile = None
            tmpfile = tempfile.NamedTemporaryFile()

            result = None

            selected_columns = []

            count = 0
            with closing(mysql_source.get_conn()) as conn:
                with closing(conn.cursor()) as cursor:
                    # cursor.execute('set global max_allowed_packet=67108864')
                    cursor.execute("select * from geo_distance_by_section where date between '"+self.date+"' and '"+self.date+"'")
                    # self.log.info(cursor.execute())
                    # cursor.execute(self.sql)
                    # print(cursor.description)
                    self.log.info(self.date)
                    # print(type(cursor))
                    selected_columns = [d[0] for d in cursor.description]
                    selected_columns = [column.replace('explain','`explain`') for column in selected_columns]
                    self.log.info(cursor.description)
                    self.log.info(selected_columns)

                    if self.bulk_load:
                        with open (tmpfile.name, "wb") as f:
                        # with NamedTemporaryFile("wb") as tmpfile:
                            self.log.info("Selecting rows from MySql to local file %s...", tmpfile.name)
                            # self.log.info("select mtime, rtslug, route_id, rtlid, skid, volume, value, date, outlets, visited, channel, ioq, sales_time from query_manager_table where date between '"+day+"' and '"+day+"'")

                            # self.log.info("select mtime, rtslug, rtlid, skid, volume, value, date, outlets, visited, ioq, sales_time from query_manager_table where month(date) = month("+month+") and year(date) = year("+month+")")
                            # self.log.info(self.sql)

                            csv_writer = csv.writer(f, delimiter='\t', encoding='utf-8')
                            result = cursor.fetchall()
                            # print(result)
                            for row in result:
                                # print(row)
                                csv_writer.writerow(row)
                                count += 1

                            f.flush()
                            # tmpfile.flush()
                    else:
                        self.log.info("Selecting rows from MySQL source...")
                        # self.log.info("select mtime, rtslug, route_id, rtlid, skid, volume, value, date, outlets, visited, channel, ioq, sales_time from query_manager_table where date between '"+day+"' and '"+day+"'")

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
                                "LOAD DATA LOCAL INFILE '%s' IGNORE INTO "
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

    class HrAnomalyTableToHrAnomalyDimensionTableMySqlOperator(BaseOperator):
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
            date,
            # ending_date,
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
            self.date = date
            # self.ending_date = ending_date
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


            # days_list = []
            #
            # delta = self.ending_date - self.starting_date       # as timedelta
            #
            # for i in range(delta.days + 1):
            #     day = self.starting_date + datetime.timedelta(days=i)
            #     print(day)
            #     days_list.append(datetime.datetime.strftime(day, '%Y-%m-%d'))

            # tmpfile = None
            tmpfile = tempfile.NamedTemporaryFile()

            result = None

            selected_columns = []

            count = 0
            with closing(mysql_source.get_conn()) as conn:
                with closing(conn.cursor()) as cursor:
                    # cursor.execute('set global max_allowed_packet=67108864')
                    cursor.execute("select * from hr_anomaly where date(created_at) between '"+self.date+"' and '"+self.date+"'")

                    # self.log.info(cursor.execute())
                    # cursor.execute(self.sql)
                    # print(cursor.description)
                    self.log.info(self.date)
                    # print(type(cursor))
                    selected_columns = [d[0] for d in cursor.description]
                    selected_columns = [column.replace('explain','`explain`') for column in selected_columns]
                    # selected_columns = [column.replace('group','`group`') for column in selected_columns]
                    self.log.info(cursor.description)
                    self.log.info(selected_columns)

                    if self.bulk_load:
                        with open (tmpfile.name, "wb") as f:
                        # with NamedTemporaryFile("wb") as tmpfile:
                            self.log.info("Selecting rows from MySql to local file %s...", tmpfile.name)
                            # self.log.info("select mtime, rtslug, route_id, rtlid, skid, volume, value, date, outlets, visited, channel, ioq, sales_time from query_manager_table where date between '"+day+"' and '"+day+"'")

                            # self.log.info("select mtime, rtslug, rtlid, skid, volume, value, date, outlets, visited, ioq, sales_time from query_manager_table where month(date) = month("+month+") and year(date) = year("+month+")")
                            # self.log.info(self.sql)

                            csv_writer = csv.writer(f, delimiter='\t', encoding='utf-8')
                            result = cursor.fetchall()
                            # print(result)
                            for row in result:
                                # print(row)
                                csv_writer.writerow(row)
                                count += 1

                            f.flush()
                            # tmpfile.flush()
                    else:
                        self.log.info("Selecting rows from MySQL source...")
                        # self.log.info("select mtime, rtslug, route_id, rtlid, skid, volume, value, date, outlets, visited, channel, ioq, sales_time from query_manager_table where date between '"+day+"' and '"+day+"'")

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
                                "LOAD DATA LOCAL INFILE '%s' IGNORE INTO "
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

    class HrmEmpAttendanceTableToHrmEmpAtenndanceFactTableMySqlOperator(BaseOperator):
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
            date,
            # ending_date,
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
            self.date = date
            # self.ending_date = ending_date
            # self.end_month = end_month
            # self.start_month = start_month
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

            # month_list = []
            # days_list = []
            #
            #
            # delta = self.ending_date - self.starting_date       # as timedelta
            #
            # for i in range(delta.days + 1):
            #     day = self.starting_date + datetime.timedelta(days=i)
            #     print(day)
            #     days_list.append(datetime.datetime.strftime(day, '%Y-%m-%d'))

            # tmpfile = None
            tmpfile = tempfile.NamedTemporaryFile()

            result = None

            selected_columns = []

            count = 0
            with closing(mysql_source.get_conn()) as conn:
                with closing(conn.cursor()) as cursor:
                    # cursor.execute('set global max_allowed_packet=67108864')
                    cursor.execute("select sys_users_id, user_code, att_designation, day_is, is_salary_enabled, bat_company_id, bat_dpid, route_number, hr_working_shifts_id, shift_day_status, shift_start_time, shift_end_time, hr_emp_sections_id, daily_status, in_time, out_time, break_time, total_work_time, ot_hours, record_mode, approved_status, is_edited, file_name, alter_user_code, created_at, created_by, updated_at, updated_by, attn_status from hr_emp_attendance where date(day_is) between '"+self.date+"' and '"+self.date+"'")

                    # cursor.execute("select mtime, dpid, route_id, skid, group, family, date, datetime, sale, dprice, rprice, dcc_price, issue, return, memoss, vmemos, tlp, cnc, vp, mvp, p, tcc, dcc, ecnc, gt, struc, semi_struc, streetk, mass_hrc, pop_hrc, prem_hrc, kaccounts, ogrocery, snb_cnc, pay_n_go, shop_n_browse, entertainment, outlets, apps_version, updated, visited from daily_sales_msr where month(date) = month("+month+") and year(date) = year("+month+")")

                    # cursor.execute(self.sql)
                    selected_columns = [d[0] for d in cursor.description]
                    # selected_columns = [column.replace('group','`group`') for column in selected_columns]
                    # selected_columns = [column.replace('return','`return`') for column in selected_columns]

                    self.log.info(cursor.description)
                    self.log.info(selected_columns)

                    if self.bulk_load:
                        with open (tmpfile.name, "wb") as f:
                        # with NamedTemporaryFile("wb") as tmpfile:
                            self.log.info("Selecting rows from MySql to local file %s...", tmpfile.name)
                            # self.log.info("select mtime, dpid, route_id, skid, `group`, family, date, sale, dprice, rprice, dcc_price, issue, `return`, memos, vmemos, tlp, cnc, vp, mvp, p, tcc, dcc, ecnc, gt, struc, semi_struc, streetk, mass_hrc, pop_hrc, prem_hrc, kaccounts, ogrocery, snb_cnc, pay_n_go, shop_n_browse, entertainment, outlets, apps_version, updated, visited from daily_sales_msr where date between '"+day+"' and '"+day+"'")

                            # self.log.info("select mtime, dpid, route_id, skid, group, family, date, datetime, sale, dprice, rprice, dcc_price, issue, return, memoss, vmemos, tlp, cnc, vp, mvp, p, tcc, dcc, ecnc, gt, struc, semi_struc, streetk, mass_hrc, pop_hrc, prem_hrc, kaccounts, ogrocery, snb_cnc, pay_n_go, shop_n_browse, entertainment, outlets, apps_version, updated, visited from daily_sales_msr where month(date) = month("+month+") and year(date) = year("+month+")")
                            # self.log.info(self.sql)

                            csv_writer = csv.writer(f, delimiter='\t', encoding='utf-8')
                            result = cursor.fetchall()
                            for row in result:
                                csv_writer.writerow(row)
                                count += 1

                            f.flush()
                            # tmpfile.flush()
                    else:
                        self.log.info("Selecting rows from MySQL source...")
                        # self.log.info("select mtime, dpid, route_id, skid, `group`, family, date, sale, dprice, rprice, dcc_price, issue, `return`, memos, vmemos, tlp, cnc, vp, mvp, p, tcc, dcc, ecnc, gt, struc, semi_struc, streetk, mass_hrc, pop_hrc, prem_hrc, kaccounts, ogrocery, snb_cnc, pay_n_go, shop_n_browse, entertainment, outlets, apps_version, updated, visited from daily_sales_msr where date between '"+day+"' and '"+day+"'")

                        # self.log.info("select mtime, dpid, route_id, skid, group, family, date, datetime, sale, dprice, rprice, dcc_price, issue, return, memoss, vmemos, tlp, cnc, vp, mvp, p, tcc, dcc, ecnc, gt, struc, semi_struc, streetk, mass_hrc, pop_hrc, prem_hrc, kaccounts, ogrocery, snb_cnc, pay_n_go, shop_n_browse, entertainment, outlets, apps_version, updated, visited from daily_sales_msr where month(date) = month("+month+") and year(date) = year("+month+")")
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
                                "LOAD DATA LOCAL INFILE '%s' IGNORE INTO "
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
                self.log.info(mysql_destination_conn_cursor._executed)
                self.log.info("Inserted rows into MySQL destinataion 0")
                raise

            if self.mysql_destination_postoperator:
                self.log.info("Running MySQL destination postoperator...")
                mysql_destination.run(self.mysql_destination_postoperator)

            self.log.info("Done")


    class HrEmployeeRecordLogsTableToHrEmployeeRecordLogsDimensionTableMySqlOperator(BaseOperator):
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
            date,
            # ending_date,
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
            self.date = date
            # self.ending_date = ending_date
            # self.end_month = end_month
            # self.start_month = start_month
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

            # month_list = []
            days_list = []

            # start_month = datetime.date(2020, 1, 1)
            # end_month = datetime.date(2021, 1, 1)

            # while self.start_month <= self.end_month:
                # month_list.append(datetime.datetime.strftime(self.start_month, '%Y-%m-01'))
                # result.append(current)
                # self.start_month += relativedelta(months=1)

            # start_date = date(2008, 8, 15)   # start date
            # end_date = date(2008, 9, 15)   # end date

            # delta = self.ending_date - self.starting_date       # as timedelta
            #
            # for i in range(delta.days + 1):
            #     day = self.starting_date + datetime.timedelta(days=i)
            #     print(day)
            #     days_list.append(datetime.datetime.strftime(day, '%Y-%m-%d'))

            # tmpfile = None
            tmpfile = tempfile.NamedTemporaryFile()

            result = None

            selected_columns = []

            count = 0
            with closing(mysql_source.get_conn()) as conn:
                with closing(conn.cursor()) as cursor:
                    # cursor.execute('set global max_allowed_packet=67108864')
                    cursor.execute("select hr_employee_record_logs_id, sys_users_id,record_type,designations_id, previous_designations_id, bat_company_id, bat_dpid, previous_dpid, applicable_date, hr_emp_grades_id, previous_grades_id, basic_salary, increment_type, increment_based_on, increment_amount, gross_salary, previous_gross, created_by, created_at, updated_at, updated_by, hr_transfer_status, hr_log_status, status, is_manual, approved_by from hr_employee_record_logs where date(created_at) between '"+self.date+"' and '"+self.date+"'")

                    # cursor.execute("select mtime, dpid, route_id, skid, group, family, date, datetime, sale, dprice, rprice, dcc_price, issue, return, memoss, vmemos, tlp, cnc, vp, mvp, p, tcc, dcc, ecnc, gt, struc, semi_struc, streetk, mass_hrc, pop_hrc, prem_hrc, kaccounts, ogrocery, snb_cnc, pay_n_go, shop_n_browse, entertainment, outlets, apps_version, updated, visited from daily_sales_msr where month(date) = month("+month+") and year(date) = year("+month+")")
                    self.log.info(self.date)
                    # cursor.execute(self.sql)
                    selected_columns = [d[0] for d in cursor.description]
                    # selected_columns = [column.replace('group','`group`') for column in selected_columns]
                    # selected_columns = [column.replace('return','`return`') for column in selected_columns]

                    self.log.info(cursor.description)
                    self.log.info(selected_columns)

                    if self.bulk_load:
                        with open (tmpfile.name, "wb") as f:
                        # with NamedTemporaryFile("wb") as tmpfile:
                            self.log.info("Selecting rows from MySql to local file %s...", tmpfile.name)
                            # self.log.info("select mtime, dpid, route_id, skid, `group`, family, date, sale, dprice, rprice, dcc_price, issue, `return`, memos, vmemos, tlp, cnc, vp, mvp, p, tcc, dcc, ecnc, gt, struc, semi_struc, streetk, mass_hrc, pop_hrc, prem_hrc, kaccounts, ogrocery, snb_cnc, pay_n_go, shop_n_browse, entertainment, outlets, apps_version, updated, visited from daily_sales_msr where date between '"+day+"' and '"+day+"'")

                            # self.log.info("select mtime, dpid, route_id, skid, group, family, date, datetime, sale, dprice, rprice, dcc_price, issue, return, memoss, vmemos, tlp, cnc, vp, mvp, p, tcc, dcc, ecnc, gt, struc, semi_struc, streetk, mass_hrc, pop_hrc, prem_hrc, kaccounts, ogrocery, snb_cnc, pay_n_go, shop_n_browse, entertainment, outlets, apps_version, updated, visited from daily_sales_msr where month(date) = month("+month+") and year(date) = year("+month+")")
                            # self.log.info(self.sql)

                            csv_writer = csv.writer(f, delimiter='\t', encoding='utf-8')
                            result = cursor.fetchall()
                            for row in result:
                                csv_writer.writerow(row)
                                count += 1

                            f.flush()
                            # tmpfile.flush()
                    else:
                        self.log.info("Selecting rows from MySQL source...")
                        # self.log.info("select mtime, dpid, route_id, skid, `group`, family, date, sale, dprice, rprice, dcc_price, issue, `return`, memos, vmemos, tlp, cnc, vp, mvp, p, tcc, dcc, ecnc, gt, struc, semi_struc, streetk, mass_hrc, pop_hrc, prem_hrc, kaccounts, ogrocery, snb_cnc, pay_n_go, shop_n_browse, entertainment, outlets, apps_version, updated, visited from daily_sales_msr where date between '"+day+"' and '"+day+"'")

                        # self.log.info("select mtime, dpid, route_id, skid, group, family, date, datetime, sale, dprice, rprice, dcc_price, issue, return, memoss, vmemos, tlp, cnc, vp, mvp, p, tcc, dcc, ecnc, gt, struc, semi_struc, streetk, mass_hrc, pop_hrc, prem_hrc, kaccounts, ogrocery, snb_cnc, pay_n_go, shop_n_browse, entertainment, outlets, apps_version, updated, visited from daily_sales_msr where month(date) = month("+month+") and year(date) = year("+month+")")
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
                                "LOAD DATA LOCAL INFILE '%s' IGNORE INTO "
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
                self.log.info(mysql_destination_conn_cursor._executed)
                self.log.info("Inserted rows into MySQL destinataion 0")
                raise

            if self.mysql_destination_postoperator:
                self.log.info("Running MySQL destination postoperator...")
                mysql_destination.run(self.mysql_destination_postoperator)

            self.log.info("Done")

    class DesignationTableToDesignationDimensionTableMySqlOperator(BaseOperator):
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
            date,
            # ending_date,
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
            self.date = date
            # self.ending_date = ending_date
            # self.end_month = end_month
            # self.start_month = start_month
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

            # month_list = []
            days_list = []

            # start_month = datetime.date(2020, 1, 1)
            # end_month = datetime.date(2021, 1, 1)

            # while self.start_month <= self.end_month:
                # month_list.append(datetime.datetime.strftime(self.start_month, '%Y-%m-01'))
                # result.append(current)
                # self.start_month += relativedelta(months=1)

            # start_date = date(2008, 8, 15)   # start date
            # end_date = date(2008, 9, 15)   # end date

            # delta = self.ending_date - self.starting_date       # as timedelta
            #
            # for i in range(delta.days + 1):
            #     day = self.starting_date + datetime.timedelta(days=i)
            #     print(day)
            #     days_list.append(datetime.datetime.strftime(day, '%Y-%m-%d'))

            # tmpfile = None
            tmpfile = tempfile.NamedTemporaryFile()

            result = None

            selected_columns = []

            count = 0
            with closing(mysql_source.get_conn()) as conn:
                with closing(conn.cursor()) as cursor:
                    # cursor.execute('set global max_allowed_packet=67108864')
                    cursor.execute("select * from designations where date(created_at) between '"+self.date+"' and '"+self.date+"'")
                    # cursor.execute("select mtime, dpid, route_id, skid, group, family, date, datetime, sale, dprice, rprice, dcc_price, issue, return, memoss, vmemos, tlp, cnc, vp, mvp, p, tcc, dcc, ecnc, gt, struc, semi_struc, streetk, mass_hrc, pop_hrc, prem_hrc, kaccounts, ogrocery, snb_cnc, pay_n_go, shop_n_browse, entertainment, outlets, apps_version, updated, visited from daily_sales_msr where month(date) = month("+month+") and year(date) = year("+month+")")
                    self.log.info(self.date)
                    # cursor.execute(self.sql)
                    selected_columns = [d[0] for d in cursor.description]
                    # selected_columns = [column.replace('group','`group`') for column in selected_columns]
                    # selected_columns = [column.replace('return','`return`') for column in selected_columns]

                    self.log.info(cursor.description)
                    self.log.info(selected_columns)

                    if self.bulk_load:
                        with open (tmpfile.name, "wb") as f:
                        # with NamedTemporaryFile("wb") as tmpfile:
                            self.log.info("Selecting rows from MySql to local file %s...", tmpfile.name)
                            # self.log.info("select mtime, dpid, route_id, skid, `group`, family, date, sale, dprice, rprice, dcc_price, issue, `return`, memos, vmemos, tlp, cnc, vp, mvp, p, tcc, dcc, ecnc, gt, struc, semi_struc, streetk, mass_hrc, pop_hrc, prem_hrc, kaccounts, ogrocery, snb_cnc, pay_n_go, shop_n_browse, entertainment, outlets, apps_version, updated, visited from daily_sales_msr where date between '"+day+"' and '"+day+"'")

                            # self.log.info("select mtime, dpid, route_id, skid, group, family, date, datetime, sale, dprice, rprice, dcc_price, issue, return, memoss, vmemos, tlp, cnc, vp, mvp, p, tcc, dcc, ecnc, gt, struc, semi_struc, streetk, mass_hrc, pop_hrc, prem_hrc, kaccounts, ogrocery, snb_cnc, pay_n_go, shop_n_browse, entertainment, outlets, apps_version, updated, visited from daily_sales_msr where month(date) = month("+month+") and year(date) = year("+month+")")
                            # self.log.info(self.sql)

                            csv_writer = csv.writer(f, delimiter='\t', encoding='utf-8')
                            result = cursor.fetchall()
                            for row in result:
                                csv_writer.writerow(row)
                                count += 1

                            f.flush()
                    else:
                        # tmpfile.flush()
                        self.log.info("Selecting rows from MySQL source...")
                        # self.log.info("select mtime, dpid, route_id, skid, `group`, family, date, sale, dprice, rprice, dcc_price, issue, `return`, memos, vmemos, tlp, cnc, vp, mvp, p, tcc, dcc, ecnc, gt, struc, semi_struc, streetk, mass_hrc, pop_hrc, prem_hrc, kaccounts, ogrocery, snb_cnc, pay_n_go, shop_n_browse, entertainment, outlets, apps_version, updated, visited from daily_sales_msr where date between '"+day+"' and '"+day+"'")

                        # self.log.info("select mtime, dpid, route_id, skid, group, family, date, datetime, sale, dprice, rprice, dcc_price, issue, return, memoss, vmemos, tlp, cnc, vp, mvp, p, tcc, dcc, ecnc, gt, struc, semi_struc, streetk, mass_hrc, pop_hrc, prem_hrc, kaccounts, ogrocery, snb_cnc, pay_n_go, shop_n_browse, entertainment, outlets, apps_version, updated, visited from daily_sales_msr where month(date) = month("+month+") and year(date) = year("+month+")")
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
                                "LOAD DATA LOCAL INFILE '%s' IGNORE INTO "
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
                self.log.info(mysql_destination_conn_cursor._executed)
                self.log.info("Inserted rows into MySQL destinataion 0")
                raise

            if self.mysql_destination_postoperator:
                self.log.info("Running MySQL destination postoperator...")
                mysql_destination.run(self.mysql_destination_postoperator)

            self.log.info("Done")

    class SubDesignationTableToSubDesignationDimensionTableMySqlOperator(BaseOperator):
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
            date,
            # ending_date,
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
            self.date = date
            # self.ending_date = ending_date
            # self.end_month = end_month
            # self.start_month = start_month
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

            # month_list = []
            days_list = []

            # start_month = datetime.date(2020, 1, 1)
            # end_month = datetime.date(2021, 1, 1)

            # while self.start_month <= self.end_month:
                # month_list.append(datetime.datetime.strftime(self.start_month, '%Y-%m-01'))
                # result.append(current)
                # self.start_month += relativedelta(months=1)

            # start_date = date(2008, 8, 15)   # start date
            # end_date = date(2008, 9, 15)   # end date

            # delta = self.ending_date - self.starting_date       # as timedelta
            #
            # for i in range(delta.days + 1):
            #     day = self.starting_date + datetime.timedelta(days=i)
            #     print(day)
            #     days_list.append(datetime.datetime.strftime(day, '%Y-%m-%d'))

            # tmpfile = None
            tmpfile = tempfile.NamedTemporaryFile()

            result = None

            selected_columns = []

            count = 0
            for day in days_list:
                with closing(mysql_source.get_conn()) as conn:
                    with closing(conn.cursor()) as cursor:
                        # cursor.execute('set global max_allowed_packet=67108864')
                        cursor.execute("select * from sub_designations where date(created_at) between '"+self.date+"' and '"+self.date+"'")
                        # cursor.execute("select mtime, dpid, route_id, skid, group, family, date, datetime, sale, dprice, rprice, dcc_price, issue, return, memoss, vmemos, tlp, cnc, vp, mvp, p, tcc, dcc, ecnc, gt, struc, semi_struc, streetk, mass_hrc, pop_hrc, prem_hrc, kaccounts, ogrocery, snb_cnc, pay_n_go, shop_n_browse, entertainment, outlets, apps_version, updated, visited from daily_sales_msr where month(date) = month("+month+") and year(date) = year("+month+")")
                        self.log.info(self.date)
                        # cursor.execute(self.sql)
                        selected_columns = [d[0] for d in cursor.description]
                        # selected_columns = [column.replace('group','`group`') for column in selected_columns]
                        # selected_columns = [column.replace('return','`return`') for column in selected_columns]

                        self.log.info(cursor.description)
                        self.log.info(selected_columns)

                        if self.bulk_load:
                            with open (tmpfile.name, "wb") as f:
                            # with NamedTemporaryFile("wb") as tmpfile:
                                self.log.info("Selecting rows from MySql to local file %s...", tmpfile.name)
                                # self.log.info("select mtime, dpid, route_id, skid, `group`, family, date, sale, dprice, rprice, dcc_price, issue, `return`, memos, vmemos, tlp, cnc, vp, mvp, p, tcc, dcc, ecnc, gt, struc, semi_struc, streetk, mass_hrc, pop_hrc, prem_hrc, kaccounts, ogrocery, snb_cnc, pay_n_go, shop_n_browse, entertainment, outlets, apps_version, updated, visited from daily_sales_msr where date between '"+day+"' and '"+day+"'")

                                # self.log.info("select mtime, dpid, route_id, skid, group, family, date, datetime, sale, dprice, rprice, dcc_price, issue, return, memoss, vmemos, tlp, cnc, vp, mvp, p, tcc, dcc, ecnc, gt, struc, semi_struc, streetk, mass_hrc, pop_hrc, prem_hrc, kaccounts, ogrocery, snb_cnc, pay_n_go, shop_n_browse, entertainment, outlets, apps_version, updated, visited from daily_sales_msr where month(date) = month("+month+") and year(date) = year("+month+")")
                                # self.log.info(self.sql)

                                csv_writer = csv.writer(f, delimiter='\t', encoding='utf-8')
                                result = cursor.fetchall()
                                for row in result:
                                    csv_writer.writerow(row)
                                    count += 1

                                f.flush()
                                # tmpfile.flush()
                        else:
                            self.log.info("Selecting rows from MySQL source...")
                            # self.log.info("select mtime, dpid, route_id, skid, `group`, family, date, sale, dprice, rprice, dcc_price, issue, `return`, memos, vmemos, tlp, cnc, vp, mvp, p, tcc, dcc, ecnc, gt, struc, semi_struc, streetk, mass_hrc, pop_hrc, prem_hrc, kaccounts, ogrocery, snb_cnc, pay_n_go, shop_n_browse, entertainment, outlets, apps_version, updated, visited from daily_sales_msr where date between '"+day+"' and '"+day+"'")

                            # self.log.info("select mtime, dpid, route_id, skid, group, family, date, datetime, sale, dprice, rprice, dcc_price, issue, return, memoss, vmemos, tlp, cnc, vp, mvp, p, tcc, dcc, ecnc, gt, struc, semi_struc, streetk, mass_hrc, pop_hrc, prem_hrc, kaccounts, ogrocery, snb_cnc, pay_n_go, shop_n_browse, entertainment, outlets, apps_version, updated, visited from daily_sales_msr where month(date) = month("+month+") and year(date) = year("+month+")")
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
                                    "LOAD DATA LOCAL INFILE '%s' IGNORE INTO "
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
                    self.log.info(mysql_destination_conn_cursor._executed)
                    self.log.info("Inserted rows into MySQL destinataion 0")
                    raise

                if self.mysql_destination_postoperator:
                    self.log.info("Running MySQL destination postoperator...")
                    mysql_destination.run(self.mysql_destination_postoperator)

                self.log.info("Done")

    class SysUsersTableToSysUsersFactTableMySqlOperator(BaseOperator):
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
            # date,
            # ending_date,
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
            # self.date = date
            # self.ending_date = ending_date
            # self.end_month = end_month
            # self.start_month = start_month
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

            # month_list = []
            days_list = []

            # start_month = datetime.date(2020, 1, 1)
            # end_month = datetime.date(2021, 1, 1)

            # while self.start_month <= self.end_month:
                # month_list.append(datetime.datetime.strftime(self.start_month, '%Y-%m-01'))
                # result.append(current)
                # self.start_month += relativedelta(months=1)

            # start_date = date(2008, 8, 15)   # start date
            # end_date = date(2008, 9, 15)   # end date

            # delta = self.ending_date - self.starting_date       # as timedelta

            # for i in range(delta.days + 1):
            #     day = self.starting_date + datetime.timedelta(days=i)
            #     print(day)
            #     days_list.append(datetime.datetime.strftime(day, '%Y-%m-%d'))

            # tmpfile = None
            tmpfile = tempfile.NamedTemporaryFile()

            result = None

            selected_columns = []

            count = 0
            with closing(mysql_source.get_conn()) as conn:
                with closing(conn.cursor()) as cursor:
                    # cursor.execute('set global max_allowed_packet=67108864')
                    cursor.execute("select user_code, username, email, is_employee, name, name_bangla, email_verified_at, mobile, date_of_birth, blood_group, gender, religion, marital_status, nationality, birth_certificate_no, nid, tin, user_sign, last_login, date_of_join, date_of_confirmation, is_reliever, reliever_to, reliever_start_datetime, reliever_end_datetime, bat_company_id, route_number, bat_dpid, privilege_houses, privilege_points, designations_id, departments_id, branchs_id, hr_emp_grades_id, hr_emp_units_id, hr_emp_categorys_id, hr_emp_sections_id, line_manager_id, hr_working_shifts_id, start_time, end_time, basic_salary, other_conveyance, pf_amount_employee, pf_amount_company, gf_amount, insurance_amount, min_gross, applicable_date, max_variable_salary, yearly_increment, ot_applicable, pf_applicable, gf_applicable, insurance_applicable, late_deduction_applied, default_salary_applied, salary_disburse_type, mfs_account_name_old, salary_account_no, reference_user_id, geo_location_7_id, created_by, created_at, updated_at, updated_by, is_roaster, status, working_type, separation_date,hr_separation_causes, leave_policy_apply, is_transfer, identity_type, identity_number, sub_designation, mfs_account_number, mfs_account_name from sys_users")

                    # cursor.execute("select mtime, dpid, route_id, skid, group, family, date, datetime, sale, dprice, rprice, dcc_price, issue, return, memoss, vmemos, tlp, cnc, vp, mvp, p, tcc, dcc, ecnc, gt, struc, semi_struc, streetk, mass_hrc, pop_hrc, prem_hrc, kaccounts, ogrocery, snb_cnc, pay_n_go, shop_n_browse, entertainment, outlets, apps_version, updated, visited from daily_sales_msr where month(date) = month("+month+") and year(date) = year("+month+")")

                    # cursor.execute(self.sql)
                    selected_columns = [d[0] for d in cursor.description]
                    # selected_columns = [column.replace('group','`group`') for column in selected_columns]
                    # selected_columns = [column.replace('return','`return`') for column in selected_columns]

                    self.log.info(cursor.description)
                    self.log.info(selected_columns)

                    if self.bulk_load:
                        with open (tmpfile.name, "wb") as f:
                        # with NamedTemporaryFile("wb") as tmpfile:
                            self.log.info("Selecting rows from MySql to local file %s...", tmpfile.name)
                            # self.log.info("select mtime, dpid, route_id, skid, `group`, family, date, sale, dprice, rprice, dcc_price, issue, `return`, memos, vmemos, tlp, cnc, vp, mvp, p, tcc, dcc, ecnc, gt, struc, semi_struc, streetk, mass_hrc, pop_hrc, prem_hrc, kaccounts, ogrocery, snb_cnc, pay_n_go, shop_n_browse, entertainment, outlets, apps_version, updated, visited from daily_sales_msr where date between '"+day+"' and '"+day+"'")

                            # self.log.info("select mtime, dpid, route_id, skid, group, family, date, datetime, sale, dprice, rprice, dcc_price, issue, return, memoss, vmemos, tlp, cnc, vp, mvp, p, tcc, dcc, ecnc, gt, struc, semi_struc, streetk, mass_hrc, pop_hrc, prem_hrc, kaccounts, ogrocery, snb_cnc, pay_n_go, shop_n_browse, entertainment, outlets, apps_version, updated, visited from daily_sales_msr where month(date) = month("+month+") and year(date) = year("+month+")")
                            # self.log.info(self.sql)

                            csv_writer = csv.writer(f, delimiter='\t', encoding='utf-8')
                            result = cursor.fetchall()
                            for row in result:
                                csv_writer.writerow(row)
                                count += 1

                            f.flush()
                            # tmpfile.flush()
                    else:
                        self.log.info("Selecting rows from MySQL source...")
                        # self.log.info("select mtime, dpid, route_id, skid, `group`, family, date, sale, dprice, rprice, dcc_price, issue, `return`, memos, vmemos, tlp, cnc, vp, mvp, p, tcc, dcc, ecnc, gt, struc, semi_struc, streetk, mass_hrc, pop_hrc, prem_hrc, kaccounts, ogrocery, snb_cnc, pay_n_go, shop_n_browse, entertainment, outlets, apps_version, updated, visited from daily_sales_msr where date between '"+day+"' and '"+day+"'")

                        # self.log.info("select mtime, dpid, route_id, skid, group, family, date, datetime, sale, dprice, rprice, dcc_price, issue, return, memoss, vmemos, tlp, cnc, vp, mvp, p, tcc, dcc, ecnc, gt, struc, semi_struc, streetk, mass_hrc, pop_hrc, prem_hrc, kaccounts, ogrocery, snb_cnc, pay_n_go, shop_n_browse, entertainment, outlets, apps_version, updated, visited from daily_sales_msr where month(date) = month("+month+") and year(date) = year("+month+")")
                        # self.log.info(self.sql)

                        result = cursor.fetchall()
                        count = len(result)

                    self.log.info("Selected rows from MySql source %s", count)

            if self.mysql_destination_preoperator:
                self.log.info("Running MySQL destination preoperator...")
                mysql_destination.run(self.mysql_destination_preoperator)

            try:
                if self.bulk_load:
                    self.log.info("Truncating rows from sys_user_fact...")
                    mysql_destination_conn_cursor.execute("truncate table `sys_users_fact`")
                    self.log.info("Bulk inserting rows into MySQL destination...")
                    with closing(mysql_destination.get_conn()) as conn:
                        with closing(conn.cursor()) as cursor:
                            cursor.execute(
                                "LOAD DATA LOCAL INFILE '%s' IGNORE INTO "
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
                self.log.info(mysql_destination_conn_cursor._executed)
                self.log.info("Inserted rows into MySQL destinataion 0")
                raise

            if self.mysql_destination_postoperator:
                self.log.info("Running MySQL destination postoperator...")
                mysql_destination.run(self.mysql_destination_postoperator)

            self.log.info("Done")

    start_operator = DummyOperator(task_id='Begin_execution')

    loading_hr_anomaly_dimension_table = HrAnomalyTableToHrAnomalyDimensionTableMySqlOperator(
                        task_id = 'loading_hr_anomaly_dimension_table',
                        date = yesterday_date,
                        mysql_source_conn_id = "hrm_bat",
                        mysql_destination_table = "hr_anomaly_dimension",
                        mysql_destination_conn_id = "aqm_dwh_column_store_database",
                        mysql_destination_preoperator = sql_statements_mariadb_column_store.CREATE_hr_anomaly_dimension_TABLE_IF_NOT_EXISTS,
                        bulk_load = False
    )

    hrm_emp_attendance_table_to_hrm_emp_attendance_fact_table = HrmEmpAttendanceTableToHrmEmpAtenndanceFactTableMySqlOperator(
                        task_id = 'hr_emp_attendance_table_to_hr_emp_attendance_fact_table',
                        date = yesterday_date,
                        mysql_source_conn_id = "hrm_bat",
                        mysql_destination_table = "hr_emp_attendance_fact",
                        mysql_destination_conn_id = "aqm_dwh_column_store_database",
                        mysql_destination_preoperator = sql_statements_mariadb_column_store.CREATE_hr_emp_attendance_fact_TABLE_IF_NOT_EXISTS,
                        bulk_load = True
    )

    loading_hr_employee_record_logs_dimension_table = HrEmployeeRecordLogsTableToHrEmployeeRecordLogsDimensionTableMySqlOperator(
                        task_id = 'loading_hr_employee_record_logs_dimension_table',
                        date = yesterday_date,
                        mysql_source_conn_id = "hrm_bat",
                        mysql_destination_table = "hr_employee_record_logs_dimension",
                        mysql_destination_conn_id = "aqm_dwh_column_store_database",
                        mysql_destination_preoperator = sql_statements_mariadb_column_store.CREATE_hr_employee_record_logs_dimension_TABLE_IF_NOT_EXISTS,
                        bulk_load = True
    )

    loading_designations_dimension_table = DesignationTableToDesignationDimensionTableMySqlOperator(
                        task_id = 'loading_designations_dimension_table',
                        date = yesterday_date,
                        mysql_source_conn_id = "hrm_bat",
                        mysql_destination_table = "designations_dimension",
                        mysql_destination_conn_id = "aqm_dwh_column_store_database",
                        mysql_destination_preoperator = sql_statements_mariadb_column_store.CREATE_designations_dimension_TABLE_IF_NOT_EXISTS,
                        bulk_load = True
    )

    loading_sub_designations_dimension_table = SubDesignationTableToSubDesignationDimensionTableMySqlOperator(
                        task_id = 'loading_sub_designations_dimension_table',
                        date = yesterday_date,
                        mysql_source_conn_id = "hrm_bat",
                        mysql_destination_table = "sub_designations_dimension",
                        mysql_destination_conn_id = "aqm_dwh_column_store_database",
                        mysql_destination_preoperator = sql_statements_mariadb_column_store.CREATE_sub_designations_dimension_TABLE_IF_NOT_EXISTS,
                        bulk_load = True
    )

    geo_distance_by_outlet_table_to_geo_distance_by_outlet_fact_table = GeoDistanceByOutletTableToGeoDistanceByOutletFactTableMySqlOperator(
                        task_id = 'geo_distance_by_outlet_table_to_geo_distance_by_outlet_fact_table',
                        date = yesterday_date,
                        mysql_source_conn_id = "geo_bat",
                        mysql_destination_table = "geo_distance_by_outlet_fact",
                        mysql_destination_conn_id = "aqm_dwh_column_store_database",
                        mysql_destination_preoperator = sql_statements_mariadb_column_store.CREATE_geo_distance_by_outlet_fact_TABLE_IF_NOT_EXISTS,
                        bulk_load = True
    )

    geo_distance_by_section_table_to_geo_distance_by_section_fact_table = GeoDistanceBySectionTableToGeoDistanceBySectionFactTableMySqlOperator(
                        task_id = 'geo_distance_by_section_table_to_geo_distance_by_section_fact_table',
                        date = yesterday_date,
                        mysql_source_conn_id = "geo_bat",
                        mysql_destination_table = "geo_distance_by_section_fact",
                        mysql_destination_conn_id = "aqm_dwh_column_store_database",
                        mysql_destination_preoperator = sql_statements_mariadb_column_store.CREATE_geo_distance_by_section_fact_TABLE_IF_NOT_EXISTS,
                        bulk_load = True
    )

    sys_users_table_to_sys_users_fact_table = SysUsersTableToSysUsersFactTableMySqlOperator(
                        task_id = 'sys_users_table_to_sys_users_fact_table',
                        mysql_source_conn_id = "hrm_bat",
                        mysql_destination_table = "sys_users_fact",
                        mysql_destination_conn_id = "aqm_dwh_column_store_database",
                        mysql_destination_preoperator = sql_statements_mariadb_column_store.CREATE_sys_users_fact_TABLE_IF_NOT_EXISTS,
                        bulk_load = True
    )


    # run_quality_checks = SQLCheckOperator(
    #                    task_id = 'Run_data_quality_checks',
    #                    sql = "select count(*) from sales_fact",
    #                    conn_id = "aqm_mysql_dw",
    # )

    # run_quality_checks_sales_fact = CheckOperator(
    #                    task_id = 'Run_data_quality_checks_sales_fact',
    #                    sql = "select count(*) from sales_fact",
    #                    conn_id = "pass_aqm_dwh_database",
    # )

    run_data_quality_checks_daily_sales_msr_fact = MySqlOperator(
                       task_id = 'run_data_quality_checks_daily_sales_msr_fact',
                       sql = "select count(*) from daily_sales_msr_fact",
                       mysql_conn_id = "aqm_dwh_column_store_database",
    )

    run_data_quality_checks_sales_by_sku_fact = MySqlOperator(
                       task_id = 'run_data_quality_checks_sales_by_sku_fact',
                       sql = "select count(*) from sales_by_sku_fact",
                       mysql_conn_id = "aqm_dwh_column_store_database",
    )

    run_data_quality_checks_sales_by_brand_fact = MySqlOperator(
                       task_id = 'run_data_quality_checks_sales_by_brand_fact',
                       sql = "select count(*) from sales_by_brand_fact",
                       mysql_conn_id = "aqm_dwh_column_store_database",
    )

    run_data_quality_checks_hr_emp_attendance_fact = MySqlOperator(
                       task_id = 'run_data_quality_checks_hr_emp_attendance_fact',
                       sql = "select count(*) from hr_emp_attendance_fact",
                       mysql_conn_id = "aqm_dwh_column_store_database",
    )

    run_data_quality_checks_geo_distance_by_outlet_fact = MySqlOperator(
                       task_id = 'run_data_quality_checks_geo_distance_by_outlet_fact',
                       sql = "select count(*) from geo_distance_by_outlet_fact",
                       mysql_conn_id = "aqm_dwh_column_store_database",
    )

    run_data_quality_checks_geo_distance_by_section_fact = MySqlOperator(
                       task_id = 'run_data_quality_checks_geo_distance_by_section_fact',
                       sql = "select count(*) from geo_distance_by_section_fact",
                       mysql_conn_id = "aqm_dwh_column_store_database",
    )


    # send_success_email = EmailOperator(
    #             task_id='send_email',
    #             to='shafiqul.islam@apsissolutions.com',
    #             subject='Daily ETL process for PRISM SALES AQM DWH',
    #             html_content=""" <h1>Congratulations! Daily ETL for PRISM AQM DWH process has been completed.</h1> """,
    # )

    send_success_email = BashOperator(
        task_id='send_email',
        bash_command = 'echo "Congratulations! ETL for PRISM AQM DWH (2nd process) has been completed." | mail -s "Daily ETL process for Mariadb Column store PRISM SALES AQM DWH" shafiqul.islam@apsissolutions.com'
    )

    end_operator = DummyOperator(task_id='Stop_execution')

    start_operator >>  loading_designations_dimension_table

    loading_designations_dimension_table >> loading_sub_designations_dimension_table
    loading_sub_designations_dimension_table >> loading_hr_anomaly_dimension_table
    loading_hr_anomaly_dimension_table >> hrm_emp_attendance_table_to_hrm_emp_attendance_fact_table
    # loading_sub_designations_dimension_table >>  hrm_emp_attendance_table_to_hrm_emp_attendance_fact_table
    hrm_emp_attendance_table_to_hrm_emp_attendance_fact_table >> loading_hr_employee_record_logs_dimension_table
    # loading_hr_employee_record_logs_dimension_table >> geo_distance_by_outlet_table_to_geo_distance_by_outlet_fact_table
    loading_hr_employee_record_logs_dimension_table >> sys_users_table_to_sys_users_fact_table
    sys_users_table_to_sys_users_fact_table >> geo_distance_by_outlet_table_to_geo_distance_by_outlet_fact_table

    geo_distance_by_outlet_table_to_geo_distance_by_outlet_fact_table >> geo_distance_by_section_table_to_geo_distance_by_section_fact_table
    geo_distance_by_section_table_to_geo_distance_by_section_fact_table >> run_data_quality_checks_daily_sales_msr_fact

    run_data_quality_checks_daily_sales_msr_fact >> run_data_quality_checks_sales_by_sku_fact
    run_data_quality_checks_sales_by_sku_fact >> run_data_quality_checks_sales_by_brand_fact
    run_data_quality_checks_sales_by_brand_fact >> run_data_quality_checks_geo_distance_by_outlet_fact
    run_data_quality_checks_geo_distance_by_outlet_fact >> run_data_quality_checks_geo_distance_by_section_fact
    run_data_quality_checks_geo_distance_by_section_fact >> run_data_quality_checks_hr_emp_attendance_fact
    # run_data_quality_checks_hr_emp_attendance_fact >> run_data_quality_checks_geo_distance_by_section_fact
    run_data_quality_checks_hr_emp_attendance_fact >> send_success_email

    send_success_email >> end_operator
