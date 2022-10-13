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

# from datacleaner import data_cleaner

yesterday_date = datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(1), '%Y-%m-%d')
# current_month = datetime.strftime(datetime.now(), '%Y-%m-%d')
# previous_month = datetime.strftime(datetime.now() - timedelta(30), '%Y-%m-%d')


default_args = {
    'owner': 'MD.Shafiqul Islam',
    'start_date': datetime.datetime(2021, 9, 20),
    'retries': 1,
    'retry_delay': datetime.timedelta(seconds=5)
}

with DAG(
    'prism_aqm_dwh_etl_daily_data_extractor_sales_pass_database',
    default_args=default_args,
    schedule_interval='0 1 * * *',
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

    class QueryManagerTableToSalesFactTableDailyDataEtxractorMySqlOperator(BaseOperator):
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
            date: str,
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
            with closing(mysql_source.get_conn()) as conn:
                with closing(conn.cursor()) as cursor:
                    # cursor.execute('set global max_allowed_packet=67108864')
                    cursor.execute("select mtime, rtslug, route_id, rtlid, skid, volume, value, date, outlets, visited, channel, ioq, sales_time from query_manager_table where date between '"+self.date+"' and '"+self.date+"'")
                    # self.log.info(cursor.execute())
                    # cursor.execute(self.sql)
                    # print(cursor.description)
                    print(self.date)
                    # print(type(cursor))
                    selected_columns = [d[0] for d in cursor.description]
                    selected_columns = [column.replace('group','`group`') for column in selected_columns]
                    self.log.info(cursor.description)
                    self.log.info(selected_columns)

                    if self.bulk_load:
                        with NamedTemporaryFile("wb") as tmpfile:
                            self.log.info("Selecting rows from MySql to local file %s...", tmpfile.name)
                            self.log.info("select mtime, rtslug, route_id, rtlid, skid, volume, value, date, outlets, visited, channel, ioq, sales_time from query_manager_table where date between '"+self.date+"' and '"+self.date+"'")

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
                        self.log.info("select mtime, rtslug, route_id, rtlid, skid, volume, value, date, outlets, visited, channel, ioq, sales_time from query_manager_table where date between '"+self.date+"' and '"+self.date+"'")

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

    class DailySalesMsrTableToDailySalesMsrFactTableDailyDataEtxractorMySqlOperator(BaseOperator):
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
            date: str,
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
            with closing(mysql_source.get_conn()) as conn:
                with closing(conn.cursor()) as cursor:
                    # cursor.execute('set global max_allowed_packet=67108864')
                    cursor.execute("select mtime, dpid, route_id, skid, prid, `group`, family, date, sale, dprice, rprice, dcc_price, issue, `return`, memos, vmemos, tlp, cnc, vp, mvp, p, tcc, dcc, ecnc, gt, struc, semi_struc, streetk, mass_hrc, pop_hrc, prem_hrc, kaccounts, ogrocery, snb_cnc, pay_n_go, shop_n_browse, entertainment, outlets, apps_version, updated, visited from daily_sales_msr where date(date) between '"+self.date+"' and '"+self.date+"'")
                    print(self.date)
                    # cursor.execute("select mtime, dpid, route_id, skid, group, family, date, datetime, sale, dprice, rprice, dcc_price, issue, return, memoss, vmemos, tlp, cnc, vp, mvp, p, tcc, dcc, ecnc, gt, struc, semi_struc, streetk, mass_hrc, pop_hrc, prem_hrc, kaccounts, ogrocery, snb_cnc, pay_n_go, shop_n_browse, entertainment, outlets, apps_version, updated, visited from daily_sales_msr where month(date) = month("+month+") and year(date) = year("+month+")")
                    # cursor.execute(self.sql)
                    selected_columns = [d[0] for d in cursor.description]
                    selected_columns = [column.replace('group','`group`') for column in selected_columns]
                    selected_columns = [column.replace('return','`return`') for column in selected_columns]

                    self.log.info(cursor.description)
                    self.log.info(selected_columns)

                    if self.bulk_load:
                        with NamedTemporaryFile("wb") as tmpfile:
                            self.log.info("Selecting rows from MySql to local file %s...", tmpfile.name)
                            self.log.info("select mtime, dpid, route_id, skid, `group`, family, date, sale, dprice, rprice, dcc_price, issue, `return`, memos, vmemos, tlp, cnc, vp, mvp, p, tcc, dcc, ecnc, gt, struc, semi_struc, streetk, mass_hrc, pop_hrc, prem_hrc, kaccounts, ogrocery, snb_cnc, pay_n_go, shop_n_browse, entertainment, outlets, apps_version, updated, visited from daily_sales_msr where date between '"+self.date+"' and '"+self.date+"'")

                            # self.log.info("select mtime, dpid, route_id, skid, group, family, date, datetime, sale, dprice, rprice, dcc_price, issue, return, memoss, vmemos, tlp, cnc, vp, mvp, p, tcc, dcc, ecnc, gt, struc, semi_struc, streetk, mass_hrc, pop_hrc, prem_hrc, kaccounts, ogrocery, snb_cnc, pay_n_go, shop_n_browse, entertainment, outlets, apps_version, updated, visited from daily_sales_msr where month(date) = month("+month+") and year(date) = year("+month+")")
                            # self.log.info(self.sql)

                            csv_writer = csv.writer(tmpfile, delimiter='\t', encoding='utf-8')
                            result = cursor.fetchall()
                            for row in result:
                                csv_writer.writerow(row)
                                count += 1

                            tmpfile.flush()
                    else:
                        self.log.info("Selecting rows from MySQL source...")
                        self.log.info("select mtime, dpid, route_id, skid, `group`, family, date, sale, dprice, rprice, dcc_price, issue, `return`, memos, vmemos, tlp, cnc, vp, mvp, p, tcc, dcc, ecnc, gt, struc, semi_struc, streetk, mass_hrc, pop_hrc, prem_hrc, kaccounts, ogrocery, snb_cnc, pay_n_go, shop_n_browse, entertainment, outlets, apps_version, updated, visited from daily_sales_msr where date between '"+self.date+"' and '"+self.date+"'")

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
                self.log.info(mysql_destination_conn_cursor._executed)
                self.log.info("Inserted rows into MySQL destinataion 0")
                raise

            if self.mysql_destination_postoperator:
                self.log.info("Running MySQL destination postoperator...")
                mysql_destination.run(self.mysql_destination_postoperator)

            self.log.info("Done")

    class QueryManagerTableToSalesBySkuFactTableDailyDataEtxractorMySqlOperator(BaseOperator):
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
            date: str,
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

            tmpfile = None
            result = None

            selected_columns = []

            count = 0
            with closing(mysql_source.get_conn()) as conn:
                with closing(conn.cursor()) as cursor:
                    # cursor.execute('set global max_allowed_packet=67108864')
                    cursor.execute("select dpid, route_id, rtlid, retailer_code, skid, sum(volume) as volume, round(sum(volume*price_log.rprice))`value`, date from query_manager_table inner join price_log on price_log.sku_id = query_manager_table.skid and query_manager_table.date between start_date and end_date where date between '"+self.date+"' and '"+self.date+"' and dpid not in (334,344) group by dpid, route_id, rtlid, retailer_code, skid, date")
                    # self.log.info(cursor.execute())
                    # cursor.execute(self.sql)
                    # print(cursor.description)
                    print(self.date)
                    # print(type(cursor))
                    selected_columns = [d[0] for d in cursor.description]
                    selected_columns = [column.replace('group','`group`') for column in selected_columns]
                    self.log.info(cursor.description)
                    self.log.info(selected_columns)

                    if self.bulk_load:
                        with NamedTemporaryFile("wb") as tmpfile:
                            self.log.info("Selecting rows from MySql to local file %s...", tmpfile.name)
                            self.log.info("select mtime, rtslug, route_id, rtlid, skid, volume, value, date, outlets, visited, channel, ioq, sales_time from query_manager_table where date between '"+self.date+"' and '"+self.date+"'")

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
                        self.log.info("select mtime, rtslug, route_id, rtlid, skid, volume, value, date, outlets, visited, channel, ioq, sales_time from query_manager_table where date between '"+self.date+"' and '"+self.date+"'")

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
                    # self.log.info("Deleting rows from MySQL destination...")
                    # mysql_destination_conn_cursor.execute("delete from sales_fact where date between '"+self.date+"' and '"+self.date+"'")
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

    class SalesBySkuFactTableToSalesByBrandFactTableDailyDataEtxractorMySqlOperator(BaseOperator):
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

            tmpfile = None
            result = None

            selected_columns = []

            count = 0
            with closing(mysql_source.get_conn()) as conn:
                with closing(conn.cursor()) as cursor:
                    # cursor.execute('set global max_allowed_packet=67108864')
                    cursor.execute("select dpid, route_id, rtlid, retailer_code, products_dimension.id as prid, products_dimension.`group` as `group`, products_dimension.`family` as family, round(sum(volume),3)volume, round(sum(`value`),3)`value`, date from `sales_by_sku_fact` inner join sku_dimension on sku_dimension.id = sales_by_sku_fact.skid inner join products_dimension on sku_dimension.prid = products_dimension.id where date between '"+self.date+"' and '"+self.date+"' group by dpid, route_id, rtlid, retailer_code, products_dimension.id, products_dimension.`group`, products_dimension.`family`, date")
                    # cursor.execute("select mtime, dpid, route_id, skid, group, family, date, datetime, sale, dprice, rprice, dcc_price, issue, return, memoss, vmemos, tlp, cnc, vp, mvp, p, tcc, dcc, ecnc, gt, struc, semi_struc, streetk, mass_hrc, pop_hrc, prem_hrc, kaccounts, ogrocery, snb_cnc, pay_n_go, shop_n_browse, entertainment, outlets, apps_version, updated, visited from daily_sales_msr where month(date) = month("+month+") and year(date) = year("+month+")")
                    print(self.date)
                    # cursor.execute(self.sql)
                    selected_columns = [d[0] for d in cursor.description]
                    selected_columns = [column.replace('group','`group`') for column in selected_columns]
                    # selected_columns = [column.replace('return','`return`') for column in selected_columns]

                    self.log.info(cursor.description)
                    self.log.info(selected_columns)

                    if self.bulk_load:
                        with NamedTemporaryFile("wb") as tmpfile:
                            self.log.info("Selecting rows from MySql to local file %s...", tmpfile.name)
                            # self.log.info("select mtime, dpid, route_id, skid, `group`, family, date, sale, dprice, rprice, dcc_price, issue, `return`, memos, vmemos, tlp, cnc, vp, mvp, p, tcc, dcc, ecnc, gt, struc, semi_struc, streetk, mass_hrc, pop_hrc, prem_hrc, kaccounts, ogrocery, snb_cnc, pay_n_go, shop_n_browse, entertainment, outlets, apps_version, updated, visited from daily_sales_msr where date between '"+day+"' and '"+day+"'")

                            # self.log.info("select mtime, dpid, route_id, skid, group, family, date, datetime, sale, dprice, rprice, dcc_price, issue, return, memoss, vmemos, tlp, cnc, vp, mvp, p, tcc, dcc, ecnc, gt, struc, semi_struc, streetk, mass_hrc, pop_hrc, prem_hrc, kaccounts, ogrocery, snb_cnc, pay_n_go, shop_n_browse, entertainment, outlets, apps_version, updated, visited from daily_sales_msr where month(date) = month("+month+") and year(date) = year("+month+")")
                            # self.log.info(self.sql)

                            csv_writer = csv.writer(tmpfile, delimiter='\t', encoding='utf-8')
                            result = cursor.fetchall()
                            for row in result:
                                csv_writer.writerow(row)
                                count += 1

                            tmpfile.flush()
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
                self.log.info(mysql_destination_conn_cursor._executed)
                self.log.info("Inserted rows into MySQL destinataion 0")
                raise

            if self.mysql_destination_postoperator:
                self.log.info("Running MySQL destination postoperator...")
                mysql_destination.run(self.mysql_destination_postoperator)

            self.log.info("Done")

    # class DailySalesMsrStagingTableToDailySalesMsrFactTableOperator(BaseOperator):
    #     """
    #     Moves data from MySQL to MySQL.
    #
    #     :param sql: SQL query to execute against the MySQL source database. (templated)
    #     :type sql: str
    #     :param mysql_source_conn_id: source MySQL source connection
    #     :type mysql_source_conn_id: str
    #     :param mysql_destination_table: target MySQL table, use dot notation to target a
    #         specific database. (templated)
    #     :type mysql_destination_table: str
    #     :param mysql_destination_conn_id: Reference to :ref:`mysql connection id <howto/connection:mysql>`.
    #     :type mysql_destination_conn_id: str
    #     :param mysql_destination_preoperator: sql statement to run against MySQL destination prior to
    #         import, typically use to truncate of delete in place of the data
    #         coming in, allowing the task to be idempotent (running the task
    #         twice won't double load data). (templated)
    #     :type mysql_destination_preoperator: str
    #     :param mysql_destination_postoperator: sql statement to run against MySQL destination after the
    #         import, typically used to move data from staging to production
    #         and issue cleanup commands. (templated)
    #     :type mysql_destination_postoperator: str
    #     :param bulk_load: flag to use bulk_load option.  This loads MySQL destination directly
    #         from a tab-delimited text file using the LOAD DATA LOCAL INFILE command.
    #         This option requires an extra connection parameter for the
    #         destination MySQL connection: {'local_infile': true}.
    #     :type bulk_load: bool
    #     """
    #
    #     # template_fields = ('sql', 'mysql_source_table', 'mysql_destination_preoperator', 'mysql_destination_postoperator')
    #
    #     # template_ext = ('.sql',)
    #
    #     # template_fields_renderers = {
    #     #     "mysql_destination_preoperator": "sql",
    #     #     "mysql_destination_postoperator": "sql",
    #     #
    #     # }
    #     ui_color = '#a0e08c'
    #
    #
    #     @apply_defaults
    #     def __init__(
    #         self,
    #         # sql: str,
    #         mysql_destination_table: str,
    #         mysql_source_conn_id: str = 'mysql_source_default',
    #         mysql_destination_conn_id: str = 'mysql_destination_default',
    #         mysql_destination_preoperator: Optional[str] = None,
    #         mysql_destination_postoperator: Optional[str] = None,
    #         bulk_load: bool = False,
    #         *args,
    #         **kwargs,
    #     ) -> None:
    #         super().__init__(*args, **kwargs)
    #         # self.sql = sql
    #         self.mysql_destination_table = mysql_destination_table
    #         self.mysql_destiantion_conn_id = mysql_destination_conn_id
    #         self.mysql_destination_preoperator = mysql_destination_preoperator
    #         self.mysql_destination_postoperator = mysql_destination_postoperator
    #         self.mysql_source_conn_id = mysql_source_conn_id
    #         self.bulk_load = bulk_load
    #
    #     def execute(self, context):
    #         mysql_source = MySqlHook(mysql_conn_id=self.mysql_source_conn_id)
    #         mysql_destination = MySqlHook(mysql_conn_id=self.mysql_destination_conn_id)
    #         mysql_destination_conn = mysql_desintation.get_conn()
    #         mysql_destination_cursor = mysql_destination_conn.cursor()
    #         tmpfile = None
    #         result = None
    #
    #         selected_columns = []
    #
    #         count = 0
    #         with closing(mysql_source.get_conn()) as conn:
    #             with closing(conn.cursor()) as cursor:
    #                 mysql_destination_cursor.execute("SELECT MAX(id) FROM daily_sales_msr_fact;")
    #                 daily_sales_msr_fact_id = mysql_destination_cursor.fectchone()[0]
    #                 cursor.execute("SELECT * FROM staging_daily_sales_msr WHERE id > %s", [daily_sales_msr_fact_id])
    #                 selected_columns = [d.name for d in cursor.description]
    #                 if daily_sales_msr_fact_id is None:
    #                     daily_sales_msr_fact_id = 0
    #
    #                 if self.bulk_load:
    #                     with NamedTemporaryFile("w") as tmpfile:
    #                         self.log.info("Selecting rows from MySql to local file %s...", tmpfile.name)
    #                         self.log.info("SELECT * FROM staging_daily_sales_msr WHERE id > %s", [daily_sales_msr_fact_id])
    #
    #                         csv_writer = csv.writer(tmpfile, delimiter='\t', encoding='utf-8')
    #                         for row in cursor.iterate():
    #                             csv_writer.writerow(row)
    #                             count += 1
    #
    #                         tmpfile.flush()
    #                 else:
    #                     self.log.info("Selecting rows from MySQL source...")
    #                     self.log.info("SELECT * FROM staging_daily_sales_msr WHERE id > %s", [daily_sales_msr_fact_id])
    #
    #                     result = cursor.fetchall()
    #                     count = len(result)
    #
    #                 self.log.info("Selected rows from MySql source %s", count)
    #
    #         if self.mysql_destination_preoperator:
    #             self.log.info("Running MySQL destination preoperator...")
    #             mysql_destination.run(self.mysql_destination_preoperator)
    #
    #         try:
    #             if self.bulk_load:
    #                 self.log.info("Bulk inserting rows into MySQL destination...")
    #                 with closing(mysql_destination.get_conn()) as conn:
    #                     with closing(conn.cursor()) as cursor:
    #                         cursor.execute(
    #                             "LOAD DATA LOCAL INFILE '%s' INTO "
    #                             "TABLE %s LINES TERMINATED BY '\r\n' (%s)" %
    #                             (tmpfile.name,
    #                             self.mysql_destination_table,
    #                             ", ".join(selected_columns))
    #                         )
    #                         conn.commit()
    #                 tmpfile.close()
    #             else:
    #                 self.log.info("Inserting rows into MySQL destination...")
    #                 mysql_destination.insert_rows(table=self.mysql_destination_table, rows=result, target_fields=selected_columns)
    #             self.log.info("Inserted rows into MySQL destination %s", count)
    #         except (MySQLdb.Error, MySQLdb.Warning):  # pylint: disable=no-member
    #             self.log.info("Inserted rows into MySQL destinataion 0")
    #             raise
    #
    #         if self.mysql_destination_postoperator:
    #             self.log.info("Running MySQL destination postoperator...")
    #             mysql_destination.run(self.mysql_destination_postoperator)
    #
    #         self.log.info("Done")
    #
    # class QueryManagerTableStagingTableToSalesFactTableOperator(BaseOperator):
    #     """
    #     Moves data from MySQL to MySQL.
    #
    #     :param sql: SQL query to execute against the MySQL source database. (templated)
    #     :type sql: str
    #     :param mysql_source_conn_id: source MySQL source connection
    #     :type mysql_source_conn_id: str
    #     :param mysql_destination_table: target MySQL table, use dot notation to target a
    #         specific database. (templated)
    #     :type mysql_destination_table: str
    #     :param mysql_destination_conn_id: Reference to :ref:`mysql connection id <howto/connection:mysql>`.
    #     :type mysql_destination_conn_id: str
    #     :param mysql_destination_preoperator: sql statement to run against MySQL destination prior to
    #         import, typically use to truncate of delete in place of the data
    #         coming in, allowing the task to be idempotent (running the task
    #         twice won't double load data). (templated)
    #     :type mysql_destination_preoperator: str
    #     :param mysql_destination_postoperator: sql statement to run against MySQL destination after the
    #         import, typically used to move data from staging to production
    #         and issue cleanup commands. (templated)
    #     :type mysql_destination_postoperator: str
    #     :param bulk_load: flag to use bulk_load option.  This loads MySQL destination directly
    #         from a tab-delimited text file using the LOAD DATA LOCAL INFILE command.
    #         This option requires an extra connection parameter for the
    #         destination MySQL connection: {'local_infile': true}.
    #     :type bulk_load: bool
    #     """
    #
    #     # template_fields = ('sql', 'mysql_source_table', 'mysql_destination_preoperator', 'mysql_destination_postoperator')
    #
    #     # template_ext = ('.sql',)
    #
    #     # template_fields_renderers = {
    #     #     "mysql_destination_preoperator": "sql",
    #     #     "mysql_destination_postoperator": "sql",
    #     #
    #     # }
    #     ui_color = '#a0e08c'
    #
    #
    #     @apply_defaults
    #     def __init__(
    #         self,
    #         # sql: str,
    #         mysql_destination_table: str,
    #         mysql_source_conn_id: str = 'mysql_source_default',
    #         mysql_destination_conn_id: str = 'mysql_destination_default',
    #         mysql_destination_preoperator: Optional[str] = None,
    #         mysql_destination_postoperator: Optional[str] = None,
    #         bulk_load: bool = False,
    #         *args,
    #         **kwargs,
    #     ) -> None:
    #         super().__init__(*args, **kwargs)
    #         # self.sql = sql
    #         self.mysql_destination_table = mysql_destination_table
    #         self.mysql_destiantion_conn_id = mysql_destination_conn_id
    #         self.mysql_destination_preoperator = mysql_destination_preoperator
    #         self.mysql_destination_postoperator = mysql_destination_postoperator
    #         self.mysql_source_conn_id = mysql_source_conn_id
    #         self.bulk_load = bulk_load
    #
    #     def execute(self, context):
    #         mysql_source = MySqlHook(mysql_conn_id=self.mysql_source_conn_id)
    #         mysql_destination = MySqlHook(mysql_conn_id=self.mysql_destination_conn_id)
    #         mysql_destination_conn = mysql_desintation.get_conn()
    #         mysql_destination_cursor = mysql_destination_conn.cursor()
    #         tmpfile = None
    #         result = None
    #
    #         selected_columns = []
    #
    #         count = 0
    #         with closing(mysql_source.get_conn()) as conn:
    #             with closing(conn.cursor()) as cursor:
    #                 mysql_destination_cursor.execute("SELECT MAX(id) FROM sales_fact;")
    #                 sales_fact_id = mysql_destination_cursor.fectchone()[0]
    #                 cursor.execute("SELECT * FROM staging_query_manager_table WHERE id > %s", [sales_fact_id])
    #                 selected_columns = [d.name for d in cursor.description]
    #                 if sales_fact_id is None:
    #                     sales_fact_id = 0
    #
    #                 if self.bulk_load:
    #                     with NamedTemporaryFile("w") as tmpfile:
    #                         self.log.info("Selecting rows from MySql to local file %s...", tmpfile.name)
    #                         self.log.info("SELECT * FROM staging_query_manager_table WHERE id > %s", [sales_fact_id])
    #
    #                         csv_writer = csv.writer(tmpfile, delimiter='\t', encoding='utf-8')
    #                         for row in cursor.iterate():
    #                             csv_writer.writerow(row)
    #                             count += 1
    #
    #                         tmpfile.flush()
    #                 else:
    #                     self.log.info("Selecting rows from MySQL source...")
    #                     self.log.info("SELECT * FROM staging_query_manager_table WHERE id > %s", [sales_fact_id])
    #
    #                     result = cursor.fetchall()
    #                     count = len(result)
    #
    #                 self.log.info("Selected rows from MySql source %s", count)
    #
    #         if self.mysql_destination_preoperator:
    #             self.log.info("Running MySQL destination preoperator...")
    #             mysql_destination.run(self.mysql_destination_preoperator)
    #
    #         try:
    #             if self.bulk_load:
    #                 self.log.info("Bulk inserting rows into MySQL destination...")
    #                 with closing(mysql_destination.get_conn()) as conn:
    #                     with closing(conn.cursor()) as cursor:
    #                         cursor.execute(
    #                             "LOAD DATA LOCAL INFILE '%s' INTO "
    #                             "TABLE %s LINES TERMINATED BY '\r\n' (%s)" %
    #                             (tmpfile.name,
    #                             self.mysql_destination_table,
    #                             ", ".join(selected_columns))
    #                         )
    #                         conn.commit()
    #                 tmpfile.close()
    #             else:
    #                 self.log.info("Inserting rows into MySQL destination...")
    #                 mysql_destination.insert_rows(table=self.mysql_destination_table, rows=result, target_fields=selected_columns)
    #             self.log.info("Inserted rows into MySQL destination %s", count)
    #         except (MySQLdb.Error, MySQLdb.Warning):  # pylint: disable=no-member
    #             self.log.info("Inserted rows into MySQL destinataion 0")
    #             raise
    #
    #         if self.mysql_destination_postoperator:
    #             self.log.info("Running MySQL destination postoperator...")
    #             mysql_destination.run(self.mysql_destination_postoperator)
    #
    #         self.log.info("Done")
    #
    start_operator = DummyOperator(task_id='Begin_execution')

    # query_manager_table_to_sales_fact_table = QueryManagerTableToSalesFactTableDailyDataEtxractorMySqlOperator(
    #                     task_id = 'query_manager_table_to_sales_fact_table',
    #                     date = yesterday_date,
    #                     mysql_source_conn_id = "prism_live_114",
    #                     mysql_destination_table = "sales_fact",
    #                     mysql_destination_conn_id = "pass_aqm_dwh_database",
    #                     mysql_destination_preoperator = sql_statements.CREATE_sales_fact_TABLE_IF_NOT_EXISTS,
    #                     bulk_load = False
    # )

    daily_sales_msr_table_to_daily_sales_msr_fact_table = DailySalesMsrTableToDailySalesMsrFactTableDailyDataEtxractorMySqlOperator(
                        task_id = 'daily_sales_msr_table_to_daily_sales_msr_fact_table',
                        date = yesterday_date,
                        mysql_source_conn_id = "prism_live_114",
                        mysql_destination_table = "daily_sales_msr_fact",
                        mysql_destination_conn_id = "pass_aqm_dwh_database",
                        mysql_destination_preoperator = sql_statements.CREATE_daily_sales_msr_fact_TABLE_IF_NOT_EXISTS,
                        bulk_load = False
    )

    query_manager_table_to_sales_by_sku_fact_table = QueryManagerTableToSalesBySkuFactTableDailyDataEtxractorMySqlOperator(
                        task_id = 'query_manager_table_to_sales_by_sku_fact_table',
                        date = yesterday_date,
                        mysql_source_conn_id = "prism_live_114",
                        mysql_destination_table = "sales_by_sku_fact",
                        mysql_destination_conn_id = "pass_aqm_dwh_database",
                        mysql_destination_preoperator = sql_statements.CREATE_sales_by_sku_fact_TABLE_IF_NOT_EXISTS,
                        bulk_load = False
    )

    sales_by_sku_fact_table_to_sales_by_brand_fact_table = SalesBySkuFactTableToSalesByBrandFactTableDailyDataEtxractorMySqlOperator(
                        task_id = 'sales_by_sku_fact_table_to_sales_by_brand_fact_table',
                        date = yesterday_date,
                        mysql_source_conn_id = "pass_aqm_dwh_database",
                        mysql_destination_table = "sales_by_brand_fact",
                        mysql_destination_conn_id = "pass_aqm_dwh_database",
                        mysql_destination_preoperator = sql_statements.CREATE_sales_by_brand_fact_TABLE_IF_NOT_EXISTS,
                        bulk_load = False
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

    run_quality_checks_daily_sales_msr_fact = CheckOperator(
                       task_id = 'Run_data_quality_checks_daily_sales_msr_fact',
                       sql = "select count(*) from daily_sales_msr_fact",
                       conn_id = "pass_aqm_dwh_database",
    )

    run_quality_checks_sales_by_sku_fact = CheckOperator(
                       task_id = 'Run_data_quality_checks_sales_by_sku_fact',
                       sql = "select count(*) from sales_by_sku_fact",
                       conn_id = "pass_aqm_dwh_database",
    )

    run_quality_checks_sales_by_brand_fact = CheckOperator(
                       task_id = 'Run_data_quality_checks_sales_by_brand_fact',
                       sql = "select count(*) from sales_by_brand_fact",
                       conn_id = "pass_aqm_dwh_database",
    )

    # send_success_email = EmailOperator(
    #             task_id='send_email',
    #             to='shafiqul.islam@apsissolutions.com',
    #             subject='Daily ETL process for PRISM SALES AQM DWH',
    #             html_content=""" <h1>Congratulations! Daily ETL for PRISM AQM DWH process has been completed.</h1> """,
    # )

    send_success_email = BashOperator(
        task_id='send_email',
        bash_command = 'echo "Congratulations! ETL for PRISM SALES AQM DWH process has been completed." | mail -s "Daily ETL process for PRISM SALES AQM DWH" shafiqul.islam@apsissolutions.com'
    )

    end_operator = DummyOperator(task_id='Stop_execution')

    start_operator >> daily_sales_msr_table_to_daily_sales_msr_fact_table

    daily_sales_msr_table_to_daily_sales_msr_fact_table >> query_manager_table_to_sales_by_sku_fact_table

    query_manager_table_to_sales_by_sku_fact_table >> sales_by_sku_fact_table_to_sales_by_brand_fact_table

    sales_by_sku_fact_table_to_sales_by_brand_fact_table >> run_quality_checks_daily_sales_msr_fact

    run_quality_checks_daily_sales_msr_fact >> run_quality_checks_sales_by_sku_fact

    run_quality_checks_sales_by_sku_fact >> run_quality_checks_sales_by_brand_fact

    run_quality_checks_sales_by_brand_fact >> send_success_email

    send_success_email >> end_operator
