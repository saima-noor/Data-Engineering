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

# yesterday_date = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')
# current_month = datetime.strftime(datetime.now(), '%Y-%m-%d')
# previous_month = datetime.strftime(datetime.now() - timedelta(30), '%Y-%m-%d')


default_args = {
    'owner': 'MD.Shafiqul Islam',
    "start_date": datetime.datetime(2021, 6, 11),
    'retries': 1,
    'retry_delay': datetime.timedelta(seconds=5)
}

with DAG(
    'prism_aqm_dwh_etl',
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

    class QueryManagerTableToSalesFactTableMonthlyDataEtxractorMySqlOperator(BaseOperator):
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
            # month_list = []

            # start_month = datetime.date(2020, 1, 1)
            # end_month = datetime.date(2021, 1, 1)

            # while self.start_month <= self.end_month:
            #     month_list.append(datetime.datetime.strftime(self.start_month, '%Y-%m-01'))
                # result.append(current)
                # self.start_month += relativedelta(months=1)

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
                        cursor.execute("select mtime, rtslug, dpid, route_id, rtlid, retailer_code, skid, volume, value, date, outlets, visited, channel, ioq, sales_time from query_manager_table where date between '"+day+"' and '"+day+"'")
                        # self.log.info(cursor.execute())
                        # cursor.execute(self.sql)
                        # print(cursor.description)
                        print(day)
                        # print(type(cursor))
                        selected_columns = [d[0] for d in cursor.description]
                        selected_columns = [column.replace('group','`group`') for column in selected_columns]
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

    class DailySalesMsrTableToDailySalesMsrFactTableMonthlyDataEtxractorMySqlOperator(BaseOperator):
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
                        cursor.execute("select mtime, dpid, route_id, skid, `group`, family, date, sale, dprice, rprice, dcc_price, issue, `return`, memos, vmemos, tlp, cnc, vp, mvp, p, tcc, dcc, ecnc, gt, struc, semi_struc, streetk, mass_hrc, pop_hrc, prem_hrc, kaccounts, ogrocery, snb_cnc, pay_n_go, shop_n_browse, entertainment, outlets, apps_version, updated, visited from daily_sales_msr where date between '"+day+"' and '"+day+"'")
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
                                self.log.info("select mtime, dpid, route_id, skid, `group`, family, date, sale, dprice, rprice, dcc_price, issue, `return`, memos, vmemos, tlp, cnc, vp, mvp, p, tcc, dcc, ecnc, gt, struc, semi_struc, streetk, mass_hrc, pop_hrc, prem_hrc, kaccounts, ogrocery, snb_cnc, pay_n_go, shop_n_browse, entertainment, outlets, apps_version, updated, visited from daily_sales_msr where date between '"+day+"' and '"+day+"'")

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
                            self.log.info("select mtime, dpid, route_id, skid, `group`, family, date, sale, dprice, rprice, dcc_price, issue, `return`, memos, vmemos, tlp, cnc, vp, mvp, p, tcc, dcc, ecnc, gt, struc, semi_struc, streetk, mass_hrc, pop_hrc, prem_hrc, kaccounts, ogrocery, snb_cnc, pay_n_go, shop_n_browse, entertainment, outlets, apps_version, updated, visited from daily_sales_msr where date between '"+day+"' and '"+day+"'")

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

    query_manager_table_to_sales_fact_table = QueryManagerTableToSalesFactTableMonthlyDataEtxractorMySqlOperator(
                        task_id = 'query_manager_table_to_sales_fact_table',
                        starting_date = datetime.date(2021, 1, 1),
                        ending_date = datetime.date(2021, 6, 13),
                        # start_month = datetime.date(2020, 1, 1),
                        # end_month = datetime.date(2021, 1, 1),
                        mysql_source_conn_id = "prism_live_114",
                        mysql_destination_table = "sales_fact",
                        mysql_destination_conn_id = "aqm_dwh_server",
                        mysql_destination_preoperator = sql_statements.CREATE_sales_fact_TABLE_IF_NOT_EXISTS,
                        bulk_load = False
    )

    daily_sales_msr_table_to_daily_sales_msr_fact_table = DailySalesMsrTableToDailySalesMsrFactTableMonthlyDataEtxractorMySqlOperator(
                        task_id = 'daily_sales_msr_table_to_daily_sales_msr_fact_table',
                        starting_date = datetime.date(2021, 1, 1),
                        ending_date = datetime.date(2021, 6, 13),
                        # start_month = datetime.date(2020, 1, 1),
                        # end_month = datetime.date(2021, 1, 1),
                        mysql_source_conn_id = "prism_live_114",
                        mysql_destination_table = "daily_sales_msr_fact",
                        mysql_destination_conn_id = "aqm_dwh_server",
                        mysql_destination_preoperator = sql_statements.CREATE_daily_sales_msr_fact_TABLE_IF_NOT_EXISTS,
                        bulk_load = False
    )

    # # daily_sales_msr_table_to_staged_daily_sales_msr_fact_table = MySqlToMySqlOperator(
    # #                     sql = "select mtime, dpid, route_id, skid, group, family, date, datetime, sale, dprice, rprice, dcc_price, issue, return, memoss, vmemos, tlp, cnc, vp, mvp, p, tcc, dcc, ecnc, gt, struc, semi_struc, streetk, mass_hrc, pop_hrc, prem_hrc, kaccounts, ogrocery, snb_cnc, pay_n_go, shop_n_browse, entertainment, outlets, apps_version, updated, visited from daily_sales_msr quick",
    # #                     task_id = 'daily_sales_msr_to_staged_daily_sales_msr_fact_table',
    # #                     mysql_destination_table = "staged_daliy_sales_msr_table",
    # #                     mysql_source_conn_id = "prism_local_118",
    # #                     mysql_destination_conn_id = "aqm_dwh_server",
    # #                     mysql_destination_preoperator = sql_statements.CREATE_staging_daily_sales_msr_TABLE_IF_NOT_EXISTS,
    # #                     bulk_load = True
    # # )
    #
    # # staged_query_manager_table_to_sales_fact_table = QueryManagerTableStagingTableToSalesFactTableOperator(
    # #                     task_id = 'staged_query_manager_table_to_sales_fact_table',
    # #                     # sql = "select rtlid, skid, volume, value, date, sales_time from query_manager_table where date between "+yesterday_date+" and "+yesterday_date+" 23:59:59",
    # #                     mysql_source_conn_id = "aqm_dwh_server",
    # #                     mysql_destination_table = "sales_fact",
    # #                     mysql_destination_conn_id = "aqm_dwh_server",
    # #                     mysql_destination_preoperator = sql_statements.CREATE_sales_fact_TABLE_IF_NOT_EXISTS,
    # #                     mysql_destination_postoperator = "DROP TABLE IF EXISTS staged_query_manager_table;",
    # #                     bulk_load = True
    # # )
    #
    # # staged_daily_sales_msr_table_to_daily_sales_msr_fact_table = DailySalesMsrStagingTableToDailySalesMsrFactTableOperator(
    # #                     task_id = 'staged_daliy_sales_msr_table_to_daily_sales_msr_fact_table',
    # #                     # sql = "select rtlid, skid, volume, value, date, sales_time from query_manager_table where date between "+yesterday_date+" and "+yesterday_date+" 23:59:59",
    # #                     mysql_source_conn_id = "aqm_dwh_server",
    # #                     mysql_destination_table = "daily_sales_msr_fact",
    # #                     mysql_destination_conn_id = "aqm_dwh_server",
    # #                     mysql_destination_preoperator = sql_statements.CREATE_daily_sales_msr_fact_TABLE_IF_NOT_EXISTS,
    # #                     mysql_destination_postoperator = "DROP TABLE IF EXISTS staged_daliy_sales_msr_table;",
    # #                     bulk_load = True
    # # )
    #
    loading_cats_dimension_table = MySqlToMySqlOperator(
                        task_id = 'load_cats_dimension_table',
                        sql = "select * from _cats",
                        mysql_source_conn_id = "prism_live_114",
                        mysql_destination_table = "cats_dimension",
                        mysql_destination_conn_id = "aqm_dwh_server",
                        bulk_load = False
    )

    loading_locations_dimension_table = MySqlToMySqlOperator(
                        task_id = 'load_locations_dimension_table',
                        sql = "select * from _locations",
                        mysql_source_conn_id = "prism_live_114",
                        mysql_destination_table = "locations_dimension",
                        mysql_destination_conn_id = "aqm_dwh_server",
                        bulk_load = False
    )

    loading_company_dimension_table = MySqlToMySqlOperator(
                        task_id = 'load_company_dimension_table',
                        sql = "select * from company",
                        mysql_source_conn_id = "prism_live_114",
                        mysql_destination_table = "company_dimension",
                        mysql_destination_conn_id = "aqm_dwh_server",
                        bulk_load = False
    )

    loading_distributorspoint_dimension_table = MySqlToMySqlOperator(
                        task_id = 'load_distributorpoints_dimension_table',
                        sql = "select * from distributorspoint",
                        mysql_source_conn_id = "prism_live_114",
                        mysql_destination_table = "distributorspoint_dimension",
                        mysql_destination_conn_id = "aqm_dwh_server",
                        bulk_load = False
    )

    loading_products_dimension_table = MySqlToMySqlOperator(
                        task_id = 'load_productions_dimension_table',
                        sql = "select * from products",
                        mysql_source_conn_id = "prism_live_114",
                        mysql_destination_table = "products_dimension",
                        mysql_destination_conn_id = "aqm_dwh_server",
                        bulk_load = False
    )

    loading_routes_dimension_table = MySqlToMySqlOperator(
                        task_id = 'load_routes_dimension_table',
                        sql = "select * from routes where section != 0",
                        mysql_source_conn_id = "prism_live_114",
                        mysql_destination_table = "routes_dimension",
                        mysql_destination_conn_id = "aqm_dwh_server",
                        bulk_load = False
    )

    loading_rtl_dimension_table = MySqlToMySqlOperator(
                        task_id = 'load_rtl_dimension_table',
                        sql = "select * from retailers",
                        mysql_source_conn_id = "prism_live_114",
                        mysql_destination_table = "rtl_dimension",
                        mysql_destination_conn_id = "aqm_dwh_server",
                        bulk_load = False
    )

    loading_section_days_dimension_table = MySqlToMySqlOperator(
                        task_id = 'load_section_days_dimension_table',
                        sql = "select * from section_days",
                        mysql_source_conn_id = "prism_live_114",
                        mysql_destination_table = "section_days_dimension",
                        mysql_destination_conn_id = "aqm_dwh_server",
                        bulk_load = False
    )

    loading_sku_dimension_table = MySqlToMySqlOperator(
                        task_id = 'load_sku_dimension_table',
                        sql = "select * from sku",
                        mysql_source_conn_id = "prism_live_114",
                        mysql_destination_table = "sku_dimension",
                        mysql_destination_conn_id = "aqm_dwh_server",
                        bulk_load = False
    )

    create_cats_dimension_table = MySqlOperator(
                        task_id = 'create_cats_dimension_table',
                        mysql_conn_id = "aqm_dwh_server",
                        sql = sql_statements.CREATE_cats_dimension_TABLE_IF_NOT_EXISTS,
    )

    create_locations_dimension_table = MySqlOperator(
                        task_id = 'create_locations_dimension_table',
                        mysql_conn_id = "aqm_dwh_server",
                        sql = sql_statements.CREATE_locations_dimension_TABLE_IF_NOT_EXISTS,
    )

    create_company_dimension_table = MySqlOperator(
                        task_id = 'create_company_dimension_table',
                        mysql_conn_id = "aqm_dwh_server",
                        sql = sql_statements.CREATE_company_dimension_TABLE_IF_NOT_EXISTS,
    )

    create_distributorspoint_dimension_table = MySqlOperator(
                        task_id = 'create_distributorspoint_dimension_table',
                        mysql_conn_id = "aqm_dwh_server",
                        sql = sql_statements.CREATE_distributorspoint_dimension_TABLE_IF_NOT_EXISTS,
    )

    create_products_dimension_table = MySqlOperator(
                        task_id = 'create_products_dimension_table',
                        mysql_conn_id = "aqm_dwh_server",
                        sql = sql_statements.CREATE_products_dimension_TABLE_IF_NOT_EXISTS,
    )

    create_section_days_dimension_table = MySqlOperator(
                        task_id = 'create_section_days_dimension_table',
                        mysql_conn_id = "aqm_dwh_server",
                        sql = sql_statements.CREATE_section_days_dimension_TABLE_IF_NOT_EXISTS,
    )

    create_sku_dimension_table = MySqlOperator(
                        task_id = 'create_sku_dimension_table',
                        mysql_conn_id = "aqm_dwh_server",
                        sql = sql_statements.CREATE_sku_dimension_TABLE_IF_NOT_EXISTS,
    )

    create_routes_dimension_table = MySqlOperator(
                        task_id = 'create_routes_dimension_table',
                        mysql_conn_id = "aqm_dwh_server",
                        sql = sql_statements.CREATE_routes_dimension_TABLE_IF_NOT_EXISTS,
    )

    create_rtl_dimension_table = MySqlOperator(
                        task_id = 'create_rtl_dimension_table',
                        mysql_conn_id = "aqm_dwh_server",
                        sql = sql_statements.CREATE_rtl_dimension_TABLE_IF_NOT_EXISTS,
    )

    drop_sales_fact_table = MySqlOperator(
                        task_id = 'drop_sales_fact_table',
                        mysql_conn_id = "aqm_dwh_server",
                        sql = "DROP TABLE IF EXISTS sales_fact",
    )

    drop_daily_sales_msr_fact_table = MySqlOperator(
                        task_id = 'drop_daily_sales_msr_fact_table',
                        mysql_conn_id = "aqm_dwh_server",
                        sql = "DROP TABLE IF EXISTS daily_sales_msr_fact",
    )

    drop_distributorspoint_dimension_table = MySqlOperator(
                        task_id = 'drop_distributorspoint_dimension_table',
                        mysql_conn_id = "aqm_dwh_server",
                        sql = "DROP TABLE IF EXISTS distributorspoint_dimension",
    )

    drop_rtl_dimension_table = MySqlOperator(
                        task_id = 'drop_rtl_dimension_table',
                        mysql_conn_id = "aqm_dwh_server",
                        sql = "DROP TABLE IF EXISTS rtl_dimension",
    )

    drop_routes_dimension_table = MySqlOperator(
                        task_id = 'drop_routes_dimension_table',
                        mysql_conn_id = "aqm_dwh_server",
                        sql = "DROP TABLE IF EXISTS routes_dimension",
    )


    drop_sku_dimension_table = MySqlOperator(
                        task_id = 'drop_sku_dimension_table',
                        mysql_conn_id = "aqm_dwh_server",
                        sql = "DROP TABLE IF EXISTS sku_dimension",
    )

    drop_section_days_dimension_table = MySqlOperator(
                        task_id = 'drop_section_days_dimension_table',
                        mysql_conn_id = "aqm_dwh_server",
                        sql = "DROP TABLE IF EXISTS section_days_dimension",
    )

    drop_products_dimension_table = MySqlOperator(
                        task_id = 'drop_products_dimension_table',
                        mysql_conn_id = "aqm_dwh_server",
                        sql = "DROP TABLE IF EXISTS products_dimension",
    )

    drop_cats_dimension_table = MySqlOperator(
                        task_id = 'drop_cats_dimension_table',
                        mysql_conn_id = "aqm_dwh_server",
                        sql = "DROP TABLE IF EXISTS cats_dimension",
    )

    drop_locations_dimension_table = MySqlOperator(
                        task_id = 'drop_locations_dimension_table',
                        mysql_conn_id = "aqm_dwh_server",
                        sql = "DROP TABLE IF EXISTS locations_dimension",
    )

    drop_company_dimension_table = MySqlOperator(
                        task_id = 'drop_company_dimension_table',
                        mysql_conn_id = "aqm_dwh_server",
                        sql = "DROP TABLE IF EXISTS company_dimension",
    )


    # run_quality_checks = SQLCheckOperator(
    #                    task_id = 'Run_data_quality_checks',
    #                    sql = "select count(*) from sales_fact",
    #                    conn_id = "aqm_mysql_dw",
    # )

    run_quality_checks = CheckOperator(
                       task_id = 'Run_data_quality_checks',
                       sql = "select count(*) from sales_fact",
                       conn_id = "aqm_dwh_server",
    )

    send_success_email = EmailOperator(
                task_id='send_email',
                to='shafiqulislam561@gmail.com',
                subject='ETL process for PRISM AQM DWH',
                html_content=""" <h1>Congratulations! ETL for PRISM AQM DWH process has been completed.</h1> """,
    )


    end_operator = DummyOperator(task_id='Stop_execution')

    start_operator >> drop_sales_fact_table

    drop_sales_fact_table >> drop_daily_sales_msr_fact_table
    drop_daily_sales_msr_fact_table >> drop_routes_dimension_table
    drop_routes_dimension_table >> drop_distributorspoint_dimension_table
    drop_distributorspoint_dimension_table >> drop_rtl_dimension_table
    drop_rtl_dimension_table >> drop_sku_dimension_table
    drop_sku_dimension_table >> drop_section_days_dimension_table
    drop_section_days_dimension_table >> drop_products_dimension_table
    drop_products_dimension_table >> drop_cats_dimension_table
    drop_cats_dimension_table >> drop_locations_dimension_table
    drop_locations_dimension_table >> drop_company_dimension_table
    drop_company_dimension_table >> create_cats_dimension_table

    create_cats_dimension_table >> create_locations_dimension_table
    create_locations_dimension_table >> create_company_dimension_table
    create_company_dimension_table >> create_distributorspoint_dimension_table
    create_distributorspoint_dimension_table >> create_products_dimension_table
    create_products_dimension_table >> create_section_days_dimension_table
    create_section_days_dimension_table >> create_sku_dimension_table
    create_sku_dimension_table >> create_routes_dimension_table
    create_routes_dimension_table >> create_rtl_dimension_table

    create_rtl_dimension_table >> loading_cats_dimension_table
    loading_cats_dimension_table >> loading_locations_dimension_table
    loading_locations_dimension_table >> loading_company_dimension_table
    loading_company_dimension_table >> loading_section_days_dimension_table
    loading_section_days_dimension_table >> loading_products_dimension_table
    loading_products_dimension_table >> loading_sku_dimension_table
    loading_sku_dimension_table >> loading_distributorspoint_dimension_table
    loading_distributorspoint_dimension_table >> loading_routes_dimension_table
    loading_routes_dimension_table >> loading_rtl_dimension_table

    loading_rtl_dimension_table >> query_manager_table_to_sales_fact_table
    query_manager_table_to_sales_fact_table >> daily_sales_msr_table_to_daily_sales_msr_fact_table

    # start_operator >> query_manager_table_to_staged_query_manager_table
    # query_manager_table_to_staged_query_manager_table >> staged_query_manager_table_to_sales_fact_table
    # staged_query_manager_table_to_sales_fact_table >> daily_sales_msr_table_to_staged_daily_sales_msr_fact_table
    # daily_sales_msr_table_to_staged_daily_sales_msr_fact_table >> staged_daily_sales_msr_table_to_daily_sales_msr_fact_table
    # staged_daily_sales_msr_table_to_daily_sales_msr_fact_table >> loading_cats_dimension_table
    # staged_daily_sales_msr_table_to_daily_sales_msr_fact_table >> loading_locations_dimension_table
    # staged_daily_sales_msr_table_to_daily_sales_msr_fact_table >> loading_company_dimension_table
    # staged_daily_sales_msr_table_to_daily_sales_msr_fact_table >> loading_distibutorpoints_dimension_table
    # staged_daily_sales_msr_table_to_daily_sales_msr_fact_table >> loading_products_dimension_table
    # staged_daily_sales_msr_table_to_daily_sales_msr_fact_table >> loading_routes_dimension_table
    # staged_daily_sales_msr_table_to_daily_sales_msr_fact_table >> loading_rtl_dimension_table
    # staged_daily_sales_msr_table_to_daily_sales_msr_fact_table >> loading_section_days_dimension_table
    # staged_daily_sales_msr_table_to_daily_sales_msr_fact_table >> loading_sku_dimension_table

    daily_sales_msr_table_to_daily_sales_msr_fact_table >> run_quality_checks

    run_quality_checks >> send_success_email

    send_success_email >> end_operator
