from src.BaseDataTable import BaseDataTable
import pandas as pd
import pymysql

class RDBDataTable(BaseDataTable):
    """
    RDBDataTable is relation DB implementation of the BaseDataTable.
    """

    # Default connection information in case the code does not pass an object
    # specific connection on object creation.
    _default_connect_info = {
        'host': 'localhost',
        'user': 'dbuser',
        'password': 'dbuserdbuser',
        'db': 'lahman2019raw',
        'port': 3306
    }

    def __init__(self, table_name, key_columns, connect_info=None, debug=False):
        """
        :param table_name: The name of the RDB table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """

        # Initialize and store information in the parent class.
        super().__init__(table_name, connect_info, key_columns, debug)

        self.table_name = table_name;
        self.key_columns = key_columns;

        # If there is not explicit connect information, use the defaults.
        if connect_info is None:
            self._connect_info = RDBDataTable._default_connect_info

        # Create a connection to use inside this object. In general, this is not the right approach.
        # There would be a connection pool shared across many classes and applications.
        self._cnx = pymysql.connect(
            host=self._connect_info['host'],
            user=self._connect_info['user'],
            password=self._connect_info['password'],
            db=self._connect_info['db'],
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor)

    def __str__(self):
        """
                :return: String representation of the data table.
                """
        result = "RDBDataTable: table_name = " + self.table_name
        result += "\nKey fields: " + str(self.key_columns)

        # Find out how many rows are in the table.
        q1 = "SELECT count(*) as count from " + self.table_name
        r1 = self._run_q(q1, fetch=True, commit=True)
        result += "\nNo. of rows = " + str(r1[0]['count'])

        # Get the first five rows and print to show sample data.
        # Query to get data.
        q = "select * from " + self.table_name + " limit 5"

        # Read into a data frame to make pretty print easier.
        df = pd.read_sql(q, self._cnx)
        result += "\nFirst five rows:\n"
        result += df.to_string()

        return result

    def _run_q(self, q, args=None, fields=None, fetch=True, cnx=None, commit=True):
        """
        :param q: An SQL query string that may have %s slots for argument insertion. The string
            may also have {} after select for columns to choose.
        :param args: A tuple of values to insert in the %s slots.
        :param fetch: If true, return the result.
        :param cnx: A database connection. May be None
        :param commit: Do not worry about this for now. This is more wizard stuff.
        :return: A result set or None.
        """

        # Use the connection in the object if no connection provided.
        if cnx is None:
            cnx = self._cnx

        # Convert the list of columns into the form "col1, col2, ..." for following SELECT.
        if fields:
            q = q.format(",".join(fields))

        cursor = cnx.cursor()  # Just ignore this for now.

        # If debugging is turned on, will print the query sent to the database.
        # self.debug_message("Query = ", cursor.mogrify(q, args))

        cursor.execute(q, args)  # Execute the query.

        # Technically, INSERT, UPDATE and DELETE do not return results.
        # Sometimes the connector libraries return the number of created/deleted rows.
        if fetch:
            r = cursor.fetchall()  # Return all elements of the result.
        else:
            r = None

        if commit:  # Do not worry about this for now.
            cnx.commit()

        return r

    def _run_insert(self, table_name, column_list, values_list, cnx=None, commit=True):
        """
        :param table_name: Name of the table to insert data. Probably should just get from the object data.
        :param column_list: List of columns for insert.
        :param values_list: List of column values.
        :param cnx: Ignore this for now.
        :param commit: Ignore this for now.
        :return:
        """
        try:
            q = "insert into " + table_name + " "

            # If the column list is not None, form the (col1, col2, ...) part of the statement.
            if column_list is not None:
                q += "(" + ",".join(column_list) + ") "

            # We will use query parameters. For a term of the form values(%s, %s, ...) with one slot for
            # each value to insert.
            values = ["%s"] * len(values_list)

            # Form the values(%s, %s, ...) part of the statement.
            values = " ( " + ",".join(values) + ") "
            values = "values" + values

            # Put all together.
            q += values

            self._run_q(q, args=values_list, fields=None, fetch=False, cnx=cnx, commit=commit)

        except Exception as e:
            print("Got exception = ", e)
            raise e

    def find_by_primary_key(self, key_fields, field_list=None):
        """
        :param key_fields: The values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the request fields for the record identified
            by the key.
        """
        if not isinstance(key_fields, list):
            raise ValueError("Wrong keys!")
        dic = {self.key_columns[i]: key_fields[i] for i in range(len(key_fields))}
        where, args = self._template_to_where_clause(dic)
        if field_list != None:
            q = "SELECT " + "{}" + " From " + self.table_name + " " + where + ";"
        else:
            q = "SELECT " + "*" + " From " + self.table_name + " " + where + ";"
        r = self._run_q(q, args, field_list)
        return r
        pass

    def _template_to_where_clause(self, t):
        """
        Convert a query template into a WHERE clause.
        :param t: Query template.
        :return: (WHERE clause, arg values for %s in clause)
        """
        terms = []
        args = []
        w_clause = ""

        # The where clause will be of the for col1=%s, col2=%s, ...
        # Build a list containing the individual col1=%s
        # The args passed to +run_q will be the values in the template in the same order.
        for k, v in t.items():
            temp_s = k + "=%s "
            terms.append(temp_s)
            args.append(v)

        if len(terms) > 0:
            w_clause = "WHERE " + " AND ".join(terms)
        else:
            w_clause = ""
            args = None

        return w_clause, args

    def find_by_template(self, template, field_list=None, limit=None, offset=None, order_by=None, commit=True):
        """
        :param template: A dictionary of the form { "field1" : value1, "field2": value2, ...}
        :param field_list: A list of request fields of the form, ['fielda', 'fieldb', ...]
        :param limit: Do not worry about this for now.
        :param offset: Do not worry about this for now.
        :param order_by: Do not worry about this for now.
        :return: A list containing dictionaries. A dictionary is in the list representing each record
            that matches the template. The dictionary only contains the requested fields.
        """
        if not isinstance(template, dict):
            raise ValueError("Wrong template!")
        where, args = self._template_to_where_clause(template)
        if field_list != None:
            q = "SELECT " + "{}" + " From " + self.table_name + " " + where + ";"
        else:
            q = "SELECT " + "*" + " From " + self.table_name + " " + where + ";"
        r = self._run_q(q, args, field_list)
        return r
        pass

    def insert(self, new_record):
        """
        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """
        try:
            c_list = list(new_record.keys())
            v_list = list(new_record.values())
            self._run_insert(self.table_name, c_list, v_list)
        except Exception as e:
            print("insert: Exception e = ", e)
            raise (e)

    def delete_by_template(self, template):
        """
        Deletes all records that match the template.
        :param template: A template.
        :return: A count of the rows deleted.
        """
        if not isinstance(template, dict):
            raise ValueError("Wrong template!")
        r = len(self.find_by_template(template))
        cursor = self._cnx.cursor()
        where, args = self._template_to_where_clause(template)
        q = "DELETE FROM " + self.table_name + " " + where + ";"
        self._run_q(q, args)
        return r
        pass

    def delete_by_key(self, key_fields):
        """
        Delete record with corresponding key.
        :param key_fields: List containing the values for the key columns
        :return: A count of the rows deleted.
        """
        if not isinstance(key_fields, list):
            raise ValueError("Wrong keys!")
        dic = {self.key_columns[i]: key_fields[i] for i in range(len(key_fields))}
        r = len(self.find_by_template(dic))
        where, args = self._template_to_where_clause(dic)
        q = "DELETE FROM " + self.table_name + " " + where + ";"
        self._run_q(q, args)
        return r
        pass

    def update_by_template(self, template, new_values):
        """
        :param template: A template that defines which matching rows to update.
        :param new_values: A dictionary containing fields and the values to set for the corresponding fields
            in the records. This returns an error if the update would create a duplicate primary key. NO ROWS are
            update on this error.
        :return: The number of rows updates.
        """
        if not isinstance(template, dict):
            raise ValueError("Wrong template!")
        r = len(self.find_by_template(template))
        where, args = self._template_to_where_clause(template)

        update, terms, set_args = "", [], []
        for k, v in new_values.items():
            terms.append(k + '=%s')
            set_args.append(v)
        set_args.extend(args)

        q = "UPDATE " + self.table_name + " SET " + ", ".join(terms) + " " + where
        self._run_q(q, set_args)
        return r
        pass

    def update_by_key(self, key_fields, new_values):
        """
        :param key_fields: List of values for primary key fields
        :param new_values: A dictionary containing fields and the values to set for the corresponding fields
            in the records. This returns an error if the update would create a duplicate primary key. NO ROWS are
            update on this error.
        :return: The number of rows updates.
        """
        if not isinstance(key_fields, list):
            raise ValueError("Wrong keys!")
        dic = {self.key_columns[i]: key_fields[i] for i in range(len(key_fields))}
        r = self.update_by_template(dic, new_values)

        return r
        pass
