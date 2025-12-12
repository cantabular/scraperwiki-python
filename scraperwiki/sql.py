from collections import OrderedDict
from collections.abc import Iterable, Mapping

import atexit
import datetime
import time
import os
import re
import warnings

import alembic.ddl
import sqlalchemy
import six

DATABASE_NAME = os.environ.get("SCRAPERWIKI_DATABASE_NAME",
                               "sqlite:///scraperwiki.sqlite")

DATABASE_TIMEOUT = float(os.environ.get("SCRAPERWIKI_DATABASE_TIMEOUT", 300))
SECONDS_BETWEEN_COMMIT = 2
unicode = str

# The scraperwiki.sqlite.SqliteError exception
SqliteError = sqlalchemy.exc.SQLAlchemyError

class Blob(bytes):
    """
    Represents a blob as a string.
    """
    pass

PYTHON_SQLITE_TYPE_MAP = {
    str: sqlalchemy.types.Text,
    str: sqlalchemy.types.Text,
    int: sqlalchemy.types.BigInteger,
    bool: sqlalchemy.types.Boolean,
    float: sqlalchemy.types.Float,

    datetime.date: sqlalchemy.types.Date,
    datetime.datetime: sqlalchemy.types.DateTime,

    Blob: sqlalchemy.types.LargeBinary,
}

if bytes is not str:
    # On 2.7, bytes *is* str, so we don't want to overwrite that.
    PYTHON_SQLITE_TYPE_MAP[bytes] = sqlalchemy.types.LargeBinary

try:
    PYTHON_SQLITE_TYPE_MAP[long] = sqlalchemy.types.BigInteger
except NameError:
    pass


class _State:

    """
    This class maintains global state relating to the database such as
    connection. It does not form part of the public interface.
    """
    db_path = DATABASE_NAME
    engine = None
    _connection = None
    _transaction = None
    metadata = None
    table = None
    # Whether or not we need to create the table. It's set by
    # _set_table(); it's left unassigned here to catch
    # accidental uses of it.
    # table_pending = None
    vars_table_name = 'swvariables'
    last_commit = None
    echo = False

    @classmethod
    def connection(cls):
        if cls._connection is None:
            create = sqlalchemy.create_engine
            cls.engine = create(cls.db_path, echo=cls.echo,
                                connect_args={'timeout': DATABASE_TIMEOUT})
            cls._connection = cls.engine.connect()
            cls.new_transaction()
        if cls.table is None:
            cls.reflect_metadata()
            cls.table = sqlalchemy.Table('swdata', _State.metadata,
                                         extend_existing=True)
        if cls._transaction is None:
            cls.new_transaction()
        return cls._connection

    @classmethod
    def new_transaction(cls):
        cls.last_commit = time.time()
        if cls._transaction is not None:
            cls._transaction.commit()
        cls._transaction = cls._connection.begin()

    @classmethod
    def reflect_metadata(cls):
        if cls.metadata is None:
            cls.metadata = sqlalchemy.MetaData()
        cls.metadata.reflect(bind=cls.engine)

    @classmethod
    def check_last_committed(cls):
        if time.time() - cls.last_commit > SECONDS_BETWEEN_COMMIT:
            cls.new_transaction()


class Transaction:

    """
    This context manager must be used when other services need
    to connect to the database.
    """

    def __enter__(self):
        _State.connection()
        _State.new_transaction()

    def __exit__(self, *args):
        _State._transaction.commit()
        _State._transaction = None


@atexit.register
def commit_transactions():
    """
    Ensure any outstanding transactions are committed on exit
    """
    if _State is not None and _State._transaction is not None:
        _State._transaction.commit()
        _State._transaction = None


def execute(query, data=None):
    """
    Execute an arbitrary SQL query given by query, returning any
    results as a list of OrderedDicts. A list of values can be supplied as an,
    additional argument, which will be substituted into question marks in the
    query.
    """
    connection = _State.connection()
    _State.new_transaction()

    if data is None:
        data = []

    if isinstance(data, list):
        data = tuple(data)
    elif not isinstance(data, (tuple, dict)):
        data = (data,)

    result = connection.exec_driver_sql(query, data)

    _State.table = None
    _State.metadata = None
    try:
        del _State.table_pending
    except AttributeError:
        pass

    if not result.returns_rows:
        return {'data': [], 'keys': []}

    return {'data': result.fetchall(), 'keys': list(result.keys())}


def select(query, data=None):
    """
    Perform a sql select statement with the given query (without 'select') and
    return any results as a list of OrderedDicts.
    """
    connection = _State.connection()
    _State.new_transaction()
    if data is None:
        data = []

    if isinstance(data, list):
        data = tuple(data)
    elif not isinstance(data, (tuple, dict)):
        data = (data,)

    result = connection.exec_driver_sql('select ' + query, data)

    rows = []
    for row in result:
        rows.append(dict(list(row._mapping.items())))

    return rows


def save(unique_keys, data, table_name='swdata'):
    """
    Save the given data to the table specified by `table_name`
    (which defaults to 'swdata'). The data must be a mapping
    or an iterable of mappings. Unique keys is a list of keys that exist
    for all rows and for which a unique index will be created.
    """

    _set_table(table_name)

    connection = _State.connection()

    if isinstance(data, Mapping):
        # Is a single datum
        data = [data]
    elif not isinstance(data, Iterable):
        raise TypeError("Data must be a single mapping or an iterable "
                        "of mappings")

    insert = sqlalchemy.insert(_State.table).prefix_with('OR REPLACE')
    for row in data:
        if not isinstance(row, Mapping):
            raise TypeError("Elements of data must be mappings, got {}".format(
                            type(row)))
        fit_row(connection, row, unique_keys)
        connection.execute(insert.values(row))
    _State.check_last_committed()


def _set_table(table_name):
    """
    Specify the table to work on.
    """
    _State.connection()
    _State.reflect_metadata()
    _State.table = sqlalchemy.Table(table_name, _State.metadata,
                                    extend_existing=True)

    if list(_State.table.columns.keys()) == []:
        _State.table_pending = True
    else:
        _State.table_pending = False


def show_tables():
    """
    Return the names of the tables currently in the database.
    """
    _State.connection()
    _State.reflect_metadata()
    metadata = _State.metadata

    response = select('name, sql from sqlite_master where type="table"')

    return {row['name']: row['sql'] for row in response}


def save_var(name, value):
    """
    Save a variable to the table specified by _State.vars_table_name. Key is
    the name of the variable, and value is the value.
    """
    connection = _State.connection()
    _State.reflect_metadata()

    vars_table = sqlalchemy.Table(
        _State.vars_table_name, _State.metadata,
        sqlalchemy.Column('name', sqlalchemy.types.Text, primary_key=True),
        sqlalchemy.Column('value_blob', sqlalchemy.types.LargeBinary),
        sqlalchemy.Column('type', sqlalchemy.types.Text),
        keep_existing=True
    )

    vars_table.create(connection, checkfirst=True)

    column_type = get_column_type(value)

    if column_type == sqlalchemy.types.LargeBinary:
        value_blob = value
    else:
        value_blob = unicode(value).encode('utf-8')

    values = dict(name=name,
                  value_blob=value_blob,
                  # value_blob=Blob(value),
                  type=column_type.__visit_name__.lower())

    stmt = sqlalchemy.insert(vars_table).prefix_with('OR REPLACE').values(**values)
    connection.execute(stmt)
    _State.new_transaction()

def get_var(name, default=None):
    """
    Returns the variable with the provided key from the
    table specified by _State.vars_table_name.
    """
    alchemytypes = {"text": lambda x: x.decode('utf-8'),
                    "big_integer": lambda x: int(x),
                    "date": lambda x: x.decode('utf-8'),
                    "datetime": lambda x: x.decode('utf-8'),
                    "float": lambda x: float(x),
                    "large_binary": lambda x: x,
                    "boolean": lambda x: x==b'True'}

    connection = _State.connection()
    _State.new_transaction()

    if _State.vars_table_name not in list(_State.metadata.tables.keys()):
        return None

    table = sqlalchemy.Table(_State.vars_table_name, _State.metadata)
    s = sqlalchemy.select(table.c.value_blob, table.c.type)
    s = s.where(table.c.name == name)
    result = connection.execute(s).fetchone()

    if not result:
        return None

    return alchemytypes[result[1]](result[0])

    # This is to do the variable type conversion through the SQL engine
    execute = connection.execute
    execute(f"CREATE TEMPORARY TABLE _sw_tmp ('value' {result.type})")
    execute("INSERT INTO _sw_tmp VALUES (:value)", value=result.value_blob)
    var = execute('SELECT value FROM _sw_tmp').fetchone().value
    execute("DROP TABLE _sw_tmp")
    return var.decode('utf-8')


def create_index(column_names, unique=False):
    """
    Create a new index of the columns in column_names, where column_names is
    a list of strings. If unique is True, it will be a
    unique index.
    """
    connection = _State.connection()
    _State.reflect_metadata()
    table_name = _State.table.name

    table = _State.table

    index_name = re.sub(r'[^a-zA-Z0-9]', '', table_name) + '_'
    index_name += '_'.join(re.sub(r'[^a-zA-Z0-9]', '', x)
                           for x in column_names)

    if unique:
        index_name += '_unique'

    columns = []
    for column_name in column_names:
        columns.append(table.columns[column_name])

    current_indices = [x.name for x in table.indexes]
    index = sqlalchemy.schema.Index(index_name, *columns, unique=unique)
    if index.name not in current_indices:
        index.create(bind=_State.engine)


def fit_row(connection, row, unique_keys):
    """
    Takes a row and checks to make sure it fits in the columns of the
    current table. If it does not fit, adds the required columns.
    """
    new_columns = []
    for column_name, column_value in list(row.items()):
        new_column = sqlalchemy.Column(column_name,
                                       get_column_type(column_value))

        if not column_name in list(_State.table.columns.keys()):
            new_columns.append(new_column)
            _State.table.append_column(new_column)

    if _State.table_pending:
        create_table(unique_keys)
        return

    for new_column in new_columns:
        add_column(connection, new_column)


def create_table(unique_keys):
    """
    Save the table currently waiting to be created.
    """
    _State.new_transaction()
    _State.table.create(bind=_State.engine, checkfirst=True)
    if unique_keys != []:
        create_index(unique_keys, unique=True)
    _State.table_pending = False
    _State.reflect_metadata()


def add_column(connection, column):
    """
    Add a column to the current table.
    """
    stmt = alembic.ddl.base.AddColumn(_State.table.name, column)
    connection.execute(stmt)
    _State.reflect_metadata()


def get_column_type(column_value):
    """
    Return the appropriate SQL column type for the given value.
    """
    return PYTHON_SQLITE_TYPE_MAP.get(type(column_value),
      sqlalchemy.types.Text)



def commit():
    warnings.warn("scraperwiki.sql.commit is now a no-op")


def drop():
    """
    Drop the current table if it exists
    """
    # Ensure the connection is up
    _State.connection()
    _State.table.drop(checkfirst=True)
    _State.metadata.remove(_State.table)
    _State.table = None
    _State.new_transaction()
