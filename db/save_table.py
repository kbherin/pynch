from sqlalchemy.engine import Connection, Engine
from sqlalchemy import MetaData, Table
from sqlalchemy.dialects.postgresql import insert
from typing import Callable, Iterable
import pandas as pd
from datetime import datetime

def _parse_index_columns(index_columns):
    if type(index_columns) == list:
        idx_columns = index_columns
    elif type(index_columns) == str:
        idx_columns = [index_columns]
    else:
        raise TypeError('index_columns should be list of column names or a string of a single column name')

    return idx_columns

def make_inserter(table_name:str, schema_name:str, index_columns:list[str]|str, conn:Connection|Engine, meta:MetaData,
        table_update_user='DEFAULT_USER') -> Callable[[pd.io.sql.SQLTable, Connection, list[str], Iterable[any]], int]:
    """
    Generate a Pandas inserter method to insert or ignore if key exists

    Parameters
    ----------
    index_columns : primary key column names
    """

    idx_columns = _parse_index_columns(index_columns)
    tbl = Table(table_name, meta, schema=schema_name, autoload_with=conn)
    tbl_col_names = set([col.name for col in tbl.columns])

    def _insert_to_psql(table :pd.io.sql.SQLTable,
        conn :Connection,
        columns :list[str],
        data_iter :Iterable[any]) -> int:
        """
        Execute SQL statement to insert or ignore

        Parameters
        ----------
        table : pandas.io.sql.SQLTable
        conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
        keys : list of str
            Column names
        data_iter : Iterable that iterates the values to be inserted
        """

        add_created_by = 'created_by' in tbl_col_names
        add_updated_by = 'updated_by' in tbl_col_names

        row_count = 0
        insert_values = []

        for rec in data_iter:
            insert_cv = dict([cv for cv in zip(columns, rec) if cv[0] in tbl_col_names])
            if add_created_by:
                insert_cv['created_by'] = table_update_user
            if add_updated_by:
                insert_cv['updated_by'] = table_update_user

            insert_values.append(insert_cv)
        
        insert_stmt = (insert(tbl)
                .values(insert_values)
                .on_conflict_do_nothing(index_elements=idx_columns)
        )
        row_count += conn.execute(insert_stmt).rowcount
        return row_count

    return _insert_to_psql

    

def make_upserter(table_name:str, schema_name:str, index_columns:list[str]|str, conn:Connection|Engine, meta:MetaData,
        where_maker:Callable[[insert, Table], any]=None,
        table_update_user='DEFAULT_USER') -> Callable[[pd.io.sql.SQLTable, Connection, list[str], Iterable[any]], int]:
    """
    Generate a Pandas upserter method to insert or update if key exists

    Parameters
    ----------
    index_columns : primary key column names
    """

    idx_columns = _parse_index_columns(index_columns)
    tbl = Table(table_name, meta, schema=schema_name, autoload_with=conn)
    tbl_col_names = set([col.name for col in tbl.columns])
    tbl_pk_col_names = set([col.name for col in tbl.columns if col.primary_key])
    
    def _upsert_to_psql(table :pd.io.sql.SQLTable,
        conn :Connection,
        columns :list[str],
        data_iter :Iterable[any]) -> int:
        """
        Execute SQL statement to insert or update if key exists

        Parameters
        ----------
        table : pandas.io.sql.SQLTable
        conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
        keys : list of str
            Column names
        data_iter : Iterable that iterates the values to be inserted
        """

        add_created_by = 'created_by' in tbl_col_names
        add_updated_by = 'updated_by' in tbl_col_names
        add_updated_at = 'updated_at' in tbl_col_names

        row_count = 0
        insert_values = []

        for rec in data_iter:
            insert_cv = dict([cv for cv in zip(columns, rec) if cv[0] in tbl_col_names])

            if add_created_by:
                insert_cv['created_by'] = table_update_user
            if add_updated_by:
                insert_cv['updated_by'] = table_update_user

            insert_values.append(insert_cv)
        
        insert_stmt = insert(tbl).values(insert_values)
        update_cv = {c:insert_stmt.excluded[c] for c in insert_cv.keys() if c not in tbl_pk_col_names and c != 'created_by'}
        if add_updated_by:
            update_cv['updated_by'] = table_update_user
        if add_updated_at:
            update_cv['updated_at'] = datetime.now().astimezone()
        
        if where_maker == None:
            where_clause = None
        else:
            where_clause = where_maker(insert_stmt, tbl)
        
        upsert_stmt = (insert_stmt
            .on_conflict_do_update(
                index_elements=idx_columns,
                set_=update_cv,
                where=where_clause
            )
        )
        row_count += conn.execute(upsert_stmt).rowcount
        return row_count

    return _upsert_to_psql