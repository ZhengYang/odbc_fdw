/*----------------------------------------------------------
 *
 *          foreign-data wrapper for ODBC
 *
 * Copyright (c) 2011, PostgreSQL Global Development Group
 *
 * This software is released under the PostgreSQL Licence.
 *
 * Author: Zheng Yang <zhengyang4k@gmail.com>
 *
 * IDENTIFICATION
 *        odbc_fdw/odbc_fdw.c
 *
 *----------------------------------------------------------
 */

/* Debug mode flag */
/* #define DEBUG */

#include "postgres.h"
#include <string.h>

#include "funcapi.h"
#include "access/reloptions.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "utils/memutils.h"
#include "utils/builtins.h"
#include "utils/relcache.h"
#include "storage/lock.h"
#include "miscadmin.h"

#include <stdio.h>
#include <sql.h>
#include <sqlext.h>

PG_MODULE_MAGIC;

#define PROCID_TEXTEQ 67
#define PROCID_TEXTCONST 25


typedef struct odbcFdwExecutionState
{
	AttInMetadata	*attinmeta;
	char			*svr_dsn;
	char			*svr_database;
	char			*svr_table;
	char			*svr_username;
	char			*svr_password;
	SQLHSTMT		stmt;
} odbcFdwExecutionState;

struct odbcFdwOption
{
	const char	*optname;
	Oid		optcontext;	/* Oid of catalog in which option may appear */
};

/*
 * Array of valid options
 * 
 */
static struct odbcFdwOption valid_options[] =
{
    /* Foreign server options */
	{ "dsn",    ForeignServerRelationId },

    /* Foreign table options */
	{ "database",   ForeignTableRelationId },
	{ "table",		ForeignTableRelationId },
	
	/* User mapping options */
	{ "username", UserMappingRelationId },
	{ "password", UserMappingRelationId },

    /* Sentinel */
    { NULL,         InvalidOid}
};


/*
 * SQL functions
 */
extern Datum odbc_fdw_handler(PG_FUNCTION_ARGS);
extern Datum odbc_fdw_validator(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(odbc_fdw_handler);
PG_FUNCTION_INFO_V1(odbc_fdw_validator);

/*
 * FDW callback routines
 */
static FdwPlan *odbcPlanForeignScan(Oid foreigntableid, PlannerInfo *root, RelOptInfo *baserel);
static void odbcExplainForeignScan(ForeignScanState *node, ExplainState *es);
static void odbcBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *odbcIterateForeignScan(ForeignScanState *node);
static void odbcReScanForeignScan(ForeignScanState *node);
static void odbcEndForeignScan(ForeignScanState *node);

/*
 * helper functions
 */
static bool odbcIsValidOption(const char *option, Oid context);

Datum
odbc_fdw_handler(PG_FUNCTION_ARGS)
{
    FdwRoutine *fdwroutine = makeNode(FdwRoutine);

    fdwroutine->PlanForeignScan = odbcPlanForeignScan;
    fdwroutine->ExplainForeignScan = odbcExplainForeignScan;
    fdwroutine->BeginForeignScan = odbcBeginForeignScan;
    fdwroutine->IterateForeignScan = odbcIterateForeignScan;
    fdwroutine->ReScanForeignScan = odbcReScanForeignScan;
    fdwroutine->EndForeignScan = odbcEndForeignScan;

    PG_RETURN_POINTER(fdwroutine);
}


/*
 * Validate function
 */
Datum
odbc_fdw_validator(PG_FUNCTION_ARGS)
{
	List	*options_list = untransformRelOptions(PG_GETARG_DATUM(0));
    Oid		catalog = PG_GETARG_OID(1);
    char	*dsn = NULL;
    char	*svr_database = NULL;
	char	*svr_table = NULL;
    char	*username = NULL;
    char	*password = NULL;
    ListCell	*cell;
	
#ifdef DEBUG	
    elog(NOTICE, "odbc_fdw_validator");
#endif
	
    /*
     * Check that the necessary options: address, port, database
     */
    foreach(cell, options_list)
    {
		
        DefElem	   *def = (DefElem *) lfirst(cell);
		
        /* Complain invalid options */
        if (!odbcIsValidOption(def->defname, catalog))
        {
            struct odbcFdwOption *opt;
            StringInfoData buf;
			
            /*
             * Unknown option specified, complain about it. Provide a hint
             * with list of valid options for the object.
             */
            initStringInfo(&buf);
            for (opt = valid_options; opt->optname; opt++)
            {
                if (catalog == opt->optcontext)
                    appendStringInfo(&buf, "%s%s", (buf.len > 0) ? ", " : "",
                                     opt->optname);
            }
			
            ereport(ERROR,
                    (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                     errmsg("invalid option \"%s\"", def->defname),
                     errhint("Valid options in this context are: %s", buf.len ? buf.data : "<none>")
					 ));
        }
		
        /* Complain about redundent options */
        if (strcmp(def->defname, "dsn") == 0)
        {
            if (dsn)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("conflicting or redundant options: dsn (%s)", defGetString(def))
								));
			
            dsn = defGetString(def);
        }
        else if (strcmp(def->defname, "database") == 0)
        {
            if (svr_database)
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("conflicting or redundant options: database (%s)", defGetString(def))
						 ));
			
            svr_database = defGetString(def);
        }
		else if (strcmp(def->defname, "table") == 0)
        {
            if (svr_table)
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("conflicting or redundant options: table (%s)", defGetString(def))
						 ));
			
            svr_database = defGetString(def);
        }
        else if (strcmp(def->defname, "username") == 0)
        {
            if (username)
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("conflicting or redundant options: username (%s)", defGetString(def))
						 ));
			
            username = defGetString(def);
        }
        else if (strcmp(def->defname, "password") == 0)
        {
            if (password)
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("conflicting or redundant options: password (%s)", defGetString(def))
						 ));
			
            password = defGetString(def);
        }
    }
	
	/* Complain about missing essential options: dsn */
	if (!dsn && catalog == ForeignServerRelationId)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("missing eaaential information: dsn (Database Source Name)")
				 ));
	
	
    PG_RETURN_VOID();
}


/*
 * Fetch the options for a odbc_fdw foreign table.
 */
static void
odbcGetOptions(Oid foreigntableid, char **svr_dsn, char **svr_database, char ** svr_table, 
                  char **username, char **password, List **mapping_list)
{
    ForeignTable	*table;
    ForeignServer	*server;
    UserMapping	*mapping;
    List		*options;
    ListCell	*lc;

#ifdef DEBUG
    elog(NOTICE, "odbcGetOptions");
#endif
	
    /*
     * Extract options from FDW objects.
     */
    table = GetForeignTable(foreigntableid);
    server = GetForeignServer(table->serverid);
    mapping = GetUserMapping(GetUserId(), table->serverid);
	
    options = NIL;
    options = list_concat(options, table->options);
    options = list_concat(options, server->options);
    options = list_concat(options, mapping->options);
	
    *mapping_list = NIL;
    /* Loop through the options, and get the foreign table options */
    foreach(lc, options)
    {
        DefElem *def = (DefElem *) lfirst(lc);
		
        if (strcmp(def->defname, "dsn") == 0)
        {
            *svr_dsn = defGetString(def);
            continue;
        }
		
        if (strcmp(def->defname, "database") == 0)
        {
            *svr_database = defGetString(def);
            continue;
        }
		
        if (strcmp(def->defname, "table") == 0)
        {
            *svr_table = defGetString(def);
            continue;
        }
        if (strcmp(def->defname, "username") == 0)
        {
            *username = defGetString(def);
            continue;
        }
		
        if (strcmp(def->defname, "password") == 0)
        {
            *password = defGetString(def);
            continue;
        }
		
        /* Column mapping goes here */
        *mapping_list = lappend(*mapping_list, def);
    }
	
#ifdef DEBUG
    elog(NOTICE, "list length: %i", (*mapping_list)->length);
#endif
	
    /* Default values, if required */
	if (!*svr_dsn)
		*svr_dsn = NULL;
		
    if (!*svr_database)
        *svr_database = NULL;
	
	if (!*svr_table)
		*svr_table = NULL;
	
    if (!*username)
        *username = NULL;
	
    if (!*password)
        *password = NULL;
}

void static extract_error(
						  char *fn,
						  SQLHANDLE handle,
						  SQLSMALLINT type)
{
    SQLINTEGER	 i = 0;
    SQLINTEGER	 native;
    SQLCHAR	 state[ 7 ];
    SQLCHAR	 text[256];
    SQLSMALLINT	 len;
    SQLRETURN	 ret;
	
    elog(NOTICE,
		 "\n"
		 "The driver reported the following diagnostics whilst running "
		 "%s\n\n",
		 fn);
	
    do
    {
        ret = SQLGetDiagRec(type, handle, ++i, state, &native, text,
                            sizeof(text), &len );
        if (SQL_SUCCEEDED(ret))
            elog(NOTICE, "%s:%ld:%ld:%s\n", state, (long int) i, (long int) native, text);
    }
    while( ret == SQL_SUCCESS );
}

/*
 * get table size of a table
 */
static void
odbcGetTableSize(char *svr_dsn, char *svr_database, char *svr_table, char *username, char *password, unsigned int *size)
{
	SQLHENV env;
	SQLHDBC dbc;
	SQLHSTMT stmt;
	SQLRETURN ret;
	
	StringInfoData	conn_str;
	StringInfoData	sql_str;
	SQLCHAR OutConnStr[1024];
	SQLSMALLINT OutConnStrLen;
	
	SQLUBIGINT table_size;
	SQLLEN indicator;
	
	/* Construct connection string */
	initStringInfo(&conn_str);
	appendStringInfo(&conn_str, "DSN=%s;DATABASE=%s;UID=%s;PWD=%s;", svr_dsn, svr_database, username, password);
#ifdef DEBUG
	elog(NOTICE, "Connection string: %s", conn_str.data);
#endif
	/* Allocate an environment handle */
	SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
	/* We want ODBC 3 support */
	SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void *) SQL_OV_ODBC3, 0);
	/* Allocate a connection handle */
	SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
	
	/* Connect to the DSN */
	ret = SQLDriverConnect(dbc, NULL, (SQLCHAR *) conn_str.data, SQL_NTS,
						   OutConnStr, 1024, &OutConnStrLen, SQL_DRIVER_COMPLETE);
	
#ifdef DEBUG
	if (SQL_SUCCEEDED(ret)) 
		elog(NOTICE, "Successfully connected to driver");
	else {
		extract_error("SQLDriverConnect", dbc, SQL_HANDLE_DBC);
	}
#endif
	
	/* Allocate a statement handle */
	SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
	
	initStringInfo(&sql_str);
	appendStringInfo(&sql_str, "SELECT COUNT(*) FROM `%s`.`%s`", svr_database, svr_table);
	
	ret = SQLExecDirect(stmt, (SQLCHAR *) sql_str.data, SQL_NTS);
	if (SQL_SUCCEEDED(ret)) {
		SQLFetch(stmt);
		/* retrieve column data as a big int */
		ret = SQLGetData(stmt, 1, SQL_C_UBIGINT, &table_size, 0, &indicator);
		if (SQL_SUCCEEDED(ret)) {
			*size = (unsigned int) table_size;
		}
	}
	else {
		elog(NOTICE, "Opps!");
	}
	
	/* Free handles, and disconnect */   
	if (stmt) { 
		SQLFreeHandle(SQL_HANDLE_STMT, stmt);
		stmt = NULL; 
	}
	if (dbc) { 
		SQLFreeHandle(SQL_HANDLE_DBC, dbc);
		dbc = NULL; 
	}
	if (env) { 
		SQLFreeHandle(SQL_HANDLE_ENV, env);
		env = NULL; 
	}
	if (dbc)
		SQLDisconnect(dbc);
#ifdef DEBUG
	elog(NOTICE, "Count:   %u", *size);
#endif
}

/*
 * get quals in the select if there is one
 */
static void
odbcGetQual(Node *node, TupleDesc tupdesc, List *col_mapping_list, char **key, char **value, bool *pushdown)
{
    ListCell *col_mapping;
    *key = NULL;
    *value = NULL;
    *pushdown = false;
#ifdef DEBUG	
    elog(NOTICE, "odbcGetQual");
#endif
    if (!node)
        return;
	
    if (IsA(node, OpExpr))
    {
        OpExpr	*op = (OpExpr *) node;
        Node	*left, *right;
        Index	varattno;
		
        if (list_length(op->args) != 2)
            return;

        left = list_nth(op->args, 0);
		
        if (!IsA(left, Var))
            return;
		
        varattno = ((Var *) left)->varattno;
		
        right = list_nth(op->args, 1);
		
        if (IsA(right, Const))
        {
            StringInfoData  buf;
            initStringInfo(&buf);
            /* And get the column and value... */
            *key = NameStr(tupdesc->attrs[varattno - 1]->attname);
#ifdef DEBUG
			elog(NOTICE, "constant type: %u", ((Const *) right)->consttype);
#endif
			if (((Const *) right)->consttype == PROCID_TEXTCONST)
				*value = TextDatumGetCString(((Const *) right)->constvalue);
			else {
				return;
			}
			
            /* convert qual keys to mapped couchdb attribute name */
            foreach(col_mapping, col_mapping_list)
            {
                DefElem *def = (DefElem *) lfirst(col_mapping);
                if (strcmp(def->defname, *key) == 0)
                {
                    *key = defGetString(def);
                    break;
                }
            }
			
            /*
             * We can push down this qual if:
             * - The operatory is TEXTEQ
             * - The qual is on the _id column (in addition, _rev column can be also valid)
             */
			
            if (op->opfuncid == PROCID_TEXTEQ)
                *pushdown = true;
#ifdef DEBUG
            elog(NOTICE, "Got qual %s = %s", *key, *value);
#endif
            return;
        }
    }
	
    return;
}



/*
 * Check if the provided option is one of the valid options.
 * context is the Oid of the catalog holding the object the option is for.
 */
static bool
odbcIsValidOption(const char *option, Oid context)
{
    struct odbcFdwOption *opt;

#ifdef DEBUG
    elog(NOTICE, "odbcIsValidOption");
#endif
	
    /* Check if the options presents in the valid option list */
    for (opt = valid_options; opt->optname; opt++)
    {
        if (context == opt->optcontext && strcmp(opt->optname, option) == 0)
            return true;
    }
	
    /* Foreign table may have anything as a mapping option */
    if (context == ForeignTableRelationId)
        return true;
    else
        return false;
}



/*
 * odbcPlanForeignScan
 *		Create a FdwPlan for a scan on the foreign table
 */
static FdwPlan *
odbcPlanForeignScan(Oid foreigntableid, PlannerInfo *root, RelOptInfo *baserel)
{
	FdwPlan	*fdwplan;
	unsigned int table_size	= 0;
	char *svr_dsn			= NULL;
	char *svr_database		= NULL;
	char *svr_table			= NULL;
	char *username			= NULL;
	char *password			= NULL;
	List *col_mapping_list;

#ifdef DEBUG
    elog(NOTICE, "odbcPlanForeignScan");
#endif
	
	/* Fetch the foreign table options */
	odbcGetOptions(foreigntableid, &svr_dsn, &svr_database, &svr_table, 
				   &username, &password, &col_mapping_list);
	
	fdwplan = makeNode(FdwPlan);
	fdwplan->startup_cost = 10;
    fdwplan->total_cost = 100 + fdwplan->startup_cost;
    fdwplan->fdw_private = NIL;	/* not used */

#ifdef DEBUG
	elog(NOTICE, "new total cost: %f", fdwplan->total_cost);
#endif
	
	odbcGetTableSize(svr_dsn, svr_database, svr_table, username, password, &table_size);
	
	fdwplan->total_cost = fdwplan->total_cost + table_size;
#ifdef DEBUG
	elog(NOTICE, "new total cost: %f", fdwplan->total_cost);
#endif
    return fdwplan;
}






/*
 * odbcBeginForeignScan
 *
 */
static void
odbcBeginForeignScan(ForeignScanState *node, int eflags)
{
	SQLHENV env;
	SQLHDBC dbc;
	odbcFdwExecutionState	*festate;
	SQLSMALLINT result_columns;
	SQLHSTMT stmt;
	SQLRETURN ret;
	SQLCHAR OutConnStr[1024];
	SQLSMALLINT OutConnStrLen;
	
#ifdef DEBUG
	char dsn[256];
	char desc[256];
	SQLSMALLINT dsn_ret;
	SQLSMALLINT desc_ret;
	SQLUSMALLINT direction;
#endif
	
	char *svr_dsn		= NULL;
	char *svr_database	= NULL;
	char *svr_table	= NULL;
	char *username		= NULL;
	char *password		= NULL;
	
	StringInfoData conn_str;
	
	Relation rel;
	int		num_of_columns;
	StringInfoData	*columns;
	int i;
	ListCell	*col_mapping;
	List	*col_mapping_list;
	StringInfoData	sql;
	StringInfoData	col_str;
	
	char	*qual_key = NULL;
	char	*qual_value = NULL;
	bool	pushdown = FALSE;
	
#ifdef DEBUG	
	elog(NOTICE, "odbcBeginForeignScan");
#endif
	
	/* Fetch the foreign table options */
	odbcGetOptions(RelationGetRelid(node->ss.ss_currentRelation), &svr_dsn, &svr_database, &svr_table, 
				   &username, &password, &col_mapping_list);
#ifdef DEBUG	
	elog(NOTICE, "dsn: %s", svr_dsn);
	elog(NOTICE, "db: %s", svr_database);
	elog(NOTICE, "table: %s", svr_table);
	elog(NOTICE, "username: %s", username);
	elog(NOTICE, "password: %s", password);
#endif
	initStringInfo(&conn_str);
	appendStringInfo(&conn_str, "DSN=%s;DATABASE=%s;UID=%s;PWD=%s;", svr_dsn, svr_database, username, password);
	
#ifdef DEBUG
	elog(NOTICE, "connection string: %s", conn_str.data);
#endif
	
    /* Allocate an environment handle */
	SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
	/* We want ODBC 3 support */
	SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void *) SQL_OV_ODBC3, 0);
	
	
#ifdef DEBUG
	/* Print DSNs available: for debugging purposes */
	direction = SQL_FETCH_FIRST;
	while(SQL_SUCCEEDED(ret = SQLDataSources(env, direction,
											 (SQLCHAR *) dsn, sizeof(dsn), &dsn_ret,
											 (SQLCHAR *) desc, sizeof(desc), &desc_ret))) {
		direction = SQL_FETCH_NEXT;
		elog(NOTICE, "%s - %s", dsn, desc);
		if (ret == SQL_SUCCESS_WITH_INFO) elog(NOTICE, "data truncation");
	}
#endif
	
	/* Allocate a connection handle */
	SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
	/* Connect to the DSN */
	ret = SQLDriverConnect(dbc, NULL, (SQLCHAR *) conn_str.data, SQL_NTS,
					 OutConnStr, 1024, &OutConnStrLen, SQL_DRIVER_COMPLETE);

#ifdef DEBUG
	if (SQL_SUCCEEDED(ret)) 
		elog(NOTICE, "Successfully connected to driver");
	else {
		extract_error("SQLDriverConnect", dbc, SQL_HANDLE_DBC);
	}
#endif
	
	/* Fetch the table column info */
    rel = heap_open(RelationGetRelid(node->ss.ss_currentRelation), AccessShareLock);
    num_of_columns = rel->rd_att->natts;
    columns = (StringInfoData *) palloc(sizeof(StringInfoData) * num_of_columns);
	initStringInfo(&col_str);
    for (i = 0; i < num_of_columns; i++)
    {
        StringInfoData col;
        StringInfoData mapping;
        bool	mapped;
		
        /* retrieve the column name */
        initStringInfo(&col);
        appendStringInfo(&col, "%s", NameStr(rel->rd_att->attrs[i]->attname));
        mapped = FALSE;
		
        /* check if the column name is mapping to a different name in remote table */
        foreach(col_mapping, col_mapping_list)
        {
            DefElem *def = (DefElem *) lfirst(col_mapping);
            if (strcmp(def->defname, col.data) == 0)
            {
                initStringInfo(&mapping);
                appendStringInfo(&mapping, "%s", defGetString(def));
                mapped = TRUE;
                break;
            }
        }
		
        /* decide which name is going to be used */
        if (mapped)
            columns[i] = mapping;
        else
            columns[i] = col;
		appendStringInfo(&col_str, i == 0 ? "`%s`" : ",`%s`", columns[i].data);
    }
    heap_close(rel, NoLock);

#ifdef DEBUG
	/* print out the actual column names in remote table for debug only*/
	for (i = 0; i < num_of_columns; i++) {
		elog(NOTICE, "Column Mapping %i: %s", i, columns[i].data);
	}
	elog(NOTICE, "Column String: %s", col_str.data);
#endif
	
	/* See if we've got a qual we can push down */
	if (node->ss.ps.plan->qual)
	{
		ListCell	*lc;
		
		foreach (lc, node->ss.ps.qual)
		{
			/* Only the first qual can be pushed down to remote DBMS */
			ExprState  *state = lfirst(lc);
			
			odbcGetQual((Node *) state->expr, node->ss.ss_currentRelation->rd_att, col_mapping_list, &qual_key, &qual_value, &pushdown);
			if (pushdown)
				break;
		}
	}
	
	/* Construct the SQL statement used for remote querying */
	initStringInfo(&sql);
	if (pushdown) {
		appendStringInfo(&sql, "SELECT %s FROM `%s`.`%s` WHERE `%s` = '%s'", 
						 col_str.data, svr_database, svr_table, qual_key, qual_value);
	}
	else {
		appendStringInfo(&sql, "SELECT %s FROM `%s`.`%s`", col_str.data, svr_database, svr_table);
	}
	
#ifdef DEBUG
	elog(NOTICE, "SQL: %s", sql.data);
#endif
	
	/* Allocate a statement handle */
	SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
	/* Retrieve a list of rows */
	SQLExecDirect(stmt, (SQLCHAR *) sql.data, SQL_NTS);
	SQLNumResultCols(stmt, &result_columns);
	
#ifdef DEBUG
	elog(NOTICE, "num of columns (begin): %i", (int) result_columns);
#endif
	
	festate = (odbcFdwExecutionState *) palloc(sizeof(odbcFdwExecutionState));
	festate->attinmeta = TupleDescGetAttInMetadata(node->ss.ss_currentRelation->rd_att);
	festate->svr_dsn = svr_dsn;
	festate->svr_database = svr_database;
	festate->svr_table = svr_table;
	festate->svr_username = username;
	festate->svr_password = password;
	festate->stmt = stmt;
	node->fdw_state = (void *) festate;
}



/*
 * odbcIterateForeignScan
 *
 */
static TupleTableSlot *
odbcIterateForeignScan(ForeignScanState *node)
{
	/* ODBC API return status */
	SQLRETURN ret; 
	odbcFdwExecutionState *festate = (odbcFdwExecutionState *) node->fdw_state;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	SQLSMALLINT columns;
	char	**values;
	HeapTuple	tuple;
	StringInfoData	col_data;
	SQLHSTMT stmt = festate->stmt;

#ifdef DEBUG
	elog(NOTICE, "odbcIterateForeignScan");
#endif
	
	ret = SQLFetch(stmt);
	
	SQLNumResultCols(stmt, &columns);
	
#ifdef DEBUG
	elog(NOTICE, "num of columns (iterate): %i", (int) columns);
#endif
	
	ExecClearTuple(slot);
	
	if (SQL_SUCCEEDED(ret)) {
		SQLSMALLINT i;
		values = (char **) palloc(sizeof(char *) * columns);
		/* Loop through the columns */
		for (i = 1; i <= columns; i++) {
			SQLLEN indicator;
			char buf[1024];

			/* retrieve column data as a string */
			ret = SQLGetData(stmt, i, SQL_C_CHAR,
							 buf, sizeof(buf), &indicator);
			
			if (SQL_SUCCEEDED(ret)) {
				
				/* Handle null columns */
				if (indicator == SQL_NULL_DATA) strcpy(buf, "NULL");
				initStringInfo(&col_data);
				appendStringInfoString (&col_data, buf);
				values[i-1] = col_data.data;
#ifdef DEBUG
				elog(NOTICE, "values[%i] = %s", i-1, values[i-1]);
#endif
			}
		}
		
		tuple = BuildTupleFromCStrings(festate->attinmeta, values);
		ExecStoreTuple(tuple, slot, InvalidBuffer, false);
	}
#ifdef DEBUG
	elog(NOTICE, "iterate end");
#endif
	
	return slot;
}



/*
 * odbcExplainForeignScan
 *
 */
static void
odbcExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
	odbcFdwExecutionState *festate;
	unsigned int table_size = 0;
	
#ifdef DEBUG
	elog(NOTICE, "odbcExplainForeignScan");
#endif
	
	festate = (odbcFdwExecutionState *) node->fdw_state;
	
	odbcGetTableSize(festate->svr_dsn, festate->svr_database, festate->svr_table, 
					 festate->svr_username, festate->svr_password, &table_size);
	
	/* Suppress file size if we're not showing cost details */
	if (es->costs)
	{
		ExplainPropertyLong("Foreign Table Size", table_size, es);
	}
}

/*
 * odbcEndForeignScan
 *		Finish scanning foreign table and dispose objects used for this scan
 */
static void
odbcEndForeignScan(ForeignScanState *node)
{
	odbcFdwExecutionState *festate;

#ifdef DEBUG
	elog(NOTICE, "odbcEndForeignScan");
#endif
	
	/* if festate is NULL, we are in EXPLAIN; nothing to do */
	festate = (odbcFdwExecutionState *) node->fdw_state;
	
	if (festate)
	{
		if (festate->stmt) {
			SQLFreeHandle(SQL_HANDLE_STMT, festate->stmt);
			festate->stmt = NULL;
		}
	}
}

/*
 * odbcReScanForeignScan
 *		Rescan table, possibly with new parameters
 */
static void
odbcReScanForeignScan(ForeignScanState *node)
{
#ifdef DEBUG
    elog(NOTICE, "odbcReScanForeignScan");
#endif
}