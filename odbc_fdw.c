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


typedef struct odbcFdwExecutionState
{
	AttInMetadata	*attinmeta;
	SQLHENV	*env;
	SQLHDBC	*dbc;
	SQLHSTMT	*stmt;
} odbcFdwExecutionState;

struct odbcFdwOption
{
	const char	*optname;
	Oid		optcontext;	/* Oid of catalog in which option may appear */
};

/*
 * Array of valid options, the rest will be assumed as column->json attribute mapping.
 */
static struct odbcFdwOption valid_options[] =
{
    /* Foreign server options */
    { "dsn",    ForeignServerRelationId },

    /* Foreign table options */
    { "database",   ForeignTableRelationId },

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

    PG_RETURN_VOID();
}


/*
 * odbcPlanForeignScan
 *		Create a FdwPlan for a scan on the foreign table
 */
static FdwPlan *
odbcPlanForeignScan(Oid foreigntableid, PlannerInfo *root, RelOptInfo *baserel)
{
	elog(NOTICE, "odbcPlanForeignScan");
	
    FdwPlan	*fdwplan;
    
    fdwplan = makeNode(FdwPlan);

    return fdwplan;
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
            elog(NOTICE, "%s:%ld:%ld:%s\n", state, i, native, text);
    }
    while( ret == SQL_SUCCESS );
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
	SQLHSTMT stmt;
	odbcFdwExecutionState	*festate;
	SQLSMALLINT columns;
	
	elog(NOTICE, "odbcBeginForeignScan");
	
    /* Allocate an environment handle */
	SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
	/* We want ODBC 3 support */
	SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void *) SQL_OV_ODBC3, 0);
	/* Allocate a connection handle */
	SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
	/* Connect to the DSN mydsn */
	/* You will need to change mydsn to one you have created and tested */
	SQLRETURN ret;
	ret = SQLDriverConnect(dbc, NULL, "DSN=myodbc;", SQL_NTS,
					 NULL, 0, NULL, SQL_DRIVER_COMPLETE);
	
	if (SQL_SUCCEEDED(ret)) 
		elog(NOTICE, "success connected to driver");
	else {
		extract_error("SQLDriverConnect", dbc, SQL_HANDLE_DBC);
	}

	
	
	/* Allocate a statement handle */
	SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
	/* Retrieve a list of rows */
	SQLExecDirect(stmt, "USE test", SQL_NTS);
	SQLExecDirect(stmt, "SELECT * FROM mytable", SQL_NTS);
	SQLNumResultCols(stmt, &columns);
	elog(NOTICE, "num of column (begin): %i", (int) columns);
	SQLFetch(stmt);
	
	festate = (odbcFdwExecutionState *) palloc(sizeof(odbcFdwExecutionState));
	festate->attinmeta = TupleDescGetAttInMetadata(node->ss.ss_currentRelation->rd_att);
	festate->stmt = &stmt;
	elog(NOTICE, "STMT (begin): %p", &stmt);
	festate->dbc = &dbc;
	festate->env = &env;
	node->fdw_state = (void *) festate;
}



/*
 * odbcIterateForeignScan
 *
 */
static TupleTableSlot *
odbcIterateForeignScan(ForeignScanState *node)
{
	SQLRETURN ret; /* ODBC API return status */
	odbcFdwExecutionState *festate = (odbcFdwExecutionState *) node->fdw_state;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	SQLSMALLINT columns;
	char	**values;
	HeapTuple	tuple;
	SQLHSTMT *stmt = festate->stmt;
	
	elog(NOTICE, "odbcIterateForeignScan");
	elog(NOTICE, "STMT (iterate): %p", stmt);
	ret = SQLFetch(*stmt);
	
	
	
	if (ret==SQL_SUCCESS) {
		elog(NOTICE, "fetched");
	}
	else {
		extract_error("SQLFetch", *stmt, SQL_HANDLE_STMT);
	}


	
	ExecClearTuple(slot);
	
	if (SQL_SUCCEEDED(ret)) {
		SQLUSMALLINT i;
		values = (char **) palloc(sizeof(char *) * columns);
		/* Loop through the columns */
		for (i = 1; i <= columns; i++) {
			SQLINTEGER indicator;
			char buf[512];
			/* retrieve column data as a string */
			ret = SQLGetData(*stmt, i, SQL_C_CHAR,
							 buf, sizeof(buf), &indicator);
			
			if (SQL_SUCCEEDED(ret)) {
				
				/* Handle null columns */
				if (indicator == SQL_NULL_DATA) strcpy(buf, "NULL");
				values[i-1] = buf;
			}
		}
		tuple = BuildTupleFromCStrings(festate->attinmeta, values);
		ExecStoreTuple(tuple, slot, InvalidBuffer, false);
	}
	
	return slot;
}



/*
 * odbcExplainForeignScan
 *
 */
static void
odbcExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
	elog(NOTICE, "odbcExplainForeignScan");
}

/*
 * odbcEndForeignScan
 *		Finish scanning foreign table and dispose objects used for this scan
 */
static void
odbcEndForeignScan(ForeignScanState *node)
{
    elog(NOTICE, "odbcEndForeignScan");
}

/*
 * odbcReScanForeignScan
 *		Rescan table, possibly with new parameters
 */
static void
odbcReScanForeignScan(ForeignScanState *node)
{
    elog(NOTICE, "odbcReScanForeignScan");
}