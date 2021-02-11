/*----------------------------------------------------------
 *
 *          foreign-data wrapper for ODBC
 *
 * Copyright (c) 2011, PostgreSQL Global Development Group
 *
 * This software is released under the PostgreSQL Licence.
 *
 * Author: Zheng Yang <zhengyang4k@gmail.com>
 * Updated to 9.2+ by Gunnar "Nick" Bluth <nick@pro-open.de>
 *	 based on tds_fdw code from Geoff Montee
 *
 * IDENTIFICATION
 *        odbc_fdw/odbc_fdw.c
 *
 *----------------------------------------------------------
 */

/* Debug mode flag */
/*
#define DEBUG
*/
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
#include "mb/pg_wchar.h"
#include "optimizer/cost.h"
#include "storage/fd.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/rel.h"

#if (PG_VERSION_NUM >= 90200)
#include "optimizer/pathnode.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/planmain.h"
#endif


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
    char			*svr_schema;
    char			*svr_table;
    char			*svr_username;
    char			*svr_password;
    SQLHSTMT		stmt;
    int				num_of_result_cols;
    int				num_of_table_cols;
    StringInfoData	*table_columns;
    bool			first_iteration;
    List			*col_position_mask;
    List			*col_size_array;
    char			*sql_count;
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
    { "schema",		ForeignTableRelationId },
    { "table",		ForeignTableRelationId },
    { "sql_query",	ForeignTableRelationId },
    { "sql_count",	ForeignTableRelationId },

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
static void odbcExplainForeignScan(ForeignScanState *node, ExplainState *es);
static void odbcBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *odbcIterateForeignScan(ForeignScanState *node);
static void odbcReScanForeignScan(ForeignScanState *node);
static void odbcEndForeignScan(ForeignScanState *node);
/* routines for 9.2.0+ */
#if (PG_VERSION_NUM >= 90200)
static void odbcGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
static void odbcEstimateCosts(PlannerInfo *root, RelOptInfo *baserel, Cost *startup_cost, Cost *total_cost, Oid foreigntableid);
static void odbcGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
static bool odbcAnalyzeForeignTable(Relation relation, AcquireSampleRowsFunc *func, BlockNumber *totalpages);
static ForeignScan* odbcGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid, ForeignPath *best_path, List *tlist, List *scan_clauses);
/* routines for versions older than 9.2.0 */
#else
static FdwPlan *odbcPlanForeignScan(Oid foreigntableid, PlannerInfo *root, RelOptInfo *baserel);
#endif




/*
 * helper functions
 */
static bool odbcIsValidOption(const char *option, Oid context);

Datum
odbc_fdw_handler(PG_FUNCTION_ARGS)
{
    FdwRoutine *fdwroutine = makeNode(FdwRoutine);
	/* FIXME */
	#if (PG_VERSION_NUM >= 90200)
	fdwroutine->GetForeignRelSize = odbcGetForeignRelSize;
	fdwroutine->GetForeignPaths = odbcGetForeignPaths;
	fdwroutine->AnalyzeForeignTable = odbcAnalyzeForeignTable;
	fdwroutine->GetForeignPlan = odbcGetForeignPlan;
	#else
    fdwroutine->PlanForeignScan = odbcPlanForeignScan;
	#endif
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
    char	*svr_schema = NULL;
    char	*svr_table = NULL;
    char	*sql_query = NULL;
    char	*sql_count = NULL;
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
        else if (strcmp(def->defname, "schema") == 0)
        {
            if (svr_schema)
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("conflicting or redundant options: schema (%s)", defGetString(def))
                        ));
            svr_schema = defGetString(def);
        }
        else if (strcmp(def->defname, "table") == 0)
        {
            if (svr_table)
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("conflicting or redundant options: table (%s)", defGetString(def))
                        ));

            svr_table = defGetString(def);
        }
        else if (strcmp(def->defname, "sql_query") == 0)
        {
            if (sql_query)
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("conflicting or redundant options: sql_query (%s)", defGetString(def))
                        ));

            sql_query = defGetString(def);
        }
        else if (strcmp(def->defname, "sql_count") == 0)
        {
            if (sql_count)
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("conflicting or redundant options: sql_count (%s)", defGetString(def))
                        ));

            sql_count = defGetString(def);
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
odbcGetOptions(Oid foreigntableid, char **svr_dsn, char **svr_database, char **svr_schema, char ** svr_table, char ** sql_query,
               char **sql_count, char **username, char **password, List **mapping_list)
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

        if (strcmp(def->defname, "schema") == 0)
        {
            *svr_schema = defGetString(def);
            continue;
        }

        if (strcmp(def->defname, "table") == 0)
        {
            *svr_table = defGetString(def);
            continue;
        }
        if (strcmp(def->defname, "sql_query") == 0)
        {
            *sql_query = defGetString(def);
            continue;
        }
        if (strcmp(def->defname, "sql_count") == 0)
        {
            *sql_count = defGetString(def);
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

    if (!*svr_schema)
        *svr_schema = NULL;

    if (!*svr_table)
        *svr_table = NULL;

    if (!*sql_query)
    {
        *sql_query = NULL;
    }

    if (!*sql_count)
    {
        *sql_count = NULL;
    }

    if (!*username)
        *username = NULL;

    if (!*password)
        *password = NULL;
}

#ifdef DEBUG
void static extract_error(char *fn,
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
#endif


/*
 * Get name qualifier char
 */
static void
getNameQualifierChar(SQLHDBC dbc, StringInfoData *nq_char)
{
    SQLCHAR name_qualifier_char[2];

#ifdef DUBUG
    elog(NOTICE, "getNameQualifierChar");
#endif

    SQLGetInfo(dbc,
               SQL_QUALIFIER_NAME_SEPARATOR,
               (SQLPOINTER)&name_qualifier_char,
               2,
               NULL);
    initStringInfo(nq_char);
    appendStringInfo(nq_char, "%s", (char *) name_qualifier_char);
}


/*
 * Get quote cahr
 */
static void
getQuoteChar(SQLHDBC dbc, StringInfoData *q_char)
{
    SQLCHAR quote_char[2];
#ifdef DEBUG
    elog(NOTICE, "getQuoteChar");
#endif

    SQLGetInfo(dbc,
               SQL_IDENTIFIER_QUOTE_CHAR,
               (SQLPOINTER)&quote_char,
               2,
               NULL);

    initStringInfo(q_char);
    appendStringInfo(q_char, "%s", (char *) quote_char);
}


/*
 * get table size of a table
 */
static void
odbcGetTableSize(char *svr_dsn, char *svr_database, char *svr_schema, char *svr_table,
                 char *username, char *password, char *sql_count, unsigned int *size)
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

    StringInfoData name_qualifier_char;
    StringInfoData quote_char;

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
    else
    {
        extract_error("SQLDriverConnect", dbc, SQL_HANDLE_DBC);
    }
#endif

    /* Allocate a statement handle */
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);

    if (sql_count == NULL)
    {
        /* Get quote char */
        getQuoteChar(dbc, &quote_char);

        /* Get name qualifier char */
        getNameQualifierChar(dbc, &name_qualifier_char);

        initStringInfo(&sql_str);
        appendStringInfo(&sql_str, "SELECT COUNT(*) FROM %s%s%s%s%s%s%s",
                         quote_char.data, svr_schema, quote_char.data,
                         name_qualifier_char.data,
                         quote_char.data, svr_table, quote_char.data);
    }
    else
    {
        initStringInfo(&sql_str);
        appendStringInfo(&sql_str, "%s", sql_count);
    }

#ifdef DEBUG
    elog(NOTICE, "%s", sql_str.data);
#endif
    ret = SQLExecDirect(stmt, (SQLCHAR *) sql_str.data, SQL_NTS);
    if (SQL_SUCCEEDED(ret))
    {
        SQLFetch(stmt);
        /* retrieve column data as a big int */
        ret = SQLGetData(stmt, 1, SQL_C_UBIGINT, &table_size, 0, &indicator);
        if (SQL_SUCCEEDED(ret))
        {
            *size = (unsigned int) table_size;
        }
    }
    else
    {
        elog(NOTICE, "Opps!");
    }

    /* Free handles, and disconnect */
    if (stmt)
    {
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        stmt = NULL;
    }
    if (dbc)
    {
        SQLFreeHandle(SQL_HANDLE_DBC, dbc);
        dbc = NULL;
    }
    if (env)
    {
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
            else
            {
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


/* routines for 9.2.0+ */
#if (PG_VERSION_NUM >= 90200)

static void odbcGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
    unsigned int table_size	= 0;
    char *svr_dsn			= NULL;
    char *svr_database		= NULL;
    char *svr_schema		= NULL;
    char *svr_table			= NULL;
    char *sql_query			= NULL;
    char *sql_count			= NULL;
    char *username			= NULL;
    char *password			= NULL;
    List *col_mapping_list;

#ifdef DEBUG
    elog(NOTICE, "odbcGetForeignRelSize");
#endif

    /* Fetch the foreign table options */
    odbcGetOptions(foreigntableid, &svr_dsn, &svr_database, &svr_schema, &svr_table, &sql_query,
                   &sql_count, &username, &password, &col_mapping_list);

    odbcGetTableSize(svr_dsn, svr_database, svr_schema, svr_table, username, password, sql_count, &table_size);

    baserel->rows = table_size;
    baserel->tuples = baserel->rows;
}

static void odbcEstimateCosts(PlannerInfo *root, RelOptInfo *baserel, Cost *startup_cost, Cost *total_cost, Oid foreigntableid)
{
    unsigned int table_size	= 0;
    char *svr_dsn			= NULL;
    char *svr_database		= NULL;
    char *svr_schema		= NULL;
    char *svr_table			= NULL;
    char *sql_query			= NULL;
    char *sql_count			= NULL;
    char *username			= NULL;
    char *password			= NULL;
    List *col_mapping_list;

#ifdef DEBUG
    elog(NOTICE, "odbcEstimateCosts");
#endif

    /* Fetch the foreign table options */
    odbcGetOptions(foreigntableid, &svr_dsn, &svr_database, &svr_schema, &svr_table, &sql_query,
                   &sql_count, &username, &password, &col_mapping_list);

    odbcGetTableSize(svr_dsn, svr_database, svr_schema, svr_table, username, password, sql_count, &table_size);
	
	*startup_cost = 25;
		
	*total_cost = baserel->rows + *startup_cost;
	
	#ifdef DEBUG
		ereport(NOTICE,
			(errmsg("----> finishing odbcEstimateCosts")
			));
	#endif
}

static void odbcGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
	Cost startup_cost;
	Cost total_cost;
	
	#ifdef DEBUG
		ereport(NOTICE,
			(errmsg("----> starting odbcGetForeignPaths")
			));
	#endif
	
	odbcEstimateCosts(root, baserel, &startup_cost, &total_cost, foreigntableid);
	
	add_path(baserel, 
		(Path *) create_foreignscan_path(root, baserel, baserel->rows, startup_cost, total_cost,
			NIL, NULL, NIL));
	
	#ifdef DEBUG
		ereport(NOTICE,
			(errmsg("----> finishing odbcGetForeignPaths")
			));
	#endif
}

static bool odbcAnalyzeForeignTable(Relation relation, AcquireSampleRowsFunc *func, BlockNumber *totalpages)
{
	#ifdef DEBUG
		ereport(NOTICE,
			(errmsg("----> starting odbcAnalyzeForeignTable")
			));
	#endif
	
	#ifdef DEBUG
		ereport(NOTICE,
			(errmsg("----> finishing odbcAnalyzeForeignTable")
			));
	#endif
	
	return false;
}

static ForeignScan* odbcGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, 
	Oid foreigntableid, ForeignPath *best_path, List *tlist, List *scan_clauses)
{
	Index scan_relid = baserel->relid;
	#ifdef DEBUG
		ereport(NOTICE,
			(errmsg("----> starting odbcGetForeignPlan")
			));
	#endif
	
	scan_clauses = extract_actual_clauses(scan_clauses, false);
	
	#ifdef DEBUG
		ereport(NOTICE,
			(errmsg("----> finishing odbcGetForeignPlan")
			));
	#endif
	
	return make_foreignscan(tlist, scan_clauses, scan_relid, NIL, NIL);
}

/* routines for versions older than 9.2.0 */
#else

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
    char *svr_schema		= NULL;
    char *svr_table			= NULL;
    char *sql_query			= NULL;
    char *sql_count			= NULL;
    char *username			= NULL;
    char *password			= NULL;
    List *col_mapping_list;

#ifdef DEBUG
    elog(NOTICE, "odbcPlanForeignScan");
#endif

    /* Fetch the foreign table options */
    odbcGetOptions(foreigntableid, &svr_dsn, &svr_database, &svr_schema, &svr_table, &sql_query,
                   &sql_count, &username, &password, &col_mapping_list);

    fdwplan = makeNode(FdwPlan);
    fdwplan->startup_cost = 10;
    fdwplan->total_cost = 100 + fdwplan->startup_cost;
    fdwplan->fdw_private = NIL;	/* not used */

#ifdef DEBUG
    elog(NOTICE, "new total cost: %f", fdwplan->total_cost);
#endif

    odbcGetTableSize(svr_dsn, svr_database, svr_schema, svr_table, username, password, sql_count, &table_size);

    fdwplan->total_cost = fdwplan->total_cost + table_size;
#ifdef DEBUG
    elog(NOTICE, "new total cost: %f", fdwplan->total_cost);
#endif
    return fdwplan;
}

#endif

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
    char *svr_schema	= NULL;
    char *svr_table		= NULL;
    char *sql_query		= NULL;
    char *sql_count		= NULL;
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
    SQLCHAR quote_char[2];
    SQLCHAR name_qualifier_char[2];

    char	*qual_key = NULL;
    char	*qual_value = NULL;
    bool	pushdown = FALSE;

#ifdef DEBUG
    elog(NOTICE, "odbcBeginForeignScan");
#endif

    /* Fetch the foreign table options */
    odbcGetOptions(RelationGetRelid(node->ss.ss_currentRelation), &svr_dsn, &svr_database, &svr_schema, &svr_table, &sql_query,
                   &sql_count, &username, &password, &col_mapping_list);
#ifdef DEBUG
    elog(NOTICE, "dsn: %s", svr_dsn);
    elog(NOTICE, "db: %s", svr_database);
    elog(NOTICE, "schema: %s", svr_schema);
    elog(NOTICE, "table: %s", svr_table);
    elog(NOTICE, "sql_query: %s", sql_query);
    elog(NOTICE, "sql_count: %s", sql_count);
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
                              (SQLCHAR *) desc, sizeof(desc), &desc_ret)))
    {
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
    else
    {
        extract_error("SQLDriverConnect", dbc, SQL_HANDLE_DBC);
    }
#endif

    /* Getting the Quote char */
    SQLGetInfo(dbc,
               SQL_IDENTIFIER_QUOTE_CHAR,
               (SQLPOINTER)&quote_char,
               2,
               NULL);


    /* Getting the Qualifier name separator */
    SQLGetInfo(dbc,
               SQL_QUALIFIER_NAME_SEPARATOR,
               (SQLPOINTER)&name_qualifier_char,
               2,
               NULL);

#ifdef DEBUG
    elog(NOTICE, "QUOTE CHAR: %s", quote_char);
    elog(NOTICE, "SQL_QUALIFIER_NAME_SEPARATOR: %s", name_qualifier_char);
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
        appendStringInfo(&col_str, i == 0 ? "%s%s%s" : ",%s%s%s", (char *) quote_char, columns[i].data, (char *) quote_char);
    }
    heap_close(rel, NoLock);


#ifdef DEBUG
    /* print out the actual column names in remote table for debug only*/
    for (i = 0; i < num_of_columns; i++)
    {
        elog(NOTICE, "Column Mapping %i: %s", i, columns[i].data);
    }
    elog(NOTICE, "Column String: %s", col_str.data);
    elog(NOTICE, "Experiment: ");

    /*
    // SUBSTRING supported
    if (fFuncs & SQL_FN_STR_CHAR_LENGTH)
    	elog(NOTICE, "HAS COUNT!");   // do something

    // SUBSTRING not supported
    else
    	elog(NOTICE, "DUN HAVE!");   // do something else
     */
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
    if (pushdown)
    {
        appendStringInfo(&sql, "SELECT %s FROM `%s`.`%s` WHERE `%s` = '%s'",
                         col_str.data, svr_database, svr_table, qual_key, qual_value);
    }
    else
    {
        /* Use custom query if it's available */
        if (sql_query)
        {
            appendStringInfo(&sql, "%s", sql_query);
        }
        else
        {
            appendStringInfo(&sql, "SELECT %s FROM %s%s%s%s%s%s%s", col_str.data,
                             (char *) quote_char, svr_schema, (char *) quote_char,
                             (char *) name_qualifier_char, (char *) quote_char, svr_table, (char *) quote_char);
        }
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
    festate->svr_schema = svr_schema;
    festate->svr_table = svr_table;
    festate->svr_username = username;
    festate->svr_password = password;
    festate->stmt = stmt;
    festate->table_columns = columns;
    festate->num_of_table_cols = num_of_columns;
    /* prepare for the first iteration, there will be some precalculation needed in the first iteration*/
    festate->first_iteration = TRUE;
    festate->sql_count = sql_count;
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
    bool first_iteration = festate->first_iteration;
    int num_of_table_cols = festate->num_of_table_cols;
    int num_of_result_cols;
    StringInfoData	*table_columns = festate->table_columns;
    List *col_position_mask = NIL;
    List *col_size_array = NIL;

#ifdef DEBUG
    elog(NOTICE, "odbcIterateForeignScan");
#endif

    ret = SQLFetch(stmt);

    SQLNumResultCols(stmt, &columns);

#ifdef DEBUG
    elog(NOTICE, "num of columns (iterate): %i", (int) columns);
#endif

    /*
     * If this is the first iteration,
     * we need to calculate the mask for column mapping as well as the column size
     */
    if (first_iteration == TRUE)
    {
        SQLCHAR *ColumnName;
        SQLSMALLINT NameLengthPtr;
        SQLSMALLINT DataTypePtr;
        SQLULEN		ColumnSizePtr;
        SQLSMALLINT	DecimalDigitsPtr;
        SQLSMALLINT	NullablePtr;
        int i;
        int k;
        bool found = FALSE;

        /* Allocate memory for the masks */
        col_position_mask = NIL;
        col_size_array = NIL;
        num_of_result_cols = columns;
        /* Obtain the column information of the first row. */
        for (i = 1; i <= columns; i++)
        {
            ColumnName = (SQLCHAR *) palloc(sizeof(SQLCHAR) * 255);
            SQLDescribeCol(stmt,
                           i,						/* ColumnName */
                           ColumnName,
                           sizeof(SQLCHAR) * 255,	/* BufferLength */
                           &NameLengthPtr,
                           &DataTypePtr,
                           &ColumnSizePtr,
                           &DecimalDigitsPtr,
                           &NullablePtr);

#ifdef DEBUG
            elog(NOTICE, "SQLDescribeCol: ");
            elog(NOTICE, "Result Column Name: %s", (char *) ColumnName);
            elog(NOTICE, "Result DataType: %i",  DataTypePtr);
            elog(NOTICE, "Result Column Size: %i",  (int) ColumnSizePtr);
            elog(NOTICE, "Result Decimal Size: %i",  DecimalDigitsPtr);
            elog(NOTICE, "Result Nullable: %i",  NullablePtr);
#endif

            /* Get the position of the column in the FDW table */
            for (k=0; k<num_of_table_cols; k++)
            {
                if (strcmp(table_columns[k].data, (char *) ColumnName) == 0)
                {
                    found = TRUE;
#ifdef DEBUG
                    elog(NOTICE, "I value: %i", i-1);
                    elog(NOTICE, "K value: %i", k);
#endif
                    col_position_mask = lappend_int(col_position_mask, k);
                    col_size_array = lappend_int(col_size_array, (int) ColumnSizePtr);
                    break;
                }
            }
            /* if current column is not used by the foreign table */
            if (!found)
            {
                col_position_mask = lappend_int(col_position_mask, -1);
                col_size_array = lappend_int(col_size_array, -1);
            }

            pfree(ColumnName);
        }
        festate->num_of_result_cols = num_of_result_cols;
        festate->col_position_mask = list_copy(col_position_mask);
        festate->col_size_array = list_copy(col_size_array);
        festate->first_iteration = FALSE;
    }
    else
    {
        num_of_result_cols = festate->num_of_result_cols;
        col_position_mask = list_copy(festate->col_position_mask);
        col_size_array = festate->col_size_array;
    }

    ExecClearTuple(slot);

    if (SQL_SUCCEEDED(ret))
    {
        SQLSMALLINT i;
        values = (char **) palloc(sizeof(char *) * columns);
        /* Loop through the columns */
        for (i = 1; i <= columns; i++)
        {
            SQLLEN indicator;
            char * buf;

            int mask_index = i - 1;
            int col_size = list_nth_int(col_size_array, mask_index);
            int mapped_pos = list_nth_int(col_position_mask, mask_index);

#ifdef DEBUG
            /* Dump the content of the mask */
			int p;
            elog(NOTICE, "Mask index: %i", mask_index);
            elog(NOTICE, "Content of the mask:");
            for (p=0; p<num_of_result_cols; p++)
            {
                elog(NOTICE, "%i => %i (%i)", p, list_nth_int(col_position_mask, p), list_nth_int(col_size_array, p));
            }
#endif
            /* Ignore this column if position is marked as invalid */
            if (mapped_pos == -1)
                continue;

            buf = (char *) palloc(sizeof(char) * col_size);

            /* retrieve column data as a string */
            ret = SQLGetData(stmt, i, SQL_C_CHAR,
                             buf, sizeof(char) * col_size, &indicator);

            if (SQL_SUCCEEDED(ret))
            {
                /* Handle null columns */
                if (indicator == SQL_NULL_DATA) strcpy(buf, "NULL");
                initStringInfo(&col_data);
                appendStringInfoString (&col_data, buf);

                values[mapped_pos] = col_data.data;
#ifdef DEBUG
                elog(NOTICE, "values[%i] = %s", mapped_pos, values[mapped_pos]);
#endif
            }
            pfree(buf);
        }

        tuple = BuildTupleFromCStrings(festate->attinmeta, values);
        ExecStoreTuple(tuple, slot, InvalidBuffer, false);
        pfree(values);
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

    odbcGetTableSize(festate->svr_dsn, festate->svr_database, festate->svr_schema, festate->svr_table,
                     festate->svr_username, festate->svr_password, festate->sql_count, &table_size);

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
        if (festate->stmt)
        {
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
