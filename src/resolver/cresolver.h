// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef HUSTLE_CRESOLVER_H
#define HUSTLE_CRESOLVER_H
#ifdef __cplusplus
extern "C" {
#endif

#ifndef SQLITE_MAX_EXPR_DEPTH
#define SQLITE_MAX_EXPR_DEPTH 1000
#endif

typedef signed char i8;
typedef unsigned char u8;
typedef unsigned int u32;
typedef unsigned short u16;
typedef short i16;
typedef long long int i64;
typedef unsigned long long int u64;

typedef struct Table Table;
typedef struct VTable VTable;
typedef struct Module Module;
typedef struct Column Column;
typedef struct ExprList ExprList;
typedef struct Expr Expr;
typedef struct Select Select;
typedef struct Select Sqlite3Select;
typedef struct AggInfo AggInfo;
typedef struct Window Window;
typedef struct FuncDef FuncDef;
typedef struct FuncDestructor FuncDestructor;
typedef struct FKey FKey;
typedef struct Trigger Trigger;
typedef struct Index Index;
typedef struct Schema Schema;
typedef struct With With;
typedef struct IdList IdList;
typedef struct TriggerStep TriggerStep;
typedef struct SrcList SrcList;
typedef struct HashElem HashElem;
typedef struct Upsert Upsert;
typedef struct Hash Hash;
typedef struct sqlite3_vtab sqlite3_vtab;
typedef struct sqlite3_module sqlite3_module;
typedef struct sqlite3_value sqlite3_value;
typedef struct sqlite3_context sqlite3_context;

struct sqlite3;

#define TK_SEMI 1
#define TK_EXPLAIN 2
#define TK_QUERY 3
#define TK_PLAN 4
#define TK_BEGIN 5
#define TK_TRANSACTION 6
#define TK_DEFERRED 7
#define TK_IMMEDIATE 8
#define TK_EXCLUSIVE 9
#define TK_COMMIT 10
#define TK_END 11
#define TK_ROLLBACK 12
#define TK_SAVEPOINT 13
#define TK_RELEASE 14
#define TK_TO 15
#define TK_TABLE 16
#define TK_CREATE 17
#define TK_IF 18
#define TK_NOT 19
#define TK_EXISTS 20
#define TK_TEMP 21
#define TK_LP 22
#define TK_RP 23
#define TK_AS 24
#define TK_WITHOUT 25
#define TK_COMMA 26
#define TK_ABORT 27
#define TK_ACTION 28
#define TK_AFTER 29
#define TK_ANALYZE 30
#define TK_ASC 31
#define TK_ATTACH 32
#define TK_BEFORE 33
#define TK_BY 34
#define TK_CASCADE 35
#define TK_CAST 36
#define TK_CONFLICT 37
#define TK_DATABASE 38
#define TK_DESC 39
#define TK_DETACH 40
#define TK_EACH 41
#define TK_FAIL 42
#define TK_OR 43
#define TK_AND 44
#define TK_IS 45
#define TK_MATCH 46
#define TK_LIKE_KW 47
#define TK_BETWEEN 48
#define TK_IN 49
#define TK_ISNULL 50
#define TK_NOTNULL 51
#define TK_NE 52
#define TK_EQ 53
#define TK_GT 54
#define TK_LE 55
#define TK_LT 56
#define TK_GE 57
#define TK_ESCAPE 58
#define TK_ID 59
#define TK_COLUMNKW 60
#define TK_DO 61
#define TK_FOR 62
#define TK_IGNORE 63
#define TK_INITIALLY 64
#define TK_INSTEAD 65
#define TK_NO 66
#define TK_KEY 67
#define TK_OF 68
#define TK_OFFSET 69
#define TK_PRAGMA 70
#define TK_RAISE 71
#define TK_RECURSIVE 72
#define TK_REPLACE 73
#define TK_RESTRICT 74
#define TK_ROW 75
#define TK_ROWS 76
#define TK_TRIGGER 77
#define TK_VACUUM 78
#define TK_VIEW 79
#define TK_VIRTUAL 80
#define TK_WITH 81
#define TK_NULLS 82
#define TK_FIRST 83
#define TK_LAST 84
#define TK_CURRENT 85
#define TK_FOLLOWING 86
#define TK_PARTITION 87
#define TK_PRECEDING 88
#define TK_RANGE 89
#define TK_UNBOUNDED 90
#define TK_EXCLUDE 91
#define TK_GROUPS 92
#define TK_OTHERS 93
#define TK_TIES 94
#define TK_GENERATED 95
#define TK_ALWAYS 96
#define TK_REINDEX 97
#define TK_RENAME 98
#define TK_CTIME_KW 99
#define TK_ANY 100
#define TK_BITAND 101
#define TK_BITOR 102
#define TK_LSHIFT 103
#define TK_RSHIFT 104
#define TK_PLUS 105
#define TK_MINUS 106
#define TK_STAR 107
#define TK_SLASH 108
#define TK_REM 109
#define TK_CONCAT 110
#define TK_COLLATE 111
#define TK_BITNOT 112
#define TK_ON 113
#define TK_INDEXED 114
#define TK_STRING 115
#define TK_JOIN_KW 116
#define TK_CONSTRAINT 117
#define TK_DEFAULT 118
#define TK_NULL 119
#define TK_PRIMARY 120
#define TK_UNIQUE 121
#define TK_CHECK 122
#define TK_REFERENCES 123
#define TK_AUTOINCR 124
#define TK_INSERT 125
#define TK_DELETE 126
#define TK_UPDATE 127
#define TK_SET 128
#define TK_DEFERRABLE 129
#define TK_FOREIGN 130
#define TK_DROP 131
#define TK_UNION 132
#define TK_ALL 133
#define TK_EXCEPT 134
#define TK_INTERSECT 135
#define TK_SELECT 136
#define TK_VALUES 137
#define TK_DISTINCT 138
#define TK_DOT 139
#define TK_FROM 140
#define TK_JOIN 141
#define TK_USING 142
#define TK_ORDER 143
#define TK_GROUP 144
#define TK_HAVING 145
#define TK_LIMIT 146
#define TK_WHERE 147
#define TK_INTO 148
#define TK_NOTHING 149
#define TK_FLOAT 150
#define TK_BLOB 151
#define TK_INTEGER 152
#define TK_VARIABLE 153
#define TK_CASE 154
#define TK_WHEN 155
#define TK_THEN 156
#define TK_ELSE 157
#define TK_INDEX 158
#define TK_ALTER 159
#define TK_ADD 160
#define TK_WINDOW 161
#define TK_OVER 162
#define TK_FILTER 163
#define TK_COLUMN 164
#define TK_AGG_FUNCTION 165
#define TK_AGG_COLUMN 166
#define TK_TRUEFALSE 167
#define TK_ISNOT 168
#define TK_FUNCTION 169
#define TK_UMINUS 170
#define TK_UPLUS 171
#define TK_TRUTH 172
#define TK_REGISTER 173
#define TK_VECTOR 174
#define TK_SELECT_COLUMN 175
#define TK_IF_NULL_ROW 176
#define TK_ASTERISK 177
#define TK_SPAN 178
#define TK_SPACE 179
#define TK_ILLEGAL 180

struct VTable {
  struct sqlite3 *db;  /* Database connection associated with this table */
  Module *pMod;        /* Pointer to module implementation */
  sqlite3_vtab *pVtab; /* Pointer to vtab instance */
  int nRef;            /* Number of pointers to this structure */
  u8 bConstraint;      /* True if constraints are supported */
  u8 eVtabRisk;        /* Riskiness of allowing hacker access */
  int iSavepoint;      /* Depth of the SAVEPOINT stack */
  VTable *pNext;       /* Next in linked list (see above) */
};

/*
** Each SQLite module (virtual table definition) is defined by an
** instance of the following structure, stored in the sqlite3.aModule
** hash table.
*/
struct Module {
  const sqlite3_module *pModule; /* Callback pointers */
  const char *zName;             /* Name passed to create_module() */
  int nRefModule;                /* Number of pointers to this object */
  void *pAux;                    /* pAux passed to create_module() */
  void (*xDestroy)(void *);      /* Module destructor function */
  Table *pEpoTab;                /* Eponymous table for this module */
};
/*
** information about each column of an SQL table is held in an instance
** of this structure.
*/
struct Column {
  char *zName;   /* Name of this column, \000, then the type */
  Expr *pDflt;   /* Default value of this column */
  char *zColl;   /* Collating sequence.  If NULL, use the default */
  u8 notNull;    /* An OE_ code for handling a NOT NULL constraint */
  char affinity; /* One of the SQLITE_AFF_... values */
  u8 szEst;      /* Estimated size of value in this column. sizeof(INT)==1 */
  u8 colFlags;   /* Boolean properties.  See COLFLAG_ defines below */
};

struct ExprList {
  int nExpr;               /* Number of expressions on the list */
  struct ExprList_item {   /* For each expression in the list */
    Expr *pExpr;           /* The parse tree for this expression */
    char *zEName;          /* Token associated with this expression */
    u8 sortFlags;          /* Mask of KEYINFO_ORDER_* flags */
    unsigned eEName : 2;   /* Meaning of zEName */
    unsigned done : 1;     /* A flag to indicate when processing is finished */
    unsigned reusable : 1; /* Constant expression is reusable */
    unsigned bSorterRef : 1; /* Defer evaluation until after sorting */
    unsigned bNulls : 1;     /* True if explicit "NULLS FIRST/LAST" */
    union {
      struct {
        u16 iOrderByCol; /* For ORDER BY, column number in result set */
        u16 iAlias;      /* Index into Parse.aAlias[] for zName */
      } x;
      int iConstExprReg; /* Register in which Expr value is cached */
    } u;
  } a[1]; /* One slot for each expression in the list */
};

struct Expr {
  u8 op;        /* Operation performed by this node */
  char affExpr; /* affinity, or RAISE type */
  u8 op2;       /* TK_REGISTER/TK_TRUTH: original value of Expr.op
                ** TK_COLUMN: the value of p5 for OP_Column
                ** TK_AGG_FUNCTION: nesting depth
                ** TK_FUNCTION: NC_SelfRef flag if needs OP_PureFunc */
  u32 flags;    /* Various flags.  EP_* See below */
  union {
    char *zToken; /* Token value. Zero terminated and dequoted */
    int iValue;   /* Non-negative integer value if EP_IntValue */
  } u;

  /* If the EP_TokenOnly flag is set in the Expr.flags mask, then no
  ** space is allocated for the fields below this point. An attempt to
  ** access them will result in a segfault or malfunction.
  *********************************************************************/

  Expr *pLeft;  /* Left subnode */
  Expr *pRight; /* Right subnode */
  union {
    ExprList *pList; /* op = IN, EXISTS, SELECT, CASE, FUNCTION, BETWEEN */
    Select *pSelect; /* EP_xIsSelect and op = IN, EXISTS, SELECT */
  } x;

  /* If the EP_Reduced flag is set in the Expr.flags mask, then no
  ** space is allocated for the fields below this point. An attempt to
  ** access them will result in a segfault or malfunction.
  *********************************************************************/

#if SQLITE_MAX_EXPR_DEPTH > 0
  int nHeight; /* Height of the tree headed by this node */
#endif
  int iTable;          /* TK_COLUMN: cursor number of table holding column
                       ** TK_REGISTER: register number
                       ** TK_TRIGGER: 1 -> new, 0 -> old
                       ** EP_Unlikely:  134217728 times likelihood
                       ** TK_IN: ephemerial table holding RHS
                       ** TK_SELECT_COLUMN: Number of columns on the LHS
                       ** TK_SELECT: 1st register of result vector */
  i16 iColumn;         /* TK_COLUMN: column index.  -1 for rowid.
                       ** TK_VARIABLE: variable number (always >= 1).
                       ** TK_SELECT_COLUMN: column of the result vector */
  i16 iAgg;            /* Which entry in pAggInfo->aCol[] or ->aFunc[] */
  i16 iRightJoinTable; /* If EP_FromJoin, the right table of the join */
  AggInfo *pAggInfo;   /* Used by TK_AGG_COLUMN and TK_AGG_FUNCTION */
  union {
    Table *pTab;     /* TK_COLUMN: Table containing column. Can be NULL
                     ** for a column of an index on an expression */
    Window *pWin;    /* EP_WinFunc: Window/Filter defn for a function */
    struct {         /* TK_IN, TK_SELECT, and TK_EXISTS */
      int iAddr;     /* Subroutine entry address */
      int regReturn; /* Register used to hold return address */
    } sub;
  } y;
};

struct AggInfo {
  u8 directMode;       /* Direct rendering mode means take data directly
                       ** from source tables rather than from accumulators */
  u8 useSortingIdx;    /* In direct mode, reference the sorting index rather
                       ** than the source table */
  int sortingIdx;      /* Cursor number of the sorting index */
  int sortingIdxPTab;  /* Cursor number of pseudo-table */
  int nSortingColumn;  /* Number of columns in the sorting index */
  int mnReg, mxReg;    /* Range of registers allocated for aCol and aFunc */
  ExprList *pGroupBy;  /* The group by clause */
  struct AggInfo_col { /* For each column used in source tables */
    Table *pTab;       /* Source table */
    int iTable;        /* Cursor number of the source table */
    int iColumn;       /* Column number within the source table */
    int iSorterColumn; /* Column number in the sorting index */
    int iMem;          /* Memory location that acts as accumulator */
    Expr *pExpr;       /* The original expression */
  } * aCol;
  int nColumn;          /* Number of used entries in aCol[] */
  int nAccumulator;     /* Number of columns that show through to the output.
                        ** Additional columns are used only as parameters to
                        ** aggregate functions */
  struct AggInfo_func { /* For each aggregate function */
    Expr *pExpr;        /* Expression encoding the function */
    FuncDef *pFunc;     /* The aggregate function implementation */
    int iMem;           /* Memory location that acts as accumulator */
    int iDistinct;      /* Ephemeral table used to enforce DISTINCT */
  } * aFunc;
  int nFunc; /* Number of entries in aFunc[] */
};

struct FuncDef {
  i8 nArg;         /* Number of arguments.  -1 means unlimited */
  u32 funcFlags;   /* Some combination of SQLITE_FUNC_* */
  void *pUserData; /* User data parameter */
  FuncDef *pNext;  /* Next function with same name */
  void (*xSFunc)(sqlite3_context *, int,
                 sqlite3_value **);     /* func or agg-step */
  void (*xFinalize)(sqlite3_context *); /* Agg finalizer */
  void (*xValue)(sqlite3_context *);    /* Current agg value */
  void (*xInverse)(sqlite3_context *, int,
                   sqlite3_value **); /* inverse agg-step */
  const char *zName;                  /* SQL name of the function. */
  union {
    FuncDef *pHash; /* Next with a different name but the same hash */
    FuncDestructor *pDestructor; /* Reference counted destructor function */
  } u;
};

struct FuncDestructor {
  int nRef;
  void (*xDestroy)(void *);
  void *pUserData;
};

struct Window {
  char *zName;          /* Name of window (may be NULL) */
  char *zBase;          /* Name of base window for chaining (may be NULL) */
  ExprList *pPartition; /* PARTITION BY clause */
  ExprList *pOrderBy;   /* ORDER BY clause */
  u8 eFrmType;          /* TK_RANGE, TK_GROUPS, TK_ROWS, or 0 */
  u8 eStart;            /* UNBOUNDED, CURRENT, PRECEDING or FOLLOWING */
  u8 eEnd;              /* UNBOUNDED, CURRENT, PRECEDING or FOLLOWING */
  u8 bImplicitFrame;    /* True if frame was implicitly specified */
  u8 eExclude;          /* TK_NO, TK_CURRENT, TK_TIES, TK_GROUP, or 0 */
  Expr *pStart;         /* Expression for "<expr> PRECEDING" */
  Expr *pEnd;           /* Expression for "<expr> FOLLOWING" */
  Window **ppThis;      /* Pointer to this object in Select.pWin list */
  Window *pNextWin;     /* Next window function belonging to this SELECT */
  Expr *pFilter;        /* The FILTER expression */
  FuncDef *pFunc;       /* The function */
  int iEphCsr;          /* Partition buffer or Peer buffer */
  int regAccum;         /* Accumulator */
  int regResult;        /* Interim result */
  int csrApp;           /* Function cursor (used by min/max) */
  int regApp;           /* Function register (also used by min/max) */
  int regPart;          /* Array of registers for PARTITION BY values */
  Expr *pOwner;         /* Expression object this window is attached to */
  int nBufferCol;       /* Number of columns in buffer table */
  int iArgCol;          /* Offset of first argument for this function */
  int regOne;           /* Register containing constant value 1 */
  int regStartRowid;
  int regEndRowid;
  u8 bExprArgs; /* Defer evaluation of window function arguments
                ** due to the SQLITE_SUBTYPE flag */
};

struct FKey {
  Table *pFrom;    /* Table containing the REFERENCES clause (aka: Child) */
  FKey *pNextFrom; /* Next FKey with the same in pFrom. Next parent of pFrom */
  char *zTo;       /* Name of table that the key points to (aka: Parent) */
  FKey *pNextTo;   /* Next with the same zTo. Next child of zTo. */
  FKey *pPrevTo;   /* Previous with the same zTo */
  int nCol;        /* Number of columns in this key */
  /* EV: R-30323-21917 */
  u8 isDeferred; /* True if constraint checking is deferred till COMMIT */
  u8 aAction[2]; /* ON DELETE and ON UPDATE actions, respectively */
  Trigger *apTrigger[2]; /* Triggers for aAction[] actions */
  struct sColMap {       /* Mapping of columns in pFrom to columns in zTo */
    int iFrom;           /* Index of column in pFrom */
    char *zCol;          /* Name of column in zTo.  If NULL use PRIMARY KEY */
  } aCol[1];             /* One entry for each of nCol columns */
};

struct Table {
  char *zName;      /* Name of the table or view */
  Column *aCol;     /* Information about each column */
  Index *pIndex;    /* List of SQL indexes on this table. */
  Select *pSelect;  /* NULL for tables.  Points to definition if a view. */
  FKey *pFKey;      /* Linked list of all foreign keys in this table */
  char *zColAff;    /* String defining the affinity of each column */
  ExprList *pCheck; /* All CHECK constraints */
                    /*   ... also used as column name list in a VIEW */
  int tnum;         /* Root BTree page for this table */
  u32 nTabRef;      /* Number of pointers to this Table */
  u32 tabFlags;     /* Mask of TF_* values */
  i16 iPKey;        /* If not negative, use aCol[iPKey] as the rowid */
  i16 nCol;         /* Number of columns in this table */
  i16 nNVCol;       /* Number of columns that are not VIRTUAL */
  u16 nRowLogEst;   /* Estimated rows in table - from sqlite_stat1 table */
  u16 szTabRow;     /* Estimated size of each table row in bytes */
#ifdef SQLITE_ENABLE_COSTMULT
  uint16_t costMult; /* Cost multiplier for using this table */
#endif
  u8 keyConf; /* What to do in case of uniqueness conflict on iPKey */
#ifndef SQLITE_OMIT_ALTERTABLE
  int addColOffset; /* Offset in CREATE TABLE stmt to add a new column */
#endif
#ifndef SQLITE_OMIT_VIRTUALTABLE
  int nModuleArg;     /* Number of arguments to the module */
  char **azModuleArg; /* 0: module 1: schema 2: vtab name 3...: args */
  VTable *pVTable;    /* List of VTable objects. */
#endif
  Trigger *pTrigger;  /* List of triggers stored in pSchema */
  Schema *pSchema;    /* Schema that contains this table */
  Table *pNextZombie; /* Next on the Parse.pZombieTab list */
};

struct Trigger {
  char *zName;            /* The name of the trigger                        */
  char *table;            /* The table or view to which the trigger applies */
  u8 op;                  /* One of TK_DELETE, TK_UPDATE, TK_INSERT         */
  u8 tr_tm;               /* One of TRIGGER_BEFORE, TRIGGER_AFTER */
  Expr *pWhen;            /* The WHEN clause of the expression (may be NULL) */
  IdList *pColumns;       /* If this is an UPDATE OF <column-list> trigger,
                             the <column-list> is stored here */
  Schema *pSchema;        /* Schema containing the trigger */
  Schema *pTabSchema;     /* Schema containing the table */
  TriggerStep *step_list; /* Link list of trigger program steps             */
  Trigger *pNext;         /* Next trigger associated with the table */
};

struct IdList {
  struct IdList_item {
    char *zName; /* Name of the identifier */
    int idx;     /* Index in some Table.aCol[] of a column named zName */
  } * a;
  int nId; /* Number of identifiers on the list */
};

struct Index {
  char *zName;             /* Name of this index */
  i16 *aiColumn;           /* Which columns are used by this index.  1st is 0 */
  u64 *aiRowLogEst;        /* From ANALYZE: Est. rows selected by each column */
  Table *pTable;           /* The SQL table being indexed */
  char *zColAff;           /* String defining the affinity of each column */
  Index *pNext;            /* The next index associated with the same table */
  Schema *pSchema;         /* Schema containing this index */
  u8 *aSortOrder;          /* for each column: True==DESC, False==ASC */
  const char **azColl;     /* Array of collation sequence names for index */
  Expr *pPartIdxWhere;     /* WHERE clause for partial indices */
  ExprList *aColExpr;      /* Column expressions */
  int tnum;                /* DB Page containing root of this index */
  u64 szIdxRow;            /* Estimated average row size in bytes */
  u16 nKeyCol;             /* Number of columns forming the key */
  u16 nColumn;             /* Number of columns stored in the index */
  u8 onError;              /* OE_Abort, OE_Ignore, OE_Replace, or OE_None */
  unsigned idxType : 2;    /* 0:Normal 1:UNIQUE, 2:PRIMARY KEY, 3:IPK */
  unsigned bUnordered : 1; /* Use this index for == or IN queries only */
  unsigned uniqNotNull : 1; /* True if UNIQUE and NOT NULL for all columns */
  unsigned isResized : 1;   /* True if resizeIndexObject() has been called */
  unsigned isCovering : 1;  /* True if this is a covering index */
  unsigned noSkipScan : 1;  /* Do not try to use skip-scan if true */
  unsigned hasStat1 : 1;    /* aiRowLogEst values come from sqlite_stat1 */
  unsigned bNoQuery : 1;    /* Do not use this index to optimize queries */
  unsigned bAscKeyBug : 1;  /* True if the bba7b69f9849b5bf bug applies */
  unsigned bHasVCol : 1;    /* Index references one or more VIRTUAL columns */
#ifdef SQLITE_ENABLE_STAT4
  int nSample;          /* Number of elements in aSample[] */
  int nSampleCol;       /* Size of IndexSample.anEq[] and so on */
  tRowcnt *aAvgEq;      /* Average nEq values for keys not in aSample */
  IndexSample *aSample; /* Samples of the left-most key */
  tRowcnt *aiRowEst;    /* Non-logarithmic stat1 data for this index */
  tRowcnt nRowEst0;     /* Non-logarithmic number of rows in the index */
#endif
  u64 colNotIdxed; /* 0 for unindexed columns in pTab */
};

struct With {
  int nCte;              /* Number of CTEs in the WITH clause */
  With *pOuter;          /* Containing WITH clause, or NULL */
  struct Cte {           /* For each CTE in the WITH clause.... */
    char *zName;         /* Name of this CTE */
    ExprList *pCols;     /* List of explicit column names, or NULL */
    Select *pSelect;     /* The definition of this CTE */
    const char *zCteErr; /* Error message for circular references */
  } a[1];
};

struct SrcList {
  int nSrc;   /* Number of tables or subqueries in the FROM clause */
  u32 nAlloc; /* Number of entries allocated in a[] below */
  struct SrcList_item {
    Schema *pSchema; /* Schema to which this item is fixed */
    char *zDatabase; /* Name of database holding this table */
    char *zName;     /* Name of the table */
    char *zAlias;    /* The "B" part of a "A AS B" phrase.  zName is the "A" */
    Table *pTab;     /* An SQL table corresponding to zName */
    Select *pSelect; /* A SELECT statement used in place of a table name */
    int addrFillSub; /* Address of subroutine to manifest a subquery */
    int regReturn;   /* Register holding return address of addrFillSub */
    int regResult;   /* Registers holding results of a co-routine */
    struct {
      u8 jointype; /* Type of join between this table and the previous */
      unsigned notIndexed : 1;   /* True if there is a NOT INDEXED clause */
      unsigned isIndexedBy : 1;  /* True if there is an INDEXED BY clause */
      unsigned isTabFunc : 1;    /* True if table-valued-function syntax */
      unsigned isCorrelated : 1; /* True if sub-query is correlated */
      unsigned viaCoroutine : 1; /* Implemented as a co-routine */
      unsigned isRecursive : 1;  /* True for recursive reference in WITH */
      unsigned fromDDL : 1;      /* Comes from sqlite_master */
    } fg;
    int iCursor;    /* The VDBE cursor number used to access this table */
    Expr *pOn;      /* The ON clause of a join */
    IdList *pUsing; /* The USING clause of a join */
    u64 colUsed;    /* Bit N (1<<N) set if column N of pTab is used */
    union {
      char *zIndexedBy;   /* Identifier from "INDEXED BY <zIndex>" clause */
      ExprList *pFuncArg; /* Arguments to table-valued-function */
    } u1;
    Index *pIBIndex; /* Index structure corresponding to u1.zIndexedBy */
  } a[1];            /* One entry for each identifier on the list */
};

struct Select {
  ExprList *pEList;    /* The fields of the result */
  u8 op;               /* One of: TK_UNION TK_ALL TK_INTERSECT TK_EXCEPT */
  u16 nSelectRow;      /* Estimated number of result rows */
  u32 selFlags;        /* Various SF_* values */
  int iLimit, iOffset; /* Memory registers holding LIMIT & OFFSET counters */
  u32 selId;           /* Unique identifier number for this SELECT */
  int addrOpenEphm[2]; /* OP_OpenEphem opcodes related to this select */
  SrcList *pSrc;       /* The FROM clause */
  Expr *pWhere;        /* The WHERE clause */
  ExprList *pGroupBy;  /* The GROUP BY clause */
  Expr *pHaving;       /* The HAVING clause */
  ExprList *pOrderBy;  /* The ORDER BY clause */
  Select *pPrior;      /* Prior select in a compound select statement */
  Select *pNext;       /* Next select to the left in a compound */
  Expr *pLimit;        /* LIMIT expression. NULL means not used. */
  With *pWith;         /* WITH clause attached to this select. Or NULL. */
#ifndef SQLITE_OMIT_WINDOWFUNC
  Window *pWin;     /* List of window functions */
  Window *pWinDefn; /* List of named window definitions */
#endif
};

struct TriggerStep {
  u8 op;               /* One of TK_DELETE, TK_UPDATE, TK_INSERT, TK_SELECT */
  u8 orconf;           /* OE_Rollback etc. */
  Trigger *pTrig;      /* The trigger that this step is a part of */
  Select *pSelect;     /* SELECT statement or RHS of INSERT INTO SELECT ... */
  char *zTarget;       /* Target table for DELETE, UPDATE, INSERT */
  Expr *pWhere;        /* The WHERE clause for DELETE or UPDATE steps */
  ExprList *pExprList; /* SET clause for UPDATE */
  IdList *pIdList;     /* Column names for INSERT */
  Upsert *pUpsert;     /* Upsert clauses on an INSERT */
  char *zSpan;         /* Original SQL text of this command */
  TriggerStep *pNext;  /* Next in the link-list */
  TriggerStep *pLast;  /* Last element in link-list. Valid for 1st elem only */
};

struct Hash {
  unsigned int htsize;  /* Number of buckets in the hash table */
  unsigned int count;   /* Number of entries in this table */
  HashElem *first;      /* The first element of the array */
  struct _ht {          /* the hash table */
    unsigned int count; /* Number of entries with this hash */
    HashElem *chain;    /* Pointer to first entry with this hash */
  } * ht;
};

struct Upsert {
  ExprList *pUpsertTarget;  /* Optional description of conflicting index */
  Expr *pUpsertTargetWhere; /* WHERE clause for partial index targets */
  ExprList *pUpsertSet;     /* The SET clause from an ON CONFLICT UPDATE */
  Expr *pUpsertWhere;       /* WHERE clause for the ON CONFLICT UPDATE */
  /* The fields above comprise the parse tree for the upsert clause.
  ** The fields below are used to transfer information from the INSERT
  ** processing down into the UPDATE processing while generating code.
  ** Upsert owns the memory allocated above, but not the memory below. */
  Index *pUpsertIdx;   /* Constraint that pUpsertTarget identifies */
  SrcList *pUpsertSrc; /* Table to be updated */
  int regData;         /* First register holding array of VALUES */
  int iDataCur;        /* Index of the data cursor */
  int iIdxCur;         /* Index of the first index cursor */
};

/* Each element in the hash table is an instance of the following
** structure.  All elements are stored on a single doubly-linked list.
**
** Again, this structure is intended to be opaque, but it can't really
** be opaque because it is used by macros.
*/
struct HashElem {
  HashElem *next, *prev; /* Next and previous elements in the table */
  void *data;            /* Data associated with this element */
  const char *pKey;      /* Key associated with this element */
};

struct Schema {
  int schema_cookie; /* Database schema version number for this file */
  int iGeneration;   /* Generation counter.  Incremented with each change */
  Hash tblHash;      /* All tables indexed by name */
  Hash idxHash;      /* All (named) indices indexed by name */
  Hash trigHash;     /* All triggers indexed by name */
  Hash fkeyHash;     /* All foreign keys by referenced table name */
  Table *pSeqTab;    /* The sqlite_sequence table used by AUTOINCREMENT */
  u8 file_format;    /* Schema format version for this file */
  u8 enc;            /* Text encoding used by this database */
  u16 schemaFlags;   /* Flags associated with this schema */
  int cache_size;    /* Number of pages to use in the cache */
};

int resolveSelect(Select *queryTree);

#ifdef __cplusplus
}
#endif

#endif  // HUSTLE_CRESOLVER_H