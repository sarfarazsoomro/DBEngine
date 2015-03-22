#ifndef DBI_H
#define DBI_H
#include <iostream>
#include <stdlib.h>
#include "ParseTree.h"
#include "RelOpNode.h"
//#include "y.tab.h"
#include <set>
#include <vector>
#include <unordered_map>

using namespace __gnu_cxx;

extern struct FuncOperator *finalFunction; // the aggregate function (NULL if no agg)
extern struct TableList *tables; // the list of tables and aliases in the query
extern struct AndList *boolean; // the predicate in the WHERE clause
extern struct NameList *groupingAtts; // grouping atts (NULL if no grouping)
extern struct NameList *attsToSelect; // the set of attributes in the SELECT (NULL if no such atts)
extern int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query 
extern int distinctFunc;  // 1 if there is a DISTINCT in an aggregate query

extern "C" int yyparse(void);



class DBInstance {
private:
    string getRelAlias(string relName);
    string CalcNewRelName(string one, string two, unordered_map<string, vector<string>> jc);
    string CalcNewRelNameHelper(set<string> rels);
public:
	DBInstance();
        void GetCommand();
        void SelectQ();
        void CreateQueryPlan();
        int clear_pipe(Pipe &in_pipe, Schema *schema, bool print);
};

#endif