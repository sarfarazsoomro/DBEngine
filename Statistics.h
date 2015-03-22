#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include <string>
#include <iostream>
#include <vector>
#include <unordered_map>
#include <set>
#include <iostream>
#include <fstream>

using namespace __gnu_cxx;
using namespace std;

class Statistics
{
private:
    //um is the hash that stores the information such as the #of tuples in 
    //a relationship or #of unique vals for an attribute
    unordered_map<string, double> um;
    //relFields, keeps track of what fields are present in a relationship, relName->attName
    unordered_map<string, vector<string>> relFields;
    //fieldsRel, keeps track of what attName->relName
    unordered_map<string, string> fieldsRel;
    //sets keeps tracks of what relationships have been joined
    unordered_map<string, set<string>> sets;
    //keep track of the rels to Join
    set<string> joinSets;
    //keeps track of the equi-join attributes in the cnf
    set<string> joinAtts;
    double estimate_(OrList *orL, char **relNames, bool multOr, bool first);
    double estimate_or(OrList *orL, char **relNames, bool first);
    double apply_(OrList *orL, char **relNames);
    string CalcNewRelName(set<string> rels);
    bool VerifyJoinable(char **relNames, int numToJoin);
    string getRelName(string relName);
    string getAttName(string relName);
public:
	Statistics();
	Statistics(Statistics &copyMe);	 // Performs deep copy
	~Statistics();
        
	void AddRel(char *relName, int numTuples);
	void AddAtt(char *relName, char *attName, int numDistincts);
	void CopyRel(char *oldName, char *newName);
	
	void Read(char *fromWhere);
	void Write(char *fromWhere);

	void  Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);
        
        void Print();
};

#endif
