#include "DBInstance.h"
#include "SelectFileOp.h"
#include "ProjectOp.h"
#include "JoinOp.h"

using namespace std;
DBInstance::DBInstance() {
    cout << "Started DB " << endl;
    GetCommand();
}

void DBInstance::GetCommand() {
    int tindx = 0;
    while (tindx < 1 || tindx > 5) {
            cout << "Commands: \n";
            cout << " \t 1. CREATE\n";
            cout << " \t 2. INSERT\n";
            cout << " \t 3. DROP \n";
            cout << " \t 4. SET OUTPUT\n";
            cout << " \t 5. SELECT\n \t";
            cin >> tindx;
    }
    
    if( tindx==5 ) {
        cout << "Enter Select Query " << endl;
        SelectQ();
    }
}

void DBInstance::SelectQ() {
    yyparse();
    //create a query plan
    //now the data structures have been populated so just create a query plan
    CreateQueryPlan();
}

void DBInstance::CreateQueryPlan() {
    //need selections for all the tables
    //go through the Table list
    int tblCount=0;
    unordered_map<string, string> aliasTableName;
    unordered_map<string, Pipe*> outPipes;
    unordered_map<string, CNF*> cnfs;
    unordered_map<string, Record*> recLits;
    unordered_map<string, DBFile*> dbfs;
    unordered_map<string, Schema*> schemas;
    vector<string> aliases;
    vector<string> joinAliases;
    unordered_map<string, vector<string>> joinConstituentRels;
    unordered_map<string, vector<string>> joinConstituents;
    unordered_map<string, string> relNameToJoinName;
    TableList *tl=tables;
    while(tl!=NULL) {
        aliasTableName[string(tl->aliasAs)]=string(tl->tableName);
        outPipes.insert({string(tl->aliasAs), new Pipe(100)});
        cnfs.insert({string(tl->aliasAs), new CNF()});
        recLits.insert({string(tl->aliasAs), new Record()});
        dbfs.insert({string(tl->aliasAs), new DBFile()});
        aliases.push_back(string(tl->aliasAs));
        relNameToJoinName[string(tl->aliasAs)]=string(tl->aliasAs);
        tblCount++;
        tl=tl->next;
    }
    tl=tables;
    //these are the select file operators, one select for each table
    unordered_map<string, SelectFile> selects;
    //these are the join operators, one join op for each join
    unordered_map<string, Join> joins;
    //Go through the AND list and create individual AND lists for each table  
    unordered_map<string, vector<OrList *>> selectionAnds; 
    //list of OrList for joins, will use to create cnf for joins
    unordered_map<string, vector<OrList *>> joinAnds;
    
    AndList *b_=boolean;
    while( b_!=NULL ) {
        OrList *orL=b_->left;
        //need to find whether this ORList is a selection or not
        bool selection=true;
        while(orL!=NULL) {
            //if both the left and right operands contain NAME,
            //then this is a join
            //also assume that in a single ORList all attributes are from
            //the same relation
            //this condition checks for a join
            if( orL->left->left->code==NAME && orL->left->right->code==NAME ) {
                //this is a join
                //add the names(aliases) of the joining tables to the joins vector
                //newAlName would also consider if one or both of these tables 
                //have been joined with some other tables already
                string left=getRelAlias(string(orL->left->left->value));
                string right=getRelAlias(string(orL->left->right->value));                
                
                string newAlName=CalcNewRelName(getRelAlias(string(orL->left->left->value)),
                        getRelAlias(string(orL->left->right->value)), joinConstituents);
                joinAnds[newAlName]=vector<OrList*>();
                joinAnds[newAlName].push_back(orL);
                joinAliases.push_back(newAlName);
                
                outPipes.insert({newAlName, new Pipe(100)});
                cnfs.insert({newAlName, new CNF()});
                recLits.insert({newAlName, new Record()});
                
                joinConstituents[newAlName]=vector<string>();
                
                
                joinConstituents[newAlName].push_back(relNameToJoinName[left]);
                joinConstituents[newAlName].push_back(relNameToJoinName[right]);
                cout << newAlName << "-> " << relNameToJoinName[left] << " , " << relNameToJoinName[right] << endl;
                relNameToJoinName[left]=newAlName;
                relNameToJoinName[right]=newAlName;
                
                selection=false;
                break;
            }
            orL=orL->rightOr;
        }
        //if selection==true at this point, means that the orL was for 
        //selection
        //set orL back to the orList we just scanned
        orL=b_->left;
        if( selection==true ) {
            //figure out the name of the table(alias)
            string relAlias=getRelAlias(string(orL->left->left->value));
            if( selectionAnds.count(relAlias)==0 ) {
                //means this alias hasn't been added to the map yet
                selectionAnds[relAlias]=vector<OrList*>();
            }
            //this map would contain all the individual selections grouped
            //together for each relation
            selectionAnds[relAlias].push_back(orL);
            //later we will create AndList out of these orList for each individual
            //table to get CNF object to be fed into SelectFile
        }
        b_=b_->rightAnd;
    }
    //at the end of this loop some tables would have orlist against them, others
    //won't have entries
    //make empty entries for those tables which are not present in selectionAnds
    for( int i=0; i<tblCount;i++ ) {
        if( selectionAnds.count( aliases.at(i) ) == 0 ) {
            selectionAnds[aliases.at(i)]=vector<OrList*>();
        }
    }
    
    //-----------
    //SELECTIONS
    //-----------
    //we have the table list, we can create an array of SelectFile ops for
    //each table
    //but before we do that we also need to know if there are any predicates that
    
    //would go into selections
    for(int i=0; i<tblCount; i++) {
        //need DBFile, outputPipe, CNF, record literal
        //DBFile dbf;
        string fileName= string("./dfiles/10M/")+aliasTableName[aliases.at(i)]+string(".bin");
        //dbf.Open( (char*)fileName.c_str() );
        dbfs[aliases.at(i)]->Open((char*)fileName.c_str());
        selects[aliases.at(i)]=SelectFile();
        //cnf needs, the andList, schema, and a record literal
        schemas[aliases.at(i)]=new Schema("catalog", (char*)aliasTableName[aliases.at(i)].c_str(), (char*)aliases.at(i).c_str() );
        //cout << "AND Size for " << aliasTableName[aliases.at(i)] << " " << selectionAnds[aliases.at(i)].size() << endl;
        
        //create the and list
        AndList *al=new AndList();
        AndList *temp=al;
        
        for( int j=0; j<selectionAnds[aliases.at(i)].size(); j++ ) {
            al->left=selectionAnds[aliases.at(i)].at(j);
            if( j<selectionAnds[aliases.at(i)].size()-1 ) {
                al->rightAnd=new AndList();
                al=al->rightAnd;
            }
        }
        al->rightAnd=NULL;
        al=temp;
        
        //now that we have the complete ANDList for this relationship,
        //we can use this to create a CNF that will be passed to Run functions
        cnfs[aliases.at(i)]->GrowFromParseTree(al, schemas[aliases.at(i)], *recLits[aliases.at(i)]);
        selects[aliases.at(i)].Run(*dbfs[aliases.at(i)], *outPipes[aliases.at(i)], *cnfs[aliases.at(i)], *recLits[aliases.at(i)]);
    }
    
    //-------
    //JOINS
    //-------
    //if the number of tables is greater than one, then there are joins involved
    //if not then no joins are involved, proceed with, GroupBy, Druplicate 
    //Elimination and Projection
    if( tblCount > 1 ) {
        //execute each join
        for(int i=0; i<joinAnds.size(); i++) {
            cout << "To Execute Join " << joinAliases.at(i) << endl;
            joins[joinAliases.at(i)]=Join();
            joins[joinAliases.at(i)].Use_n_Pages(10);
            //every join would have two constituents
            string leftPipeAlias= joinConstituents[joinAliases.at(i)].at(0);
            string rightPipeAlias= joinConstituents[joinAliases.at(i)].at(1);

            //now to construct the AndList, it's simple this time because it's 
            //just one OrList
            AndList *al=new AndList();
            al->left=joinAnds[joinAliases.at(i)].at(0);
            al->rightAnd=NULL;
            cnfs[joinAliases.at(i)]->GrowFromParseTree(al, schemas[leftPipeAlias], schemas[rightPipeAlias], *recLits[joinAliases.at(i)] );
            //also add new schema for this new join
            schemas[joinAliases.at(i)]=new Schema("catalog", schemas[leftPipeAlias].GetNumAtts()+schemas[rightPipeAlias], attsforthismergedschema);
            //pipeL
            //pipeR
            //pipeOut
            //cnf
            //recordLiteral
            joins[joinAliases.at(i)].Run(*outPipes[leftPipeAlias], *outPipes[rightPipeAlias], *outPipes[joinAliases.at(i)], *cnfs[joinAliases.at(i)], *recLits[joinAliases.at(i)]);
        }
    }
    
    //out of the joins we will have one schema, one output pipe
    
//    SelectFile sf;
//    
//    DBFile dbf;
//    dbf.Open("./dfiles/1G/partsupp.bin");
//    
//    CNF cnf;
//    Schema myschema("catalog", "partsupp", "ps");
//    Record lit;
//    
//    //do we need to remove the part before .?
//    cnf.GrowFromParseTree(boolean, &myschema, lit);
//    
//    Pipe p(100);
//    
//    sf.Run(dbf, p, cnf, lit);
//    
//    Pipe out(100);
//    Project prj;
//    
//    NameList *nl=attsToSelect;
//    int numAttsToKeep=0;
//    while( nl!=NULL ) {
//        numAttsToKeep++;
//        nl=nl->next;
//    }
//    
//    int attsToKeepArr[numAttsToKeep];
//    Attribute newAtts[numAttsToKeep];
//    nl=attsToSelect;
//    for(int i=0; i< numAttsToKeep; i++) {
//        int attNum=myschema.Find(nl->name);
//        if( attNum>=0 ) {
//            attsToKeepArr[i]=attNum;
//            newAtts[i].name=nl->name;
//            newAtts[i].myType=myschema.FindType(nl->name);
//        }
//        nl=nl->next;
//    }
//    
//    Schema newSchema("test", numAttsToKeep, newAtts);
//    prj.Run(p, out, attsToKeepArr, myschema.GetNumAtts(), numAttsToKeep);
//    
    //Pipe out(100);
//    int cnt=clear_pipe(*outPipes["ps"] , schemas["ps"], false);
//    cout << "#Rec " << cnt << endl;
//    cnt=clear_pipe(*outPipes["s"] , schemas["s"], false);
//    cout << "#Rec " << cnt << endl;
//    cnt=clear_pipe(*outPipes["p"] , schemas["p"], false);
//    cout << "#Rec " << cnt << endl;
    int cnt=clear_pipe(*outPipes["p_ps"] , schemas["p_ps"], false);
    cout << "#Rec " << cnt << endl;
}

int DBInstance::clear_pipe(Pipe &in_pipe, Schema *schema, bool print) {
	Record rec;
	int cnt = 0;
	while (in_pipe.Remove (&rec)) {
		if (print) {
                    rec.Print (schema);
		}
		cnt++;
	}
	return cnt;
}

string DBInstance::getRelAlias(string relName) {
    unsigned pos=relName.find(".");
    //if there is a dot in the relName

    //relationship name is the part before "."
    string relN=relName.substr(0,pos);
    return relN;
}

string DBInstance::CalcNewRelNameHelper(set<string> rels) {
    string t="";
    for(auto it=rels.begin(); it!=rels.end(); ++it) {
        t+=*it+"_";
    }
    return t.substr(0, t.size()-1);
}

string DBInstance::CalcNewRelName(string one, string two, unordered_map<string, vector<string>> jc) {
    set<string> ts_;
    ts_.insert(one);
    ts_.insert(two);
    
    for(auto it=jc.begin(); it!=jc.end();++it) {
        if( one.compare(string(it->second.at(0)))==0 || one.compare(string(it->second.at(1)))==0) {
            ts_.insert( string(it->second.at(0) ) );
            ts_.insert( string(it->second.at(1) ) );
        }
        if( two.compare(string(it->second.at(0)))==0 || two.compare(string(it->second.at(1)))==0) {
            ts_.insert( string(it->second.at(0) ) );
            ts_.insert( string(it->second.at(1) ) );
        }        
    }
    
    return CalcNewRelNameHelper(ts_);
}