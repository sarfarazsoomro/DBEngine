#include <algorithm>
#include "Statistics.h"
#include "Defs.h"

Statistics::Statistics() {
}

Statistics::Statistics(Statistics &copyMe) {
}

Statistics::~Statistics() {
    //delete um;
    //delete relFields;
}

void Statistics::AddRel(char *relName, int numTuples) {
    //add the relation #tuples to the hash
    um[relName]=numTuples;
    //next we need to check if this relationship exists in the relFields map
    unordered_map<string, vector<string>>::const_iterator got= relFields.find(relName);
    if(got==relFields.end()) {
        relFields[relName]=vector<string>();
    }
    //next check if this relationship exists in the sets map
    unordered_map<string, set<string>>::const_iterator gotS= sets.find(relName);
    if(gotS==sets.end()) {
        sets[relName]=set<string>();
        sets[relName].insert(relName);
    }
}

void Statistics::AddAtt(char *relName, char *attName, int numDistincts) {
    //if numDistincts is not -1 store as it is
    if( numDistincts!=-1 ) {
        um[string(relName)+string("_")+string(attName)]=numDistincts;
    } else {
        //else look uo the number of tuples in the associated relationship
        um[string(relName)+string("_")+string(attName)]=um[relName];
    }
    //make sure we are not adding the same attribute again
    //search first
    if( std::find(relFields[relName].begin(), relFields[relName].end(), string(attName) ) == relFields[relName].end() )
        relFields[relName].push_back(string(attName));
    //also add to the fieldsRel map
    fieldsRel[attName]=relName;
}

void Statistics::CopyRel(char *oldName, char *newName) {
    //copy rel to effect the following data structures
    //um, relFields, sets
    //not doing fieldsRel right now coz 1-1 associativity in fieldsRel
    
    //first add the new name to um
    um[newName]=um[oldName];
    //init the vector in relFields for this newName
    relFields[newName]=vector<string>();
    for( int i=0; i<relFields[oldName].size(); i++ ) {
        //add all the attributes in this relation to the um with oldName_attName
        um[string(newName)+"_"+relFields[oldName].at(i)]=um[string(oldName)+"_"+relFields[oldName].at(i)];
        relFields[newName].push_back(relFields[oldName].at(i));
    }
    
    //also add to the sets
    sets[newName]=set<string>();
    sets[newName].insert(string(newName));
}
	
void Statistics::Read(char *fromWhere) {
    um.clear();
    relFields.clear();
    fieldsRel.clear();
    sets.clear();
    ifstream statsFile(fromWhere);
    string type;
    string curRel;
    
    string str;
    if(statsFile.is_open()) {
        getline(statsFile, str);
        if( str.compare("#rels")==0 ) {
            //keeps reading rels till stats don't start
            while( str.compare("#stats")!=0 ) {
                getline(statsFile, str);
                while( str.compare("rel")==0 ) {
                    //get the name of the relation
                    getline(statsFile, str);
                    curRel=str;
                    //this is not correct
                    //sets[curRel]=set<string>();
                    //sets[curRel].insert(curRel);
                    //next line should be an att
                    getline(statsFile, str);
                    while( str.compare("att")==0 ) {
                        //this line would have the name of the attribute
                        getline(statsFile, str);
                        //so add this attribute to the relFields
                        relFields[curRel].push_back(str);
                        //also add the att->rel mapping in feildsRel
                        fieldsRel[str]=curRel;
                        //get the next line, which would be either, att, rel
                        //or #stats
                        getline(statsFile, str);
                    }
                }
            }
        }
        if( str.compare("#stats")==0 ) {
            string stat;
            int statVal;
            while(getline(statsFile, str) && str.compare("#sets")!=0) {
                stat=str;
                getline(statsFile, str);
                statVal=StringToNumber<int>(str);
                um[stat]=statVal;
            }
        }
        if( str.compare("#sets")==0 ) {
            //next line is set
            string currentSet;
            while(getline(statsFile, str)) {
                if( str.compare("set")==0 ) {
                    getline(statsFile, str);
                    sets[str]=set<string>();
                    currentSet=str;
                } else if( str.compare("setmember")==0 ) {
                    getline(statsFile, str);
                    sets[currentSet].insert(str);
                }
            }
        }
    }
    statsFile.close();    
}

void Statistics::Write(char *fromWhere) {
    ofstream statsFile(fromWhere);
        if(statsFile.is_open()) {
            statsFile << "#rels" << endl;
            for ( auto it = relFields.begin(); it != relFields.end(); ++it ) {
                statsFile << "rel" << endl;
                statsFile << it->first << endl;
                for(int i=0; i<it->second.size(); i++) {
                    statsFile << "att" << endl;
                    statsFile << it->second[i] <<endl;
                }
            }
            statsFile << "#stats" << endl;
            for ( auto it = um.begin(); it != um.end(); ++it ) {
                statsFile << it->first << endl;
                statsFile << it->second << endl;
            }
            statsFile << "#sets" << endl;
            for(auto it=sets.begin(); it!=sets.end();++it) {
                statsFile << "set" << endl;
                statsFile << it->first << endl;
                for(auto jt=it->second.begin(); jt!=it->second.end();++jt) {
                    statsFile << "setmember" << endl;
                    statsFile << *jt << endl;
                }
            }
            statsFile.close();
        }    
}

void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin) {
    double estResult;
    estResult=Estimate(parseTree, relNames, numToJoin);
        
    if( numToJoin > 1 ) {
//        set<string> tSet;
//        for(int i=0; i< numToJoin; i++) {
//            tSet.insert(string(relNames[i]));
//        }

        //string newRelName=CalcNewRelName(tSet);
        string newRelName=CalcNewRelName(joinSets);
        //add the new joined relationship name to the map
        //estResult is the number of tuples in the joined relationship
        um[newRelName]=estResult;
        //next add all the fields from the relationships which have been joined
        //into the um
        //if the field is a join attribute, then it would have the number equal to
        //estResult,
        //otherwise, it would retain it's old value

        relFields[newRelName]=vector<string>();
        
        //for(int i=0; i< numToJoin; i++) {
        for(auto it=joinSets.begin(); it!=joinSets.end(); ++it) {
            //for each joining relationship
            for(int j=0; j<relFields[*it].size(); j++) {
                //go through all it's fields
                if( joinAtts.count(relFields[*it].at(j))==0 ) {
                    //if it's not a join attribute
                    um[newRelName+string("_")+relFields[*it].at(j)]=um[(*it)+string("_")+relFields[*it].at(j)];
                } else {
                    //if it's a join attribute then the number is estResult
                    um[newRelName+string("_")+relFields[*it].at(j)]=estResult;
                }
                //erase this oldrel_oldfield from um
                um.erase( (*it)+string("_")+relFields[*it].at(j) );
                //add this attribute to the newRelName in relFields
                relFields[newRelName].push_back(relFields[*it].at(j));
                //remove this Att->Relationship mapping from fieldsRel
                fieldsRel.erase(relFields[*it].at(j));
                //and add a new Att->Relation mapping into the fieldsRel
                fieldsRel[relFields[*it].at(j)]=newRelName;
            }
            //erase old rel from um
            um.erase(*it);
            //this relName has been processed, so erase this from relFields map
            relFields.erase(*it);
        }
        //empty joinAtts now
        joinAtts.clear();
        //update the sets to reflect what relations have been joined
        for(auto it=joinSets.begin(); it!=joinSets.end(); ++it) {
            for(auto jt=joinSets.begin(); jt!=joinSets.end(); ++jt) {
                sets[*it].insert(*jt);
            }
        }
        //empty joinSets now
        joinSets.clear();
    }
}

double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin) {
    //first make sure that the relationships in relNames are joinable
    //===============================================================
    //1. take the union of sets of each relationship and compare it to the set 
    //formed by the relationships in relNames. If both the sets have equal number 
    //of attributes then, these relationships are joinable
    //example: Initially when adding relationships, each relationship has a 
    //singleton set containing itself.
    //A={A}, B={B}, C={C}
    //Now required to join A, B, the set out of relNames is {A,B}
    //and set by union of sets of relationship names is {A}U{B}={A,B}
    //so both are joinable,
    
    //had it been the case that B and C were already joined, then this would be 
    //the scenario
    //A={A}, B={B,C}, C={B,C}
    //and were required to join A and B, {A,B}
    //and A U B = {A,B,C} so the number of elements is not equal hence A and B
    //cannot be joined.
    
    if(!VerifyJoinable(relNames, numToJoin)) {
        cerr << "These relationships are not joinable" << endl;
        exit(0);
    }
    //
    double estResult=1;
    int andCount=0;
    int orCount=0;
    AndList *pt_=parseTree;
    OrList *ot_;
    bool first=true;
    while( pt_ != NULL ) {
        //parseTree->left is the Orlist
        ot_=pt_->left;
        double tR=0;
        tR=estimate_or( ot_, relNames, first );
        first=false;
        //cout << "est for this segment " << tR << endl;
        estResult*=tR;
        //parseTree->right is the And List i.e. more conditions on the right
        //for the join
        pt_ = pt_->rightAnd;
    }
    //cout << "Result CNF " << estResult << endl;
    return estResult;
}

double Statistics::estimate_or(OrList *orL, char **relNames, bool first) {
    OrList *tempOr;
    tempOr=orL;
    int count=0;
    //we also need to figure out whether these conditions are dependent or 
    //independent
    //if the same attribute is used through out, then these are not independent
    //and their selectivity factor needs to be added together instead of
    //(1-c1*c2*c3..) formula
    
    set<string> atts;
    
    while(tempOr!=NULL) {
        if(tempOr->left->left->code==NAME) {
            atts.insert( string(tempOr->left->left->value) );
        } else if( tempOr->left->right->code==NAME ) {
            atts.insert( string(tempOr->left->right->value) );
        }
        tempOr=tempOr->rightOr;
        count++;
    }
    
    //if at the end of the above loop atts contains only one attribute, it means
    //that these or conditions are independent, provided count is greater than 1
    
    //if it's just one condition,
    if( count == 1 ) {
        return estimate_(orL, relNames, false, first);
    } else {
        tempOr=orL;
        if( atts.size()>1 ) {
            double r=1.0;
            while(tempOr!=NULL) {
                r*=1-(estimate_(tempOr, relNames, true, first));
                tempOr=tempOr->rightOr;
            }
            return 1-r;
        } else {
            double r=0.0;
            while(tempOr!=NULL) {
                r+=estimate_(tempOr, relNames, true, first);
                tempOr=tempOr->rightOr;
            }
            return r;
        }
    }
}

double Statistics::estimate_(OrList *orL, char **relNames, bool multOr, bool first) {
    //first check if both the attributes are in the relations named in relNames
    //right now just assume they are correct inputs
    
    //check which attribute belongs to which relation and pull out the numbers 
    //for that attribute and relation
    double result=0.0;
    if( orL->left->code==EQUALS ) {
        int tR, tS, v;
        //case of an equi join
        if( orL->left->left->code == NAME && orL->left->right->code == NAME) {
            //it's an attribute name
            //lookup the name to find what relation it belongs to
            double tR, tS, v;
            tR=um[getRelName(string(orL->left->left->value))];
            tS=um[getRelName(string(orL->left->right->value))];
            //join sets would be used to join the sets and update the statistics
            
            for( auto it=sets[getRelName(string(orL->left->left->value))].begin();
                    it!=sets[getRelName(string(orL->left->left->value))].end();++it) {
                joinSets.insert(*it);
            }
            
            for( auto it=sets[getRelName(string(orL->left->right->value))].begin();
                    it!=sets[getRelName(string(orL->left->right->value))].end();++it) {
                joinSets.insert(*it);
            }
            
            //v is the max of distinct values for attributes in these two relations
            string attR=getRelName(string(orL->left->left->value))+string("_")+getAttName(string(orL->left->left->value));
            string attS=getRelName(string(orL->left->right->value))+string("_")+getAttName(string(orL->left->right->value));
            
            joinAtts.insert(getAttName(string(orL->left->left->value)));
            joinAtts.insert(getAttName(string(orL->left->right->value)));
            
            if(um[attR] > um[attS]) {
                v=um[attR];
            } else {
                v=um[attS];
            }
            result=((double)tR*tS)/v;
        } else {
            //a selection on equality, but not a join
            //the estimate in this case is tR/V(R, a), where
            //tR is the number of tuples in R and V(R,a) is the number of unique
            //values attribute 'a' has in relationship R
            
            //we need to find the relationship name to find out the number of
            //tuples this relationship  has
            //and also the attribute name to find out the number of unique values 
            //this attribute has
            
            //will store the relationship name the attribute belongs to
            string relName;
            string attName;
            //if left operand contains the name
            if( orL->left->left->code==NAME ) {
                relName=getRelName(orL->left->left->value);
                attName=getAttName(orL->left->left->value);
            } else if( orL->left->right->code==NAME ) {
                //if the right operand contains the name
                relName=getRelName(orL->left->right->value);
                attName=getAttName(orL->left->right->value);
            }
            
            for( auto it=sets[relName].begin(); it!=sets[relName].end();++it) {
                joinSets.insert(*it);
            }

            //first and single
            if( first && !multOr ) {
                result=um[relName]/um[relName+"_"+attName];
            } else if( first && multOr ) {
                //first and multiple ors
                
            } else if( !first && !multOr ) {
                //not first and single or
                result=1.0/um[relName+"_"+attName];
            } else if( !first && multOr ) {
                //not first but mult ors
                result=um[relName]/um[relName+"_"+attName];
                result=result/um[relName];
            }
        }
    } else {
        //an inequality, can either be a selection or a inequality join
        if( orL->left->left->code == NAME && orL->left->right->code == NAME) {
            //inequality join, i.e. both left and right operands are names 
            //of attributes from some relationship
        } else {
            //a selection, result is 1/3rd of the number of tuples in the 
            //relationship that contains this attribute
            //
            //will store the relationship name the attribute belongs to
            string relName;
            //if left operand contains the name
            if( orL->left->left->code==NAME ) {
                relName=getRelName(orL->left->left->value);
            } else if( orL->left->right->code==NAME ) {
                //if the right operand contains the name
                relName=getRelName(orL->left->right->value);
            }
            for( auto it=sets[relName].begin(); it!=sets[relName].end();++it) {
                joinSets.insert(*it);
            }            
            
            if( first && !multOr ) {
                result=um[relName]/3;
            } else if( first && multOr ) {
                
            } else if( !first && !multOr ) {
                result=1.0/3;
            } else if( !first && multOr ) {
                result=um[relName]/3;
                result=result/um[relName];
            }
        }
    }
    return result;
}

bool Statistics::VerifyJoinable(char **relNames, int numToJoin) {
    //first create a new set from the relations in relNames
    set<string> s;
    for(int i=0; i<numToJoin; i++) {
        s.insert(string(relNames[i]));
    }
    
    set<string> tempSet;
    set<string> r;
    
    //this is for initial state when all are singleton sets
    for(int i=0; i<numToJoin; i++) {
        set_union( tempSet.begin(), tempSet.end(), sets[relNames[i]].begin(), sets[relNames[i]].end(), inserter(r, r.begin()) );
        tempSet=r;
        //cout << "TempSet Size: " << tempSet.size() << endl;
        r=set<string>();
    }
    
//    set<string>::iterator si;
//    for(si=tempSet.begin(); si!=tempSet.end();++si) {
//        cout << "Elem " << *si << endl;
//    }
  
//    cout << "s size: " << s.size() << " t size " << tempSet.size() << endl;
    
    if( s.size()!=tempSet.size() ) {
        return false;
    }
    return true;
}


void Statistics::Print() {
    for ( auto it = relFields.begin(); it != relFields.end(); ++it ) {
        cout << it->first;  
        cout << " lookup:" << um[it->first] << endl;
        for(int i=0; i<it->second.size(); i++) {
            cout << "\t" << it->second[i];
            cout << " lookup:" << um[it->first+string("_")+it->second[i]] << endl;
        }
    }
}

string Statistics::CalcNewRelName(set<string> rels) {
    string t="";
    for(auto it=rels.begin(); it!=rels.end(); ++it) {
        t+=*it+"_";
    }
    return t.substr(0, t.size()-1);
}

string Statistics::getRelName(string relName) {
    unsigned pos=relName.find(".");
    //if there is a dot in the relName
    if( pos==string::npos ) {
        //means that . doesn't exist in the relName
        //so find out what relationship this attribute belongs to using 
        //fieldsRel
        return fieldsRel[relName];
    } else {
        //relationship name is the part before "."
        //it's also possible that this rel has been joined with other relations
        //in which case the correct relName would be the concatenation of the
        //names of the rels this rel has been joined to
        string relN=relName.substr(0,pos);
        return CalcNewRelName(sets[relN]);
    }
}

string Statistics::getAttName(string relName) {
    unsigned pos=relName.find(".");
    //if there is a dot in the relName
    if( pos==string::npos ) {
        //means that . doesn't exist in the relName
        //so the AttName is relName
        return relName;
    } else {
        //relationship name is the part after "."
        return relName.substr(pos+1);
    }
}