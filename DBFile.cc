#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include <string>
#include <fstream>
#include <iostream>

DBFile::DBFile () {
}

int DBFile::Create (char *f_path, fType f_type, void *startup) {
    if( f_type==heap ) {
        idbf=&idbfh;
    } else if( f_type==sorted ) {
        idbf=&idbfs;
    }
    idbf->Create(f_path, f_type, startup);
    return 1;
}

int DBFile::Open (char *f_path) {
    ifstream metafile((std::string(f_path) + std::string(".meta")).c_str());
    string str_ftype;
    if(metafile.is_open()) {
        getline(metafile, str_ftype);
    }
    metafile.close();
    
    if( str_ftype.compare("heap") == 0 ) {
        idbf=&idbfh;
    } else if( str_ftype.compare("sorted") == 0 ) {
        idbf=&idbfs;
    }
    return idbf->Open(f_path);
}

void DBFile::Load (Schema &f_schema, char *loadpath) {
    idbf->Load(f_schema, loadpath);
}

void DBFile::MoveFirst () {
    idbf->MoveFirst();
}

int DBFile::Close () {
    return idbf->Close();
}

void DBFile::Add (Record &rec) {
    idbf->Add(rec);
}

int DBFile::GetNext (Record &fetchme) {
    return idbf->GetNext(fetchme);
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    return idbf->GetNext(fetchme, cnf, literal);
}

int DBFile::NumberPages() {
    return idbf->NumberPages();
}