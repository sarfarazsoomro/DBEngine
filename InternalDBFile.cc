#include "InternalDBFile.h"
#include <iostream>

using namespace std;

InternalDBFile::InternalDBFile() {

}

int InternalDBFile::Create(char* fpath, fType file_type, void* startup) {
    return 0;
}

int InternalDBFile::Open (char *fpath) {
    
}

int InternalDBFile::Close () {
    
}

void InternalDBFile::Load (Schema &myschema, char *loadpath) {
    
}

void InternalDBFile::MoveFirst () {
    
}

void InternalDBFile::Add (Record &addme) {
    
}

int InternalDBFile::GetNext (Record &fetchme) {
    
}

int InternalDBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    
}

int InternalDBFile::NumberPages() {
    
}