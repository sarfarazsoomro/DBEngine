#ifndef INTERNALDBFILE_HEAP_H
#define INTERNALDBFILE_HEAP_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "InternalDBFile.h"

class InternalDBFileHeap: public InternalDBFile {
    friend class Page;
    friend class InternalDBFileSorted;
private:
    Record internalRecord;
    Page internalPage;
    File internalFile;
    int pageCount;
    int writePageNum;
    int readPageNum;
    int readRecNum;
    OperationType op;
    
public:
	InternalDBFileHeap ();

        //
        virtual int Create (char *fpath, fType file_type, void *startup);
        //
	virtual int Open (char *fpath);
        //
	virtual int Close ();

	virtual void Load (Schema &myschema, char *loadpath);

	virtual void MoveFirst ();
        
        virtual void Seek(int pageNum, int recNum=0);
        
        //
	virtual void Add (Record &addme);
	virtual int GetNext (Record &fetchme);
	virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal);
        //
        virtual int NumberPages();
};
#endif