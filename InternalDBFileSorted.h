#ifndef INTERNALDBFILE_SORTED_H
#define INTERNALDBFILE_SORTED_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "InternalDBFile.h"
#include "BigQ.h"
#include "Pipe.h"
#include "InternalDBFileHeap.h"

class InternalDBFileSorted: public InternalDBFile {
    friend class Page;
private:
    char* fname;
    InternalDBFileHeap internalDBH;
    int readPageNum;
    int readRecNum;
    OperationType op;
    OrderMaker *om;
    int runLen;
    BigQ *bq;
    int buffsz; // pipe cache size
    Pipe *input;
    Pipe *output;
    bool omCreated;
    void TwoWayMerge();
    void WriteMetaFile(char* fname, int runLen, OrderMaker *order);
public:
	InternalDBFileSorted ();
        ~InternalDBFileSorted ();
        //
        virtual int Create (char *fpath, fType file_type, void *startup);
        //
	virtual int Open (char *fpath);
        //
	virtual int Close ();

	virtual void Load (Schema &myschema, char *loadpath);

	virtual void MoveFirst ();
        //
	virtual void Add (Record &addme);
	virtual int GetNext (Record &fetchme);
	virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal);
        //
        virtual int NumberPages();
};
#endif