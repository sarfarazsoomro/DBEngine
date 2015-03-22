#ifndef INTERNALDBFILE_H
#define INTERNALDBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

class InternalDBFile {
    friend class Page;
private:
public:
	InternalDBFile ();

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