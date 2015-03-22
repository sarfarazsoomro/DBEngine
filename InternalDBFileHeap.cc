#include "InternalDBFileHeap.h"
#include <iostream>
#include <fstream>
using namespace std;

InternalDBFileHeap::InternalDBFileHeap() {
    pageCount=0;
    writePageNum=0;
    readPageNum=0;
    readRecNum=0;
    op = READ;
}

int InternalDBFileHeap::Create (char *f_path, fType f_type, void *startup) {
    internalFile.Open(0, f_path);
    ofstream metafile((std::string(f_path) + std::string(".meta")).c_str());
    if(metafile.is_open()) {
        metafile << "heap";
        metafile.close();
    }
    return 1;
}

int InternalDBFileHeap::Open (char *f_path) {
    internalFile.Open(1, f_path);
    //Open will reset the internal pointer
    MoveFirst();
    return 1;
}

void InternalDBFileHeap::Load (Schema &f_schema, char *loadpath) {
    //the Add function will automatically load up the correct page to which the 
    //record is to be added using the writePageNum variable
    op = WRITE;
    Record myRec;
    FILE *tableFile = fopen (loadpath, "r");
    while (myRec.SuckNextRecord (&f_schema, tableFile) == 1) {
        Add(myRec);
    }
    
    //the last page to which the records are appended is not written 
    //out to the disk. We don't need to flush it out to the disk now because it 
    //will automatically be written out on any subsequent call to GetNext, Close
    //or Add
}

void InternalDBFileHeap::MoveFirst () {
    if(op==WRITE && internalPage.numRecs > 0) {
        internalFile.AddPage(&internalPage, writePageNum);
    }
    
    readPageNum=0;
    readRecNum=0;    
    
    if( internalFile.GetLength() > 0 ) {
        internalFile.GetPage(&internalPage, readPageNum);
    }
}

int InternalDBFileHeap::Close () {
    //if the last operation was a write, only then flush this page out to the disk.
    //this shouldn't result in duplicate pages because File::AddPage function takes 
    //offset as page number argument
    if(op==WRITE && internalPage.numRecs > 0) {
        internalFile.AddPage(&internalPage, writePageNum);
    }
    if( internalFile.Close() > 0 )
        return 1;
    else 
        return 0;
}

void InternalDBFileHeap::Add (Record &rec) {
    //if a read was performed before add, then the page that was read from is 
    //probably different than the one to which write is to be performed. A write
    //is performed always on the last page (pageCount-1) and can be tracked by 
    //writePageNum variable.
    //Read can be tracked by readPageNum.
    //if in this case, writePageNum and readPageNum are different and the last 
    //operation was a read, then, writePageNum needs to be loaded into internalPage
    if( op==READ && readPageNum!=writePageNum ) {
        internalFile.GetPage(&internalPage, writePageNum);
    }
    //set the operation to write
    op=WRITE;
    //the Add function works in a way that whenever the internalPage is detected 
    //full it's flushed out to the disk.
    //after this the record which helped detecting the full page is added to the 
    //page. So we can safely assume that once records have started to be written 
    //to the page, there will always be at least one record in the page.
    int result;
    result = internalPage.Append(&rec);
    if(pageCount==0) {
        //the case when first record is added
        pageCount++;
    }
    if(result==0) {
        //the page is full & needs to be written to the file.
        internalFile.AddPage(&internalPage, writePageNum);
        pageCount++;
        writePageNum++;
        internalPage.EmptyItOut();
        internalPage.Append(&rec);
    }
}

int InternalDBFileHeap::GetNext (Record &fetchme) {
    //This function is to return the next record.
    //When the dbfile object is created, the pointer points to 
    //first record in first page
    //Regardless of what has been inserted into the page/file,
    //this pointer is only incremented in GetNext operation.
    //So if there is a sequence of pending writes in the buffer, then that 
    //needs to be written back to disk and then the page corresponding page for
    //GetNext needs to be fetched again
    
    if(op==WRITE && internalPage.numRecs > 0) {
        internalFile.AddPage(&internalPage, writePageNum);
    }    
    
    //and now if the last operation was a write and to a different page, load
    //the page pointed to by readPageNum & move forward by readRecNum records
    if(op==WRITE && writePageNum!=readPageNum) {
        internalFile.GetPage(&internalPage, readPageNum);
        for(int i=0; i<readRecNum; i++) {
            internalPage.GetFirst(&internalRecord);
        }
    }
    
    //set the operation to read
    op = READ;
    if(internalPage.GetFirst(&fetchme)==0) {
        //fetch the next page,
        //it may be a case here that this is the last record in the last page
        //so do checking for that as well
        //next page counter
        readPageNum++;
        //reset the record number to 0
        readRecNum=0;
        if( readPageNum >= (internalFile.GetLength()-1)) {
            //no more pages to be read
            return 0;
        } else {
            //fetch the next page
            internalFile.GetPage(&internalPage, readPageNum);
            if( internalPage.GetFirst(&fetchme)==0 ) {
                return 0;
            } else {
                readRecNum++;
                return 1;
            }
        }
    } else {
        readRecNum++;
        return 1;
    }
}

int InternalDBFileHeap::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    op = READ;
    ComparisonEngine ce;
    while(GetNext(fetchme)) {
        if(ce.Compare(&fetchme, &literal, &cnf)) {
            return 1;
        }
    }
    return 0;
}

void InternalDBFileHeap::Seek(int pageNum, int recNum) {
    if( pageNum < internalFile.GetLength()-1) {
        //fetch the next page
        op=READ;
        readPageNum=pageNum;
        readRecNum=0;
        internalFile.GetPage(&internalPage, readPageNum);
        Record temp;
        for(int i=0; i<recNum; i++) {
            internalPage.GetFirst(&temp);
        }
    }
}

int InternalDBFileHeap::NumberPages() {
    return internalFile.GetLength();
}