#include "InternalDBFileSorted.h"
#include "BigQ.h"
#include "InternalDBFileHeap.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <string.h>
using namespace std;

typedef struct SortInfo {
    OrderMaker *myOrder;
    int runLength;
} SortInfoT;

InternalDBFileSorted::InternalDBFileSorted() {
    buffsz=100;
    readRecNum=0;
    readPageNum=0;
    op = READ;
    om = new OrderMaker();
    fname=new char[1];
    input = new Pipe(buffsz);
    output = new Pipe(buffsz);
    bq=0;
    omCreated=false;
}

void InternalDBFileSorted::WriteMetaFile(char *filename, int rl, OrderMaker *order) {
    //1st line of meta file contains whether it's sorted or heap
    //2nd line run length
    //3rd line Num of attributes
    //other lines sort order attributes info    
    ofstream metafile((std::string(filename) + std::string(".meta")).c_str());
    if(metafile.is_open()) {
        metafile << "sorted" << endl;
        metafile << runLen << endl;
        metafile << om->toString();
        metafile.close();
    }
}

int InternalDBFileSorted::Create (char *f_path, fType f_type, void *startup) {
    internalDBH.Create(f_path, heap, NULL);
    fname=f_path;
    SortInfoT *si = (SortInfoT*)startup;
    om=si->myOrder;
    runLen=si->runLength;
    WriteMetaFile(f_path, runLen, om);
    return 1;
}

int InternalDBFileSorted::Open (char *f_path) {
    //open will reset the pointer to the start of the file
    internalDBH.Open(f_path);
    readPageNum=0;
    readRecNum=0;
    omCreated=false;
    fname=f_path;
    ifstream metafile((std::string(f_path) + std::string(".meta")).c_str());
    string line;
    if( metafile.is_open() ) {
        //first line file type
        getline(metafile, line);
        //second line run length
        getline(metafile, line);
        runLen = StringToNumber<int>(line);
        //third line numAtts
        getline(metafile, line);
        int numAtts = StringToNumber<int>(line);
        
        om->SetNumAtts(numAtts);
        //for each att, read two lines
        int temp;
        for(int i=0; i< numAtts; i++) {
            //i would be the array offset for WhichAtts and WhichTypes Arrays
            //1st line, contains the attribute number as per catalog file
            getline(metafile, line);
            temp = StringToNumber<int>(line);
            //2nd line, contains the type of attribute
            getline(metafile, line);
            if( line.compare("Int") == 0 ) {
                om->SetAttAndType(i, temp, Int);
            } else if(line.compare("Double") == 0 ) {
                om->SetAttAndType(i, temp, Double);
            } else 
                om->SetAttAndType(i, temp, String);
        }
        //at the end of this loop we have reconstructed the ordermaker class
    }
    return 1;
}

void InternalDBFileSorted::Load (Schema &f_schema, char *loadpath) {
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

void InternalDBFileSorted::MoveFirst () {
    if( op==WRITE ) {
        input->ShutDown();
        op=READ;
        TwoWayMerge();
        internalDBH.Close();
    }
    op=READ;
    readPageNum=0;
    readRecNum=0;
    omCreated=false;
    internalDBH.Open(fname);
}

int InternalDBFileSorted::Close () {
    if( op==WRITE ) {
        input->ShutDown();
        op=READ;
        TwoWayMerge();
        internalDBH.Close();
    }
    omCreated=false;
    return 1;
}

void InternalDBFileSorted::Add (Record &rec) {
    //for the heap file, we would directly add the record to the internal page
    //but here we just add it to the BigQs input pipe
    if( op==READ ) {
        //last operation was read operation, which means that BigQ class is empty
        //now,
        //add the record to the input pipe of BigQ class
        
        //BigQ is either uninitialized or empty
        if( bq==0 )
            //initialize BigQ
            bq = new BigQ(*input, *output, *om, runLen);
    }
    input->Insert(&rec);
    omCreated=false;
    op=WRITE;
}

int InternalDBFileSorted::GetNext (Record &fetchme) {
    //keep track using read rec num
    //will use internalDBHeap file to get Next Record
    if( op==WRITE ) {
        input->ShutDown();
        op=READ;
        TwoWayMerge();
        //close and reload
        internalDBH.Close();
        internalDBH.Open(fname);
        //now skip recNum of records
        //this needs to be done because after the dbfile is close and open again
        //the internal pointer is reset.
        internalDBH.Seek(readPageNum, readRecNum);
        //must have skipped readRecNum # of records
    }
    op=READ;
    if( internalDBH.NumberPages() > 0 ) {
        if( internalDBH.GetNext(fetchme) == 1 ) {
            readPageNum=internalDBH.readPageNum;
            readRecNum=internalDBH.readRecNum;
            return 1;
        } else
            return 0;
    } else {
        return 0;
    }
}

int InternalDBFileSorted::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    //here we build two orderMaker objects.
    //one would provide sorting info for the record fetched from the file
    //while the other would provide sorting info for the literal record
    //We can use these two orderMakers with CE.Compare function that take two 
    //records and two order makers, one for each.
    //This is important to do here, because,
    //CE.Compare(Record, Record, OrderMaker) only takes the one OrderMaker class
    //If the OrderMaker is built upto specs as per the catalog file, then the 
    //Compare function also expects the record to be build as per the catalog 
    //file.
    //While this will work with fetchme record, because it'll be pulled from file,
    //this OrderMaker won't work with Literal record becuase literal doesn't have 
    //all the fields in particular order most of the times
    OrderMaker *recOm=new OrderMaker();
    OrderMaker *litOm=new OrderMaker();
    if( omCreated==false ) {
        bool found=false;

        for( int i=0; i < om->numAtts; i++ ) {
            //for each attribute in our sortfile order maker,
            //search in cnf expression.
            found = false;
            for( int j=0; j< cnf.numAnds; j++ ) {
                //1.check if each of this expression has just one subexpression
                if( cnf.orLens[j]==1 ){
                    //only then proceed
                    //2.check if it's this attribute
                    if( //check if it's the same attribute number
                            cnf.orList[j][0].whichAtt1==om->whichAtts[i] 
                            //and also that this attribute number is not that of a
                            //literal
                            && ( cnf.orList[j][0].operand1==Left 
                                || cnf.orList[j][0].operand1==Right )
                            //and also the other operand must be a literal
                            && cnf.orList[j][0].operand2==Literal
                            //and also it's an equal to comparison
                            && cnf.orList[j][0].op==Equals
                            ) {
                        //if that's the case then add this attribute to the order 
                        recOm->SetAttAndType(recOm->numAtts, om->whichAtts[i], om->whichTypes[i]);
                        recOm->SetNumAtts(recOm->numAtts+1);

                        litOm->SetAttAndType(litOm->numAtts, j, om->whichTypes[i]);
                        litOm->SetNumAtts(litOm->numAtts+1);

                        found=true;
                        break;
                    } else if( //check if it's the same attribute number
                            cnf.orList[j][0].whichAtt2==om->whichAtts[i] 
                            //and also that this attribute number is not that of a
                            //literal
                            && ( cnf.orList[j][0].operand2==Left 
                                || cnf.orList[j][0].operand2==Right )
                            //and also the other operand must be a literal
                            && cnf.orList[j][0].operand1==Literal
                            //and also it's an equal to comparison
                            && cnf.orList[j][0].op==Equals
                            ) {
                        //if that's the case then add this attribute to the order 
                        recOm->SetAttAndType(recOm->numAtts, om->whichAtts[i], om->whichTypes[i]);
                        recOm->SetNumAtts(recOm->numAtts+1);

                        litOm->SetAttAndType(litOm->numAtts, j, om->whichTypes[i]);
                        litOm->SetNumAtts(litOm->numAtts+1);

                        found=true;
                        break;
                    }
                }
            }
            if(found==false) {
                break;
            }
        }
    }
    // we have now an ordermaker to help us in Bin Search
    //if this queryOM doesn't have any attributes, then do the linear search 
    //instead
    
    if( op==WRITE ) {
        input->ShutDown();
        op=READ;
        TwoWayMerge();
        //close and reload
        internalDBH.Close();
        internalDBH.Open(fname);
        internalDBH.Seek(readPageNum, readRecNum);
    }
    
    op = READ;
    ComparisonEngine ce;
    if( recOm->numAtts==0 || omCreated==true) {
        while(GetNext(fetchme)) {
            if(ce.Compare(&fetchme, &literal, &cnf)) {
                readPageNum=internalDBH.readPageNum;
                readRecNum=internalDBH.readRecNum;
                return 1;
            }
        }
        return 0;
    } else {
        //do the bin search here
        int initStartPage = internalDBH.readPageNum;
        int initStartRec = internalDBH.readRecNum;
        int start = internalDBH.readPageNum;
        int end = internalDBH.NumberPages()-2;
        int mid = start + ((end - start) / 2);
        
        internalDBH.Seek(mid);
        while(start<end) {
            GetNext(fetchme);
            if( ce.Compare(&fetchme, recOm, &literal, litOm ) > 0 ) {
                //continue search on left half
                //start would stay the same
                //mid-1 would become end
                end=mid-1;
                mid = start + ((end - start) / 2);
                //if mid and start are same and turn out to be the first page for
                //binsearch, we skip the initial # of records on that page
                if( mid==start && mid==initStartPage ) {
                    internalDBH.Seek(mid, initStartRec);
                } else {
                    internalDBH.Seek(mid);
                }
            } if( ce.Compare(&fetchme, recOm, &literal, litOm ) < 0 ) {
                //continue search on right half
                start=mid+1;
                //end would stay the same
                mid = start + ((end - start) / 2);
                if( mid==start && mid==initStartPage ) {
                    internalDBH.Seek(mid, initStartRec);
                } else {
                    internalDBH.Seek(mid);
                }
            } else {
                //both are equal
                //in this case check to see if the record matches the cnf as well
                //if it matches the cnf return true,
                //otherwise keep moving forward
                if( ce.Compare(&fetchme, &literal, &cnf) ) {
                    readPageNum=internalDBH.readPageNum;
                    readRecNum=internalDBH.readRecNum;
                    omCreated=true;
                    return 1;
                } else {
                    //keep looking forward till the end page is reached
                    while( GetNext(fetchme)==1 && internalDBH.readPageNum <= end ) {
                        //stop when ordermaker no longer satisfies the condition
                        if( ce.Compare(&fetchme, recOm, &literal, litOm ) == 0 ) {
                            if( ce.Compare(&fetchme, &literal, &cnf) ) {
                                readPageNum=internalDBH.readPageNum;
                                readRecNum=internalDBH.readRecNum;
                                omCreated=true;
                                return 1;
                            }                         
                        } else {
                            return 0;
                        }
                    }
                    //if nothing found by the end of this loop return 0
                    return 0;
                }
                return 0;
            }
        }
        
        //if we reach here, start, mid and end are all the same
        int curPageBeingRead=internalDBH.readPageNum;
        if( start==mid && mid==end ) {
            GetNext(fetchme);
            if( ce.Compare(&fetchme, recOm, &literal, litOm ) < 0 ) {
            //if fetchme < literal: only look at rest of the records on this page
                curPageBeingRead=internalDBH.readPageNum;
                //since only need to read records from this page
                //check if the page number changes or GetNext returns a zero,
                //i.e. the case when the current page being read is actually
                //the page page on file
                while( internalDBH.readPageNum==curPageBeingRead && GetNext(fetchme)==1) {
                    //if we find an equal record, means it satisfies the query
                    //ordermaker, now make sure it satisfies the cnf aswell
                    if( ce.Compare(&fetchme, recOm, &literal, litOm ) == 0 ) {
                        if( ce.Compare(&fetchme, &literal, &cnf) ) {
                            readPageNum=internalDBH.readPageNum;
                            readRecNum=internalDBH.readRecNum;                            
                            omCreated=true;
                            return 1;
                        }
                    }
                }
                //if by the end of this loop, we don't find a record, it means 
                //there's no matching record, so return a zero
                return 0;
            }
            else if( ce.Compare(&fetchme, recOm, &literal, litOm ) > 0 ) {
                //if fetchme > literal only look at all the records on the previous 
                //page, except maybe the first record on the previous page
            
                //here we have to move 1 page backward since the literal can be 
                //in the records of the previous page, except the first record
                //how ever, we need to be careful here.
                //Consider the case when the file has 50 pages
                //and we do a sequence of regular GetNext Operations, and we are 
                //currently at page 22.
                //The next GetNext operation is one involving binary search, 
                //so the binsearch should span pages 22-50.
                //Consider that the binsearch sequence resulted in start,mid,end:
                //all equal to 22.
                //and the first record we fetch on this page is greater than the 
                //literal.
                //In this situation we have to look at the previous page, which 
                //is page 21. But page 21 doesn't span our search. So there's no 
                //chance to find a record in this case.
                
                //note the current page being read
                curPageBeingRead=internalDBH.readPageNum;
                
                //if the current page being read is the same as the initial page
                //set at the start of binsearch return a zero
                if( curPageBeingRead==initStartPage ) {
                    return 0;
                } else {
                    curPageBeingRead=curPageBeingRead-1;
                    internalDBH.Seek(curPageBeingRead);
                    while( internalDBH.readPageNum==curPageBeingRead && GetNext(fetchme)==1) {
                        //if we find an equal record, means it satisfies the query
                        //ordermaker, now make sure it satisfies the cnf aswell
                        if( ce.Compare(&fetchme, recOm, &literal, litOm ) == 0 ) {
                            if( ce.Compare(&fetchme, &literal, &cnf) ) {
                                readPageNum=internalDBH.readPageNum;
                                readRecNum=internalDBH.readRecNum;
                                omCreated=true;
                                return 1;
                            }
                        }
                    }
                    return 0;
                }
            }
        }
        return 0;
    }
}

//wrong implementation
int InternalDBFileSorted::NumberPages() {
    //return internalFile.GetLength();
    return 1;
}

void InternalDBFileSorted::TwoWayMerge() {
    Record fromQueue;
    Record fromFile;
    
    InternalDBFileHeap tmpDBHOut;    
    tmpDBHOut.Create((char *)(std::string(fname)+std::string(".tmp")).c_str(), heap, NULL);

    if( internalDBH.NumberPages() > 0 ) {
        internalDBH.MoveFirst();
    }
    
    int resQ=output->Remove(&fromQueue);
    int resD=internalDBH.GetNext(fromFile);
    
    ComparisonEngine ce;
    while( resQ==1 && resD==1 ) {
        if( ce.Compare(&fromQueue, &fromFile, om) < 0 ) {
            tmpDBHOut.Add(fromQueue);
            resQ=output->Remove(&fromQueue);
        } else {
            tmpDBHOut.Add(fromFile);
            resD=internalDBH.GetNext(fromFile);
        }
    }

    //
    if( resQ==1 && resD==0 ) {
        tmpDBHOut.Add(fromQueue);
    } else if( resQ==0 && resD==1 ) {
        tmpDBHOut.Add(fromFile);
    }    
    
    //at the end of this loop one of the sources would be consumed, so iterate 
    //over the rest of the records wherever available
    while( output->Remove(&fromQueue) == 1 ) {
        tmpDBHOut.Add(fromQueue);
    }
    
    while( internalDBH.GetNext(fromFile) == 1 ) {
        tmpDBHOut.Add(fromFile);
    }
    
    tmpDBHOut.Close();
    
    remove( fname );
    remove( (std::string(fname)+std::string(".tmp.meta")).c_str() );
    rename( (std::string(fname)+std::string(".tmp")).c_str(), fname );
    WriteMetaFile(fname, runLen, om);
}

InternalDBFileSorted::~InternalDBFileSorted() {
    //delete om;
    delete input;
    delete output;
}