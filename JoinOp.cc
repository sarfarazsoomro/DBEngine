#include "JoinOp.h"
#include "BigQ.h"
//this uses a new function in Pipe::GetNext which just gets the next rec in 
//succession but doesn't remove it.
//This helps us in writing the join very concisely and cleanly.
//http://www.dcs.ed.ac.uk/home/tz/phd/thesis/node20.htm
void* JoinProcessor(void* data) {
    Pipe *inPipeL= ((JoinOpDataT*)data)->inPipeL;
    Pipe *inPipeR= ((JoinOpDataT*)data)->inPipeR;
    Pipe *outPipe= ((JoinOpDataT*)data)->outPipe;
    CNF *selOp=((JoinOpDataT*)data)->selOp;
    Record *literal=((JoinOpDataT*)data)->literal;
    int runLen=((JoinOpDataT*)data)->runLen;
    Record mergedRecord;
    
    OrderMaker omL;
    OrderMaker omR;
    
    Record t;
    inPipeL->GetNext(&t);
    int numAttsL=t.GetNumAtts();
    inPipeR->GetNext(&t);
    int numAttsR=t.GetNumAtts();

    inPipeL->ResetGetNext();
    inPipeR->ResetGetNext();
    
    int numTotalAtts=numAttsL+numAttsR;

    //create the att list for merged record
    int *attsToKeep=new int[numTotalAtts];
    int curElem=0;
    for(int i=0; i< numAttsL; i++) {
        attsToKeep[curElem++]=i;
    }
    for(int i=0; i< numAttsR; i++) {
        attsToKeep[curElem++]=i;
    }
    
    
    //if ordermakers are created
    if( selOp->GetSortOrders(omL, omR) ) {
        //feed the left to the left bigq
        Pipe *outPipeL=new Pipe(1000);
        BigQ *bqL=new BigQ(*inPipeL, *outPipeL, omL, runLen);
        
        //feed the right to the right bigq
        Pipe *outPipeR=new Pipe(1000);
        BigQ *bqR=new BigQ(*inPipeR, *outPipeR, omR, runLen);
        
        //next we start taking items out of bigq and start putting them out in 
        //the output pipe
        Record left;
        Record right;
        Record left_d;
        Record right_d;
        Record mergedRecord;
                
        ComparisonEngine ce;
        int compResult;
        
        int moreInL;
        int moreInR;
        
        moreInL=outPipeL->Remove(&left);
        moreInR=outPipeR->Remove(&right);
                
        while( moreInL==1 && moreInR==1 ) {
            compResult=ce.Compare(&left, &omL, &right, &omR);
            if( compResult < 0 ) {
                //left is less than right
                //so get the next record in the left list
                moreInL=outPipeL->Remove(&left);
            } else if( compResult > 0 ) {
                //left is greater than right
                //so get the next record in the right list
                moreInR=outPipeR->Remove(&right);
            } else {
                //both are equal
                //insert left.right in the output pipe
                //keep matching new records first for this record in left with 
                //more records on right and then for the record on right with 
                //more records on the left
                mergedRecord.MergeRecords(&left, &right, numAttsL, numAttsR, attsToKeep, numTotalAtts, numAttsL);
                outPipe->Insert(&mergedRecord);
                //now keep the record on left and move forward on the right
                //and keep adding till it matches the current record on left
                moreInR=outPipeR->GetNext(&right_d);
                
                while( moreInR==1 && ce.Compare(&left, &omL, &right_d, &omR)==0 ) {
                    mergedRecord.MergeRecords(&left, &right_d, numAttsL, numAttsR, attsToKeep, numTotalAtts, numAttsL);
                    outPipe->Insert(&mergedRecord);
                    moreInR=outPipeR->GetNext(&right_d);
                }
                
                //now keep the record on right and move forward on the left
                //and keep adding till it matches the current record on right
                moreInL=outPipeL->GetNext(&left_d);
                
                while( moreInL==1 && ce.Compare(&left_d, &omL, &right, &omR)==0 ) {
                    mergedRecord.MergeRecords(&left_d, &right, numAttsL, numAttsR, attsToKeep, numTotalAtts, numAttsL);
                    outPipe->Insert(&mergedRecord);
                    moreInL=outPipeL->GetNext(&left_d);
                }
                
                //get next records
                moreInL=outPipeL->Remove(&left);
                moreInR=outPipeR->Remove(&right);
            }
        }
    } else {
        Record rec, left, right;
        ComparisonEngine ce;
        int result;
        
        int len=10;
        char *s = new char[len];
        static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
    
        for (int i = 0; i < len; ++i) {
            s[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
        }

        s[len] = 0;        
        
        //create a temp db heap file to hold the contents of the right pipe        
        std::string tmpFName = std::string("./temp/")+std::string(s)+std::string(".bin");
        
        InternalDBFileHeap idbh;
        idbh.Create((char*)tmpFName.c_str(), heap, NULL);
        while( inPipeR->Remove(&rec) ) {
            idbh.Add(rec);
        }
        idbh.Close();
        
        InternalDBFileHeap idbh2;
        idbh2.Open((char*)tmpFName.c_str());
        while( inPipeL->Remove(&left) ) {
            while( idbh2.GetNext(right) ) {
                result=ce.Compare(&left, &right, selOp);
                if( result!=0 ) {
                    mergedRecord.MergeRecords(&left, &right, numAttsL, numAttsR, attsToKeep, numTotalAtts, numAttsL);
                    outPipe->Insert(&mergedRecord);
                }
            }
            idbh2.MoveFirst();
        }
        idbh2.Close();
        remove((char *)tmpFName.c_str());
        remove((char *)(tmpFName+std::string(".meta")).c_str());
    }
    outPipe->ShutDown();
}

//this uses slightly more mem than the above because of the use of vectors
//as additional buffers to do cross product betwen matching subsets
void* JoinProcessor1(void* data) {
    Pipe *inPipeL= ((JoinOpDataT*)data)->inPipeL;
    Pipe *inPipeR= ((JoinOpDataT*)data)->inPipeR;
    Pipe *outPipe= ((JoinOpDataT*)data)->outPipe;
    CNF *selOp=((JoinOpDataT*)data)->selOp;
    Record *literal=((JoinOpDataT*)data)->literal;
    int runLen=((JoinOpDataT*)data)->runLen;
    
    OrderMaker omL;
    OrderMaker omR;
    
    //if ordermakers are created
    if( selOp->GetSortOrders(omL, omR) ) {
        //feed the left to the left bigq
        Pipe *outPipeL=new Pipe(1000);
        BigQ *bqL=new BigQ(*inPipeL, *outPipeL, omL, runLen);
        
        //feed the right to the right bigq
        Pipe *outPipeR=new Pipe(1000);
        BigQ *bqR=new BigQ(*inPipeR, *outPipeR, omR, runLen);
        
        //next we start taking items out of bigq and start putting them out in 
        //the output pipe
        Record left;
        Record right;
                
        ComparisonEngine ce;
        int compResult;
        
        int moreInL;
        int moreInR;
        
        moreInL=outPipeL->Remove(&left);
        moreInR=outPipeR->Remove(&right);
        
        int numAttsL=left.GetNumAtts();
        int numAttsR=right.GetNumAtts();
        
        int numTotalAtts=numAttsL+numAttsR;
        
        //create the att list for merged record
        int *attsToKeep=new int[numTotalAtts];
        int curElem=0;
        for(int i=0; i< numAttsL; i++) {
            attsToKeep[curElem++]=i;
        }
        for(int i=0; i< numAttsR; i++) {
            attsToKeep[curElem++]=i;
        }
        
        vector<Record> lBuf;
        vector<Record> rBuf;
        Record mergedRecord;
        Record oldLeft;
        Record oldRight;
        
        while( moreInL==1 && moreInR==1 ) {
            compResult=ce.Compare(&left, &omL, &right, &omR);
            if( compResult < 0 ) {
                //left is less than right
                //so get the next record in the left list
                moreInL=outPipeL->Remove(&left);
            } else if( compResult > 0 ) {
                //left is greater than right
                //so get the next record in the right list
                moreInR=outPipeR->Remove(&right);
            } else {
                lBuf.push_back(left);
                rBuf.push_back(right);
                
                oldLeft.Copy(&left);
                
                moreInL=outPipeL->Remove(&left);
                while(moreInL==1 && ce.Compare(&oldLeft, &left, &omL)==0) {
                    lBuf.push_back(left);
                    oldLeft.Copy(&left);
                    moreInL=outPipeL->Remove(&left);
                }
                
                oldRight.Copy(&right);
                
                moreInR=outPipeR->Remove(&right);
                while(moreInR==1 && ce.Compare(&oldRight, &right, &omR)==0) {
                    rBuf.push_back(right);
                    oldRight.Copy(&right);
                    moreInR=outPipeR->Remove(&right);
                }
                
                for(int i=0; i<lBuf.size(); i++) {
                    for(int j=0; j<rBuf.size(); j++) {
                        mergedRecord.MergeRecords(&lBuf[i], &rBuf[j], numAttsL, numAttsR, attsToKeep, numTotalAtts, numAttsL );
                        outPipe->Insert(&mergedRecord);
                    }
                }
                
                //clear the buffers
                lBuf.clear();
                rBuf.clear();
            }
        }        
    } else {
        //do a nested block loop join
    }
    
    outPipe->ShutDown();
}

void Join::Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) {
    runLen=10;
    JoinOpDataT *opData = new JoinOpDataT();
    opData->inPipeL=&inPipeL;
    opData->inPipeR=&inPipeR;
    opData->outPipe=&outPipe;
    opData->selOp=&selOp;
    opData->literal=&literal;
    opData->runLen=runLen;
    pthread_create(&thread, NULL, &JoinProcessor, (void*)opData);
}

void Join::WaitUntilDone () {
    pthread_join (thread, NULL);
}

void Join::Use_n_Pages (int runlen) {
    runLen=runlen;
}