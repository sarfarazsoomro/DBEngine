#include "BigQ.h"
#include <string>
#include <stdlib.h>

//http://stackoverflow.com/questions/440133/how-do-i-create-a-random-alpha-numeric-string-in-c
char * randStr(const int len) {
    char *s = new char[len];
    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
    
    for (int i = 0; i < len; ++i) {
        s[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
    }

    s[len] = 0;
    return s;    
}

struct CompareRecords {
  CompareRecords(OrderMaker& om) {
      sortOrder = om;   
  }
  
  bool operator()(const Record& r1, const Record& r2) {
    ComparisonEngine ce;
    int result = ce.Compare((Record*)&r1, (Record*)&r2, &sortOrder);
    if( result > 0 ) 
        return false;
    else 
        return true;
  }

private:
  OrderMaker sortOrder;
};

struct CompareRecords1 {
  CompareRecords1(OrderMaker& om) {
      sortOrder = om;   
  }
  
  bool operator()(const Record& r1, const Record& r2) {
    ComparisonEngine ce;
    int result = ce.Compare((Record*)&r1, (Record*)&r2, &sortOrder);
    if( result > 0 ) 
        return true;
    else 
        return false;
  }

private:
  OrderMaker sortOrder;
};

typedef struct BigQTData {
    Pipe *in;
    Pipe *out;
    OrderMaker sortorder;
    int runlen;
} BigQTDataT;

void* BigQProcessor(void* obj) {
    Pipe* in= ((BigQTDataT*)obj)->in;
    Pipe* out= ((BigQTDataT*)obj)->out;
    OrderMaker sortorder= ((BigQTDataT*)obj)->sortorder;
    int runlen=((BigQTDataT*)obj)->runlen;

   // read data from in pipe sort them into runlen pages
    Record temp;
    int count=0;
    int runlencnt=0;
    
    Page internalPage;
    Page outPage;
    File outFile;
    
    std::string fileName=std::string("./temp/")+std::string(randStr(10))+std::string(".bin");
    outFile.Open(0, (char *)fileName.c_str() );
    int result=1;
    int pageNumInOutFile=0;
    //use this to store run ranges
    vector< pair<int, int> > runRanges;
    int start, end;
    vector<Record> recArr;
    //keep removing records from the input pipe till all of them are consumed
    while(in->Remove(&temp)==1) {
        //add to which run this record belongs will help with phase 2 of tpmms
        temp.runNum=runRanges.size();
        
        //add it to the vector first because adding it to the page will consume
        //the record
        recArr.push_back(temp);
        //try adding it to the page
        result=internalPage.Append(&temp);
        if( result==1 ) {
            //this is giving a segmentation fault, because the record is already
            //consumed by internalPage.Append() function
            //recArr.push_back(temp);
        } else {
            //since the last element added was not added to the page so pop it 
            //out of the vector
            recArr.pop_back();
            //means the runlen is reached
            runlencnt++;
            if(runlencnt==runlen) {
                //it's time to sort these records
                std::stable_sort(recArr.begin(), recArr.end(), CompareRecords(sortorder));
                //now recArr has the sorted records
                //now need to write all of these records to a File Object on disk
                int outPageAppendResult=1;
                start=pageNumInOutFile;
                for(int i=0; i<recArr.size(); i++) {
                    outPageAppendResult=outPage.Append(&recArr[i]);
                    if(outPageAppendResult==0) {
                        //write the page out to file
                        outFile.AddPage(&outPage, pageNumInOutFile);
                        //increment this page number
                        pageNumInOutFile++;
                        //clear the page so that subsequent records can be added
                        outPage.EmptyItOut();
                        //also add to page the last record that was rejected entry
                        //into page
                        outPage.Append(&recArr[i]);
                    }
                }
                //at the end of this loop there will always be atleast one record
                //in the page that has yet to be committed to the disk
                //this page number would also mark the end of current run
                outFile.AddPage(&outPage, pageNumInOutFile);
                
                //clear the outPage
                outPage.EmptyItOut();
                
                end=pageNumInOutFile;
                runRanges.push_back({start, end});
                
                //increment the page num here so that it doesn't get overridden 
                //by a subsequent run's page
                pageNumInOutFile++;
                
                //empty the vector, because it has been sorted and written back 
                //to the disk and now 
                recArr.clear();
                //also reset the runlencnt for next set of runlen
                //pages
                runlencnt=0;
            }
            //empty this page out so it can be used for next iteration
            internalPage.EmptyItOut();
            //and add to the vector as well
            recArr.push_back(temp);            
            //also remember that the record that was rejected would also 
            //need to be added to the page
            internalPage.Append(&temp);
        }
    }
    //at the end of this loop, there may be elements in the recArr which haven't 
    //been sorted and written to the disk, because the runlen was not reached for
    //this set of records
    if( !recArr.empty() ) {
        //sort the remaining elements
        std::stable_sort(recArr.begin(), recArr.end(), CompareRecords(sortorder));
        //now recArr has the sorted records
        //now need to write all of these records to a File Object on disk
        int outPageAppendResult=1;
        
        start=pageNumInOutFile;
        for(int i=0; i<recArr.size(); i++) {
            outPageAppendResult=outPage.Append(&recArr[i]);
            if(outPageAppendResult==0) {
                //write the page out to file
                outFile.AddPage(&outPage, pageNumInOutFile);
                //increment this page number
                pageNumInOutFile++;
                //clear the page so that subsequent records can be added
                outPage.EmptyItOut();
                //also add to page the last record that was rejected entry
                //into page
                outPage.Append(&recArr[i]);
            }
        }
        //at the end of this loop there will always be atleast one record
        //in the page that has yet to be committed to the disk
        //this page number would also mark the end of current run
        //Also this is the last page of the last run
        outFile.AddPage(&outPage, pageNumInOutFile);
        
        end=pageNumInOutFile;
        runRanges.push_back({start, end});        
        
        //clear the outPage
        outPage.EmptyItOut();
    }
    //
// +++++++++++++++++++++++++++++++++++    
// phase 1 of TPMMS Algo finishes here
// +++++++++++++++++++++++++++++++++++    
    // construct priority queue over sorted runs and dump sorted data 
    // into the out pipe
    Page pageArr[runRanges.size()];
    int currentPageInRun[runRanges.size()];
    
    //get first page of each run
    for(int i=0; i<runRanges.size(); i++) {
        outFile.GetPage(&pageArr[i], runRanges[i].first);
        currentPageInRun[i]=runRanges[i].first;
    }
    //start off by inserting first record from each page into the priority 
    //queue
    Record st;
    std::priority_queue<Record, vector<Record>, CompareRecords1> pqmerge(sortorder);
    
    //
    Attribute IA = {"int", Int};
    Schema __ps_sch ("__ps", 1, &IA);
    
    for(int i=0; i<runRanges.size(); i++) {
        pageArr[i].GetFirst(&st);
        st.runNum=i;
        pqmerge.push(st);
    }
    
    Record r;
    int ct=0;

    while( !pqmerge.empty() ) {
        //remove from queue
        r=pqmerge.top();
        pqmerge.pop();
        
        out->Insert(&r);
        if(pageArr[r.runNum].GetFirst(&st)) {
                st.runNum=r.runNum;
                pqmerge.push(st);
        } else {
            //else means,
            //this page in this runNum is exhausted and need to fetch the next 
            //page in this runNum
            if( currentPageInRun[r.runNum] < runRanges[r.runNum].second ) {
                //means that the current page is within the range and there can 
                //be next page loaded into the pageArr for this runNum
                currentPageInRun[r.runNum]++;
                //cout << "Loading page# " << currentPageInRun[r.runNum] << " in run#" << r.runNum << endl;
                outFile.GetPage(&pageArr[r.runNum], currentPageInRun[r.runNum]);
                pageArr[r.runNum].GetFirst(&st);
                st.runNum=r.runNum;
                pqmerge.push(st);
            }
        }
        ct++;
    }
    //cout << "BQ# " << ct << endl;
//    close the outFile
    outFile.Close();
    //remove/delete the temp file
    //remove(fileName.c_str());
    // finally shut down the out pipe
    out->ShutDown ();
}

//runlen is the number of pages
BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
    BigQTDataT *bigqtdata = new BigQTDataT();
    bigqtdata->in=&in;
    bigqtdata->out=&out;
    bigqtdata->sortorder=sortorder;
    bigqtdata->runlen=runlen;
    pthread_create(&t1, NULL, &BigQProcessor, (void*)bigqtdata);
}

BigQ::~BigQ () {
    //pthread_join(t1, NULL);
}

void BigQ::WaitUntilDone () {
    pthread_join(t1, NULL);
}
