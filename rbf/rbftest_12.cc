#include <iostream>
#include <cassert>
#include <cstdlib>
#include <cstring>
#include <stdexcept>

#include "pfm.h"
#include "rbfm.h"
#include "test_util.h"

using namespace std;

int RBFTest_12(RecordBasedFileManager &rbfm) {
    // Functions tested
    // 1. Open Record-Based File
    // 2. Read Multiple Records
    // 3. Close Record-Based File
    cout << endl << "***** In RBF Test Case 12 *****" << endl;

    RC rc;
    string fileName = "test11";

    // Open the file "test11"
    FileHandle fileHandle;
    rc = rbfm.openFile(fileName, fileHandle);
    assert(rc == success && "Opening the file should not fail.");

    void *record = malloc(1000);
    void *returnedData = malloc(1000);
    int numRecords = 10000;

    vector<Attribute> recordDescriptor;
    createLargeRecordDescriptor2(recordDescriptor);

    for (Attribute &i : recordDescriptor) {
        cout << "Attr Name: " << i.name << " Attr Type: " << (AttrType) i.type
             << " Attr Len: " << i.length << endl;
    }

    vector<RID> rids;
    RID tempRID;

    // Read rids from the disk - do not use this code. This is not a page-based operation. For test purpose only.
    ifstream ridsFileRead("test11rids", ios::in | ios::binary);

    unsigned pageNum;
    unsigned slotNum;

    if (ridsFileRead.is_open()) {
        ridsFileRead.seekg(0, ios::beg);
        for (int i = 0; i < numRecords; i++) {
            ridsFileRead.read(reinterpret_cast<char *>(&pageNum), sizeof(unsigned));
            ridsFileRead.read(reinterpret_cast<char *>(&slotNum), sizeof(unsigned));
            if (i % 1000 == 0) {
                cout << "loaded RID #" << i << ": " << pageNum << ", " << slotNum << endl;
            }
            tempRID.pageNum = pageNum;
            tempRID.slotNum = slotNum;
            rids.push_back(tempRID);
        }
        ridsFileRead.close();
    }

    assert(rids.size() == (unsigned) numRecords && "Reading records should not fail.");

    // NULL field indicator
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptor.size());
    auto *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

    // Compare records from the disk read with the record created from the method
    for (int i = 0; i < numRecords; i++) {
        memset(record, 0, 1000);
        memset(returnedData, 0, 1000);
        rc = rbfm.readRecord(fileHandle, recordDescriptor, rids[i],
                             returnedData);
        if (rc != success) {
            return -1;
        }
        assert(rc == success);

        int size = 0;
        prepareLargeRecord2(recordDescriptor.size(), nullsIndicator, i, record, &size);
        if (memcmp(returnedData, record, size) != 0) {
            cout << "Test Case 12 Failed!" << endl << endl;
            free(record);
            free(returnedData);
            return -1;
        }
    }


    // Close the file
    rc = rbfm.closeFile(fileHandle);
    assert(rc == success && "Closing the file should not fail.");

    free(record);
    free(returnedData);

    cout << "RBF Test Case 12 Finished! The result will be examined." << endl << endl;

    free(nullsIndicator);
    return 0;
}

int main() {

    // To test the functionality of the record-based file manager
    RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

    RC rc = RBFTest_12(rbfm);
    remove("test11");
    remove("test11rids");
    return rc;
}