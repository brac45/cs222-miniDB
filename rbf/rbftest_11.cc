#include <iostream>
#include <cassert>
#include <cstdlib>
#include <cstring>
#include <stdexcept>
#include <cstdio>

#include "pfm.h"
#include "rbfm.h"
#include "test_util.h"

using namespace std;

int RBFTest_11(RecordBasedFileManager &rbfm) {
    // Functions tested
    // 1. Create Record-Based File
    // 2. Open Record-Based File
    // 3. Insert Multiple Records
    // 4. Close Record-Based File
    cout << endl << "***** In RBF Test Case 11 *****" << endl;

    RC rc;
    string fileName = "test11";

    // Create a file named "test11"
    rc = rbfm.createFile(fileName);
    assert(rc == success && "Creating the file should not fail.");

    rc = createFileShouldSucceed(fileName);
    assert(rc == success && "Creating the file should not fail.");

    // Open the file "test11"
    FileHandle fileHandle;
    rc = rbfm.openFile(fileName, fileHandle);
    assert(rc == success && "Opening the file should not fail.");

    RID rid;
    void *record = malloc(1000);
    void *returnedData = malloc(1000);
    int numRecords = 10000;

    vector<Attribute> recordDescriptor;
    createLargeRecordDescriptor2(recordDescriptor);

    for (Attribute &i : recordDescriptor) {
        cout << "Attr Name: " << i.name << " Attr Type: " << (AttrType) i.type
             << " Attr Len: " << i.length << endl;
    }

    // NULL field indicator
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptor.size());
    auto *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

    vector<RID> rids;
    // Insert 10000 records into file
    for (int i = 0; i < numRecords; i++) {
        // Test insert Record
        memset(record, 0, 1000);
        int size = 0;
        prepareLargeRecord2(recordDescriptor.size(), nullsIndicator, i, record, &size);

        rc = rbfm.insertRecord(fileHandle, recordDescriptor, record, rid);
        assert(rc == success && "Inserting a record should not fail.");

        rids.push_back(rid);
    }

    // Close the file
    rc = rbfm.closeFile(fileHandle);
    assert(rc == success && "Closing the file should not fail.");

    free(record);
    free(returnedData);

    assert(rids.size() == (unsigned) numRecords && "Inserting records should not fail.");

    // Write RIDs to the disk. Do not use this code in your codebase. This is not a page-based operation - for the test purpose only.
    ofstream ridsFile("test11rids", ios::out | ios::trunc | ios::binary);

    if (ridsFile.is_open()) {
        ridsFile.seekp(0, ios::beg);
        for (int i = 0; i < numRecords; i++) {
            ridsFile.write(reinterpret_cast<const char *>(&rids[i].pageNum),
                           sizeof(unsigned));
            ridsFile.write(reinterpret_cast<const char *>(&rids[i].slotNum),
                           sizeof(unsigned));
            if (i % 1000 == 0) {
                cout << "RID #" << i << ": " << rids[i].pageNum << ", "
                     << rids[i].slotNum << endl;
            }
        }
        ridsFile.close();
    }

    cout << "RBF Test Case 11 Finished! The result will be examined." << endl << endl;
    free(nullsIndicator);
    return 0;
}

int main() {
    // To test the functionality of the record-based file manager
    RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

    remove("test11");
    remove("test11rids");

    return RBFTest_11(rbfm);
}