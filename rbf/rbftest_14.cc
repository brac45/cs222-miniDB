//
// Created by Seoyeon Hwang on 10/15/19.
//
#include "pfm.h"
#include "test_util.h"


using namespace std;

int RBFTest_14(RecordBasedFileManager &rbfm) {
    /* Testing readRecord() and recordToData() */
    cout << endl << "***** In RBF Test Case 14 - printRecord *****" << endl;

    RC rc;
    string fileName = "test14";

    // Create a file named "test14"
    rc = rbfm.createFile(fileName);
    assert(rc == success && "Creating the file should not fail.");

    rc = createFileShouldSucceed(fileName);
    assert(rc == success && "Creating the file should not fail.");

    // Open the file "test14"
    FileHandle fileHandle;
    rc = rbfm.openFile(fileName, fileHandle);
    assert(rc == success && "Opening the file should not fail.");

    RID rid, rid1;
    void *record = malloc(100);
    void *returnedData = malloc(100);
    int numRecords = 100;

    vector<Attribute> recordDescriptor;
    createRecordDescriptor(recordDescriptor);

    for (Attribute &i : recordDescriptor) {
        cout << "Attr Name: " << i.name << " Attr Type: " << (AttrType) i.type
             << " Attr Len: " << i.length << endl;
    }

    // NULL field indicator
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptor.size());
    auto *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

    int recordSize = 0;

    //Prepare and insert a record
    memset(record, 0, 100);
    prepareRecord(recordDescriptor.size(), nullsIndicator, 13, "Anteater Test", 25, 177.8, 6200, record, &recordSize);
    cout << endl << "Inserting Data:" << endl;
    rbfm.printRecord(recordDescriptor, record);

    rc = rbfm.insertRecord(fileHandle, recordDescriptor, record, rid);
    assert(rc == success && "Inserting a record should not fail.");

    // Given the rid, read the record from file
    rc = rbfm.readRecord(fileHandle, recordDescriptor, rid, returnedData);
    assert(rc == success && "Reading a record should not fail.");
    cout << endl << "Returned Data:" << endl;
    rbfm.printRecord(recordDescriptor, returnedData);

    // Compare whether the two memory blocks are the same
    if (memcmp(record, returnedData, recordSize) != 0) {
        cout << "[FAIL] Test Case 14 Failed!" << endl << endl;
        free(nullsIndicator);
        free(record);
        free(returnedData);
        return -1;
    }

    std::cout << "record matched" << std::endl;

    //Try one more
    memset(record, 0, 100);
    prepareRecord(recordDescriptor.size(), nullsIndicator, 22, "Anteater test one more", 32, 150.1, 3500, record, &recordSize);
    cout << endl << "Inserting Data:" << endl;
    rbfm.printRecord(recordDescriptor, record);

    rc = rbfm.insertRecord(fileHandle, recordDescriptor, record, rid1);
    assert(rc == success && "Inserting a record should not fail.");

    // Given the rid, read the record from file
    rc = rbfm.readRecord(fileHandle, recordDescriptor, rid1, returnedData);
    assert(rc == success && "Reading a record should not fail.");
    cout << endl << "Returned Data:" << endl;
    rbfm.printRecord(recordDescriptor, returnedData);

    // Compare whether the two memory blocks are the same
    if (memcmp(record, returnedData, recordSize) != 0) {
        cout << "[FAIL] Test Case 14 Failed!" << endl << endl;
        free(nullsIndicator);
        free(record);
        free(returnedData);
        return -1;
    }

    std::cout << "record matched" << std::endl;



    // Close the file
    rc = rbfm.closeFile(fileHandle);
    assert(rc == success && "Closing the file should not fail.");

    free(nullsIndicator);
    free(record);
    free(returnedData);
    return 0;

}
int main(){

    RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
    remove("test14");

    return RBFTest_14(rbfm);

};