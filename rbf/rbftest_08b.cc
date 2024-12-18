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

int RBFTest_8b(RecordBasedFileManager &rbfm) {
    // Functions tested
    // 1. Create Record-Based File
    // 2. Open Record-Based File
    // 3. Insert Record - NULL
    // 4. Read Record
    // 5. Close Record-Based File
    // 6. Destroy Record-Based File
    cout << endl << "***** In RBF Test Case 08b *****" << endl;

    RC rc;
    string fileName = "test8b";

    // Create a file named "test8b"
    rc = rbfm.createFile(fileName);
    assert(rc == success && "Creating the file should not fail.");

    rc = createFileShouldSucceed(fileName);
    assert(rc == success && "Creating the file failed.");

    // Open the file "test8b"
    FileHandle fileHandle;
    rc = rbfm.openFile(fileName, fileHandle);
    assert(rc == success && "Opening the file should not fail.");

    RID rid;
    int recordSize = 0;
    void *record = malloc(100);
    void *returnedData = malloc(100);

    vector<Attribute> recordDescriptor;
    createRecordDescriptor(recordDescriptor);

    // NULL field indicator
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptor.size());
    auto *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

    // Setting the age & salary fields value as null
    nullsIndicator[0] = 80; // 01010000

    // Insert a record into a file
    prepareRecord(recordDescriptor.size(), nullsIndicator, 8, "Anteater", NULL, 177.8, NULL, record, &recordSize);
    cout << endl << "Inserting Data:" << endl;
    rbfm.printRecord(recordDescriptor, record);

    rc = rbfm.insertRecord(fileHandle, recordDescriptor, record, rid);
    assert(rc == success && "Inserting a record should not fail.");

    // Given the rid, read the record from file
    rc = rbfm.readRecord(fileHandle, recordDescriptor, rid, returnedData);
    assert(rc == success && "Reading a record should not fail.");

    // The salary field should not be printed
    cout << endl << "Returned Data:" << endl;
    rbfm.printRecord(recordDescriptor, returnedData);

    // Compare whether the two memory blocks are the same
    if (memcmp(record, returnedData, recordSize) != 0) {
        cout << "[FAIL] Test Case 8b Failed!" << endl << endl;
        free(nullsIndicator);
        free(record);
        free(returnedData);
        return -1;
    }

    cout << endl;

    // Close the file "test8b"
    rc = rbfm.closeFile(fileHandle);
    assert(rc == success && "Closing the file should not fail.");

    // Destroy File
    rc = rbfm.destroyFile(fileName);
    assert(rc == success && "Destroying the file should not fail.");

    rc = destroyFileShouldSucceed(fileName);
    assert(rc == success && "Destroying the file should not fail.");
    free(nullsIndicator);
    free(record);
    free(returnedData);

    cout << "RBF Test Case 08b Finished! The result will be examined." << endl << endl;

    return 0;
}

int main() {
    // To test the functionality of the paged file manager
    // PagedFileManager *pfm = PagedFileManager::instance();

    // To test the functionality of the record-based file manager 
    RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

    remove("test8b");

    return RBFTest_8b(rbfm);
}