//
// Created by jong on 10/2/19.
//

#include <iostream>
#include <cassert>
#include <cstdio>
#include "pfm.h"
#include <bitset>
#include <algorithm>
#include "test_util.h"

int RBFTest_13(RecordBasedFileManager &rbfm) {
    /* Testing printRecord */
    cout << endl << "***** In RBF Test Case 13 - printRecord *****" << endl;

    RC rc;

    int recordSize = 0;
    void *record = malloc(100);

    vector<Attribute> recordDescriptor;
    createRecordDescriptor(recordDescriptor);

    // Initialize a NULL field indicator
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptor.size());
    auto *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

    cout << endl << "Printing Data:" << endl;

    // Insert a record into a file and print the record
    memset(record, 0, 100);
    prepareRecord(recordDescriptor.size(), nullsIndicator, 13, "Anteater Test", 25, 177.8, 6200, record, &recordSize);
    rc = rbfm.printRecord(recordDescriptor, record);
    assert(rc == success && "printRecord should not fail.");

    memset(record, 0, 100);
    prepareRecord(recordDescriptor.size(), nullsIndicator, 18, "Anteater Test Test", 32, 187.8, 69200, record, &recordSize);
    rc = rbfm.printRecord(recordDescriptor, record);
    assert(rc == success && "printRecord should not fail.");

    memset(record, 0, 100);
    prepareRecord(recordDescriptor.size(), nullsIndicator, 23, "Anteater Test Test Test", 33, 189.8, 23, record, &recordSize);
    rc = rbfm.printRecord(recordDescriptor, record);
    assert(rc == success && "printRecord should not fail.");

    /* NULL included record */
    memset(record, 0, 100);
    nullsIndicator[0] = 80; // 01010000
    prepareRecord(recordDescriptor.size(), nullsIndicator, 8, "Anteater", NULL, 177.8, NULL, record, &recordSize);
    rc = rbfm.printRecord(recordDescriptor, record);
    assert(rc == success && "printRecord should not fail.");

    free(nullsIndicator);
    free(record);

    /* Large record */
    record = malloc(1000);

    vector<Attribute> recordDescriptorL;
    createLargeRecordDescriptor(recordDescriptorL);
    for (Attribute &i : recordDescriptorL) {
        cout << "Attr Name: " << i.name << " Attr Type: " << (AttrType) i.type
             << " Attr Len: " << i.length << endl;
    }

    // NULL field indicator
    nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptorL.size());
    nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);
    int size = 0;
    prepareLargeRecord(recordDescriptorL.size(), nullsIndicator, 777, record, &size);
    rc = rbfm.printRecord(recordDescriptorL, record);
    assert(rc == success && "printRecord should not fail.");

    cout << "RBF Test Case 13 Finished!" << endl << endl;

    free(nullsIndicator);
    free(record);

    return 0;
}

int main() {
    // To test the functionality of the record-based file manager
    RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

    return RBFTest_13(rbfm);
}