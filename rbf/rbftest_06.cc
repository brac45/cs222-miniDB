#include <iostream>
#include <cassert>
#include <cstring>
#include <stdexcept>

#include "pfm.h"
#include "test_util.h"

using namespace std;

int RBFTest_6(PagedFileManager &pfm) {
    // Functions Tested:
    // 1. Open File
    // 2. Write Page
    // 3. Read Page
    // 4. Close File
    // 5. Destroy File
    cout << endl << "***** In RBF Test Case 06 *****" << endl;

    RC rc;
    string fileName = "test3";

    unsigned readPageCount = 0;
    unsigned writePageCount = 0;
    unsigned appendPageCount = 0;
    unsigned readPageCount1 = 0;
    unsigned writePageCount1 = 0;
    unsigned appendPageCount1 = 0;

    // Open the file "test3"
    FileHandle fileHandle;
    rc = pfm.openFile(fileName, fileHandle);
    assert(rc == success && "Opening the file should not fail.");

    // collect before counters
    rc = fileHandle.collectCounterValues(readPageCount, writePageCount, appendPageCount);
    if (rc != success) {
        cout << "[FAIL] collectCounterValues() failed. Test Case 6 failed." << endl;
        pfm.closeFile(fileHandle);
        return -1;
    }

    // Update the first page
    void *data = malloc(PAGE_SIZE);
    for (unsigned i = 0; i < PAGE_SIZE; i++) {
        *((char *) data + i) = i % 10 + 30;
    }
    rc = fileHandle.writePage(0, data);
    assert(rc == success && "Writing a page should not fail.");

    // Read the page
    void *buffer = malloc(PAGE_SIZE);
    rc = fileHandle.readPage(0, buffer);
    assert(rc == success && "Reading a page should not fail.");

    // collect after counters
    rc = fileHandle.collectCounterValues(readPageCount1, writePageCount1, appendPageCount1);
    if (rc != success) {
        cout << "[FAIL] collectCounterValues() failed. Test Case 6 failed." << endl;
        pfm.closeFile(fileHandle);
        return -1;
    }
    cout << "before:R W A - " << readPageCount << " " << writePageCount << " " << appendPageCount << " after:R W A - "
         << readPageCount1 << " " << writePageCount1 << " " << appendPageCount1 << endl;
    assert(writePageCount1 > writePageCount && "The writePageCount should have been increased.");
    assert(readPageCount1 > readPageCount && "The readPageCount should have been increased.");

    // Check the integrity
    rc = memcmp(data, buffer, PAGE_SIZE);
    assert(rc == success && "Checking the integrity of a page should not fail.");

    // Write a non-existing page
    rc = fileHandle.writePage(1, buffer);
    assert(rc != success && "Writing a non-existing page should fail.");

    // Close the file "test3"
    rc = pfm.closeFile(fileHandle);
    assert(rc == success && "Closing the file should not fail.");

    free(data);
    free(buffer);

    // Destroy the file
    rc = pfm.destroyFile(fileName);
    assert(rc == success && "Destroying the file should not fail.");

    rc = destroyFileShouldSucceed(fileName);
    assert(rc == success && "Destroying the file should not fail.");

    cout << "RBF Test Case 06 Finished! The result will be examined." << endl << endl;

    return 0;

}

int main() {
    // To test the functionality of the paged file manager
    PagedFileManager &pfm = PagedFileManager::instance();

    return RBFTest_6(pfm);
}