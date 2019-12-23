//
// Created by Seoyeon Hwang on 11/27/19.
// To test insertTuple with an index file
//

#include "rm_test_util.h"

RC TEST_RM_1(const std::string &tableName, const int nameLength, const std::string &name, const int age,
             const float height, const int salary) {
    // Functions tested
    // 1. Insert Tuple **
    // 2. Read Tuple **
    // NOTE: "**" signifies the new functions being tested in this test case.
    std::cout << std::endl << "***** In RM Test Case 1 *****" << std::endl;

    RID rid;
    unsigned tupleSize = 0;
    void *tuple = malloc(200);
    void *returnedData = malloc(200);
    void *returnedData2 = malloc(200);

    std::vector<Attribute> attrs;
    RC rc = rm.getAttributes(tableName, attrs);
    assert(rc == success && "RelationManager::getAttributes() should not fail.");



    // Create an index file
    rc = rm.createIndex("tbl_employee", "Age");
    assert (rc == success && "Creating a table should not fail.");

    rc = rm.createIndex("tbl_employee", "EmpName");
    assert (rc == success && "Creating a table should not fail.");

    rc = rm.createIndex("tbl_employee", "Height");
    assert (rc == success && "Creating a table should not fail.");



    // Initialize a NULL field indicator
    unsigned nullAttributesIndicatorActualSize = getActualByteForNullsIndicator(attrs.size());
    auto *nullsIndicator = (unsigned char *) malloc(nullAttributesIndicatorActualSize);
    memset(nullsIndicator, 0, nullAttributesIndicatorActualSize);

    // Insert a tuple into a table
    prepareTuple(attrs.size(), nullsIndicator, nameLength, name, age, height, salary, tuple, &tupleSize);
    std::cout << "The tuple to be inserted:" << std::endl;
    rm.printTuple(attrs, tuple);
    std::cout << std::endl;
    
    RID rid2, rid3;

    rc = rm.insertTuple(tableName, tuple, rid);
    assert(rc == success && "RelationManager::insertTuple() should not fail.");

    rc = rm.insertTuple(tableName, tuple, rid2);
    assert(rc == success && "RelationManager::insertTuple() should not fail.");

    rc = rm.insertTuple(tableName, tuple, rid3);
    assert(rc == success && "RelationManager::insertTuple() should not fail.");

    
    // Given the rid, read the tuple from table
    rc = rm.readTuple(tableName, rid, returnedData);
    assert(rc == success && "RelationManager::readTuple() should not fail.");

    rc = rm.readTuple(tableName, rid2, returnedData2);
    assert(rc == success && "RelationManager::readTuple() should not fail.");


    std::cout << "The returned tuple:" << std::endl;
    rm.printTuple(attrs, returnedData);
    std::cout << std::endl;

    std::cout << "The returned tuple2:" << std::endl;
    rm.printTuple(attrs, returnedData2);
    std::cout << std::endl;

    


    IndexManager &ix = IndexManager::instance();
    IXFileHandle ixFileHandle;

    for(Attribute &i: attrs){
        std::string fileName = tableName;
        fileName.append(i.name);
        fileName.append(".idx");

        bool exists = pfm_helpers::fileExists(fileName);
        if(exists) {
            rc = ix.openFile(fileName, ixFileHandle);
            if (rc != 0) return 1;
            ix.printBtree(ixFileHandle, i);
            rc = ix.closeFile(ixFileHandle);
            if (rc != 0) return 1;
        }
    }


    std::cout << "" << std::endl;

    // Delete the tuple
    rc = rm.deleteTuple(tableName, rid);
    assert(rc == success && "RelationManager::deleteTuple() should not fail.");

    for(Attribute &i: attrs){
        std::string fileName = tableName;
        fileName.append(i.name);
        fileName.append(".idx");

        bool exists = pfm_helpers::fileExists(fileName);
        if(exists) {
            rc = ix.openFile(fileName, ixFileHandle);
            if (rc != 0) return 1;
            ix.printBtree(ixFileHandle, i);
            rc = ix.closeFile(ixFileHandle);
            if (rc != 0) return 1;
        }
    }


    // Test Update Tuple
    void *updatedTuple = malloc(200);
    unsigned updatedTupleSize = 0;
    
    prepareTuple(attrs.size(), nullsIndicator, 7, "Barbara", age, height, 12000, updatedTuple, &updatedTupleSize);
    rc = rm.updateTuple(tableName, updatedTuple, rid2);
    assert(rc == success && "RelationManager::updateTuple() should not fail.");

    // Test Read Tuple 
    rc = rm.readTuple(tableName, rid2, returnedData);
    assert(rc == success && "RelationManager::readTuple() should not fail.");

    // Print the tuples

    std::cout << "Updated data:" << std::endl;
    rm.printTuple(attrs, updatedTuple);
    std::cout << std::endl;

    std::cout << "Returned Data:" << std::endl;
    rm.printTuple(attrs, returnedData);
    std::cout << std::endl;

    for(Attribute &i: attrs){
        std::string fileName = tableName;
        fileName.append(i.name);
        fileName.append(".idx");

        bool exists = pfm_helpers::fileExists(fileName);
        if(exists) {
            rc = ix.openFile(fileName, ixFileHandle);
            if (rc != 0) return 1;
            ix.printBtree(ixFileHandle, i);
            rc = ix.closeFile(ixFileHandle);
            if (rc != 0) return 1;
        }
    }



    
    std::cout << "***** RM Test Case 99 finished. The result will be examined. *****" << std::endl << std::endl;
    free(tuple);
    free(returnedData);
    free(returnedData2);
    free(nullsIndicator);
    free(updatedTuple);
    return success;
    


}

int main() {
    // Insert/Read Tuple
    return TEST_RM_1("tbl_employee", 14, "Peter Anteater", 27, 6.2, 10000);
}
