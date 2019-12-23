//
// Created by jong on 12/7/19.
//

#include "qe_test_util.h"

int testCase_1() {
    // Mandatory for all
    // Create an Index
    // Load Data
    // Create an Index

    RC rc;
    std::cerr << std::endl << "***** In QE Test Case 1 *****" << std::endl;

    // Create an index before inserting tuples.
    rc = createIndexforLeftB();
    if (rc != success) {
        std::cerr << "***** createIndexforLeftB() failed.  *****" << std::endl;
        return rc;
    }

    // Insert tuples.
    rc = populateLeftTable();
    if (rc != success) {
        std::cerr << "***** populateLeftTable() failed.  *****" << std::endl;
        return rc;
    }

    // Create an index after inserting tuples - should reflect the currently existing tuples.
    rc = createIndexforLeftC();
    if (rc != success) {
        std::cerr << "***** createIndexforLeftC() failed.  *****" << std::endl;
        return rc;
    }
    return rc;
}

RC testCase_2() {
    // Mandatory for all
    // Create an Index
    // Load Data
    // Create an Index

    RC rc;
    std::cerr << std::endl << "***** In QE Test Case 2 *****" << std::endl;

    rc = createIndexforRightB();
    if (rc != success) {
        std::cerr << "***** createIndexforRightB() failed.  *****" << std::endl;
        return rc;
    }

    rc = populateRightTable();
    if (rc != success) {
        std::cerr << "***** populateRightTable() failed.  *****" << std::endl;
        return rc;
    }

    rc = createIndexforRightC();
    if (rc != success) {
        std::cerr << "***** createIndexforRightC() failed.  *****" << std::endl;
        return rc;
    }

    return rc;
}

int main() {
    // Tables created: left
    // Indexes created: left.B, left.C

    // Initialize the system catalog
    if (deleteAndCreateCatalog() != success) {
        std::cerr << "***** deleteAndCreateCatalog() failed." << std::endl;
        std::cerr << "***** [FAIL] QE Test Case 1 failed. *****" << std::endl;
        return fail;
    }

    // Create the left table
    if (createLeftTable() != success) {
        std::cerr << "***** createLeftTable() failed." << std::endl;
        std::cerr << "***** [FAIL] QE Test Case 1 failed. *****" << std::endl;
        return fail;
    }

    if (testCase_1() != success) {
        std::cerr << "***** [FAIL] QE Test Case 1 failed. *****" << std::endl;
        return fail;
    } else {
        std::cerr << "***** QE Test Case 1 finished. The result will be examined. *****" << std::endl;
    }

    // Tables created: right
    // Indexes created: right.B, right.C

    // Create the right table
    if (createRightTable() != success) {
        std::cerr << "***** createRightTable() failed. *****" << std::endl;
        std::cerr << "***** [FAIL] QE Test Case 2 failed. *****" << std::endl;
        return fail;
    }

    if (testCase_2() != success) {
        std::cerr << "***** [FAIL] QE Test Case 2 failed. *****" << std::endl;
        return fail;
    } else {
        std::cerr << "***** QE Test Case 2 finished. The result will be examined. *****" << std::endl;
    }

    // Tables created: leftvarchar, rightvarchar
    // Indexes created: none

    // Create left/right large table, and populate the two tables
    if (createLeftVarCharTable() != success) {
        std::cerr << "***** [FAIL] QE Test Case 4 failed. *****" << std::endl;
        return fail;
    }

    if (populateLeftVarCharTable() != success) {
        std::cerr << "***** [FAIL] QE Test Case 4 failed. *****" << std::endl;
        return fail;
    }

    if (createRightVarCharTable() != success) {
        std::cerr << "***** [FAIL] QE Test Case 4 failed. *****" << std::endl;
        return fail;
    }

    if (populateRightVarCharTable() != success) {
        std::cerr << "***** [FAIL] QE Test Case 4 failed. *****" << std::endl;
        return fail;
    }

    // Tables created: largeleft, largeright
    // Indexes created: none

    // Create left/right large table, and populate the table
    if (createLargeLeftTable() != success) {
        std::cerr << "***** [FAIL] QE Test Case 11 failed. *****" << std::endl;
        return fail;
    }

    if (populateLargeLeftTable() != success) {
        std::cerr << "***** [FAIL] QE Test Case 11 failed. *****" << std::endl;
        return fail;
    }

    if (createLargeRightTable() != success) {
        std::cerr << "***** [FAIL] QE Test Case 11 failed. *****" << std::endl;
        return fail;
    }

    if (populateLargeRightTable() != success) {
        std::cerr << "***** [FAIL] QE Test Case 11 failed. *****" << std::endl;
        return fail;
    }

    if (createGroupTable() != success) {
        std::cerr << "***** [FAIL] QE Test Case 15 failed. *****" << std::endl;
        return fail;
    }

    if (populateGroupTable() != success) {
        std::cerr << "***** [FAIL] QE Test Case 15 failed. *****" << std::endl;
        return fail;
    }

    // Tables created: left3
    // Indexes created: left3.B

    if (createLeftTable3() != success) {
        std::cerr << "***** [FAIL] QE Private Test Case 0 failed. *****" << std::endl;
        return fail;
    }

    if (createIndexforLeftB3() != success) {
        std::cerr << "***** [FAIL] QE Private Test Case 0 failed. *****" << std::endl;
        return fail;
    }

    std::vector<RID> rids;

    if (populateLeftTable3(rids) != success) {
        std::cerr << "***** [FAIL] QE Private Test Case 0 failed. *****" << std::endl;
        return fail;
    }

    if (createIndexforLeftC3() != success) {
        std::cerr << "***** [FAIL] QE Private Test Case 0 failed. *****" << std::endl;
        return fail;
    }

    if (updateLeftTable3(rids) != success) {
        std::cerr << "***** [FAIL] QE Private Test Case 0 failed. *****" << std::endl;
        return fail;
    }

    if (createLargeLeftTable2() != success) {
        std::cerr << "***** [FAIL] QE Private Test Case 1 failed. *****" << std::endl;
        return fail;
    }

    if (createIndexforLargeLeftB2() != success) {
        std::cerr << "***** [FAIL] QE Private Test Case 1 failed. *****" << std::endl;
        return fail;
    }

    if (populateLargeLeftTable2() != success) {
        std::cerr << "***** [FAIL] QE Private Test Case 1 failed. *****" << std::endl;
        return fail;
    }

    if (createLeftTable2() != success) {
        std::cerr << "***** [FAIL] QE Private Test Case 2 failed. *****" << std::endl;
        return fail;
    }

    if (createIndexforLeftB2() != success) {
        std::cerr << "***** [FAIL] QE Private Test Case 2 failed. *****" << std::endl;
        return fail;
    }

    if (populateLeftTable2() != success) {
        std::cerr << "***** [FAIL] QE Private Test Case 2 failed. *****" << std::endl;
        return fail;
    }

    // Inserting more records into table
    if (addRecordsToLargeLeftTable2() != success) {
        std::cout << "***** [FAIL] QE Private Test Case 3 failed. *****" << std::endl;
        return fail;
    }

    // Tables created: largeright2
    // Indexes created: largeright2.B

    if (createLargeRightTable2() != success) {
        std::cerr << "***** [FAIL] QE Private Test Case 4 failed. *****" << std::endl;
        return fail;
    }

    if (populateLargeRightTable2() != success) {
        std::cerr << "***** [FAIL] QE Private Test Case 4 failed. *****" << std::endl;
        return fail;
    }

    if (createIndexforLargeRightB2() != success) {
        std::cerr << "***** [FAIL] QE Private Test Case 4 failed. *****" << std::endl;
        return fail;
    }

    return 0;
}