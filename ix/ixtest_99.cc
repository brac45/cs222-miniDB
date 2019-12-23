//
// Created by jong on 11/17/19.
//

#include "ix.h"
#include "ix_test_util.h"

int main() {
    RID rid;
    rid.pageNum = 99; rid.slotNum = 99;
    int minusone = -1;
    int zero = 0;
    int one = 1;
    int two = 2;
    int three = 3;
    int four = 4;
    int five = 5;
    int six = 6;
    int seven = 7;
    unsigned dummy = PAGE_SIZE;

    /* Test intermediate node */
    byte isLeaf = 0;
    unsigned N = 1;
    unsigned F = PAGE_SIZE
            - 9             // Headers
            - 4             // p_0
            - N * (4 + 8 + 4);      // k_i, p_i

    byte testIntermediatePage[PAGE_SIZE];
    int offset = 0;
    memcpy(testIntermediatePage + offset, &isLeaf, 1); offset += 1;
    memcpy(testIntermediatePage + offset, &N, 4); offset += 4;
    memcpy(testIntermediatePage + offset, &F, 4); offset += 4;
    memcpy(testIntermediatePage + offset, &dummy, 4); offset += 4;
    memcpy(testIntermediatePage + offset, &one, 4); offset += 4;
    memcpy(testIntermediatePage + offset, &rid.pageNum, 4); offset += 4;
    memcpy(testIntermediatePage + offset, &rid.slotNum, 4); offset += 4;
    memcpy(testIntermediatePage + offset, &dummy, 4); offset += 4;

    IndexManager &ixManager = IndexManager::instance();

    Attribute intAttribute;
    intAttribute.type = TypeInt;
    intAttribute.length = 4;
    intAttribute.name = "TestInt";

    int firstEntry = 13;
    int secondEntry = (firstEntry + 16);
    int thirdEntry = (secondEntry + 16);

    /* Test 0 */
    byte intkey[4];
    memcpy(intkey, &zero, 4);
    int testOffset = -1;
    RC rc = ix_helpers::searchEntryInclusive(testIntermediatePage, intAttribute, intkey, rid, testOffset);
    assert(rc == IX_SCAN_ENTRY_FOUND_EOP);
    assert(testOffset == firstEntry);

    N = 3;
    F = PAGE_SIZE
                 - 9             // Headers
                 - 4             // p_0
                 - N * (4 + 8 + 4);      // k_i, p_i
    memcpy(testIntermediatePage + 1, &N, 4);
    memcpy(testIntermediatePage + 5, &F, 4);

    memcpy(testIntermediatePage + offset, &three, 4); offset += 4;
    memcpy(testIntermediatePage + offset, &rid.pageNum, 4); offset += 4;
    memcpy(testIntermediatePage + offset, &rid.slotNum, 4); offset += 4;
    memcpy(testIntermediatePage + offset, &dummy, 4); offset += 4;
    memcpy(testIntermediatePage + offset, &five, 4); offset += 4;
    memcpy(testIntermediatePage + offset, &rid.pageNum, 4); offset += 4;
    memcpy(testIntermediatePage + offset, &rid.slotNum, 4); offset += 4;
    memcpy(testIntermediatePage + offset, &dummy, 4); offset += 4;

    rc = ix_helpers::searchEntryInclusive(testIntermediatePage, intAttribute, intkey, rid, testOffset);
    assert(rc == IX_SCAN_ENTRY_FOUND);
    assert(testOffset == firstEntry);

    /* Test -1 */
    memcpy(intkey, &minusone, 4);
    testOffset = -1;
    rc = ix_helpers::searchEntryInclusive(testIntermediatePage, intAttribute, intkey, rid, testOffset);
    assert(rc == IX_SCAN_ENTRY_FOUND);
    assert(testOffset == firstEntry);

    /* Test 0 */
    memcpy(intkey, &zero, 4);
    testOffset = -1;
    rc = ix_helpers::searchEntryInclusive(testIntermediatePage, intAttribute, intkey, rid, testOffset);
    assert(rc == IX_SCAN_ENTRY_FOUND);
    assert(testOffset == firstEntry);

    /* Test 1 */
    memcpy(intkey, &one, 4);
    testOffset = -1;
    rc = ix_helpers::searchEntryInclusive(testIntermediatePage, intAttribute, intkey, rid, testOffset);
    assert(rc == IX_SCAN_ENTRY_FOUND);
    assert(testOffset == firstEntry);

    /* Test 2 */
    memcpy(intkey, &two, 4);
    testOffset = -1;
    rc = ix_helpers::searchEntryInclusive(testIntermediatePage, intAttribute, intkey, rid, testOffset);
    assert(rc == IX_SCAN_ENTRY_FOUND);
    assert(testOffset == secondEntry);

    /* Test 3 */
    memcpy(intkey, &three, 4);
    testOffset = -1;
    rc = ix_helpers::searchEntryInclusive(testIntermediatePage, intAttribute, intkey, rid, testOffset);
    assert(rc == IX_SCAN_ENTRY_FOUND);
    assert(testOffset == secondEntry);

    /* Test 4 */
    memcpy(intkey, &four, 4);
    testOffset = -1;
    rc = ix_helpers::searchEntryInclusive(testIntermediatePage, intAttribute, intkey, rid, testOffset);
    assert(rc == IX_SCAN_ENTRY_FOUND_EOP);
    assert(testOffset == thirdEntry);

    /* Test 5 */
    memcpy(intkey, &five, 4);
    testOffset = -1;
    rc = ix_helpers::searchEntryInclusive(testIntermediatePage, intAttribute, intkey, rid, testOffset);
    assert(rc == IX_SCAN_ENTRY_FOUND_EOP);
    assert(testOffset == thirdEntry);

    ///////// TEST ERRORS ///////////
    /* Test 6 */
    memcpy(intkey, &six, 4);
    testOffset = -1;
    rc = ix_helpers::searchEntryInclusive(testIntermediatePage, intAttribute, intkey, rid, testOffset);
    assert(rc == IX_SCAN_NOT_FOUND);
    assert(testOffset == thirdEntry);

    /* Test 7 */
    memcpy(intkey, &seven, 4);
    testOffset = -1;
    rc = ix_helpers::searchEntryInclusive(testIntermediatePage, intAttribute, intkey, rid, testOffset);
    assert(rc == IX_SCAN_NOT_FOUND);
    assert(testOffset == thirdEntry);
}