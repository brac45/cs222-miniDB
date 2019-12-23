#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <climits>
#include <map>

#include "../rbf/rbfm.h"

#define IX_EOF (-1)  // end of the index scan

#define NODE_HEADER_SIZE 13

class IX_ScanIterator;
class IXFileHandle;

enum Side {
    UNDECIDED = 0,
    RHS = 1,
    LHS = -1
};

enum ReturnCode {
    IX_OK = 0,
    IX_ERROR = -1,
    IX_SCAN_ENTRY_FOUND = 10,
    IX_SCAN_ENTRY_FOUND_EOP = 11,
    IX_SCAN_NOT_FOUND = 12,
    IX_NO_ENTRIES = 13,
    IX_SEARCH_NOT_FOUND = 14
};

/**
 * Notes by Jongho
 * Page Structure (pages are nodes)
 *      isLeaf: 0: intermediate 1: leaf
 *      N: number of keys
 *      F: number of free bytes
 *      [ 0 (byte,1) | n (unsigned, 4) | F (unsigned, 4) | p_0 | k_1 | p_1 | … | p_(n-1) | k_n | p_n ]
 *      [ 1 (byte,1) | m (unsigned, 4) | F (unsigned, 4) | nextLeaf (int, 4) | k_1 | … | k_m ]
 * Key structure
 *      k_i = (key_value_i, RID_i) : INT/REAL/VARCHAR
 * Children pointers
 *      p_i = page number : unsigned
 * nextLeaf
 *      int: if last, -1
 * RID
 *      pagenum: unsigned, slotnum: unsigned
 */

namespace IX_globals {
    //static std::map<int, IX_ScanIterator*> *activeIterators = new std::map<int, IX_ScanIterator*>;
    static std::map<int, IX_ScanIterator*> activeIterators;
    static int iteratorIDs = 0;
}

class IndexManager {

public:
    static IndexManager &instance();

    // Create an index file.
    RC createFile(const std::string &fileName);

    // Delete an index file.
    RC destroyFile(const std::string &fileName);

    // Open an index and return an ixFileHandle.
    RC openFile(const std::string &fileName, IXFileHandle &ixFileHandle);

    // Close an ixFileHandle for an index.
    RC closeFile(IXFileHandle &ixFileHandle);

    // Insert an entry into the given index that is indicated by the given ixFileHandle.
    RC insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

    // Delete an entry from the given index that is indicated by the given ixFileHandle.
    RC deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

    // Initialize and IX_ScanIterator to support a range search
    RC scan(IXFileHandle &ixFileHandle,
            const Attribute &attribute,
            const void *lowKey,
            const void *highKey,
            bool lowKeyInclusive,
            bool highKeyInclusive,
            IX_ScanIterator &ix_ScanIterator);

    // Print the B+ tree in pre-order (in a JSON record format)
    void printBtree(IXFileHandle &ixFileHandle, const Attribute &attribute) const;

    // FIXME: Print for debugging purposes
    void printBtreeDebug(IXFileHandle &ixFileHandle, const Attribute &attribute) const;

    // FIXME: Print for debugging purposes
    void printBtreeDebug(std::string indexFileName, const Attribute &attribute);

    /***
     * Returns the leaf node PageNum, recursive function
     * @param ixFileHandle
     * @param NODE_NUM
     * @param attribute
     * @param keyValue
     * @param rid
     * @return PageNum
     */
    PageNum BtreeSearch(IXFileHandle &ixFileHandle, const PageNum NODE_NUM, const Attribute &attribute, const void *keyValue, const RID &rid);
private:
    void printPrettyBtree(IXFileHandle &ixFileHandle, unsigned nodePtr, const Attribute &attribute, int level) const;
    void printKeys(const byte* pageBuffer, const Attribute &attribute, unsigned pagenum) const;

    // FIXME: Print for debugging purposes
    void printPrettyBtreeDebug(IXFileHandle &ixFileHandle, unsigned nodePtr, const Attribute &attribute, int level) const;

protected:
    IndexManager() = default;                                                   // Prevent construction
    ~IndexManager() = default;                                                  // Prevent unwanted destruction
    IndexManager(const IndexManager &) = default;                               // Prevent construction by copying
    IndexManager &operator=(const IndexManager &) = default;                    // Prevent assignment

};

class IXFileHandle {
public:

    std::string thisFileName;

    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;

    FILE* filePointer;
    unsigned rootPageNum;
    unsigned init; 


    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

    // Put the current counter values of associated PF FileHandles into variables
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

    RC readPage(PageNum pageNum, void *data);
    RC writePage(PageNum pageNum, const void *data);
    RC appendPage(const void *data);
    unsigned getNumberOfPages();

};

class IX_ScanIterator {
private:
    int ID = -1;
    int nextEntryOffset;
    //IXFileHandle ixFileHandle;
    PageNum currentNodeNum;

    Attribute attribute;
    void *highKey;
    bool highKeyInclusive;

    void *prevReturnedEntry;
    RID prevReturnedRID;

    bool isEOF = false;

public:

    IXFileHandle ixFileHandle;

    // Constructor
    IX_ScanIterator();

    // Destructor
    ~IX_ScanIterator();

    // Get next matching entry
    RC getNextEntry(RID &rid, void *key);

    // Terminate index scan
    RC close();

    RC initScanIterator(IXFileHandle ixFH,
                        PageNum nodeNum, const Attribute &attr,
                        void *hi, bool hiInclusive);

    RC setCursor(int nextEntry);

    void reInitialize();

    void setIsEOF(bool val);
};

namespace ix_helpers {

    RC UpdateVariables(IXFileHandle &ixFileHandle);
    RC insertInit(IXFileHandle &ixFileHandle, const Attribute &attribute, const void* key, const RID& rid);
    RC insertTree(IXFileHandle &ixFileHandle, unsigned nodePageNum, const Attribute &attribute, const void* key, const RID& rid, void* &newChildEntry);
    RC FindWhereToInsertInter(void* currentPage, const Attribute &attribute, const void* key, const RID& rid, unsigned& offsetToInsert);
    RC FindWhereToInsertLeaf(void* currentPage, const Attribute &attribute, const void* key, const RID& rid, unsigned& offsetToInsert);
    RC shiftToRightandInsertChild(void* currentPage, const Attribute &attribute, const unsigned offsetToInsert, const unsigned needSpaceAmount, const void* data);
    RC shiftToRightandInsertKey(void* currentPage, const Attribute &attribute, const unsigned offsetToInsert, const unsigned needSpaceAmount, const void* key, const RID &rid);
    RC splitNodeIntoTwoInter(IXFileHandle &ixFileHandle, void* currentPage, const Attribute &attribute, void* newNode, void *SecondHalfStartingKey);
    RC splitNodeIntoTwoLeaf(IXFileHandle &ixFileHandle, void* currentPage, const Attribute &attribute, void* newNode, unsigned& newPageNum);

    /***
     * Searches the node for the key.
     * If no exact key is found, returns the offset for the entry that is the closest but greater than
     * @param btreenode
     * @param attribute
     * @param key
     * @param retEntryOffset
     * @return
     */
    RC searchEntryInclusive(const byte* btreenode, const Attribute &attribute, const void *key, const RID &rid, int &retEntryOffset);
    /***
     * Searches the node for the key.
     * If no exact key is found, returns IX_NOT_FOUND
     * @param btreenode
     * @param attribute
     * @param key
     * @param retEntryOffset
     * @return
     */
    RC searchExactEntry(const byte* btreenode, const Attribute &attribute, const void *key, const RID &rid, int &retEntryOffset);
    int keyCompare(const Attribute &attribute,
                    const void *lhs, const RID &lhs_rid,
                    const void *rhs, const RID &rhs_rid);
    RC chooseSubtree(byte* btreenode, const Attribute &attribute, const void *key, const RID &keyRID, int& retOffset);

    /***
     * Searches node for key and keyRID and removes it by shifting left
     * @param btreenode
     * @param attribute
     * @param key
     * @param keyRID
     * @return
     */
    RC removeEntry(byte* btreenode, const Attribute &attribute, const void *key, const RID &keyRID);
}

#endif
