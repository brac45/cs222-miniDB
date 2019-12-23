#ifndef _rbfm_h_
#define _rbfm_h_

#include <string>
#include <vector>
#include <climits>
#include <cstring>
#include <chrono>
#include <unordered_map>

#include "pfm.h"

class FileHandle;

// Record ID

typedef struct _rid {
    unsigned pageNum;    // page number
    unsigned slotNum;    // slot number in the page

    bool operator==(const _rid &rhs) const;
    bool operator!=(const _rid &rhs) const;
    bool operator<(const _rid &rhs) const;
    bool operator>(const _rid &rhs) const;
    bool operator<=(const _rid &rhs) const;
    bool operator>=(const _rid &rhs) const;
} RID;

// Attribute
typedef enum {
    TypeInt = 0, TypeReal, TypeVarChar
} AttrType;

typedef unsigned AttrLength;

struct Attribute {
    std::string name;     // attribute name
    AttrType type;     // attribute type
    AttrLength length; // attribute length
};

// Comparison Operator (NOT needed for part 1 of the project)
typedef enum {
    EQ_OP = 0, // no condition// =
    LT_OP,      // <
    LE_OP,      // <=
    GT_OP,      // >
    GE_OP,      // >=
    NE_OP,      // !=
    NO_OP       // no condition
} CompOp;

namespace rbfm_helpers {
    /**
     * From test_utils.h */
    int getActualByteForNullsIndicator(int fieldCount);

    /**
     * Convert void* data to byte array record
     * @param recordDescriptor representing format of data, input record, output data
     * record format : [ n| null_ind | offsets | values ]
     * data format : [null_ind | values ]
     * @return 0 with no error, 1 with error, 2 for pointing record
     */
    RC recordToData(const std::vector<Attribute> &recordDescriptor, const byte* record, void* data);

    /**
     * Search the slot directory for the next record having offset 'nextRecordOffset'
     * @param
     * *pagePtr: page pointer,
     * nextRecordOffset: search the slot that has the same offset as this,
     * *nextRecordLength: save the corresponding length
     * @return 0 if finding the slot, 1 if no next record
     */
    RC findNextRecordSlot(byte *pagePtr, unsigned short nextRecordOffset, unsigned short &nextRecordLength, unsigned short &foundSlotNo);

    /**
     * Shifts all the records (if there exists) to the left
     * @param pagePtr
     * @param startOffset initial offset to be written to
     * @param nextRecordOffset offset of the next record (the next record can be non-existent)
     * @return 0 if successful, other if failed.
     */
    RC shiftRecordsToLeft(byte* pagePtr, unsigned short startOffset, unsigned short nextRecordOffset);

    /**
     * Convert input data to byte* record
     * @param recordDescriptor
     * @param data
     * @param destination
     * @return RC 0 if successful
     */
    RC dataToRecord(const std::vector<Attribute> &recordDescriptor, const void* data, byte* destination);

    /**
     * Convert input data to byte* record, returns with size of the record
     * @param recordDescriptor
     * @param data
     * @param destination
     * @param size
     * @return
     */
    RC dataToRecordWithSize(const std::vector<Attribute> &recordDescriptor, const void *data, byte *destination, int* size);

    /***
     * Calculate record size for input data
     * @param recordDescriptor
     * @param data
     * @return
     */
    unsigned getPredictedRecordSize(const std::vector<Attribute> &recordDescriptor, const void *data);

    /***
     *
     * @param memPtr
     * @return
     */
    RC memcpyRecordPointer(byte* memPtr, const RID &rid);

    RC getDetailsFromSlotNo(byte* pagePtr, unsigned slotNum, unsigned short* recordOffset, unsigned short* recordLength);

    RC writeDetailsToSlotNo(byte* pagePtr, unsigned slotNum, unsigned short recordOffset, unsigned short recordLength);

    unsigned short getRemainingBytesFromPage(byte* pagePtr);

    unsigned short getNumberOfSlotEntriesFromPage(byte* pagePtr);

    RC reformatPage(byte* pagePtr);

    RC appendRecordToPage(const std::vector<Attribute> &recordDescriptor, const void *data, byte* pagePtr,
                            unsigned* slotNum, unsigned short REM_BYTES, unsigned short NUM_SLOT_ENTRIES);

    /**
     * Attempts to read record from in-memory page; if there's a pointer, internally calls rbfm.readRecord()
     * @param fileHandle
     * @param recordDescriptor
     * @param rid
     * @param page
     * @param data
     * @return
     */
    RC readRecordFromPage(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                          const RID &rid, const byte* page, void *data);

    /**
     * Attempts to delete record from in-memory page; if there's a pointer, internally calls rbfm.deleteRecord()
     * @param fileHandle
     * @param recordDescriptor
     * @param rid
     * @param page
     * @return
     */
    RC deleteRecordFromPage(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                            const RID &rid, byte* page);

    RC stdString2RawValue(const std::string& originalStr, byte* data);

    std::string rawValue2StdString(const byte* data);
}

/********************************************************************
* The scan iterator is NOT required to be implemented for Project 1 *
********************************************************************/

# define RBFM_EOF (-1)  // end of a scan operator

// RBFM_ScanIterator is an iterator to go through records
// The way to use it is like the following:
//  RBFM_ScanIterator rbfmScanIterator;
//  rbfm.open(..., rbfmScanIterator);
//  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
//    process the data;
//  }
//  rbfmScanIterator.close();

class RBFM_ScanIterator {
private:
    FileHandle fileHandle;
    std::vector<Attribute> recordDescriptor;
    std::string conditionAttribute;
    CompOp compOp;
    const void *value;
    std::vector<std::string> attributeNames;

    bool isInitialized;
    RID RIDCursor;    // Cursor that points to the next RID
    RID RIDcachedData;     // Cursor that has RID of the current cached data
    unsigned cachedPageNum;
    unsigned short cachedPageNumOfSlots;
    byte* cachedPage;
    byte* cachedData;
    unsigned fileNumOfPages;
    bool isEOF;

public:
    RBFM_ScanIterator();

    ~RBFM_ScanIterator();

    RC initScanIterator(const FileHandle fh, const std::vector<Attribute> &recordDescriptor,
                       const std::string &conditionAttribute, const CompOp compOp, const void *value,
                       const std::vector<std::string> &attributeNames);

    // Never keep the results in the memory. When getNextRecord() is called,
    // a satisfying record needs to be fetched from the file.
    // "data" follows the same format as RecordBasedFileManager::insertRecord().
    RC getNextRecord(RID &rid, void *data);

    RC close();

private:
    RC findAndCacheRecord();
};

class RecordBasedFileManager {
public:
    static RecordBasedFileManager &instance();                          // Access to the _rbf_manager instance

    /** 
     * This method creates a record-based file called fileName.
     * The file should not already exist.
     * Should internally use the method PagedFileManager::createFile */
    RC createFile(const std::string &fileName);                         // Create a new record-based file

    /**
     * This method destroys the record-based file whose name is fileName.
     * The file should exist.
     * Please note that this method should internally use the method PagedFileManager::destroyFile */
    RC destroyFile(const std::string &fileName);                        // Destroy a record-based file

    /**
     * This method opens the record-based file whose name is fileName.
     * The file must already exist and it must have been created using
     * the RecordBasedFileManager::createFile method.
     * If the method is successful, the fileHandle object whose address is passed
     * as a parameter becomes a "handle" for the open file.
     * It is an error if fileHandle is already a handle for some open file.
     * It is not an error to open the same file more than once if desired,
     * but this would be done by using a different fileHandle object each time.
     * Each call to the openFile method creates a new "instance" of FileHandle.
     * Warning: No need to prevent multiple openings of a file
     * Also note that this method should internally
     * use the method PagedFileManager::openFile(const char *fileName, FileHandle &fileHandle)*/
    RC openFile(const std::string &fileName, FileHandle &fileHandle);


    /**
     * This method closes the open file instance referred to by fileHandle.
     * The file must have been opened using the RecordBasedFileManager::openFile method.
     * Note that this method should internally
     * use the method PagedFileManager::closeFile(FileHandle &fileHandle).*/
    RC closeFile(FileHandle &fileHandle);

    //  Format of the data passed into the function is the following:
    //  [n byte-null-indicators for y fields] [actual value for the first field] [actual value for the second field] ...
    //  1) For y fields, there is n-byte-null-indicators in the beginning of each record.
    //     The value n can be calculated as: ceil(y / 8). (e.g., 5 fields => ceil(5 / 8) = 1. 12 fields => ceil(12 / 8) = 2.)
    //     Each bit represents whether each field value is null or not.
    //     If k-th bit from the left is set to 1, k-th field value is null.
    //          We do not include anything in the actual data part.
    //     If k-th bit from the left is set to 0, k-th field contains non-null values.
    //     If there are more than 8 fields, then you need to find the corresponding byte first,
    //     then find a corresponding bit inside that byte.
    //  2) Actual data is a concatenation of values of the attributes.
    //  3) For Int and Real: use 4 bytes to store the value;
    //     For Varchar: use 4 bytes to store the length of characters, then store the actual characters.
    //  !!! The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute().
    // For example, refer to the Q8 of Project 1 wiki page.

    /**
     * */
    RC insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data, RID &rid);

    /**
     * */
    RC readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid, void *data);

    /** 
     * Print the record that is passed to this utility method.
     * This method will be mainly used for debugging/testing.
     * The format is as follows:
     * field1-name: field1-value  field2-name: field2-value ...
     *      (e.g., age: 24  height: 6.1  salary: 9000
     *             age: NULL  height: 7.5  salary: 7500) */
    RC printRecord(const std::vector<Attribute> &recordDescriptor, const void *data);

    /*****************************************************************************************************
    * IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) *
    * are NOT required to be implemented for Project 1                                                   *
    *****************************************************************************************************/
    // Delete a record identified by the given rid.
    RC deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid);

    // Assume the RID does not change after an update
    RC updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data,
                    const RID &rid);

    // Read an attribute given its name and the rid.
    RC readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid,
                     const std::string &attributeName, void *data);

    // Scan returns an iterator to allow the caller to go through the results one by one.
    RC scan(FileHandle &fileHandle,
            const std::vector<Attribute> &recordDescriptor,
            const std::string &conditionAttribute,
            const CompOp compOp,                  // comparision type such as "<" and "="
            const void *value,                    // used in the comparison
            const std::vector<std::string> &attributeNames, // a list of projected attributes
            RBFM_ScanIterator &rbfm_ScanIterator);

public:

protected:
    RecordBasedFileManager();                                                   // Prevent construction
    ~RecordBasedFileManager();                                                  // Prevent unwanted destruction
    RecordBasedFileManager(const RecordBasedFileManager &);                     // Prevent construction by copying
    RecordBasedFileManager &operator=(const RecordBasedFileManager &);          // Prevent assignment


private:
    static RecordBasedFileManager *_rbf_manager;
};

#endif