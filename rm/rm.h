#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>

#include "../rbf/rbfm.h"
#include "../ix/ix.h"

# define RM_EOF (-1)  // end of a scan operator

namespace rm_helpers {

    /**
     * Create attributes for Tables
     */
    void createTablesAttributes(std::vector<Attribute> &TablesAttributes);

    /**
     * Create attributes for Columns
     */
    void createColumnsAttributes(std::vector<Attribute> &ColumnsAttributes);

    /**
     * Create file named fileName and append one empty but formatted(F,N at the end) page in the file
     * Only used for system tables (catalog)
     * @return 0 with no error, 1 if any error occurrs
     */
    RC createSystemTable(RecordBasedFileManager &rbfm, FileHandle &fileHandle, const std::string &fileName, byte* pagePtr);

    /**
     * Return *buffer with format [nullInd (00000000) | tableId | [tableNameLength|tableName] | [fileNameLength|fileName] | systemFlag] for Tables
     */
    void prepareSystemTupleforTables(unsigned int tableId, int tableNameLength, const std::string &tableName,
                                     int fileNameLength, const std::string &fileName, int systemFlag, void *buffer, unsigned *tupleSize);

    /**
     * Return *buffer with format [nullInd (00000000) | tableId | [columnNameLength|columnName] | columnType | columnLength | columnPosition | schemaVersioning ] for Columns
     */
    void prepareSystemTupleforColumns(unsigned int tableId, int columnNameLength, const std::string &columnName,
                                                  int columnType, int columnLength, int columnPosition, unsigned int schemaVersioning, void *buffer, unsigned *tupleSize);

    /** Put default catalog tuples into Tables
     * 1 tuple for Tables itself, and 1 tuple for Columns
     */
    RC InitializeTables(RecordBasedFileManager &rbfm, FileHandle &fileHandle, const std::vector<Attribute> &Attributes, RID &rid);

    /** Put default catalog tuples into Columns
     * 4 tuple for Tables, and 6 tuple for Columns itself
     */
    RC InitializeColumns(RecordBasedFileManager &rbfm, FileHandle &fileHandle, const std::vector<Attribute> &Attributes, RID &rid);


    RC FindAttributeInVector(const std::string tableName, const std::string &attributeName, Attribute &attribute);

    RC ExtractKeyValueFromData(const std::string tableName, const std::string &attributeName, const void* data, void* key);

    RC ExtractKeyValueFromData(const std::vector<Attribute> attributeVector, const std::string &attributeName, const void* data, void* key);
}



// RM_ScanIterator is an iterator to go through tuples
class RM_ScanIterator {
public:
    RBFM_ScanIterator rbfmsi;

// RM_ScanIterator is an iterator to go through tuples
    RM_ScanIterator() = default;

    ~RM_ScanIterator() = default;

    // "data" follows the same format as RelationManager::insertTuple()
    RC getNextTuple(RID &rid, void *data);

    RC close();
};

// RM_IndexScanIterator is an iterator to go through index entries
class RM_IndexScanIterator {
public:
    IX_ScanIterator ixsi;

    RM_IndexScanIterator() = default;    // Constructor
    ~RM_IndexScanIterator() = default;    // Destructor

    // "key" follows the same format as in IndexManager::insertEntry()
    RC getNextEntry(RID &rid, void *key) { return ixsi.getNextEntry(rid,key); };    // Get next matching entry
    RC close() { return ixsi.close(); };                        // Terminate index scan
};

// Relation Manager
class RelationManager {
public:
    static RelationManager &instance();
    std::vector<Attribute> TablesAttributes;
    std::vector<Attribute> ColumnsAttributes;

    RC createCatalog();

    RC deleteCatalog();

    RC createTable(const std::string &tableName, const std::vector<Attribute> &attrs);

    RC deleteTable(const std::string &tableName);

    RC getAttributes(const std::string &tableName, std::vector<Attribute> &attrs);

    RC insertTuple(const std::string &tableName, const void *data, RID &rid);

    RC deleteTuple(const std::string &tableName, const RID &rid);

    RC updateTuple(const std::string &tableName, const void *data, const RID &rid);

    RC readTuple(const std::string &tableName, const RID &rid, void *data);

    // Print a tuple that is passed to this utility method.
    // The format is the same as printRecord().
    RC printTuple(const std::vector<Attribute> &attrs, const void *data);

    RC readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName, void *data);

    // Scan returns an iterator to allow the caller to go through the results one by one.
    // Do not store entire results in the scan iterator.
    RC scan(const std::string &tableName,
            const std::string &conditionAttribute,
            const CompOp compOp,                  // comparison type such as "<" and "="
            const void *value,                    // used in the comparison
            const std::vector<std::string> &attributeNames, // a list of projected attributes
            RM_ScanIterator &rm_ScanIterator);

    // Extra credit work (10 points)
    RC addAttribute(const std::string &tableName, const Attribute &attr);

    RC dropAttribute(const std::string &tableName, const std::string &attributeName);

    // QE IX related
    RC createIndex(const std::string &tableName, const std::string &attributeName);

    RC destroyIndex(const std::string &tableName, const std::string &attributeName);

    // indexScan returns an iterator to allow the caller to go through qualified entries in index
    RC indexScan(const std::string &tableName,
                 const std::string &attributeName,
                 const void *lowKey,
                 const void *highKey,
                 bool lowKeyInclusive,
                 bool highKeyInclusive,
                 RM_IndexScanIterator &rm_IndexScanIterator);

protected:
    RelationManager();                                                  // Prevent construction
    ~RelationManager();                                                 // Prevent unwanted destruction
    RelationManager(const RelationManager &);                           // Prevent construction by copying
    RelationManager &operator=(const RelationManager &);                // Prevent assignment

private:
    static RelationManager *_relation_manager;
};

#endif