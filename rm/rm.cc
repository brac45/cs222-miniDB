#include "rm.h"

//////////// rm_helpers Definitions ////////////

void rm_helpers::createTablesAttributes(std::vector<Attribute> &TablesAttributes){

    Attribute attr;

    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = (AttrLength) 4;
    TablesAttributes.push_back(attr);

    attr.name = "table-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength) 50;
    TablesAttributes.push_back(attr);

    attr.name = "file-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength) 50;
    TablesAttributes.push_back(attr);

    attr.name = "system-flag";
    attr.type = TypeInt;
    attr.length = (AttrLength) 4;
    TablesAttributes.push_back(attr);

}

void rm_helpers::createColumnsAttributes(std::vector<Attribute> &ColumnsAttributes){

    Attribute attr;

    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = (AttrLength) 4;
    ColumnsAttributes.push_back(attr);

    attr.name = "column-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength) 50;
    ColumnsAttributes.push_back(attr);

    attr.name = "column-type";
    attr.type = TypeInt;
    attr.length = (AttrLength) 4;
    ColumnsAttributes.push_back(attr);

    attr.name = "column-length";
    attr.type = TypeInt;
    attr.length = (AttrLength) 4;
    ColumnsAttributes.push_back(attr);

    attr.name = "column-position";
    attr.type = TypeInt;
    attr.length = (AttrLength) 4;
    ColumnsAttributes.push_back(attr);

    attr.name = "schema-versioning";
    attr.type = TypeInt;
    attr.length = (AttrLength) 4;
    ColumnsAttributes.push_back(attr);


}

RC rm_helpers::createSystemTable(RecordBasedFileManager &rbfm, FileHandle &fileHandle,
                                const std::string &fileName, byte* pagePtr){

    RC rc = rbfm.createFile(fileName);
    if(rc != 0){
        //std::cerr << "create file error" << std::endl;
        return 1;
    }

    rc = rbfm.openFile(fileName, fileHandle);
    if(rc != 0){
        //std::cerr << "open file error" << std::endl;
        return 1;
    }

    rc = rbfm_helpers::reformatPage(pagePtr);
    if(rc != 0){
        //std::cerr << "reformat page error" << std::endl;
        return 1;
    }

    rc = fileHandle.appendPage(pagePtr);
    if(rc != 0){
        //std::cerr << "append page error" << std::endl;
        return 1;
    }

    return 0;
}

void rm_helpers::prepareSystemTupleforTables(unsigned int tableId, int tableNameLength, const std::string &tableName,
                                 int fileNameLength, const std::string &fileName, int systemFlag, void *buffer, unsigned *tupleSize){


    /* data format for Tables: [ nullIndicator(00000000) | tableId | [tableNameLength | tableName] | [fileNameLength | fileName] | systemFlag ] */

    unsigned offset = 0;
    unsigned char nullIndicator = 0u;  // Since there's no null value in an input tuple for Tables

    memcpy( (char*) buffer + offset, &nullIndicator, 1);
    offset += 1;

    memcpy((char*) buffer + offset, &tableId, 4);
    offset += 4;

    memcpy((char*) buffer + offset, &tableNameLength, 4);
    offset += 4;
    memcpy((char*) buffer + offset, tableName.c_str(), tableNameLength);
    offset += tableNameLength;

    memcpy((char*) buffer + offset, &fileNameLength, 4);
    offset += 4;
    memcpy((char*) buffer + offset, fileName.c_str(), fileNameLength);
    offset += fileNameLength;

    memcpy((char*) buffer + offset, &systemFlag, 4);
    offset += 4;


    *tupleSize = offset;

}

void rm_helpers::prepareSystemTupleforColumns(unsigned int tableId, int columnNameLength, const std::string &columnName,
                                             int columnType, int columnLength, int columnPosition, unsigned int schemaVersioning, void *buffer, unsigned *tupleSize){


    /* data format for Tables: [ nullIndicator(00000000) | tableId | [columnNameLength | columnName] | columnType | columnLength | columnPosition | schemaVersioning ] */

    unsigned offset = 0;
    unsigned char nullIndicator = 0u;  // Since there's no null value in an input tuple for Columns

    memcpy( (char*) buffer + offset, &nullIndicator, 1);
    offset += 1;

    memcpy((char*) buffer + offset, &tableId, 4);
    offset += 4;

    memcpy((char*) buffer + offset, &columnNameLength, 4);
    offset += 4;
    memcpy((char*) buffer + offset, columnName.c_str(), columnNameLength);
    offset += columnNameLength;

    memcpy((char*) buffer + offset, &columnType, 4);
    offset += 4;

    memcpy((char*) buffer + offset, &columnLength, 4);
    offset += 4;

    memcpy((char*) buffer + offset, &columnPosition, 4);
    offset += 4;

    memcpy((char*) buffer + offset, &schemaVersioning, 4);
    offset += 4;

    *tupleSize = offset;

}

RC rm_helpers::InitializeTables(RecordBasedFileManager &rbfm, FileHandle &fileHandle, const std::vector<Attribute> &Attributes, RID &rid){

    unsigned tupleSize = 0;
    void *tuple = malloc(200);

    ///////////Tables tuple/////////
    rm_helpers::prepareSystemTupleforTables(1, 6, "Tables", 10, "Tables.tbl", 1, tuple, &tupleSize);
    RC rc = rbfm.insertRecord(fileHandle, Attributes, tuple, rid);
    if(rc != 0){
        //std::cerr << "insert record error" << std::endl;
        free(tuple);
        return 1;
    }

    fileHandle.tableIdCounter++;

    ///////////Columns tuple/////////
    rm_helpers::prepareSystemTupleforTables(2, 7, "Columns", 11, "Columns.tbl", 1, tuple, &tupleSize);
    rc = rbfm.insertRecord(fileHandle, Attributes, tuple, rid);
    if(rc != 0){
        //std::cerr << "insert record error" << std::endl;
        free(tuple);
        return 1;
    }

    fileHandle.tableIdCounter++;
    /////////

    free(tuple);
    return 0;
}


RC rm_helpers::InitializeColumns(RecordBasedFileManager &rbfm, FileHandle &fileHandle, const std::vector<Attribute> &Attributes, RID &rid){

    unsigned tupleSize = 0;
    void *tuple = malloc(200);

    ///////////Tables tuples/////////
    rm_helpers::prepareSystemTupleforColumns(1, 8, "table-id", TypeInt, 4, 1, 0, tuple, &tupleSize);
    RC rc = rbfm.insertRecord(fileHandle, Attributes, tuple, rid);
    if(rc != 0){
        //std::cerr << "insert record error" << std::endl;
        free(tuple);
        return 1;
    }

    rm_helpers::prepareSystemTupleforColumns(1, 10, "table-name", TypeVarChar, 50, 2, 0, tuple, &tupleSize);
    rc = rbfm.insertRecord(fileHandle, Attributes, tuple, rid);
    if(rc != 0){
        //std::cerr << "insert record error" << std::endl;
        free(tuple);
        return 1;
    }

    rm_helpers::prepareSystemTupleforColumns(1, 9, "file-name", TypeVarChar, 50, 3, 0, tuple, &tupleSize);
    rc = rbfm.insertRecord(fileHandle, Attributes, tuple, rid);
    if(rc != 0){
        //std::cerr << "insert record error" << std::endl;
        free(tuple);
        return 1;
    }

    rm_helpers::prepareSystemTupleforColumns(1, 11, "system-flag", TypeInt, 4, 4, 0, tuple, &tupleSize);
    rc = rbfm.insertRecord(fileHandle, Attributes, tuple, rid);
    if(rc != 0){
        //std::cerr << "insert record error" << std::endl;
        free(tuple);
        return 1;
    }



    fileHandle.tableIdCounter++;

    ////////Columns tuples////////////
    rm_helpers::prepareSystemTupleforColumns(2, 8, "table-id", TypeInt, 4, 1, 0, tuple, &tupleSize);
    rc = rbfm.insertRecord(fileHandle, Attributes, tuple, rid);
    if(rc != 0){
        //std::cerr << "insert record error" << std::endl;
        free(tuple); return 1;
    }

    rm_helpers::prepareSystemTupleforColumns(2, 11, "column-name", TypeVarChar, 50, 2, 0, tuple, &tupleSize);
    rc = rbfm.insertRecord(fileHandle, Attributes, tuple, rid);
    if(rc != 0){
        //std::cerr << "insert record error" << std::endl;
        free(tuple); return 1;
    }

    rm_helpers::prepareSystemTupleforColumns(2, 11, "column-type", TypeInt, 4, 3, 0, tuple, &tupleSize);
    rc = rbfm.insertRecord(fileHandle, Attributes, tuple, rid);
    if(rc != 0){
        //std::cerr << "insert record error" << std::endl;
        free(tuple); return 1;
    }

    rm_helpers::prepareSystemTupleforColumns(2, 13, "column-length", TypeInt, 4, 4, 0, tuple, &tupleSize);
    rc = rbfm.insertRecord(fileHandle, Attributes, tuple, rid);
    if(rc != 0){
        //std::cerr << "insert record error" << std::endl;
        free(tuple); return 1;
    }

    rm_helpers::prepareSystemTupleforColumns(2, 15, "column-position", TypeInt, 4, 5, 0, tuple, &tupleSize);
    rc = rbfm.insertRecord(fileHandle, Attributes, tuple, rid);
    if(rc != 0){
        //std::cerr << "insert record error" << std::endl;
        free(tuple); return 1;
    }

    rm_helpers::prepareSystemTupleforColumns(2, 17, "schema-versioning", TypeInt, 4, 6, 0, tuple, &tupleSize);
    rc = rbfm.insertRecord(fileHandle, Attributes, tuple, rid);
    if(rc != 0){
        //std::cerr << "insert record error" << std::endl;
        free(tuple); return 1;
    }



    fileHandle.tableIdCounter++;

    /////////

    free(tuple);
    return 0;
}



RC rm_helpers::FindAttributeInVector(const std::string tableName, const std::string &attributeName, Attribute &attribute){

    RelationManager &rm = RelationManager::instance();
    std::vector<Attribute> attributeVector;

    RC rc = rm.getAttributes(tableName, attributeVector);
    if(rc!=0) return 1;

    int set = 0;
    for(Attribute &i : attributeVector){
        if(i.name == attributeName){
            attribute.name = i.name;
            attribute.type = i.type;
            attribute.length = i.length;

            set = 1;
            return 0;
        }
    }
    if(set == 0){
        std::cerr << "no such attribute in the table" << std::endl;
        return 1;
    }

}



RC rm_helpers::ExtractKeyValueFromData(const std::string tableName, const std::string &attributeName, const void* data, void* key){

    IndexManager &indexManager = IndexManager::instance();
    IXFileHandle ixFileHandle;

    RelationManager &rm = RelationManager::instance();
    std::vector<Attribute> attributeVector;

    RC rc = rm.getAttributes(tableName, attributeVector);
    if(rc!=0) return 1;

    int offset = 0;
    int numberOfAttributes = attributeVector.size();
    int nullIndicatorSize = rbfm_helpers::getActualByteForNullsIndicator(numberOfAttributes);
    auto nullBitIndicator = (unsigned char*) malloc(nullIndicatorSize);
    memcpy(nullBitIndicator, data, nullIndicatorSize); offset += nullIndicatorSize;

    int quotient = numberOfAttributes / nullIndicatorSize;
    //int remainder = numberOfAttributes % nullIndicatorSize;
    int hit = 0;


    for(int i=0; i< quotient; i++){
        for(int j=0; j< 8; j++){
            int now = i*8+j;
            if(now >= numberOfAttributes){ hit = 1; break; }

            if(attributeVector[now].name == attributeName){
                bool nullBit = nullBitIndicator[i] & ((unsigned)1 << (unsigned)(7-j));
                if(!nullBit){
                    if(attributeVector[now].type == TypeInt || attributeVector[now].type == TypeReal){
                        memcpy(key, (byte*)data+offset, 4);
                        free(nullBitIndicator);
                        return 0;
                    }else if(attributeVector[now].type == TypeVarChar){
                        int len = 0;
                        memcpy(&len, (byte*)data+offset, 4);
                        memcpy(key, (byte*)data+offset, 4+len);
                        free(nullBitIndicator);
                        return 0;
                    }
                }else{ // the value we're looking for is null
                    std::cerr << "no key value in this data" << std::endl;
                    free(nullBitIndicator);
                    return 1;
                }
            }else{
                bool nullBit = nullBitIndicator[i] & ((unsigned)1 << (unsigned)(7-j));
                if(!nullBit){
                    if(attributeVector[now].type == TypeInt || attributeVector[now].type == TypeReal){
                        offset += 4;
                    }else if(attributeVector[now].type == TypeVarChar){
                        int len = 0;
                        memcpy(&len, (byte*)data+offset, 4);
                        offset += (4+len);
                    }
                }else{
                    continue;
                }
            }

        } // inner for loop
        if(hit == 1) break;
    } // outer for loop

    std::cerr << "no such attribute in this table" << std::endl;
    free(nullBitIndicator);
    return 1;

}

RC rm_helpers::ExtractKeyValueFromData(const std::vector<Attribute> attributeVector, const std::string &attributeName, const void* data, void* key){
    int offset = 0;
    int numberOfAttributes = attributeVector.size();
    int nullIndicatorSize = rbfm_helpers::getActualByteForNullsIndicator(numberOfAttributes);
    auto nullBitIndicator = (unsigned char*) malloc(nullIndicatorSize);
    memcpy(nullBitIndicator, data, nullIndicatorSize); offset += nullIndicatorSize;

    int quotient = numberOfAttributes / nullIndicatorSize;
    int hit = 0;

    for(int i=0; i< quotient; i++){
        for(int j=0; j< 8; j++){
            int now = i*8+j;
            if(now >= numberOfAttributes){ hit = 1; break; }

            if(attributeVector[now].name == attributeName){
                bool nullBit = nullBitIndicator[i] & ((unsigned)1 << (unsigned)(7-j));
                if(!nullBit){
                    if(attributeVector[now].type == TypeInt || attributeVector[now].type == TypeReal){
                        memcpy(key, (byte*)data+offset, 4);
                        free(nullBitIndicator);
                        return 0;
                    }else if(attributeVector[now].type == TypeVarChar){
                        int len = 0;
                        memcpy(&len, (byte*)data+offset, 4);
                        memcpy(key, (byte*)data+offset, 4+len);
                        free(nullBitIndicator);
                        return 0;
                    }
                }else{ // the value we're looking for is null
                    std::cerr << "no key value in this data" << std::endl;
                    free(nullBitIndicator);
                    return 1;
                }
            }else{
                bool nullBit = nullBitIndicator[i] & ((unsigned)1 << (unsigned)(7-j));
                if(!nullBit){
                    if(attributeVector[now].type == TypeInt || attributeVector[now].type == TypeReal){
                        offset += 4;
                    }else if(attributeVector[now].type == TypeVarChar){
                        int len = 0;
                        memcpy(&len, (byte*)data+offset, 4);
                        offset += (4+len);
                    }
                }else{
                    continue;
                }
            }

        } // inner for loop
        if(hit == 1) break;
    } // outer for loop

    std::cerr << "no such attribute in this table" << std::endl;
    free(nullBitIndicator);
    return 1;

}



//////////// RelationManager Definitions ////////////


RelationManager *RelationManager::_relation_manager = nullptr;

RelationManager &RelationManager::instance() {
    static RelationManager _relation_manager = RelationManager();
    return _relation_manager;
}

RelationManager::RelationManager() {

    /* Create attribute vectors for each catalog */
    rm_helpers::createTablesAttributes(TablesAttributes);
    rm_helpers::createColumnsAttributes(ColumnsAttributes);

}

RelationManager::~RelationManager() { delete _relation_manager; }

RelationManager::RelationManager(const RelationManager &) = default;

RelationManager &RelationManager::operator=(const RelationManager &) = default;

RC RelationManager::createCatalog() {

    /* Create files for each catalog */
    std::string file_name1 =  "Tables.tbl";
    std::string file_name2 = "Columns.tbl";

    FileHandle fileHandle;
    std::string fileName = file_name1;
    byte* pagePtr = new byte[PAGE_SIZE];
    RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

    /* Create and initialize the catalog Tables */
    RC rc = rm_helpers::createSystemTable(rbfm, fileHandle, fileName, pagePtr);
    if(rc != 0){
        //std::cerr << "create system tables error" << std::endl;
        delete[] pagePtr; return 1;
    }

    RID rid;
    rc = rm_helpers::InitializeTables(rbfm, fileHandle, TablesAttributes, rid);
    if(rc != 0){
        //std::cerr << "Initialize tuples on Tables error" << std::endl;
        delete[] pagePtr;  return 1;
    }

    rc = rbfm.closeFile(fileHandle);
    if(rc != 0){
        //std::cerr << "close file error" << std::endl;
        delete[] pagePtr;  return 1;
    }

    /* Create and initialize the catalog Columns */
    fileName = file_name2;
    memset(pagePtr, 0, PAGE_SIZE);
    rc = rm_helpers::createSystemTable(rbfm, fileHandle, fileName, pagePtr);
    if(rc != 0){
        //std::cerr << "create system tables error" << std::endl;
        delete[] pagePtr; return 1;
    }

    rc = rm_helpers::InitializeColumns(rbfm, fileHandle, ColumnsAttributes, rid);
    if(rc != 0){
        //std::cerr << "Initialize tuples on Tables error" << std::endl;
        delete[] pagePtr; return 1;
    }

    rc = rbfm.closeFile(fileHandle);
    if(rc != 0){
        //std::cerr << "close file error" << std::endl;
        delete[] pagePtr; return 1;
    }

    delete[] pagePtr;
    return 0;
}

RC RelationManager::deleteCatalog() {

    RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
    std::string file_name1 =  "Tables.tbl";
    std::string file_name2 = "Columns.tbl";

    /* Destroy the catalog Tables */
    RC rc = rbfm.destroyFile(file_name1);
    if(rc != 0){
        //std::cerr << "destroy file for Tables error" << std::endl;
        return 1;
    }

    /* Destroy the catalog Columns */
    rc = rbfm.destroyFile(file_name2);
    if(rc != 0){
        //std::cerr << "destroy file for Columns error" << std::endl;
        return 1;
    }

    return 0;
}

RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {

    RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;

    /* tableName should be different from catalog tables */
    if(tableName == "Tables" || tableName == "Columns"){
        //std::cerr << "invalid table name" << std::endl;
        return 1;
    }

    /* Create new file for the table and append empty formatted page in the file */
    std::string fileName = tableName;
    fileName.append(".tbl");

    RC rc = rbfm.createFile(fileName);
    if(rc != 0){
        //std::cerr << "create file error" << std::endl;
        return 1;
    }

    rc = rbfm.openFile(fileName, fileHandle);
    if(rc != 0){
        //std::cerr << "open file error" << std::endl;
        return 1;
    }

    byte* pagePtr = new byte[PAGE_SIZE];
    rc = rbfm_helpers::reformatPage(pagePtr);
    if(rc != 0){
        //std::cerr << "reformat page error" << std::endl;
        delete[] pagePtr; return 1;
    }

    //RBFM_Page *rbfmPage = new RBFM_Page(pagePtr, 0, false);
    rc = fileHandle.appendPage(pagePtr);
    if(rc != 0){
        //std::cerr << "append page error" << std::endl;
        delete[] pagePtr; return 1;
    }

    rc = rbfm.closeFile(fileHandle);
    if(rc != 0){
        //std::cerr << "close file error" << std::endl;
        delete[] pagePtr; return 1;
    }


    /* Add one tuple in Tables */
    rc = rbfm.openFile("Tables.tbl", fileHandle);
    if(rc != 0){
        //std::cerr << "open file error (Tables)" << std::endl;
        delete[] pagePtr; return 1;
    }

    unsigned nextTableId = fileHandle.tableIdCounter + 1;
    unsigned tupleSize = 0;
    void *tuple = malloc(200);
    RID rid;

    rm_helpers::prepareSystemTupleforTables(nextTableId, tableName.size(), tableName, fileName.size(), fileName, 0, tuple, &tupleSize);
    rc = rbfm.insertRecord(fileHandle, TablesAttributes, tuple, rid);
    if(rc != 0){
        //std::cerr << "insert record error (Tables)" << std::endl;
        delete[] pagePtr; free(tuple); return 1;
    }
    fileHandle.tableIdCounter++;

    rc = rbfm.closeFile(fileHandle);
    if(rc != 0){
        //std::cerr << "close file error" << std::endl;
        delete[] pagePtr; free(tuple); return 1;
    }

    /* Add attrs.size() tuples in Columns */
    rc = rbfm.openFile("Columns.tbl", fileHandle);
    if(rc != 0){
        //std::cerr << "open file error (Columns)" << std::endl;
        delete[] pagePtr; free(tuple); return 1;
    }

    unsigned int currentSchemaVersioning = fileHandle.schemaVersioningCounter;
    for(int i = 0; i < attrs.size(); i++){
        rm_helpers::prepareSystemTupleforColumns(nextTableId, attrs[i].name.size(), attrs[i].name, attrs[i].type, attrs[i].length, i+1, currentSchemaVersioning, tuple, &tupleSize);
        rc = rbfm.insertRecord(fileHandle,ColumnsAttributes, tuple, rid);
        if(rc != 0){
            //std::cerr << "insert record error (Columns)" << std::endl;
            delete[] pagePtr; free(tuple); return 1;
        }
    }

    rc = rbfm.closeFile(fileHandle);
    if(rc != 0){
        //std::cerr << "close file error" << std::endl;
        delete[] pagePtr; free(tuple); return 1;
    }

    delete[] pagePtr;
    free(tuple);
    return 0;
}


RC RelationManager::deleteTable(const std::string &tableName) {

    RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
    FileHandle tablesFileHandle;
    FileHandle colFileHandle;

    /* tableName should be different from catalog tables */
    if(tableName == "Tables" || tableName == "Columns"){
        //std::cerr << "invalid table name" << std::endl;
        return 1;
    }

    /* Delete the file for the table */
    std::string fileName = tableName;
    fileName.append(".tbl");

    RC rc = rbfm.destroyFile(fileName);
    if(rc != 0){
        //std::cerr << "destroy file error" << std::endl;
        return 1;
    }

    /* Delete the corresponding tuple in catalog Tables */
    rc = rbfm.openFile("Tables.tbl", tablesFileHandle);
    if(rc != 0){
        //std::cerr << "open file error (Tables)" << std::endl;
        return 1;
    }

    /* Change by Jongho: need to use RBFM_ScanIterator since rm::scan() uses getAttributes() */
    //RM_ScanIterator rmsi;
    RBFM_ScanIterator rbfmsi_tbl;
    std::vector<std::string> attributeName;
    std::string attr = "table-id";
    attributeName.push_back(attr);

    //rc = scan("Tables", "table-name", EQ_OP, &tableName, attributeName, rmsi);
    byte* tableNameDataRep = new byte[tableName.size() + 4];
    rbfm_helpers::stdString2RawValue(tableName, tableNameDataRep);
    rc = rbfm.scan(tablesFileHandle,
                   TablesAttributes, "table-name",
                   EQ_OP, tableNameDataRep, attributeName, rbfmsi_tbl);

    if(rc != 0){
        //std::cerr << "scan error (Tables)" << std::endl;
        delete[] tableNameDataRep;
        return 1;
    }


    RID rid;
    unsigned int tableId;
    void *returnedData = malloc(200);
    while(rbfmsi_tbl.getNextRecord(rid, returnedData) != RBFM_EOF){
        // returned data format: [ nullBitInd | values ], here : [ 00000000 (1 byte) | tableId ]
        memcpy(&tableId, (byte*)returnedData + 1, 4);
        rc = rbfm.deleteRecord(tablesFileHandle, TablesAttributes, rid);
        if(rc != 0){
            //std::cerr << "delete record error (Tables)" << std::endl;
            delete[] tableNameDataRep; free(returnedData); return 1;
        }
    }
    rbfmsi_tbl.close();

    rc = rbfm.closeFile(tablesFileHandle);
    if(rc != 0){
        //std::cerr << "close file error (Tables)" << std::endl;
        delete[] tableNameDataRep; free(returnedData); return 1;
    }

    /* Delete corresponding tuples (having the same tableId) in catalog Columns */
    rc = rbfm.openFile("Columns.tbl", colFileHandle);
    if(rc != 0){
        //std::cerr << "open file error (Columns)" << std::endl;
        delete[] tableNameDataRep; free(returnedData); return 1;
    }

    RBFM_ScanIterator rbfmsi_col;

    byte tableIDDataRep[4];
    memcpy(tableIDDataRep, &tableId, 4);
    //rc = scan("Columns", "table-id", EQ_OP, tableIDDataRep, attributeName, rbfmsi_col);
    rc = rbfm.scan(colFileHandle, ColumnsAttributes, "table-id", EQ_OP, tableIDDataRep, attributeName, rbfmsi_col);

    if(rc != 0){
        //std::cerr << "scan error (Columns)" << std::endl;
        delete[] tableNameDataRep; free(returnedData); return 1;
    }

    while(rbfmsi_col.getNextRecord(rid, returnedData) != RBFM_EOF){
        // returned data format: [ nullBitInd | values ], here : [ 00000000 (1 byte) | tableId ]
        rc = rbfm.deleteRecord(colFileHandle, ColumnsAttributes, rid);
        if(rc != 0){
            //std::cerr << "delete record error (Columns)" << std::endl;
            delete[] tableNameDataRep; free(returnedData); return 1;
        }
    }
    rbfmsi_col.close();

    rc = rbfm.closeFile(colFileHandle);
    if(rc != 0){
        //std::cerr << "close file error (Columns)" << std::endl;
        delete[] tableNameDataRep; free(returnedData); return 1;
    }


    free(returnedData);
    delete[] tableNameDataRep;
    return 0;
}

RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {

    RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
    FileHandle tablesFileHandle;
    FileHandle colFileHandle;

    RC rc = rbfm.openFile("Tables.tbl", tablesFileHandle);
    if(rc != 0){
        //std::cerr << "open file error (Tables)" << std::endl;
        return 1;
    }

    /* Change by Jongho: need to use RBFM_ScanIterator since rm::scan() uses getAttributes() */
    //RM_ScanIterator rmsi;
    RBFM_ScanIterator rbfmsi_tbl;
    std::vector<std::string> attributeName;
    std::string attr = "table-id";
    attributeName.push_back(attr);

    /* Changed by Seoyeon: need to use fileName instead of tableName, because the index file has same name */
    std::string fileName = tableName;
    fileName.append(".tbl");

    /*
    //rc = scan("Tables", "table-name", EQ_OP, &tableName, attributeName, rmsi);
    byte* tableNameDataRep = new byte[tableName.size() + 4];
    rbfm_helpers::stdString2RawValue(tableName, tableNameDataRep);
    rc = rbfm.scan(tablesFileHandle,
            TablesAttributes, "table-name",
            EQ_OP, tableNameDataRep, attributeName, rbfmsi_tbl);
            */

    byte* tableNameDataRep = new byte[fileName.size() + 4];
    rbfm_helpers::stdString2RawValue(fileName, tableNameDataRep);
    rc = rbfm.scan(tablesFileHandle, TablesAttributes, "file-name", EQ_OP, tableNameDataRep, attributeName, rbfmsi_tbl);

    if(rc != 0){
        //std::cerr << "scan error (Tables)" << std::endl;
        delete[] tableNameDataRep;
        return 1;
    }

    RID rid;
    unsigned int tableId;
    void *returnedData = malloc(200);
    while(rbfmsi_tbl.getNextRecord(rid, returnedData) != RBFM_EOF){
        // returned data format: [ nullBitInd | values ], here : [ 00000000 (1 byte) | tableId ]
        memcpy(&tableId, (byte*)returnedData + 1, 4);
    }
    rbfmsi_tbl.close();

    rc = rbfm.closeFile(tablesFileHandle);
    if(rc != 0){
        //std::cerr << "close file error (Tables)" << std::endl;
        delete[] tableNameDataRep; free(returnedData);
        return 1;
    }

    /* Push back all the column-names in catalog Columns using tableId */
    rc = rbfm.openFile("Columns.tbl", colFileHandle);
    if(rc != 0){
        //std::cerr << "open file error (Columns)" << std::endl;
        delete[] tableNameDataRep; free(returnedData);
        return 1;
    }

    attributeName.clear();
    
    attr = "column-name";
    attributeName.push_back(attr);
    attr = "column-type";
    attributeName.push_back(attr);
    attr = "column-length";
    attributeName.push_back(attr);

    RBFM_ScanIterator rbfmsi_col;

    byte tableIDDataRep[4];
    memcpy(tableIDDataRep, &tableId, 4);
    rc = rbfm.scan(colFileHandle, ColumnsAttributes,
            "table-id", EQ_OP, tableIDDataRep, attributeName, rbfmsi_col);
    //rc = scan("Columns", "table-id", EQ_OP, &tableId, attributeName, rb);
    if(rc != 0){
        //std::cerr << "scan error (Columns)" << std::endl;
        delete[] tableNameDataRep; free(returnedData);
        return 1;
    }

    int length = 0; int offset = 0; byte buffer[512];
    while(rbfmsi_col.getNextRecord(rid, returnedData) != RBFM_EOF){
        // returned data format: [ nullBitInd | values ], here : [ 00000000 (1 byte) | [columnNameLength|columnName] | columnType | columnLength ]
        Attribute attribute;
        offset += 1; // for nullBitIterator (1 byte)

        memcpy(&length, (byte*)returnedData + offset, 4);
        memcpy(buffer, (byte*)returnedData + offset, length + 4);
        attribute.name = rbfm_helpers::rawValue2StdString(buffer);
        offset += 4 + length; // for columnNameLength (int) + columnName

        /*
        memcpy(&(attribute.name), (byte*)returnedData + offset, length);
        offset += length; // for columnName
         */

        memcpy(&(attribute.type), (byte*) returnedData + offset, 4);
        offset += 4; // for columnType (int)

        memcpy(&(attribute.length), (byte*) returnedData + offset, 4);
        offset = 0; // reset to 0
        attrs.push_back(attribute);
    }
    rbfmsi_col.close();

    rc = rbfm.closeFile(colFileHandle);
    if(rc != 0){
        //std::cerr << "close file error (Columns)" << std::endl;
        delete[] tableNameDataRep; free(returnedData);
        return 1;
    }

    free(returnedData);
    delete[] tableNameDataRep;
    return 0;
}

RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {

    RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;

    /* tableName should be different from catalog tables */
    if(tableName == "Tables" || tableName == "Columns"){
        //std::cerr << "invalid table name" << std::endl;
        return 1;
    }

    std::string fileName = tableName;
    fileName.append(".tbl");

    RC rc = rbfm.openFile(fileName, fileHandle);
    if(rc != 0){
        //std::cerr << "open file error" << std::endl;
        return 1;
    }

    std::vector<Attribute> tableAttributes;
    rc = getAttributes(tableName, tableAttributes);
    if(rc != 0){
        //std::cerr << "get attribute error" << std::endl;
        return 1;
    }

    rc = rbfm.insertRecord(fileHandle, tableAttributes, data, rid);
    if(rc != 0){
        //std::cerr << "insert record error" << std::endl;
        return 1;
    }

    rc = rbfm.closeFile(fileHandle);
    if(rc != 0){
        //std::cerr << "close file error" << std::endl;
        return 1;
    }

    //////////// Insert this into the index file as well /////////
    void* key = malloc(PAGE_SIZE);
    IndexManager &ix = IndexManager::instance();
    IXFileHandle ixFileHandle;
    /* See if there's an index file with each attribute, and insert the data if there exists. */

    for(Attribute &attr:tableAttributes){

        fileName = tableName;
        fileName.append(attr.name);
        fileName.append(".idx");

        bool exists = pfm_helpers::fileExists(fileName);
        if(exists){

            rc = rm_helpers::ExtractKeyValueFromData(tableName, attr.name, data, key);
            if(rc!=0){
                std::cerr << "Error occurred while extracting the key value from data" << std::endl;
                free(key);
                return 1;
            }

            rc = ix.openFile(fileName, ixFileHandle);
            if(rc!=0){ free(key); return 1;}

            rc = ix.insertEntry(ixFileHandle, attr, key, rid);
            if(rc!=0){ free(key); return 1;}

            rc = ix.closeFile(ixFileHandle);
            if(rc!=0){ free(key); return 1;}

        }else{
            continue;
        }

    }

    free(key);
    return 0;
}

RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {

    RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;

    /* tableName should be different from catalog tables */
    if(tableName == "Tables" || tableName == "Columns"){
        //std::cerr << "invalid table name" << std::endl;
        return 1;
    }

    std::string fileName = tableName;
    fileName.append(".tbl");

    RC rc = rbfm.openFile(fileName, fileHandle);
    if(rc != 0){
        //std::cerr << "open file error" << std::endl;
        return 1;
    }

    std::vector<Attribute> tableAttributes;
    rc = getAttributes(tableName, tableAttributes);
    if(rc != 0){
        //std::cerr << "get attribute error" << std::endl;
        return 1;
    }


    /* To delete this data from every index file, we should know the data deleted */ ///added///
    void* data = malloc(PAGE_SIZE);
    rc = readTuple(tableName, rid, data);
    if(rc!=0){ free(data); return 1;}


    rc = rbfm.deleteRecord(fileHandle, tableAttributes, rid);
    if(rc != 0){
        //std::cerr << "delete record error" << std::endl;
        free(data); return 1;
    }

    rc = rbfm.closeFile(fileHandle);
    if(rc != 0){
        //std::cerr << "close file error" << std::endl;
        free(data); return 1;
    }

    //////////// Delete this into the index file as well /////////
    void* key = malloc(PAGE_SIZE);
    IndexManager &ix = IndexManager::instance();
    IXFileHandle ixFileHandle;
    /* See if there's an index file with each attribute, and delete the data if it exists. */

    for(Attribute &attr:tableAttributes){

        fileName = tableName;
        fileName.append(attr.name);
        fileName.append(".idx");

        bool exists = pfm_helpers::fileExists(fileName);
        if(exists){

            rc = rm_helpers::ExtractKeyValueFromData(tableName, attr.name, data, key);
            if(rc!=0){
                std::cerr << "Error occurred while extracting the key value from data" << std::endl;
                free(key); free(data);
                return 1;
            }

            rc = ix.openFile(fileName, ixFileHandle);
            if(rc!=0){ free(key); free(data); return 1;}

            rc = ix.deleteEntry(ixFileHandle, attr, key, rid);
            if(rc!=0){ free(key); free(data); return 1;}

            rc = ix.closeFile(ixFileHandle);
            if(rc!=0){ free(key); free(data); return 1;}

        }else{
            continue;
        }

    }

    free(key);
    free(data);
    return 0;
}

RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {

    RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;

    /* tableName should be different from catalog tables */
    if(tableName == "Tables" || tableName == "Columns"){
        //std::cerr << "invalid table name" << std::endl;
        return 1;
    }

    std::string fileName = tableName;
    fileName.append(".tbl");

    RC rc = rbfm.openFile(fileName, fileHandle);
    if(rc != 0){
        //std::cerr << "open file error" << std::endl;
        return 1;
    }

    std::vector<Attribute> tableAttributes;
    rc = getAttributes(tableName, tableAttributes);
    if(rc != 0){
        //std::cerr << "get attribute error" << std::endl;
        return 1;
    }

    /* To update this data from every index file, we should remember the data to delete and insert new data */ ///added///
    void* originalData = malloc(PAGE_SIZE);
    rc = readTuple(tableName, rid, originalData);
    if(rc!=0){ free(originalData); return 1;}


    rc = rbfm.updateRecord(fileHandle, tableAttributes, data, rid);
    if(rc != 0){
        //std::cerr << "update record error" << std::endl;
        free(originalData); return 1;
    }

    rc = rbfm.closeFile(fileHandle);
    if(rc != 0){
        //std::cerr << "close file error" << std::endl;
        free(originalData); return 1;
    }

    //////////// Update this into the index file as well /////////
    void* key = malloc(PAGE_SIZE);
    void* originalKey = malloc(PAGE_SIZE);
    IndexManager &ix = IndexManager::instance();
    IXFileHandle ixFileHandle;
    /* See if there's an index file with each attribute, and update(delete and insert) the data if there exists. */

    for(Attribute &attr:tableAttributes){

        fileName = tableName;
        fileName.append(attr.name);
        fileName.append(".idx");

        bool exists = pfm_helpers::fileExists(fileName);
        if(exists){

            rc = rm_helpers::ExtractKeyValueFromData(tableName, attr.name, data, key);
            if(rc!=0){
                std::cerr << "Error occurred while extracting the key value from data" << std::endl;
                free(originalData); free(originalKey); free(key);
                return 1;
            }

            rc = rm_helpers::ExtractKeyValueFromData(tableName, attr.name, originalData, originalKey);
            if(rc!=0){
                std::cerr << "Error occurred while extracting the key value from data" << std::endl;
                free(originalData); free(originalKey); free(key);
                return 1;
            }

            rc = ix.openFile(fileName, ixFileHandle);
            if(rc!=0){ free(originalData); free(originalKey); free(key); return 1;}

            rc = ix.deleteEntry(ixFileHandle, attr, originalKey, rid);
            if(rc!=0){ free(originalData); free(originalKey); free(key); return 1;}

            rc = ix.insertEntry(ixFileHandle, attr, key, rid);
            if(rc!=0){ free(originalData); free(originalKey); free(key); return 1;}

            rc = ix.closeFile(ixFileHandle);
            if(rc!=0){ free(originalData); free(originalKey); free(key); return 1;}

        }else{
            continue;
        }

    }

    free(originalData);
    free(originalKey);
    free(key);
    return 0;
}

RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {

    RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;

    /* tableName should be different from catalog tables */
    if(tableName == "Tables" || tableName == "Columns"){
        //std::cerr << "invalid table name" << std::endl;
        return 1;
    }

    std::string fileName = tableName;
    fileName.append(".tbl");

    RC rc = rbfm.openFile(fileName, fileHandle);
    if(rc != 0){
        //std::cerr << "open file error" << std::endl;
        return 1;
    }

    std::vector<Attribute> tableAttributes;
    rc = getAttributes(tableName, tableAttributes);
    if(rc != 0){
        //std::cerr << "get attribute error" << std::endl;
        return 1;
    }

    rc = rbfm.readRecord(fileHandle, tableAttributes, rid, data);
    if(rc != 0){
        //std::cerr << "read record error" << std::endl;
        return 1;
    }

    rc = rbfm.closeFile(fileHandle);
    if(rc != 0){
        //std::cerr << "close file error" << std::endl;
        return 1;
    }

    return 0;
}

RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data) {

    RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

    return rbfm.printRecord(attrs, data);
}

RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                  void *data) {

    RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;

    /* tableName should be different from catalog tables */
    if(tableName == "Tables" || tableName == "Columns"){
        //std::cerr << "invalid table name" << std::endl;
        return 1;
    }

    std::string fileName = tableName;
    fileName.append(".tbl");

    RC rc = rbfm.openFile(fileName, fileHandle);
    if(rc != 0){
        //std::cerr << "open file error" << std::endl;
        return 1;
    }

    std::vector<Attribute> tableAttributes;
    rc = getAttributes(tableName, tableAttributes);
    if(rc != 0){
        //std::cerr << "get attribute error" << std::endl;
        return 1;
    }

    rc = rbfm.readAttribute(fileHandle, tableAttributes, rid, attributeName, data);
    if(rc != 0){
        //std::cerr << "read attribute error" << std::endl;
        return 1;
    }

    rc = rbfm.closeFile(fileHandle);
    if(rc != 0){
        //std::cerr << "close file error" << std::endl;
        return 1;
    }

    return 0;
}

RC RelationManager::scan(const std::string &tableName,
                         const std::string &conditionAttribute,
                         const CompOp compOp,
                         const void *value,
                         const std::vector<std::string> &attributeNames,
                         RM_ScanIterator &rm_ScanIterator) {
    RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;

    /* tableName should be different from catalog tables
    if(tableName == "Tables" || tableName == "Columns"){
        //std::cerr << "invalid table name" << std::endl;
        return 1;
    }
     */

    /* Open table file */
    std::string fileName = tableName;
    fileName.append(".tbl");
    RC rc = rbfm.openFile(fileName, fileHandle);
    if(rc != 0){
        //std::cerr << "open file error" << std::endl;
        return 1;
    }

    /* Get the table schema */
    std::vector<Attribute> recordDescriptor;
    rc = getAttributes(tableName, recordDescriptor);
    if(rc != 0){
        //std::cerr << "getAttributes() Error" << std::endl;
        return 1;
    }

    /* Perform scan */
    return rbfm.scan(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames, rm_ScanIterator.rbfmsi);
}

// Extra credit work
RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
    return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
    return -1;
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
    return rbfmsi.getNextRecord(rid, data);
}

RC RM_ScanIterator::close() {
    return rbfmsi.close();
}

// QE IX related
RC RelationManager::createIndex(const std::string &tableName, const std::string &attributeName) {

    IndexManager &indexManager = IndexManager::instance();
    IXFileHandle ixFileHandle;
    RM_ScanIterator rmsi; RID rid;
    void* returnedData = malloc(PAGE_SIZE);

    RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;

    std::vector<std::string> attributeNameVector;
    attributeNameVector.push_back(attributeName); // to transform the format to vector for scan

    Attribute attribute; // one attribute the index file is made with

    /* Find the attribute in catalog */
    RC rc = rm_helpers::FindAttributeInVector(tableName, attributeName, attribute);
    if(rc!=0){ free(returnedData); return 1;}

    /* Create index file with same name as tableName */
    std::string fileName = tableName;
    fileName.append(attributeName);
    fileName.append(".idx");

    rc = indexManager.createFile(fileName);
    if(rc!=0){
        free(returnedData); return 1;
    }

    /* Scan the table w.r.t. attribute and insert key (attribute value) and rid into B tree */
    rc = indexManager.openFile(fileName, ixFileHandle);
    if(rc!=0){
        free(returnedData); return 1;
    }

    rc = this->scan(tableName, "", NO_OP, NULL, attributeNameVector, rmsi);
    if(rc!=0){
        free(returnedData); return 1;
    }

    while(rmsi.getNextTuple(rid, returnedData) != RM_EOF){
        // we need to skip the preceding null bit indicators since insertEntry expects no null flags
        rc = indexManager.insertEntry(ixFileHandle, attribute, (byte*)returnedData + 1, rid);
        if(rc!=0){
            free(returnedData); return 1;
        }
    }
    rmsi.close();

    /* Reflect this index file's existence in the catalog Tables.tbl */
    rc = rbfm.openFile("Tables.tbl", fileHandle);
    if(rc!=0){
        free(returnedData); return 1;
    }

    unsigned nextTableID = fileHandle.tableIdCounter + 1;
    unsigned tupleSize = 0;
    void* tuple = malloc(200);

    rm_helpers::prepareSystemTupleforTables(nextTableID, tableName.size(), tableName, fileName.size(), fileName, 0, tuple, &tupleSize);
    rc = rbfm.insertRecord(fileHandle, TablesAttributes, tuple, rid);
    if(rc!=0){
        free(returnedData); free(tuple); return 1;
    }
    fileHandle.tableIdCounter++;

    rc = rbfm.closeFile(fileHandle);
    if(rc!=0){
        free(returnedData); free(tuple); return 1;
    }

    /* Reflect this index file's existence in the catalog Columns.tbl */
    rc = rbfm.openFile("Columns.tbl", fileHandle);
    if(rc!=0){
        free(returnedData); free(tuple); return 1;
    }

    unsigned currentSchemaVersioning = fileHandle.schemaVersioningCounter;
    rm_helpers::prepareSystemTupleforColumns(nextTableID, attribute.name.size(), attribute.name, attribute.type, attribute.length, 1, currentSchemaVersioning, tuple, &tupleSize);
    rc = rbfm.insertRecord(fileHandle, ColumnsAttributes, tuple, rid);
    if(rc!=0){
        free(returnedData); free(tuple); return 1;
    }

    rc = rbfm.closeFile(fileHandle);
    if(rc!=0){
        free(returnedData); free(tuple); return 1;
    }

    rc = indexManager.closeFile(ixFileHandle);
    if(rc!=0){
        free(returnedData); free(tuple); return 1;
    }

    free(returnedData);
    free(tuple);
    return 0;
}

RC RelationManager::destroyIndex(const std::string &tableName, const std::string &attributeName) {

    IndexManager &indexManager = IndexManager::instance();
    RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    RBFM_ScanIterator rbfmsi_tbl, rbfmsi_col;

    std::vector<std::string> attributeNameVector;
    attributeNameVector.push_back(attributeName); // to transform the format to vector for scan


    std::string fileName = tableName;
    fileName.append(attributeName);
    fileName.append(".idx");

    /* Destroy index file */
    RC rc = indexManager.destroyFile(fileName);
    if(rc!=0) return 1;

    /* Remove corresponding tuples in catalog Tables.tbl */
    rc = rbfm.openFile("Tables.tbl", fileHandle);
    if(rc!=0) return 1;

    byte* fileNameDataRep = new byte[fileName.size() + 4];
    rbfm_helpers::stdString2RawValue(tableName, fileNameDataRep);
    rc = rbfm.scan(fileHandle, TablesAttributes, "table-name", EQ_OP, fileNameDataRep, attributeNameVector, rbfmsi_tbl);
    if(rc!=0){
        delete[] fileNameDataRep;
        return 1;
    }

    RID rid; unsigned tableID = 0;
    void* returnedData = malloc(PAGE_SIZE);
    while(rbfmsi_tbl.getNextRecord(rid,returnedData) != RBFM_EOF){
        memcpy(&tableID, (byte*)returnedData+1, 4);
        rc = rbfm.deleteRecord(fileHandle, TablesAttributes, rid);
        if(rc!=0){
            delete[] fileNameDataRep;
            free(returnedData);
            return 1;
        }
    }
    rbfmsi_tbl.close();

    rc = rbfm.closeFile(fileHandle);
    if(rc!=0){
        delete[] fileNameDataRep;
        free(returnedData);
        return 1;
    }

    /* Remove corresponding tuples in catalog Columns.tbl */
    rc = rbfm.openFile("Columns.tbl", fileHandle);
    if(rc!=0){
        delete[] fileNameDataRep;
        free(returnedData);
        return 1;
    }

    byte tableIDDataRep[4];
    memcpy(tableIDDataRep, &tableID, 4);
    rc = rbfm.scan(fileHandle, ColumnsAttributes, "table-id", EQ_OP, tableIDDataRep, attributeNameVector, rbfmsi_col);
    if(rc!=0){
        delete[] fileNameDataRep;
        free(returnedData);
        return 1;
    }

    while(rbfmsi_col.getNextRecord(rid,returnedData)!= RBFM_EOF){
        rc = rbfm.deleteRecord(fileHandle, ColumnsAttributes, rid);
        if(rc!=0){
            delete[] fileNameDataRep;
            free(returnedData);
            return 1;
        }
    }
    rbfmsi_col.close();

    rc = rbfm.closeFile(fileHandle);
    if(rc!=0){
        delete[] fileNameDataRep;
        free(returnedData);
        return 1;
    }

    delete[] fileNameDataRep;
    free(returnedData);
    return 0;

}

RC RelationManager::indexScan(const std::string &tableName,
                              const std::string &attributeName,
                              const void *lowKey,
                              const void *highKey,
                              bool lowKeyInclusive,
                              bool highKeyInclusive,
                              RM_IndexScanIterator &rm_IndexScanIterator) {

    IndexManager &indexManager = IndexManager::instance();
    //IXFileHandle ixFileHandle;

    std::string fileName = tableName;
    fileName.append(attributeName);
    fileName.append(".idx");

    Attribute attribute; // one attribute the index file is made with

    /* Find the attribute from catalog with name to set up the Attribute (name/type/length) */
    RC rc = rm_helpers::FindAttributeInVector(tableName, attributeName, attribute);
    if(rc!=0) return 1;

    /* Open the index file and return scan */
    rc = indexManager.openFile(fileName, rm_IndexScanIterator.ixsi.ixFileHandle);
    if(rc!=0) return 1;

    return indexManager.scan(rm_IndexScanIterator.ixsi.ixFileHandle, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive, rm_IndexScanIterator.ixsi);

}

