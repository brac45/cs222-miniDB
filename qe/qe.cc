
#include "qe.h"

/////////// START OF Filter Implementation ////////////

Filter::Filter(Iterator *input, const Condition &condition) {
    this->filterIter = input;
    this->filterCondition = condition;
}

RC Filter::getNextTuple(void *data) {

    while (filterIter->getNextTuple(data) == 0) {

        RC isSatisfied = qe_helpers::satisfyCondition(filterIter, filterCondition, data);
        if (isSatisfied == 0) {
            return 0;
        }
    }
    return QE_EOF;

}

void Filter::getAttributes(std::vector<Attribute> &attrs) const {

    attrs.clear();
    filterIter->getAttributes(attrs);

}

/////////// END OF Filter Implementation ////////////

/////////// START OF Project Iterator Implementation ////////////

Project::Project(Iterator *input, const std::vector<std::string> &attrNames) {
    this->inputR = input;
    this->attributeNames = attrNames;
}

RC Project::getNextTuple(void *data) {
    byte retDataBuffer[PAGE_SIZE] = {0};

    RC rc = inputR->getNextTuple(retDataBuffer);

    if (rc != QE_SUCCESS) return QE_EOF;

    /* Get the record descriptor for this input iterator
     * this returns concatenated tablename and attribute */
    std::vector<Attribute> recordDescriptor;
    inputR->getAttributes(recordDescriptor);

    /* Only memcpy the relevant values specified by attribute
     * Use a map of string to attribute position */
    std::unordered_map<std::string, unsigned> attrPosMap;

    /* Get the positions of the attributeNames */
    for (const std::string &aname: attributeNames) {
        for (unsigned i = 0; i < recordDescriptor.size(); ++i) {
            if (aname == recordDescriptor.at(i).name) {
                attrPosMap[aname] = i;
            }
        }
    }

    /* Temporary buffer for record-representation of the data,
     * For easy implementation by code reuse from RBFM getNextRecord() */
    byte recordRep[PAGE_SIZE] = {0};// int recordRepSize = 0;
    rc = rbfm_helpers::dataToRecord(recordDescriptor,
                                    retDataBuffer, recordRep);
    if (rc != 0) {
        return rc;
    }

    /* Construct null-byte indicator */
    byte *dataPtr = (byte *) data;
    unsigned numOfAFields = attributeNames.size();          // Actual number of fields
    unsigned nullSizeForReturnData = rbfm_helpers::getActualByteForNullsIndicator((int) numOfAFields);

    memset(dataPtr, 0, nullSizeForReturnData);     // Set to all zeroes for null-byte indicator

    /* Copy data while setting null bits */
    int nullBitIter = 0;        // Iterator for byte-null-indicator
    const byte MASK = 128;            // Bit mask
    unsigned writeOffset = nullSizeForReturnData;
    for (unsigned j = 0; j < numOfAFields; ++j) {
        if ((j != 0) && (j % 8 == 0)) { nullBitIter++; }  // Advance null byte iterator if 8 bits are read

        /* Construct null-bit indicator
         * TODO: Potential bug might be found here */
        unsigned attributePos = attrPosMap[attributeNames[j]];
        const byte i_result = MASK & (byte) (retDataBuffer[nullBitIter] << (attributePos % 8));
        dataPtr[nullBitIter] = dataPtr[nullBitIter] | (byte) (i_result >> (j % 8));

        /* memcpy record if not null */
        if (i_result != MASK) {
            /* Get offsets */
            unsigned nullSizeForCacheData = rbfm_helpers::getActualByteForNullsIndicator(recordDescriptor.size());
            unsigned offsetFieldStartForCacheData = 4 + nullSizeForCacheData;
            unsigned endOffset = 0, startOffset = 0;
            if (attributePos == 0) {
                startOffset = 4 + nullSizeForCacheData + (4 * recordDescriptor.size());
            } else {
                memcpy(&startOffset, recordRep + offsetFieldStartForCacheData + (4 * (attributePos - 1)), 4);
            }
            memcpy(&endOffset, recordRep + offsetFieldStartForCacheData + (4 * attributePos), 4);

            /* Memcpy contents to data */
            if (recordDescriptor[attributePos].type == TypeVarChar) {
                int valueLen = (int) endOffset - (int) startOffset;
                memcpy(dataPtr + writeOffset, &valueLen, 4);        // Write length first
                writeOffset += 4;
                memcpy(dataPtr + writeOffset, recordRep + startOffset, valueLen);
                writeOffset += valueLen;
            } else {
                unsigned valueLen = endOffset - startOffset;
                memcpy(dataPtr + writeOffset, recordRep + startOffset, valueLen);
                writeOffset += valueLen;
            }
        }
    }

    return 0;
}

void Project::getAttributes(std::vector<Attribute> &attrs) const {
    attrs.clear();

    std::vector<Attribute> recordDescriptor;
    inputR->getAttributes(recordDescriptor);

    int i = 0;
    for (const Attribute &attribute: recordDescriptor) {
        if (attribute.name == attributeNames.at(i)) {
            attrs.push_back(attribute);
            i++;
        }
    }
}

/////////// END OF Project Iterator Implementation ////////////

/////////// START OF BNLJoin Implementation ////////////

BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned numPages) {

    this->BNLJleftIn = leftIn;
    this->BNLJrightIn = rightIn;
    this->BNLJcondition = condition;
    this->BNLJnumPages = numPages;

    Value NULL_VALUE;

    RC rc = qe_helpers::setVariables(this->BNLJcondition, this->lhsTableName, this->lhsAttributeName, this->lhsAttribute, this->rhsTableName, this->rhsAttributeName, this->rhsAttribute, NULL_VALUE, ".");
    if(rc!=0) std::cerr << "error occurred while setting variables in BNLJoin" << std::endl;

    /* joinedAttr setting */
    std::vector<Attribute> lhsAttrs, rhsAttrs;
    leftIn->getAttributes(lhsAttrs);
    rightIn->getAttributes(rhsAttrs);

    joinedAttr.clear();

    for(Attribute &i:lhsAttrs)
        joinedAttr.push_back(i);
    for(Attribute &i:rhsAttrs)
        joinedAttr.push_back(i);

    /* temp file setting */
    std::chrono::milliseconds ms = std::chrono::duration_cast< std::chrono::milliseconds >(
            std::chrono::system_clock::now().time_since_epoch()
    );
    std::string tempfilename = ".bnl_" + std::to_string(ms.count()) + ".tbl";

    RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
    rc = rbfm.createFile(tempfilename);
    if (rc != 0) {
        std::cerr << "Error in BNLJoin constructor! createFile() " << std::endl;
        exit(-1);
    }

    rc = rbfm.openFile(tempfilename, resultFileHandle);
    if (rc != 0) {
        std::cerr << "Error in BNLJoin constructor! openFile() " << std::endl;
        exit(-1);
    }

    /* Do join on R, S and save results to temp file */
    rc = constructTempScanTbl();
    if (rc != 0) {
        std::cerr << "Error in constructing temporary scan file! " << std::endl;
        exit(-1);
    }

    /* Do a scan on the join result table */
    std::vector<std::string> allAttrs;
    for (const Attribute& attr: joinedAttr) {
        allAttrs.push_back(attr.name);
    }
    rbfm.scan(resultFileHandle,
              joinedAttr,
              "",
              NO_OP,
              NULL,
              allAttrs,
              resultIterator);

}


RC BNLJoin::getNextTuple(void *data) {

    RID rid;
    return resultIterator.getNextRecord(rid,data);

}

void BNLJoin::getAttributes(std::vector<Attribute> &attrs) const {

    attrs.clear();
    attrs = this->joinedAttr;

}

RC BNLJoin::constructTempScanTbl() {
    RC rc;
    RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

    std::vector<Attribute> lhsAttrs, rhsAttrs;
    this->BNLJleftIn->getAttributes(lhsAttrs);
    this->BNLJrightIn->getAttributes(rhsAttrs);

    this->lhsBlockInMem = malloc((this->BNLJnumPages - 2)*PAGE_SIZE ); // Assign memory for hash table

    byte lhsData[PAGE_SIZE]  ; // one tuple
    byte lhsKey[PAGE_SIZE]  ; // one attribute value in the tuple

    unsigned totalSize = 0, sizeOfTuple = 0;
    unsigned totalSizeMax = (this->BNLJnumPages - 2) * PAGE_SIZE;

    //while( this->BNLJleftIn->getNextTuple(lhsData) != QE_EOF && totalSize < totalSizeMax ) {
    if (BNLJleftIn->getNextTuple(lhsData) != QE_SUCCESS) {
        std::cerr << "No records in left table!!" <<std::endl;
        return QE_FAIL;
    }

    while (true) {
        /* Put (B-2) pages into the hash table, ummap, using lhsAttribute value as a key */
        sizeOfTuple = qe_helpers::computeSizeOfTuple(lhsAttrs, lhsData);
        rm_helpers::ExtractKeyValueFromData(this->lhsTableName, this->lhsAttributeName, lhsData, lhsKey);

        // Attach the size of this tuple in front of the original data
        byte lhsDataPlusSize[sizeOfTuple + 4];
        memcpy(lhsDataPlusSize, &sizeOfTuple, 4);
        memcpy(lhsDataPlusSize + 4, lhsData, sizeOfTuple);

        memcpy((byte *) lhsBlockInMem + totalSize, lhsDataPlusSize, sizeOfTuple + 4);

        AttrType type = this->lhsAttribute.type;
        switch (type) {
            case TypeInt: {

                int lhsKeyValue = *(int *) lhsKey;
                std::pair<int, void *> insertingPair(lhsKeyValue, (byte *) lhsBlockInMem + totalSize);
                ummapInt.insert(insertingPair);
                break;

            }
            case TypeReal: {

                float lhsKeyValue = *(float *) lhsKey;
                std::pair<float, void *> insertingPair(lhsKeyValue, (byte *) lhsBlockInMem + totalSize);
                ummapReal.insert(insertingPair);
                break;

            }
            case TypeVarChar: {

                int len = 0;
                memcpy(&len, lhsKey, 4);
                lhsKey[len] = '\0';

                std::string lhsKeyValue;
                lhsKeyValue.append((const char *) lhsKey, len + 1);

                std::pair<std::string, void *> insertingPair(lhsKeyValue, (byte *) lhsBlockInMem + totalSize);
                ummapVarChar.insert(insertingPair);
                break;

            }
        } // end switch

        totalSize += sizeOfTuple + 4;
        // end setting ummap

        if (totalSize < totalSizeMax) {
            if (BNLJleftIn->getNextTuple(lhsData) != QE_EOF) {
                continue;
            }
        }

        /* pull each record from right table and to join */
        this->BNLJrightIn->setIterator();

        byte rhsData[PAGE_SIZE] = {0}; // one record
        byte rhsKey[PAGE_SIZE] = {0}; // one attribute value in the tuple
        while (this->BNLJrightIn->getNextTuple(rhsData) != QE_EOF) {

            rm_helpers::ExtractKeyValueFromData(this->rhsTableName, this->rhsAttributeName, rhsData, rhsKey);
            unsigned sizeOfRhsTuple = qe_helpers::computeSizeOfTuple(rhsAttrs, rhsData);

            AttrType type = this->rhsAttribute.type;
            switch (type) {
                case TypeInt: {

                    int rhsKeyValue = *(int *) rhsKey;

                    auto it = ummapInt.find(rhsKeyValue);
                    for (int i = 0; i < ummapInt.count(rhsKeyValue); i++) {
                        void *lhsTupleMatched = it->second;

                        int lhsTupleSize = -1;
                        memcpy(&lhsTupleSize, lhsTupleMatched, 4);

                        byte lhsTuple[PAGE_SIZE] = {0};
                        memcpy(lhsTuple, (byte *) lhsTupleMatched + 4, lhsTupleSize);

                        byte *JoinedTuple = new byte[lhsTupleSize + sizeOfRhsTuple];
                        rc = qe_helpers::concatTuples(lhsTuple, lhsAttrs, rhsData, rhsAttrs, JoinedTuple);
                        if (rc != 0) {
                            delete[] JoinedTuple;
                            return 1;
                        }
                        RID rid;
                        rc = rbfm.insertRecord(resultFileHandle, joinedAttr, JoinedTuple, rid);
                        if (rc != 0) {
                            delete[] JoinedTuple;
                            return 1;
                        }
                        delete[] JoinedTuple;

                        //rbfm.printRecord(joinedAttr, JoinedTuple); //FIXME: debugging
                    }
                    break;

                }
                case TypeReal: {

                    float rhsKeyValue = *(float *) rhsKey;
                    auto it = ummapReal.find(rhsKeyValue);
                    for (int i = 0; i < ummapReal.count(rhsKeyValue); i++) {
                        void *lhsTupleMatched = it->second;

                        int lhsTupleSize = -1;
                        memcpy(&lhsTupleSize, lhsTupleMatched, 4);

                        byte lhsTuple[PAGE_SIZE] = {0};
                        memcpy(lhsTuple, (byte *) lhsTupleMatched + 4, lhsTupleSize);

                        byte *JoinedTuple = new byte[lhsTupleSize + sizeOfRhsTuple];
                        rc = qe_helpers::concatTuples(lhsTuple, lhsAttrs, rhsData, rhsAttrs, JoinedTuple);
                        if (rc != 0) {
                            delete[] JoinedTuple;
                            return 1;
                        }
                        RID rid;
                        rc = rbfm.insertRecord(resultFileHandle, joinedAttr, JoinedTuple, rid);
                        if (rc != 0) {
                            delete[] JoinedTuple;
                            return 1;
                        }
                        delete[] JoinedTuple;
                    }
                    break;

                }
                case TypeVarChar: {

                    int strLen = 0;
                    memcpy(&strLen, rhsKey, 4);
                    rhsKey[strLen] = '\0';

                    std::string rhsKeyValue;
                    rhsKeyValue.append((const char *) rhsKey, strLen + 1);

                    auto it = ummapVarChar.find(rhsKeyValue);
                    for (int i = 0; i < ummapVarChar.count(rhsKeyValue); i++) {
                        void *lhsTupleMatched = it->second;

                        int lhsTupleSize = -1;
                        memcpy(&lhsTupleSize, lhsTupleMatched, 4);

                        byte lhsTuple[PAGE_SIZE] = {0};
                        memcpy(lhsTuple, (byte *) lhsTupleMatched + 4, lhsTupleSize);

                        byte *JoinedTuple = new byte[lhsTupleSize + sizeOfRhsTuple];
                        rc = qe_helpers::concatTuples(lhsTuple, lhsAttrs, rhsData, rhsAttrs, JoinedTuple);
                        if (rc != 0) {
                            delete[] JoinedTuple;
                            return 1;
                        }
                        RID rid;
                        rc = rbfm.insertRecord(resultFileHandle, joinedAttr, JoinedTuple, rid);
                        if (rc != 0) {
                            delete[] JoinedTuple;
                            return 1;
                        }
                        delete[] JoinedTuple;
                    }
                    break;
                }
            }

        }

        /* Init the map to read new block from the left */
        if (this->BNLJleftIn->getNextTuple(lhsData) != QE_EOF) {
            totalSize = 0;
            ummapInt.clear();
            ummapReal.clear();
            ummapVarChar.clear();
        } else {
            break;
        }
    }
    //} while (this->BNLJleftIn->getNextTuple(lhsData) != QE_EOF);

    return 0;

}

BNLJoin::~BNLJoin() {
    resultIterator.close();
    /* Close and destroy temporary file */
    RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
    RC rc = rbfm.closeFile(resultFileHandle);
    if (rc != 0) {
        std::cerr << "Error in BNLJoin destructor! closeFile() " << std::endl;
        exit(-1);
    }

    rc = rbfm.destroyFile(resultFileHandle.thisFileName);
    if (rc != 0) {
        std::cerr << "Error in BNLJoin destructor! destroyFile() " << std::endl;
        exit(-1);
    }

    free(lhsBlockInMem);
}



/////////// END OF BNLJoin Implementation ////////////


/////////// START OF INLJoin Implementation ////////////

INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition) {
    this->inputR = leftIn;
    this->indexInputS = rightIn;
    this->joinCondition = condition;

    joinedAttr.clear();

    /* Concatenate attributes */
    std::vector<Attribute> leftAttrs;
    std::vector<Attribute> rightAttrs;

    inputR->getAttributes(leftAttrs);
    indexInputS->getAttributes(rightAttrs);

    for (const Attribute &attribute: leftAttrs) {
        joinedAttr.push_back(attribute);
    }

    for (const Attribute &attribute: rightAttrs) {
        joinedAttr.push_back(attribute);
    }

    /* Create empty temporary record-based file */
    std::chrono::milliseconds ms = std::chrono::duration_cast< std::chrono::milliseconds >(
            std::chrono::system_clock::now().time_since_epoch()
    );
    std::string tempfilename = ".inl_" + std::to_string(ms.count()) + ".tbl";

    RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
    RC rc = rbfm.createFile(tempfilename);
    if (rc != 0) {
        std::cerr << "Error in INLJoin constructor! createFile() " << std::endl;
        exit(-1);
    }

    rc = rbfm.openFile(tempfilename, resultFileHandle);
    if (rc != 0) {
        std::cerr << "Error in INLJoin constructor! openFile() " << std::endl;
        exit(-1);
    }

    /* Do join on R, S and save results to temp file */
    rc = constructTempScanTbl();
    if (rc != 0) {
        std::cerr << "Error in constructing temporary scan file! " << std::endl;
        exit(-1);
    }

    /* Do a scan on the join result table */
    std::vector<std::string> allAttrs;
    for (const Attribute &attr: joinedAttr) {
        allAttrs.push_back(attr.name);
    }

    rbfm.scan(resultFileHandle,
              joinedAttr,
              "",
              NO_OP,
              NULL,
              allAttrs,
              resultIterator);
}

RC INLJoin::getNextTuple(void *data) {
    RID rid;
    return resultIterator.getNextRecord(rid, data);
}

void INLJoin::getAttributes(std::vector<Attribute> &attrs) const {
    attrs.clear();
    attrs = joinedAttr;
}

RC INLJoin::constructTempScanTbl() {
    // TODO: constructs temporary table that houses the join results
    if (joinCondition.op == EQ_OP && joinCondition.bRhsIsAttr) {
        /* Equal join, project scope requires only equal join on attributes */
        byte leftTuple[PAGE_SIZE]={0};
        byte rightTuple[PAGE_SIZE]={0};

        /* For each tuple in the left outer table */
        while (inputR->getNextTuple(leftTuple) != QE_EOF) {
            /* Get attributes */
            std::vector<Attribute> leftAttrs;
            inputR->getAttributes(leftAttrs);

            /* Check if attribute value is NULL -> we need to skip these */
            if (qe_helpers::isAttrValueNull(leftAttrs, joinCondition.lhsAttr, leftTuple)) {
                continue;       // Skip
            }

            /* Find and get the data */
            byte condValue[PAGE_SIZE]={0};       // Buffer for holding condition data
            RC rc = rm_helpers::ExtractKeyValueFromData(leftAttrs, joinCondition.lhsAttr, leftTuple, condValue);
            if (rc != QE_SUCCESS) {
                return rc;
            }

            /* Set iterator for right inner table to match */
            indexInputS->setIterator(condValue, condValue, true, true);
            while (indexInputS->getNextTuple(rightTuple) != QE_EOF) {
                byte joinedTuple[PAGE_SIZE]={0};
                std::vector<Attribute> rightAttrs;
                indexInputS->getAttributes(rightAttrs);

                /* Check if attribute value is NULL -> we need to skip these */
                if (qe_helpers::isAttrValueNull(rightAttrs, joinCondition.rhsAttr, rightTuple)) {
                    continue;       // Skip
                }

                /* Concatenate two tuples */
                qe_helpers::concatTuples(leftTuple, leftAttrs, rightTuple, rightAttrs, joinedTuple);

                /* Insert into resulting table */
                RID rid;
                RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
                rbfm.insertRecord(resultFileHandle, joinedAttr, joinedTuple, rid);
            }
        }

        return QE_SUCCESS;
    } else {
        std::cerr << "Unsupported operation!" << std::endl;
        return QE_FAIL;
    }
}

INLJoin::~INLJoin() {
    resultIterator.close();
    /* Close and destroy temporary file */
    RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
    RC rc = rbfm.closeFile(resultFileHandle);
    if (rc != 0) {
        std::cerr << "Error in INLJoin destructor! closeFile() " << std::endl;
        exit(-1);
    }

    rc = rbfm.destroyFile(resultFileHandle.thisFileName);
    if (rc != 0) {
        std::cerr << "Error in INLJoin destructor! destroyFile() " << std::endl;
        exit(-1);
    }

    // TODO: should I call delete on the R and S input pointers??
}

/////////// END OF INLJoin Implementation ////////////

/////////// START OF Aggregate Implementation ////////////

Aggregate::Aggregate(Iterator *_input, const Attribute &_aggAttr, AggregateOp op) {
    aggAttr = _aggAttr;
    aggOp = op;
    input = _input;
    isGroupAggr = false;
    groupAttr.type = TypeInt;
    groupAttr.name = "";
    groupAttr.length = 4;
    interpretAs = TypeReal;

    aggregate();
}

Aggregate::Aggregate(Iterator *_input, const Attribute &_aggAttr, const Attribute &_groupAttr, AggregateOp op) {
    aggAttr = _aggAttr;
    aggOp = op;
    input = _input;
    isGroupAggr = true;
    groupAttr = _groupAttr;
    interpretAs = TypeReal;

    aggregate();
}

void Aggregate::aggregate() {
    /* Aggregate all tuples */
    byte buffer[PAGE_SIZE]={0};
    byte groupAttrValue[PAGE_SIZE]={0};         // VarChar can be at most PAGE_SIZE
    memset(groupAttrValue, 0, PAGE_SIZE);

    //bool firstTime = true;
    while (input->getNextTuple(buffer) != QE_EOF) {
        /* Extract the value based on attribute */
        byte attrValue[PAGE_SIZE]={0};
        std::vector<Attribute> tableAttrs;
        input->getAttributes(tableAttrs);

        /* Check if attribute value is NULL -> we need to skip these */
        if (qe_helpers::isAttrValueNull(tableAttrs, aggAttr.name, buffer)) {
            continue;       // Skip
        }

        /* Get the key for groupby, key is int type with 0 value if basic */

        if (isGroupAggr) {
            memset(groupAttrValue, '\0', PAGE_SIZE);
            RC rc = rm_helpers::ExtractKeyValueFromData(tableAttrs, groupAttr.name, buffer, groupAttrValue);
            if (rc != QE_SUCCESS) {
                std::cerr << "Error in extracting value from returned tuple!" << std::endl;
            }
        }

        RC rc = rm_helpers::ExtractKeyValueFromData(tableAttrs, aggAttr.name, buffer, attrValue);
        if (rc != QE_SUCCESS) {
            std::cerr << "Error in extracting value from returned tuple!" << std::endl;
        }

        /* Switch on operation */
        if (aggOp == MIN) {
            /* Try to find the value associated with the groupAttrValue */
            if (groupAttr.type == TypeInt) {
                auto get = intKeyTable.find(*(int*)groupAttrValue);

                if (get == intKeyTable.end()) {
                    interpretAs = aggAttr.type;
                    byte* valuePair = new byte[4];

                    /* Set current MIN to this */
                    memcpy(valuePair, attrValue, 4);         // Should only be real or int

                    /* Insert to map */
                    std::pair<int, byte*> item(*(int*)groupAttrValue,valuePair);
                    intKeyTable.insert(item);

                } else {
                    if (qe_helpers::compareValue(LT_OP, aggAttr.type, attrValue, get->second) == 0) {
                        /* Set current MIN to this */
                        memcpy(get->second, attrValue, 4);         // Should only be real or int
                    }
                }
            } else if (groupAttr.type == TypeReal) {
                auto get = realKeyTable.find(*(float*)groupAttrValue);

                if (get == realKeyTable.end()) {
                    interpretAs = aggAttr.type;
                    byte* valuePair = new byte[4];

                    /* Set current MIN to this */
                    memcpy(valuePair, attrValue, 4);         // Should only be real or int

                    /* Insert to map */
                    std::pair<float, byte*> item(*(float*)groupAttrValue,valuePair);
                    realKeyTable.insert(item);

                } else {
                    if (qe_helpers::compareValue(LT_OP, aggAttr.type, attrValue, get->second) == 0) {
                        /* Set current MIN to this */
                        memcpy(get->second, attrValue, 4);         // Should only be real or int
                    }
                }
            } else {
                auto get = varCharKeyTable.find(std::string((char*)groupAttrValue));        // MUST be null-terminated

                if (get == varCharKeyTable.end()) {     // If not found
                    interpretAs = aggAttr.type;
                    byte* valuePair = new byte[4];

                    /* Set current MIN to this */
                    memcpy(valuePair, attrValue, 4);         // Should only be real or int

                    /* Insert to map */
                    std::pair<std::string, byte*> item(std::string((char*)groupAttrValue),valuePair);
                    varCharKeyTable.insert(item);

                } else {
                    if (qe_helpers::compareValue(LT_OP, aggAttr.type, attrValue, get->second) == 0) {
                        /* Set current MIN to this */
                        memcpy(get->second, attrValue, 4);         // Should only be real or int
                    }
                }
            }
        }
        else if (aggOp == MAX) {
            /* Try to find the value associated with the groupAttrValue */
            if (groupAttr.type == TypeInt) {
                auto get = intKeyTable.find(*(int*)groupAttrValue);

                if (get == intKeyTable.end()) {
                    interpretAs = aggAttr.type;
                    byte* valuePair = new byte[4];

                    /* Set current MIN to this */
                    memcpy(valuePair, attrValue, 4);         // Should only be real or int

                    /* Insert to map */
                    std::pair<int, byte*> item(*(int*)groupAttrValue,valuePair);
                    intKeyTable.insert(item);

                } else {
                    if (qe_helpers::compareValue(GT_OP, aggAttr.type, attrValue, get->second) == 0) {
                        /* Set current MIN to this */
                        memcpy(get->second, attrValue, 4);         // Should only be real or int
                    }
                }
            } else if (groupAttr.type == TypeReal) {
                auto get = realKeyTable.find(*(float*)groupAttrValue);

                if (get == realKeyTable.end()) {
                    interpretAs = aggAttr.type;
                    byte* valuePair = new byte[4];

                    /* Set current MIN to this */
                    memcpy(valuePair, attrValue, 4);         // Should only be real or int

                    /* Insert to map */
                    std::pair<float, byte*> item(*(float*)groupAttrValue,valuePair);
                    realKeyTable.insert(item);

                } else {
                    if (qe_helpers::compareValue(GT_OP, aggAttr.type, attrValue, get->second) == 0) {
                        /* Set current MIN to this */
                        memcpy(get->second, attrValue, 4);         // Should only be real or int
                    }
                }
            } else {
                auto get = varCharKeyTable.find(std::string((char*)groupAttrValue));        // MUST be null-terminated

                if (get == varCharKeyTable.end()) {     // If not found
                    interpretAs = aggAttr.type;
                    byte* valuePair = new byte[4];

                    /* Set current MIN to this */
                    memcpy(valuePair, attrValue, 4);         // Should only be real or int

                    /* Insert to map */
                    std::pair<std::string, byte*> item(std::string((char*)groupAttrValue),valuePair);
                    varCharKeyTable.insert(item);

                } else {
                    if (qe_helpers::compareValue(GT_OP, aggAttr.type, attrValue, get->second) == 0) {
                        /* Set current MIN to this */
                        memcpy(get->second, attrValue, 4);         // Should only be real or int
                    }
                }
            }
        }
        else if (aggOp == COUNT) {
            /* Try to find the value associated with the groupAttrValue */
            if (groupAttr.type == TypeInt) {
                auto get = intKeyTable.find(*(int*)groupAttrValue);

                if (get == intKeyTable.end()) {
                    interpretAs = TypeReal;
                    byte* valuePair = new byte[4];
                    *(float*)valuePair = 1.0;

                    /* Insert to map */
                    std::pair<int, byte*> item(*(int*)groupAttrValue,valuePair);
                    intKeyTable.insert(item);

                } else {
                    *(float*)get->second += 1.0;      // Increase count
                }
            } else if (groupAttr.type == TypeReal) {
                auto get = realKeyTable.find(*(float*)groupAttrValue);

                if (get == realKeyTable.end()) {
                    interpretAs = TypeReal;
                    byte* valuePair = new byte[4];
                    *(float*)valuePair = 1.0;

                    /* Insert to map */
                    std::pair<float, byte*> item(*(float*)groupAttrValue,valuePair);
                    realKeyTable.insert(item);

                } else {
                    *(float*)get->second += 1.0;      // Increase count
                }
            } else {
                auto get = varCharKeyTable.find(std::string((char*)groupAttrValue));

                if (get == varCharKeyTable.end()) {
                    interpretAs = TypeReal;
                    byte* valuePair = new byte[4];
                    *(float*)valuePair = 1.0;

                    /* Insert to map */
                    std::pair<std::string, byte*> item(std::string((char*)groupAttrValue),valuePair);
                    varCharKeyTable.insert(item);

                } else {
                    *(float*)get->second += 1.0;      // Increase count
                }
            }
        }
        else if (aggOp == SUM) {
            /* Try to find the value associated with the groupAttrValue */
            if (groupAttr.type == TypeInt) {
                /* Add to data buffer -> keeps counting sum
                 * We switch between types since IEEE 754 standard is not precise and we want to just pass the tests.. */
                auto getSum = intKeyTable.find(*(int*)groupAttrValue);
                if (getSum == intKeyTable.end()) {
                    byte* valuePair = new byte[4];
                    if (aggAttr.type == TypeInt)
                        *(int*)valuePair = 0;
                    else *(float*)valuePair = 0.0;

                    interpretAs = aggAttr.type;

                    /* Insert to map */
                    std::pair<int, byte*> item(*(int*)groupAttrValue,valuePair);
                    intKeyTable.insert(item);

                }

                getSum = intKeyTable.find(*(int*)groupAttrValue);
                if (aggAttr.type == TypeInt) {
                    int convertedAttrValue = (*(int*)attrValue);
                    int sum = (*(int*)getSum->second);
                    sum += convertedAttrValue;
                    memcpy(getSum->second, &sum, 4);
                } else {
                    float convertedAttrValue = (*(float*)attrValue);
                    float sum = (*(float*)getSum->second);
                    sum += convertedAttrValue;
                    memcpy(getSum->second, &sum, 4);
                }
            } else if (groupAttr.type == TypeReal) {
                /* Add to data buffer -> keeps counting sum
                 * We switch between types since IEEE 754 standard is not precise and we want to just pass the tests.. */
                auto getSum = realKeyTable.find(*(float*)groupAttrValue);
                if (getSum == realKeyTable.end()) {
                    byte* valuePair = new byte[4];
                    if (aggAttr.type == TypeInt)
                        *(int*)valuePair = 0;
                    else *(float*)valuePair = 0.0;

                    interpretAs = aggAttr.type;

                    /* Insert to map */
                    std::pair<float, byte*> item(*(float*)groupAttrValue,valuePair);
                    realKeyTable.insert(item);

                }

                getSum = realKeyTable.find(*(float*)groupAttrValue);
                if (aggAttr.type == TypeInt) {
                    int convertedAttrValue = (*(int*)attrValue);
                    int sum = (*(int*)getSum->second);
                    sum += convertedAttrValue;
                    memcpy(getSum->second, &sum, 4);
                } else {
                    float convertedAttrValue = (*(float*)attrValue);
                    float sum = (*(float*)getSum->second);
                    sum += convertedAttrValue;
                    memcpy(getSum->second, &sum, 4);
                }
            } else {
                /* Add to data buffer -> keeps counting sum
                 * We switch between types since IEEE 754 standard is not precise and we want to just pass the tests.. */
                auto getSum = varCharKeyTable.find(std::string((char*)groupAttrValue));
                if (getSum == varCharKeyTable.end()) {
                    byte* valuePair = new byte[4];
                    if (aggAttr.type == TypeInt)
                        *(int*)valuePair = 0;
                    else *(float*)valuePair = 0.0;

                    interpretAs = aggAttr.type;

                    /* Insert to map */
                    std::pair<std::string, byte*> item(std::string((char*)groupAttrValue),valuePair);
                    varCharKeyTable.insert(item);

                }

                getSum = varCharKeyTable.find(std::string((char*)groupAttrValue));
                if (aggAttr.type == TypeInt) {
                    int convertedAttrValue = (*(int*)attrValue);
                    int sum = (*(int*)getSum->second);
                    sum += convertedAttrValue;
                    memcpy(getSum->second, &sum, 4);
                } else {
                    float convertedAttrValue = (*(float*)attrValue);
                    float sum = (*(float*)getSum->second);
                    sum += convertedAttrValue;
                    memcpy(getSum->second, &sum, 4);
                }
            }
        }
        else if (aggOp == AVG) {
            /* Try to find the value associated with the groupAttrValue */
            if (groupAttr.type == TypeInt) {
                auto get = intKeyTable.find(*(int*)groupAttrValue);

                if (get == intKeyTable.end()) {
                    byte* valuePair = new byte[4];
                    *(float*)valuePair = 1.0;

                    /* Insert to map */
                    std::pair<int, byte*> item(*(int*)groupAttrValue,valuePair);
                    intKeyTable.insert(item);

                } else {
                    *(float*)get->second += 1.0;      // Increase count
                }

                /* Add to data buffer -> keeps counting sum
                 * We switch between types since IEEE 754 standard is not precise and we want to just pass the tests.. */
                auto getSum = intKeySumTable.find(*(int*)groupAttrValue);
                if (getSum == intKeySumTable.end()) {
                    byte* valuePair = new byte[4];
                    if (aggAttr.type == TypeInt)
                        *(int*)valuePair = 0;
                    else *(float*)valuePair = 0.0;

                    /* Insert to map */
                    std::pair<int, byte*> item(*(int*)groupAttrValue,valuePair);
                    intKeySumTable.insert(item);

                }

                getSum = intKeySumTable.find(*(int*)groupAttrValue);
                if (getSum == intKeySumTable.end()) std::cerr << "Error" << std::endl;
                if (aggAttr.type == TypeInt) {
                    int convertedAttrValue = (*(int*)attrValue);
                    int sum = (*(int*)getSum->second);
                    sum += convertedAttrValue;
                    memcpy(getSum->second, &sum, 4);
                } else {
                    float convertedAttrValue = (*(float*)attrValue);
                    float sum = (*(float*)getSum->second);
                    sum += convertedAttrValue;
                    memcpy(getSum->second, &sum, 4);
                }
            } else if (groupAttr.type == TypeReal) {
                auto get = realKeyTable.find(*(float*)groupAttrValue);

                if (get == realKeyTable.end()) {
                    byte* valuePair = new byte[4];
                    *(float*)valuePair = 1.0;

                    /* Insert to map */
                    std::pair<float, byte*> item(*(float*)groupAttrValue,valuePair);
                    realKeyTable.insert(item);

                } else {
                    *(float*)get->second += 1.0;      // Increase count
                }

                /* Add to data buffer -> keeps counting sum
                 * We switch between types since IEEE 754 standard is not precise and we want to just pass the tests.. */
                auto getSum = realKeySumTable.find(*(float*)groupAttrValue);
                if (getSum == realKeySumTable.end()) {
                    byte* valuePair = new byte[4];
                    if (aggAttr.type == TypeInt)
                        *(int*)valuePair = 0;
                    else *(float*)valuePair = 0.0;

                    /* Insert to map */
                    std::pair<float, byte*> item(*(float*)groupAttrValue,valuePair);
                    realKeySumTable.insert(item);

                }

                getSum = realKeySumTable.find(*(float*)groupAttrValue);
                if (aggAttr.type == TypeInt) {
                    int convertedAttrValue = (*(int*)attrValue);
                    int sum = (*(int*)getSum->second);
                    sum += convertedAttrValue;
                    memcpy(getSum->second, &sum, 4);
                } else {
                    float convertedAttrValue = (*(float*)attrValue);
                    float sum = (*(float*)getSum->second);
                    sum += convertedAttrValue;
                    memcpy(getSum->second, &sum, 4);
                }
            }
            else {
                auto get = varCharKeyTable.find(std::string((char*)groupAttrValue));

                if (get == varCharKeyTable.end()) {
                    byte* valuePair = new byte[4];
                    *(float*)valuePair = 1.0;

                    /* Insert to map */
                    std::pair<std::string, byte*> item(std::string((char*)groupAttrValue),valuePair);
                    varCharKeyTable.insert(item);

                } else {
                    *(float*)get->second += 1.0;      // Increase count
                }

                /* Add to data buffer -> keeps counting sum
                 * We switch between types since IEEE 754 standard is not precise and we want to just pass the tests.. */
                auto getSum = varCharKeySumTable.find(std::string((char*)groupAttrValue));
                if (getSum == varCharKeySumTable.end()) {
                    byte* valuePair = new byte[4];
                    if (aggAttr.type == TypeInt)
                        *(int*)valuePair = 0;
                    else *(float*)valuePair = 0.0;

                    /* Insert to map */
                    std::pair<std::string, byte*> item(std::string((char*)groupAttrValue),valuePair);
                    varCharKeySumTable.insert(item);

                }

                getSum = varCharKeySumTable.find(std::string((char*)groupAttrValue));
                if (aggAttr.type == TypeInt) {
                    int convertedAttrValue = (*(int*)attrValue);
                    int sum = (*(int*)getSum->second);
                    sum += convertedAttrValue;
                    memcpy(getSum->second, &sum, 4);
                } else {
                    float convertedAttrValue = (*(float*)attrValue);
                    float sum = (*(float*)getSum->second);
                    sum += convertedAttrValue;
                    memcpy(getSum->second, &sum, 4);
                }
            }
        }
    }

    // sum: 6950
    // count: 100
    if (aggOp == AVG) {
        interpretAs = TypeReal;
        if (groupAttr.type == TypeInt) {
            for (auto & itr: intKeySumTable) {
                /* Average it out and save to opBuffer */
                float sum = (aggAttr.type == TypeReal) ? (*(float*)itr.second) : (float)(*(int*)itr.second);
                auto resultItr = intKeyTable.find(itr.first);
                float count = *(float*)resultItr->second;
                float average = sum / count;
                memcpy(resultItr->second, &average, 4);
            }
        } else if (groupAttr.type == TypeReal) {
            for (auto & itr: realKeySumTable) {
                /* Average it out and save to opBuffer */
                float sum = (aggAttr.type == TypeReal) ? (*(float*)itr.second) : (float)(*(int*)itr.second);
                auto resultItr = realKeyTable.find(itr.first);
                float count = *(float*)resultItr->second;
                float average = sum / count;
                memcpy(resultItr->second, &average, 4);
            }
        } else {
            for (auto & itr: varCharKeySumTable) {
                /* Average it out and save to opBuffer */
                float sum = (aggAttr.type == TypeReal) ? (*(float*)itr.second) : (float)(*(int*)itr.second);
                auto resultItr = varCharKeyTable.find(itr.first);
                float count = *(float*)resultItr->second;
                float average = sum / count;
                memcpy(resultItr->second, &average, 4);
            }
        }
    }

    // Set all the iterators
    intTblIter = intKeyTable.begin();
    realTblIter = realKeyTable.begin();
    varCharTblIter = varCharKeyTable.begin();
}

RC Aggregate::getNextTuple(void *data) {
    if (isGroupAggr) {
        if (groupAttr.type == TypeInt) {
            if (intTblIter != intKeyTable.end()) {
                byte nullFlag = 0;
                float result = interpretAs == TypeInt ?
                               (float)*(int*)intTblIter->second :
                               *(float*)intTblIter->second;
                memcpy(data, &nullFlag, 1);
                memcpy((byte*)data + 1, &intTblIter->first, 4);
                memcpy((byte*)data + 5, &result, 4);

                ++intTblIter;

                return 0;
            } else {
                return QE_EOF;
            }
        } else if (groupAttr.type == TypeReal) {
            if (realTblIter != realKeyTable.end()) {
                byte nullFlag = 0;
                float result = interpretAs == TypeInt ?
                               (float)*(int*)realTblIter->second :
                               *(float*)realTblIter->second;

                memcpy(data, &nullFlag, 1);
                memcpy((byte*)data + 1, &realTblIter->first, 4);
                memcpy((byte*)data + 5, &result, 4);

                ++realTblIter;

                return 0;
            } else {
                return QE_EOF;
            }
        } else {
            if (varCharTblIter != varCharKeyTable.end()) {
                byte nullFlag = 0;
                float result = interpretAs == TypeInt ?
                               (float)*(int*)varCharTblIter->second :
                               *(float*)varCharTblIter->second;

                int groupAttrLen = varCharTblIter->first.size();

                memcpy(data, &nullFlag, 1);
                memcpy((byte*)data + 1, &groupAttrLen, 4);
                memcpy((byte*)data + 5, varCharTblIter->first.c_str(), groupAttrLen);
                memcpy((byte*)data + groupAttrLen + 5, &result, 4);

                ++varCharTblIter;

                return 0;
            } else {
                return QE_EOF;
            }
        }
    } else {
        if (groupAttr.type == TypeInt) {
            if (intTblIter != intKeyTable.end()) {
                byte nullFlag = 0;
                float result = interpretAs == TypeInt ?
                               (float)*(int*)intTblIter->second :
                                *(float*)intTblIter->second;

                memcpy(data, &nullFlag, 1);
                memcpy((byte*)data + 1, &result, 4);

                ++intTblIter;

                return 0;
            } else {
                return QE_EOF;
            }
        } else if (groupAttr.type == TypeReal) {
            if (realTblIter != realKeyTable.end()) {
                byte nullFlag = 0;
                float result = interpretAs == TypeInt ?
                               (float)*(int*)realTblIter->second :
                                *(float*)realTblIter->second;

                memcpy(data, &nullFlag, 1);
                memcpy((byte*)data + 1, &result, 4);

                ++realTblIter;

                return 0;
            } else {
                return QE_EOF;
            }
        } else {
            if (varCharTblIter != varCharKeyTable.end()) {
                byte nullFlag = 0;
                float result = interpretAs == TypeInt ?
                               (float)*(int*)varCharTblIter->second :
                                *(float*)varCharTblIter->second;

                memcpy(data, &nullFlag, 1);
                memcpy((byte*)data + 1, &result, 4);

                ++varCharTblIter;

                return 0;
            } else {
                return QE_EOF;
            }
        }
    }
}

void Aggregate::getAttributes(std::vector<Attribute> &attrs) const {
    attrs.clear();
    std::vector<Attribute> inputAttrs;
    input->getAttributes(inputAttrs);
    for (const Attribute &attr: inputAttrs) {
        if (attr.name == aggAttr.name) {
            if (aggOp == MIN) {
                Attribute output = attr;
                output.name = "MIN(" + aggAttr.name + ")";
                attrs.push_back(output);
            } else if (aggOp == MAX) {
                Attribute output = attr;
                output.name = "MAX(" + aggAttr.name + ")";
                attrs.push_back(output);
            } else if (aggOp == COUNT) {
                Attribute output = attr;
                output.name = "COUNT(" + aggAttr.name + ")";
                attrs.push_back(output);
            } else if (aggOp == SUM) {
                Attribute output = attr;
                output.name = "SUM(" + aggAttr.name + ")";
                attrs.push_back(output);
            } else if (aggOp == AVG) {
                Attribute output = attr;
                output.name = "AVG(" + aggAttr.name + ")";
                attrs.push_back(output);
            }
        }
    }
}

Aggregate::~Aggregate() {
    /* clean up */
    for (auto & itr : intKeyTable){
        delete[] itr.second;
    }
    for (auto & itr : realKeyTable){
        delete[] itr.second;
    }
    for (auto & itr : varCharKeyTable){
        delete[] itr.second;
    }
    for (auto & itr : intKeySumTable){
        delete[] itr.second;
    }
    for (auto & itr : realKeySumTable){
        delete[] itr.second;
    }
    for (auto & itr : varCharKeySumTable){
        delete[] itr.second;
    }
}

/////////// END OF Aggregate Implementation ////////////

////////////// qe_helpers /////////////

void qe_helpers::SplitAttrName(std::string InputAttrName, std::string delimiter, std::string &TableName,
                               std::string &AttributeName) {

    size_t sizeOfAttr = InputAttrName.size();
    size_t delimiterPosition = InputAttrName.find(delimiter);
    TableName = InputAttrName.substr(0, delimiterPosition);
    AttributeName = InputAttrName.substr(delimiterPosition + 1, sizeOfAttr - delimiterPosition);

}

RC qe_helpers::concatTuples(const byte *lhs, const std::vector<Attribute> leftAttrs, const byte *rhs,
                            const std::vector<Attribute> rightAttrs, byte *resultTuple) {
    /* Concat null-indicator bytes */
    /* Calculate the actual null indicator size */
    const int lhsActualNullsSize = rbfm_helpers::getActualByteForNullsIndicator(leftAttrs.size());
    const int rhsActualNullsSize = rbfm_helpers::getActualByteForNullsIndicator(rightAttrs.size());
    const int joinedAttrsSize = leftAttrs.size() + rightAttrs.size();
    const int joinedActualNullsSize = rbfm_helpers::getActualByteForNullsIndicator(joinedAttrsSize);

    /* Fill in a vector of bools (bitset) */
    std::vector<bool> concatNulls;
    int nullBitIter = 0;
    const byte MASK = 128;
    for (int i = 0; i < leftAttrs.size(); ++i) {
        if ((i != 0) && (i % 8 == 0)) { nullBitIter++; }  // Advance null byte iterator if 8 bits are read

        const byte i_result = MASK & (byte) (lhs[nullBitIter] << (unsigned) (i % 8));

        if (i_result == MASK) {
            concatNulls.push_back(true);
        } else if (i_result == 0) {
            concatNulls.push_back(false);
        } else {
            std::cerr << "Somethings wrong!" << std::endl;
        }
    }

    nullBitIter = 0;
    for (int i = 0; i < rightAttrs.size(); ++i) {
        if ((i != 0) && (i % 8 == 0)) { nullBitIter++; }  // Advance null byte iterator if 8 bits are read

        const byte i_result = MASK & (byte) (rhs[nullBitIter] << (unsigned) (i % 8));

        if (i_result == MASK) {
            concatNulls.push_back(true);
        } else if (i_result == 0) {
            concatNulls.push_back(false);
        } else {
            std::cerr << "Somethings wrong!" << std::endl;
        }
    }

    /* Fill in the resulting null indicator bytes for the joined tuple */
    memset(resultTuple, 0, joinedActualNullsSize);     // Set to all zeroes for null-byte indicator
    nullBitIter = 0;
    for (int j = 0; j < joinedAttrsSize; ++j) {
        if ((j != 0) && (j % 8 == 0)) { nullBitIter++; }  // Advance null byte iterator if 8 bits are read
        const byte i_result = concatNulls[j] ? MASK : 0;
        resultTuple[nullBitIter] = resultTuple[nullBitIter] | (byte) (i_result >> ((unsigned) j % 8));
    }

    /* Fill in the joined tuples */
    unsigned writeOffset = joinedActualNullsSize;
    unsigned readOffset = lhsActualNullsSize;
    for (const auto &leftAttr : leftAttrs) {
        if (leftAttr.type == TypeVarChar) {
            int len = 0;
            memcpy(&len, lhs + readOffset, 4);
            readOffset += 4;

            memcpy(resultTuple + writeOffset, &len, 4);
            writeOffset += 4;
            memcpy(resultTuple + writeOffset, lhs + readOffset, len);
            writeOffset += len;
            readOffset += len;
        } else {
            memcpy(resultTuple + writeOffset, lhs + readOffset, 4);
            writeOffset += 4;
            readOffset += 4;
        }
    }

    readOffset = rhsActualNullsSize;
    for (const auto &rightAttr : rightAttrs) {
        if (rightAttr.type == TypeVarChar) {
            int len = 0;
            memcpy(&len, rhs + readOffset, 4);
            readOffset += 4;

            memcpy(resultTuple + writeOffset, &len, 4);
            writeOffset += 4;
            memcpy(resultTuple + writeOffset, rhs + readOffset, len);
            writeOffset += len;
            readOffset += len;
        } else {
            memcpy(resultTuple + writeOffset, rhs + readOffset, 4);
            writeOffset += 4;
            readOffset += 4;
        }
    }

    return QE_SUCCESS;
}

RC qe_helpers::setVariables(const Condition &condition, std::string &lhsTableName, std::string &lhsAttrName,
                            Attribute &lhsAttr, std::string &rhsTableName, std::string &rhsAttrName,
                            Attribute &rhsAttr, Value &rhsValue, std::string delimiter) {

    qe_helpers::SplitAttrName(condition.lhsAttr, delimiter, lhsTableName, lhsAttrName);
    RC rc = rm_helpers::FindAttributeInVector(lhsTableName, lhsAttrName, lhsAttr);
    if (rc != 0) return 1;

    if (condition.bRhsIsAttr) {
        qe_helpers::SplitAttrName(condition.rhsAttr, delimiter, rhsTableName, rhsAttrName);
        RC rc = rm_helpers::FindAttributeInVector(rhsTableName, rhsAttrName, rhsAttr);
        if (rc != 0) return 1;
    } else {
        rhsValue = condition.rhsValue;
    }

    return 0;
}

RC qe_helpers::satisfyCondition(Iterator *left, const Condition &condition, void *data) {

    byte lhsKey[PAGE_SIZE] = {0};
    byte rhsKey[PAGE_SIZE] = {0};
    void *lhsComparePtr = nullptr;
    void *rhsComparePtr = nullptr;

    std::vector<Attribute> lhsAttrs;
    left->getAttributes(lhsAttrs);
    
    /* Set left-hand side key value */

    RC rc = rm_helpers::ExtractKeyValueFromData(lhsAttrs, condition.lhsAttr, data, lhsKey);

    if (rc != 0) return 1;
    lhsComparePtr = lhsKey;

    /* Set right-hand side key value */
    if (condition.bRhsIsAttr) { //rhs is attribute

        rc = rm_helpers::ExtractKeyValueFromData(lhsAttrs, condition.rhsAttr, data, rhsKey);
        if (rc != 0) return 1;
        rhsComparePtr = rhsKey;

    } else { //rhs is a value

        rhsComparePtr = condition.rhsValue.data;

    }

    /* Compare left-hand side key value with right-hand side key value */
    rc = qe_helpers::compareValue(condition, lhsComparePtr, rhsComparePtr);

    return rc;

}


RC qe_helpers::compareValue(const Condition &condition, void *lhsKey, void *rhsKey) {

    std::string lhsTableName, lhsAttributeName, rhsTableName, rhsAttributeName;
    Attribute lhsAttr, rhsAttr;
    Value rhsValue;

    RC rc = qe_helpers::setVariables(condition, lhsTableName,
            lhsAttributeName, lhsAttr, rhsTableName, rhsAttributeName,
                                     rhsAttr, rhsValue, ".");
    if (rc != 0) return 1;

    AttrType lhsType = lhsAttr.type;
    AttrType rhsType;
    if (condition.bRhsIsAttr) {
        rhsType = rhsAttr.type;
    } else {
        rhsType = rhsValue.type;
    }


    CompOp op = condition.op;

    if (lhsType == rhsType) { // compare only if the types of each key value are same
        switch (lhsType) {
            case TypeInt: {
                int lhsKeyValue = *(int *) lhsKey;
                int rhsKeyValue = *(int *) rhsKey;
                switch (op) {
                    case EQ_OP: {
                        if (lhsKeyValue == rhsKeyValue)
                            return 0;
                        else
                            return 1;
                    }
                    case LT_OP: {
                        if (lhsKeyValue < rhsKeyValue)
                            return 0;
                        else
                            return 1;
                    }
                    case LE_OP: {
                        if (lhsKeyValue <= rhsKeyValue)
                            return 0;
                        else
                            return 1;
                    }
                    case GT_OP: {
                        if (lhsKeyValue > rhsKeyValue)
                            return 0;
                        else
                            return 1;
                    }
                    case GE_OP: {
                        if (lhsKeyValue >= rhsKeyValue)
                            return 0;
                        else
                            return 1;
                    }
                    case NE_OP: {
                        if (lhsKeyValue != rhsKeyValue)
                            return 0;
                        else
                            return 1;
                    }
                    case NO_OP: {
                        return 0;
                    }
                } //end switch
            } // end TypeInt
            case TypeReal: {
                float lhsKeyValue = *(float *) lhsKey;
                float rhsKeyValue = *(float *) rhsKey;
                switch (op) {
                    case EQ_OP: {
                        if (lhsKeyValue == rhsKeyValue)
                            return 0;
                        else
                            return 1;
                    }
                    case LT_OP: {
                        if (lhsKeyValue < rhsKeyValue)
                            return 0;
                        else
                            return 1;
                    }
                    case LE_OP: {
                        if (lhsKeyValue <= rhsKeyValue)
                            return 0;
                        else
                            return 1;
                    }
                    case GT_OP: {
                        if (lhsKeyValue > rhsKeyValue)
                            return 0;
                        else
                            return 1;
                    }
                    case GE_OP: {
                        if (lhsKeyValue >= rhsKeyValue)
                            return 0;
                        else
                            return 1;
                    }
                    case NE_OP: {
                        if (lhsKeyValue != rhsKeyValue)
                            return 0;
                        else
                            return 1;
                    }
                    case NO_OP: {
                        return 0;
                    }
                } //end switch
            } // end TypeReal
            case TypeVarChar: {
                int sizeOfLhsKey = -1, sizeOfRhsKey = -1;
                memcpy(&sizeOfLhsKey, lhsKey, 4);
                memcpy(&sizeOfRhsKey, rhsKey, 4);
                char lhsKeyValue[sizeOfLhsKey + 1];
                char rhsKeyValue[sizeOfRhsKey + 1];
                memcpy(lhsKeyValue, (byte *) lhsKey + 4, sizeOfLhsKey);
                lhsKeyValue[sizeOfLhsKey] = '\0';
                memcpy(rhsKeyValue, (byte *) rhsKey + 4, sizeOfRhsKey);
                rhsKeyValue[sizeOfRhsKey] = '\0';
                int strcmpResult = std::strcmp(lhsKeyValue, rhsKeyValue);
                switch (op) {
                    case EQ_OP: {
                        if (strcmpResult == 0)
                            return 0;
                        else
                            return 1;
                    }
                    case LT_OP: {
                        if (strcmpResult < 0)
                            return 0;
                        else
                            return 1;
                    }
                    case LE_OP: {
                        if (strcmpResult <= 0)
                            return 0;
                        else
                            return 1;
                    }
                    case GT_OP: {
                        if (strcmpResult > 0)
                            return 0;
                        else
                            return 1;
                    }
                    case GE_OP: {
                        if (strcmpResult >= 0)
                            return 0;
                        else
                            return 1;
                    }
                    case NE_OP: {
                        if (strcmpResult != 0)
                            return 0;
                        else
                            return 1;
                    }
                    case NO_OP: {
                        return 0;
                    }
                } // end switch
            } // end TypeVarChar
        } // end outer switch
    } else { // cannot compare them if they have different types
        std::cerr << "Error: comparing values have different types" << std::endl;
        return 1;
    }
}

RC qe_helpers::compareValue(const CompOp &op, const AttrType &type, void *lhsKey, void *rhsKey) {
    switch (type) {
        case TypeInt: {
            int lhsKeyValue = *(int *) lhsKey;
            int rhsKeyValue = *(int *) rhsKey;
            switch (op) {
                case EQ_OP: {
                    if (lhsKeyValue == rhsKeyValue)
                        return 0;
                    else
                        return 1;
                }
                case LT_OP: {
                    if (lhsKeyValue < rhsKeyValue)
                        return 0;
                    else
                        return 1;
                }
                case LE_OP: {
                    if (lhsKeyValue <= rhsKeyValue)
                        return 0;
                    else
                        return 1;
                }
                case GT_OP: {
                    if (lhsKeyValue > rhsKeyValue)
                        return 0;
                    else
                        return 1;
                }
                case GE_OP: {
                    if (lhsKeyValue >= rhsKeyValue)
                        return 0;
                    else
                        return 1;
                }
                case NE_OP: {
                    if (lhsKeyValue != rhsKeyValue)
                        return 0;
                    else
                        return 1;
                }
                case NO_OP: {
                    return 0;
                }
            } //end switch
        } // end TypeInt
        case TypeReal: {
            float lhsKeyValue = *(float *) lhsKey;
            float rhsKeyValue = *(float *) rhsKey;
            switch (op) {
                case EQ_OP: {
                    if (lhsKeyValue == rhsKeyValue)
                        return 0;
                    else
                        return 1;
                }
                case LT_OP: {
                    if (lhsKeyValue < rhsKeyValue)
                        return 0;
                    else
                        return 1;
                }
                case LE_OP: {
                    if (lhsKeyValue <= rhsKeyValue)
                        return 0;
                    else
                        return 1;
                }
                case GT_OP: {
                    if (lhsKeyValue > rhsKeyValue)
                        return 0;
                    else
                        return 1;
                }
                case GE_OP: {
                    if (lhsKeyValue >= rhsKeyValue)
                        return 0;
                    else
                        return 1;
                }
                case NE_OP: {
                    if (lhsKeyValue != rhsKeyValue)
                        return 0;
                    else
                        return 1;
                }
                case NO_OP: {
                    return 0;
                }
            } //end switch
        } // end TypeReal
        case TypeVarChar: {
            int sizeOfLhsKey = -1, sizeOfRhsKey = -1;
            memcpy(&sizeOfLhsKey, lhsKey, 4);
            memcpy(&sizeOfRhsKey, rhsKey, 4);
            char lhsKeyValue[sizeOfLhsKey + 1];
            char rhsKeyValue[sizeOfRhsKey + 1];
            memcpy(lhsKeyValue, (byte *) lhsKey + 4, sizeOfLhsKey);
            lhsKeyValue[sizeOfLhsKey] = '\0';
            memcpy(rhsKeyValue, (byte *) rhsKey + 4, sizeOfRhsKey);
            rhsKeyValue[sizeOfRhsKey] = '\0';
            int strcmpResult = std::strcmp(lhsKeyValue, rhsKeyValue);
            switch (op) {
                case EQ_OP: {
                    if (strcmpResult == 0)
                        return 0;
                    else
                        return 1;
                }
                case LT_OP: {
                    if (strcmpResult < 0)
                        return 0;
                    else
                        return 1;
                }
                case LE_OP: {
                    if (strcmpResult <= 0)
                        return 0;
                    else
                        return 1;
                }
                case GT_OP: {
                    if (strcmpResult > 0)
                        return 0;
                    else
                        return 1;
                }
                case GE_OP: {
                    if (strcmpResult >= 0)
                        return 0;
                    else
                        return 1;
                }
                case NE_OP: {
                    if (strcmpResult != 0)
                        return 0;
                    else
                        return 1;
                }
                case NO_OP: {
                    return 0;
                }
            } // end switch
        } // end TypeVarChar
    } // end outer switch
}

unsigned qe_helpers::computeSizeOfTuple(std::vector<Attribute> attrs, void *data) {

    unsigned result = 0;

    int numberOfAttributes = attrs.size();
    int nullIndicatorSize = rbfm_helpers::getActualByteForNullsIndicator(numberOfAttributes);
    byte nullBitIndicator[nullIndicatorSize];
    memcpy(nullBitIndicator, data, nullIndicatorSize);
    result += nullIndicatorSize;

    int quotient = numberOfAttributes / nullIndicatorSize;
    int hit = 0;

    for (int i = 0; i < quotient; i++) {
        for (int j = 0; j < 8; j++) {
            int now = i * 8 + j;
            if (now >= numberOfAttributes) {
                hit = 1;
                break;
            }

            bool nullBit = nullBitIndicator[i] & ((unsigned) 1 << (unsigned) (7 - j));
            if (!nullBit) {
                if (attrs[now].type == TypeInt || attrs[now].type == TypeReal) {
                    result += 4;
                } else if (attrs[now].type == TypeVarChar) {
                    int len = 0;
                    memcpy(&len, (byte *) data + result, 4);
                    result += 4 + len;
                }
            } else {
                continue;
            }
        }
        if (hit == 1) break;
    }

    return result;

}

bool qe_helpers::isAttrValueNull(std::vector<Attribute> attrs, std::string attrName, byte *data) {
    /* Only memcpy the relevant values specified by attribute */
    int attrPosition = -1;

    /* Get the positions of the attributeNames */
    for (int i = 0; i < attrs.size(); ++i) {
        if (attrName == attrs.at(i).name) {
            attrPosition = i;
        }
    }

    int outerIter = (attrPosition == 0) ? 0 : attrPosition / 8;
    int innerIter = attrPosition % 8;

    const byte MASK = 128;            // Bit mask
    const byte i_result = MASK & (byte) (data[outerIter] << (unsigned)innerIter);

    return i_result != 0;
}
