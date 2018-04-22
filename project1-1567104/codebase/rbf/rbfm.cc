#include <cmath>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <string>
#include <cstdio>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <string>
#include <cmath>

#include "rbfm.h"

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;
PagedFileManager *RecordBasedFileManager::_pf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
    _pf_manager = PagedFileManager::instance();
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

/**
 * Create a file, append a new page, then fill it with SlotDirectoryHeader from memory
 *
 * @param fileName
 * @return
 */
RC RecordBasedFileManager::createFile(const string &fileName) {
    try {
        if (_pf_manager->createFile(fileName) != 0) {
            return -1;
        }
        //void* calloc (size_t num, size_t size);
        //Allocates a block of memory for an array of num elements,
        //each of them size bytes long, and initializes all its bits to zero. (here is a 4096 items array, each is 1 byte)
        //return: a pointer to the memory block allocated by the function
        void *allocatedPagedData = calloc(PAGE_SIZE, 1);
        if(allocatedPagedData == NULL){
            cout << "No space is allocated!" << endl;
            return -1;
        }
        //Use this allocatedPagedData to store the slot header
        //allocatedPagedData: a pointer to the memory block allocated by the calloc function
        initializeSlotDirectoryHeader(allocatedPagedData);
        FileHandle fileHandle;
        if (_pf_manager->openFile(fileName.c_str(), fileHandle) != 0) {
            //fileHandle object will get fileName as its currentFileHandled
            return -1;
        }
        //memory(which stores SlotDirectoryHeader) -> write to new appended page (we only have memory, don't have a page before this)
        if (fileHandle.appendPage(allocatedPagedData) != 0) {
            return -1;
        }
        if (_pf_manager->closeFile(fileHandle) != 0) {
            return -1;
        }
        //void free (void* ptr);
        //Deallocate memory block
        free(allocatedPagedData);
        return 0;
    } catch (const std::exception &e) {
        std::cout << e.what();
        return -1;
    } catch (const std::runtime_error &e) {
        std::cout << e.what();
        return -1;
    }
}

/**
 * Similar to _pf_manager, use directly
 *
 * @param fileName
 * @return
 */
RC RecordBasedFileManager::destroyFile(const string &fileName) {
    return _pf_manager->destroyFile(fileName);
}

/**
 * Similar to _pf_manager, use directly
 *
 * @param fileName
 * @param fileHandle
 * @return
 */
RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    return _pf_manager->openFile(fileName, fileHandle);
}

/**
 * Similar to _pf_manager, use directly
 *
 * @param fileHandle
 * @return
 */
RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return _pf_manager->closeFile(fileHandle);
}

/**
 * Insert a record into a file
 *
 * @param fileHandle
 * @param recordDescriptor
 * @param data
 * @param rid
 * @return
 */
RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    try{
        //Calculate record size
        void *allocatedPageData = malloc(PAGE_SIZE);
        if (allocatedPageData == NULL) {
            cout << "No space is allocated!" << endl;
            return -1;
        }
        bool pageFound = false;
        unsigned currentRecordSize = getRecordSize(recordDescriptor, data);
        unsigned numberOfPagesInFile = fileHandle.getNumberOfPages();
        unsigned pageNum;
        //From the beginning, try to find the page for insertion
        for (pageNum = 0; pageNum < numberOfPagesInFile; pageNum++)
        {
            //allocatedPageData is 4KB size big

            //readPage(PageNum pageNum, void *data)
            //Read ith page into the allocatedPageData
            if (fileHandle.readPage(pageNum, allocatedPageData)) {
                return -1;
            }
            //Check if the free space is still enough to insert the current record
            if (getFreeSpaceSizeOnPage(allocatedPageData) >= sizeof(SlotDirectoryEntry) + currentRecordSize) {
                pageFound = true;
                break;
            }
        }
        if(!pageFound) {
            initializeSlotDirectoryHeader(allocatedPageData);
        }
        SlotDirectoryHeader slotHeader;
        memcpy (&slotHeader, allocatedPageData, sizeof(SlotDirectoryHeader));
        rid.pageNum = pageNum;
        rid.slotNum = slotHeader.numberOfSlots;
        SlotDirectoryEntry newRecordEntry;
        newRecordEntry.length = currentRecordSize;
        newRecordEntry.offset = slotHeader.offsetToStartOfFreespace - currentRecordSize;
        //Add a entry in the slot table
        memcpy (((char*) allocatedPageData + sizeof(SlotDirectoryHeader) + rid.slotNum * sizeof(SlotDirectoryEntry) ), &newRecordEntry, sizeof(SlotDirectoryEntry));
        slotHeader.offsetToStartOfFreespace = newRecordEntry.offset;
        slotHeader.numberOfSlots += 1;
        //Update the slot table header
        memcpy (allocatedPageData, &slotHeader, sizeof(SlotDirectoryHeader));

        // Actually add record data to allocatedPageData
        setDataAtOffset (allocatedPageData, newRecordEntry.offset, recordDescriptor, data);

        if (pageFound) {
            //writePage(PageNum pageNum, const void *data)
            if (fileHandle.writePage(pageNum, allocatedPageData)) {
                return -1;
            }
        } else { //No space, then append a new page then add to it
            if (fileHandle.appendPage(allocatedPageData)) {
                return -1;
            }
        }
        //Release memory
        free(allocatedPageData);
        return 0;
    } catch (const std::exception &e) {
        std::cout << e.what();
        return -1;
    } catch (const std::runtime_error &e) {
        std::cout << e.what();
        return -1;
    }
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    try {
        void *pagedData = malloc(PAGE_SIZE);
        //Read all data in that page into pagedData
        if (fileHandle.readPage(rid.pageNum, pagedData) != 0) {
            cout << "Illegal page Num in RID!" << endl;
            return -1;
        }
        SlotDirectoryHeader slotDirectoryHeader;
        memcpy (&slotDirectoryHeader, pagedData, sizeof(SlotDirectoryHeader));
        if(slotDirectoryHeader.numberOfSlots < rid.slotNum) {
            cout << "Illegal slot Num in RID!" << endl;
            return -1;
        }
        SlotDirectoryEntry slotDirectoryEntry;
        //Get the target slot entry, put into slotDirectoryEntry
        memcpy  (&slotDirectoryEntry, ((char*) pagedData + sizeof(SlotDirectoryHeader) + rid.slotNum * sizeof(SlotDirectoryEntry)), sizeof(SlotDirectoryEntry));


        getDataAtOffset(slotDirectoryEntry.offset, pagedData, recordDescriptor, data);

        free(pagedData);
        return 0;
    } catch (const std::exception &e) {
        std::cout << e.what();
        return -1;
    } catch (const std::runtime_error &e) {
        std::cout << e.what();
        return -1;
    }
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    try{
        int nullIndicatorSize = int(ceil((double) recordDescriptor.size() / 8));
        char nullIndicator[nullIndicatorSize];
        memset(nullIndicator, 0, nullIndicatorSize);
        memcpy(nullIndicator, data, nullIndicatorSize);
        unsigned offset = nullIndicatorSize;
        for (unsigned i = 0; i < (unsigned) recordDescriptor.size(); i++)
        {
            //Sets the field width to be used on output operations
            cout << setw(10) << left << recordDescriptor[i].name << ": ";
            bool isNull = isFieldNull(nullIndicator, i);
            if (isNull) {
                cout << "NULL" << endl;
                continue;
            }
            switch (recordDescriptor[i].type)
            {
                case TypeInt:
                    uint32_t data_integer;
                    memcpy(&data_integer, ((char*) data + offset), INT_SIZE);
                    offset += INT_SIZE;
                    cout << "" << data_integer << endl;
                    break;
                case TypeReal:
                    float data_real;
                    memcpy(&data_real, ((char*) data + offset), REAL_SIZE);
                    offset += REAL_SIZE;
                    cout << "" << data_real << endl;
                    break;
                case TypeVarChar:
                    uint32_t varcharSize;
                    memcpy(&varcharSize, ((char*) data + offset), VARCHAR_SIZE);
                    offset += VARCHAR_SIZE;
                    char *data_string = (char*) malloc(varcharSize + 1);
                    if (data_string == NULL) {
                        return -1;
                    }
                    memcpy(data_string, ((char*) data + offset), varcharSize);
                    data_string[varcharSize] = '\0';  //Last one, make it string
                    offset += varcharSize;
                    cout << data_string << endl;
                    free(data_string);
                    break;
            }
        }
        return 0;
    } catch (const std::exception &e) {
        std::cout << e.what();
        return -1;
    } catch (const std::runtime_error &e) {
        std::cout << e.what();
        return -1;
    }
}


void RecordBasedFileManager::initializeSlotDirectoryHeader(void *page)
{
    //void * memset ( void * ptr, int value, size_t num );
    //Fill block of memory
    //Sets the first num bytes of the block of memory pointed by ptr, to the specified value
    memset(page, 0, PAGE_SIZE);
    SlotDirectoryHeader slotHeader;
    //Initially, offsetToStartOfFreespace is 0, because there grow toward each other
    //See PPT page 18
    slotHeader.offsetToStartOfFreespace = PAGE_SIZE;
    slotHeader.numberOfSlots = 0;
    //void * memcpy ( void * destination, const void * source, size_t num );
    //Copy block of memory
    //Copies the values of num bytes, from the location pointed to by source, directly to the memory block pointed to by destination.
    memcpy (page, &slotHeader, sizeof(SlotDirectoryHeader));
}

void RecordBasedFileManager::getDataAtOffset(unsigned offset, void *page, const vector<Attribute> &recordDescriptor, void *data)
{
    char *start = (char*) page + offset;
    int nullIndicatorSize = int(ceil((double) recordDescriptor.size() / 8));
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    NumberOfColumnsSize numberOfColumnsSize = 0;
    memcpy (&numberOfColumnsSize, start, sizeof(NumberOfColumnsSize));
    int recordNullIndicatorSize = int(ceil((double) numberOfColumnsSize / 8));
    memcpy (nullIndicator, start + sizeof(NumberOfColumnsSize), recordNullIndicatorSize);
    for (unsigned i = numberOfColumnsSize; i < recordDescriptor.size(); i++)
    {
        int indicatorIndex = (i+1) / 8;
        int indicatorMask  = 1 << (8 - 1 - (i % 8));
        nullIndicator[indicatorIndex] = nullIndicator[indicatorIndex] | indicatorMask;
    }
    memcpy(data, nullIndicator, nullIndicatorSize);
    unsigned record_offset = sizeof(NumberOfColumnsSize) + recordNullIndicatorSize + numberOfColumnsSize * sizeof(EachColumnLengthSize);
    unsigned data_offset = nullIndicatorSize;
    char *directory_base = start + sizeof(NumberOfColumnsSize) + recordNullIndicatorSize;  //PPT P12
    for (unsigned i = 0; i < recordDescriptor.size(); i++)
    {
        if (isFieldNull(nullIndicator, i)) {
            continue;
        }
        EachColumnLengthSize endPointer;
        memcpy(&endPointer, directory_base + i * sizeof(EachColumnLengthSize), sizeof(EachColumnLengthSize));
        uint32_t fieldSize = endPointer - record_offset;
        if (recordDescriptor[i].type == TypeVarChar) {
            memcpy((char*) data + data_offset, &fieldSize, VARCHAR_SIZE);
            data_offset = data_offset + VARCHAR_SIZE;
        }
        memcpy((char*) data + data_offset, start + record_offset, fieldSize);
        record_offset += fieldSize;
        data_offset += fieldSize;
    }
}

bool RecordBasedFileManager::isFieldNull(char *nullIndicator, int i)
{
    int indicatorIndex = i / 8;
    int indicatorMask  = 1 << (8 - 1 - (i % 8));  //1 shifted to left by (8 - 1 - (i % 8)) positions
    return (nullIndicator[indicatorIndex] & indicatorMask) != 0;
}

/**
 * Get size of input *data
 * Return in bytes
 *
 * @param recordDescriptor
 * @param data
 * @return
 */
unsigned RecordBasedFileManager::getRecordSize(const vector<Attribute> &recordDescriptor, const void *data)
{
    //Get how many bytes for null indicator, for example 1 for 7 attributes, 2 for 11 attributes
    int nullIndicatorSize = int(ceil((double) recordDescriptor.size() / 8)); //recordDescriptor.size() == #Attribute
    char nullIndicator[nullIndicatorSize]; //Initial a char array, all items is ''
    //Set first nullIndicatorSize bytes to 0, of the memory block pointed to by nullIndicator (ready for copy)
    memset(nullIndicator, 0, nullIndicatorSize);
    //Copy nullIndicatorSize bytes of data, to the memory block pointed to by nullIndicator (copy "real null-indicator data" to that space)
    memcpy(nullIndicator, (char*) data, nullIndicatorSize);
    unsigned offset = nullIndicatorSize; //for example 1 for 7 Attributes
    //Size of the first part (before values)
    //Strictly follow the PPT P12 design!
    /**
     * First Part
     */
    unsigned size = sizeof (NumberOfColumnsSize) + nullIndicatorSize + recordDescriptor.size() * sizeof(EachColumnLengthSize);
    /**
     * Second Part
     */
    for (unsigned i = 0; i < (unsigned) recordDescriptor.size(); i++)
    {
        if (isFieldNull(nullIndicator, i)) {
            continue;
        }
        switch (recordDescriptor[i].type)
        {
            case TypeInt:
                size += INT_SIZE; //4(bytes)
                offset += INT_SIZE;
                break;
            case TypeReal:
                size += REAL_SIZE; //4(bytes)
                offset += REAL_SIZE;
                break;
            case TypeVarChar:
                uint32_t varcharSize; //Used to store the "actual length" of varchar value
                //void * memcpy ( void * destination, const void * source, size_t num );
                //varcharSize get the "actual length" of varchar value
                memcpy(&varcharSize, (char*) data + offset, VARCHAR_SIZE);
                size += varcharSize; //It is the "Second Part", so only append value
                offset += varcharSize + VARCHAR_SIZE;
                break;
        }
    }
    return size;  //Return size
}

unsigned RecordBasedFileManager::getFreeSpaceSizeOnPage(void *page)
{
    SlotDirectoryHeader slotHeader;
    //void * memcpy ( void * destination, const void * source, size_t num );
    //Get the "slot header" (which is already on that page long time ago)
    memcpy (&slotHeader, page, sizeof(SlotDirectoryHeader));
    //Get the free space (PPT P18)
    return slotHeader.offsetToStartOfFreespace - (slotHeader.numberOfSlots * sizeof(SlotDirectoryEntry) + sizeof(SlotDirectoryHeader));
}

/**
 * Actually add data to page
 *
 * @param page
 * @param offset
 * @param recordDescriptor
 * @param data
 */
void RecordBasedFileManager::setDataAtOffset(void *page, unsigned offset, const vector<Attribute> &recordDescriptor, const void *data)
{
    int nullIndicatorSize = int(ceil((double) recordDescriptor.size() / 8));
    char nullIndicator[nullIndicatorSize];
    memset (nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char*) data, nullIndicatorSize);
    char *start = (char*) page + offset;
    unsigned data_offset = nullIndicatorSize;
    unsigned header_offset = 0;
    NumberOfColumnsSize numberOfColumnsSize = recordDescriptor.size();
    memcpy(start + header_offset, &numberOfColumnsSize, sizeof(numberOfColumnsSize));
    header_offset += sizeof(numberOfColumnsSize);
    memcpy(start + header_offset, nullIndicator, nullIndicatorSize);
    header_offset += nullIndicatorSize;
    EachColumnLengthSize record_offset = header_offset + (recordDescriptor.size()) * sizeof(EachColumnLengthSize);
    unsigned i = 0;
    for (i = 0; i < recordDescriptor.size(); i++)
    {
        if (!isFieldNull(nullIndicator, i))
        {
            char *data_start = (char*) data + data_offset;
            switch (recordDescriptor[i].type)
            {
                case TypeInt:
                    memcpy (start + record_offset, data_start, INT_SIZE);
                    record_offset += INT_SIZE; //Keep updating offset
                    data_offset += INT_SIZE;
                    break;
                case TypeReal:
                    memcpy (start + record_offset, data_start, REAL_SIZE);
                    record_offset += REAL_SIZE; //Keep updating offset
                    data_offset += REAL_SIZE;
                    break;
                case TypeVarChar:
                    unsigned varcharSize;
                    memcpy(&varcharSize, data_start, VARCHAR_SIZE);
                    memcpy(start + record_offset, data_start + VARCHAR_SIZE, varcharSize);
                    record_offset += varcharSize; //Keep updating offset
                    data_offset += VARCHAR_SIZE + varcharSize;
                    break;
            }
        }
        memcpy(start + header_offset, &record_offset, sizeof(EachColumnLengthSize));
        header_offset += sizeof(EachColumnLengthSize);  //PPT P12
    }
}

