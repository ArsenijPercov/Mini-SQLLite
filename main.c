#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>


#define USERNAME_FIELD_LENGTH 32
#define EMAIL_FIELD_LENGTH 255
#define TABLE_MAX_PAGES 100

const uint32_t ROW_ID_SIZE_BYTE = 4;
const uint32_t ROW_EMAIL_SIZE_BYTE = 255;
const uint32_t ROW_USER_SIZE_BYTE = 32;
const uint32_t ROW_SIZE = ROW_ID_SIZE_BYTE + ROW_EMAIL_SIZE_BYTE + ROW_USER_SIZE_BYTE;
const uint32_t PAGE_SIZE = 4096;

const uint32_t ID_OFFSET = 0;
const uint32_t USER_OFFSET = ID_OFFSET + ROW_ID_SIZE_BYTE;
const uint32_t EMAIL_OFFSET = USER_OFFSET + ROW_USER_SIZE_BYTE;

const uint32_t ROWS_PER_PAGES = PAGE_SIZE/ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = TABLE_MAX_PAGES * ROWS_PER_PAGES;

// COMMON NODE HEADER LAYOUT
const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_NODE_SIZE = sizeof(uint8_t);
const uint32_t IS_ROOT_NODE_OFFSET = NODE_TYPE_OFFSET + NODE_TYPE_SIZE;
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t PARENT_POINTER_OFFSET = IS_ROOT_NODE_OFFSET + IS_ROOT_NODE_SIZE;
const uint32_t COMMON_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_NODE_SIZE + PARENT_POINTER_SIZE;

// LEAF NODE HEADER
const uint32_t NUM_OF_CELLS_SIZE = sizeof(uint32_t);
const uint32_t NUM_OF_CELLS_OFFSET = PARENT_POINTER_OFFSET + PARENT_POINTER_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_HEADER_SIZE + NUM_OF_CELLS_SIZE;

// LEAF NODE BODY LAYOUT
const uint32_t KEY_SIZE = sizeof(uint32_t);
const uint32_t CELLS_OFFSET = LEAF_NODE_HEADER_SIZE;
const uint32_t VALUE_SIZE = ROW_SIZE;
const uint32_t KEY_CELL_OFFSET = 0;
const uint32_t VALUE_CELL_OFFSET = KEY_SIZE;
const uint32_t CELL_SIZE = KEY_SIZE + VALUE_SIZE;
const uint32_t SPACE_FOR_CELLS_SIZE = (PAGE_SIZE - LEAF_NODE_HEADER_SIZE);
const uint32_t CELL_PER_PAGE = SPACE_FOR_CELLS_SIZE / CELL_SIZE;

typedef enum{
    INTERNAL_NODE,
    LEAF_NODE
} NodeType;

typedef enum{
    META_COMMAND_SUCCESS,
    META_UNKNOWN_COMMAND
} MetaCommandResult;

typedef enum { STATEMENT_INSERT, STATEMENT_SELECT } StatementType;

typedef enum {
    PARSING_SUCCESS,
    PARSING_TOO_MANY_ARGUMENTS,
    PARSING_ERROR,
    PARSING_TOO_LONG,
    PARSING_NEGATIVE_ID
} ParsingResult;

typedef enum{
    PREPARE_SUCCESS,
    PREPARE_UNKOWN,
    PREPARE_PARSING_TOO_LONG,
    PREPARE_PARSING_TOO_MANY_ARGUMENTS,
    PREPARE_PARSING_FAILURE,
    PREPARE_NEGATIVE_ID
} PrepareStatementResult;

typedef enum{
    EXECUTE_SUCCESS,
    EXECUTE_TABLE_FULL
} ExecuteResult;

typedef struct{
    int id;
    char username[USERNAME_FIELD_LENGTH+1];
    char email[EMAIL_FIELD_LENGTH+1];
} Arguments;

typedef struct{
    int fd;
    uint32_t filesize;
    uint32_t num_of_pages;
    void* pages[TABLE_MAX_PAGES];
} Pager;

typedef struct{
    uint32_t root_page_num;
} BTree;

typedef struct{
    Pager* pager;    
    BTree* btree;
} Table;

typedef struct{
    Table* table;
    uint32_t page_num;
    uint32_t cell_num;
    bool end_of_table;
} Cursor;

typedef struct {
    StatementType type;
Arguments* arguments; // only used by "insert" statements
} Statement;

typedef struct{
    char* buffer;
    size_t buffer_length;
    ssize_t input_length;
} InputBuffer;


NodeType* get_node_type(void* node){
    return node + NODE_TYPE_OFFSET;
}

uint8_t* get_is_root(void* node){
    return node + IS_ROOT_NODE_OFFSET;
}

uint32_t* get_parent_node_pointer(void* node){
    return node + PARENT_POINTER_OFFSET;
}

uint32_t* get_leaf_num_of_cells(void* node){
    return node + NUM_OF_CELLS_OFFSET;
}

void* get_leaf_cell(void* node, int cell_num){
    return node + CELLS_OFFSET + cell_num*(CELL_SIZE);
}

uint32_t* get_leaf_key(void* node, int cell_num){
    return get_leaf_cell(node,cell_num) + KEY_CELL_OFFSET;
}

void* get_leaf_value(void* node, int cell_num){
    return get_leaf_cell(node,cell_num) + VALUE_CELL_OFFSET;
}

void initialise_leaf_node(void* node){
    (*get_leaf_num_of_cells(node)) = 0;
}

void serialise_row(Arguments* src, void* dest){
    memcpy(dest+ID_OFFSET, &(src->id), ROW_ID_SIZE_BYTE);
    memcpy(dest+USER_OFFSET, src->username, ROW_USER_SIZE_BYTE); 
    memcpy(dest+EMAIL_OFFSET, src->email, ROW_EMAIL_SIZE_BYTE);
}

void deserialise_row(Arguments* dest, void* src){
    memcpy(&(dest->id), src+ID_OFFSET , ROW_ID_SIZE_BYTE);
    memcpy(dest->username, src+USER_OFFSET, ROW_USER_SIZE_BYTE); 
    memcpy(dest->email, src+EMAIL_OFFSET, ROW_EMAIL_SIZE_BYTE);
}

InputBuffer* new_input_bufffer(){
    InputBuffer* inputBuffer = (InputBuffer*)malloc(sizeof(InputBuffer));
    inputBuffer->buffer = NULL;
    inputBuffer->buffer_length = 0;
    inputBuffer->input_length = 0;
    
    return inputBuffer;
}

Pager* pager_init(const char* filename){
    Pager* pager = (Pager*)malloc(sizeof(Pager));

    for (int i = 0; i < TABLE_MAX_PAGES; i++){
        pager->pages[i] = NULL;
    }

    int fd = open(filename,O_CREAT|O_RDWR, S_IWUSR|S_IRUSR);

    if (fd==-1){
        printf("DB File could not be open, exiting.\n");
        perror("Error: ");
        exit(EXIT_FAILURE);
    }
    pager->filesize = lseek(fd,0,SEEK_END);
    pager->fd = fd;
    pager->num_of_pages = pager->filesize/PAGE_SIZE;

    if (pager->filesize%PAGE_SIZE != 0){
        printf("Database file has incomplete page. The file is corrupted. Exiting...\n");
        exit(EXIT_FAILURE);
    }

    return pager;
}

void print_leaf_node(void* node) {
    uint32_t num_cells = *get_leaf_num_of_cells(node);
    printf("leaf (size %d)\n", num_cells);
    for (uint32_t i = 0; i < num_cells; i++) {
        uint32_t key = *get_leaf_key(node, i);
        printf("  - %d : %d\n", i, key);
    }
}

Statement* new_statement(){
    Statement* statement = (Statement*)malloc(sizeof(Statement));
    statement->arguments = (Arguments*)malloc(sizeof(Arguments));
    return statement;
}

void print_prompt(){
    printf("db > ");
}

void* get_page(Pager* pager, uint32_t page_num){
    void* page = pager->pages[page_num];
    if (page == NULL){
        page = (void*)malloc(PAGE_SIZE);

        if (page_num > pager->num_of_pages){
            pager->num_of_pages += 1;
        }
        if (page_num <= pager->num_of_pages){
            lseek(pager->fd,page_num*PAGE_SIZE,SEEK_SET);
            ssize_t readBytes = read(pager->fd,page,PAGE_SIZE);
            if (readBytes == -1){
                printf("Could not read a page. Exiting. \n");
                exit(EXIT_FAILURE);
            }
        }
        pager->pages[page_num] = page;
    }
    return page;
}

void cursor_advance(Cursor* cursor){
    if (cursor->cell_num + 1 > CELL_PER_PAGE){
        cursor->page_num += 1;
        cursor->cell_num = 0;
    } else {
        cursor->cell_num += 1;
    }
    void* page = get_page(cursor->table->pager, cursor->page_num);
    if (cursor->cell_num >= (*get_leaf_num_of_cells(page))){
        cursor->end_of_table = true;
    } 
}

Cursor* table_start(Table* table){
    Cursor* cursor = malloc(sizeof(Cursor)); 
    cursor->page_num = table->btree->root_page_num;
    cursor->cell_num = 0;
    cursor->table = table;
    cursor->end_of_table = false;

    void* page = get_page(table->pager, cursor->page_num);
    cursor-> end_of_table = (*get_leaf_num_of_cells(page) == 0);
    return cursor;
}

Cursor* table_end(Table* table){
    Cursor* cursor = malloc(sizeof(Cursor));
    cursor->end_of_table = true;
    cursor->page_num = table->btree->root_page_num; 

    void* page = get_page(table->pager, cursor->page_num);
    cursor->cell_num = *get_leaf_num_of_cells(page);

    cursor->table = table;


    return cursor;
}

void* cursor_value(Cursor* cursor){
    void* page = get_page(cursor->table->pager,cursor->page_num);
    return get_leaf_value(page,cursor->cell_num);
}

void insert_row(Cursor* cursor, Arguments* arguments){
    void* page = get_page(cursor->table->pager,cursor->page_num);
    uint32_t num_of_cells = *get_leaf_num_of_cells(page);

    if (num_of_cells == CELL_PER_PAGE){ //TODO temporary while we can't split the nodes
        printf("Too many cells for one page. For now we support only one page.\n");
        exit(EXIT_FAILURE);
    }

    (*get_leaf_num_of_cells(page)) += 1;
    (*get_leaf_key(page,num_of_cells)) = arguments->id;

    void* dest = cursor_value(cursor);
    serialise_row(arguments, dest);
}

void read_input(InputBuffer* inputBuffer){
    ssize_t inputLength = getline(&(inputBuffer->buffer),&(inputBuffer->buffer_length), stdin);

    if (inputLength <= 0){
        printf("Could not read a line. Exiting.\n");
        exit(EXIT_FAILURE);
    }

    inputBuffer->input_length = inputLength - 1;
    inputBuffer->buffer[inputLength-1] = 0;
}

void flush_page(Pager* pager, uint16_t page_num){
    void* page = pager->pages[page_num];
    if (page == NULL){
        printf("Attempting to write empty page.\n");
        return;
    }

    lseek(pager->fd,page_num*PAGE_SIZE,SEEK_SET);
    ssize_t res = write(pager->fd,page,PAGE_SIZE);
    if (res == -1){
        printf("Persisting a page to a filesystem failed. \n");
        exit(EXIT_FAILURE);
    }
}

Table* db_open(const char* filename){
    Table* table = (Table*)malloc(sizeof(Table));

    table->pager = pager_init(filename);

    BTree* bTree = malloc(sizeof(BTree));
    if (table->pager->num_of_pages==0){
        void* page = get_page(table->pager,0);
        initialise_leaf_node(page);
        table->pager->num_of_pages = 1;
    }
    bTree->root_page_num = 0;//TODO tbd
    table->btree = bTree;

    return table;
}

void close_db(Table* table){
    Pager* pager = table->pager;
    for (int i = 0; i < pager->num_of_pages;i++){
        if (pager->pages[i] != NULL){
            flush_page(pager,i);
        }
    }
    
    for (int i = 0; i < TABLE_MAX_PAGES; i++){
        free(table->pager->pages[i]);
    }

    free(table->btree);

    int res = close(table->pager->fd);
    if (res == -1){
        printf("Could not close a file descriptor. Exiting. \n");
        exit(EXIT_FAILURE);
    }
    free(table->pager);
    free(table);
}



void close_input_buffer(InputBuffer* inputBuffer){
    free(inputBuffer->buffer);
    free(inputBuffer);
}

void close_statement(Statement* statement){
    free(statement->arguments);
    free(statement);
}


MetaCommandResult do_meta_command(InputBuffer* inputBuffer, Table* table){
    if (strcmp(inputBuffer->buffer,".exit")==0){
        close_input_buffer(inputBuffer);
        close_db(table);
        exit(EXIT_SUCCESS);
    } else if (strcmp(inputBuffer->buffer, ".btree") == 0) {
        printf("Tree:\n");
        print_leaf_node(get_page(table->pager, 0));
        return META_COMMAND_SUCCESS;
    }
    else {
        return META_UNKNOWN_COMMAND;
    }
}

ParsingResult parse_arguments_insert(InputBuffer* inputBuffer, Statement* statement){
    statement->type = STATEMENT_INSERT;
    char delim[2] = " ";
    char* modifier = strtok(inputBuffer->buffer, delim);
    char* id_string = strtok(NULL, delim); 
    char* user = strtok(NULL, delim);
    char* email = strtok(NULL, delim);

    if ( strtok(NULL, delim) != NULL){
        return PARSING_TOO_MANY_ARGUMENTS;
    }

    if (id_string == NULL || user == NULL || email == NULL){
        return PARSING_ERROR;
    }

    int id = atoi(id_string); //THIS IS VERY UNSAFE. IF WE PASS '0' or "RANDOM STEING", both would resolve to 0
    if (id < 0){
        return PARSING_NEGATIVE_ID;
    } 
    if (strlen(user) > USERNAME_FIELD_LENGTH || strlen(email) > EMAIL_FIELD_LENGTH){
        return PARSING_TOO_LONG;
    }


    statement->arguments->id = id;
    strcpy(statement->arguments->username, user);
    strcpy(statement->arguments->email, email);


    return PARSING_SUCCESS;
}

PrepareStatementResult prepare_statement(InputBuffer* inputBuffer, Statement* statement){
    
    if (strncmp(inputBuffer->buffer,"select",6) == 0){    
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    } else if (strncmp(inputBuffer->buffer, "insert", 6) == 0){

        switch (parse_arguments_insert(inputBuffer, statement)){
            case(PARSING_SUCCESS):
                return PREPARE_SUCCESS;
            case(PARSING_TOO_LONG):
                return PREPARE_PARSING_TOO_LONG;
            case(PARSING_TOO_MANY_ARGUMENTS):
                return PREPARE_PARSING_TOO_MANY_ARGUMENTS;
            case(PARSING_ERROR):
                return PREPARE_PARSING_FAILURE;
            case(PARSING_NEGATIVE_ID):
                return PREPARE_NEGATIVE_ID;
        }

        return PREPARE_SUCCESS;
    } else{
        return PREPARE_UNKOWN;
    }
}

ExecuteResult execute_insert(Table* table, Arguments* arguments){
    Cursor* cursor = table_end(table);
    void* page = get_page(cursor->table->pager,cursor->page_num);
    uint32_t num_of_cells = *get_leaf_num_of_cells(page); 
    if (num_of_cells == CELL_PER_PAGE){ //TODO temporary while we can't split the nodes
        free(cursor);
        return EXECUTE_TABLE_FULL; 
    }
    insert_row(cursor,arguments);

    free(cursor);
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Table* table){
    Cursor* cursor = table_start(table);
    while(!cursor->end_of_table){
        Arguments* arguments = (Arguments*)malloc(sizeof(Arguments));
        deserialise_row(arguments,cursor_value(cursor));
        printf("{id:%d, email:%s, user:%s }\n",arguments->id, arguments->email, arguments->username);
        cursor_advance(cursor);
    }
    free(cursor);
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_statement(Table* table, Statement* statement){
    switch (statement->type)
    {
    case STATEMENT_SELECT:
        return execute_select(table);
    case STATEMENT_INSERT:
        return execute_insert(table,statement->arguments);
    default:
        printf("Illegal State.\n");
        exit(EXIT_FAILURE);
    }
}

int main(int argc, char* argv[]){

    if (argc < 2){
        printf("Must supply a database filename.\n");
        exit(EXIT_FAILURE);
    }

    char* filename = argv[1];
    Table* table = db_open(filename);
    
    InputBuffer* inputBuffer = new_input_bufffer();

    while(true){
        print_prompt();
        read_input(inputBuffer);
        if (inputBuffer->buffer[0] == '.'){
            MetaCommandResult result = do_meta_command(inputBuffer, table);
            if (result == META_UNKNOWN_COMMAND){
                printf("Unkown command: %s\n",inputBuffer->buffer);
            }
            continue;
        }

        Statement* statement = new_statement();
        

        switch (prepare_statement(inputBuffer, statement)){
            case(PREPARE_UNKOWN):
                printf("Unkown query: %s.\n", inputBuffer->buffer);
                continue;
            case(PREPARE_PARSING_FAILURE):
                printf("Failed to parse arguments for query %s. \n", inputBuffer->buffer);
                continue;
            case(PREPARE_PARSING_TOO_LONG):
                printf("Failed to parse query. The fields exceeded maximum length.\n");
                continue;
            case(PREPARE_PARSING_TOO_MANY_ARGUMENTS):
                printf("Failed to parse query. Too many fields were provided.\n");
                continue;
            case(PREPARE_NEGATIVE_ID):
                printf("Failed to parse the query. It contains a negative id.\n");
                continue;
            case PARSING_SUCCESS:
                break;
        }

        if (execute_statement(table, statement) == EXECUTE_TABLE_FULL){
            printf("Cannot insert new data. Table is full.\n");
        } else {
            printf("Executed.\n");       
        }

        close_statement(statement);
    }
}