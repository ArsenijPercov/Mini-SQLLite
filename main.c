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
    void* pages[TABLE_MAX_PAGES];
} Pager;

typedef struct{
    uint32_t row_count;
    Pager* pager;    
} Table;

typedef struct{
    Table* table;
    int row_num;
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

Cursor* table_start(Table* table){
    Cursor* cursor = malloc(sizeof(Cursor));
    cursor->end_of_table = (table->row_count == 0);
    cursor->row_num = 0;
    cursor->table = table;
    return cursor;
}


Cursor* table_end(Table* table){
    Cursor* cursor = malloc(sizeof(Cursor));
    cursor->end_of_table = true;
    cursor->row_num = table->row_count;
    cursor->table = table;
    return cursor;
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
    //TODO initialise everything open the file

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
    return pager;
}

Table* db_open(const char* filename){
    Table* table = (Table*)malloc(sizeof(Table));
    
    table->pager = pager_init(filename);
    table->row_count = table->pager->filesize/ROW_SIZE;



    return table;
}

Statement* new_statement(){
    Statement* statement = (Statement*)malloc(sizeof(Statement));
    statement->arguments = (Arguments*)malloc(sizeof(Arguments));
    return statement;
}

void cursor_advance(Cursor* cursor){
    cursor->row_num+=1;
    if (cursor->row_num >= cursor->table->row_count){
        cursor->end_of_table = true;
    } 
}

void print_prompt(){
    printf("db > ");
}
void* get_page(Cursor* cursor){
    uint32_t page_num = cursor->row_num / ROWS_PER_PAGES;
    Pager* pager = cursor->table->pager;
    void* page = pager->pages[page_num];
    if (page == NULL){
        page = (void*)malloc(PAGE_SIZE);
        int pages_total = pager->filesize/ROW_SIZE;
        if (pager->filesize%ROW_SIZE){
            pages_total++;
        }
        if (page_num <= pages_total){
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

void* cursor_value(Cursor* cursor){
    void* page = get_page(cursor);
    uint32_t row_offset = cursor->row_num % ROW_SIZE;
    uint32_t byte_offset = row_offset * ROW_SIZE;

    return page + byte_offset;
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

void flush_page(Pager* pager, uint16_t page_num, uint32_t size){
    void* page = pager->pages[page_num];
    if (page == NULL){
        printf("Attempting to write empty page.\n");
        return;
    }

    lseek(pager->fd,page_num*PAGE_SIZE,SEEK_SET);
    ssize_t res = write(pager->fd,page,size);
    if (res == -1){
        printf("Persisting a page to a filesystem failed. \n");
        exit(EXIT_FAILURE);
    }
}

void close_db(Table* table){
    Pager* pager = table->pager;
    for (int i = 0; i < table->row_count/ROW_SIZE;i++){
        if (pager->pages[i] != NULL){
            flush_page(pager,i, PAGE_SIZE);
        }
    }
    int extra_rows = table->row_count%ROW_SIZE;
    if (extra_rows){
       flush_page(pager,table->row_count/ROW_SIZE,extra_rows*ROW_SIZE); 
    }
    
    for (int i = 0; i < TABLE_MAX_PAGES; i++){
        free(table->pager->pages[i]);
    }


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

MetaCommandResult do_meta_command(InputBuffer* inputBuffer, Table* table){
    if (strcmp(inputBuffer->buffer,".exit")==0){
        close_input_buffer(inputBuffer);
        close_db(table);
        exit(EXIT_SUCCESS);
    } else {
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
    if (table->row_count >= TABLE_MAX_ROWS){
        return EXECUTE_TABLE_FULL;
    }
    Cursor* cursor = table_end(table);

    void* dest = cursor_value(cursor);
    serialise_row(arguments, dest);
    table->row_count += 1;
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