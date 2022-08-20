#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <time.h>
#include <btree.h>
typedef enum
{
  META_COMMAND_SUCCESS,
  META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

typedef enum
{
  PREPARE_SUCCESS,
  PREPARE_UNRECOGNIZED_STATEMENT,
  PREPARE_SYNTAX_ERROR
} PrepareResult;

typedef enum
{
  STATEMENT_INSERT,
  STATEMENT_SELECT
} StatementType;

typedef enum
{
  EXECUTE_TABLE_FULL,
  EXECUTE_SUCCESS
} ExecResult;

typedef struct
{
  StatementType type;
  Row row_to_insert;
} Statement;

typedef struct
{
  char *buffer;
  size_t buffer_length;
  ssize_t input_length;
} InputBuffer;
const u_int32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;

void print_constants()
{
  printf("ROW_SIZE: %d\n", ROW_SIZE);
  printf("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE);
  printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE);
  printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE);
  printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LEAF_NODE_SPACE_FOR_CELLS);
  printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS);
}

void *get_page(Pager *pager, uint32_t page_num)
{
  if (page_num > TABLE_MAX_PAGES)
  {
    printf("Tried to fetch page outof bound %d > %d.", page_num, TABLE_MAX_PAGES);
    exit(EXIT_FAILURE);
  }
  if (pager->pages[page_num] == NULL)
  {
    // Cache miss. Allocate memory and load from file.
    void *page = malloc(PAGE_SIZE);
    uint32_t num_pages = pager->file_length / PAGE_SIZE;

    // We might save a partial page at the end of the file
    if (pager->file_length % PAGE_SIZE)
    {
      num_pages += 1;
    }

    if (page_num <= num_pages)
    {
      lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
      ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
      if (bytes_read == -1)
      {
        printf("Error reading file: %d\n", errno);
        exit(EXIT_FAILURE);
      }
    }

    pager->pages[page_num] = page;
    if (page_num >= pager->num_pages)
    {
      pager->num_pages++;
    }
  }

  return pager->pages[page_num];
}

Cursor *table_start(Table *table)
{
  Cursor *cursor = malloc(sizeof(Cursor));
  cursor->table = table;
  cursor->cell_num = 0;
  cursor->page_num = table->root_page_num;

  void *root_node = get_page(table->pager, table->root_page_num);
  uint32_t num_cells = *leaf_node_num_cells(root_node);
  cursor->end_of_table = (num_cells == 0);

  return cursor;
}

Cursor *table_end(Table *table)
{
  Cursor *cursor = malloc(sizeof(Cursor));
  cursor->table = table;
  cursor->page_num = table->root_page_num;
  void *root_node = get_page(table->pager, table->root_page_num);
  uint32_t num_cells = *leaf_node_num_cells(root_node);
  cursor->cell_num = num_cells;
  cursor->end_of_table = true;

  return cursor;
}

void serialize_row(Row *source, void *destination)
{
  memcpy(destination + ID_OFFSET, &source->id, ID_SIZE);
  memcpy(destination + USERNAME_OFFSET, &source->username, USERNAME_SIZE);
  memcpy(destination + EMAIL_OFFSET, &source->email, EMAIL_SIZE);
}

void deserialize_row(void *source, Row *destination)
{
  memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
  memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
  memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

void *cursor_value(Cursor *cursor)
{
  void *page = get_page(cursor->table->pager, cursor->page_num);

  return leaf_node_value(page, cursor->cell_num);
}

void cursor_advance(Cursor *cursor)
{
  void *node = get_page(cursor->table->pager, cursor->page_num);
  cursor->cell_num++;
  if (cursor->cell_num >= (*leaf_node_num_cells(node)))
  {
    cursor->end_of_table = true;
  }
}
void leaf_node_insert(Cursor *cursor, uint32_t key, Row *value)
{
  void *node = get_page(cursor->table->pager, cursor->page_num);
  uint32_t num_cells = *leaf_node_num_cells(node);
  if (num_cells >= LEAF_NODE_MAX_CELLS)
  {
    printf("Need to implement splitting a leaf node.\n");
    exit(EXIT_FAILURE);
  }
  if (cursor->cell_num < num_cells)
  {
    for (uint32_t i = num_cells; i > cursor->cell_num; i--)
    {
      memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i - 1), LEAF_NODE_CELL_SIZE);
    }
  }
  *(leaf_node_num_cells(node)) += 1;
  *(leaf_node_key(node, cursor->cell_num)) = key;
  serialize_row(value, leaf_node_value(node, cursor->cell_num));
}

InputBuffer *new_input_buffer()
{
  InputBuffer *input_buffer = malloc(sizeof(InputBuffer));
  input_buffer->buffer = NULL;
  input_buffer->buffer_length = 0;
  input_buffer->input_length = 0;

  return input_buffer;
}
void pager_flush(Pager *pager, uint32_t page_num)
{
  if (pager->pages[page_num] == NULL)
  {
    printf("Tried to flush null page\n");
    exit(EXIT_FAILURE);
  }

  off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);

  if (offset == -1)
  {
    printf("Error seeking: %d\n", errno);
    exit(EXIT_FAILURE);
  }

  ssize_t bytes_written =
      write(pager->file_descriptor, pager->pages[page_num], PAGE_SIZE);

  if (bytes_written == -1)
  {
    printf("Error writing: %d\n", errno);
    exit(EXIT_FAILURE);
  }
}

void db_close(Table *table)
{
  Pager *pager = table->pager;

  for (uint32_t i = 0; i < pager->num_pages; i++)
  {
    if (pager->pages[i] == NULL)
    {
      continue;
    }
    pager_flush(pager, i);
    free(pager->pages[i]);
    pager->pages[i] = NULL;
  }

  int result = close(pager->file_descriptor);
  if (result == -1)
  {
    printf("Error closing db file.\n");
    exit(EXIT_FAILURE);
  }
  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++)
  {
    void *page = pager->pages[i];
    if (page)
    {
      free(page);
      pager->pages[i] = NULL;
    }
  }
  free(pager);
  free(table);
}

MetaCommandResult do_meta_command(InputBuffer *inbfr, Table *table)
{
  if (strcmp(inbfr->buffer, ".exit") == 0)
  {
    db_close(table);
    exit(EXIT_SUCCESS);
  }
  else if (strcmp(inbfr->buffer, ".btree") == 0)
  {
    printf("Tree:\n");
    print_leaf_node(get_page(table->pager, 0));
    return META_COMMAND_SUCCESS;
  }
  else if (strcmp(inbfr->buffer, ".constants") == 0)
  {
    printf("Constants:\n");
    print_constants();
    return META_COMMAND_SUCCESS;
  }
  return META_COMMAND_UNRECOGNIZED_COMMAND;
}

void read_input(InputBuffer *inbfr)
{
  ssize_t bytes_read = getline(&(inbfr->buffer), &(inbfr->buffer_length), stdin);
  if (bytes_read <= 0)
  {
    fprintf(stderr, "Could not read line!\n");
    exit(EXIT_FAILURE);
  }
  inbfr->input_length = bytes_read - 1;
  inbfr->buffer[bytes_read - 1] = 0;
}

void close_input_buffer(InputBuffer *inbfr)
{
  free(inbfr->buffer);
  free(inbfr);
}

void print_prompt()
{
  printf("db> ");
}

PrepareResult prepare_statement(InputBuffer *inbfr, Statement *stmt)
{
  if (strncasecmp(inbfr->buffer, "select", 6) == 0)
  {
    stmt->type = STATEMENT_SELECT;
    return PREPARE_SUCCESS;
  }

  if (strncasecmp(inbfr->buffer, "insert", 6) == 0)
  {
    stmt->type = STATEMENT_INSERT;
    int args_assigned = sscanf(
        inbfr->buffer, "insert %d %s %s", &(stmt->row_to_insert.id),
        stmt->row_to_insert.username, stmt->row_to_insert.email);
    if (args_assigned < 3)
    {
      return PREPARE_SYNTAX_ERROR;
    }
    return PREPARE_SUCCESS;
  }
  return PREPARE_UNRECOGNIZED_STATEMENT;
}

ExecResult execute_insert(Row *row, Table *table)
{
  void *node = get_page(table->pager, table->root_page_num);
  if (full_leaf_node(node))
  {
    return EXECUTE_TABLE_FULL;
  }

  Cursor *c = table_end(table);
  void *row_location = cursor_value(c);
  leaf_node_insert(c, row->id, row);
  free(c);
  printf("Inserted.\n");
  return EXECUTE_SUCCESS;
}

void print_row(Row *row)
{
  printf("%i | %s | %s\n", row->id, row->username, row->email);
}

ExecResult execute_select(Statement *statement, Table *table)
{
  Cursor *c = table_start(table);
  Row row;
  while (!(c->end_of_table))
  {
    deserialize_row(cursor_value(c), &row);
    print_row(&row);
    cursor_advance(c);
  }
  free(c);
  return EXECUTE_SUCCESS;
}

ExecResult execute_statement(Statement *statement, Table *table)
{
  switch (statement->type)
  {
  case (STATEMENT_INSERT):
  {
    return execute_insert(&statement->row_to_insert, table);
  }
  case (STATEMENT_SELECT):
  {
    return execute_select(statement, table);
  }
  }
}

Pager *pager_open(const char *filename)
{
  int fd = open(filename, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);
  if (fd == -1)
  {
    fprintf(stderr, "Unable to open file\n");
    exit(EXIT_FAILURE);
  }
  off_t file_length = lseek(fd, 0, SEEK_END);
  Pager *pager = malloc(sizeof(Pager));
  pager->file_descriptor = fd;
  pager->file_length = file_length;
  pager->num_pages = file_length / PAGE_SIZE;

  if (file_length % PAGE_SIZE != 0)
  {
    printf("Db file is not a whole number of pages. Corrupt file.\n");
    exit(EXIT_FAILURE);
  }
  for (size_t i = 0; i < TABLE_MAX_PAGES; i++)
  {
    pager->pages[i] = NULL;
  }
  return pager;
}

Table *open_db(const char *filename)
{
  Pager *p = pager_open(filename);
  Table *t = malloc(sizeof(Table));
  t->pager = p;
  t->root_page_num = 0;

  if (p->num_pages == 0)
  {
    printf("OH SHIT NERW FILE\n");
    // A new DB file.
    void *root_node = get_page(p, 0);
    initialize_leaf_node(root_node);
  }

  return t;
}

int main(int argc, char *argv[])
{
  bool verbose;
  char *filename;
  if (argc >= 2)
  {
    if (argc >= 3)
    {
      verbose = strcmp(argv[2], "-v") == 0;
    }
    filename = argv[1];
  }
  else
  {
    printf("Please supply the file name\n");
    exit(EXIT_FAILURE);
  }

  Table *table = open_db(filename);

  InputBuffer *input_buffer = new_input_buffer();
  while (true)
  {
    print_prompt();
    read_input(input_buffer);

    if (input_buffer->buffer[0] == '.')
    {
      switch (do_meta_command(input_buffer, table))
      {
      case (META_COMMAND_SUCCESS):
        continue;
      case (META_COMMAND_UNRECOGNIZED_COMMAND):
        printf("Unrecognized command '%s'\n", input_buffer->buffer);
        continue;
      }
    }

    Statement stmt;

    switch (prepare_statement(input_buffer, &stmt))
    {
    case PREPARE_SUCCESS:
      break;
    case PREPARE_UNRECOGNIZED_STATEMENT:
      printf("Unrecognized keyword at start of '%s'.\n", input_buffer->buffer);
      continue;
    case PREPARE_SYNTAX_ERROR:
      printf("Syntax error. dunno what to do\n");
      continue;
    }
    clock_t start_time = clock();
    ExecResult res = execute_statement(&stmt, table);
    switch (res)
    {
    case EXECUTE_TABLE_FULL:
      printf("Table do be full\n");
      break;

    case EXECUTE_SUCCESS:
    {

      if (verbose)
      {
        int time_taken = (clock() - start_time);
        printf("Success. Took %d ms.\n", time_taken);
      }
      break;
    }
    }
  }
}
