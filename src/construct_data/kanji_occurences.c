#include <iso646.h>
#include <stdint.h>
#include <m-dict.h>
#include <m-string.h>
#include <postgresql/libpq-fe.h>
#include <arpa/inet.h>
#include <omp.h>
#include <utf32.h>

DICT_DEF2 (character_count, uint32_t, uint32_t);

uint32_t* input_data_global;
struct OccurenceResult
{
  uint32_t id;
  uint32_t origin;
  character_count_t result;
  uint32_t* input_data;
};

// maybe correct lol
bool
is_chinese_char (uint32_t codepoint)
{
  return (codepoint >= 0x4E00 && codepoint <= 0x9FFF) ||
         (codepoint >= 0x3400 && codepoint <= 0x4DBF) ||
         (codepoint >= 0x20000 && codepoint <= 0x2A6DF) ||
         (codepoint >= 0x2A700 && codepoint <= 0x2B73F) ||
         (codepoint >= 0x2B740 && codepoint <= 0x2B81F) ||
         (codepoint >= 0x2B820 && codepoint <= 0x2CEAF) ||
         (codepoint >= 0x2CEB0 && codepoint <= 0x2EBEF) ||
         (codepoint >= 0xF900 && codepoint <= 0xFAFF);
}

bool
setup_conn (PGconn** conn)
{
  *conn = PQconnectdb (
    "host=localhost dbname=jpcodes user=jpcodes password=JpCodes");
  if (PQstatus (*conn) != CONNECTION_OK)
  {
    fprintf (stderr,
             "Failed to conenct to databsae. %s",
             PQerrorMessage (*conn));
    PQfinish (*conn);
    printf ("Failled to Connect.");
    exit (1);
  }

  return true;
};

struct OccurenceResult* output;

int
init_occurences ()
{
  PGconn* conn;
  setup_conn (&conn);

  PGresult* res =
    PQexecParams (conn,
                  "select * from training_data.AllTextSources;",
                  0,
                  0,
                  0,
                  NULL,
                  NULL,
                  1);

  int rows = PQntuples (res);
  int cols = PQnfields (res);

  int id_indice = PQfnumber (res, "id");
  int data_indice = PQfnumber (res, "data");
  int source_id_indice = PQfnumber (res, "sourceId");

  output = calloc (sizeof (struct OccurenceResult), rows);

  uint64_t totalPayloadSize = 0;

  uint64_t offsets[rows];

#pragma omp parallel for
  for (int i = 0; i != rows; i++)
  {

    char* data = PQgetvalue (res, i, data_indice);

    int length = utf8_to_utf32_length (data) + 1;

    offsets[i] = totalPayloadSize;
    totalPayloadSize += length;
  };

  input_data_global = malloc (totalPayloadSize * 4);

#pragma omp parallel for
  for (int i = 0; i != rows; i++)
  {
    struct OccurenceResult* current = &output[i];
    memcpy (
      &current->id, PQgetvalue (res, i, id_indice), sizeof (int32_t));

    current->id = ntohl (current->id);

    memcpy (&current->origin,
            PQgetvalue (res, i, source_id_indice),
            sizeof (int32_t));

    current->origin = ntohl (current->origin);

    char* data = PQgetvalue (res, i, data_indice);
    uint32_t* writePos = &input_data_global[offsets[i]];
    str_to_utf32 (data, writePos);
    current->input_data = writePos;
  }

  PQclear (res);

#pragma omp parallel for
  for (uint32_t i = 0; i != rows; i++)
  {

    struct OccurenceResult* current = &output[i];
    character_count_init (current->result);
    character_count_reserve (current->result, 1024);

    uint32_t* readPos = current->input_data;

    character_count_t* cmap = &current->result;

    while (true)
    {
      uint32_t c = readPos[0];
      if (c == 0)
      {
        break;
      };

      // printf ("%s", c_utf32_char_to_utf8_char (c).buf);
      uint32_t* num = character_count_safe_get (*cmap, c);
      uint32_t cnum = num ? *num + 1 : 1;
      character_count_set_at (*cmap, c, cnum);
      readPos += 1;
    }
    // printf ("\0");
  }

  int num_threads = omp_get_max_threads ();
  PGconn* conns[num_threads + 1];
  for (int i = 0; i != num_threads + 1; i++)
  {
    printf ("THREAD: %d\n", i);
    PGconn* conn;
    setup_conn (&conn);

    conns[i] = conn;
    res = PQexec (conn, "BEGIN;");
    PQclear (res);
  };

  printf ("number threads: %d\n", num_threads);

#pragma omp parallel for
  for (int i = 0; i != rows; i++)
  {
    PGresult* res;
    int thread_id = omp_get_thread_num ();

    // printf ("id: %d\n", thread_id);
    PGconn* sub_conn = conns[thread_id];

    // printf ("insering: %d\n", i);
    struct OccurenceResult current = output[i];

    character_count_t* dict = &current.result;

    int size = character_count_size (current.result);
    if (size == 0)
    {
      continue;
    }
    // each uint32 can be around 10 unary long
    //  5 = the size of FALSE
    //  7 = the commas and ()
    //  20 = CALL Insert_Into_CharacterCount...

    uint64_t approxSize = (((10 * 4) + 20) * size * 4) + 5 + 7;
    char insertString[approxSize];
    char* writePos = insertString;
    struct character_count_it_s it;

    for (character_count_it (&it, *dict);
         !character_count_end_p (&it);
         character_count_next (&it))
    {
      character_count_itref_t* pair = character_count_ref (&it);
      uint32_t key = pair->key;
      uint32_t value = pair->value;
      uint32_t origin = current.origin;
      uint32_t id = current.id;
      char* isKanji = is_chinese_char (pair->key) ? "TRUE" : "FALSE";

      int length = sprintf (
        writePos,
        "CALL Insert_Into_CharacterCount(%d, %d, %s, %d, %d);",
        key,
        value,
        isKanji,
        origin,
        id);
      writePos += length;
    }

    // printf ("EXECUTING\n");
    res = PQexec (sub_conn, insertString);
    if (PQresultStatus (res) != PGRES_COMMAND_OK)
    {
      printf ("%s\n", insertString);
      printf ("totla size: %d\n", size);
      fprintf (
        stderr, "INSERT failed: %s", PQerrorMessage (sub_conn));
      PQclear (res);
      PQfinish (sub_conn);
      exit (1);
    }

    PQclear (res);
  }

  for (int i = 0; i != num_threads + 1; i++)
  {
    PGconn* conn = conns[i];
    res = PQexec (conn, "COMMIT;");
    PQclear (res);
    PQfinish (conn);
  };

  PQfinish (conn);
  return rows;
}

int
main ()
{
  init_occurences ();
  free (input_data_global);
  return 0;
}
