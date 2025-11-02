#include <stdint.h>
#include <postgresql/libpq-fe.h>
#include <stdbool.h>
#include <stdlib.h>
#include <stdio.h>
#include <m-dict.h>
#include <utf32.h>
#include <omp.h>
#include <arpa/inet.h>

DICT_DEF2 (character_count, uint32_t, uint32_t);
struct OccurenceResult
{
  uint32_t id;
  uint32_t origin;
  character_count_t result;
  uint32_t* input_data;
};

// A single allocated buffer containing the all the data, several
// gigabytes
uint32_t* input_data_global;
struct OccurenceResult* output;

inline static bool
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

inline static bool
setup_conn (PGconn** conn)
{
  *conn = PQconnectdb (
    "host=localhost dbname=jpcodes user=jpcodes password=JpCodes");
  if (PQstatus (*conn) != CONNECTION_OK)
  {
    fprintf (stderr,
             "Failed to connect to database. %s",
             PQerrorMessage (*conn));
    PQfinish (*conn);
    printf ("Failed to Connect.");
    exit (1);
  }

  return true;
};

inline static void
query_all_text_sources (PGconn** conn, PGresult** res)
{

  *res = PQexecParams (*conn,
                       "select * from training_data.AllTextSources "
                       "order by id;",
                       0,
                       0,
                       0,
                       NULL,
                       NULL,
                       1);
}

static inline int
unpack_all_text_sources (PGconn** con_ptr, PGresult** res_ptr)
{
  PGresult* res = *res_ptr;
  int rows = PQntuples (res);

  int id_indice = PQfnumber (res, "id");
  int data_indice = PQfnumber (res, "data");
  int source_id_indice = PQfnumber (res, "sourceId");

  output = calloc (sizeof (struct OccurenceResult), rows);

  uint64_t totalPayloadSize = 0;
  uint64_t offsets[rows];

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
  return rows;
}
