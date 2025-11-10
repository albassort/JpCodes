#include <iso646.h>
#include <stdint.h>
#include <m-dict.h>
#include <m-string.h>
#include <postgresql/libpq-fe.h>
#include <arpa/inet.h>
#include <omp.h>
#include <utf32.h>
#include "shared.h"

void
commit_occuerences (PGconn** conn_ptr, int rows)
{
  PGconn* conn = *conn_ptr;
  PGresult* res;

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
  for (int i = 0; i < rows; i++)
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

  res =
    PQexec (conn, "REFRESH MATERIALIZED VIEW CharacterCountsSums;");

  PQclear (res);
  PQfinish (conn);
}

int
main ()
{
  PGconn* conn;
  setup_conn (&conn);
  PGresult* data;
  query_all_text_sources (&conn, &data);
  int rows = unpack_all_text_sources (&conn, &data);
  commit_occuerences (&conn, rows);
  free (input_data_global);
  free (output);
  return 0;
}
