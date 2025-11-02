#include <iso646.h>
#include <stdint.h>
#include <m-dict.h>
#include <m-string.h>
#include <postgresql/libpq-fe.h>
#include <arpa/inet.h>
#include <omp.h>
#include <utf32.h>
#include "shared.h"

// not so efficient but good enough
DICT_DEF2 (sequence_count, string_t, uint32_t);

void
get_all_permutations (uint32_t* strstart,
                      uint8_t length,
                      int curPos,
                      char** outBuf,

                      sequence_count_t* map)
{

  int distanceToEnd = (length - curPos);

  // for (int i = 0; i != length; i++)
  // {
  //   char* c = c_utf32_char_to_utf8_char (strstart[i]).buf;
  //   // printf ("%s\n", c);
  // }

  int written;
  bool openA = false;

  string_t s1;
  string_init (s1);
  for (int print_length = 2; distanceToEnd >= print_length;
       print_length++)
  {

    int writePos = 0;
    written = sprintf (*outBuf, "ARRAY[");
    writePos += written;

    int endPos = curPos + print_length;
    for (int i = curPos; endPos != i; i++)
    {
      uint32_t c = strstart[i];

      int written = sprintf ((*outBuf) + writePos, "%d, ", c);
      writePos += written;
    }

    written = sprintf ((*outBuf) + writePos - 2, "]\0");
    writePos += written - 2;

    string_set_str (s1, *outBuf);

    uint32_t* val = sequence_count_safe_get (*map, s1);
    uint32_t newVal = val ? *val + 1 : 1;
    sequence_count_set_at (*map, s1, newVal);
  }

  for (int print_length = 2; curPos + 1 >= print_length;
       print_length++)
  {

    int writePos = 0;
    written = sprintf (*outBuf, "ARRAY[");
    writePos += written;

    for (int i = curPos - print_length + 1; curPos >= i; i++)
    {

      uint32_t c = strstart[i];

      int written = sprintf ((*outBuf) + writePos, "%d, ", c);
      writePos += written;
    }

    written = sprintf ((*outBuf) + writePos - 2, "]\0");
    writePos += written - 2;

    string_set_str (s1, *outBuf);

    uint32_t* val = sequence_count_safe_get (*map, s1);
    uint32_t newVal = val ? *val + 1 : 1;
    sequence_count_set_at (*map, s1, newVal);
  }
};

void
commit_occuerences (PGconn** conn_ptr, int rows)
{

  int num_threads = omp_get_max_threads ();

  char* sql_execs[num_threads];
  uint32_t t_length[num_threads];
  sequence_count_t sequences[num_threads];

  uint32_t t_commit_length[num_threads];
  char* db_commit_strings[num_threads];

  PGconn* conns[num_threads];
  static const int start_size = 8192;

  for (int i = 0; i != num_threads; i++)
  {
    sql_execs[i] = malloc (start_size);
    t_length[i] = start_size;
    sequence_count_init (sequences[i]);

    sequence_count_reserve (sequences[i], start_size);
    t_commit_length[i] = start_size;
    db_commit_strings[i] = malloc (start_size);
    setup_conn (&conns[i]);

    PGresult* res = PQexec (conns[i], "BEGIN;");
    PQclear (res);
  }

#pragma omp parallel for
  for (uint32_t i = 0; i != rows; i++)
  {

    uint64_t totalSize = 0;

    int thread_id = omp_get_thread_num ();
    struct OccurenceResult current = output[i];

    uint32_t* input = current.input_data;

    bool reading_kanji = false;

    int endPos = 0;
    uint32_t* startPos = 0;

    char** writeBuf = &sql_execs[thread_id];
    uint32_t* max_length = &t_length[thread_id];

    sequence_count_t* map = &sequences[thread_id];
    sequence_count_init (*map);

    for (uint32_t* readPos = input; *readPos != 0; readPos++)
    {
      uint32_t c = *readPos;
      bool kanji = is_chinese_char (c);
      if (kanji && reading_kanji)
      {
        // printf ("%s\n", c_utf32_char_to_utf8_char (c).buf);
      }
      else if (!kanji && reading_kanji)
      {
        reading_kanji = false;

        uint8_t length = readPos - startPos;

        if (length == 1)
        {
          continue;
        }

        if (length >= 2)
        {
          for (int i = 0; i != length; i++)
          {

            get_all_permutations (startPos, length, i, writeBuf, map);
          }
        }
      }
      else if (kanji & !reading_kanji)
      {

        reading_kanji = true;
        startPos = readPos;
      }
    }

    struct sequence_count_it_s it;

    uint64_t length = 0;
    uint64_t count = 0;
    for (sequence_count_it (&it, *map); !sequence_count_end_p (&it);
         sequence_count_next (&it))

    {
      sequence_count_itref_t* pair = sequence_count_ref (&it);
      length += string_size (pair->key);
      count++;
      // printf ("%s, %d\n", string_get_cstr (pair->key),
      // pair->value);
    }
    length += count * 80;
    if (length >= t_commit_length[thread_id])
    {
      int desiredLength = length * 2;
      db_commit_strings[thread_id] =
        realloc (db_commit_strings[thread_id], desiredLength);

      t_commit_length[thread_id] = desiredLength;
    }

    int writePos = 0;
    char* outBuf = db_commit_strings[thread_id];
    for (sequence_count_it (&it, *map); !sequence_count_end_p (&it);
         sequence_count_next (&it))

    {

      sequence_count_itref_t* pair = sequence_count_ref (&it);

      int writeLen =
        sprintf (outBuf + writePos,
                 "CALL Insert_Into_KanjiSequences(%s, %d, %d, %d);",
                 string_get_cstr (pair->key),
                 pair->value,
                 current.origin,
                 current.id);

      writePos += writeLen;

      // printf ("%s, %d\n", string_get_cstr (pair->key),
      // pair->value);
    }

    if (count == 0)
      continue;

    outBuf[writePos] = 0;
    PGresult* res = PQexec (conns[thread_id], outBuf);

    if (PQresultStatus (res) != PGRES_COMMAND_OK)
    {
      fprintf (stderr,
               "INSERT failed: %s",
               PQerrorMessage (conns[thread_id]));
      printf ("%s\n", outBuf);
      PQclear (res);
      exit (1);
    }

    PQclear (res);

    sequence_count_reset (*map);
  }

  uint64_t totalSize2 = 0;

  printf ("totalSize2 %ld\n", totalSize2);

  for (int i = 0; i != num_threads; i++)
  {

    PGresult* res = PQexec (conns[i], "COMMIT;");
    PQclear (res);
  }
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

  PQfinish (conn);
  return 0;
}
