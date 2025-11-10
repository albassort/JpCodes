#include <iso646.h>
#include <stdint.h>
#include <m-dict.h>
#include <m-string.h>
#include <arpa/inet.h>
#include <omp.h>
#include <utf32.h>
#include "shared.h"

struct Sequence
{
  uint32_t first;
  uint32_t middle;
  uint32_t third;
};

uint64_t
FNV1a (const uint8_t* data, size_t dataLen)
{
  uint64_t hash = 14695981039346656037ull;
  for (size_t i = 0; i < dataLen; i++)
  {
    hash ^= data[i];
    hash *= 1099511628211ull;
  }
  return hash;
}

uint64_t
hash_sequence (const struct Sequence s)
{

  uint32_t packed[3];
  packed[0] = s.first;
  packed[1] = s.middle;
  packed[2] = s.third;
  return FNV1a ((uint8_t*) &packed[0], 12);
};

bool
eq_sequence (const struct Sequence s, const struct Sequence s1)
{
  // should vectorize well
  bool first = s.first == s1.first;
  bool second = s.middle == s1.middle;
  bool third = s.third == s1.third;
  return (first && second && third);
};

#define HASHABLE_OPLIST (HASH (hash_sequence), EQUAL (eq_sequence))

DICT_DEF2 (character_sequences,
           struct Sequence,
           HASHABLE_OPLIST,
           uint32_t,
           M_BASIC_OPLIST);
;

void
commit_occuerences (int rows)
{

  int num_threads = omp_get_max_threads ();

  char* sql_execs[num_threads][NUMBER_OF_SOURCES];
  uint32_t t_commit_length[num_threads][NUMBER_OF_SOURCES];
  uint32_t t_write_pos[num_threads][NUMBER_OF_SOURCES];
  bool has_data[num_threads][NUMBER_OF_SOURCES];

  char* base_strs[NUMBER_OF_SOURCES];

  character_sequences_t sequences[num_threads];

  PGconn* conns[num_threads];
  int commit_counter[num_threads];

  static const int start_size = 8192;

  int commit_interval = 64;

  for (int i2 = 0; NUMBER_OF_SOURCES - 1 >= i2; i2++)
  {
    base_strs[i2] = malloc (1024);
    sprintf (base_strs[i2],
             "INSERT INTO training_data.characterSequences"
             "(FIRST, middle, third, count, %s) values ",
             sources[i2]);
  }

  for (int i = 0; i != num_threads; i++)
  {

    for (int i2 = 0; NUMBER_OF_SOURCES - 1 >= i2; i2++)
    {

      sql_execs[i][i2] = malloc (start_size);
      int length = sprintf (sql_execs[i][i2], "%s", base_strs[i2]);

      t_commit_length[i][i2] = start_size;
      t_write_pos[i][i2] = length;
      has_data[i][i2] = false;
    }

    character_sequences_init (sequences[i]);
    character_sequences_reserve (sequences[i], start_size);

    setup_conn (&conns[i]);

    commit_counter[i] = 0;
  }

  printf ("A\n");

#pragma omp parallel for
  for (uint32_t i = 0; i < rows; i++)
  {
    PGresult* res;
    int thread_id = omp_get_thread_num ();
    int* counter = &commit_counter[thread_id];

    uint32_t* max_length = t_commit_length[thread_id];
    uint32_t* write_pos = t_write_pos[thread_id];
    char** write_buf = sql_execs[thread_id];

    if (*counter >= commit_interval)
    {
      for (int i = 0; NUMBER_OF_SOURCES - 1 >= i; i++)
      {
        if (!has_data[thread_id][i])
          continue;

        char* pos = write_buf[i] + write_pos[i];
        pos[0] = 0;
        pos[-1] = ';';

        printf ("%s\n", write_buf[i]);
        res = PQexec (conns[thread_id], write_buf[i]);
        handle_query_outcome (&conns[thread_id], &res);
        int length = sprintf (write_buf[i], "%s", base_strs[i]);

        has_data[thread_id][i] = false;
        write_pos[i] = length;
      }
      *counter = 0;
    }

    character_sequences_t* map = &sequences[thread_id];

    struct OccurenceResult current = output[i];
    uint32_t* body = current.input_data;

    for (uint32_t* read_pos = body + 1; read_pos[1] != 0; read_pos++)
    {
      struct Sequence seq;
      seq.first = read_pos[-1];
      seq.middle = read_pos[0];
      seq.third = read_pos[1];

      bool is_valid =
        (is_kana (seq.first) || is_chinese_char (seq.first)) &&
        (is_kana (seq.middle) || is_chinese_char (seq.middle)) &&
        (is_kana (seq.third) || is_chinese_char (seq.third));
      if (!is_valid)
      {
        continue;
      }

      uint32_t* num = character_sequences_safe_get (*map, seq);
      uint32_t cnum = num ? *num + 1 : 1;
      character_sequences_set_at (*map, seq, cnum);
    }

    struct character_sequences_it_s it;

    for (character_sequences_it (&it, *map);
         !character_sequences_end_p (&it);
         character_sequences_next (&it))
    {

      int estimatedSize =
        50 /*template size*/ + 6 * 10; /*6 10 digit integers*/

      if (write_pos[current.origin] + estimatedSize >
          max_length[current.origin])
      {
        printf ("thread id: %d - old max length: %d\n",
                thread_id,
                max_length[current.origin]);

        write_buf[current.origin] = realloc (
          write_buf[current.origin], max_length[current.origin] * 2);

        max_length[current.origin] *= 2;

        printf ("thread id: %d - new max length: %d\n",
                thread_id,
                max_length[current.origin]);
      }

      character_sequences_itref_t* pair =
        character_sequences_ref (&it);

      struct Sequence seq = pair->key;
      uint32_t size = pair->value;

      printf ("thread id: %d - max length: %d - write pos: %d\n",
              thread_id,
              max_length[current.origin],
              write_pos[current.origin]);

      int length_written = sprintf (write_buf[current.origin] +
                                      write_pos[current.origin],
                                    "(%d, %d, %d, %d, %d),",
                                    seq.first,
                                    seq.middle,
                                    seq.third,
                                    size,
                                    current.id);

      has_data[thread_id][current.origin] = true;
      write_pos[current.origin] += length_written;
    }

    character_sequences_reset (*map);

    printf ("%d\n", *counter);
    (*counter)++;
  }

  uint64_t totalSize2 = 0;

  printf ("totalSize2 %ld\n", totalSize2);

#pragma omp parallel for
  for (int i = 0; i != num_threads; i++)
  {

    for (int i2 = 0; NUMBER_OF_SOURCES - 1 >= i2; i2++)
    {

      printf ("thread: %d; source: %d\n", i, i2);
      if (!has_data[i][i2])
        continue;

      char* write_pos = sql_execs[i][i2] + t_write_pos[i][i2];
      write_pos[0] = 0;
      write_pos[-1] = ';';

      printf ("commited!: %s\n", sql_execs[i][i2]);

      PGresult* res = PQexec (conns[i], sql_execs[i][i2]);

      handle_query_outcome (&conns[i], &res);

      PQclear (res);
      free (sql_execs[i][i2]);
      PQfinish (conns[i]);
    }

    character_sequences_t* map = &sequences[i];
    character_sequences_clear (*map);
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

  commit_occuerences (rows);

  free (input_data_global);
  free (output);
  PQfinish (conn);
  return 0;
}
