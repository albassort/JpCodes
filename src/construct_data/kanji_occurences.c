#include <stdint.h>
#include <utf32.h>
#include <m-dict.h>
#include <m-string.h>
#include <postgresql/libpq-fe.h>
#include <arpa/inet.h>

DICT_DEF2 (character_count, uint32_t, uint32_t);

struct OccurenceResult
{
  uint32_t id;
  uint32_t origin;
  character_count_t result;
  char* input_data;
};

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
worker ()
{
  PGconn* conn;
  setup_conn (&conn);

  PQexec (conn, "SET max_parallel_workers_per_gather = 12;");

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
    current->input_data = malloc (strlen (data) + 1);
    strcpy (current->input_data, data);
  }

  PQclear (res);
  printf ("%d\n", output[0].id);
  return 0;
}

int
main ()
{
  PGconn* conn;
  setup_conn (&conn);
  worker ();
  return 0;
}
