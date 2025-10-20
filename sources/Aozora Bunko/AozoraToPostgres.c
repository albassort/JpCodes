#include <sqlite3.h>
#include <postgresql/libpq-fe.h>
#include <stdio.h>
int
main ()
{
  PGconn* conn = PQconnectdb (
    "host=localhost dbname=jpcodes user=jpcodes password=JpCodes");

  if (PQstatus (conn) != CONNECTION_OK)
  {
    fprintf (stderr,
             "Failed to conenct to databsae. %s",
             PQerrorMessage (conn));
    PQfinish (conn);
    return 1;
  }

  sqlite3* db;
  int rc;
  rc = sqlite3_open ("./aozora.db", &db);
  if (rc)
  {
    printf ("%s\n", sqlite3_errmsg (db));
    return 1;
  }

  FILE* q = fopen ("./topostgres.sql", "r");
  fseek (q, 0, SEEK_END);
  int size = ftell (q);
  rewind (q);
  char query[size + 1];
  fread (query, 1, size, q);
  query[size] = 0;
  printf ("%s", query);
  sqlite3_stmt* stmt;

  PQexec (conn, "Begin;");
  sqlite3_prepare_v2 (db, query, size, &stmt, 0);
  while (sqlite3_step (stmt) == SQLITE_ROW)
  {
    const unsigned char* id = sqlite3_column_text (stmt, 0);

    const unsigned char* dif = sqlite3_column_text (stmt, 1);
    const unsigned char* year = sqlite3_column_text (stmt, 2);
    const unsigned char* name = sqlite3_column_text (stmt, 3);
    const unsigned char* body = sqlite3_column_text (stmt, 4);

    const unsigned char* params[5] = { name, id, dif, body, year };

    printf ("%s\n", id);
    printf ("%s\n", name);

    PGresult* res = PQexecParams (
      conn,
      "INSERT INTO training_data.AozoraBooks (aozoraName, aozoraId, "
      "difficulty, data, year) "
      "VALUES ($1::text, $2::decimal, $3::decimal, $4::text, "
      "$5::int);",
      5,
      NULL,
      (const char* const*) params,
      NULL,
      NULL,
      0);

    if (PQresultStatus (res) != PGRES_COMMAND_OK)
    {
      fprintf (stderr, "INSERT failed: %s", PQerrorMessage (conn));
      PQclear (res);
      PQfinish (conn);
      return 1;
    }
  }

  PQexec (conn, "COMMIT;");
}
