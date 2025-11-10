#include <stdbool.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#define _XOPEN_SOURCE 500
#include <dirent.h>
#include <m-array.h>
#include <postgresql/libpq-fe.h>
#include <glib.h>

int
read_until_newline (char* str, bool* atEnd)
{
  char c = 1;
  int i = 0;
  while (true)
  {
    c = str[i++];
    if (c == '\n')
    {
      *atEnd = false;
      return i;
    }
    else if (c == 0)
    {
      *atEnd = true;
      return i - 1;
    }
  }
}

int
parseSrt (char** inputBuf, char** output, char** lb, int buflen)
{
  int pos = 0;
  char swap = 0;
  char* writePos = *output;

  bool atEnd = false;
  while (true)
  {
    char* current = *inputBuf + pos;
    int c = read_until_newline (current, &atEnd);

    if (c == 0 && buflen > pos)
    {
      pos += 1;
      continue;
    }
    swap = current[c];
    current[c] = 0;

    bool isDiigt = atoi (current);
    bool isTStamp = strstr (current, "-->");
    bool isTooSmall = 3 > strlen (current);

    bool wrote = false;
    if (!isDiigt && !isTStamp && !isTooSmall)
    {
      const gchar* sPos = current;
      while (*sPos)
      {
        gunichar c = g_utf8_get_char_validated (sPos, -1);
        if (c == (gunichar) -1 || c == (gunichar) -2)
        {
          fprintf (stderr, "Invalid UTF-8 sequence\n");
          sPos += 1;
          printf ("continuing\n");
          continue;
        }

        if ((c >= 0x3040 && 0x30FF >= c) ||
            (c >= 0x4E00 && 0x9FFF >= c))
        {
          char utf8Buf[3];
          g_unichar_to_utf8 (c, utf8Buf);
          memcpy (writePos, utf8Buf, 3);
          writePos += 3;
          if (!wrote)
          {
            wrote = true;
          }
        }

        sPos = g_utf8_next_char (sPos);
      }
    }

    if (wrote)
    {
      writePos[0] = ' ';
      writePos++;
    }

    current[c] = swap;
    pos += c;

    if (pos >= buflen)
    {
      break;
    }
  }

  writePos[0] = 0;
}

ARRAY_DEF (paths, char*);

void
add_files (const char* dir_path, paths_t* paths)
{
  DIR* dir = opendir (dir_path);
  if (!dir)
  {
    perror (dir_path);
    printf ("PATH NOT FOUND %s\n", dir_path);
    return;
  }
  struct dirent* entry;
  while ((entry = readdir (dir)) != NULL)
  {
    if (entry->d_type == DT_DIR)
    {
      if (strcmp (entry->d_name, ".") == 0)
      {
        continue;
      }
      if (strcmp (entry->d_name, "..") == 0)
      {
        continue;
      }

      char* buf;
      char* absolutePath = malloc (4096);

      asprintf (&buf, "%s/%s", dir_path, entry->d_name);
      realpath (buf, absolutePath);
      add_files (absolutePath, paths);

      free (buf);
      free (absolutePath);
    }
    else
    {
      char* d_name = entry->d_name;

      int length = strlen (d_name);
      printf ("%s\n", d_name + length - 4);

      if (length < 4)
        continue;

      if (strcmp (d_name + length - 4, ".srt") != 0)
        continue;

      char* buf;
      asprintf (&buf, "%s/%s", dir_path, entry->d_name);
      paths_push_back (*paths, buf);
    }
  }
  closedir (dir);
}

int
main (void)
{
  paths_t a;
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

  // Simple INSERT
  paths_init (a);
  add_files ("../sources/subtitles/subtitles", &a);
  int size = paths_size (a);
  uint64_t totalLength = 0;
  PQexec (conn, "Begin;");

  for (int i = 0; i != size; i++)
  {
    char** path = paths_get (a, i);
    // printf ("%s\n", *path);
    FILE* file = fopen (*path, "r");
    fseek (file, 0, SEEK_END);
    int length = ftell (file);
    rewind (file);
    char* data = malloc (length + 1);
    fread (data, 1, length, file);

    totalLength += length;

    char* output = malloc (length + 1);
    data[length] = 0;

    char* lineBuf = malloc (length + 1);

    parseSrt (&data, &output, &lineBuf, length);
    fclose (file);

    char* sub;
    char* name = strrchr (*path, '/') + 1;

    if (name)
    {
      // printf ("%s\n", output);

      printf ("%s\n", output);
      printf ("%s\n", *path);
    }

    char* params[2] = { output, *path };

    PGresult* res = PQexecParams (
      conn,
      "INSERT INTO training_data.SubtitleData (data, path) "
      "VALUES ($1::text, $2::text)",
      2,
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

    free (data);
    free (lineBuf);
    free (output);
  }

  PQexec (conn, "COMMIT;");
  printf ("total size : %ld \n", totalLength);

  return 0;
}
