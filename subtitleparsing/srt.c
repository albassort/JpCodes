#include <stdbool.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#define _XOPEN_SOURCE 500
#include <dirent.h>
#include <m-array.h>

int
read_until_newline (char** str)
{
  char c = 1;
  int i = 0;
  while (true)
  {
    c = (*str)[i++];
    if (c == '\n' || c == 0)
    {
      return i;
    }
  }
}
int
parseSrt (char** inputBuf, char** output, char** lb, int buflen)
{
  int pos = 0;
  char* currentBuf = *inputBuf + 1;
  int c;
  char* outp = *output;
  int outputPos = 0;
  char* lineBuf = *lb;
  while ((c = read_until_newline (&currentBuf)) != 0)
  {
    pos = (pos + c) + 1;

    if (pos >= buflen)
    {
      break;
    }

    if (c > 2)
    {
      memcpy (lineBuf, currentBuf - 1, c);

      lineBuf[c] = 0;
      for (int i = 0; i != c; i++)
      {
        if (lineBuf[i] == '\n' || lineBuf[i] == '\r')
        {
          lineBuf[i] = ' ';
        }
      }
      if (!strstr (lineBuf, "-->"))
      {
        int i = 0;
        if (!atoi (lineBuf) && c > 1)
        {
          // for (i = 0; i != c + 1; i++)
          // {
          //   printf ("%02x", (unsigned char) lineBuf[i]);
          // }
          // printf ("\n");

          // printf (
          //   "writing %d, size %ld\n", outputPos, strlen (lineBuf));
          // printf ("%d\n", c);
          int size = sprintf (&(*output)[outputPos], "%s ", lineBuf);
          outputPos += size;
        }
      }
    }

    currentBuf = &((*inputBuf)[pos]);
  }
  if (outputPos >> buflen)
  {
    (*output)[buflen] = 0;
  }
  else
  {

    (*output)[outputPos] = 0;
  }

  // for (int i = 0; i != outputPos + 1; i++)
  // {
  //   printf ("%02x", (unsigned char) (*output)[i]);
  // }
  // printf ("%s\n", *output);
  return 1;
}

int
main2 ()
{
  const char** got =
    "../sources/subtitles/subtitles/薄桜記/шЦДцбЬшиШя╝Г05.srt";
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
      asprintf (&buf, "%s/%s", dir_path, entry->d_name);
      add_files (buf, paths);
      free (buf);
    }
    else
    {
      if (!strstr (entry->d_name, "srt"))
      {

        continue;
      }
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
  paths_init (a);
  add_files ("../sources/subtitles/subtitles", &a);
  int size = paths_size (a);
  for (int i = 0; i != size; i++)
  {
    char** got = paths_get (a, i);
    // printf ("%s\n", *got);
    FILE* file = fopen (*got, "r");
    fseek (file, 0, SEEK_END);
    int length = ftell (file);
    rewind (file);
    char* data = malloc (length + 1);
    fread (data, 1, length, file);

    char* output = malloc (length + 1);
    data[length] = 0;

    char* lineBuf = malloc (length + 1);
    data[length] = 0;

    parseSrt (&data, &output, &lineBuf, length);
    fclose (file);

    char* sub;
    char* name = strrchr (*got, '/') + 1;

    if (name)
    {

      // printf ("%s\n", output);
      asprintf (&sub, "./output/%s.txt", name);
      printf ("%s\n", sub);
    }

    free (data);
    free (lineBuf);
    free (output);
  }
  printf ("%d\n", size);
  return 0;
}
