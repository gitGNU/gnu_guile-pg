#include <stdio.h>
#include "libpq-fe.h"
#include "libpq/libpq-fs.h"

void exec_cmd(PGconn *conn, char *str);

main (int argc, char *argv[])
{
   PGconn *conn;
   Oid lobjId;
   int lobj_fd;
   char buf[256];
   int i, ret;

   conn = PQsetdb(NULL, NULL, NULL, NULL, NULL);
   if (PQstatus(conn) != CONNECTION_OK) {
      fprintf(stderr, "Can't connect to backend.\n");
      fprintf(stderr, "ERROR: %s\n", PQerrorMessage(conn));
      exit(1);
   }
   PQtrace (conn, stdout);
   exec_cmd(conn, "BEGIN");
   if ((lobjId = lo_creat(conn, INV_READ|INV_WRITE)) == 0) {
      fprintf(stderr, "can't create large object.\n");
      exit(1);
   }
   if ((lobj_fd = lo_open(conn, lobjId, INV_WRITE)) < 0) {
      fprintf(stderr, "Can't open large object.\n");
      fprintf(stderr, "Error message: %s\n", PQerrorMessage(conn));
      exit(1);
   }
   for (i = 0; i < 5; i++) {
      sprintf(buf, "Test Line %d\n", i);
      ret = lo_write(conn, lobj_fd, buf, strlen(buf));
      if (ret < strlen(buf)) {
         fprintf(stderr, "error (%d) while reading large object.\n", ret);
      }
   }
   ret = lo_close(conn, lobj_fd);
   printf("lo_close returned %d.\n", ret);
   if (ret) {
      fprintf(stderr, "Error message: %s\n", PQerrorMessage(conn));
      exit(1);   
   }
   exec_cmd(conn, "END");
   exec_cmd(conn, "BEGIN");
   if ((lobj_fd = lo_open(conn, lobjId, INV_WRITE)) < 0) {
      fprintf(stderr, "Can't open large object.\n");
      fprintf(stderr, "Error message: %s\n", PQerrorMessage(conn));
      exit(1);
   }
   if ((ret = lo_lseek(conn, lobj_fd, 12, 0)) != 12) {
      fprintf(stderr, "Can't seek into large object.\n");
      fprintf(stderr, "Error message: %s\n", PQerrorMessage(conn));
      exit(1);
   }
   if ((ret = lo_write(conn, lobj_fd, "X\x09", 1)) != 1) {
      fprintf(stderr, "Can't write large object.\n");
      fprintf(stderr, "Error message: %s\n", PQerrorMessage(conn));
      exit(1);
   }
   ret = lo_close(conn, lobj_fd);
   if (ret) {
      fprintf(stderr, "Error message: %s\n", PQerrorMessage(conn));
      exit(1);   
   }
   exec_cmd(conn, "END");
   exec_cmd(conn, "BEGIN");
   if (lo_export(conn, lobjId, "test.out") != 1) {
      fprintf(stderr, "Export failed.\n");
      fprintf(stderr, "Error message: %s\n", PQerrorMessage(conn));
      exit(1);   
   }
   exec_cmd(conn, "END");
   PQuntrace (conn);
   PQfinish(conn);
   exit(0);
}


void exec_cmd(PGconn *conn, char *str)
{
   PGresult *res;

   if ((res = PQexec(conn, str)) == NULL) {
      fprintf(stderr, "Error executing %s.\n", str);
      fprintf(stderr, "Error message: %s\n", PQerrorMessage(conn));
      exit(1);
   }
   if (PQresultStatus(res) != PGRES_COMMAND_OK) {
      fprintf(stderr, "Error executing %s.\n", str);
      fprintf(stderr, "Error message: %s\n", PQerrorMessage(conn));
      PQclear(res);
      exit(1);
   }
   PQclear(res);
}
