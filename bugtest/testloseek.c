#include <stdio.h>
#include "libpq-fe.h"
#include "libpq/libpq-fs.h"

void exec_cmd(PGconn *conn, char *str);

main (int argc, char *argv[])
{
   PGconn *conn;
   int lobj_fd;
   char buf[256];
   int ret, i;
   Oid lobj_id;

   conn = PQconnectdb("dbname=test");
   if (PQstatus(conn) != CONNECTION_OK) {
      fprintf(stderr, "Can't connect to backend.\n");
      fprintf(stderr, "ERROR: %s\n", PQerrorMessage(conn));
      exit(1);
   }
   exec_cmd(conn, "BEGIN TRANSACTION");
   PQtrace (conn, stdout);
   if ((lobj_id = lo_creat(conn, INV_READ | INV_WRITE)) < 0) {
      fprintf(stderr, "Can't create lobj.\n");
      fprintf(stderr, "ERROR: %s\n", PQerrorMessage(conn));
      exit(1);
   }
   fprintf(stderr, "lo_creat() returned OID %ld.\n", lobj_id);
   if ((lobj_fd = lo_open(conn, lobj_id, INV_READ | INV_WRITE)) < 0) {
      fprintf(stderr, "Can't open lobj.\n");
      fprintf(stderr, "ERROR: %s\n", PQerrorMessage(conn));
      exit(1);
   }
   fprintf(stderr, "lo_open returned fd = %d.\n", lobj_fd);
   if ((ret = lo_write(conn, lobj_fd, "aaaaaa", 6)) != 6) {
      fprintf(stderr, "Can't write lobj.\n");
      fprintf(stderr, "ERROR: %s\n", PQerrorMessage(conn));
      exit(1);
   }
   ret = lo_close(conn, lobj_fd);
   printf("lo_close returned %d.\n", ret);
   exec_cmd(conn, "END TRANSACTION");

   exec_cmd(conn, "BEGIN TRANSACTION");
   if ((lobj_fd = lo_open(conn, lobj_id, INV_READ | INV_WRITE)) < 0) {
      fprintf(stderr, "Can't open lobj.\n");
      fprintf(stderr, "ERROR: %s\n", PQerrorMessage(conn));
      exit(1);
   }
   fprintf(stderr, "lo_open returned fd = %d.\n", lobj_fd);
   if (ret)
      fprintf(stderr, "Error message: %s\n", PQerrorMessage(conn));
   if ((ret = lo_lseek(conn, lobj_fd, 1, 0)) != 1) {
      fprintf(stderr, "error (%d) lseeking in large object.\n", ret);
      fprintf(stderr, "ERROR: %s\n", PQerrorMessage(conn));
      exit(1);
   }
   if ((ret = lo_write(conn, lobj_fd, "b", 1)) != 1) {
      fprintf(stderr, "Can't write lobj.\n");
      fprintf(stderr, "ERROR: %s\n", PQerrorMessage(conn));
      exit(1);
   }
   if ((ret = lo_lseek(conn, lobj_fd, 3, 0)) != 3) {
      fprintf(stderr, "error (%d) lseeking in large object.\n", ret);
      fprintf(stderr, "ERROR: %s\n", PQerrorMessage(conn));
      exit(1);
   }
   if ((ret = lo_write(conn, lobj_fd, "b", 1)) != 1) {
      fprintf(stderr, "Can't write lobj.\n");
      fprintf(stderr, "ERROR: %s\n", PQerrorMessage(conn));
      exit(1);
   }
   ret = lo_close(conn, lobj_fd);
   printf("lo_close returned %d.\n", ret);
   if (ret)
      fprintf(stderr, "Error message: %s\n", PQerrorMessage(conn));
   PQuntrace(conn);
   exec_cmd(conn, "END TRANSACTION");

   exec_cmd(conn, "BEGIN TRANSACTION");
   ret = lo_export(conn, lobj_id, "testloseek.c.lobj");
   printf("lo_export returned %d.\n", ret);
   if (ret != 1)
      fprintf(stderr, "Error message: %s\n", PQerrorMessage(conn));
   exec_cmd(conn, "END TRANSACTION");
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
