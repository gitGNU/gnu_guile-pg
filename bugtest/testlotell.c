#include <stdio.h>
#include "libpq-fe.h"
#include "libpq/libpq-fs.h"

main (int argc, char *argv[])
{
   PGconn *conn;
   int lobj_fd;
   char buf[256];
   int ret;

   conn = PQsetdb(NULL, NULL, NULL, NULL, NULL);
   if (PQstatus(conn) != CONNECTION_OK) {
      fprintf(stderr, "Can't connect to backend.\n");
      fprintf(stderr, "ERROR: %s\n", PQerrorMessage(conn));
      exit(1);
   }
   if ((lobj_fd = lo_open(conn, 20321, INV_READ)) < 0) {
      fprintf(stderr, "Can't open lobj.\n");
      fprintf(stderr, "ERROR: %s\n", PQerrorMessage(conn));
      exit(1);
   }
   fprintf(stderr, "lo_open returned fd = %d.\n", lobj_fd);
   ret = lo_tell(conn, lobj_fd);
   fprintf(stderr, "lo_tell returned %d.\n", ret);
   if (ret < 0)
      fprintf(stderr, "ERROR: %s\n", PQerrorMessage(conn));
   ret = lo_read(conn, lobj_fd, buf, 2);
   if (ret != 2) {
      fprintf(stderr, "error (%d) while reading large object.\n", ret);
      fprintf(stderr, "ERROR: %s\n", PQerrorMessage(conn));
   } else
      fprintf(stderr, "Read 2 bytes from lobj.\n", ret);
   ret = lo_tell(conn, lobj_fd);
   fprintf(stderr, "lo_tell returned %d.\n", ret);
   if (ret < 0)
      fprintf(stderr, "ERROR: %s\n", PQerrorMessage(conn));
   ret = lo_close(conn, lobj_fd);
   printf("lo_close returned %d.\n", ret);
   if (ret)
      fprintf(stderr, "Error message: %s\n", PQerrorMessage(conn));
   exit(0);
}
