#ifndef LIBCASSA_H
#define LIBCASSA_H

#include <stdio.h>
#include <stdlib.h>
#include "python_api.h"
#define SIMPLE_STRATEGY                  PyObject_GetAttrString(mainModule, "SIMPLE_STRATEGY")
#define NETWORK_TOPOLOGY_STRATEGY	  PyObject_GetAttrString(mainModule, "NETWORK_TOPOLOGY_STRATEGY")
#define OLD_NETWORK_TOPOLOGY_STRATEGY    PyObject_GetAttrString(mainModule, "OLD_NETWORK_TOPOLOGY_STRATEGY")


typedef struct dictionary {
  char *key;
  char *value;
}dict_t;

typedef struct result {
  dict_t **dict;
  int    total_Key;
  
}result_t;

int cassa_close(connID_t *connid);
int cassa_init(void);
int cassa_shutdown(void);
connID_t * cassa_connect(cassa_option conn);
int cassa_close(connID_t *connid);
result_t *cassa_get(connID_t *connid, char *row, char *col_start, char*col_end, int count);
int cassa_insert(connID_t *connid,char *rowkey, result_t *result);
int
cassa_create_keyspace (cassaManager *CM,
		       char *keyspace_name,
		       char *strategy, int replication_factor);
int
cassa_create_columnfamily (cassaManager *CM,
		       char *keyspace_name,
		       char *columnfamily);

int cassa_drop_keyspace(cassaManager *CM, char *keyspace);
int
cassa_drop_columnfamily (cassaManager *CM,
		       char *keyspace_name,
		       char *columnfamily);
cassaManager* cassa_initSysManager(char *serverIp_port);
int cassa_shutdownSysManager(cassaManager *CM);


int dict_FromPyDict(PyObject *src, result_t *dst);
int dict_SetItem(result_t *result, char *value, char *key);
PyObject * dict_AsPyDict(result_t *result);
void print_dict(result_t *dict);
int dict_setitem(result_t *result, char *key, char *value);
result_t *dict_New(int size);
int dict_next(result_t *dict, int *pos, dict_t *item);
#endif
