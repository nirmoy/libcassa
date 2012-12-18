#include <python2.7/Python.h>


 PyObject *mainModule;	


 int cassa_init(void);
 PyObject* importModule(char * modulename);
 int ImportAll(char * modulename);
 void printPyDict(PyObject *dict);
 int pythonDictAdditems(PyObject *dict,char *key,long value);
 typedef struct connectionID {
  
	PyObject *insert;        //for inserting into cassandra
	PyObject *get;
	PyObject *multiget;
	PyObject *remove;
	PyObject *pool;
	PyObject *ConnectionPool;
	PyObject *columnfamily;
	PyObject *cfFunct;
	PyObject *getcount;
}connID_t;


typedef struct cassaInput {
	
	char **serverIP;
	char *keyspace;
	char *columnfamily;
	int  totalserver;  
}cassa_option;

typedef struct SystemManager {
	PyObject *systemManager;
	PyObject *createKeyspace;
	PyObject *createColumnFamily;
	PyObject *dropColumnFamily;
	PyObject *dropKeyspace;
}cassaManager;