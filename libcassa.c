#include "libcassa.h"
#include <assert.h>
 
    
#define ERRLOGGER(msg) \
    fprintf(stderr, "%s", msg);\
    fprintf(stderr, "ERR: %s:%d ", __FILE__, __LINE__); 

static int IsActiveCassa = 0;  //Flag whether libcassa is activated or not

/*********************************************************
this method converts result * from PyObject *
pycassa returns dictionary data structure for columns in cassandra
we need to convert c data structure for processing
data.

keylist = mydict.keys()
keylist.sort()
for key in keylist:
    print "%s: %s" % (key, mydict[key])
**********************************************************/
int
dict_FromPyDict (PyObject * src, result_t * dst)
{
//solve the malloc issue
  if (!PyDict_Check (src) || !dst) //check for valid arguments
    {
      ERRLOGGER("pythonAPI::printPyDict()::Invalid arguments\n");
      return -1;
    }
  int i     = 0;
  int count = 0;
  PyObject *keys    = PyObject_GetAttrString(src, "keys");
  PyObject *keylist = PyObject_CallObject(keys, NULL);
  Py_XDECREF(keys);
  PyObject_CallMethod(keylist, "sort", NULL);
  
  dst->dict = (dict_t **) malloc (sizeof (dict_t *) * PyDict_Size (src));
  dst->total_Key = PyDict_Size (src);
  
  if (!dst->dict)
    {
      ERRLOGGER("libcassa:dict_asPyDict():low memory\n");
      return -1;
    }
  count=dst->total_Key;
  while (count--)
    {
      if (i <= dst->total_Key) {
          dst->dict[i] = malloc (sizeof (dict_t));
          dst->dict[i]->key = strdup (PyString_AsString(PyList_GetItem(keylist, 
					(Py_ssize_t) dst->total_Key - count -1)));
          dst->dict[i++]->value = strdup (PyString_AsString (PyDict_GetItem(src,
			      PyList_GetItem(keylist,
					      (Py_ssize_t) dst->total_Key - count -1))));
      }
	
      }
  Py_XDECREF(keylist);
  //print_dict (dst);
  
  return 0;
}
/************************************************************************
args::c dictionary datastructure that is result *

return value:: A Python dictionary object

detail::it converts Python dictionary data structure to c dictionary
*************************************************************************/

PyObject *
dict_AsPyDict (result_t * result)
{

  if ( !result ) {
     ERRLOGGER("dict_AsPyDict : INVALID argument\n");
     return NULL;
  }

  PyObject *py_dict = PyDict_New ();
  int i = 0;

  for (i = 0; i < result->total_Key; i++)
    {
      PyObject *key = PyString_FromString (result->dict[i]->key);
      PyObject *value = PyString_FromString (result->dict[i]->value);
      PyDict_SetItem (py_dict, key, value);
      Py_XDECREF (key);
      Py_XDECREF (value);
    }

  return py_dict;
}
/************************************************************************/
int dict_setitem(result_t *result, char *key, char *value)
{
if ( !result )
	return -1;
result->dict[result->total_Key] = malloc (sizeof (dict_t));
result->dict[result->total_Key]->key = strdup (key);
result->dict[(result->total_Key)++]->value = strdup (value);
return 0;
}

result_t *dict_New(int size)
{
result_t *dict  = malloc(sizeof(result_t));
dict->total_Key = 0;
if (dict) {
	dict->dict   = (dict_t **) malloc (sizeof(dict_t *) * size);
	return dict;
}
return NULL;
}

int dict_Distroy(result_t *dict)
{

return 0;
}
/*************************************************************************/
/*************************************************************************
args: c dictionary , key, value
return : boolean
detail :it add a key value pair in c dictionary 
*************************************************************************/
int
dict_SetItem (result_t * result, char *value, char *key)
{
  result->dict[result->total_Key]->key = key;
  result->dict[result->total_Key]->value = value;
  result->total_Key++;
  return 0;
}
/**************************************************************************
prints c dictionary 


**************************************************************************/

void
print_dict (result_t * dict)
{

  if (! dict) {
     ERRLOGGER("print_dict: INVALID argument\n");
     return;
  }
  int i;

  for (i = 0; i < dict->total_Key; i++)
    {
      printf ("key::%s\tvalue::%s\n", dict->dict[i]->key,
	      dict->dict[i]->value);

    }
}
/********************************************************************
cassa_init does  following tasks:
		>Initialize python Interpreter
		>Import pycassa module 
********************************************************************/

int
cassa_init (void)
{

  Py_Initialize ();

  if (Py_IsInitialized ())
    fprintf (stderr, "libcassa::Python interpreter is Initialized\n");

  mainModule = PyImport_AddModule ("__main__");
  PyObject *sysModule = PyImport_ImportModule ("sys");

  PyModule_AddObject (mainModule, "sys", sysModule);
  //ImportAll ("pycassa");//what if pycassa in not install
  if ( ImportAll ("pycassa") != 0) {
     ERRLOGGER("pycassa not installed\n");
     return -1;
  }
  return 0;
}
/***********************************************************************
detail :: its stop python interpeter

************************************************************************/
int
cassa_shutdown (void)
{
  //Py_DecRef(mainMod   ule); //check borrowed reference  
  Py_Finalize ();
  return 0;
}

/***********************************************************************
args: cassa_option(it contains keyspace and columnfamily)
returns: connID * 

detail:: cassa_connect() it initialize all function pointer of PyObject * type 
in connID so that  using connID we can talk with cassandra

***********************************************************************/

connID_t *
cassa_connect (cassa_option conn)
{
  if (!Py_IsInitialized ())
    {
      fprintf (stderr, "libcassa:call cassa_init() First\n");
      return NULL;
    }
  connID_t *connid;
  PyObject *args = NULL;
  PyObject *keyspace = PyString_FromString (conn.keyspace);

  connid = malloc (sizeof (connID_t));

  bzero (connid, sizeof (connID_t));
  connid->ConnectionPool = PyObject_GetAttrString (mainModule, 
							"ConnectionPool");
  connid->cfFunct        = PyObject_GetAttrString (mainModule, "ColumnFamily");

  if (!connid->cfFunct || !connid->ConnectionPool )
    {
      fprintf (stderr, "pycassa is not imported Check pycassa is installed \
								or not\n");
      return NULL;
    }

  args = PyTuple_New (1);
  PyTuple_SetItem (args, 0, keyspace);
  printf("\n%s\n", conn.columnfamily);
  connid->pool = PyObject_CallObject (connid->ConnectionPool, args);

  Py_DECREF (args);
  Py_DECREF (keyspace);
  if (!connid->pool) {
	ERRLOGGER("connid->loop FAIL\n");
	goto no_cassandra;
  }
  args = PyTuple_New (2);
  PyTuple_SetItem (args, 0, connid->pool);
  PyTuple_SetItem (args, 1, PyString_FromString (conn.columnfamily));

  connid->columnfamily = PyObject_CallObject (connid->cfFunct, args);

  if ( !connid->columnfamily) {
     ERRLOGGER("libcassa::cassa_connect()fail:because invalid argument or \
		cassandra is not running\n");
     Py_DECREF(connid->pool);
     goto no_cassandra;	    
   }

  if (connid->columnfamily)
    {
      connid->insert   = PyObject_GetAttrString (connid->columnfamily,
						"insert");

      connid->get      = PyObject_GetAttrString (connid->columnfamily, 
						"get");

      connid->multiget = PyObject_GetAttr (connid->columnfamily,
					   PyString_FromString ("multiget"));

      connid->remove   = PyObject_GetAttr (connid->columnfamily,
					 PyString_FromString ("remove"));
      connid->getcount   = PyObject_GetAttr (connid->columnfamily,
					 PyString_FromString ("get_count"));

    }

  return connid;

  no_cassandra: free(connid);
		ERRLOGGER("cassandra unable to connected\n");
		return NULL;
}

/**************************************************************************
cassa_close:
           decrement reference so that python garbage collector can freeup 
	   some memory
**************************************************************************/

int
cassa_close (connID_t * connid)
{
  if (connid)
    {
      Py_DECREF (connid->insert);
      Py_DECREF (connid->get);
      Py_DECREF (connid->multiget);
      Py_DECREF (connid->remove);
      Py_DECREF (connid->pool);
      Py_DECREF (connid->ConnectionPool);
      Py_DECREF (connid->columnfamily);
      free (connid);
      fprintf (stderr, "libcassa::cassa_close():successful\n");
      return 0;
    }
  fprintf (stderr, "libcasaa: got NULL in cassa_close()\n");
  return -1;
}
/*************************************************************************
args:: connection id return by cassa_connect() and row name

returns:: return c dictionary(result *) 

**************************************************************************/

result_t *
cassa_get (connID_t * connid, char *row, char *col_start, char *col_end, int count)
{

  if (!connid || !row)
    {
      fprintf (stderr, "libcassa:cassa_get fail:NUll arguments");
      return NULL;
    }
// cf.get('ID',None,'1','1349880905',0,10);
  result_t *result = malloc (sizeof (result_t));
  PyObject *py_result = NULL;
  PyObject *py_row = PyString_FromString (row);
  PyObject *args = PyTuple_New (6);

  assert (result);
  PyTuple_SetItem (args, 0, py_row);
  PyTuple_SetItem (args, 1, Py_BuildValue(""));
  PyTuple_SetItem (args, 2, PyString_FromString(col_start));
  PyTuple_SetItem (args, 3, PyString_FromString(col_end));
  PyTuple_SetItem (args, 4, PyInt_FromLong(0));
  PyTuple_SetItem (args, 5, PyInt_FromLong(count));
  assert (args);
  assert (py_row);
  assert (connid);
  if (PyCallable_Check (connid->get) &&
        (py_result = PyObject_CallObject (connid->get, args))==NULL)
  {
      fprintf (stderr, "libcassa:cassa_get() fail to retrive data%p\n",
	       py_result);
     
    }
  Py_XDECREF (py_row);
  //Py_XDECREF (args); //check cassa_close is giving sigsegv
  if (py_result)
    {
      dict_FromPyDict (py_result, result);
      Py_XDECREF (py_result);
      //printPyDict(py_result);
      //Py_DecRef(args);  check why segv fault
      return result;
    }

  return NULL;
}

/*************************************************************************
args :: connection ID return by cassa_connect
        row key "at row going  to store data"
	result_t * a c dictionary contains data to store in cassandra	
*************************************************************************/
int
cassa_insert (connID_t * connid,
	      char *rowkey,
	      result_t * result)
{

  PyObject *py_dict = dict_AsPyDict (result);
  PyObject *args = PyTuple_New (2);
  PyObject *row = PyString_FromString (rowkey);
  PyTuple_SetItem (args, 0, row);
  PyTuple_SetItem (args, 1, py_dict);
  //printf("%d\n", PyDict_Size(py_dict)); 
  if (PyCallable_Check (connid->insert)
      && PyObject_CallObject (connid->insert, args))
    {
      printf ("data inserted\n");
    }
  //Py_XDECREF (py_dict);
  //Py_DecRef(args);
  //Py_XDECREF (row);
  return 0;
}

/*************************************************************************
 * 
 * 
 * 
 * 
 * 
 * 
 * 
 * ***********************************************************************
int cassa_getcount(connID_t *connid, char *row, char *col_start, char *col_end)
{
  if (!connid || !row)
    {
      fprintf (stderr, "libcassa:cassa_get fail:NUll arguments");
      return NULL;
    }
  PyObject *py_row = PyString_FromString (row);
  PyObject *args = PyTuple_New (5);

  assert (result);
  PyTuple_SetItem (args, 0, py_row);
  PyTuple_SetItem (args, 1, Py_BuildValue(""));
  PyTuple_SetItem (args, 2, Py_BuildValue(""));
  PyTuple_SetItem (args, 3, PyString_FromString(col_start));
  PyTuple_SetItem (args, 4, PyString_FromString(col_end));
  assert (args);
  assert (py_row);
  assert (connid);
  if (PyCallable_Check (connid->get) &&
        (py_result = PyObject_CallObject (connid->get, args))==NULL)
  {
      fprintf (stderr, "libcassa:cassa_get() fail to retrive data%p\n",
	       py_result);
     
    }
  
  
  
  
  
}*/
/*************************************************************************
detail: returs a cassaManager that holds function pointer to 
implements cassandra managing operation

*************************************************************************/

cassaManager *cassa_initSysManager(char *serverIp_port)
{
  if (! IsActiveCassa)
    cassa_init();
  
  if (! (serverIp_port)) {
     ERRLOGGER("Invalid Argument\n");
     return NULL;
  }
  
  cassaManager *CM = (cassaManager *) malloc(sizeof(cassaManager));
  
  if ( !CM) {
     ERRLOGGER("libcassa:cassa_initsysManager:low memory\n");
     return NULL;
  }

  PyObject *temp = PyObject_GetAttrString (mainModule, "SystemManager");
  
  if ( !( temp && PyCallable_Check(temp) ) ) {
     ERRLOGGER("Fail to get SystemManager\n");
     return NULL;
  }
  
  PyObject *args       = PyTuple_New(1);
  PyObject *py_ip_port = PyString_FromString(serverIp_port);
  PyTuple_SetItem(args, 0, py_ip_port);
  
  CM->systemManager      = PyObject_CallObject(temp, args);
  
  if ( !(CM->systemManager)) {
     ERRLOGGER("Invalid serverIP or port\n");
     return NULL;
  }
  CM->createColumnFamily = PyObject_GetAttrString(CM->systemManager,
						 "create_column_family");
  CM->dropColumnFamily   = PyObject_GetAttrString(CM->systemManager, 
						"drop_column_family");
  CM->createKeyspace     = PyObject_GetAttrString(CM->systemManager,
						 "create_keyspace");
  CM->dropKeyspace       = PyObject_GetAttrString(CM->systemManager,
						 "drop_keyspace");
  return CM;
}
/**************************************************************************
args:CM(a cassaManager * return by cassa_initcassaSysManager)
    : keyspace_name
    : strategy("what sort of strategy 1>SimpleStrategy
				      2>NetworkTopologyStrategy 
				      3>OldNetworkTopologyStrategy")
    : replication_factor ('how many replication for each data')

It creats a keyspace for a given cassaManager
**************************************************************************/
int
cassa_create_keyspace (cassaManager *CM,
		       char *keyspace_name,
		       char *strategy,
		       int replication_factor)
{

  if ( !(CM && keyspace_name && strategy)) {
     ERRLOGGER("libcassa :cassa_create_keyspace INVALID argument\n");
     return -1;
  }
  
  PyObject *args                 = PyTuple_New (3);
  PyObject *keyspace             = PyString_FromString (keyspace_name);
  PyObject *py_strategy          = PyString_FromString (strategy);
  PyObject *pyreplication_factor = PyDict_New();
  pythonDictAdditems(pyreplication_factor, "replication_factor", 1);
  
  PyTuple_SetItem (args, 0, keyspace);
  PyTuple_SetItem (args, 1, py_strategy);
  PyTuple_SetItem (args, 2, pyreplication_factor);
  
  if (PyCallable_Check (CM->createKeyspace) &&
      PyObject_CallObject (CM->createKeyspace, args))
    {
      fprintf (stderr, "libcassa::keyspace [%s] created\n", keyspace_name);
      return 0;
    }
  fprintf (stderr, "libcassa::Fail to create keyspace [%s]\n", keyspace_name);  
  return 0;
}
/************************************************************************
args: cassaManager * CM datastructure return by cassa_sysManager()
      keyspace name to be dropped
returs: return boolean 

**************************************************************************/
int cassa_drop_keyspace(cassaManager *CM, char *keyspace)
{
  PyObject *args  =  PyTuple_New(1);
  PyObject *py_keyspace = PyString_FromString (keyspace);
  PyTuple_SetItem (args, 0, py_keyspace);
  if (PyCallable_Check (CM->dropKeyspace) &&
      PyObject_CallObject (CM->dropKeyspace, args))
    {
      fprintf (stderr, "libcassa::keyspace [%s] dropped\n", keyspace);
      return 0;
    }
  fprintf (stderr, "libcassa::Fail to drop keyspace [%s]\n", keyspace);  
  return -1;
}



int
cassa_create_columnfamily (cassaManager *CM,
		       char *keyspace_name,
		       char *columnfamily)
{
 if (! (CM && keyspace_name && columnfamily)) {
    ERRLOGGER("libcassa:cassa_create_columnfamily(): NULL argument\n");
    return -1;
 }

  PyObject *args     = PyTuple_New (2);
  PyObject *py_keyspace = PyString_FromString (keyspace_name);
  PyObject *py_columnfamily = PyString_FromString(columnfamily);
 
  PyTuple_SetItem (args, 0, py_keyspace);
  PyTuple_SetItem (args, 1, py_columnfamily);
   
  if (PyCallable_Check (CM->createColumnFamily) &&
      PyObject_CallObject (CM->createColumnFamily, args))
    {
      fprintf (stderr, "libcassa::column family [%s] created in keyspace\
					 [%s]\n",columnfamily, keyspace_name);
      return 0;
    }
  fprintf (stderr, "libcassa::column family [%s]fail to created in keyspace\
					 [%s]\n",columnfamily, keyspace_name); 
  return -1;
}



int
cassa_drop_columnfamily (cassaManager *CM,
		       char *keyspace_name,
		       char *columnfamily)
{
  PyObject *args     = PyTuple_New (2);
  PyObject *py_keyspace = PyString_FromString (keyspace_name);
  PyObject *py_columnfamily = PyString_FromString(columnfamily);
 
  PyTuple_SetItem (args, 0, py_keyspace);
  PyTuple_SetItem (args, 1, py_columnfamily);
   
  if (PyCallable_Check (CM->dropColumnFamily) &&
      PyObject_CallObject (CM->dropColumnFamily, args))
    {
      fprintf (stderr, "libcassa::column family [%s] dropped in keyspace\
					 [%s]\n",columnfamily, keyspace_name);
      return 0;
    }
  fprintf (stderr, "libcassa::column family [%s]fail to dropped in keyspace\
					 [%s]\n",columnfamily, keyspace_name); 
  return -1;
}

//not tested
int cassa_shutdownSysManager(cassaManager *CM)
{
  if (! CM) {
    ERRLOGGER("cassa_shutdownSysManager INVALID arguments\n");
    return -1;
  }
  Py_XDECREF(CM->createColumnFamily);
  Py_XDECREF(CM->createKeyspace);
  Py_XDECREF(CM->dropColumnFamily);
  Py_XDECREF(CM->dropKeyspace);
  Py_XDECREF(CM->systemManager);
  free(CM);
 return 0;
}

int dict_next(result_t *dict, int *pos, dict_t *item)
{
  
 if (!dict) {
    ERRLOGGER("dict_next::Invalid args");
 }
 
 if ((*pos > (dict->total_Key - 1))) {
    return 0;
 }
 
  item->key   = dict->dict[*pos]->key;
  item->value = dict->dict[*pos]->value;
  printf("11%s\t%s\n",item->key, item->value);
  *pos   = *pos + 1;
   return 1;
}
