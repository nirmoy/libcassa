
#include "python_api.h"

int IsPyActive = 0;



 PyObject* importModule(char * modulename)
{
  
 PyObject	*module = PyImport_ImportModule(modulename);
 
 if( module ) {
  PyModule_AddObject(mainModule, modulename, module);
  return module;
 }
 
 return NULL; 
}

 int ImportAll(char * modulename)
{
  
 PyObject *module = PyImport_ImportModule(modulename);
 if(module==NULL) {
  fprintf(stderr, "pycassa:: ImportAll() Fail module not found\n");
   return -1;
 }
 PyObject * moduleAttributes = PyObject_Dir(module);
 Py_ssize_t pos = 0;
 Py_ssize_t attrSize = PyList_Size(moduleAttributes);
 
 while (pos < attrSize) {
   PyObject * attrValue = PyList_GetItem(moduleAttributes, pos); //borrowed reference
   PyModule_AddObject(mainModule, PyString_AsString(attrValue), PyObject_GetAttr(module,attrValue));
   pos++;
 }
 Py_DecRef(module);
 Py_DecRef(moduleAttributes);
 return 0;
}

void printPyDict(PyObject *dict)
{
 if(!PyDict_Check(dict)) {
   printf("pythonAPI::printPyDict()::not a dictionary");
   
}
PyObject *key, *value;
Py_ssize_t pos = 0;

while (PyDict_Next(dict, &pos, &key, &value)) {
    printf("%s\t%s\n", PyString_AsString(key), PyString_AsString(value));
}  
  
}

int pythonDictAdditems(PyObject *dict,char *key,long value)
{
  PyDict_SetItemString(dict,key,PyString_FromFormat("%ld",value));
  return 0;
}
