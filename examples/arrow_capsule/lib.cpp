#include <iostream>
#include <nanoarrow/nanoarrow.hpp>
#include <nanoarrow/nanoarrow_ipc.hpp>

#include <Python.h>
#include <nanoarrow/nanoarrow_ipc.h>

#define RETURN_IF_STATUS_NOT_OK(s) \
  if (!(s).ok()) {                 \
    return nullptr;                \
  }

// 析构函数（PyCapsule 销毁时调用）
void MyStruct_Destructor(PyObject* capsule) {
  auto* ptr = reinterpret_cast<ArrowArrayStream*>(PyCapsule_GetPointer(capsule, "arrow_array_stream"));
  std::cout << "Destructor called, deleting MyStruct" << std::endl;
  delete ptr;
}

// 返回 PyCapsule
PyObject* make_capsule() {
  nanoarrow::UniqueSchema schema;

  // 直接创建 struct schema，一次性设置所有属性
  ArrowSchemaInitFromType(schema.get(), NANOARROW_TYPE_STRUCT);

  // 分配并设置子列
  ArrowSchemaAllocateChildren(schema.get(), 2);

  // 设置列1：long类型
  ArrowSchemaInitFromType(schema->children[0], NANOARROW_TYPE_INT64);
  ArrowSchemaSetName(schema->children[0], "col1");

  // 设置列2：unsigned int类型
  ArrowSchemaInitFromType(schema->children[1], NANOARROW_TYPE_UINT32);
  ArrowSchemaSetName(schema->children[1], "col2");

  nanoarrow::UniqueArray struct_array;
  ArrowArrayInitFromType(struct_array.get(), NANOARROW_TYPE_STRUCT);
  ArrowArrayAllocateChildren(struct_array.get(), 2);
  ArrowArrayInitFromType(struct_array->children[0], NANOARROW_TYPE_INT64);
  ArrowArrayAppendInt(struct_array->children[0], 1);
  ArrowArrayAppendInt(struct_array->children[0], 2);
  ArrowArrayInitFromType(struct_array->children[1], NANOARROW_TYPE_UINT32);
  ArrowArrayAppendInt(struct_array->children[1], 5);
  ArrowArrayAppendInt(struct_array->children[1], 6);
  ArrowArrayFinishBuildingDefault(struct_array.get(), nullptr);
  struct_array->length = 2;

  ArrowArrayStream* array_stream = new ArrowArrayStream;
  ArrowBasicArrayStreamInit(array_stream, schema.get(), 1);
  ArrowBasicArrayStreamSetArray(array_stream, 0, struct_array.get());

  return PyCapsule_New(static_cast<void*>(array_stream), "arrow_array_stream", MyStruct_Destructor);
}

// Python 模块方法定义
static PyMethodDef ArrowCapsuleMethods[] = {
    {"make_capsule", (PyCFunction)make_capsule, METH_NOARGS, "Create a new MyStruct capsule"},
    {NULL, NULL, 0, NULL}  // 哨兵
};

// Python 模块定义
static struct PyModuleDef ArrowCapsuleModule = {PyModuleDef_HEAD_INIT,
                                                "libarrow_capsule",              // 模块名
                                                "Arrow Capsule Example Module",  // 模块文档
                                                -1,                              // 模块状态大小
                                                ArrowCapsuleMethods};

// Python 模块初始化函数 - 必须命名为 PyInit_模块名
PyMODINIT_FUNC PyInit_libarrow_capsule(void) { return PyModule_Create(&ArrowCapsuleModule); }
