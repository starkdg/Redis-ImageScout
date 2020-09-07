#include <cstdlib>
#include <string>
#include <ctime>
#include <chrono>
#include <list>
#include "redismodule.h"
#include "mvptree.hpp"

#define MVPTREE_ENCODING_VERSION 0

using namespace std;

static RedisModuleType *MVPTreeType;

static const char *descr_field = "descr";

/* =================== dyn mem management ==========================*/
void* operator new(size_t sz){
	void *ptr = RedisModule_Alloc(sz);
	return ptr;
}

void operator delete(void *ptr){
	RedisModule_Free(ptr);
}

/* ================= aux. functions ==================================*/

int get_next_id(RedisModuleCtx *ctx, RedisModuleString *keystr, long long &id){
	id = RedisModule_Milliseconds() << 32;

	id |= (int64_t)(rand() & 0xffff0000);

	string key = RedisModule_StringPtrLen(keystr, NULL);
	key += ":counter";

	RedisModuleCallReply *reply = RedisModule_Call(ctx, "INCRBY", "cl", key.c_str(), 1);
	if (RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_INTEGER){
		return REDISMODULE_ERR;
	}

	id |= (0x0000ffffULL & RedisModule_CallReplyInteger(reply));
	RedisModule_FreeCallReply(reply);
	return REDISMODULE_OK;
}
/* retrieve a descr field stored in keystr+id hash redis datatype */
RedisModuleString* GetDescriptionField(RedisModuleCtx *ctx, RedisModuleString *keystr, long long id){
	string idstr = RedisModule_StringPtrLen(keystr, NULL);
	idstr += ":" + to_string(id);

	RedisModuleString *keyidstr = RedisModule_CreateString(ctx, idstr.c_str(), idstr.length());
	RedisModuleKey *key = (RedisModuleKey*)RedisModule_OpenKey(ctx, keyidstr, REDISMODULE_READ);
	if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_HASH){
		RedisModule_CloseKey(key);
		return NULL;
	}

	RedisModuleString *descr = NULL;
	RedisModule_HashGet(key, REDISMODULE_HASH_CFIELDS, descr_field, &descr, NULL);
	RedisModule_CloseKey(key);
	return descr;
}

/* set a descr field for a keystr+id redis hash data type */ 
void SetDescriptionField(RedisModuleCtx *ctx, RedisModuleString *keystr, long long id, RedisModuleString *descr){
	string idstr = RedisModule_StringPtrLen(keystr, NULL);
	idstr += ":" + to_string(id);

	RedisModuleString *keyidstr = RedisModule_CreateString(ctx, idstr.c_str(), idstr.length());
	RedisModuleKey *key = (RedisModuleKey*)RedisModule_OpenKey(ctx, keyidstr, REDISMODULE_WRITE);
	if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY){
		RedisModule_CloseKey(key);
		return;
	}

	RedisModule_HashSet(key, REDISMODULE_HASH_CFIELDS|REDISMODULE_HASH_NX, descr_field, descr, NULL);
	RedisModule_CloseKey(key);
}

void DeleteDescriptionField(RedisModuleCtx *ctx, RedisModuleString *keystr, long long id){
	string idstr = RedisModule_StringPtrLen(keystr, NULL);
	idstr += ":" + to_string(id);

	RedisModuleString *keyidstr = RedisModule_CreateString(ctx, idstr.c_str(), idstr.length());
	RedisModuleKey *key = (RedisModuleKey*)RedisModule_OpenKey(ctx, keyidstr, REDISMODULE_WRITE);
	if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_HASH){
		RedisModule_CloseKey(key);
		return;
	}

	RedisModule_HashSet(key, REDISMODULE_HASH_CFIELDS, descr_field, REDISMODULE_HASH_DELETE, NULL);
	RedisModule_CloseKey(key);
}

void DeleteDescriptionKey(RedisModuleCtx *ctx, RedisModuleString *keystr, long long id){
	string idstr = RedisModule_StringPtrLen(keystr, NULL);
	idstr += ":" + to_string(id);

	RedisModuleString *keyidstr = RedisModule_CreateString(ctx, idstr.c_str(), idstr.length());
	RedisModuleKey *key = (RedisModuleKey*)RedisModule_OpenKey(ctx, keyidstr, REDISMODULE_WRITE);
	if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_HASH){
		RedisModule_CloseKey(key);
		return;
	}

	RedisModule_DeleteKey(key);
	RedisModule_CloseKey(key);
}

void DeleteCounterKey(RedisModuleCtx *ctx, RedisModuleString *keystr){
	string counterstr = RedisModule_StringPtrLen(keystr, NULL);
	counterstr += ":counter";

	RedisModuleString *keycounterstr = RedisModule_CreateString(ctx, counterstr.c_str(), counterstr.length());
	RedisModuleKey *key = (RedisModuleKey*)RedisModule_OpenKey(ctx, keycounterstr, REDISMODULE_WRITE);
	RedisModule_DeleteKey(key);
	RedisModule_CloseKey(key);
	return;
}

void DeleteKey(RedisModuleCtx *ctx, RedisModuleString *keystr){
	RedisModuleKey *key = (RedisModuleKey*)RedisModule_OpenKey(ctx, keystr, REDISMODULE_WRITE);
	RedisModule_DeleteKey(key);
	RedisModule_CloseKey(key);
	return;
}

unsigned long long RMStringToUnsignedLongLong(const RedisModuleString *str){
	return strtoull(RedisModule_StringPtrLen(str, NULL), NULL, 10);
}

/* ============== Get MVPTree =======================================*/

MVPTree* GetMVPTree(RedisModuleCtx *ctx, RedisModuleString *keystr){
	RedisModuleKey *key = (RedisModuleKey*)RedisModule_OpenKey(ctx, keystr, REDISMODULE_READ);
	int keytype = RedisModule_KeyType(key);
	if (keytype == REDISMODULE_KEYTYPE_EMPTY){
		RedisModule_CloseKey(key);
		return NULL;
	}
	
	if (RedisModule_ModuleTypeGetType(key) != MVPTreeType){
		RedisModule_CloseKey(key);
		throw -1;
	}

	MVPTree *tree = (MVPTree*)RedisModule_ModuleTypeGetValue(key);

	RedisModule_CloseKey(key);
	return tree;
}

/* Create a new data type, throw -1 exception if already exists for a different type */
MVPTree* CreateMVPTree(RedisModuleCtx *ctx, RedisModuleString *keystr){
	RedisModuleKey *key = (RedisModuleKey*)RedisModule_OpenKey(ctx, keystr, REDISMODULE_WRITE);
	int keytype = RedisModule_KeyType(key);
	if (keytype != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(key) != MVPTreeType){
		RedisModule_CloseKey(key);
		throw -1;
	}

	MVPTree *tree = NULL;
	if (keytype == REDISMODULE_KEYTYPE_EMPTY){
		tree = (MVPTree*)new MVPTree();
		RedisModule_ModuleTypeSetValue(key, MVPTreeType, tree);
	} else {
		tree = (MVPTree*)RedisModule_ModuleTypeGetValue(key);
	}

	RedisModule_CloseKey(key);
	return tree;
}

/* ============== MVPTree type methods ==============================*/
extern "C" void* MVPTreeTypeRdbLoad(RedisModuleIO *rdb, int encver){
	if (encver != MVPTREE_ENCODING_VERSION){
		RedisModule_LogIOError(rdb, "warning", "rdbload unable to encode for encver %d", encver);
		return NULL;
	}

	MVPTree *tree = (MVPTree*)new MVPTree();

	unsigned long long n_points = RedisModule_LoadUnsigned(rdb);
	for (unsigned long long i=0;i<n_points;i++){
		DataPoint *dp = new DataPoint();
		dp->id = RedisModule_LoadSigned(rdb);
		dp->value = RedisModule_LoadUnsigned(rdb);
		tree->Add(dp);
	}
	
	tree->Sync();
	return (void*)tree;
}
extern "C" void MVPTreeTypeRdbSave(RedisModuleIO *rdb, void *value){
	MVPTree *tree = (MVPTree*)value;

	const map<long long, DataPoint*> ids = tree->GetMap();
	RedisModule_SaveUnsigned(rdb, ids.size());
	for (auto iter=ids.begin();iter!=ids.end();iter++){
		RedisModule_SaveSigned(rdb, iter->second->id);
		RedisModule_SaveUnsigned(rdb, iter->second->value);
	}
}
extern "C" void MVPTreeTypeAofRewrite(RedisModuleIO *aof, RedisModuleString *key, void *value){
	MVPTree *tree = (MVPTree*)value;
	
	const map<long long, DataPoint*> ids = tree->GetMap();
	for (auto iter=ids.begin();iter!=ids.end();iter++){
		RedisModule_EmitAOF(aof, "imgscout.addrepl", "sll", key, iter->second->value, iter->first);
	}
}
extern "C" void MVPTreeTypeFree(void *value){
	MVPTree *tree = (MVPTree*)value;
	tree->Clear();
}

extern "C" size_t MVPTreeTypeMemUsage(const void *value){
	MVPTree *tree = (MVPTree*)value;
	size_t n_bytes = tree->MemoryUsage();
	return n_bytes;
	
}
extern "C" void MVPTreeTypeDigest(RedisModuleDigest *digest, void *value){
	REDISMODULE_NOT_USED(digest);
	REDISMODULE_NOT_USED(value);
	
}
/* ============== Redis Command functions ===========================*/

/* args: key hashvalue id */
extern "C" int MVPTreeAddRepl_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc < 4) return RedisModule_WrongArity(ctx);

	RedisModule_AutoMemory(ctx);

	MVPTree *tree = NULL;
	try {
		tree = GetMVPTree(ctx, argv[1]);
		if (tree == NULL) tree = CreateMVPTree(ctx, argv[1]);
	} catch (int &e){
		RedisModule_ReplyWithError(ctx, "ERR - key exists for different type.  Delete first.");
		return REDISMODULE_ERR;
	}

	long long id;
	if (RedisModule_StringToLongLong(argv[3], &id) == REDISMODULE_ERR){
		RedisModule_ReplyWithError(ctx, "ERR - unable to parse id value");
		return REDISMODULE_ERR;
	}

	unsigned long long hashvalue = RMStringToUnsignedLongLong(argv[2]);
	DataPoint *dp = new DataPoint();
	dp->id = id;
	dp->value = hashvalue;

	tree->Add(dp);

	RedisModule_ReplyWithLongLong(ctx, id);
	return REDISMODULE_OK;
}

/*args: key hashvalue descr [id] */
extern "C" int MVPTreeAdd_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc < 4) return RedisModule_WrongArity(ctx);

	RedisModule_AutoMemory(ctx);

	MVPTree *tree = NULL;
	try {
		tree = GetMVPTree(ctx, argv[1]);
		if (tree == NULL) tree = CreateMVPTree(ctx, argv[1]);
	} catch (int &e){
		RedisModule_ReplyWithError(ctx, "ERR - key exists for different type.  Delete first.");
		return REDISMODULE_ERR;
	}

	long long id;
	if (argc == 5){
		if (RedisModule_StringToLongLong(argv[4], &id) == REDISMODULE_ERR){
			RedisModule_ReplyWithError(ctx, "ERR - unable to parse id value");
			return REDISMODULE_ERR;
		}
	} else {
		if (get_next_id(ctx, argv[1], id) == REDISMODULE_ERR){
			RedisModule_ReplyWithError(ctx, "ERR - unable to get next id value");
			return REDISMODULE_ERR;
		}
	}

	unsigned long long hash_value = RMStringToUnsignedLongLong(argv[2]);

	DataPoint *dp = new DataPoint();
	dp->id = id;
	dp->value = hash_value;
	try {
		tree->Add(dp);
	} catch (exception &ex){
		RedisModule_ReplyWithError(ctx, "ERR - unable to add element");
		return REDISMODULE_ERR;
	}


	SetDescriptionField(ctx, argv[1], id, argv[3]);

	RedisModule_ReplyWithLongLong(ctx, id);
	
	if (RedisModule_Replicate(ctx, "imgscout.add", "sssl", argv[1], argv[2], argv[3], id) == REDISMODULE_ERR){
		RedisModule_Log(ctx, "warning", "unable to replicate add command for id = %lld", id);
	}

	return REDISMODULE_OK;
}

/* args: key */
extern "C" int MVPTreeSync_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc != 2) return RedisModule_WrongArity(ctx);

	RedisModule_AutoMemory(ctx);

	chrono::time_point<chrono::high_resolution_clock> start = chrono::high_resolution_clock::now();
	
	MVPTree *tree = NULL;
	try {
		tree = GetMVPTree(ctx, argv[1]);
		if (tree == NULL){
			RedisModule_ReplyWithError(ctx, "ERR - no such key");
			return REDISMODULE_ERR;
		}
	} catch (int &e){
		RedisModule_ReplyWithError(ctx, "ERR - key exists for different type.  Delete first.");
		return REDISMODULE_ERR;
	}

	try {
		tree->Sync();
	} catch (exception &ex){
		RedisModule_ReplyWithError(ctx, "ERR - unable to sync");
		return REDISMODULE_ERR;
	}
  
	int n_points = tree->Size();

	RedisModule_ReplyWithSimpleString(ctx, "OK");
	RedisModule_ReplicateVerbatim(ctx);

	chrono::time_point<chrono::high_resolution_clock> end = chrono::high_resolution_clock::now();
	auto elapsed = chrono::duration_cast<chrono::microseconds>(end - start).count();
	RedisModule_Log(ctx, "debug", "%llu points synced in %u microseconds", n_points, elapsed);
	
	return REDISMODULE_OK;
}

/* args: key hashtarget radius */
extern "C" int MVPTreeQuery_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc != 4) return RedisModule_WrongArity(ctx);

	RedisModule_AutoMemory(ctx);

	chrono::time_point<chrono::high_resolution_clock> start = chrono::high_resolution_clock::now();
	
	MVPTree *tree = NULL;
	try {
		tree = GetMVPTree(ctx, argv[1]);
		if (tree == NULL){
			RedisModule_ReplyWithError(ctx, "ERR - no such key");
			return REDISMODULE_ERR;
		}
	} catch (int &e){
		RedisModule_ReplyWithError(ctx, "ERR - key exists for different type.  Delete first.");
		return REDISMODULE_ERR;
	}

	unsigned long long hash_value = RMStringToUnsignedLongLong(argv[2]);
	double radius;
	if (RedisModule_StringToDouble(argv[3], &radius) == REDISMODULE_ERR){
		RedisModule_ReplyWithError(ctx, "unable to parse radius value");
		return REDISMODULE_ERR;
	}

	DataPoint target;
	target.value = hash_value;

	list<QueryResult> results;
	try {
		results = tree->Query(target, radius);
	} catch (exception &ex){
		RedisModule_ReplyWithError(ctx, "ERR - unable to complete query");
		return REDISMODULE_ERR;
	}
	
	RedisModule_ReplyWithArray(ctx, results.size());
	for (QueryResult &r: results){
		RedisModuleString *reply_descr = GetDescriptionField(ctx, argv[1], r.dp->id);
		RedisModule_ReplyWithArray(ctx, 3);
		RedisModule_ReplyWithString(ctx, reply_descr);
		RedisModule_ReplyWithLongLong(ctx, r.dp->id);
		RedisModule_ReplyWithDouble(ctx, r.distance);
	}

	// calculate pct of distance operations
	double pct_opers = (double)MVPTree::n_ops/(double)tree->Size();

	chrono::time_point<chrono::high_resolution_clock> end = chrono::high_resolution_clock::now();
	auto elapsed = chrono::duration_cast<chrono::microseconds>(end - start).count();
	RedisModule_Log(ctx, "debug", "query in %llu microseconds (%f ops)", elapsed, pct_opers);
	
	return REDISMODULE_OK;
}

/* args: key id */
extern "C" int MVPTreeLookup_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc != 3) return RedisModule_WrongArity(ctx);

	RedisModule_AutoMemory(ctx);

	long long id;
	if (RedisModule_StringToLongLong(argv[2], &id) == REDISMODULE_ERR){
		RedisModule_ReplyWithError(ctx, "Error - unable to parse id");
		return REDISMODULE_ERR;
	}

	RedisModuleString *descr = GetDescriptionField(ctx, argv[1], id);
	RedisModule_ReplyWithString(ctx, descr);
	
	return REDISMODULE_OK;
}

/* args: key */
extern "C" int MVPTreeSize_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc != 2) return RedisModule_WrongArity(ctx);

	RedisModule_AutoMemory(ctx);
	
	MVPTree *tree = NULL;
	try {
		tree = GetMVPTree(ctx, argv[1]);
		if (tree == NULL){
			RedisModule_ReplyWithError(ctx, "ERR - no such key");
			return REDISMODULE_ERR;
		}
	} catch (int &e){
		RedisModule_ReplyWithError(ctx, "ERR - key exists for different type.  Delete first.");
		return REDISMODULE_ERR;
	}

	long long n_points;
	try {
		n_points = tree->Size();
	} catch (exception &ex){
		RedisModule_ReplyWithError(ctx, "ERR - unable to get size");
		return REDISMODULE_ERR;
	}
	
	RedisModule_ReplyWithLongLong(ctx, n_points);
	return REDISMODULE_OK;
}

/* args: key id */
extern "C" int MVPTreeDelete_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc != 3) return RedisModule_WrongArity(ctx);

	RedisModule_AutoMemory(ctx);
	
	MVPTree *tree = NULL;
	try {
		tree = GetMVPTree(ctx, argv[1]);
		if (tree == NULL){
			RedisModule_ReplyWithError(ctx, "ERR - no such key");
			return REDISMODULE_ERR;
		}
	} catch (int &e){
		RedisModule_ReplyWithError(ctx, "ERR - key exists for different type.  Delete first.");
		return REDISMODULE_ERR;
	}

	long long id;
	if (RedisModule_StringToLongLong(argv[2], &id) == REDISMODULE_ERR){
		RedisModule_ReplyWithError(ctx, "ERR - unable to parse id");
		return  REDISMODULE_ERR;
	}

	try {
		tree->Delete(id);
	} catch (exception &ex){
		RedisModule_ReplyWithError(ctx, "ERR - unable to delete id");
		return REDISMODULE_ERR;
	}

	DeleteDescriptionField(ctx, argv[1], id);
	RedisModule_ReplyWithSimpleString(ctx, "OK");
	RedisModule_ReplicateVerbatim(ctx);
	return REDISMODULE_OK;
}

/* ============== Onload Init Function ==============================*/
extern "C" int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){

	int rc = REDISMODULE_OK;
	if (RedisModule_Init(ctx, "imgscout", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
		rc = REDISMODULE_ERR;
	
	RedisModuleTypeMethods tm = {.version = REDISMODULE_TYPE_METHOD_VERSION,
	                             .rdb_load = MVPTreeTypeRdbLoad,
	                             .rdb_save = MVPTreeTypeRdbSave,
	                             .aof_rewrite = MVPTreeTypeAofRewrite,
	                             .mem_usage = MVPTreeTypeMemUsage,
	                             .digest = MVPTreeTypeDigest,
	                             .free = MVPTreeTypeFree};

	MVPTreeType = RedisModule_CreateDataType(ctx, "MVPTreeDS", MVPTREE_ENCODING_VERSION, &tm);
	if (MVPTreeType == NULL) rc = REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "imgscout.add", MVPTreeAdd_RedisCmd,
								  "write deny-oom", 1, -1, 1) == REDISMODULE_ERR)
		rc = REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "imgscout.addrepl", MVPTreeAddRepl_RedisCmd,
								  "write deny-oom", 1, -1, 1) == REDISMODULE_ERR)
		rc = REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "imgscout.sync", MVPTreeSync_RedisCmd,
								  "write deny-oom", 1, -1, 1) == REDISMODULE_ERR)
		rc = REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "imgscout.query", MVPTreeQuery_RedisCmd,
								  "readonly", 1, -1, 1) == REDISMODULE_ERR)
		rc = REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "imgscout.lookup", MVPTreeLookup_RedisCmd,
								  "readonly fast", 1, -1, 1) == REDISMODULE_ERR)
		rc = REDISMODULE_ERR;
	
	if (RedisModule_CreateCommand(ctx, "imgscout.size", MVPTreeSize_RedisCmd,
								  "readonly fast", 1, -1, 1) == REDISMODULE_ERR)
		rc = REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "imgscout.del", MVPTreeDelete_RedisCmd,
								  "write fast", 1, -1, 1) == REDISMODULE_ERR)
		rc = REDISMODULE_ERR;

	
	return rc;;
}
