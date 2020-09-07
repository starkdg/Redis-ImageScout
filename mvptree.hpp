#ifndef _MVPTREE_H
#define _MVPTREE_H

#include <list>
#include "mvpnode.hpp"

using namespace std;

class MVPTree {
private:
	vector<DataPoint*> m_arrivals;
	
	map<long long, DataPoint*> m_ids;
	
	MVPNode* m_top;

	int n_internal, n_leaf;
	
	void LinkNodes(map<int, MVPNode*> &nodes, map<int, MVPNode*> &childnodes)const;
	void ExpandNode(MVPNode *node, map<int, MVPNode*> &childnodes, const int index)const;
	MVPNode* ProcessNode(const int level, const int index, MVPNode *node, vector<DataPoint*> &points,
						 map<int, MVPNode*> &childnodes, map<int, vector<DataPoint*>*> &childpoints);
public:

	static int n_ops;

	MVPTree():m_top(NULL),n_internal(0),n_leaf(0){};

	const DataPoint* Lookup(const long long id);
	
	void Add(DataPoint *dp);
	
	void Add(vector<DataPoint*> &points);

	void Sync();
	
	void Delete(const long long id);

	const int Size()const;

	void CountNodes(int &n_internal, int &n_leaf)const;
	
	void Clear();

	const list<QueryResult> Query(const DataPoint &target, const double radius) const;

	void Print()const;

	size_t MemoryUsage()const;

	const map<long long, DataPoint*> GetMap()const;
};

#endif /* _MVPTREE_H */
