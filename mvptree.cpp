#include <iostream>
#include <typeinfo>
#include <queue>
#include "mvptree.hpp"

using namespace std;

int MVPTree::n_ops = 0;

void MVPTree::LinkNodes(map<int, MVPNode*> &nodes, map<int, MVPNode*> &childnodes)const{
	for (auto iter=nodes.begin();iter!=nodes.end();iter++){
		int i = iter->first;
		MVPNode *mvpnode = iter->second;
		if (mvpnode != NULL){
			for (int j=0;j<MVP_FANOUT;j++){
				MVPNode *child = childnodes[i*MVP_FANOUT+j];
				if (child != NULL) mvpnode->SetChildNode(j, child);
			}
		}
	}
}

void MVPTree::ExpandNode(MVPNode *node, map<int, MVPNode*> &childnodes,  const int index)const{
	if (node != NULL){
		for (int i=0;i<MVP_FANOUT;i++){
			MVPNode *child = node->GetChildNode(i);
			if (child != NULL) childnodes[index*MVP_FANOUT+i] = child;
		}
	}
}

MVPNode* MVPTree::ProcessNode(const int level, const int index,
							  MVPNode *node,
							  vector<DataPoint*> &points,
							  map<int, MVPNode*> &childnodes,
							  map<int, vector<DataPoint*>*> &childpoints){
	MVPNode *retnode = node;
	if (node == NULL){ // create new node
		retnode = MVPNode::CreateNode(points, childpoints, level, index);
	} else {           // node exists
		retnode = node->AddDataPoints(points, childpoints, level, index);
	}

	if (retnode == NULL)
		throw runtime_error("failure to assign node");
	
	return retnode;
}

const DataPoint* MVPTree::Lookup(const long long id){
	auto iter = m_ids.find(id);
	if (iter != m_ids.end()){
		return iter->second;
	}
	return NULL;
}

void MVPTree::Add(DataPoint *dp){
	if (dp != NULL){
		m_arrivals.push_back(dp);
		if (m_arrivals.size() >= MVP_SYNC) Add(m_arrivals);
	}
}

void MVPTree::Add(vector<DataPoint*> &points){
	if (points.empty()) return;

	for (DataPoint* dp : points) m_ids[dp->id] = dp;

	map<int, MVPNode*> prevnodes, currnodes, childnodes;
	if (m_top != NULL) currnodes[0] = m_top;

	map<int, vector<DataPoint*>*> pnts, pnts2;
	pnts[0] = &points;

	int n = 0;
	do {
		for (auto iter=pnts.begin();iter!=pnts.end();iter++){
			int index = iter->first;
			vector<DataPoint*> *list = iter->second;
			MVPNode *mvpnode = currnodes[index];
			MVPNode *newnode = ProcessNode(n, index, mvpnode, *list, childnodes, pnts2);
			if (newnode != mvpnode){
				if (newnode != NULL && typeid(*newnode).hash_code() == typeid(MVPInternal).hash_code()){
					n_internal++;
				} else if (typeid(*newnode).hash_code() == typeid(MVPLeaf).hash_code()){
					n_leaf++;
				}
				if (mvpnode != NULL){
					if (typeid(*mvpnode).hash_code() == typeid(MVPLeaf).hash_code()){
						n_leaf--;
					}
					delete mvpnode;
				}
				if (n == 0) m_top = newnode;
			}
			currnodes[index] = newnode;
			ExpandNode(newnode, childnodes, index);

			if (n > 0) delete list;
		}

		if (!prevnodes.empty()) {
			LinkNodes(prevnodes, currnodes);
		}
		prevnodes = move(currnodes);
		currnodes = move(childnodes);
		pnts = move(pnts2);
		childnodes.clear();
		n += MVP_LEVELSPERNODE;
	} while (!pnts.empty());
}

void MVPTree::Sync(){
	if (m_arrivals.size() > 0) {
		Add(m_arrivals);
	}
}

void MVPTree::Delete(const long long id){
	auto iter = m_ids.find(id);
	if (iter != m_ids.end()) iter->second->active = false;
	m_ids.erase(id);
}

const int MVPTree::Size()const{
	return m_ids.size();
}

void MVPTree::CountNodes(int &n_internal, int &n_leaf)const{
	n_internal = n_leaf = 0;

	queue<MVPNode*> nodes;
	if (m_top != NULL) nodes.push(m_top);

	while (!nodes.empty()){
		MVPNode *curr_node = nodes.front();

		if (typeid(*curr_node).hash_code() == typeid(MVPInternal).hash_code()){
			n_internal++;
		} else {
			n_leaf++;
		}
	
		for (int i=0;i<MVP_FANOUT;i++){
			MVPNode *child = curr_node->GetChildNode(i);
			if (child != NULL) nodes.push(child);
		}
		
		nodes.pop();
	}
}

void MVPTree::Clear(){
	map<int, MVPNode*> currnodes, childnodes;
	if (m_top != NULL) currnodes[0] = m_top;

	int n = 0, n_internal = 0, n_leaf = 0;
	do {
		int n_nodes = pow(MVP_BRANCHFACTOR, n);
		int n_childnodes = pow(MVP_BRANCHFACTOR, n+MVP_LEVELSPERNODE);
		for (auto iter=currnodes.begin();iter!=currnodes.end();iter++){
			int index = iter->first;
			MVPNode *mvpnode = iter->second;

			if (mvpnode != NULL){
				vector<DataPoint*> pts = mvpnode->PurgeDataPoints();
				for (DataPoint *dp : pts){
					delete dp;
				}

				ExpandNode(mvpnode, childnodes, index);
				
				if (typeid(*mvpnode).hash_code() == typeid(MVPInternal).hash_code()){
					n_internal++;
				} else {
					n_leaf++;
				}

				delete mvpnode;
			}
		}
		currnodes = move(childnodes);
		n += MVP_LEVELSPERNODE;

	} while (!currnodes.empty());
	m_top = NULL;
	m_ids.clear();
}

const list<QueryResult> MVPTree::Query(const DataPoint &target, const double radius) const{
	list<QueryResult> results;
	
	map<int, MVPNode*> currnodes, childnodes;
	if (m_top != NULL) currnodes[0] = m_top;

	n_ops = 0;
	int n = 0;
	do {
		int n_nodes = pow(MVP_BRANCHFACTOR, n);
		int n_childnodes = pow(MVP_BRANCHFACTOR, n+MVP_LEVELSPERNODE);
		
		for (auto iter=currnodes.begin();iter!=currnodes.end();iter++){
			int node_index = iter->first;
			MVPNode *mvpnode = iter->second;
			if (mvpnode != NULL){
				mvpnode->TraverseNode(target, radius, childnodes, node_index, results);
			}
		}
		currnodes = move(childnodes);
		n += MVP_LEVELSPERNODE;
	} while (!currnodes.empty());

	return results;
}

void MVPTree::Print()const{
	map<int, MVPNode*> currnodes, childnodes;
	if (m_top != NULL) currnodes[0] = m_top;
	else cout << "Tree is empty" << endl;

	cout << "MVP Tree" << endl;
	cout << "branch factor: " << MVP_BRANCHFACTOR << endl;
	cout << "path length: " << MVP_PATHLENGTH << endl;
	cout << "leaf cap: " << MVP_LEAFCAP << endl;
	cout << "no. vp's: " << MVP_LEVELSPERNODE << endl;

	int n = 0, depth = 0;
	bool done;
	do {
		done = true;
		int n_nodes = pow(MVP_BRANCHFACTOR, n);
		int n_childnodes = pow(MVP_BRANCHFACTOR, n+MVP_LEVELSPERNODE);

		cout << "level=" << n << "  ";
		for (auto iter=currnodes.begin();iter!=currnodes.end();iter++){
			int index = iter->first;
			MVPNode *mvpnode = iter->second;

			cout << "node " << index << " (" << mvpnode->GetCount() << " points) - ";

			ExpandNode(mvpnode, childnodes, index);
		}
		cout << endl;
		currnodes = move(childnodes);
		n += MVP_LEVELSPERNODE;
		if (!currnodes.empty()) done = false;
	} while (!done);
}

size_t MVPTree::MemoryUsage()const{
	map<int, MVPNode*> currnodes, childnodes;
	if (m_top != NULL) currnodes[0] = m_top;

	int n_points = m_ids.size();
	int n_internal=0, n_leaf=0;
	do {
		for (auto iter=currnodes.begin();iter!=currnodes.end();iter++){
			int index = iter->first;
			MVPNode *node = iter->second;

			if (typeid(*node).hash_code() == typeid(MVPInternal).hash_code())
				n_internal++;
			else
				n_leaf++;
			ExpandNode(node, childnodes, index);
		}
		currnodes = move(childnodes);
	} while (!currnodes.empty());
	
	return  n_points*sizeof(DataPoint) + n_internal*sizeof(MVPInternal)
		+ n_leaf*sizeof(MVPLeaf) + sizeof(MVPLeaf);
}

const map<long long, DataPoint*> MVPTree::GetMap()const{
	return m_ids;
}
