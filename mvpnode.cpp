#include <iostream>
#include "mvptree.hpp"
#include "mvpnode.hpp"

static double PointDistance(const DataPoint *a, const DataPoint *b){
	MVPTree::n_ops++;
	return __builtin_popcountll((a->value)^(b->value));
}

bool CompareDistance(const double a, const double b, const bool less){
	if (less) return (a <= b);
	return (a > b);
}

/********** MVPNode methods *********************/

MVPNode* MVPNode::CreateNode(vector<DataPoint*> &points,
							 map<int, vector<DataPoint*>*> &childpoints,
							 int level, int index){
	MVPNode *node = NULL;
	if (points.size() <= MVP_LEAFCAP + MVP_PATHLENGTH){
		node = new MVPLeaf();
	} else {
		node = new MVPInternal();
	}

	node = node->AddDataPoints(points, childpoints, level, index);
	if (node == NULL) throw runtime_error("unable to create node");
	return node;
}

void MVPNode::InsertItemIntoList(list<QueryResult> &list, QueryResult &item)const{
	auto iter = list.begin();
	for ( ;iter != list.end();iter++){
		if (item.distance <= iter->distance){
			list.insert(iter, item);
			break;
		}
	}
	if (iter == list.end()){
		list.insert(iter, item);
	}
}

/********** MVPInternal methods *******************/
MVPInternal::MVPInternal(){
	m_nvps = 0;
	for (int i=0;i<MVP_FANOUT;i++) m_childnodes[i] = NULL;
	for (int i=0;i<MVP_LEVELSPERNODE;i++){
		for (int j=0;j<MVP_NUMSPLITS;j++){
			m_splits[i][j] = -1.0;
		}
	}
}

void MVPInternal::SelectVantagePoints(vector<DataPoint*> &points){
	while (m_nvps < MVP_LEVELSPERNODE && points.size() > 0){
		m_vps[m_nvps++] = points.back();
		points.pop_back();
	}
}

void MVPInternal::CalcSplitPoints(const vector<double> &dists, int n, int split_index){
	int lengthM = MVP_BRANCHFACTOR - 1;
	if (dists.size() > 0){
		if (m_splits[n][split_index*lengthM] == -1){
			vector<double> tmpdists = dists;
			sort(tmpdists.begin(), tmpdists.end());
			double factor = (double)tmpdists.size()/(double)MVP_BRANCHFACTOR;
			for (int i=0;i<lengthM;i++){
				double pos = (i+1)*factor;
				int lo = floor(pos);
				int hi = (pos <= tmpdists.size()-1) ? ceil(pos) : 0;
				m_splits[n][split_index*lengthM+i] = (tmpdists[lo] + tmpdists[hi])/2.0;
			}
		}
	}
}


vector<double> MVPInternal::CalcPointDistances(DataPoint &vp, vector<DataPoint*> &points){
	vector<double> results;
	for (DataPoint *dp : points){
		results.push_back(PointDistance(&vp, dp));
	}
	return results;
}

vector<DataPoint*>* MVPInternal::CullPoints(vector<DataPoint*> &list, vector<double> &dists,
							  double split, bool less){
	vector<DataPoint*> *results = new vector<DataPoint*>();

	vector<DataPoint*>::iterator list_iter = list.begin();
	vector<double>::iterator dist_iter = dists.begin(); 
	while (list_iter != list.end() && dist_iter != dists.end()){
		if (CompareDistance(*dist_iter,split,less)){
			results->push_back(*list_iter);
			list_iter = list.erase(list_iter);
			dist_iter = dists.erase(dist_iter);
		} else {
			list_iter++;
			dist_iter++;
		}
	}
	if (results->size() > 0){
		return results;
	}
	delete results;
	return NULL;
}

void MVPInternal::CollatePoints(vector<DataPoint*> &points,
								map<int, vector<DataPoint*>*> &childpoints,
								const int level, const int index){
	map<int, vector<DataPoint*>*> pnts, pnts2;
	pnts[0] = &points;

	int lengthM = MVP_BRANCHFACTOR - 1;
	int n = 0;
	do {
		for (auto iter=pnts.begin();iter!=pnts.end();iter++){
			int node_index = iter->first;
			vector<DataPoint*> *list = iter->second;

			int list_size = list->size();

			vector<double> dists = CalcPointDistances(*m_vps[n], *list);
			if (dists.size() > 0){
				CalcSplitPoints(dists, n, node_index);

				double m;
				vector<DataPoint*> *culledpts = NULL;
				for (int j=0;j<lengthM;j++){
					m = m_splits[n][node_index*lengthM+j];

					culledpts = CullPoints(*list, dists, m, true);
					if (culledpts != NULL){
						pnts2[node_index*MVP_BRANCHFACTOR+j] = culledpts;
					}
				}
				m = m_splits[n][node_index*lengthM+lengthM-1];
				culledpts = CullPoints(*list, dists, m, false);
				if (culledpts != NULL){
					pnts2[node_index*MVP_BRANCHFACTOR+MVP_BRANCHFACTOR-1] = culledpts;
				}
			}
			if (list->size() > 0)
				throw length_error("not fully collated");

			if (n > 0) delete list;
		}
		pnts = move(pnts2);
		n++;
	} while (n < MVP_LEVELSPERNODE);

	for (auto iter=pnts.begin();iter!=pnts.end();iter++){
		int i=iter->first;
		vector<DataPoint*> *list = iter->second;
		if (list != NULL)
			childpoints[index*MVP_FANOUT+i] = list;  
	}
	
}

MVPNode* MVPInternal::AddDataPoints(vector<DataPoint*> &points,
									map<int,vector<DataPoint*>*> &childpoints,
									const int level, const int index){
	SelectVantagePoints(points);
	if (m_nvps < MVP_LEVELSPERNODE) throw invalid_argument("too few points for internal node");
	CollatePoints(points, childpoints, level, index);
	points.clear();
	return this;
}

const int MVPInternal::GetCount()const{
	return 0;
}

void MVPInternal::SetChildNode(const int n, MVPNode *node){
	if (n < 0 || n >= MVP_FANOUT) throw invalid_argument("index out of range");
	m_childnodes[n] = node;
}

MVPNode* MVPInternal::GetChildNode(const int n)const{
	if (n < 0 || n >= MVP_FANOUT) throw invalid_argument("index out of range");
	return m_childnodes[n];
}

const vector<DataPoint*> MVPInternal::GetVantagePoints()const{
	vector<DataPoint*> results;
	for (int i=0;i<m_nvps;i++) results.push_back(m_vps[i]);
	return results;
}

const vector<DataPoint*> MVPInternal::GetDataPoints()const{
	vector<DataPoint*> results;
	return results;
}

const vector<DataPoint*> MVPInternal::FilterDataPoints(const DataPoint *target, const double radius)const{
	vector<DataPoint*> results;
	for (int i=0;i<m_nvps;i++){
		if (!m_vps[i]->active) continue;
		double d = PointDistance(target, m_vps[i]);
		if (d <= radius){
			results.push_back(m_vps[i]);
		}
	}

	return results;
}

void MVPInternal::TraverseNode(const DataPoint &target, const double radius, map<int, MVPNode*> &childnodes,
							   const int index, list<QueryResult> &results)const{
	int lengthM = MVP_BRANCHFACTOR - 1;
	int n = 0;
	bool *currnodes  = new bool[1];
	currnodes[0] = true;
	do {
		int n_nodes = pow(MVP_BRANCHFACTOR, n);
		int n_childnodes = pow(MVP_BRANCHFACTOR, n+1);
		bool *nextnodes = new bool[n_childnodes];
		for (int i=0;i<n_childnodes;i++) nextnodes[i] = false;

		double d = PointDistance(m_vps[n], &target);
		if (m_vps[n]->active && d <= radius){
			QueryResult r;
			r.dp = m_vps[n];
			r.distance = d;
			InsertItemIntoList(results, r);
		}

		int lengthMn = lengthM*n_nodes;
		for (int node_index=0;node_index<n_nodes;node_index++){
			if (currnodes[node_index]){
				if (m_splits[n][node_index*lengthM] >= 0){
					double m = m_splits[n][node_index*lengthM];
					for (int j=0;j<lengthM;j++){
						m = m_splits[n][node_index*lengthM+j];
						if (d <= m + radius) nextnodes[node_index*MVP_BRANCHFACTOR+j] = true;
					}
					if (d > m - radius) nextnodes[node_index*MVP_BRANCHFACTOR+MVP_BRANCHFACTOR-1] = true;
				}
			}
		}

		delete[] currnodes;
		currnodes = nextnodes;
		n++;
	} while (n < MVP_LEVELSPERNODE);

	for (int i=0;i<MVP_FANOUT;i++){
		if (currnodes[i]){
			MVPNode *child = GetChildNode(i);
			if (child != NULL)
				childnodes[index*MVP_FANOUT+i] = child;
		}
	}

	delete[] currnodes;

}

const vector<DataPoint*> MVPInternal::PurgeDataPoints(){
	vector<DataPoint*> results;
	for (int i=0;i<m_nvps;i++){
		if (!m_vps[i]->active)
			delete m_vps[i];
		else
			results.push_back(m_vps[i]);
	}
	
	return results;
}


/********* MVPLeaf methods **********************/

MVPLeaf::MVPLeaf(){
	m_nvps = 0;
	for (int i=0;i<MVP_PATHLENGTH;i++)
		for (int j=0;j<MVP_LEAFCAP;j++)
			m_pdists[i][j] = -1.0;
}

void MVPLeaf::SelectVantagePoints(vector<DataPoint*> &points){
	while (m_nvps < MVP_PATHLENGTH && points.size() > 0){
		m_vps[m_nvps++] = points.back();
		points.pop_back();
	}
}

void MVPLeaf::MarkLeafDistances(vector<DataPoint*> &points){
	if (m_points.size() + points.size() > MVP_LEAFCAP)
		throw invalid_argument("no. points exceed leaf capacity");
	
	for (int m = 0; m < m_nvps;m++){
		int index = m_points.size();
		for (DataPoint *dp : points){
			m_pdists[m][index++] = PointDistance(m_vps[m], dp);
		}
	}
}

MVPNode* MVPLeaf::AddDataPoints(vector<DataPoint*> &points,
								map<int,vector<DataPoint*>*> &childpoints,
								const int level, const int index){
	SelectVantagePoints(points);
	MVPNode *retnode = this;
	if (m_points.size() + points.size() <= MVP_LEAFCAP){ 
		// add points to existing leaf
		MarkLeafDistances(points);
		for (DataPoint *dp : points){
			dp->active = true;
			m_points.push_back(dp);
		}
		points.clear();
	} else {  // create new internal node 

		// get existing points, purge inactive poins
		vector<DataPoint*> pts = PurgeDataPoints();

		// merge points
		for (DataPoint *dp : pts) points.push_back(dp);

		if (points.size() <= MVP_LEAFCAP + MVP_PATHLENGTH){
			// clear out points
			m_nvps = 0;
			m_points.clear();
			SelectVantagePoints(points);
			MarkLeafDistances(points);
			for (DataPoint *dp : points){
				dp->active = true;
				m_points.push_back(dp);
			}
			points.clear();
		} else {
			retnode = new MVPInternal();
			retnode = retnode->AddDataPoints(points, childpoints, level, index);
		}
	}
	if (retnode == NULL) throw runtime_error("unable to create node");
	
	return retnode;
}

const int MVPLeaf::GetCount()const{
	return m_points.size();
}

void MVPLeaf::SetChildNode(const int n, MVPNode* node){}

MVPNode* MVPLeaf::GetChildNode(const int n)const{return NULL;}

const vector<DataPoint*> MVPLeaf::GetVantagePoints()const{
	vector<DataPoint*> results;
	for (int i=0;i<m_nvps;i++) results.push_back(m_vps[i]);
	return results;
}

const vector<DataPoint*> MVPLeaf::GetDataPoints()const{
	vector<DataPoint*> results;
	for (DataPoint *dp : m_points) results.push_back(dp);
	return results;
}

const vector<DataPoint*> MVPLeaf::FilterDataPoints(const DataPoint *target, const double radius)const{
	vector<DataPoint*> results;

	double qdists[MVP_PATHLENGTH];
	for (int i=0;i<m_nvps;i++){
		qdists[i] = PointDistance(m_vps[i], target);
		if (m_vps[i]->active && qdists[i] <= radius){
			results.push_back(m_vps[i]);
		}
	}
	
	for (int j=0;j < (int)m_points.size();j++){
		bool skip = false;
		if (!m_points[j]->active) continue;
		
		for (int i=0;i<m_nvps;i++){
			if (!(m_pdists[i][j] >= qdists[i] - radius) && (m_pdists[i][j] <= qdists[i] + radius)){
				skip = true;
				break;
			}
		}
		if (!skip){
			double d = PointDistance(m_points[j], target);
			if (d <= radius){
				results.push_back(m_points[j]);
			}
		}
	}
	return results;
}


void MVPLeaf::TraverseNode(const DataPoint &target, const double radius,
						   map<int, MVPNode*> &childnodes,
						   const int index, list<QueryResult> &results)const{
	double qdists[MVP_PATHLENGTH];
	for (int i=0;i<m_nvps;i++){
		qdists[i] = PointDistance(m_vps[i], &target);
		if (m_vps[i]->active && qdists[i] <= radius){
			QueryResult item;
			item.dp = m_vps[i];
			item.distance = qdists[i];
			InsertItemIntoList(results, item);
		}
	}
	
	for (int j=0;j < (int)m_points.size();j++){
		bool skip = false;
		if (!m_points[j]->active) continue;
		
		for (int i=0;i<m_nvps;i++){
			if (!(m_pdists[i][j] >= qdists[i] - radius) && (m_pdists[i][j] <= qdists[i] + radius)){
				skip = true;
				break;
			}
		}
		if (!skip){
			double d = PointDistance(m_points[j], &target);
			if (d <= radius){
				QueryResult item;
				item.dp = m_points[j];
				item.distance = d;
				InsertItemIntoList(results, item);
			}
		}
	}
}

const vector<DataPoint*> MVPLeaf::PurgeDataPoints(){
	vector<DataPoint*> results;
	for (int i=0;i<m_nvps;i++){
		if (!m_vps[i]->active)
			delete m_vps[i];
		else
			results.push_back(m_vps[i]);
	}
	for (DataPoint *dp : m_points){
		if (!dp->active)
			delete dp;
		else
			results.push_back(dp);
	}
	return results;
}


