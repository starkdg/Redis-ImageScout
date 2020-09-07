#ifndef _MVPNODE_H
#define _MVPNODE_H

#include <cstring>
#include <map>
#include <vector>
#include <cmath>
#include <algorithm>
#include <stdexcept>
#include "datapoint.hpp"

using namespace std;

class MVPNode {
public:
	MVPNode(){}

	virtual ~MVPNode(){};

	static MVPNode* CreateNode(vector<DataPoint*> &points,
							   map<int,vector<DataPoint*>*> &childpoints,
							   int level, int index);

	void InsertItemIntoList(list<QueryResult> &list, QueryResult &item)const;
	
	virtual MVPNode* AddDataPoints(vector<DataPoint*> &points,
								   map<int,vector<DataPoint*>*> &childpoints,
								   const int level, const int index) = 0;

	virtual const int GetCount()const = 0;

	virtual void SetChildNode(const int n, MVPNode *node) = 0;

	virtual MVPNode* GetChildNode(int n)const = 0;

	virtual const vector<DataPoint*> GetVantagePoints()const = 0;
	
	virtual const vector<DataPoint*> GetDataPoints()const = 0;

	virtual const vector<DataPoint*> FilterDataPoints(const DataPoint *target, const double radius)const = 0;

	virtual void TraverseNode(const DataPoint &target,
							  const double radius,
							  map<int, MVPNode*> &childnodes,
							  const int index,
							  list<QueryResult> &results)const = 0;

	virtual const vector<DataPoint*> PurgeDataPoints()=0;

};

class MVPInternal : public MVPNode {
private:
	int m_nvps;
	DataPoint* m_vps[MVP_LEVELSPERNODE];
	MVPNode* m_childnodes[MVP_FANOUT];
	double m_splits[MVP_LEVELSPERNODE][MVP_NUMSPLITS];

	void SelectVantagePoints(vector<DataPoint*> &points);
	
	void CalcSplitPoints(const vector<double> &dists, int n, int split_index);

	vector<double> CalcPointDistances(DataPoint &vp, vector<DataPoint*> &points);

	vector<DataPoint*>* CullPoints(vector<DataPoint*> &list, vector<double> &dists,
								  double split, bool less);
		
	void CollatePoints(vector<DataPoint*> &points,
					   map<int, vector<DataPoint*>*> &childpoints,
					   const int level, const int index);
	
public:
	MVPInternal();
	~MVPInternal(){};
	MVPNode* AddDataPoints(vector<DataPoint*> &points,
						   map<int,vector<DataPoint*>*> &childpoints,
						   const int level, const int index);

	const int GetCount()const;

	void SetChildNode(const int n, MVPNode *node);

	MVPNode* GetChildNode(const int n)const;

	const vector<DataPoint*> GetVantagePoints()const;
	
	const vector<DataPoint*> GetDataPoints()const;

	const vector<DataPoint*> FilterDataPoints(const DataPoint *target, const double radius)const;

	void TraverseNode(const DataPoint &target,const double radius,
							  map<int, MVPNode*> &childnodes,
							  const int index,
							  list<QueryResult> &results)const;

	const vector<DataPoint*> PurgeDataPoints();
};

class MVPLeaf : public MVPNode {
private:
	int m_nvps;
	DataPoint* m_vps[MVP_PATHLENGTH];
	double m_pdists[MVP_PATHLENGTH][MVP_LEAFCAP];
	vector<DataPoint*> m_points;

	void SelectVantagePoints(vector<DataPoint*> &points);

	void MarkLeafDistances(vector<DataPoint*> &points);
	
public:
	MVPLeaf();
	~MVPLeaf(){};
	MVPNode* AddDataPoints(vector<DataPoint*> &points,
						   map<int,vector<DataPoint*>*> &childpoints,
						   const int level, const int index);

	const int GetCount()const;

	void SetChildNode(const int n, MVPNode *node);

	MVPNode* GetChildNode(const int n)const;

	const vector<DataPoint*> GetVantagePoints()const;

	const vector<DataPoint*> GetDataPoints()const;

	const vector<DataPoint*> FilterDataPoints(const DataPoint *target, const double radius)const;

	void TraverseNode(const DataPoint &target,const double radius,
					  map<int, MVPNode*> &childnodes,
					  const int index,
					  list<QueryResult> &results)const;

	const vector<DataPoint*> PurgeDataPoints();
};

#endif /* _MVPNODE_H */
