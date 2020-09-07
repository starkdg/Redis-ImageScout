#ifndef _DATAPOINT_H
#define _DATAPOINT_H

#include <cmath>
#include "defs.hpp"

using namespace std;

struct DataPoint {
	long long id;
	unsigned long long value;
	bool active;

	DataPoint():id(0),active(true){}
	
	DataPoint(const long long id, const double value):id(id),value(value),active(true){}

	DataPoint(const DataPoint &other){
		active = other.active;
		value = other.value;
	}
	
	DataPoint& operator=(const DataPoint &other){
		active = other.active;
		value = other.value;
		return *this;
	}
};

struct QueryResult {
	DataPoint *dp;
	double distance;
	QueryResult():dp(NULL),distance(0){};
	QueryResult(const QueryResult &other){
		dp = other.dp;
		distance = other.distance;
	}
	QueryResult& operator=(const QueryResult &other){
		dp = other.dp;
		distance = other.distance;
		return *this;
	}
};


#endif /* _DATAPOINT_H */ 
