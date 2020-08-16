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

#endif /* _DATAPOINT_H */ 
