#include <cstdlib>
#include <iostream>
#include <iomanip>
#include <string>
#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>
#include "hiredis.h"
#include "pHash.h"

using namespace std;

namespace fs = boost::filesystem;
namespace po = boost::program_options;

struct Args {
	string cmd, host, key;
	fs::path dirname;
	int port;
	long long id;
	double radius;
};

Args ParseOptions(int argc, char **argv){
	Args args;
	po::options_description descr("Imgscout Options");

	try {
		descr.add_options()
			("help,h", "produce help message")
			("key,k", po::value<string>(&args.key)->required(), "redis key")
			("dir,d", po::value<fs::path>(&args.dirname)->default_value(""), "directory of images to process")
			("cmd,c", po::value<string>(&args.cmd)->required(), "command: add, sync, del, query, lookup, help")
			("server,s", po::value<string>(&args.host)->default_value("localhost"), "redis server address")
			("port,p", po::value<int>(&args.port)->default_value(6379), "redis server port")
			("radius,r", po::value<double>(&args.radius)->default_value(5), "query radius")
			("id,i", po::value<long long>(&args.id)->default_value(0), "id value for lookup");

		po::positional_options_description pd;
		pd.add("cmd", 1);

		po::variables_map vm;
		po::store(po::command_line_parser(argc, argv).options(descr).positional(pd).run(), vm);
		if (vm.count("help") || args.cmd == "help"){
			cout << descr << endl;
			exit(0);
		}

		if (args.cmd == "add" || args.cmd == "query"){
			if (args.cmd.empty()) {
				cout << "missing directory argument" << endl;
				cout << descr << endl;
				exit(0);
			}
			
		}

		if (args.cmd == "del" || args.cmd == "lookup"){
			if (args.id == 0){
				cout << "missing id arg" << endl;
				cout << descr << endl;
				exit(0);
			}
		}
		
		po::notify(vm);
	} catch(const po::error &ex){
		cout << ex.what() << endl;
		cout << descr << endl;
		exit(0);
	}
	return args;
}

/*-------- execute commmands -----------------*/

int SubmitFiles(redisContext *c, const string &key, const fs::path &dirname){
	fs::directory_iterator dir(dirname), end;
	
	int count = 0;
	for (;dir!= end;++dir){
		if (fs::is_regular_file(dir->status())){

			string filename = dir->path().string();
			cout << "(" << count++ << ") " << filename << endl;

			ulong64 hash_value;
			if (ph_dct_imagehash(filename.c_str(), hash_value) < 0){
				cout << "unable to calc hash fingerprint" << endl;
				continue;
			}

			redisReply *reply = (redisReply*)redisCommand(c, "imgscout.add %s %llu %s",
														  key.c_str(), hash_value, filename.c_str());
			if (reply && reply->type == REDIS_REPLY_INTEGER){
				cout << "=> id = " << reply->integer << endl;
			} else if (reply && reply->type == REDIS_REPLY_ERROR){
				cerr << "=> error: " << reply->str << endl;
			} else {
				cerr << "Disconnected" << endl;
				break;
			}

			freeReplyObject(reply);
		} else {
			cerr << "not a file: " << dir->path() << endl;
		}

	}
	return count;
}

int Sync(redisContext *c, const string &key){

	redisReply *reply = (redisReply*)redisCommand(c, "imgscout.sync %s", key.c_str());
	if (reply && reply->type == REDIS_REPLY_STATUS){
		cout << "=> sync'd" << endl;
	} else if (reply && reply->type == REDIS_REPLY_ERROR){
		cout << "Error: " << reply->str << endl;
	} else {
		cerr << "Disconnected" << endl;
		return 0;
	}

	freeReplyObject(reply);
	return 1;
}

int Size(redisContext *c, const string &key){

	redisReply *reply = (redisReply*)redisCommand(c, "imgscout.size %s", key.c_str());
	if (reply && reply->type == REDIS_REPLY_INTEGER){
		cout << "size " << reply->integer << " bytes" << endl;
	} if (reply && reply->type == REDIS_REPLY_ERROR){
		cout << "Error: " << reply->str << endl;
	} else {
		cerr << "Disconnected" << endl;
		return 0;
	}

	return 1;
}

int DeleteId(redisContext *c, const string &key, const long long id){

	redisReply *reply = (redisReply*)redisCommand(c, "imgscout.del %s %lld", key.c_str(), id);

	if (reply && reply->type == REDIS_REPLY_STRING){
		cout << "=> deleted " << id << endl;
	} else if (reply && reply->type == REDIS_REPLY_ERROR){
		cout << "Error: " << reply->str << endl;
	} else {
		cerr << "Disconnected" << endl;
		return -1;
	}
	
	freeReplyObject(reply);
	return 0;
}

void ProcessSubReply(redisReply *subreply){
	if (subreply->type == REDIS_REPLY_ARRAY && subreply->elements == 3){
		cout << "        descr = " << subreply->element[0]->str << endl;
		cout << "        id = " << subreply->element[1]->integer << endl;
		cout << "        distance = " << subreply->element[2]->str << endl;
	}
}

int QueryFiles(redisContext *c, const string &key, const fs::path &dirname, const double radius){
	fs::directory_iterator dir(dirname), end;
	
	int count = 0;
	for (;dir!=end;++dir){
		if (fs::is_regular_file(dir->status())){
			string filename = dir->path().string();

			ulong64 hash_value;
			if (ph_dct_imagehash(filename.c_str(), hash_value) < 0){
				cout << "unable to calc hash fingerprint" << endl;
				continue;
			}

			cout << "(" << ++count << ") query: " << filename << endl;
			redisReply *reply = (redisReply*)redisCommand(c, "imgscout.query %s %llu %f",
														  key.c_str(), hash_value, radius);
			if (reply && reply->type == REDIS_REPLY_ARRAY){
				for (int i=0;i<reply->elements;i++){
					cout << "    (" << i << ")" << endl;
					ProcessSubReply(reply->element[i]);
				}
			} else if (reply && reply->type == REDIS_REPLY_ERROR){
				cerr << "=> error: " << reply->str << endl;
			} else if (reply){
				cerr << "error: " << reply->str << endl;
			} else {
				cerr << "Disconnected" << endl;
				break;
			}

			freeReplyObject(reply);
		} else {
			cerr << "not a file: " << dir->path() << endl;
		}
	}
	
	return count;
}

int LookupId(redisContext *c, const string &key, const long long id){

	redisReply *reply = (redisReply*)redisCommand(c, "imgscout.lookup %s %lld", key.c_str(), id);
	if (reply && reply->type == REDIS_REPLY_STRING){
		cout << " => " << reply->str << endl;
	} else if (reply && reply->type == REDIS_REPLY_ERROR){
		cout << " Error: " << reply->str << endl;
	} else {
		cout << "Disconnected" << endl;
		return 0;
	}
	
	freeReplyObject(reply);
	return 1;
}

void print_header(){
	cout << endl << "------- Image Scout Client -----------" << endl << endl;
}

int main(int argc, char **argv){
	print_header();

	Args args = ParseOptions(argc, argv);
	
	cout << "Connect to " << args.host << ":" << args.port << endl;

	redisContext *c = redisConnect(args.host.c_str(), args.port);
	if (c == NULL || c->err){
		if (c) cerr << "Unable to connect: " << c->errstr << endl;
		else cerr << "Unable to connect to server" << endl;
		exit(0);
	}

	int n_files = 0;
	if (args.cmd == "add"){

		cout << "Submit files in " << args.dirname << endl;
		
		n_files = SubmitFiles(c, args.key, args.dirname);

		cout << "Successfully submitted " << n_files << " files " << endl;
		
	} else if (args.cmd == "del"){

		cout << "Delete " << args.id << endl;
		n_files = DeleteId(c, args.key, args.id);

	} else if (args.cmd == "query"){

		cout << "Query files in " << args.dirname << endl;
		n_files = QueryFiles(c, args.key, args.dirname, args.radius);
		cout << "Successfully queried " << n_files << " files " << endl;
		
	} else if (args.cmd == "lookup"){

		cout << "Lookup id " << args.id << endl;
		n_files = LookupId(c, args.key, args.id);
		
	} else if (args.cmd == "sync"){

		cout << "Sync database" << endl;
		Sync(c, args.key);
		
	} else {
		cerr << "Unrecognized command: " << args.cmd << endl;
	}
	
	cout << "Done." << endl;
	return 0;
}


