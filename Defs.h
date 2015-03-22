#ifndef DEFS_H
#define DEFS_H

#include <string>
#include <sstream>
#include <stdlib.h>

using namespace std;

#define MAX_ANDS 20
#define MAX_ORS 20

#define PAGE_SIZE 131072

enum Target {Left, Right, Literal};
enum CompOperator {LessThan, GreaterThan, Equals};
enum Type {Int, Double, String};


unsigned int Random_Generate();

typedef enum {heap, sorted, tree} fType;
enum OperationType {READ, WRITE};


//http://www.cplusplus.com/forum/articles/9645/
template <typename T>
string NumberToString ( T Number )
{
	stringstream ss;
	ss << Number;
	return ss.str();
}

template <typename T>
T StringToNumber ( const string &Text )//Text not by const reference so that the function can be used with a 
{                               //character array as argument
	stringstream ss(Text);
	T result;
	return ss >> result ? result : 0;
}

//class RandStr {
//private:
//public:
//    char* randStr(const int len);
//};
//
////http://stackoverflow.com/questions/440133/how-do-i-create-a-random-alpha-numeric-string-in-c
//char * RandStr::randStr(const int len) {
//    
//    char *s = new char[len];
//    static const char alphanum[] =
//        "0123456789"
//        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
//        "abcdefghijklmnopqrstuvwxyz";
//    
//    for (int i = 0; i < len; ++i) {
//        s[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
//    }
//
//    s[len] = 0;
//    return s;    
//}
#endif

