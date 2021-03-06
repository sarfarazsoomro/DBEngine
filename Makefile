CC = g++ -O0 -Wno-deprecated -std=c++0x

tag = -i

ifdef linux
tag = -n
endif

test.out:  y.tab.o lex.yy.o DBInstance.o RelOpNode.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o BigQ.o InternalDBFile.o InternalDBFileHeap.o InternalDBFileSorted.o RelOp.o SelectFileOp.o SelectPipeOp.o ProjectOp.o SumOp.o JoinOp.o WriteOutOp.o DuplicateRemovalOp.o GroupByOp.o Function.o Statistics.o test.o
	$(CC) -o test.out  y.tab.o lex.yy.o DBInstance.o RelOpNode.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o BigQ.o InternalDBFile.o InternalDBFileHeap.o InternalDBFileSorted.o RelOp.o SelectFileOp.o SelectPipeOp.o ProjectOp.o SumOp.o JoinOp.o WriteOutOp.o DuplicateRemovalOp.o GroupByOp.o Function.o Statistics.o test.o -lfl -lpthread

#test.out:  y.tab.o lex.yy.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o BigQ.o InternalDBFile.o InternalDBFileHeap.o InternalDBFileSorted.o RelOp.o SelectFileOp.o SelectPipeOp.o ProjectOp.o SumOp.o JoinOp.o WriteOutOp.o DuplicateRemovalOp.o GroupByOp.o Function.o Statistics.o test.o
#	$(CC) -o test.out  y.tab.o lex.yy.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o BigQ.o InternalDBFile.o InternalDBFileHeap.o InternalDBFileSorted.o RelOp.o SelectFileOp.o SelectPipeOp.o ProjectOp.o SumOp.o JoinOp.o WriteOutOp.o DuplicateRemovalOp.o GroupByOp.o Function.o Statistics.o test.o -lfl -lpthread
	
a2-2test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o InternalDBFile.o InternalDBFileHeap.o InternalDBFileSorted.o Pipe.o y.tab.o lex.yy.o a2-2test.o
	$(CC) -o a2-2test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o InternalDBFile.o InternalDBFileHeap.o InternalDBFileSorted.o Pipe.o y.tab.o lex.yy.o a2-2test.o -lfl -lpthread
	
a2test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o a2-test.o
	$(CC) -o a2test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o a2-test.o -lfl -lpthread
	
a1test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o y.tab.o lex.yy.o a1-test.o
	$(CC) -o a1test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o y.tab.o lex.yy.o a1-test.o -lfl
	
test.o: test.cc
	$(CC) -g -c test.cc
	
a2-2test.o: a2-2test.cc
	$(CC) -g -c a2-2test.cc

a2-test.o: a2-test.cc
	$(CC) -g -c a2-test.cc

a1-test.o: a1-test.cc
	$(CC) -g -c a1-test.cc

Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc
	
ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc
	
DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc

InternalDBFile.o: InternalDBFile.cc
	$(CC) -g -c InternalDBFile.cc
	
InternalDBFileHeap.o: InternalDBFileHeap.cc
	$(CC) -g -c InternalDBFileHeap.cc

InternalDBFileSorted.o: InternalDBFileSorted.cc
	$(CC) -g -c InternalDBFileSorted.cc

Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc

BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc

RelOp.o: RelOp.cc
	$(CC) -g -c RelOp.cc
	
ProjectOp.o: ProjectOp.cc
	$(CC) -g -c ProjectOp.cc
	
SelectFileOp.o: SelectFileOp.cc
	$(CC) -g -c SelectFileOp.cc
	
SelectPipeOp.o: SelectPipeOp.cc
	$(CC) -g -c SelectPipeOp.cc	

SumOp.o: SumOp.cc
	$(CC) -g -c SumOp.cc	
	
JoinOp.o: JoinOp.cc
	$(CC) -g -c JoinOp.cc

WriteOutOp.o: WriteOutOp.cc
	$(CC) -g -c WriteOutOp.cc

DuplicateRemovalOp.o: DuplicateRemovalOp.cc
	$(CC) -g -c DuplicateRemovalOp.cc

GroupByOp.o: GroupByOp.cc
	$(CC) -g -c GroupByOp.cc

Statistics.o: Statistics.cc
	$(CC) -g -c Statistics.cc

Function.o: Function.cc
	$(CC) -g -c Function.cc

File.o: File.cc
	$(CC) -g -c File.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc

DBInstance.o: DBInstance.cc
	$(CC) -g -c DBInstance.cc	

QueryPlan.o: QueryPlan.cc
	$(CC) -g -c QueryPlan.cc
	
RelOpNode.o: RelOpNode.cc
	$(CC) -g -c RelOpNode.cc

y.tab.o: Parser.y
	yacc -d Parser.y
	sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c y.tab.c

lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c


clean: 
	rm -f *.o
	rm -f *.out
	rm -f y.tab.*
	rm -f yyfunc.tab.*
	rm -f lex.yy.*
	rm -f lex.yyfunc*
