import os
import sys
os.system("javac "+sys.argv[1]+".java")
os.system("jar cvf storedprocs.jar "+sys.argv[1]+".class")
os.system("/home/yenkuanlee/voltdb/bin/sqlcmd --query=\"load classes storedprocs.jar\";")
os.system("/home/yenkuanlee/voltdb/bin/sqlcmd --query=\"drop procedure "+sys.argv[1]+"\"")
os.system("/home/yenkuanlee/voltdb/bin/sqlcmd --query=\"CREATE PROCEDURE from class "+sys.argv[1]+";\"")
