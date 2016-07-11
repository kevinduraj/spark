1 i \
{\
 "cells": [
$ i \
{ \
"cell_type": "code", \
"execution_count": null, \
"metadata": { \
"collapsed": true \
},\
"outputs": [],\
"source": [ ] \
}\
],\
 "metadata": {\
  "kernelspec": {\
   "display_name": "Spark-DSE Cluster",\
   "language": "scala",\
   "name": "spark-dse-cluster"\
  },\
  "language_info": {\
   "name": "scala"\
  }\
 },\
 "nbformat": 4,\
 "nbformat_minor": 0\
}
/^\/\/ COMMAND/ { s/.*//;
x
s/,$//
i\
{
t foo
:foo
s/%md//
t markdown
i\
"cell_type": "code",
b cont1
:markdown
i\
"cell_type": "markdown",
:cont1

i\
   "execution_count": null,\
   "metadata": {\
    "collapsed": true\
   },\
   "outputs": [],\
   "source": [
s/^\n*/"/
s/\n*$/"/
s/\n/\\n","/g
p
i\
]\
},
s/.*//
}
s/^\/\/ MAGIC //
s/"/\\"/g
s/	/\\t/g
H
