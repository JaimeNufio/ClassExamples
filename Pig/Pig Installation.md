# Pig Installation

### Book for pig: Programming.Pig.pdf

* Download Pig (pig-0.16.0.tar.gz) from a mirror site
http://apache.mirrors.pair.com/pig/pig-0.16.0/


* Set environment path:

```
export PIG_HOME=<Installation directory of Pig>
export PATH=$PIG_HOME/bin:$PATH
```

* Verifying the installation

```
pig -version
```

* Run pig (default is mapreduce mode, unless specify pig -x local) to enter grunt shell

```
pig
```

Execution modes in Apache Pig:

(1) MapReduce Mode – This is the default mode, which requires access to a Hadoop cluster and HDFS installation. Since, this is a default mode, it is not necessary to specify -x flag ( you can execute pig OR pig -x mapreduce). The input and output in this mode are present on HDFS.

(2) Local Mode – With access to a single machine, all files are installed and run using a local host and file system. Here the local mode is specified using ‘-x flag’ (pig -x local). The input and output in this mode are present on local file system.

* Exit from grunt shell

```
quit
```


You might see error message like this:

```
2019-07-26 11:26:04,400 [main] INFO  org.apache.hadoop.mapred.ClientServiceDelegate - Application state is completed. FinalApplicationStatus=SUCCEEDED. Redirecting to job history server
2019-07-26 11:26:05,404 [main] INFO  org.apache.hadoop.ipc.Client - Retrying connect to server: 0.0.0.0/0.0.0.0:10020. Already tried 0 time(s); retry policy is RetryUpToMaximumCountWithFixedSleep(maxRetries=10, sleepTime=1000 MILLISECONDS)
```

which means that the job history server is not running, simply solve this by:

```
mr-jobhistory-daemon.sh start historyserver
```


* Example

Download truck_event_text_partition.csv and drivers.csv
Copy files from your local directory to HDFS

```
hdfs dfs -put truck_event_text_partition.csv /input
hdfs dfs -put drivers.csv /input
```

Run command in grunt shell:

```
truck_events = LOAD '/input/truck_event_text_partition.csv' USING PigStorage(',');
DESCRIBE truck_events;
```

Save commands in a pig file, e.g., truck.pig, and run:

```
pig -f truck.pig
```

You will see "Schema for truck_events unknown" because we did not define one when loading the data into relation truck_events.


* Define a relation with a schema:

```
truck_events = LOAD '/input/truck_event_text_partition.csv' USING PigStorage(',')
AS (driverId:int, truckId:int, eventTime:chararray,
eventType:chararray, longitude:double, latitude:double,
eventKey:chararray, correlationId:long, driverName:chararray,
routeId:long,routeName:chararray,eventDate:chararray);

DESCRIBE truck_events;
```

Output:

```
truck_events: {driverId: int,truckId: int,eventTime: chararray,eventType: chararray,longitude: double,latitude: double,eventKey: chararray,correlationId: long,driverName: chararray,routeId: long,routeName: chararray,eventDate: chararray}
```

* Define a new relation from an existing relation

You can define a new relation based on an existing one. For example, define the following truck_events_subset relation, which is a collection of 100 entries (arbitrarily selected) from the truck_events relation.
Add the following line to the end of your code:

```
truck_events_subset = LIMIT truck_events 100;
DESCRIBE truck_events_subset;
```

Notice truck_events_subset has the same schema as truck_events, because truck_events_subset is a subset of truck_events relation.

* View the data

To view the data of a relation, use the DUMP command.
Add the following DUMP command to your Pig script, then save and execute it again:

```
DUMP truck_events_subset;
```

* Select specific columns from a relation

One of the key uses of Pig is data transformation. You can define a new relation based on the fields of an existing relation using the FOREACH command. Define a new relation specific_columns, which will contain only the driverId, eventTime and eventType from relation truck_events_subset.

```
specific_columns = FOREACH truck_events_subset GENERATE driverId, eventTime, eventType;
DESCRIBE specific_columns;
DUMP specific_columns;
```

* Store relationship data into a HDFS file

In this step, you will use the STORE command to output a relation into a new file in HDFS. Enter the following command to output the specific_columns relation to a folder named output/specific_columns:

```
STORE specific_columns INTO '/output/specific_columns' USING PigStorage(',');
```

You might see error message like this:

```
2019-07-26 11:26:04,400 [main] INFO  org.apache.hadoop.mapred.ClientServiceDelegate - Application state is completed. FinalApplicationStatus=SUCCEEDED. Redirecting to job history server
2019-07-26 11:26:05,404 [main] INFO  org.apache.hadoop.ipc.Client - Retrying connect to server: 0.0.0.0/0.0.0.0:10020. Already tried 0 time(s); retry policy is RetryUpToMaximumCountWithFixedSleep(maxRetries=10, sleepTime=1000 MILLISECONDS)
```

which means that the job history server is not running, simply solve this by:

```
mr-jobhistory-daemon.sh start historyserver
```

Copy the output files from HDFS to your local directory:

```
hdfs dfs -get /output/specific_columns ./
```


* Perform a JOIN between two relations

Perform a join on two driver statistics data sets: truck_event_text_partition.csv and the driver.csv files. Define a new relation named drivers then join truck_events and drivers by driverId:

```
drivers =  LOAD '/input/drivers.csv' USING PigStorage(',')
AS (driverId:int, name:chararray, ssn:chararray,
location:chararray, certified:chararray, wage_plan:chararray);

join_data = JOIN  truck_events BY (driverId), drivers BY (driverId);
DESCRIBE join_data;
```


* Sort the data using "ORDER BY"

Use the ORDER BY command to sort a relation by one or more of its fields. 

```
ordered_data = ORDER drivers BY name asc;
DUMP ordered_data;
```

* Filter by a column value

```
B = FILTER specific_columns BY driverId > 20;
dump B;
```

* Filter and group the data using "GROUP BY"

The GROUP command allows you to group a relation by one of its fields. Group the truck_events relation by the driverId for the eventType which are not ‘Normal’.

```
filtered_events = FILTER truck_events BY NOT (eventType MATCHES 'Normal');
grouped_events = GROUP filtered_events BY driverId;
DESCRIBE grouped_events;
DUMP grouped_events;
```

* If pig script hangs, configure yarn-site.xml, mapred-site.xml 

- yarn-site.xml

```
<configuration>
    <property>
      <name>yarn.nodemanager.aux-services</name>
      <value>mapreduce_shuffle</value>
      <description>shuffle service that needs to be set for Map Reduce to run </description>
    </property>
  <property> 
    <name>yarn.scheduler.minimum-allocation-mb</name> 
    <value>512</value>
  </property>
  <property> 
    <name>yarn.scheduler.maximum-allocation-mb</name> 
    <value>1024</value>
  </property>
  <property> 
    <name>yarn.nodemanager.resource.memory-mb</name> 
    <value>1024</value>
  </property>
</configuration>
```

- mapred-site.xml

```
<configuration>
  <property> 
    <name>mapreduce.framework.name</name> 
    <value>yarn</value> 
  </property>
<property>  
    <name>yarn.app.mapreduce.am.resource.mb</name>  
    <value>512</value>
</property>
<property> 
    <name>yarn.app.mapreduce.am.command-opts</name> 
    <value>-Xmx409m</value>
</property>
<property>
    <name>mapreduce.map.memory.mb</name>
    <value>512</value>
</property>
<property>
    <name>mapreduce.reduce.memory.mb</name>
    <value>512</value>
</property>
<property>
    <name>mapreduce.map.java.opts</name>
    <value>-Xmx409m</value>
</property>
<property>
    <name>mapreduce.reduce.java.opts</name>
    <value>-Xmx409m</value>
</property>
</configuration>

```

- capacity-scheduler.xml 

Change the yarn.scheduler.capacity.maximum-am-resource-percent <value> to 100





* [More basic Pig commands](https://pig.apache.org/docs/latest/basic.html)

* Referencing relations

```
A = LOAD '/input/student.csv' USING PigStorage(',') AS (name:chararray, age:int, gpa:float);
DUMP A;

```

Fields are referred to by positional notation or by name (alias).

Positional notation is indicated with the dollar sign ($) and begins with zero (0); for example, $0, $1, $2.

Names are assigned by you using schemas. You can use any name that is not a Pig keyword (see Identifiers for valid name examples).


|           | First Field | Second Field | Third Field|
|-----------|-------------|--------------|------------|
| Data type | chararray | int | float |
| Positional notation | $0 | $1 | $2 |
| Possible name | name | age | gpa |
| Field value (for the first tuple)| John | 18 | 4.0 |


```
X = FOREACH A GENERATE name,$2;
DUMP X;
```

* Data Types


| Complex Types | Description | Example |
|---------------|-------------|---------|
| tuple | An ordered set of fields | (19,2) |
| bag | An collection of tuples | {(19,2),(18,1)} |
| map | A set of key value pairs | [open#apache] |



* Reference fields that are complex data types

In this example the data file contains tuples. A schema for complex data types (in this case, tuples) is used to load the data.


```
A = LOAD '/input/complexdata.csv' USING PigStorage(' ') AS (t1:tuple(t1a:int, t1b:int,t1c:int),t2:tuple(t2a:int,t2b:int,t2c:int));
DUMP A;

X = FOREACH A GENERATE t1.t1a,t2.$0;
DUMP X;
```

Example: Outer Bag

In this example A is a relation or bag of tuples. You can think of this bag as an outer bag.

```
A = LOAD '/input/tupledata.csv' USING PigStorage(',') as (f1:int, f2:int, f3:int);
DUMP A;
```

output:

```
(1,2,3)
(4,2,1)
(8,3,4)
(4,3,3)
```

Example: Inner Bag

Now, suppose we group relation A by the first field to form relation X.

In this example X is a relation or bag of tuples. The tuples in relation X have two fields. The first field is type int. The second field is type bag; you can think of this bag as an inner bag.


```
X = GROUP A BY f1;
DUMP X;
```

output:

```
(1,{(1,2,3)})
(4,{(4,2,1),(4,3,3)})
(8,{(8,3,4)})
```


Map Example:

```
A = LOAD '/input/mapdata1.csv' AS (f1:int, f2:map[]);
DUMP A;
```

output:

```
([open#apache])
([apache#hadoop])
```


### Pig streaming

a = load '/input/test10.txt';
b = stream a through `python -c "import sys; print sys.stdin.read().replace ('Apple','Orange');"`;
dump b;

c = stream a through `cut -f2,2`;
dump c;

* Invoke perl or python script from pig and execute it

in shell:
py_test.py
#!/usr/bin/env python
import sys
print(sys.stdin.read().replace('Apple','Orange'))

in pig:

```
define py_test `py_test.py` cache ('/input/py_test.py');
d = stream a through py_test;
dump d;
```



### Word count example

code:

```
lines = LOAD '/input/wordcount_pig.txt' AS (line:chararray);
words = FOREACH lines GENERATE TOKENIZE(line);
dump words;
```

output:

```
({(This),(is),(a),(hadoop),(post)})
({(hadoop),(is),(a),(bigdata),(technology)})
```

code:

```
words = FOREACH lines GENERATE FLATTEN(TOKENIZE(line)) as word;
dump words;
```

output:

```
(This)
(is)
(a)
(hadoop)
(post)
(hadoop)
(is)
(a)
(bigdata)
(technology)
```

code:

```
grouped = GROUP words BY word;
dump grouped;
describe grouped;
```

output:

```
(a,{(a),(a)})
(is,{(is),(is)})
(This,{(This)})
(post,{(post)})
(hadoop,{(hadoop),(hadoop)})
(bigdata,{(bigdata)})
(technology,{(technology)})
```

code:

```
wordcount = FOREACH grouped GENERATE group, COUNT(words);
DUMP wordcount;
```

output:

```
(a,2)
(is,2)
(This,1)
(post,1)
(hadoop,2)
(bigdata,1)
(technology,1)
```


* Save this to wordcount.pig and run in bash:
```
pig -f wordcount2.pig
```

```
lines = LOAD '/input/SampleTextFile_1000kb.txt' AS (line:chararray);
words = FOREACH lines GENERATE FLATTEN(TOKENIZE(line)) as word;
grouped = GROUP words BY word;
wordcount = FOREACH grouped GENERATE group, COUNT(words);
DUMP wordcount;
```


### Compute average number of page visits by user

* load the log file
visits = load '/input/visitslog.txt' as (user,url,time);
* group based on the user field
user_visits = group visits by user;
describe user_visits;
* count the group
user_cnts = foreach user_visits generate group as user, COUNT(visits) as numvisits;
describe user_cnts;
* calculate average for all users
all_cnts = group user_cnts all;
avg_cnt = foreach all_cnts generate AVG(user_cnts.numvisits);
* use illustrate operator to review how data is transformed through a sequence of Pig Latin statements.
illustrate avg_cnt;
dump avg_cnt;
* use explain operator to review the logical plan, physical plan, and the M/R plan
explain avg_cnt;

output:
(3.0)


### Identify users who visit "good pages"

visits = load '/input/visitslog.txt' as (user:chararray, url:chararray, time:chararray);
pages = load '/input/pageslog.txt' as (url:chararray, pagerank:float);

* Join tables based on url
visits_pages = join visits by url, pages by url;

* group based on user
user_visits = group visits_pages by user;

* calculate average pagerank of user-visited pages
user_avgpr = foreach user_visits generate group, AVG(visits_pages.pagerank) as avgpr;

* filter user who has average pagerank greater than 0.5
good_users = filter user_avgpr by avgpr > 0.5f;

* store the result
store good_users into '/input/goodusers';


### Writing Java UDFs

```
package myudfs;
import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;


public class UPPER extends EvalFunc<String>
	{
	   public String exec(Tuple input) throws IOException {
	      if (input == null || input.size() == 0 || input.get(0) == null)
	           return null;
	        try{
	            String str = (String)input.get(0);
	           return str.toUpperCase();
	        }catch(Exception e){
	            throw new IOException("Caught exception processing input row ", e);
	        }
	    }
	  }

```

Build path -> Add external jar (commons-lang-2.6.jar, commons-logging-1.1.3.jar, pig-0.16.0-core-h2.jar, hadoop-common-2.6.5.jar)

export java file -> export generated class files and resources and export java source files and resources, 

* check jar structure:
jar -tf myudfs.jar 

META-INF/MANIFEST.MF
.project
myudfs/UPPER.class
myudfs/UPPER.java
.classpath


In pig:

REGISTER file:/Users/yaoshen/Documents/Misc/Teaching/CS644BigData/Formal/Week9/myudfs.jar;

A = load '/input/student_data' as (name:chararray, age:int, gpa:float);       
B = foreach A generate myudfs.UPPER(name);

dump B;













