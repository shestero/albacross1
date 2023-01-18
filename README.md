# **Albacross Data Team Recruitment Task**
*Michael Shestero. 17 January, 2023*

Please, find here my solution.
The solution is written using Scala 2.13.10 and Spark 3.3.1 .

**Key implementation features**

For performance and practical reasons IPs are represented as unsigned long integers.

I pay attention not to store amounts of data in memory when it possible, to emphasize it I use Iterator as most primitive way to work with data.

Methods *checkOrder* and *checkIntervalOrder* are only to corresponding debug checks and validate input and don’t change any data.

ForkIterator is my know-how – it’s to asynchronously feed the several Iterator’s consumers (which works in different threads) without storing data in the memory.

**Algorithm**

To put it simply we are to pass through only those IPs that are inside a range but in one only. 
If we represent the ranges on the ordered IP line we can move from the smallest IP to the  biggest paying attention to the “level” of overlapping and keep only those parts of ranges that have level = 1.

This is implemented in the **solution1** algorithm.

Note that it require to sort all input IP which also mean to store all input data into memory, so it isn’t good BigData solution.

If we have ordered input ranges we can optimize the algorithm storing in the memory only the different ends of currently overlapping input ranges. This is implemented in the **solution2** algorithm. It require presorted input.

**Big data algorithm**

***Lemma 1*:** Let’s split the input data by some IP into two parts (some input ranges may be split into two ones). Now if we perform the removing intersections on each of the part and combine the two results we will have the output ranges that is cover the same set of IP.
Proof: the cutting doesn’t change the count of overlapping ranges for any IP.

***Lemma 2*:** If we hadn’t cut any input range in Lemma1 we will have the exact same output range set as we will have if we perform the task over the whole input data.

***Lemma 3a*:** If we had cut apart any input range in Lemma1 and there is no output ranges right around cutting point, we still have the exact same output range set.

***Lemma 3b***: If we had cut apart any input range in Lemma1 and there are two output ranges right around cutting point, we have to unite them into one to make the exact same output range set. (If it ever have sense in the scope of end task, because the set of IP is the same).

**Theorem:** We can split input data into partitions by-range (into "ranges of ranges") and do the tasks at nodes apart, then we can combine the results joining the output ranges between partitions as described in Lemma3(a,b).

This is implemented in the **solution3** algorithm, that use Spark.

**Deployment**

My program writes input and output data to MySQL tables.

You should create the MySQL database albacross1 (the data scheme is in [albacross1.sql](https://github.com/shestero/albacross1/blob/main/albacross1.sql) file) 
and grant write permissions for db user (ALBACROSS_DB_USER environment variable, *root* by default) without password to the tables ***input*** and ***output***. 
Both tables contain IP4 ranges as two unsigned long integers (there is set id also present). To see them in human-readable form views ***sinput*** and ***soutput*** provided.

Output is also sent into ElastiSearch (default index is ***albacross***). 

**How to deploy, build and run**

Get source from github: git clone [https://github.com/shestero/albacross1](https://github.com/shestero/albacross1)

Using docker compose:

    cd albacross1
    docker-compose up

You are to stop local mysql and elasticsearch services before run this to avoid the conflicts.
The packages to be loaded and my code is to be built from sources, so it takes a time.
To shut down services run `docker-compose down` in this folder (from another terminal window).

Host-system run with manual deploy: 

    cd albacross1
    sbt run
You'll need local MySQL with database albacross deployed from [albacross1.sql](https://github.com/shestero/albacross1/blob/main/albacross1.sql) image and *root* user without password. You may specify your user name using ALBACROSS_DB_USER environment variable.
The program will also try to write data into local ElastiSearch.

See also [Polynote 0.5.0](https://github.com/polynote/polynote/releases) notebook with algorithm-only code (without Spark):
[albacross1.ipynb](https://github.com/shestero/albacross1/blob/main/albacross1.ipynb)


