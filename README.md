# DataEngineerChallenge

This is an interview challenge for PayPay. Please feel free to fork. Pull Requests will be ignored.

The challenge is to make make analytical observations about the data using the distributed tools below.

## Result:

1. Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a session.
   https://en.wikipedia.org/wiki/Session_(web_analytics)

   - I started the session window time with 15 min as you suggested.
   - How do I defined the session:
     - Partition by IP (and UserAgent in code. But I found there are lot of rows lacking userAgent, so I ignored here.).
       - Removed the port. Since a browser can use multiple ports (Also not sure should I ignore port or not)
       - Define a session can not made by multiple agent at the same time.
   - I defined the session in code like ${IP}/${SESSION_COUNT}
     - The `SESSION_COUNT` is the aggregate result of new sessions made by this ip(/agent).
     - Every access does not have the last access in 15 min is defined to the new session.
     - The userAgent is not included in the session name for readability this time.
     - Filtered the error accesses from ELB code.

```
+-----------------+-----+
|sessionId        |count|
+-----------------+-----+
|52.74.219.71/5   |11060|
|119.81.61.166/6  |10540|
|119.81.61.166/8  |7817 |
|52.74.219.71/6   |6430 |
|106.186.23.95/10 |4657 |
|52.74.219.71/9   |4085 |
|52.74.219.71/10  |3684 |
```

2. Determine the average session time

```
+------------------+
|avgSessionTime    |
+------------------+
|2024.8234247635512|
+------------------+
```

3. Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.

I clenaed the urls to without params. Since the params should not be a part of URL I think.

```
+-----------------+--------------+
|sessionId        |uniqueVisitURL|
+-----------------+--------------+
|52.74.219.71/5   |9528          |
|119.81.61.166/6  |8014          |
|119.81.61.166/8  |5786          |
|52.74.219.71/6   |5477          |
|106.186.23.95/10 |4655          |
|119.81.61.166/1  |3334          |
|52.74.219.71/9   |2906          |
|119.81.61.166/9  |2841          |
```

4. Find the most engaged users, ie the IPs with the longest session times

```
+-----------------+--------------------------+--------------------------+-----------+
|sessionId        |sessionEnd                |sessionStart              |sessionTime|
+-----------------+--------------------------+--------------------------+-----------+
|117.239.35.226/1 |2015-07-22 21:10:00.182218|2015-07-22 02:40:21.166378|66579      |
|202.3.66.50/1    |2015-07-22 21:07:46.110244|2015-07-22 02:40:16.723114|66450      |
|37.228.106.100/1 |2015-07-22 21:07:12.360783|2015-07-22 02:40:07.895056|66425      |
|122.252.231.14/1 |2015-07-22 21:09:09.691522|2015-07-22 02:42:15.643603|66414      |
|103.232.128.27/1 |2015-07-22 21:07:38.406521|2015-07-22 02:41:23.45613 |66375      |
|121.243.26.254/1 |2015-07-22 21:06:36.925556|2015-07-22 02:40:28.668659|66368      |
|107.167.107.98/1 |2015-07-22 21:08:43.667131|2015-07-22 02:43:37.014695|66306      |
|125.20.10.170/1  |2015-07-22 21:06:47.816596|2015-07-22 02:42:03.200691|66284      |
```

## Tests

Simple test to check the result with different session interval value.

## Tools:

- Scala 2.12.10
- Spark 3.1.2
- Tested in Azure Databricks 8.4
