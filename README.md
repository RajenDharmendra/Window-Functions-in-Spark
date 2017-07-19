# Window-Functions-in-Spark
Window API in Spark SQL
Spark introduced window API in 1.4 version to support smarter grouping functionalities. 
They are very useful for people coming from SQL background. One of the missing window API was ability to create windows using time. Time plays an important role in many industries like finance, telecommunication where understanding the data depending upon the time becomes crucial.

In Spark 2.0, framework has introduced built in support for time windows. These behave very similar to time windows in spark-streaming.

# Time Series Data

Before we start doing time window, we need to have access to a time series data. For my example, I will be using data of Apple stock from 1980 to 2016. You can access the data here. The original source of data is yahoo finance.

The data has six columns. Out of those six, we are only interested in Date, which signifies the date of trade and Close which signifies end of the day value.

# Importing time series data to DataFrame

Once we have time series data, we need to import it to dataframe. All the time window API’s need a column with type timestamp. Luckily spark-csv package can automatically infer the date formats from data and create schema accordingly. The below code is for importing with schema inference.

    import org.apache.spark.sql._
    import org.apache.spark.sql.SQLContext._
    val retailAllData = sqlContext.read.format("com.databricks.spark.csv")
                        .option("header","true")
                        .option("inferSchema","true").
                        load("/path/tofile/appleatock.csv")
                        
                        
# Find weekly average in 2016

Once we have data is represented as dataframe, we can start doing time window analysis. In our analysis, we want to find weekly average of the stock for 2016. The below are the steps to do that.

  <h3 id="step-1--filter-data-for-2016">Step 1 : Filter data for 2016</h3>

<p>As we are interested only in 2016, we need to filter the data for 2016. The below code show how to filter data on time.</p>

<div class="highlight"><pre><code class="language-scala" data-lang="scala"><span class="k">val</span> <span class="n">stocks2016</span> <span class="k">=</span> <span class="n">stocksDF</span><span class="o">.</span><span class="n">filter</span><span class="o">(</span><span class="s">&quot;year(Date)==2016&quot;</span><span class="o">)</span></code></pre></div>

<p>We can use builtin function year, as Date is already represented as a timestamp.</p>

<h3 id="step-2--tumbling-window-to-calculate-average">Step 2 : Tumbling window to calculate average</h3>

<p>Once we have filtered data, we need to create window for every 1 week. This kind of discretization of data is called as a tumbling window.</p>

<div class="highlight"><pre><code class="language-scala" data-lang="scala"><span class="k">val</span> <span class="n">tumblingWindowDS</span> <span class="k">=</span> <span class="n">stocks2016</span>
      <span class="o">.</span><span class="n">groupBy</span><span class="o">(</span><span class="n">window</span><span class="o">(</span><span class="n">stocks2016</span><span class="o">.</span><span class="n">col</span><span class="o">(</span><span class="s">&quot;Date&quot;</span><span class="o">),</span><span class="s">&quot;1 week&quot;</span><span class="o">))</span>
      <span class="o">.</span><span class="n">agg</span><span class="o">(</span><span class="n">avg</span><span class="o">(</span><span class="s">&quot;Close&quot;</span><span class="o">).</span><span class="n">as</span><span class="o">(</span><span class="s">&quot;weekly_average&quot;</span><span class="o">))</span></code></pre></div>

<p>The above code show how to use time window API. Window is normally used inside a group by. The first parameter signifies which column needs to be treated as time. Second parameter signifies the window duration. Window duration can be seconds, minutes, hours, days or weeks.</p>

<p>Once we have created window, we can run an aggregation like average as shown in the code.</p>

<h3 id="step-3--printing-the-window-values">Step 3 : Printing the window values</h3>

<p>Once we calculated the time window, we want to see the result.</p>

<div class="highlight"><pre><code class="language-scala" data-lang="scala"><span class="n">printWindow</span><span class="o">(</span><span class="n">tumblingWindowDS</span><span class="o">,</span><span class="s">&quot;weekly_average&quot;</span><span class="o">)</span></code></pre></div>

<p>The above code uses a helper function called <em>printWindow</em> which takes aggregated window dataframe and aggregated column name. The helper function looks as follows.</p>

<div class="highlight"><pre><code class="language-scala" data-lang="scala"><span class="k">def</span> <span class="n">printWindow</span><span class="o">(</span><span class="n">windowDF</span><span class="k">:</span><span class="kt">DataFrame</span><span class="o">,</span> <span class="n">aggCol</span><span class="k">:</span><span class="kt">String</span><span class="o">)</span> <span class="o">={</span>
    <span class="n">windowDF</span><span class="o">.</span><span class="n">sort</span><span class="o">(</span><span class="s">&quot;window.start&quot;</span><span class="o">).</span>
    <span class="n">select</span><span class="o">(</span><span class="s">&quot;window.start&quot;</span><span class="o">,</span><span class="s">&quot;window.end&quot;</span><span class="o">,</span><span class="n">s</span><span class="s">&quot;$aggCol&quot;</span><span class="o">).</span>
    <span class="n">show</span><span class="o">(</span><span class="n">truncate</span> <span class="k">=</span> <span class="kc">false</span><span class="o">)</span>
 <span class="o">}</span></code></pre></div>

<p>In above function, we are sorting dataframe using <em>window.start</em>. This column signifies the start time of window. This sorting helps us to understand the output better. Once we have sorted, we print start,end, aggregated value. As the timestamp can be long, we tell the show not to truncate results for better display.</p>

<p>When you run the example, we see the below result.</p>

<pre><code>+---------------------+---------------------+------------------+
|start                |end                  |weekly_average    |
+---------------------+---------------------+------------------+
|2015-12-31 05:30:00.0|2016-01-07 05:30:00.0|101.30249774999999|
|2016-01-07 05:30:00.0|2016-01-14 05:30:00.0|98.47199859999999 |
|2016-01-14 05:30:00.0|2016-01-21 05:30:00.0|96.72000125000001 |
|2016-01-21 05:30:00.0|2016-01-28 05:30:00.0|97.6719984        |

</code></pre>

<p>One thing you may observe is the date is started from 31st and first week is considered till 7. But if you go through the data, the first entry for 2016 start from 2016-01-04. The reason is there was no trading on 1st as it’s new year, 2 and 3 as they are weekend.</p>

<p>We can fix this by specifying the start time for window, which signifies the offset from which window should start.</p>

<h2 id="time-window-with-start-time">Time window with start time</h2>

<p>In earlier code, we used a tumbling window. In order to specify start time we need to use a sliding window. As of now, there is no API which combines tumbling window with start time. We can create tumbling window effect by keeping both window duration and slide duration same.</p>

<div class="highlight"><pre><code class="language-scala" data-lang="scala"><span class="k">val</span> <span class="n">windowWithStartTime</span> <span class="k">=</span> <span class="n">stocks2016</span><span class="o">.</span><span class="n">groupBy</span><span class="o">(</span><span class="n">window</span><span class="o">(</span><span class="n">stocks2016</span><span class="o">.</span><span class="n">col</span><span class="o">(</span><span class="s">&quot;Date&quot;</span><span class="o">),</span>
                          <span class="s">&quot;1 week&quot;</span><span class="o">,</span><span class="s">&quot;1 week&quot;</span><span class="o">,</span> <span class="s">&quot;4 days&quot;</span><span class="o">))</span>
                          <span class="o">.</span><span class="n">agg</span><span class="o">(</span><span class="n">avg</span><span class="o">(</span><span class="s">&quot;Close&quot;</span><span class="o">).</span><span class="n">as</span><span class="o">(</span><span class="s">&quot;weekly_average&quot;</span><span class="o">))</span></code></pre></div>

<p>In above code, we specify “4 days” which is a offset for start time. The first two parameters specify window duration and slide duration.When we run this code, we observe the below result</p>

<pre><code>+---------------------+---------------------+------------------+
|start                |end                  |weekly_average    |
+---------------------+---------------------+------------------+
|2015-12-28 05:30:00.0|2016-01-04 05:30:00.0|105.349998        |
|2016-01-04 05:30:00.0|2016-01-11 05:30:00.0|99.0699982        |
|2016-01-11 05:30:00.0|2016-01-18 05:30:00.0|98.49999799999999 |
|2016-01-18 05:30:00.0|2016-01-25 05:30:00.0|98.1220016        |
</code></pre>

<p>Now we have a week starting from <em>2016-01-04</em>. Still we have initial row which is take from 2015. The reason is, as our start time is 4 days, it creates a window till that time from last seven days.We can remove this row easily using filter as below.</p>

<div class="highlight"><pre><code class="language-scala" data-lang="scala"><span class="k">val</span> <span class="n">filteredWindow</span> <span class="k">=</span> <span class="n">windowWithStartTime</span><span class="o">.</span><span class="n">filter</span><span class="o">(</span><span class="s">&quot;year(window.start)=2016&quot;</span><span class="o">)</span></code></pre></div>

<p>Now we will see the expected result.</p>

<pre><code>+---------------------+---------------------+------------------+
|start                |end                  |weekly_average    |
+---------------------+---------------------+------------------+
|2016-01-04 05:30:00.0|2016-01-11 05:30:00.0|99.0699982        |
|2016-01-11 05:30:00.0|2016-01-18 05:30:00.0|98.49999799999999 |
|2016-01-18 05:30:00.0|2016-01-25 05:30:00.0|98.1220016        |
|2016-01-25 05:30:00.0|2016-02-01 05:30:00.0|96.2539976        |
|2016-02-01 05:30:00.0|2016-02-08 05:30:00.0|95.29199960000001 |
</code></pre>



<p>So now we know how to use time windows in Spark 2.0. This is one of the powerful feature which helps in wide variety analysis in big data.</p>

