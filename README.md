<h3> Bid data analysis with Spark and Scala </h3>
I use Spark, SparkSQL and Scala to do some quick analysis on a large dataset. Uncomporessed, this dataset on New york City taxi cab rides is about 400 GB -- way too much 
to store on my laptop. Instead, I load the data into Spark using the parquet format, which compresses the data down to about 35 GB. The data can be torrented
<a href="https://academictorrents.com/browse.php?search=taxi">here.</a> With a total of 1,382,375,998 rows in the full dataset, I answer:

<br>

<ul>
  <li>Which zones see the most pick-ups?</li>
  <li>Which Boroughs see the most pick-ups?</li>
  <li>Which hours see the most pick-ups?</li>
  <li>What is the mean, standard deviation, minimum and maximum of the trip distances??</li>
</ul>



I was impressed by how easy it is to process and analyze this data with SparkSQL.
I've been using the resources on <a href="https://rockthejvm.com/"> Rock the JVM,</a> which is where this dataset/problem comes from, and
a great site to learn about Scala/Spark/Akka material. My next steps with this will be to do some more analysis and data cleaning. Then I am 
going to try packaging the application and running it using Amazon EMR from AWS.

