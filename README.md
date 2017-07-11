# Big_Data
The project deals on how to perform Spatio-temporal hot-spot analysis using Apache Spark.

Project was built by two sub phases:

1) First phase deals with understanding and getting used to of the Hadoop and Spark environment. We achieved this by obtaining run time statistics for distributed computations like Spatial Range Query, Spatial KNN query, Spatial Join Query and their variations obtained by with or without joining the PointRDD using Equal grid or building R-tree indices. We also compared the variations.

2) Second phase involved spatio-temporal analysis of subset of the NYC yellow-cab taxi data set, to compute Getis-Ord statistic as a z-score to identify 50 hotspots within the mentioned envelope. PROBLEM : http://sigspatial2016.sigspatial.org/giscup2016/problem

