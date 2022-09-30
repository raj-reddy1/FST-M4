--Load input file from HDFS
inputFile = LOAD 'hdfs:///user/m4project/episodeIV_dialouges.txt' USING PigStorage('\t') AS (name:chararray,dial:chararray);
-- Combine the lines by name from the above stage
grpd = GROUP inputFile BY name;
-- Count the total number of lines spoken by each character (Reduce)
cntd = FOREACH grpd GENERATE $0 as name, COUNT($1) AS countVal;
cntd_desc = ORDER cntd BY countVal DESC;
-- Store the result in HDFS
STORE cntd_desc INTO 'hdfs:///user/m4project/episodeIV_results' USING PigStorage('\t');