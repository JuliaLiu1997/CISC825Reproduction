#!/bin/bash
for csv in /home/julia/pollution/*.csv
do 
	mysql -e "LOAD DATA LOCAL INFILE '"$csv"' INTO TABLE data FIELDS TERMINATED BY ',' LINES TERMINATED BY'\n' IGNORE 1 LINES" -u root --password=julia pollution

done
