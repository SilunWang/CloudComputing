#! /bin/bash

######################################################################
# Answer script for Project 3 module 1 Fill in the functions below ###
# for each question. You may use any other files/scripts/languages ###
# in these functions as long as they are in the submission folder. ###
######################################################################
song=million_songs_metadata.csv
sales=million_songs_sales_data.csv
result=million_songs_metadata_and_sales.csv
# Qustion 1
# How many rows match 'Aerosmith' (Case sensitive) through the grep command?
# Run your commands/code to process the dataset and output a
# single number to standard output 
answer_1() {
	echo $(grep 'Aerosmith' ${song} | wc -l)
}

# Qustion 2
# Write grep commands that result in the total number of track_id(s) with 
# artist_name containing "Bob Marley" (Case sensitive)
# Run your commands to process the dataset and output a single number 
# to standard output 
answer_2() {
	echo $(awk ' BEGIN {FS = ","} ; {if ($7 ~ /Bob Marley/) { print; }}' ${song} | wc -l)
}

# Qustion 3
# How many rows match 'The Beatles' (Case sensitive) through the awk command 
# on column 7? The output should be a single number
# Your script should be a single awk command
answer_3() {
	echo $(awk ' BEGIN {FS = "," ; IGNORECASE = 1} ; {if ($7 ~ /The Beatles/) { print; }}' ${song} | wc -l)
}

# Qustion 4
# Write awk code to do the equivalent of the SQL query SELECT
# AVG(duration) FROM songs;. The code should output a single number.
# Your script should be a single awk command
answer_4() {
	echo $(awk ' BEGIN {FS = ","} { SUM += $8 } END { if (NR > 0) print SUM / NR }' ${song} )
}

# Qustion 5
# Invoke the awk / shell program or the set of commands that you wrote
# to merge the two files into the file million_songs_metadata_and_sales.csv
# in current folder.
answer_5() {
	join -t , -1 1 -2 1 ${sales} ${song} > ${result}
}

# Question 6 
# Find the artist with maximum sales using the file million_songs_metadata_and_sales.csv.
# The output of your command(s) should be the artist name.
# NOTE: Artists can have many different artist_names, but only one artist_id, 
# which is unique to each artist. You should find the maximum sales based 
# on artist_id, and return any of that artist_id’s 
# valid artist_name as the result.
answer_6() {
	awk ' BEGIN {FS = ","} { artist[$7] += $3 } END {for (x in artist) {print x, artist[x]} } ' ${result} > tmp && \
	sort -n -r -k2 tmp > sorted && \
	artist_id=$( head -n 1 sorted | awk '{print $1}' ) && \
	rm tmp sorted && \
	echo $( grep ${artist_id} ${song} | awk 'BEGIN {FS = ","} {print $7}' | sort -u | head -n 1)
} 

# Qustion 7
# Write a SQL query that returns the trackid of the song with the maximum duration
answer_7() {
    # Write a SQL query to get the answer to Q7. Do not just echo the answer.
    # Please put your SQL statement within the double quotation marks, and 
    # don't modify the command outside the double quotation marks.
    # If you need to use quotation marks in you SQL statment, please use
    # single quotation marks instead of double.
    mysql --skip-column-names --batch -u root -pdb15319root song_db -e "SELECT songs.track_id FROM songs  
		ORDER BY songs.duration
		DESC LIMIT 1;"
}

# Question 8
# A database index is a data structure that improves the speed of data retreival.
# Identify the field that will improve the performance of query in question 9 
# and create a database index on that field
INDEX_NAME="trackIdx"
answer_8() {
	# Write a SQL query that will create a index on the field
	mysql --skip-column-names --batch -u root -pdb15319root song_db -e "CREATE INDEX trackIdx ON songs (duration);"
}

# Question 9
# Write a SQL query that returns the trackid of the song with the maximum duration
# This is the same query as Question 7. Do you see any difference in performance?
answer_9() {
	# Write a SQL query to get the answer to Q9. Do not just echo the answer.
	# Please put your SQL statement within the double quotation marks, and 
	# don't modify the command outside the double quotation marks.
	# If you need to use quotation marks in you SQL statment, please use
	# single quotation marks instead of double.
	mysql --skip-column-names --batch -u root -pdb15319root song_db -e "SELECT songs.track_id FROM songs  
		USE INDEX (trackIdx)
		ORDER BY songs.duration
		DESC LIMIT 1;"
}

#Question 10
# Write the SQL query that returns all matches (across any column), 
# similar to the command grep -P 'The Beatles' | wc -l:
answer_10() {
	# Write a SQL query to get the answer to Q10. Do not just echo the answer.
	# Please put your SQL statement within the double quotation marks, and 
	# don't modify the command outside the double quotation marks.
	# If you need to use quotation marks in you SQL statment, please use
	# single quotation marks instead of double.
	mysql --skip-column-names --batch -u root -pdb15319root song_db -e " SELECT COUNT(*) FROM songs
		WHERE songs.track_id COLLATE Latin1_BIN LIKE '%The Beatles%' OR
		songs.title COLLATE Latin1_BIN LIKE '%The Beatles%' OR
		songs.song_id COLLATE Latin1_BIN LIKE '%The Beatles%' OR
		songs.release COLLATE Latin1_BIN LIKE '%The Beatles%' OR
		songs.artist_id COLLATE Latin1_BIN LIKE '%The Beatles%' OR
		songs.artist_mbid COLLATE Latin1_BIN LIKE '%The Beatles%' OR
		songs.artist_name COLLATE Latin1_BIN LIKE '%The Beatles%' "
}

#Question 11
# Which artist has the third-most number of rows in Table songs?
# The output should be the name of the artist.
# Please use artist_id as the unique identifier of the artist
answer_11() {
	# Write a SQL query to get the answer to Q11. Do not just echo the answer.
	# Please put your SQL statement within the double quotation marks, and 
	# don't modify the command outside the double quotation marks.
	# If you need to use quotation marks in you SQL statment, please use
	# single quotation marks instead of double.
	mysql --skip-column-names --batch -u root -pdb15319root song_db -e " SELECT artist_name FROM 
		( SELECT songs.artist_name, COUNT(*) 
			AS num_rows FROM songs 
			GROUP BY songs.artist_id ) AS HOTART
		ORDER BY num_rows DESC LIMIT 1 OFFSET 2; "
}


# Answer the following questions corresponding to your experiments 
# with sysbench benchmarks in Step 3: Vertical Scaling

# Answer the following questions corresponding to your experiments on t1.micro instance

# Question 12
# Please output the RPS (Request per Second) values obtained from 
# the first three iterations of FileIO sysbench executed on t1.micro 
# instance with magnetic EBS attached. 
answer_12() {
	# Echo single numbers on line 1, 3, and 5 within quotation marks
	echo "95.33"
	echo ,
	echo "91.75"
	echo ,
	echo "86.38"
}

# Question 13
# Please output the RPS (Request per Second) values obtained from
# the first three iterations of FileIO sysbench executed on t1.micro
# instance with SSD EBS attached. 
answer_13() {
	# Echo single numbers on line 1, 3, and 5 within quotation marks
	echo "642.40" 
	echo ,
	echo "633"
	echo ,
	echo "614.66"
}

# Answer the following questions corresponding to your experiments on m3.large instance

# Question 14
# Please output the RPS (Request per Second) values obtained from
# the first three iterations of FileIO sysbench executed on m3.large
# instance with magnetic EBS attached. 
answer_14() {
	# Echo single numbers on line 1, 3, and 5 within quotation marks
	echo "118.19"
	echo ,
	echo "225.76"
	echo ,
	echo "322.92"
}

# Question 15
# Please output the RPS (Request per Second) values obtained from
# the first three iterations of FileIO sysbench executed on m3.large
# instance with SSD EBS attached.
answer_15() {
	# Echo single numbers on line 1, 3, and 5 within quotation marks
	echo "1103.02"
	echo ,
	echo "1245.15"
	echo ,
	echo "1338.66"
}

# Question 16
# For the FileIO benchmark in m3.large, why does the RPS value vary in each run
# for both Magnetic and SSD-backed EBS volumes? Did the RPS value in t1.micro
# vary as significantly as in m3.large? Why do you think this is the case?
answer_16() {
	# Put your answer with a simple paragraph in a file called "answer_16"
	# Do not change the code below
	if [ -f answer_16 ]
	then
		echo "Answered"
	else
		echo "Not answered"
	fi
}


# DO NOT MODIFY ANYTHING BELOW THIS LINE

answer_5 &> /dev/null
echo "{"

echo -en ' '\"answer1\": \"`answer_1`\"
echo ","

echo -en ' '\"answer2\": \"`answer_2`\"
echo ","

echo -en ' '\"answer3\": \"`answer_3`\"
echo ","

echo -en ' '\"answer4\": \"`answer_4`\"
echo ","

if [ -f 'million_songs_metadata_and_sales.csv' ]
then
	echo -en ' '\"answer5\": \"'million_songs_metadata_and_sales.csv' file created\"
	echo ","
else
	echo -en ' '\"answer5\": \"'million_songs_metadata_and_sales.csv' file not created\"
	echo ","
fi

echo -en ' '\"answer6\": \"`answer_6`\"
echo ","

`mysql --skip-column-names --batch -u root -pdb15319root song_db -e "set global query_cache_size = 0" &> /dev/null`
`mysql --skip-column-names --batch -u root -pdb15319root song_db -e "drop index $INDEX_NAME on songs" > /dev/null`
START_TIME=$(date +%s.%N)
TID=`answer_7 | tail -1`
END_TIME=$(date +%s.%N)
RUN_TIME=$(echo "$END_TIME - $START_TIME" | bc)
echo -en ' '\"answer7\": \"$TID,$RUN_TIME\"
echo ","

answer_8 > /dev/null
INDEX_FIELD=`mysql --skip-column-names --batch -u root -pdb15319root song_db -e "describe songs" | grep MUL | cut -f1`
echo -en ' '\"answer8\": \"$INDEX_FIELD\"
echo ","

START_TIME=$(date +%s.%N)
TID=`answer_9 | tail -1`
END_TIME=$(date +%s.%N)
RUN_TIME=$(echo "$END_TIME - $START_TIME" | bc)
echo -en ' '\"answer9\": \"$TID,$RUN_TIME\"
echo ","

echo -en ' '\"answer10\": \"`answer_10`\"
echo ","

echo -en ' '\"answer11\": \"`answer_11`\"
echo ","

echo -en ' '\"answer12\": \"`answer_12`\"
echo ","

echo -en ' '\"answer13\": \"`answer_13`\"
echo ","

echo -en ' '\"answer14\": \"`answer_14`\"
echo ","

echo -en ' '\"answer15\": \"`answer_15`\"
echo ","

echo -en ' '\"answer16\": \"`answer_16`\"
echo 
echo  "}"



