#! /bin/bash
#
# This is a shell script of Project 1.1
# Author: Silun Wang
# Andrew id: silunw
# Date: 09/10/2015

######################################################################
# Answer script for Project 1 module 1 Fill in the functions below ###
# for each question. You may use any other files/scripts/languages ###
# in these functions as long as they are in the submission folder. ###
######################################################################

# Write or invoke the code to perform filtering on the dataset. Redirect 
# the filtered output to a file called 'output' in the current folder.

filename="pagecounts-20150801-000000"

answer_0() {
    # Fill in this Bash function to filter the dataset and redirect the
    # output to a file called 'output'.
	   # Example:
	   python filter.py > output
}


# How many lines (items) were originally present in the input file 
# pagecounts-20150801-000000 i.e line count before filtering
# Run your commands/code to process the dataset and echo a 
# single number to standard output
answer_1() {
        # Write a function to get the answer to Q1. Do not just echo the answer.
	    echo $(wc -l "${filename}" | awk '{print $1}')
}

# Before filtering, what was the total number of requests made to all 
# of wikipedia (all subprojects, all elements, all languages) during 
# the hour covered by the file pagecounts-20150801-000000
# Run your commands/code to process the dataset and echo a 
# single number to standard output
answer_2() {
        # Write a function to get the answer to Q2. Do not just echo the answer.
        echo $(awk '{s+=$(NF-1)} END {print s}' "${filename}")
}

# How many lines emerged after applying all the filters?
# Run your commands/code to process the dataset and echo a 
# single number to standard output
answer_3() {
        # Write a function to get the answer to Q3. Do not just echo the answer.
        echo $(wc -l "output" | awk '{print $1}')
}

# What was the most popular article in the filtered output?
# Run your commands/code to process the dataset and echo a 
# single word to standard output
answer_4() {
        # Write a function to get the answer to Q4. Do not just echo the answer.
	    # articles start with [A-Z]
        # regex: /^[A-Z]/
        echo $(awk '$1 ~ /^[A-Z]/ {print $1}' "output" | head -n 1)
}

# How many views did the most popular article get?
# Run your commands/code to process the dataset and echo a 
# single number to standard output
answer_5() {
        # Write a function to get the answer to Q5. Do not just echo the answer.
	    # articles start with [A-Z]
        # regex: /^[A-Z]/
        echo $(awk '$1 ~ /^[A-Z]/ {print $2}' "output" | head -n 1) 
}

# What is the count of the most popular movie in the filtered output? 
# (Hint: Entries for movies have "(film)" in the article name)
# Run your commands/code to process the dataset and echo a 
# single number to standard output
answer_6() {
        # Write a function to get the answer to Q6. Do not just echo the answer.
        # regex: /(film)/
        echo $(awk '$1 ~ /(film)/ {print $2}' "output" | head -n 1)
}

# How many articles have more than 2500 views in the filtered output?
# Run your commands/code to process the dataset and echo a 
# single number to standard output
answer_7() {
        # Write a function to get the answer to Q7. Do not just echo the answer.
        echo $(awk 'BEGIN {count=0} \
            { if ($2 > 2500 && $1 ~ /^[A-Z]/) {count++}} \
            END {print count}' "output")
}

# How many views are there in the filtered dataset for all "episode lists".
# Episode list articles have titles that start with "List_of" and end with "episodes"
# Run your commands/code to process the dataset and echo a number to standard output
# Both strings above are case sensitive
answer_8() {
        # Write a function to get the answer to Q8. Do not just echo the answer.
        # regex: /^(List_of)(.*)(episodes)$/
        echo $(awk 'BEGIN {s=0} \
            { if ($1 ~ /^(List_of)(.*)(episodes)$/) {s+=$2}} \
            END {print s}' "output")
}

# What is most popular in this hour, "(2014_film)" or "(2015_film)" articles?
# Both strings above are case sensitive
answer_9() {
        # Write a function to get the answer to Q9. Do not just echo the answer.
        # The function should return either 2014 or 2015.
        count_2014=$(awk '{ if($1 ~ /(2014_film)/) {s+=$2} } \
            END {print s}' "output")
        count_2015=$(awk '{ if($1 ~ /(2015_film)/) {s+=$2} } \
            END {print s}' "output")
        if (( ${count_2014} > ${count_2015} ))
        then
	        echo "2014"
        else
            echo "2015"
        fi
}


# DO NOT MODIFY ANYTHING BELOW THIS LINE
answer_0 &> /dev/null

echo "The results of this run are : "
echo "{"

if [ -f 'output' ]
then
        echo -en ' '\"answer0\": \"'output' file created\"
        echo ","
else
        echo -en ' '\"answer0\": \"No 'output' file created\"
        echo ","
fi

a1=`answer_1`
echo -en ' '\"answer1\": \"$a1\"
echo $a1 > .1.out
echo ","

a2=`answer_2`
echo -en ' '\"answer2\": \"$a2\"
echo $a2 > .2.out
echo ","

a3=`answer_3`
echo -en ' '\"answer3\": \"$a3\"
echo $a3 > .3.out
echo ","

a4=`answer_4`
echo -en ' '\"answer4\": \"$a4\"
echo $a4 > .4.out
echo ","

a5=`answer_5`
echo -en ' '\"answer5\": \"$a5\"
echo $a5 > .5.out
echo ","

a6=`answer_6`
echo -en ' '\"answer6\": \"$a6\"
echo $a6 > .6.out
echo ","

a7=`answer_7`
echo -en ' '\"answer7\": \"$a7\"
echo $a7 > .7.out
echo ","

a8=`answer_8`
echo -en ' '\"answer8\": \"$a8\"
echo $a8 > .8.out
echo ","

a9=`answer_9`
echo -en ' '\"answer9\": \"$a9\"
echo $a9 > .9.out
echo ""

 
echo  "}"

echo ""
echo "If you feel these values are correct please run:"
echo "./submitter -a andrewID -p submission_password"
