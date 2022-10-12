---README---
-Tested and run using Python 3.8.5-

How to run the script:
	-[PYTHON] [SCRIPTNAME]
	-Example: Python 	explore_the_five.py
	-Would typically do an argparse but seeing as how it needed no input to run, decided to focus time elsewhere
		-There is great value in argparse though by allowing the input variable definitions in main() be changeable by user

Script does the following:
	-Script works in multiple steps
		-Downloads in zip file from "https://openpsychometrics.org/_rawdata/IPIP-FFM-data-8Nov2018.zip"
		-Unpacks a specific csv from that zip
		-Checks to see if a certain database we are interested is in our sql server, creates if not
		-Reads in data from now local csv and passes to mysql database
		-Runs multiple functions that perform sql queries, and put output into a local file
	-Independantly, also runs a function that makes defined classes and tests them 

Does so with the following assumptions:
	-Script is being run in the path which you hope to place the local files
	-Files are not completely empty and have a header line.
		-Columns will always be in the same order

Notes:
	-Used mysql as my database server
	-I put the imported libraries I selected for this assignment under "My added imports" comment
	-Not optimized for andling the large csv when moving data to mysql table
		-Went with 'executemany()' though a sqlalchemy apporach where we split into data chunks may prove to be faster
		-Currently takes about 10 minutes on my laptop to run
	-Was not getting the correct response for the 'uniques_by_country' function but didn't have time to debug
		-Three theories:
		-I believe it is either a result of how the data was read in
		-that the file it is comparing results to was from another dataset
		-an issue with the typing going from the csv, to pandas, to sql (what I believe to be most likely)
		-Would investigate further with more time!
