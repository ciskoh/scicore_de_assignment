# Data Engineer Position - Assignment completed by Matteo Jucker Riva

This branch contains the code for the SCICORE data-engineering assignement question 1-3

## SETUP & RUN

To install all dependecies run `python setup.py`from the main project folder (requires pip)

To run one of the exercises a __CLI is available!__ just run `python main.py -e {number of exercise 1-3} `. Further command line parameters are available. Run `python main.py --help` for more information


## TASK:

In this assignment, we are providing you with a real dataset from a research project (called NCCR-
AntiResist) supported in our secure infrastructure sciCORmed. The data set contains data from several different experiment. You can find all relevant information in the following git
repository: https://github.com/scicore-unibas-ch/data-engineer-assignment
Please look at how the given dataset is structured and describe and comment on how linkage is done
across these data.

### Exercise 1
write a script (e.g. in Python) to parse the four provided files and create an SQL database (e.g. local SQLite DB) to store the key information from the dataset.

### Exercise 2
write a script which joins the tables to list all files (file paths) belonging to a species.

### Exercise 3
as a way to enrich the data in this dataset, find all proteins in UniProtKB which are annotated with the antibiotic Imipenem for the organism Klebsellia pneumoniae (TaxID 573, annotated as "Klebs" in the previous tables). To accomplish this, please write a script using the UniProtKB API toget this information and insert it as a table in the same SQL database.
