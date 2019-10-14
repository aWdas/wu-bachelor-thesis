# Bachelor Thesis WU 2019
**Author:** Jan Hadl (jan.hadl@s.wu.ac.at)  
**Supervisor:** Amr Azzam (aazzam@wu.ac.at)

This repository contains the code produced for the creation of the author's bachelor thesis at Wirtschaftsuniversität Wien 
at the department of Information Business during the summer semester 2019.

## Setup and requirements
To work with the code in this project, you need the following things installed:
- Java 11
- Maven 3.5 or higher
- Python 3.6 or higher
- Jupyter Notebook

The datasets that were used for the analysis in the thesis are too large to upload to GitHub.
They are available under the following URLs:
- Wikidata: https://iccl.inf.tu-dresden.de/web/Wikidata_SPARQL_Logs/en
    - "Wikidata All": The 7 files that are marked with "All queries, success (HTTP code 200)"
    - "Wikidata Organic": The 7 files that are marked with "Organic queries, success (HTTP code 200)"
- DBpedia: http://usewod.org/data-sets.html
    - Access to the datasets requires signing a usage agreement
    - Once the agreement is submitted, the University of Southampton sends a download link for the datasets
    - The USEWOD Research Datasets contain logs from different databases and in the case of DBpedia, from different DBpedia versions;
      Only the log files from DBpedia versions 3.8 and 3.9 were used for the thesis; 
      The folder structure of the datasets was flattened and those log files that were BZIP2 compressed were changed to GZIP, 
      as processing GZIP is a lot faster.
      
## Running the query graph analysis
The module `log-statistics` contains the application that reads one or multiple log files,
constructs the query graphs for each query in the logs, and then finds those partitions that cover each query.

To run this application, first build it with `mvn clean install` from the `log-statistics` directory. You should now have a 
`log-statistics/target/log-statistics-1.0-SNAPSHOT-jar-with-dependencies.jar` file that you can invoke with 
`java -jar`. The application has multiple options:
- `-l` (Required): The path(s) to one/multiple directory(/ies) of logs or single log file(s); If you want to analyze multiple datasets in one go 
        (e.g. the four years of USEWOD datasets), specify a series of values for this parameter (e.g. `-l dataset1/ dataset2.log.gz`; 
        If multiple log files should be analyzed as one dataset, put them into a folder an use this folder as a parameter value (like the `dataset1/` above)
- `-o` (Required): The names of the output files that the application writes to; This parameter needs to have the same number of values as the `-l` parameter 
        (i.e. one output name per dataset).
- `-po` (Required): The file to write the final predicate map to
- `-pi`: The file to read an already existing predicate map from; This is useful for analyzing multiple log datasets that come from the same database
- `-pre`: The name of the log line preprocessor (wikidata or dbpedia)
- `-skip` (Default 0): The number of lines to skip at the beginning of each file (if there are header lines present)

For example, if one wishes to analyze the two datasets for USEWOD 2013 and 2014, which are stored as multiple log files in 
two folders, one may do it like this:  
```
java -jar log-statistics/target/log-statistics-1.0-SNAPSHOT-jar-with-dependencies.jar 
-l ~/usewod2013/ ~/usewod2014/ 
-o ~/results/usewod2013 ~/results/usewod2014
-po ~/results/usewod_predicate_map.tsv
-pre dbpedia
-skip 0
```

## Running the minimum union calculation
The module `minimum-unions` contains the application that calculates the partitions required to cover increasing percentages of 
all the queries in a log dataset. 

To run this application, first build it with `mvn clean install` from the `minimum-unions` directory. You should now have a 
`minimum-unions/target/minimum-unions-1.0-SNAPSHOT-jar-with-dependencies.jar` file that you can invoke with 
`java -jar`.

This application is not command-line configurable. To change the configuration, edit the 
`minimum-unions/src/main/java/at/hadl/minimumunions/Application.java` and rebuild the project. The configuration is explained in comments in that file.

## Running the Jupyter notebooks (smaller analysis and figure generation)
To run the jupyter notebooks, change to the `log-statistics/notebooks/` directory and invoke `jupyter notebook`.
The purpose of each notebook is explained in the first code cell in the notebook. 

## Results from the thesis
The results used in the thesis are stored in the `results` folder. 
`minimum-unions` contains the results from the `minimum-unions` application,
`queryshapes` those of the `log-statistics` application. `partition-creation` contains the results from
Javier Fernandez' partitioning code.
