# Automating Data Compilation of the World's Largest Banks

In today's fast-paced financial landscape, staying updated on the global banking industry's dynamics is crucial for various stakeholders. As an aspiring data engineer, I created this sript to automate the process of compiling and analyzing data on the world's largest banks. This involved extracting, transforming, and loading market capitalization data for the top 10 banks globally, in multiple currencies, using Python.

## Code Overview

The script, named `banks_project.py`, aimed to accomplish the following tasks:

### Task 1: Extracting Data

The data was extracted from a Wikipedia page using BeautifulSoup library in Python. Specifically, the tabular information under the 'By market capitalization' heading was identified and extracted into a DataFrame using a function named `extract()`.

### Task 2: Transforming Data

The extracted DataFrame was transformed to include market capitalization values in GBP, EUR, and INR. This transformation was based on exchange rate information provided in a CSV file. The function `transform()` was written to perform this task.

### Task 3: Loading to CSV

The transformed DataFrame was loaded into a CSV file using a function named `load_to_csv()`. This facilitated easy access and sharing of the compiled data.

### Task 4: Loading to Database

The transformed DataFrame was also loaded into an SQL database server as a table named `Largest_banks` within the `Banks.db` database. This was achieved using the function `load_to_db()`, enabling efficient storage and querying of the data.

### Task 5: Running Queries

Queries were executed on the database table to analyze the compiled data further. The results of these queries provided insights into the market capitalization of the top banks in different currencies.

### Task 6: Logging Progress

A function `log_progress()` was implemented to log progress at different stages of the code execution into a file named `code_log.txt`. This ensured traceability and facilitated debugging. Finally, the contents of the `code_log.txt` file were checked to ensure that log entries were recorded at each stage of the code execution, providing a comprehensive record of the process.

## Conclusion

By automating the process of compiling and analyzing data on the world's largest banks, we can now have a robust system in place to generate timely and accurate reports. This not only saves time and resources but also ensures consistency and reliability in the analysis of global banking trends. With the ability to execute the code quarterly, stakeholders can stay informed about the evolving landscape of the banking industry and make data-driven decisions effectively. In conclusion, the project demonstrates the power of automation in streamlining the ETL processes and enabling informed decision-making in the dynamic field of finance.
