# BoM_task

This project applies **Bill of Materials (BoM) analysis** using two different approaches:

- **Python (pandas)**  
  Perform data analysis and transformations directly in Python using the pandas library.

- **SQL (via Dockerized SQL Server)**  
  1. Convert the source Excel data to CSV using a Python script.  
  2. Load the CSV file into the /import folder mounted to Docker:  
     `/var/opt/mssql/import/`  
  3. Use `BULK INSERT` to stage the raw data into SQL Server.  
  4. Apply transformations to create a structured second layer.  
  5. Execute recursive CTE queries to traverse and analyze the BoM hierarchy.
