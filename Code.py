CREATE DATABASE IF NOT EXISTS hospital_db_2;
USE hospital_db_2;

CREATE TABLE IF NOT EXISTS StagingTable (
    customerName STRING,
    customerID STRING,
    customerOpenDate DATE,
    lastConsultedDate DATE,
    vaccinationType STRING,
    doctorConsulted STRING,
    state STRING,
    country STRING,
    postCode STRING,
    dateOfBirth DATE,
    activeCustomer STRING
);

INSERT INTO StagingTable VALUES
('Alex', '123457', '2010-10-12', '2012-10-13', 'MVD', 'Paul', 'SA', 'AUS', '5000', '1987-03-06', 'A'),
('John', '123458', '2010-10-12', '2012-10-13', 'MVD', 'Paul', 'TN', 'IND', '600001', '1987-03-06', 'A'),
('Mathew', '123459', '2010-10-12', '2012-10-13', 'MVD', 'Paul', 'WAS', 'PHIL', '1000', '1987-03-06', 'A'),
('Matt', '12345', '2010-10-12', '2012-10-13', 'MVD', 'Paul', 'BOS', 'NYC', '10001', '1987-03-06', 'A'),
('Jacob', '1256', '2010-10-12', '2012-10-13', 'MVD', 'Paul', 'VIC', 'AUS', '3000', '1987-03-06', 'A');

#VALIDATIONS
#DROPPING DUPLICATES
# Load the StagingTable as a DataFrame
staging_df = spark.sql("SELECT * FROM StagingTable")
# Drop duplicates 
cleaned_df = staging_df.dropDuplicates()
# Overwrite the original StagingTable with the cleaned DataFrame
cleaned_df.write.mode('overwrite').saveAsTable("StagingTable")

#CHECKING FOR NULL VALUES
SELECT * 
FROM Staging_Table
WHERE Customer_Name IS NULL OR Customer_Id IS NULL OR Open_Date IS NULL;

#Add new columns
ALTER TABLE StagingTable
ADD COLUMNS (
    Age INT,
    Days_Since_Last_Consulted INT,
    Is_Consulted_Recently STRING
);

#Update the newly added columns with calculated values
UPDATE StagingTable
SET Age = FLOOR(DATEDIFF(CURRENT_DATE(),dateOfBirth) / 365),
    Days_Since_Last_Consulted = DATEDIFF(CURRENT_DATE(), lastConsultedDate),
    Is_Consulted_Recently = CASE 
        WHEN DATEDIFF(CURRENT_DATE(), lastConsultedDate) > 30 THEN 'N' 
        ELSE 'Y' 
    END;

# Get unique country codes from the staging table
countries_df = spark.sql("SELECT DISTINCT country FROM StagingTable")
countries = [row['country'] for row in countries_df.collect()]

# Loop through each country and create the corresponding table
for country in countries:
    table_name = f"Table_{country}"  # Construct the table name

 # SQL to create the table
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        customerName STRING,
        customerID STRING,
        customerOpenDate DATE,
        lastConsultedDate DATE,
        vaccinationType STRING,
        doctorConsulted STRING,
        state STRING,
        country STRING,
        postCode STRING,
        dateOfBirth DATE,
        activeCustomer STRING,
        Age INT,
        Days_Since_Last_Consulted INT,
        Is_Consulted_Recently STRING
    )
    """
    
    # Execute the create table query
    spark.sql(create_table_query)

  for country in countries:
    table_name = f"Table_{country}"  # Construct the table name
    insert_query = f"""
    INSERT INTO {table_name}
    SELECT customerName, customerID, customerOpenDate, lastConsultedDate, 
           vaccinationType, doctorConsulted, state, country, postCode, 
           dateOfBirth, activeCustomer,Age, Days_Since_Last_Consulted, Is_Consulted_Recently
    FROM StagingTable
    WHERE country = '{country}';
    """
    
    # Execute the insert query
    spark.sql(insert_query)






