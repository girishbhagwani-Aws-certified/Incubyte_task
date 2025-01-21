import pandas as pd
from sqlalchemy import create_engine, Table, Column, Integer, String, Date, MetaData

# Initialize the SQLAlchemy engine and metadata
engine = create_engine('sqlite:///hospital_data.db', echo=True)
metadata = MetaData()

# Function to create the staging table
def create_staging_table():
    staging_table = Table('Staging_Customers', metadata,
        Column('Customer_Name', String(255)),
        Column('Customer_Id', String(18), primary_key=True),
        Column('Open_Date', Date),
        Column('Last_Consulted_Date', Date),
        Column('Vaccination_Id', String(5)),
        Column('Dr_Name', String(255)),
        Column('State', String(5)),
        Column('Country', String(5)),
        Column('DOB', Date),
        Column('Is_Active', String(1)),
    )
    metadata.create_all(engine)
    print("Staging Table Created")

# Function to create country-specific tables
def create_country_table(country_name):
    country_table = Table(f"Table_{country_name}", metadata,
        Column('Customer_Name', String(255)),
        Column('Customer_Id', String(18), primary_key=True),
        Column('Open_Date', Date),
        Column('Last_Consulted_Date', Date),
        Column('Vaccination_Id', String(5)),
        Column('Dr_Name', String(255)),
        Column('State', String(5)),
        Column('Country', String(5)),
        Column('DOB', Date),
        Column('Is_Active', String(1)),
        Column('Age', Integer),
        Column('Days_Since_Last_Consulted', Integer),
    )
    metadata.create_all(engine)
    print(f"Table_{country_name} Created")

# Function to insert data into the respective country table
def insert_into_country_table(country_name, df_country):
    table = Table(f"Table_{country_name}", metadata, autoload_with=engine)
    with engine.connect() as connection:
        for _, row in df_country.iterrows():
            insert_query = table.insert().values(
                Customer_Name=row['Customer_Name'],
                Customer_Id=row['Customer_Id'],
                Open_Date=row['Open_Date'],
                Last_Consulted_Date=row['Last_Consulted_Date'],
                Vaccination_Id=row['Vaccination_Id'],
                Dr_Name=row['Dr_Name'],
                State=row['State'],
                Country=row['Country'],
                DOB=row['DOB'],
                Is_Active=row['Is_Active'],
                Age=row['Age'],
                Days_Since_Last_Consulted=row['Days_Since_Last_Consulted']
            )
            connection.execute(insert_query)
    print(f"Data Inserted into Table_{country_name}")

# Function to prompt user for file paths and process CSV files
def process_csv_files():
    country_files = {}
    for country in ["USA", "IND", "AUS"]:
        file_path = input(f"Enter the full file path for {country}.csv: ")
        country_files[country] = file_path

    for country, file_path in country_files.items():
        try:
            df = pd.read_csv(file_path)
            # Convert date columns to datetime format
            df["DOB"] = pd.to_datetime(df["DOB"])
            df["Last_Consulted_Date"] = pd.to_datetime(df["Last_Consulted_Date"])
            df["Open_Date"] = pd.to_datetime(df["Open_Date"])

            # Calculate derived columns
            current_date = pd.Timestamp.now()
            df["Age"] = current_date.year - df["DOB"].dt.year
            df["Days_Since_Last_Consulted"] = (current_date - df["Last_Consulted_Date"]).dt.days

            # Filter records with Days_Since_Last_Consulted > 30
            df = df[df["Days_Since_Last_Consulted"] > 30]

            # Insert filtered data into the respective country table
            insert_into_country_table(country, df)
        except Exception as e:
            print(f"Error processing file for {country}: {e}")

# Create staging and country-specific tables
create_staging_table()
create_country_table('USA')
create_country_table('IND')
create_country_table('AUS')

# Process CSV files and insert data
process_csv_files()
