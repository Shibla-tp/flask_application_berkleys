from flask import Flask, jsonify, request
from airtable import Airtable
import pandas as pd
import numpy as np
import os
import pycountry
import matplotlib.pyplot as plt
import seaborn as sns
import re


app = Flask(__name__)

# Old Airtable Configuration
BASE_ID_OLD = 'app5s8zl7DsUaDmtx'
API_KEY = 'patELEdV0LAx6Aba3.393bf0e41eb59b4b80de15b94a3d122eab50035c7c34189b53ec561de590dff3'  # Replace with a secure method to fetch the key
TABLE_NAME_OLD = 'berkleyshomes_apollo'

# New Airtable Configuration
# BASE_ID_NEW1 = 'appTEXhgxahKgWLgx'
BASE_ID_NEW = 'app5s8zl7DsUaDmtx'
TABLE_NAME_NEW = 'cleaned_profile_data_apollo'
TABLE_NAME_NEW1 = 'outreach_data'
TABLE_NAME_NEW2 = 'ICP_information'
API_KEY_NEW = os.getenv('AIRTABLE_API_KEY', 'patELEdV0LAx6Aba3.393bf0e41eb59b4b80de15b94a3d122eab50035c7c34189b53ec561de590dff3')
# API_KEY_NEW1 = os.getenv('AIRTABLE_API_KEY', 'patPgbQSC8pAg1Gbl.7ca275de5a5c8f2cf4389452e91c8f3f6c3e37bb2967c0f4cd8f41fa9d99044d')
# API_KEY_NEW1 = 'patPgbQSC8pAg1Gbl.7ca275de5a5c8f2cf4389452e91c8f3f6c3e37bb2967c0f4cd8f41fa9d99044d'


airtable_old = Airtable(BASE_ID_OLD, TABLE_NAME_OLD, API_KEY)
airtable_new = Airtable(BASE_ID_NEW, TABLE_NAME_NEW, API_KEY_NEW)
airtable_new1 = Airtable(BASE_ID_NEW, TABLE_NAME_NEW1, API_KEY_NEW)
try:
    airtable_new2 = Airtable(BASE_ID_NEW, TABLE_NAME_NEW2, API_KEY_NEW)
except Exception as e:
    print(f"Error initializing Airtable: {e}")


def record_exists_in_airtable(airtable_instance, record_data, unique_field):
    """
    Check if a record with the same unique identifier already exists in Airtable.
    """
    unique_value = record_data.get(unique_field)
    if not unique_value:
        return False

    # Search for the uniqueId field in Airtable
    search_result = airtable_instance.search(unique_field, unique_value)
    return len(search_result) > 0



def send_to_airtable_if_new(df, airtable_instance, unique_field, desired_fields=None, field_mapping=None, default_values=None, icp_to_outreach=None, icp_df=None):
    for i, row in df.iterrows():
        record_data = row.dropna().to_dict()
        if desired_fields:
            record_data = {field: row[field] for field in desired_fields if field in row and not pd.isna(row[field])}

        # Ensure 'createdTime' is not part of the record
        if "created_time" in record_data:
            del record_data["created_time"]

        # Generate the uniqueId locally
        uniqueId = f"{record_data.get('id', '')}_{record_data.get('email', '')}"
        record_data["uniqueId"] = uniqueId

        # Apply field name mapping (if provided)
        if field_mapping:
            record_data = {field_mapping.get(k, k): v for k, v in record_data.items()}

        # Merge default values for other fields
        if default_values:
            for key, default_value in default_values.items():
                record_data.setdefault(key, default_value)

        # Apply ICP-to-outreach mapping to populate the specific fields
        if icp_to_outreach:
            for outreach_field, icp_field in icp_to_outreach.items():
                if icp_field in icp_df.columns:  # Ensure the column exists in icp_df
                    record_data[outreach_field] = icp_df.loc[0, icp_field]  # Use the first row of icp_df for the mapping

        # Insert the record if it does not already exist
        if not record_exists_in_airtable(airtable_instance, {"uniqueId": uniqueId}, "uniqueId"):
            try:
                airtable_instance.insert(record_data)
                print(f"Record {i} inserted successfully.")
            except Exception as e:
                print(f"Failed to insert record {i}: {e}")
        else:
            print(f"Record {i} already exists in Airtable. Skipping insertion.")



def process_email(email):
    """
    Processes the email field:
    - If email is `,`, empty, or missing, return 'Unknown'.
    - Otherwise, return the original email.
    """
    if not email or email in [",", "unknown", "Unknown", ""]:
        return "Unknown"
    return email.strip()  # Clean leading/trailing spaces

def expand_emails(df):
    """
    Duplicates rows for each email present in a comma-separated email field.
    If a single email is present, it returns the same row without duplication.
    """
    rows = []
    for i, row in df.iterrows():
        emails = row['email'].split(',') if row['email'] != "Unknown" else ["Unknown"]
        for email in emails:
            email = email.strip()  # Clean up individual emails
            if email:  # Ignore empty email entries
                new_row = row.copy()
                new_row['email'] = email
                rows.append(new_row)
    return pd.DataFrame(rows)

# def extract_country(location):
#     """
#     Dynamically extracts the country name from a location string using pycountry.
#     """
#     if not location or location.lower() == "unknown":
#         return "Unknown"

#     # Normalize the location string for matching
#     location = location.lower()
    
#     # Iterate through all country names in pycountry
#     for country in pycountry.countries:
#         if country.name.lower() in location:
#             return country.name
        
#         # Check alternate names like "United States of America" (official_name)
#         if hasattr(country, 'official_name') and country.official_name.lower() in location:
#             return country.name

#     return "Unknown"

# def extract_location_from_linkedin(url):
#     """
#     Extract location-related information from the LinkedIn URL.
#     Example: 'https://www.linkedin.com/company/example-company-location-dubai' -> 'Dubai'
#     """
#     import re
#     if not url or url.lower() == "unknown":
#         return "Unknown"
    
#     # Adjust regex to find location keywords (e.g., after "company")
#     match = re.search(r'company/.*?-([a-zA-Z]+[-\s]?[a-zA-Z]*)$', url)
#     if match:
#         location = match.group(1).replace('-', ' ').title()
#         return location
#     return "Unknown"


# def impute_location(df):
#     """
#     Impute missing location values using multiple methods, including LinkedIn URLs and predefined mappings.
#     """
#     import re
    
#     def extract_location_from_linkedin(url):
#         if not url or url.lower() == "unknown":
#             return "Unknown"
#         match = re.search(r'company/.*?-([a-zA-Z]+[-\s]?[a-zA-Z]*)$', url)
#         if match:
#             location = match.group(1).replace('-', ' ').title()
#             return location
#         return "Unknown"
    
#     # Add extracted location column
#     if 'location' in df.columns and 'companyLinkedInUrl' in df.columns:
#         df['extracted_location'] = df['companyLinkedInUrl'].apply(extract_location_from_linkedin)
    
#     # Mapping of company names to default locations
#     location_map = {
#         'Example Company': 'Dubai',
#         'Tech Innovators': 'Abu Dhabi'
#     }
    
#     # Impute location
#     df['location'] = df.apply(
#         lambda row: row['location']
#         if row['location'] != "Unknown"
#         else row['extracted_location']
#         if 'extracted_location' in row and row['extracted_location'] != "Unknown"
#         else location_map.get(row['company'], "Unknown"),
#         axis=1
#     )
    
#     # Drop helper columns
#     if 'extracted_location' in df.columns:
#         df.drop(columns=['extracted_location'], inplace=True)
    
#     return df
# def clean_and_validate_names(df):
#     """
#     Cleans and validates the names in the input data.

#     Args:
#         data (dict): Dictionary containing 'first_name', 'last_name', and 'name' keys.

#     Returns:
#         dict: Cleaned and validated data with a validation status.
#     """
#     cleaned_data = {}
#     # validation_status = {"status": "success", "errors": []}
    
#     # Clean 'first_name'
#     # if 'first_name' in data:
#     #     cleaned_data['first_name'] = data['first_name'].strip().capitalize()
#     df['first_name'] = df['first_name'].apply(lambda x: x.strip() if isinstance(x, str) else x)
#     # Clean 'last_name'
#     # if 'last_name' in data:
#     #     cleaned_data['last_name'] = data['last_name'].strip().capitalize()
    

#     return pd.DataFrame(cleaned_data)



@app.route("/", methods=["GET"])
def fetch_and_update_data():
    try:
        all_records = airtable_old.get_all()

        data = [record.get('fields', {}) for record in all_records]
        record_ids = [record['id'] for record in all_records]

        if not data:
            return jsonify({"message": "No data found in the old Airtable."})

        df = pd.DataFrame(data)
        df.to_csv("berkleyshomes_apollo.csv", index=False)
        # Replace problematic values
        df.replace([np.inf, -np.inf], np.nan, inplace=True)
        df = df.where(pd.notnull(df), None)

        for column in df.select_dtypes(include=['object']).columns:
            df[column] = df[column].fillna("Unknown")
        # Clean 'first_name'
        df['first_name'] = df['first_name'].apply(lambda x: x.strip() if isinstance(x, str) else x)
        # Clean 'last_name'
        df['last_name'] = df['last_name'].apply(lambda x: x.strip() if isinstance(x, str) else x)
         # Clean 'email'
        if 'email' in df.columns:
            df['email'] = (
                df['email']
                .astype(str)
                .str.lower()
                .str.strip()
                .apply(lambda x: process_email(x))
            )
        
        #clean linkedin_url
        if 'linkedin_url' in df.columns:
            def clean_company_website(url, unique_id):
                if pd.isna(url) or not str(url).strip() or url.lower() in ["unknown", "n/a"]:
                    return f"https://unknown-company-{unique_id}.com"
                url = url.strip()
                if not url.startswith(("http://", "https://")):
                    url = "https://" + url
                return url

            df['linkedin_url'] = df.apply(
                lambda row: clean_company_website(row['linkedin_url'], row.name), axis=1
            )

        # Function to clean the headline
        if 'headline' in df.columns:
            def clean_headline(headline):
                if pd.isna(headline):
                    return headline
                
                # Standardize capitalization (title case)
                headline = str(headline).strip().title()
                
                # Remove special characters and symbols (e.g., "|", ":", etc.)
                headline = re.sub(r'[^\w\s.,-]', '', headline)
                
                # Remove redundant or irrelevant phrases (you can adjust this list based on your needs)
                redundant_phrases = [
                    'notable experiences', 'luxury international brands', 'in mena', 'in international markets'
                ]
                for phrase in redundant_phrases:
                    headline = headline.replace(phrase, '')
                
                # Remove pipe symbols and extra spaces
                headline = headline.replace('|', ',')
                headline = ' '.join(headline.split())  # Strip extra spaces
                
                return headline

            # Apply cleaning function to the "headline" column
            df['headline'] = df['headline'].apply(clean_headline)


        if 'organization_phone' in df.columns:
            def clean_phone_number(x):
                if pd.isna(x) or not str(x).strip():
                    return "Unknown"
                x = str(x).strip()
                if x.lower() == "unknown":
                    return "Unknown"
                if x.startswith("+"):
                    cleaned_number = '+' + ''.join(filter(str.isdigit, x))
                else:
                    cleaned_number = ''.join(filter(str.isdigit, x))
                return cleaned_number if cleaned_number else "Unknown"

            df['organization_phone'] = df['organization_phone'].apply(clean_phone_number)


      
        
        if 'organization_website' in df.columns:
            def clean_company_website(url, unique_id):
                if pd.isna(url) or not str(url).strip() or url.lower() in ["unknown", "n/a"]:
                    return f"https://unknown-company-{unique_id}.com"
                url = url.strip()
                if not url.startswith(("http://", "https://")):
                    url = "https://" + url
                return url

            df['organization_website'] = df.apply(
                lambda row: clean_company_website(row['organization_website'], row.name), axis=1
            )
        # unknown_count = df['location'].str.lower().str.strip().eq('unknown').sum()
        # print(f"Number of 'Unknown' values in the 'location' column: {unknown_count}")

        #create country column from location
        # if 'location' in df.columns:
        #     df['country'] = df['location'].apply(extract_country)

        # unknown_count1 = df['location'].str.lower().str.strip().eq('unknown').sum()
        # print(f"Number of 'Unknown' values in the 'location' column after the function exreact_country: {unknown_count1}")


        # Impute location using LinkedIn URL
        # df = impute_location(df)

        # unknown_count2 = df['location'].str.lower().str.strip().eq('unknown').sum()
        # print(f"Number of 'Unknown' values in the 'location' column after imputation: {unknown_count2}")


        # Duplicate rows for each email
        df = expand_emails(df)
        
        # Create uniqueId column by combining 'id' and 'email'
        df['uniqueId'] = df['id'].fillna("Unknown") + "_" + df['email'].fillna("Unknown")   
         
            

        # Drop duplicates based on 'id' and 'email'
        df = df.drop_duplicates(subset=['id', 'email'])

        # Filter records with email not equal to "Unknown"
        filtered_df = df[df['email'] != "Unknown"]

        # Prepare desired fields for insertion

        #         all_records = airtable_instance.get_all()

       
        
        
        # for i in range(0, len(record_ids), 10):
        #     batch_ids = record_ids[i:i + 10]
        #     try:
        #         airtable_old.batch_delete(batch_ids)
        #         print(f"Deleted records: {batch_ids}")
        #     except Exception as e:
        #         print(f"Failed to delete records {batch_ids}: {e}")
   
        
        # # Save full data to a CSV file
        # df.to_csv('full_cleaned_data.csv', index=False)

        # # Save filtered data to a CSV file
        # filtered_df.to_csv('filtered_cleaned_data.csv', index=False)
        
        # Fetch the record from ICP_information based on the email
        campaign_field_mapping = {
            "first_name": "RecipientFirstName",
            "last_name": "RecipientLastName",
            "email": "RecipientEmail",
            "organization_name": "RecipientCompany",
            # "location": "RecipientLocation",
            "title": "RecipientRole",
            "organization_website": "RecipientCompanyWebsite",
            "organization_short_description": "RecipientBio",
            "linkedin_url" : "linkedinProfileUrl",
            # "id" : "id"
            # Add other mappings as needed
        }
       

       
           

        default_values_campaign = {
            "CtaOptions" : "Schedule a quick 15-minute call to discuss how we can help GrowthTech Solutions scale personalized email outreach. At https://taippa.com/contact/ ",
            "ColorScheme" : "#000000,#ffffff,#b366cf,#6834cb",
            "Fonts" : " Headlines: Anton Body: Poppins"
        }
       
        email = "mohammed@taippa.com"
        icp_records = airtable_new2.search('email', email)

        if not icp_records:
            return jsonify({"error": f"No record found in ICP_information for email: {email}"}), 404

        # Extract fields from the first record
        icp_data = icp_records[0]['fields']  # Assuming first match is sufficient

        # Convert icp_data dictionary to a DataFrame
        icp_df = pd.DataFrame([icp_data])  # Wrap icp_data in a list to create a single-row DataFrame
        print("Fetched ICP Data as DataFrame:")
        print(icp_df)

        # Define field mapping for outreach_data
        icp_to_outreach_mapping = {
            "SenderEmail": "email",
            "SenderCompany": "companyName",
            "SenderName": "fullName",
            "SenderTitle": "jobtitle",
            "SenderCompanyWebsite": "companyWebsite",
            "KeyBenefits" : "solutionBenefits",            
            "ImpactMetrics" : "solutionImpactExamples",
            "UniqueFeatures" : "uniqueFeatures",
        }

        send_to_airtable_if_new(df, airtable_new, unique_field='uniqueId')
        send_to_airtable_if_new(
            filtered_df,
            airtable_new1,
            unique_field="uniqueId",
            desired_fields=[
                
                "linkedin_url",
                "first_name",
                "last_name",
                "email",
                "organization_name",
                "title",
                "organization_website",
                "organization_short_description",
                "uniqueId",
                "id"
            ],
            field_mapping=campaign_field_mapping,
            icp_to_outreach=icp_to_outreach_mapping,
            default_values=default_values_campaign,
            icp_df=icp_df
        )
        

        return jsonify({"message": "Data cleaned, updated, and old records processed successfully."})

    except Exception as e:
        return jsonify({"error": f"Error fetching, processing, or deleting data: {e}"}), 500


# @app.route("/data_analysis", methods=["GET"])
# def data_analysis():
#     try:
#         # Fetch data from the Airtable cleaned_profile_data table
#         all_records = airtable_new.get_all()
#         data = [record.get('fields', {}) for record in all_records]

#         if not data:
#             return jsonify({"message": "No data found in the cleaned_profile_data Airtable table."})

#         # Load data into a pandas DataFrame
#         df = pd.DataFrame(data)

#         # Collect diagnostic information
#         diagnostics = {
#             "dataset_shape": df.shape,
#             "columns": df.columns.tolist(),
#             "data_types": df.dtypes.astype(str).to_dict(),
#             "missing_values": df.isnull().sum().to_dict(),
#             "missing_percentage": ((df.isnull().sum() / len(df)) * 100).to_dict(),
#             "sample_data": df.head().to_dict(orient='records'),
#         }

#         # Perform basic data analysis
#         analysis = {}

#         # Summary statistics for numerical columns
#         numerical_columns = df.select_dtypes(include=["number"])
#         if not numerical_columns.empty:
#             analysis["numerical_summary"] = numerical_columns.describe().to_dict()

#         # Unique value counts for categorical columns
#         categorical_columns = df.select_dtypes(include=["object"])
#         if not categorical_columns.empty:
#             analysis["unique_value_counts"] = {
#                 col: df[col].nunique() for col in categorical_columns.columns
#             }

#         # Combine diagnostics and analysis
#         response = {
#             "diagnostics": diagnostics,
#             "analysis": analysis,
#         }

#         return jsonify(response)

#     except Exception as e:
#         return jsonify({"error": f"Error performing data analysis: {e}"}), 500


if __name__ == "__main__":
    app.run(debug=True)


