from flask import Flask, jsonify, request
from airtable import Airtable
import pandas as pd
import numpy as np
import os
import pycountry

app = Flask(__name__)

# Old Airtable Configuration
BASE_ID_OLD = 'app5s8zl7DsUaDmtx'
API_KEY = 'patELEdV0LAx6Aba3.393bf0e41eb59b4b80de15b94a3d122eab50035c7c34189b53ec561de590dff3'  # Replace with a secure method to fetch the key
TABLE_NAME_OLD = 'linkedin_profile_data'

# New Airtable Configuration
BASE_ID_NEW = 'appTEXhgxahKgWLgx'
# BASE_ID_NEW = 'app5s8zl7DsUaDmtx'
TABLE_NAME_NEW = 'cleaned_profile_data'
TABLE_NAME_NEW1 = 'campaign_input'
# API_KEY_NEW = os.getenv('AIRTABLE_API_KEY', 'patELEdV0LAx6Aba3.393bf0e41eb59b4b80de15b94a3d122eab50035c7c34189b53ec561de590dff3')
API_KEY_NEW = os.getenv('AIRTABLE_API_KEY', 'patPgbQSC8pAg1Gbl.7ca275de5a5c8f2cf4389452e91c8f3f6c3e37bb2967c0f4cd8f41fa9d99044d')
#'AIRTABLE_API_KEY', 'patPgbQSC8pAg1Gbl.7ca275de5a5c8f2cf4389452e91c8f3f6c3e37bb2967c0f4cd8f41fa9d99044d'

airtable_old = Airtable(BASE_ID_OLD, TABLE_NAME_OLD, API_KEY)
airtable_new = Airtable(BASE_ID_NEW, TABLE_NAME_NEW, API_KEY_NEW)
airtable_new1 = Airtable(BASE_ID_NEW, TABLE_NAME_NEW1, API_KEY_NEW)

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


def send_to_airtable_if_new(df, airtable_instance, unique_field, desired_fields=None):
    """
    Inserts records into Airtable if they are not already present, based on a unique identifier.
    Handles duplicate linkedinProfileUrl with different emails by considering them as separate records.
    """
    for i, row in df.iterrows():
        record_data = row.dropna().to_dict()
        if desired_fields:
            record_data = {field: row[field] for field in desired_fields if field in row and not pd.isna(row[field])}

        # Ensure 'createdTime' is not part of the record
        if "createdTime" in record_data:
            del record_data["createdTime"]

        # Generate the uniqueId locally
        uniqueId = f"{record_data.get('linkedinProfileUrl', '')}_{record_data.get('email', '')}"
        record_data["uniqueId"] = uniqueId
      
        
        # Update the df with the new `uniqueId`
        # airtable_instance.update(i , {'uniqueId': uniqueId})


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

def extract_country(location):
    """
    Dynamically extracts the country name from a location string using pycountry.
    """
    if not location or location.lower() == "unknown":
        return "Unknown"

    # Normalize the location string for matching
    location = location.lower()
    
    # Iterate through all country names in pycountry
    for country in pycountry.countries:
        if country.name.lower() in location:
            return country.name
        
        # Check alternate names like "United States of America" (official_name)
        if hasattr(country, 'official_name') and country.official_name.lower() in location:
            return country.name

    return "Unknown"


@app.route("/", methods=["GET"])
def fetch_and_update_data():
    try:
        all_records = airtable_old.get_all()

        data = [record.get('fields', {}) for record in all_records]
        record_ids = [record['id'] for record in all_records]

        if not data:
            return jsonify({"message": "No data found in the old Airtable."})

        df = pd.DataFrame(data)

        # Replace problematic values
        df.replace([np.inf, -np.inf], np.nan, inplace=True)
        df = df.where(pd.notnull(df), None)

        for column in df.select_dtypes(include=['object']).columns:
            df[column].fillna("Unknown", inplace=True)

        if 'phoneNumber' in df.columns:
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

            df['phoneNumber'] = df['phoneNumber'].apply(clean_phone_number)

        if 'email' in df.columns:
            df['email'] = (
                df['email']
                .astype(str)
                .str.lower()
                .str.strip()
                .apply(lambda x: process_email(x))
            )

        if 'companyWebsite' in df.columns:
            def clean_company_website(url, unique_id):
                if pd.isna(url) or not str(url).strip() or url.lower() in ["unknown", "n/a"]:
                    return f"https://unknown-company-{unique_id}.com"
                url = url.strip()
                if not url.startswith(("http://", "https://")):
                    url = "https://" + url
                return url

            df['companyWebsite'] = df.apply(
                lambda row: clean_company_website(row['companyWebsite'], row.name), axis=1
            )

        # Duplicate rows for each email
        df = expand_emails(df)

        # Drop duplicates based on 'linkedinProfileUrl' and 'email'
        df = df.drop_duplicates(subset=['linkedinProfileUrl', 'email'])

        # Filter records with email not equal to "Unknown"
        filtered_df = df[df['email'] != "Unknown"]

        # for i in range(0, len(record_ids), 10):
        #     batch_ids = record_ids[i:i + 10]
        #     try:
        #         airtable_old.batch_delete(batch_ids)
        #         print(f"Deleted records: {batch_ids}")
        #     except Exception as e:
        #         print(f"Failed to delete records {batch_ids}: {e}")

        # Prepare desired fields for insertion

        #         all_records = airtable_instance.get_all()

        """
        #     Adds a `uniqueId` field to all records in the Airtable table by combining `linkedinProfileUrl` and `email`.
        #     """

        # for record in all_records:
        #     record_id = record['id']
        #     fields = record.get('fields', {})
        
        # Create uniqueId column by combining 'linkedinProfileUrl' and 'email'
        df['uniqueId'] = df['linkedinProfileUrl'].fillna("Unknown") + "_" + df['email'].fillna("Unknown")   
        #create country column from location
        if 'location' in df.columns:
            df['country'] = df['location'].apply(extract_country)    
        
        # Save full data to a CSV file
        df.to_csv('full_cleaned_data.csv', index=False)

        # Save filtered data to a CSV file
        filtered_df.to_csv('filtered_cleaned_data.csv', index=False)
        desired_fields = ['linkedinProfileUrl', 'firstName', 'lastName', 'email', 'Company', 'headline', 'description',
                          'location', 'country', 'imgUrl', 'fullName', 'phoneNumber', 'company', 'companyWebsite', 'timestamp', 'uniqueId']

        send_to_airtable_if_new(df, airtable_new, unique_field='uniqueId')
        send_to_airtable_if_new(filtered_df, airtable_new1, unique_field='uniqueId', desired_fields=desired_fields)

        return jsonify({"message": "Data cleaned, updated, and old records processed successfully."})

    except Exception as e:
        return jsonify({"error": f"Error fetching, processing, or deleting data: {e}"}), 500


if __name__ == "__main__":
    app.run(debug=True)

