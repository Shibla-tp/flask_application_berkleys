from flask import Flask, jsonify, request
from airtable import Airtable
import pandas as pd
import numpy as np
import os

app = Flask(__name__)

# Old Airtable Configuration
BASE_ID_OLD = 'app5s8zl7DsUaDmtx'
API_KEY = 'patELEdV0LAx6Aba3.393bf0e41eb59b4b80de15b94a3d122eab50035c7c34189b53ec561de590dff3'  # Replace with a secure method to fetch the key
TABLE_NAME_OLD = 'linkedin_profile_data'

# New Airtable Configuration
BASE_ID_NEW = 'appTEXhgxahKgWLgx'
TABLE_NAME_NEW = 'cleaned_profile_data'
TABLE_NAME_NEW1 = 'outreach_contacts'
API_KEY_NEW = os.getenv('AIRTABLE_API_KEY', 'patELEdV0LAx6Aba3.393bf0e41eb59b4b80de15b94a3d122eab50035c7c34189b53ec561de590dff3')
# 'AIRTABLE_API_KEY', 'patPgbQSC8pAg1Gbl.7ca275de5a5c8f2cf4389452e91c8f3f6c3e37bb2967c0f4cd8f41fa9d99044d'

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

    search_result = airtable_instance.search(unique_field, unique_value)
    return len(search_result) > 0


def send_to_airtable_if_new(df, airtable_instance, unique_field, desired_fields=None):
    """
    Inserts records into Airtable if they are not already present, based on a unique identifier.
    """
    for i, row in df.iterrows():
        record_data = row.dropna().to_dict()
        if desired_fields:
            record_data = {field: row[field] for field in desired_fields if field in row and not pd.isna(row[field])}

        if "createdTime" in record_data:
            del record_data["createdTime"]

        if not record_exists_in_airtable(airtable_instance, record_data, unique_field):
            try:
                airtable_instance.insert(record_data)
                print(f"Record {i} inserted successfully.")
            except Exception as e:
                print(f"Failed to insert record {i}: {e}")
        else:
            print(f"Record {i} already exists in Airtable. Skipping insertion.")

def process_email(email):
    # Handle empty strings or invalid values
    if not email or email in [",", "unknown"]:
        return "Unknown"

    # Split on commas if present, and take the first valid email
    emails = [e.strip() for e in email.split(',') if e.strip()]
    if emails:
        return emails[-1]  # Take the last email if multiple are present

    return "Unknown"  # Default to "Unknown" if no valid email found

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
                # Handle missing or invalid values
                if pd.isna(x) or not str(x).strip():
                    return "Unknown"
                
                x = str(x).strip()  # Remove leading/trailing whitespace
                
                # If already marked as "unknown"
                if x.lower() == "unknown":
                    return "Unknown"
                
                # Preserve the "+" if it exists at the start of the number
                if x.startswith("+"):
                    cleaned_number = '+' + ''.join(filter(str.isdigit, x))
                else:
                    cleaned_number = ''.join(filter(str.isdigit, x))
                
                return cleaned_number if cleaned_number else "Unknown"

            df['phoneNumber'] = df['phoneNumber'].apply(clean_phone_number)

        if 'email' in df.columns:
            df['email'] = (
                df['email']
                .astype(str)  # Ensure all entries are strings
                .str.lower()  # Convert to lowercase for consistency
                .str.strip()  # Remove leading/trailing whitespace
                .apply(lambda x: process_email(x))  # Apply custom processing
            )

        # Drop duplicates based on the 'LinkedIn Profile' column
        df = df.drop_duplicates(subset=['linkedinProfileUrl'])
        # Filter records with email not equal to "Unknown"
        filtered_df = df[df['email'] != "Unknown"]
        filtered_df = filtered_df.drop_duplicates(subset=['linkedinProfileUrl'])
        desired_fields = ['linkedinProfileUrl', 'firstName', 'lastName', 'email', 'Company', 'headline', 'description', 'location', 'imgUrl', 'fullName', 'phoneNumber', 'company', 'companyWebsite', 'timestamp']

        send_to_airtable_if_new(df, airtable_new, unique_field='linkedinProfileUrl')
        send_to_airtable_if_new(filtered_df, airtable_new1, unique_field='linkedinProfileUrl', desired_fields=desired_fields)

        # for i in range(0, len(record_ids), 10):
        #     batch_ids = record_ids[i:i + 10]
        #     try:
        #         airtable_old.batch_delete(batch_ids)
        #         print(f"Deleted records: {batch_ids}")
        #     except Exception as e:
        #         print(f"Failed to delete records {batch_ids}: {e}")

        return jsonify({"message": "Data cleaned, updated, and old records deleted successfully."})

    except Exception as e:
        return jsonify({"error": f"Error fetching, processing, or deleting data: {e}"}), 500


if __name__ == "__main__":
    app.run(debug=True)