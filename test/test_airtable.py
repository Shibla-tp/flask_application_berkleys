import unittest
import pandas as pd
from unittest import mock
from app import record_exists_in_airtable, send_to_airtable_if_new

class TestAirtableFunctions(unittest.TestCase):

    def test_record_exists_in_airtable(self):
        # Mocking the Airtable instance and its search method
        airtable_instance = mock.Mock()
        record_data = {"uniqueId": "123_email@example.com"}
        unique_field = "uniqueId"

        # Simulating the search method returning a result (record exists)
        airtable_instance.search.return_value = [{"id": "1", "uniqueId": "123_email@example.com"}]
        
        # Test if the record exists
        result = record_exists_in_airtable(airtable_instance, record_data, unique_field)
        self.assertTrue(result)

        # Simulating the search method returning an empty result (record does not exist)
        airtable_instance.search.return_value = []
        
        # Test if the record does not exist
        result = record_exists_in_airtable(airtable_instance, record_data, unique_field)
        self.assertFalse(result)
        
        # Ensure search was called with the correct parameters
        airtable_instance.search.assert_called_with(unique_field, record_data["uniqueId"])

    def test_send_to_airtable_if_new(self):
        # Sample data for the DataFrame
        df = pd.DataFrame({
            'id': ['1', '2'],
            'email': ['email1@example.com', 'email2@example.com'],
            'name': ['John Doe', 'Jane Doe'],
            'created_time': [None, None]  # Mocking created_time to ensure it's removed
        })

        # Mocking the Airtable instance
        airtable_instance = mock.Mock()

        # Mock record_exists_in_airtable to simulate checking for existing records
        airtable_instance.search.return_value = []  # Simulate no existing record

        # Define desired fields, field mapping, and default values for the test
        desired_fields = ['id', 'email', 'name']
        field_mapping = {'email': 'emailAddress'}
        default_values = {'status': 'new'}
        icp_to_outreach = {'outreach_field': 'icp_field'}
        icp_df = pd.DataFrame({'icp_field': ['value']})

        # Call the function to test
        send_to_airtable_if_new(df, airtable_instance, "email", desired_fields, field_mapping, default_values, icp_to_outreach, icp_df)

        # Debugging: Print all calls to airtable_instance.search to verify the arguments
        print(f"Calls to airtable_instance.search: {airtable_instance.search.call_args_list}")

        # Assert that the search method was called with the correct arguments for email1
        try:
            airtable_instance.search.assert_any_call("email", "email1@example.com")
        except AssertionError as e:
            print("AssertionError: ", e)
            # Optionally print the search call args to investigate further
            print(f"search call_args_list: {airtable_instance.search.call_args_list}")

        # Assert that insert is called twice, once for each row
        airtable_instance.insert.assert_any_call({
            'id': '1', 
            'emailAddress': 'email1@example.com',  # After field mapping
            'name': 'John Doe', 
            'uniqueId': '1_email1@example.com', 
            'status': 'new', 
            'outreach_field': 'value'  # From icp_df
        })
        airtable_instance.insert.assert_any_call({
            'id': '2', 
            'emailAddress': 'email2@example.com',  # After field mapping
            'name': 'Jane Doe', 
            'uniqueId': '2_email2@example.com', 
            'status': 'new', 
            'outreach_field': 'value'  # From icp_df
        })

if __name__ == "__main__":
    unittest.main()
