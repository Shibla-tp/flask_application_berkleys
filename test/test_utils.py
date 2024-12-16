import unittest
import pandas as pd
from app import expand_emails, clean_urls, clean_name, clean_phone_number  # Assuming this is your function's import

class TestExpandEmails(unittest.TestCase):
        
    def test_clean_name(self):
        # Sample DataFrame with some names having leading and trailing spaces
        data = {
            'first_name': [' John ', '  Alice', ' bob ', None],
            'last_name': [' Doe ', '  Smith ', '  Johnson ', 'Brown']
        }
        df = pd.DataFrame(data)

        # Call the clean_name function to clean the 'first_name' column
        cleaned_df = clean_name(df, 'first_name')

        # Assert the cleaned 'first_name' column values are stripped of leading/trailing spaces
        self.assertEqual(cleaned_df['first_name'][0], 'John')
        self.assertEqual(cleaned_df['first_name'][1], 'Alice')
        self.assertEqual(cleaned_df['first_name'][2], 'Bob')
        self.assertIsNone(cleaned_df['first_name'][3])  # None should remain unchanged

        # Call the clean_name function to clean the 'last_name' column
        cleaned_df = clean_name(cleaned_df, 'last_name')

        # Assert the cleaned 'last_name' column values are stripped of leading/trailing spaces
        self.assertEqual(cleaned_df['last_name'][0], 'Doe')
        self.assertEqual(cleaned_df['last_name'][1], 'Smith')
        self.assertEqual(cleaned_df['last_name'][2], 'Johnson')
        self.assertEqual(cleaned_df['last_name'][3], 'Brown')
    
    def test_single_email(self):
        data = {"email": ["test1@gmail.com"]}
        df = pd.DataFrame(data)
        
        result = expand_emails(df)
        expected_data = {"email": ["test1@gmail.com"]}
        expected_df = pd.DataFrame(expected_data)
        
        pd.testing.assert_frame_equal(result, expected_df)
    
    def test_multiple_emails(self):
        data = {"email": ["test1@gmail.com,test2@gmail.com"]}
        df = pd.DataFrame(data)
        
        result = expand_emails(df)
        expected_data = {"email": ["test1@gmail.com", "test2@gmail.com"]}
        expected_df = pd.DataFrame(expected_data)
        
        pd.testing.assert_frame_equal(result, expected_df)
    
    def test_unknown_email(self):
        data = {"email": ["Unknown"]}
        df = pd.DataFrame(data)
        
        result = expand_emails(df)
        expected_data = {"email": ["Unknown"]}
        expected_df = pd.DataFrame(expected_data)
        
        pd.testing.assert_frame_equal(result, expected_df)

    def test_clean_urls(self):
        url = " https://example.com/path?query=test "
        # Assuming 'unique_id' and 'column_name' are required by clean_urls
        unique_id = "some_unique_id"
        column_name = "url_column"
        
        # Modify the function call to include the missing arguments
        self.assertEqual(clean_urls(url, unique_id, column_name), "https://example.com/path?query=test")


    def test_valid_phone_numbers(self):
        # Test case for valid phone numbers
        self.assertEqual(clean_phone_number("+1-800-555-1234"), "+18005551234")
        self.assertEqual(clean_phone_number("123-456-7890"), "1234567890")
        self.assertEqual(clean_phone_number("+44 20 7946 0958"), "+442079460958")

    def test_invalid_phone_numbers(self):
        # Test case for invalid phone numbers (non-numeric characters removed)
        self.assertEqual(clean_phone_number("123abc456@7890"), "1234567890")
        self.assertEqual(clean_phone_number("!!!-555-0000"), "5550000")

    def test_empty_and_null(self):
        # Test case for empty, None, or invalid "Unknown" values
        self.assertEqual(clean_phone_number(""), "Unknown")
        self.assertEqual(clean_phone_number(None), "Unknown")
        self.assertEqual(clean_phone_number("Unknown"), "Unknown")

    def test_non_phone_input(self):
        # Test case for non-phone string values
        self.assertEqual(clean_phone_number("Some random text"), "Unknown")
        self.assertEqual(clean_phone_number("No phone number here"), "Unknown")
    
    def test_edge_cases(self):
        # Test case for phone number that only has digits but no "+" or extra symbols
        self.assertEqual(clean_phone_number("5551234"), "5551234")
        self.assertEqual(clean_phone_number("+5551234"), "+5551234")

if __name__ == "__main__":
    unittest.main()
