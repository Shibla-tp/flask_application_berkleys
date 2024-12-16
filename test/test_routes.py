import unittest
from app import app

class TestFlaskRoutes(unittest.TestCase):
    def setUp(self):
        self.app = app.test_client()
        self.app.testing = True

    def test_home_route(self):
        response = self.app.get('/')
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Data cleaned, updated, and old records processed successfully.", response.data)

    def test_post_data(self):
        # Adjusted to send a GET request
        response = self.app.get('/post-data')
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Data received successfully", response.data)

if __name__ == '__main__':
    unittest.main()
