import unittest
from shared_utils import normalize_text, extract_email, clean_note, location_match

class TestSharedUtils(unittest.TestCase):

    def test_normalize_text(self):
        self.assertEqual(normalize_text("  FOO  Bar  "), "foo bar")
        self.assertEqual(normalize_text("Tech Ltd"), "tech")
        self.assertEqual(normalize_text("Dave's Design"), "daves")
        self.assertEqual(normalize_text(None), "Unknown/None")

    def test_extract_email(self):
        self.assertEqual(extract_email("John Doe <john@example.com>"), "john@example.com")
        self.assertEqual(extract_email("contact info@test.ie"), "info@test.ie")
        self.assertEqual(extract_email(""), "")

    def test_clean_note(self):
        text = "Some random text. Note: This is the reason. More text."
        self.assertEqual(clean_note(text), "This is the reason. More text.")
        self.assertEqual(clean_note("No note here"), None)

    def test_location_match_coords(self):
        app1 = {'easting': 100, 'northing': 100}
        app2 = {'easting': 110, 'northing': 110} # dist ~14
        self.assertTrue(location_match(app1, app2))
        
        app3 = {'easting': 200, 'northing': 200} # dist > 50
        self.assertFalse(location_match(app1, app3))

    def test_location_match_text(self):
        app1 = {'location': "123 High St, Dublin"}
        app2 = {'location': "123 High Street, Dublin"}
        self.assertTrue(location_match(app1, app2))

if __name__ == '__main__':
    unittest.main()
