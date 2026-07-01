import unittest
from html.parser import HTMLParser

from app import create_app


class LinkParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.capture = False
        self.attrs = {}
        self.text = []
        self.links = []

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        classes = attrs.get("class", "")
        if tag == "a" and any(name in classes for name in ("btn", "nav-link", "navbar-brand")):
            self.capture = True
            self.attrs = attrs
            self.text = []

    def handle_data(self, data):
        if self.capture:
            self.text.append(data)

    def handle_endtag(self, tag):
        if tag == "a" and self.capture:
            self.links.append((" ".join(" ".join(self.text).split()), self.attrs.get("href")))
            self.capture = False
            self.attrs = {}
            self.text = []


class UITestCase(unittest.TestCase):
    def setUp(self):
        self.app = create_app()
        self.client = self.app.test_client()

    def test_main_pages_render(self):
        for path in ("/", "/materialize", "/schedule", "/schedules"):
            with self.subTest(path=path):
                response = self.client.get(path)
                self.assertEqual(response.status_code, 200)

    def test_home_navigation_buttons_target_existing_pages(self):
        parser = LinkParser()
        parser.feed(self.client.get("/").get_data(as_text=True))

        expected_links = {
            "Metrics Materializer": "/",
            "Home": "/",
            "Materialize": "/materialize",
            "Schedule": "/schedule",
            "Manage": "/schedules",
            "One-Time Run": "/materialize",
            "Manage Schedules": "/schedules",
        }

        for text, href in expected_links.items():
            with self.subTest(text=text):
                self.assertIn((text, href), parser.links)

    def test_layout_uses_consistent_navbar_and_footer_classes(self):
        html = self.client.get("/").get_data(as_text=True)
        self.assertIn('class="d-flex flex-column min-vh-100"', html)
        self.assertIn('class="navbar navbar-expand-lg navbar-dark navbar-pln"', html)
        self.assertIn('class="footer-pln text-light mt-auto"', html)


if __name__ == "__main__":
    unittest.main()
