from socketshark.utils import _scrub_url


class TestScrubUrl:
    def test_it_keeps_the_same_url_if_no_credentials(self):
        url = 'http://example.com/path?q=1'
        assert _scrub_url(url) == url

    def test_it_keeps_the_same_url_if_only_username_present(self):
        url = 'http://user@example.com/path'
        assert _scrub_url(url) == url

    def test_it_scrubs_username_and_password(self):
        assert _scrub_url('http://user:secret@example.com/path') == (
            'http://*****:*****@example.com/path'
        )

    def test_it_scrubs_a_url_with_password_only(self):
        assert _scrub_url('http://:secret@example.com/path') == (
            'http://*****:*****@example.com/path'
        )

    def test_it_preserves_the_port(self):
        assert _scrub_url('redis://user:secret@localhost:6379/0') == (
            'redis://*****:*****@localhost:6379/0'
        )

    def test_it_preserves_the_query_and_the_fragment(self):
        assert _scrub_url('http://u:p@host/path?q=1#frag') == (
            'http://*****:*****@host/path?q=1#frag'
        )
