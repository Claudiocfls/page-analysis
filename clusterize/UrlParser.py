import re

class UrlParser:
    @staticmethod
    def parse_url(url):
        if not url:
            return ''
        return UrlParser.sanitize_url(url)

    @staticmethod
    def sanitize_url(url):
        a = re.sub('\\n', '', url)
        b = re.sub('\?.*','', a)
        c = re.sub('\/$', '', b)
        return re.sub('[\/&]ref=(.*)$', '', c)