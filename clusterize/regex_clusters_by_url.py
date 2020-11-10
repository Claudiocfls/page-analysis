from UrlParser import UrlParser
import re

# regexes representing each cluster for example site
regexes = {
  0: '^https?:\/\/www\.example\.com\.br\/product\/\d+\/[^\/\s]+',
  1: '^https?:\/\/www\.example\.com\.br\/path.*',
}

prioritized_regexes = [0,1]

UNKNOWN_CLUSTER = 3

def find_cluster_by_regex_for_site_example(url):
    sanitized_url = UrlParser.parse_url(url)
    for ind in prioritized_regexes:
        regex = regexes[ind]
        if re.match(regex, sanitized_url):
            return ind
    return UNKNOWN_CLUSTER
