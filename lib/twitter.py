import http.client
import socket
from urllib.parse import urlparse

from neo4j.v1 import GraphDatabase, basic_auth

find_short_links_query = """\
MATCH (link:Link) 
WHERE exists(link.short) 
RETURN id(link) as id, link.url as url LIMIT 
{limit}
"""

unshorten_query = """\
UNWIND {data} AS row 
MATCH (link) 
WHERE id(link) = row.id 
SET link.url = row.url 
REMOVE link.short
"""


def unshorten_links(neo4j_url, neo4j_user, neo4j_pass):
    with GraphDatabase.driver(neo4j_url, auth=basic_auth(neo4j_user, neo4j_pass)) as driver:
        with driver.session() as session:
            result = session.run(find_short_links_query, {"limit": 1000})
            update = []
            rows = 0
            for record in result:
                try:
                    resolved = unshorten_url(record["url"])
                    print("original", record["url"], "resolved", resolved)
                    rows = rows + 1
                    if resolved != record["url"]:
                        update += [{"id": record["id"], "url": resolved}]
                except AttributeError:
                    print("Failed to resolve {0}. Ignoring for now".format(record["url"]))
                except socket.gaierror:
                    print("Failed to resolve {0}. Ignoring for now".format(record["url"]))
                except socket.error:
                    print("Failed to connect to {0}. Ignoring for now".format(record["url"]))

            print("urls", len(update), "records", rows)
            result = session.run(unshorten_query, {"data": update})
            print(result.consume().counters)


def unshorten_url(url):
    if url is None or len(url) < 11:
        return url
    parsed = urlparse(url)
    h = http.client.HTTPConnection(parsed.netloc)
    h.request('HEAD', parsed.path)
    response = h.getresponse()
    if response.status // 100 == 3 and response.getheader('Location'):
        loc = str(response.getheader('Location'))

        if loc != url and len(loc) <= 22:
            return unshorten_url(loc)
        else:
            return loc
    else:
        return url
