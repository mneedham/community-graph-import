import time
from base64 import b64decode
from base64 import b64encode

import requests
import urllib
import boto3

from neo4j.v1 import GraphDatabase, basic_auth

import urllib.parse


from urllib.parse import urlencode, urlparse, urlunparse, parse_qs

import_query = """\
UNWIND {tweets} AS t

WITH t
ORDER BY t.id

WITH t,
     t.entities AS e,
     t.user AS u,
     t.retweeted_status AS retweet

MERGE (tweet:Tweet:Twitter {id:t.id})
SET tweet:Content, tweet.text = t.text,
    tweet.created_at = t.created_at,
    tweet.created = apoc.date.parse(t.created_at,'s','E MMM dd HH:mm:ss Z yyyy'),
    tweet.favorites = t.favorite_count

MERGE (user:User {screen_name:u.screen_name})
SET user.name = u.name, user.id = u.id,
    user.location = u.location,
    user.followers = u.followers_count,
    user.following = u.friends_count,
    user.statuses = u.statuses_count,
    user.profile_image_url = u.profile_image_url,
    user:Twitter

MERGE (user)-[:POSTED]->(tweet)

FOREACH (h IN e.hashtags |
  MERGE (tag:Tag {name:LOWER(h.text)}) SET tag:Twitter
  MERGE (tag)<-[:TAGGED]-(tweet)
)

FOREACH (u IN e.urls |
  MERGE (url:Link {url:u.expanded_url})
  ON CREATE SET url.short = case when length(u.expanded_url) < 25 then true else null end
  SET url:Twitter
  MERGE (tweet)-[:LINKED]->(url)
)

FOREACH (m IN e.user_mentions |
  MERGE (mentioned:User {screen_name:m.screen_name})
  ON CREATE SET mentioned.name = m.name, mentioned.id = m.id
  SET mentioned:Twitter
  MERGE (tweet)-[:MENTIONED]->(mentioned)
)

FOREACH (r IN [r IN [t.in_reply_to_status_id] WHERE r IS NOT NULL] |
  MERGE (reply_tweet:Tweet:Twitter {id:r})
  MERGE (tweet)-[:REPLIED_TO]->(reply_tweet)
  SET tweet:Reply
)

FOREACH (retweet_id IN [x IN [retweet.id] WHERE x IS NOT NULL] |
    MERGE (retweet_tweet:Tweet:Twitter {id:retweet_id})
    MERGE (tweet)-[:RETWEETED]->(retweet_tweet)
    SET tweet:Retweet
)
"""


def import_links(neo4j_url, neo4j_user, neo4j_pass, bearer_token, search):
    if len(bearer_token) == 0:
        raise Exception("No Twitter Bearer token configured")

    with GraphDatabase.driver(neo4j_url, auth=basic_auth(neo4j_user, neo4j_pass)) as driver:
        with driver.session() as session:

            # Add uniqueness constraints.
            session.run("CREATE CONSTRAINT ON (t:Tweet) ASSERT t.id IS UNIQUE;")
            session.run("CREATE CONSTRAINT ON (u:User) ASSERT u.screen_name IS UNIQUE;")
            session.run("CREATE INDEX ON :Tag(name);")
            session.run("CREATE INDEX ON :Link(url);")

            q = urllib.parse.quote(search, safe='')
            max_pages = 100
            # False for retrieving history, True for catchup forward
            catch_up = True
            count = 100
            result_type = "recent"
            lang = "en"

            since_id = -1
            max_id = -1
            page = 1

            has_more = True
            while has_more and page <= max_pages:
                if catch_up:
                    result = session.run("MATCH (t:Tweet:Content) RETURN max(t.id) as sinceId")
                    for record in result:
                        print(record)
                        if record["sinceId"] is not None:
                            since_id = record["sinceId"]

                api_url = "https://api.twitter.com/1.1/search/tweets.json?q=%s&count=%s&result_type=%s&lang=%s" % (
                    q, count, result_type, lang)
                if since_id != -1:
                    api_url += "&since_id=%s" % (since_id)
                if max_id != -1:
                    api_url += "&max_id=%s" % (max_id)

                response = requests.get(api_url,
                                        headers={"accept": "application/json",
                                                 "Authorization": "Bearer " + bearer_token})
                if response.status_code != 200:
                    raise (Exception(response.status_code, response.text))

                json = response.json()
                meta = json["search_metadata"]

                if not catch_up and meta.get('next_results', None) is not None:
                    max_id = meta["next_results"].split("=")[1][0:-2]
                tweets = json.get("statuses", [])

                if len(tweets) > 0:
                    result = session.run(import_query, {"tweets": tweets})
                    print(result.consume().counters)
                    page = page + 1

                has_more = len(tweets) == count

                print("catch_up", catch_up, "more", has_more, "page", page, "max_id", max_id,
                      "since_id", since_id, "tweets", len(tweets))
                time.sleep(1)

                if json.get('backoff', None) is not None:
                    print("backoff", json['backoff'])
                    time.sleep(json['backoff'] + 5)


def clean_links(neo4j_url, neo4j_user, neo4j_pass):
    with GraphDatabase.driver(neo4j_url, auth=basic_auth(neo4j_user, neo4j_pass)) as driver:
        with driver.session() as session:
            query = "MATCH (l:Link) WHERE NOT EXISTS(l.cleanUrl) AND EXISTS(l.url) RETURN l, ID(l) AS internalId"
            result = session.run(query)

            updates = []
            for row in result:
                uri = row["l"]["url"]
                if uri:
                    uri = uri.encode('utf-8')
                    updates.append({"id": row["internalId"], "clean": clean_uri(uri)})

            print("Updates to apply", updates)

            update_query = """\
            UNWIND {updates} AS update
            MATCH (l:Link) WHERE ID(l) = update.id
            SET l.cleanUrl = update.clean
            """

            update_result = session.run(update_query, {"updates": updates})

            print(update_result)


def clean_uri(url):
    u = urlparse(url)
    query = parse_qs(u.query)

    for param in ["utm_content", "utm_source", "utm_medium", "utm_campaign", "utm_term"]:
        query.pop(param, None)

    u = u._replace(query=urlencode(query, True))
    return urlunparse(u)


def decrypt_value(encrypted):
    return boto3.client('kms').decrypt(CiphertextBlob=b64decode(encrypted))['Plaintext'].decode("utf-8")


def encrypt_value(value, kms_key):
    return b64encode(boto3.client('kms').encrypt(Plaintext=value, KeyId=kms_key)["CiphertextBlob"])
