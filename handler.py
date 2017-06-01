import os
from datetime import datetime, timezone

import boto
import flask
from ago import human
from flask import render_template
from neo4j.v1 import GraphDatabase

from lib.utils import import_links, decrypt_value, clean_links

twitter_query = """\
WITH ((timestamp() / 1000) - (7 * 24 * 60 * 60)) AS oneWeekAgo
MATCH (l:Link)<--(t:Tweet:Content)
WHERE not(t:Retweet)
WITH oneWeekAgo, l, t
ORDER BY l.cleanUrl, toInteger(t.created)
WITH oneWeekAgo, l.cleanUrl AS url, l.title AS title, collect(t) AS tweets WHERE toInteger(tweets[0].created) is not null AND tweets[0].created > oneWeekAgo AND not url contains "neo4j.com"
RETURN url, title, REDUCE(acc = 0, tweet IN tweets | acc + tweet.favorites + size((tweet)<-[:RETWEETED]-())) AS score, tweets[0].created * 1000 AS dateCreated, [ tweet IN tweets | head([ (tweet)<-[:POSTED]-(user) | user.screen_name]) ] AS users
ORDER BY score DESC
"""

github_query = """\
MATCH (n:Repository) WHERE EXISTS(n.created) AND n.updated > timestamp() - 7 * 60 * 60 * 24 * 1000
WITH n
ORDER BY n.updated desc
MATCH (n)<-[:CREATED]-(user) WHERE NOT (user.name IN ["neo4j", "neo4j-contrib"])
RETURN n.title, n.url, n.created, n.favorites, n.updated, user.name, n.created_at, n.updated_at
ORDER BY n.updated desc
"""

meetup_query = """\
MATCH (event:Event)<-[:CONTAINED]-(group)
WHERE timestamp() + 7 * 60 * 60 * 24 * 1000 > event.time > timestamp() - 7 * 60 * 60 * 24 * 1000
RETURN event, group
ORDER BY event.time
"""

app = flask.Flask('my app')


@app.template_filter('humanise')
def humanise_filter(value):
    return human(datetime.fromtimestamp(value / 1000), precision=1)


@app.template_filter("shorten")
def shorten_filter(value):
    return (value[:75] + '..') if len(value) > 75 else value


def generate_page_summary(event, _):
    print("Event:", event)

    url = os.environ["READ_ONLY_URL"]
    user = os.environ["READ_ONLY_USER"]
    password = os.environ["READ_ONLY_PASSWORD"]
    title = os.environ["TITLE"]
    summary = os.environ["SUMMARY"]

    with GraphDatabase.driver("bolt://{url}:7687".format(url=url), auth=(user, password)) as driver:
        with driver.session() as session:
            github_records = session.read_transaction(lambda tx: list(tx.run(github_query)))
            twitter_records = session.read_transaction(lambda tx: list(tx.run(twitter_query)))
            meetup_records = session.read_transaction(lambda tx: list(tx.run(meetup_query)))

    with app.app_context():
        rendered = render_template('index.html',
                                   github_records=github_records,
                                   twitter_records=twitter_records,
                                   meetup_records=meetup_records,
                                   title=title,
                                   time_now=str(datetime.now(timezone.utc)))

        local_file_name = "/tmp/{file_name}.html".format(file_name=summary)
        with open(local_file_name, "wb") as file:
            file.write(rendered.encode('utf-8'))

        s3_connection = boto.connect_s3()
        bucket = s3_connection.get_bucket(summary)
        key = boto.s3.key.Key(bucket, "{summary}.html".format(summary=summary))
        key.set_contents_from_filename(local_file_name)


def twitter_import(event, _):
    print("Event:", event)

    neo4j_url = os.environ.get('NEO4J_URL', "bolt://localhost")
    neo4j_user = os.environ.get('NEO4J_USER', "neo4j")

    neo4j_password = decrypt_value(os.environ['NEO4J_PASSWORD'])
    twitter_bearer = decrypt_value(os.environ['TWITTER_BEARER'])

    search = os.environ.get("TWITTER_SEARCH")

    import_links(neo4j_url=neo4j_url, neo4j_user=neo4j_user, neo4j_pass=neo4j_password,
                 bearer_token=twitter_bearer, search=search)


def twitter_clean_links(event, _):
    print("Event:", event)

    neo4j_url = os.environ.get('NEO4J_URL', "bolt://localhost")
    neo4j_user = os.environ.get('NEO4J_USER', "neo4j")
    neo4j_password = decrypt_value(os.environ['NEO4J_PASSWORD'])

    clean_links(neo4j_url=neo4j_url, neo4j_user=neo4j_user, neo4j_pass=neo4j_password)
