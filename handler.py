import datetime
import os
from datetime import datetime, timezone

import boto
import flask
from ago import human
from flask import render_template
from neo4j.v1 import GraphDatabase


def github_links(tx):
    records = []
    for record in tx.run("""\
        MATCH (n:Repository) WHERE EXISTS(n.created) AND n.updated > timestamp() - 7 * 60 * 60 * 24 * 1000
        WITH n
        ORDER BY n.updated desc
        MATCH (n)<-[:CREATED]-(user) WHERE NOT (user.name IN ["neo4j", "neo4j-contrib"])
        RETURN n.title, n.url, n.created, n.favorites, n.updated, user.name, n.created_at, n.updated_at
        ORDER BY n.updated desc
        """):
        records.append(record)
    return records


def twitter_links(tx):
    records = []
    for record in tx.run("""\
        WITH ((timestamp() / 1000) - (7 * 24 * 60 * 60)) AS oneWeekAgo
        MATCH (l:Link)<--(t:Tweet:Content)
        WHERE not(t:Retweet)
        WITH oneWeekAgo, l, t
        ORDER BY l.cleanUrl, toInteger(t.created)
        WITH oneWeekAgo, l.cleanUrl AS url, l.title AS title, collect(t) AS tweets WHERE toInteger(tweets[0].created) is not null AND tweets[0].created > oneWeekAgo AND not url contains "neo4j.com"
        RETURN url, title, REDUCE(acc = 0, tweet IN tweets | acc + tweet.favorites + size((tweet)<-[:RETWEETED]-())) AS score, tweets[0].created * 1000 AS dateCreated, [ tweet IN tweets | head([ (tweet)<-[:POSTED]-(user) | user.screen_name]) ] AS users
        ORDER BY score DESC
        """):
        records.append(record)
    return records


def meetup_events(tx):
    records = []
    for record in tx.run("""\
    MATCH (event:Event)<-[:CONTAINED]-(group)
    WHERE timestamp() + 7 * 60 * 60 * 24 * 1000 > event.time > timestamp() - 7 * 60 * 60 * 24 * 1000
    RETURN event, group
    ORDER BY event.time
    """):
        records.append(record)
    return records

app = flask.Flask('my app')

@app.template_filter('humanise')
def humanise_filter(value):
    return human(datetime.fromtimestamp(value / 1000), precision=1)


@app.template_filter("shorten")
def shorten_filter(value):
    return (value[:75] + '..') if len(value) > 75 else value


def run(event, context):
    current_time = datetime.datetime.now().time()
    name = context.function_name

    # print("Your cron function " + name + " ran at " + str(current_time))

    # print("Your environment variable is {neo4j_url}".format(neo4j_url=os.environ["NEO4J_URL"]))
    # print("Your varying environment variable is {varying}".format(varying=os.environ["MY_VAR"]))
    print(os.environ["READ_ONLY_URL"])


def generate_page_summary(event, context):
    print("Event:", event)

    url = os.environ["READ_ONLY_URL"]
    user = os.environ["READ_ONLY_USER"]
    password = os.environ["READ_ONLY_PASSWORD"]
    title = os.environ["TITLE"]
    summary = os.environ["SUMMARY"]

    driver = GraphDatabase.driver("bolt://{0}:7687".format(url), auth=(user, password))
    with driver.session() as session:
        github_records = session.read_transaction(github_links)
        twitter_records = session.read_transaction(twitter_links)
        meetup_records = session.read_transaction(meetup_events)

    with app.app_context():
        time_now = str(datetime.now(timezone.utc))

        rendered = render_template('index.html',
                                   github_records=github_records,
                                   twitter_records=twitter_records,
                                   meetup_records=meetup_records,
                                   title=title,
                                   time_now=time_now)

        local_file_name = "/tmp/{0}.html".format(summary)
        with open(local_file_name, "wb") as file:
            file.write(rendered.encode('utf-8'))

        s3_connection = boto.connect_s3()
        bucket = s3_connection.get_bucket(summary)
        key = boto.s3.key.Key(bucket, "{0}.html".format(summary))
        key.set_contents_from_filename(local_file_name)
