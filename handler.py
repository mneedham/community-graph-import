import os

import lib.meetup as meetup
import lib.so as so
import lib.summary as summary
import lib.twitter as twitter
import lib.github as github

from lib.encryption import decrypt_value


def str_to_bool(s):
    return s == 'True'


def generate_page_summary(event, _):
    if str_to_bool(os.environ.get("GENERATE_SUMMARY_PAGE", "False")):
        print("Event:", event)
        url = os.environ["READ_ONLY_URL"]
        user = os.environ["READ_ONLY_USER"]
        password = os.environ["READ_ONLY_PASSWORD"]
        title = os.environ["TITLE"]
        short_name = os.environ["SUMMARY"]
        logo_src = os.environ["LOGO"]

        summary.generate(url, user, password, title, short_name, logo_src)


def twitter_import(event, _):
    print("Event:", event)

    neo4j_url = os.environ.get('NEO4J_URL', "bolt://localhost")
    neo4j_user = os.environ.get('NEO4J_USER', "neo4j")

    neo4j_password = decrypt_value(os.environ['NEO4J_PASSWORD'])
    twitter_bearer = decrypt_value(os.environ['TWITTER_BEARER'])

    search = os.environ.get("TWITTER_SEARCH")

    twitter.import_links(neo4j_url=neo4j_url, neo4j_user=neo4j_user, neo4j_pass=neo4j_password,
                         bearer_token=twitter_bearer, search=search)




def twitter_clean_links(event, _):
    print("Event:", event)

    neo4j_url = os.environ.get('NEO4J_URL', "bolt://localhost")
    neo4j_user = os.environ.get('NEO4J_USER', "neo4j")
    neo4j_password = decrypt_value(os.environ['NEO4J_PASSWORD'])

    twitter.clean_links(neo4j_url=neo4j_url, neo4j_user=neo4j_user, neo4j_pass=neo4j_password)


def twitter_hydrate_links(event, _):
    print("Event:", event)

    neo4j_url = os.environ.get('NEO4J_URL', "bolt://localhost")
    neo4j_user = os.environ.get('NEO4J_USER', "neo4j")
    neo4j_password = decrypt_value(os.environ['NEO4J_PASSWORD'])

    twitter.hydrate_links(neo4j_url=neo4j_url, neo4j_user=neo4j_user, neo4j_pass=neo4j_password)


def twitter_unshorten_links(event, _):
    print("Event:", event)

    neo4j_url = os.environ.get('NEO4J_URL', "bolt://localhost")
    neo4j_user = os.environ.get('NEO4J_USER', "neo4j")
    neo4j_password = decrypt_value(os.environ['NEO4J_PASSWORD'])

    twitter.unshorten_links(neo4j_url=neo4j_url, neo4j_user=neo4j_user, neo4j_pass=neo4j_password)


def github_import(event, _):
    print("Event:", event)

    neo4j_url = os.environ.get('NEO4J_URL', "bolt://localhost")
    neo4j_user = os.environ.get('NEO4J_USER', "neo4j")
    neo4j_password = decrypt_value(os.environ['NEO4J_PASSWORD'])
    github_token = decrypt_value(os.environ["GITHUB_TOKEN"])

    tag = os.environ.get('TAG')

    github.import_github(neo4j_url=neo4j_url, neo4j_user=neo4j_user, neo4j_pass=neo4j_password, tag=tag,
                         github_token=github_token)


def meetup_events_import(event, _):
    print("Event:", event)

    neo4j_url = os.environ.get('NEO4J_URL', "bolt://localhost")
    neo4j_user = os.environ.get('NEO4J_USER', "neo4j")
    neo4j_password = decrypt_value(os.environ['NEO4J_PASSWORD'])
    meetup_key = decrypt_value(os.environ["MEETUP_API_KEY"])

    meetup.import_events(neo4j_url=neo4j_url, neo4j_user=neo4j_user, neo4j_pass=neo4j_password, meetup_key=meetup_key)


def meetup_groups_import(event, _):
    print("Event:", event)

    neo4j_url = os.environ.get('NEO4J_URL', "bolt://localhost")
    neo4j_user = os.environ.get('NEO4J_USER', "neo4j")
    neo4j_password = decrypt_value(os.environ['NEO4J_PASSWORD'])
    meetup_key = decrypt_value(os.environ["MEETUP_API_KEY"])

    tag = os.environ.get('TAG')

    meetup.import_groups(neo4j_url=neo4j_url, neo4j_user=neo4j_user, neo4j_pass=neo4j_password, tag=tag,
                         meetup_key=meetup_key)


def so_import(event, _):
    print("Event:", event)

    neo4j_url = os.environ.get('NEO4J_URL', "bolt://localhost")
    neo4j_user = os.environ.get('NEO4J_USER', "neo4j")
    neo4j_password = decrypt_value(os.environ['NEO4J_PASSWORD'])

    tag = os.environ.get('TAG')

    so.import_so(neo4j_url=neo4j_url, neo4j_user=neo4j_user, neo4j_pass=neo4j_password, tag=tag)
