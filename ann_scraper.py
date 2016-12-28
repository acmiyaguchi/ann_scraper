import argparse
import os
import logging
import random
import sys
import sqlite3

from collections import namedtuple
from xml.etree import ElementTree
from urllib.request import urlopen

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# We use the ?(anime|manga) parameter, and pass it a slash delimted list of uids
# `api.xml?anime=4658/4199/11608`
ENCYCLOPEDIA_API = " http://cdn.animenewsnetwork.com/encyclopedia/api.xml"
RECENT_ADDITIONS_API = "http://www.animenewsnetwork.com/encyclopedia/reports.xml?id=148"
DATABASE_LIST_API = "http://www.animenewsnetwork.com/encyclopedia/reports.xml?id=155"

# entry type
Entry = namedtuple('Entry', ['id', 'gid', 'type', 'name', 'precision',
                             'vintage', 'generated_on'])

def create_table(conn):
    """ Create the table for keeping track of entries. """
    db_curs = conn.cursor()
    stmt = ("create table if not exists entry ("
            "id INTEGER PRIMARY KEY, "
            "gid INTEGER, "
            "type TEXT, "
            "name TEXT, "
            "precision TEXT, "
            "vintage TEXT, "
            "generated_on DATE"
            ")")
    db_curs.execute(stmt)

def insert_entries(conn, values):
    """ Insert an Entry into the entries table.
    conn: connection to the database
    values: list of Entry
    """
    db_curs = conn.cursor()
    stmt = ("insert into entry "
            "(id, gid, type, name, precision, vintage, generated_on) "
            "values (?, ?, ?, ?, ?, ?, ?)")
    db_curs.executemany(stmt, values)

def update_entries(conn, values):
    """ Update entries given an entry tuple. """
    db_curs = conn.cursor()
    stmt = "update entry set generated_on=? where entry.id=?"
    db_curs.executemany(stmt, values)

def download_cached(url, filepath, flush=False):
    """ Download the file at the specified url if the path does not exist. """
    if os.path.exists(filepath) and not flush:
        logging.info("Using cached version of %s at %s", url, filepath)
        return
    logging.info("Downloading %s to %s", url, filepath)
    if os.path.exists(filepath):
        os.remove(filepath)
    with open(filepath, 'w') as f:
        resp = urlopen(url)
        f.write(str(resp.read(), 'utf-8'))

def find_most_recent_id(path):
    """ Find the id of the most recent anime.

    The report is a list of xml fragments. The first element in the file
    describes the number of elements that follow. Each item in this report
    contains the href link, and the date in which it was added.

    path: path to the recently added anime report
    returns: the id of the most recently added anime
    """
    item = None
    with open(path, 'r') as f:
        # skip the header
        f.readline()
        item = ElementTree.fromstring(f.readline())
    recent_url = item.find('anime')
    recent_uid = int(recent_url.attrib['href'])
    return recent_uid

def update_recently_added(path, conn):
    """ Update the local database with recently added entries. """
    recents_report = os.path.join(path, "recents.xml")
    download_cached(RECENT_ADDITIONS_API, recents_report)
    recent_uid = find_most_recent_id(recents_report)
    logger.info("The most recent ANN anime id is %i", recent_uid)

    raise NotImplementedError("updated_recently_added")

def download_entries(path, conn, entry_type='anime', num_entries=50,
                     num_batch=50):
    """ Download xml files with a uniform distribution. If it is not neccesary
    to collect the entirety of the database, this will provide a reasonable
    subset of the data at each iteration.

    entry_type: either `anime` or `manga`
    num_entries: the number of entries to download, -1 will download until
        completion.
    """
    logger.info("Running database dump for %s", entry_type)

    # generate the a set of items to choose
    db_curs = conn.cursor()
    stmt = ("select id from entry "
            "where entry.type {} in ('manga', 'anthology')"
           ).format("not" if entry_type == 'anime' else "")
    res = db_curs.execute(stmt)
    total_id = [x[0] for x in res.fetchall()]

    stmt += " and entry.generated_on is not null"
    res = db_curs.execute(stmt)
    seen_id = [x[0] for x in res.fetchall()]

    not_seen = set(total_id) - set(seen_id)
    if num_entries < 0:
        num_entries = len(not_seen)

    # load the local entries into memory
    local_xml = os.path.join(path, "{}.xml".format(entry_type))
    try:
        local = ElementTree.parse(local_xml)
    except FileNotFoundError:
        root = ElementTree.fromstring("<ann></ann>")
        local = ElementTree.ElementTree(root)

    while num_entries > 0:
        num_batch = min(num_batch, num_entries)

        # randomly sample if we can't batch everything in one go
        if len(not_seen) <= num_batch:
            to_download = not_seen
        else:
            to_download = set(random.sample(not_seen, num_batch))

        # format the download url
        params = "?{}=".format(entry_type) + "/".join([str(x) for x in to_download])
        download_url = ENCYCLOPEDIA_API + params

        # download entries from ANN and convert to xml
        logger.info("Downloading %s entries: %s", entry_type, download_url)
        resp = urlopen(download_url)
        tree = ElementTree.fromstring(str(resp.read(), 'utf-8'))

        # extend the local copy of the entries and write to disk
        children = tree.findall(entry_type)
        local.getroot().extend(children)
        local.write(local_xml, encoding='utf-8')

        # update the database to reflect new information
        values = [entry_to_tuple(node) for node in children]
        update_entries(conn, values)

        not_seen -= to_download
        num_entries -= num_batch

        num_total = len(total_id)
        num_seen = num_total - len(not_seen)
        logger.info("%2.2f%% completed - %s out of %s downloaded",
                    num_seen/num_total*100, num_seen, num_total)

        conn.commit()

def entry_to_tuple(node):
    """ Turn an local entry into a tuple for updating generated on status. """
    return (node.attrib.get('generated-on'),
            node.attrib.get('id'))

def index_to_tuple(node):
    """ Convert an item node in index.xml into an Entry """
    values = []
    for key in ['id', 'gid', 'type', 'name', 'precision', 'vintage']:
        child = node.find(key)
        value = child.text if child is not None else None
        values.append(value)
    values.append(None) # generated_on
    return Entry(*values)

def remove_duplicates(entry_path):
    """ Remove duplicate entries from the anime/manga lists. """
    tree = ElementTree.parse(entry_path)
    root = tree.getroot()

    seen_ids = set()
    for node in root:
        entry_id = node.attrib['id']
        if entry_id in seen_ids:
            root.remove(node)
        seen_ids.add(entry_id)

    tree.write(entry_path, encoding='utf-8')

def regenerate_database(path, db_name):
    """ Regenerates the database from the master database list and ANN api. """
    db_path = os.path.join(path, db_name)
    if os.path.exists(db_path):
        logger.info("Removing existing database at %s", db_path)
        os.remove(db_path)
    conn = sqlite3.connect(db_path)
    create_table(conn)

    # download the list
    index_path = os.path.join(path, "index.xml")
    list_api = DATABASE_LIST_API + "&nlist=all"
    download_cached(list_api, index_path)

    # insert the values into a database
    tree = ElementTree.parse(index_path)
    values = (index_to_tuple(node) for node in tree.findall('item'))
    insert_entries(conn, values)

    # read locally cached entries into the database
    for entry_type in ('anime', 'manga'):
        entry_path = os.path.join(path, '{}.xml'.format(entry_type))
        if not os.path.exists(entry_path):
            continue
        remove_duplicates(entry_path)
        tree = ElementTree.parse(entry_path)
        values = [entry_to_tuple(node) for node in tree.getroot()]
        update_entries(conn, values)

    conn.commit()
    return conn

def progress(conn, entry_type='anime'):
    """ Returns the number of entries seen and known. """
    db_curs = conn.cursor()
    stmt = ("select count(*) from entry "
            "where entry.type {} in ('manga', 'anthology')"
           ).format("not" if entry_type == 'anime' else "")
    total = db_curs.execute(stmt).fetchone()[0]

    stmt += " and entry.generated_on is not null"
    seen = db_curs.execute(stmt).fetchone()[0]

    return seen, total

def main(argv):
    """ Entry point into the scraping tool. """
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--path', default="data")
    parser.add_argument('-r', '--regenerate-db', action="store_true")
    parser.add_argument('-s', '--status', action="store_true")
    parser.add_argument('-n', '--num-entries', type=int, default=50)
    parser.add_argument('-t', '--type', dest='entry_type', default='anime',
                        choices=['anime', 'manga'])
    args = parser.parse_args(argv)

    # create path for output
    path = args.path
    if not os.path.exists(path):
        os.makedirs(path)

    # create the tables and entries
    db_name = "ann.db"
    if args.regenerate_db or not os.path.exists(os.path.join(path, db_name)):
        logger.info("Regenerating the database")
        conn = regenerate_database(path, db_name)
    else:
        db_path = os.path.join(path, db_name)
        conn = sqlite3.connect(db_path)

    # view the status without running anything
    if args.status:
        seen, total = progress(conn, args.entry_type)
        logger.info("%2.2f%% completed - %s out of %s downloaded",
                    seen/total*100, seen, total)

    # download entries
    if not (args.regenerate_db or args.status):
        download_entries(path, conn, args.entry_type, args.num_entries)

if __name__ == '__main__':
    main(sys.argv[1:])
