import os
import requests
import time
import hashlib
import csv
import tldextract
import logging
import sqlite3
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser
from collections import deque
from bs4 import BeautifulSoup

class Crawler:
    def __init__(self, start_url):
        self.start_url = start_url
        self.visited = set()
        self.queue = deque([start_url])
        self.domain = urlparse(start_url).netloc
        self.robots = self.get_robots_parser(start_url)
        self.crawl_delay = self.get_crawl_delay()
       
    def get_robots_parser(self, url):
        parsed = urlparse(url)
        robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
        rp = RobotFileParser()
        rp.set_url(robots_url)
        try:
            rp.read()
            logger.info('Found robots.txt with specifications below')
            logger.info(rp)
        except Exception:
            logger.warning("No robots.txt found, proceeding with nothing disallowed")
            pass  # If robots.txt is not found, allow all
        return rp

    def get_crawl_delay(self):
        delay = self.robots.crawl_delay("*")
        return delay if delay is not None else 0

    # Not functioning correctly, passing all sites as valid despite explicitly matching - not sure why this is happening
    def allowed(self, url):
        return self.robots.can_fetch("*", url)

    def crawl(self, max_pages=1):
        while self.queue and len(self.visited) < max_pages:
            url = self.queue.popleft()

            logger.warning(f"Start of crawl function, current url is {url}")
            # Commented out because of can_fetch not working as it should
            # if url in self.visited or not self.allowed(url):
                # continue
            if url in self.visited:
                continue

            try:
                logger.info(f'Fetching: {url}')
                response = requests.get(url, timeout=5)
                response_hash = hashlib.sha3_224(response.text.encode()).hexdigest()

                if response.status_code != 200 or 'text/html' not in response.headers.get('Content-Type', ''):
                    logger.warning(f'Fetched {url} with status code of: {response.status_code} or found a non text/html content type')
                    self.visited.add(url)
                    continue
               
                logger.info(f'Fetched {url} with status code of: {response.status_code} with a sha3 hash of {response_hash}')

                try:
                    # Getting the source url for the current url being crawled from the database
                    cur.execute(f"SELECT source_url FROM links WHERE url='{url}'")
                    source_url = cur.fetchone()[0]
                    # Inserting the final values into the database
                    cur.execute(f"INSERT INTO crawls VALUES('{url}', '{source_url}', {response.status_code}, '{response_hash}')")
                    con.commit()
                    logger.info(f"Saved fetched url {url} to database")
                except sqlite3.Error as e:
                    logger.warning(f"DB Error: {e}")
               
                soup = BeautifulSoup(response.text, 'html.parser')
                for link in soup.find_all('a', href=True):
                    abs_url = urljoin(url, link['href'])
                    parsed = urlparse(abs_url)
                    # Not happy with this solution of an overly large conditional check but it does work.
                    if parsed.netloc == self.domain and abs_url not in self.visited and not parsed.path.startswith("/explore") and not parsed.path.startswith("/search-master/opensearch") and tldextract.extract(abs_url).subdomain != "wayback" and not abs_url.endswith("#aitMainContent") and not abs_url.endswith(".pdf"):
                        if parsed.query:
                            # Still some weirdness with queries in certain urls but again, it works.
                            if parsed.query.startswith("page=") or parsed.query.startswith("show="):
                                self.queue.append(abs_url)
                                try:
                                    # Adding each found url into a table in the databse with the current url so it can be fetched as to where a url was found
                                    cur.execute(f"INSERT INTO links VALUES('{abs_url}', '{url}')")
                                    con.commit()
                                except sqlite3.Error as e:
                                    logger.warning(f"DB Error: {e}")
                            else:
                                logger.info(f"Not adding {abs_url} to queue because failing page query check")
                        self.queue.append(abs_url)
                        try:
                            # Adding each found url into a table in the databse with the current url so it can be fetched as to where a url was found
                            cur.execute(f"INSERT INTO links VALUES('{abs_url}', '{url}')")
                            con.commit()
                        except sqlite3.Error as e:
                            logger.warning(f"DB Error: {e}")
                   
            except Exception as error:
                logger.warning(f'Exception encountered: {error}')
                continue
            finally:
                con.commit()
                self.visited.add(url)
                logger.warning(self.visited)
                time.sleep(self.crawl_delay)

if __name__ == "__main__":
    print(f"Started at {time.strftime('%x%X%Z')}")

    con = sqlite3.connect("crawler.db")
    # Clearing out the tables if they exist to allow for clean data entry
    con.execute("DROP TABLE IF EXISTS crawls")
    con.execute("DROP TABLE IF EXISTS links")
    con.commit()

    cur = con.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS crawls (url TEXT, extract_url TEXT, status_code INTEGER, payload_hash TEXT)")
    con.commit()
   
   # Can be cleaned removed without issue if appending to previous logs is desired
    log_file_path = 'crawlerLog.log'
    if os.path.exists(log_file_path):
        os.remove(log_file_path)
        print(f"Log file '{log_file_path}' deleted successfully.")
    else:
        print(f"Log file '{log_file_path}' not found.")
   
    logger = logging.getLogger(__name__)
    logging.basicConfig(filename='crawlerLog.log', level=logging.INFO)
    logger.info(f"Started at {time.strftime('%x%X%Z')}")

    start_url = "https://archive-it.org/"
    cur.execute("CREATE TABLE IF NOT EXISTS links (url TEXT, source_url TEXT)")
    cur.execute(f"INSERT INTO links VALUES('{start_url}', '{start_url}')")
    con.commit()

    logger.info(f"Using: {start_url} as starting URL")

    crawler = Crawler(start_url)
    crawler.crawl(max_pages=3)

    logger.info(f"Finished at {time.strftime('%x%X%Z')}")
    print(f"Finished at {time.strftime('%x%X%Z')}")
    con.commit()
    con.close()
