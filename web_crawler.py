
from bs4 import BeautifulSoup
import Queue as q
import urllib2
import index
import urlparse
import urltools
import score
import threading
import sys
import robotparser
from datetime import datetime
import time
import re
from random import randint

SEED_URL_1 = "http://www.basketball-reference.com/awards/nba_50_greatest.html"
SEED_URL_2 = "http://www.basketball-reference.com/leaders/per_career.html"
SEED_URL_3 = "http://en.wikipedia.org/wiki/LeBron_James"
SEED_URL_4 = "http://www.theguardian.com/sport/2014/jul/11/lebron-james-to-return-to-cleveland"
AUTHOR = 'Purva'
MAX_DOMAIN_COUNT = 5000
MAX_CRAWL = 30000

temp_next_urls = {}
url_queue = q.PriorityQueue()
threadLock = threading.Lock()
rp = robotparser.RobotFileParser()
last_visited = {}
thread_data = []

reload(sys)
sys.setdefaultencoding("utf-8")


def normalize(url, parent):
    try:
        if '#' in url:
            url = url.split('#')[0]

        if url.startswith('http') and '//' not in url:
            url = url.lstrip('http:')

        if url:
            url = urlparse.urljoin(parent, url)
            url = urltools.normalize(url)

        if url.startswith('https'):
            url = url.replace('https','http')

        if re.match(r'http:/', url):
            url = url.replace('https', 'http')

        if url.endswith('jpg') or url.endswith('png') or url.endswith('jpeg'):
            url = None

        url = url.rstrip('/')
    except:
        url = None
    return url


def add_to_temp(link):
    global temp_next_urls
    threadLock.acquire()
    url = link.keys()[0]
    if url in temp_next_urls:
        cached_link = temp_next_urls[url]
        cached_link['anchors'].extend(link[url]['anchors'])
        cached_link['anchors'] = list(set(cached_link['anchors']))
        cached_link['titles'].extend(link[url]['titles'])
        cached_link['titles'] = list(set(cached_link['titles']))
        cached_link['inlinks'].extend(link[url]['inlinks'])
        cached_link['inlinks'] = list(set(cached_link['inlinks']))
    else:
        temp_next_urls.update(link)
    threadLock.release()
    return



def get_domain(url):
    parsed_uri = urlparse.urlparse(url)
    domain = ('{uri.scheme}://{uri.netloc}/').format(uri=parsed_uri)
    return domain


def can_request_domain(domain):
    can_visit = True
    t2 = datetime.now()

    if domain in last_visited:
        t1 = last_visited[domain]['time']
        delta = t2 - t1
        if delta.total_seconds() > 1:
            last_visited[domain]['time'] = t2
        else:
            can_visit = False
        last_visited[domain]['count'] += 1
    else:
        last_visited.update({domain:{'time':t2,'count':0}})

    return can_visit


def domain_count_exceeded(domain):
    exceeded = False
    if domain in last_visited:
        if last_visited[domain]['count'] >= MAX_DOMAIN_COUNT:
            exceeded = True
    return exceeded


def crawl_queue():
    global url_queue

    while not url_queue.empty():
        try:
            url_obj = url_queue.get()
            url = url_obj[1].keys()[0]
            print(url)

            domain = get_domain(url)
            if not can_request_domain(domain):
                time.sleep(1)

            page = urllib2.urlopen(url, timeout=10)
            html = page.read()
            header = str(page.info())
            soup = BeautifulSoup(html, "html.parser")
            page.close()
            title = soup.title.string

            out_links = []
            for link in soup.find_all('a'):

                if index.url_count() > MAX_CRAWL:
                    print('done')
                    break

                child_url = link.get('href')
                child_url = normalize(child_url, url)

                if not child_url:
                    continue

                domain = get_domain(child_url)
                try:
                    rp.set_url(urlparse.urljoin(domain, 'robots.txt'))
                    rp.read()
                    if not rp.can_fetch("*", child_url):
                        print ('no permission: ' + child_url)
                        continue
                except Exception as e:
                    print e
                    if '404' in e:
                        pass
                    else:
                        continue

                out_links.append(child_url)

                anchor = link.get_text().lstrip('\n').rstrip('\n')
                hint = link.get('title')

                if index.contains(child_url):
                    threadLock.acquire()
                    index.update_record({'url': child_url,'inlinks': [url]})
                    threadLock.release()
                else:
                    if not domain_count_exceeded(domain) and not score.reject_url(child_url):
                        add_to_temp({child_url: {'anchors': [anchor], 'titles': [hint], 'inlinks': [url], 'level': url_obj[1][url]['level'] + 1}})

            # kill all script and style elements
            for script in soup(["script", "style"]):
                script.extract()  # rip it out
            # get text
            text = soup.get_text()

            record = {}
            record['url'] = url
            record['outlinks'] = list(set(out_links))
            record['html'] = html
            record['title'] = title
            record['text'] = text
            record['inlinks'] = []
            try:
                record['inlinks'] = list(set((url_obj[1])[url]['inlinks']))
            except:
                pass
            record['author'] = AUTHOR
            record['header'] = header

            threadLock.acquire()
            index.add_record(record)
            threadLock.release()
            url_queue.task_done()

            if len(temp_next_urls) >= 20:
                threadLock.acquire()
                for x in temp_next_urls:
                    q = temp_next_urls[x]
                    s = score.get_score(x, q['anchors'], q['titles'], len(q['inlinks']), q['level'])
                    url_queue.put((s, {x: q}))
                temp_next_urls.clear()
                threadLock.release()

        except Exception as e:
            url_queue.task_done()
            print(e)
            continue

    return


def start_crawl():
    global url_queue
    index.create_index()
    index.fetch_cached_urls()

    url_cnt = index.url_count()
    if url_cnt == 0:
        url_queue.put((4, {SEED_URL_1: {'level': 0}}))
        url_queue.put((3, {SEED_URL_2: {'level': 0}}))
        url_queue.put((2, {SEED_URL_3: {'level': 0}}))
        url_queue.put((1, {SEED_URL_4: {'level': 0}}))
    else:
        for i in range(0,100):
            rand_index = randint(0,url_cnt)
            url = index.get_url(rand_index)
            url_queue.put((-1, {url: {'level': 0}}))

    threads = []

    for x in range(4):
        t = threading.Thread(target=crawl_queue)
        threads.append(t)

    for t in threads:
        t.start()

    for t in threads:
        t.join()

    return


start_crawl()

