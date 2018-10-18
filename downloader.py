"""
egghead.io free course videos downloader

fetch url = response => extract course info ==> create course_info.txt ==> create urls.txt ==> download videos and transcripts
"""

import json
import logging
import subprocess
from functools import partial
from multiprocessing.pool import Pool
from pathlib import Path
from queue import Queue
from threading import Thread
from urllib import parse as urlparse
import time

import click
import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

EGGHEAD_COURSE_BASE_URL = 'https://egghead.io/courses/'
EGGHEAD_COURSE_LESSION_BASE_URL = 'https://egghead.io/lessons/'


class EggheadDownloaderError(Exception):
    """
    Base error class for all downloader errors
    """
    pass


class EggheadSiteSchemaChangedError(EggheadDownloaderError):
    """
    This error is rasied when parser encountered unexpected site html.
    """
    pass


class URL(click.ParamType):
    name = 'url'

    def convert(self, value, param, ctx):
        prased_value = urlparse.urlparse(value)
        if not prased_value.scheme == 'https':
            self.fail('Invalid URL scheme (%s).  Only HTTPS URLs are allowed' % prased_value.scheme, param, ctx)

        if not value.startswith(EGGHEAD_COURSE_BASE_URL):
            raise click.BadParameter('Invalid course url')

        return value


def hms_string(seconds_elapsed):
    """
    Convert elapsed time in seconds to human readable form.

    Arguments:
        seconds_elapsed (float):
    """
    h = int(seconds_elapsed / (60 * 60))
    m = int((seconds_elapsed % (60 * 60)) / 60)
    s = seconds_elapsed % 60.
    return "{}h:{}m:{}s".format(h, m, int(s))


def course_meta_data(course_url):
    response = requests.get(course_url)
    return response.content


def extract_course_info(course_page_html):
    soup = BeautifulSoup(course_page_html,'html.parser')
    script_with_course_info = soup.find_all('script', attrs={"data-component-name":"App"})
    if len(script_with_course_info) != 1:
        raise EggheadSiteSchemaChangedError

    return json.loads(script_with_course_info[0].text).get('course').get('course')


def extract_lesson_urls(course_info):
    return [lesson.get('http_url') for lesson in course_info.get('lessons')]


def dump_data(course_info, download_dir):
    with open(Path(download_dir, 'course_info.txt'), 'w') as course_info_file:
        json.dump(course_info, course_info_file, indent=4)

    with open(Path(download_dir, 'course_lesson_urls.txt'), 'w') as course_lesson_urls_file:
        lesson_urls = extract_lesson_urls(course_info)
        course_lesson_urls_file.write('\n'.join(lesson_urls))


def create_download_dir(dir_name):
    download_dir = Path(dir_name)
    if not download_dir.exists():
       download_dir.mkdir()
    return download_dir


class DownloadWorker(Thread):
    def __init__(self, queue):
       Thread.__init__(self)
       self.queue = queue

    def run(self):
       while True:
           directory, url, index = self.queue.get()
           download_video(directory, url, index)
           self.queue.task_done()


def download_video(directory, url, index):
    logger.info('Downloading [%s] into [%s]', url, directory)
    completed_process = subprocess.run(
        ['youtube-dl', '--no-part', '-o', Path(directory, '{}-%(title)s.%(ext)s'.format(index)), url],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    if completed_process.returncode == 0:
        logger.info('Download completed for [%s]', url)
    else:
        logger.info('Download failed for [%s] --- stderr: [%s]', url, completed_process.stderr.decode())


def download_videos_multi_threads(course_info, download_dir):
    start_time = time.time()

    queue = Queue()

    course_title = course_info.get('title')
    logger.info('Downloading course [%s]', course_title)

    for __ in range(8):
        worker = DownloadWorker(queue)
        # let the main thread exit even though the workers are blocking
        worker.daemon = True
        worker.start()

    lesson_urls = extract_lesson_urls(course_info)

    for index, lesson_url in enumerate(lesson_urls):
       logger.info('Queueing {}'.format(lesson_url))
       queue.put((download_dir, lesson_url, index))

    # let main thread to wait for the queue to finish processing all the tasks
    queue.join()

    print('Course download completed in [{}]'.format(hms_string(time.time() - start_time)))


def download_videos_multi_processes(course_info, download_dir):
    start_time = time.time()

    logger.info('Downloading course [%s]', course_info.get('title'))
    lesson_urls = extract_lesson_urls(course_info)
    download = partial(download_video, download_dir)
    with Pool(8) as pool:
       pool.map(download, lesson_urls)
       pool.close()
       pool.join()

    print('Course download completed in [{}]'.format(hms_string(time.time() - start_time)))


@click.command()
@click.argument('course_url', type=URL())
@click.argument('dest_dir', type=click.Path(exists=True))
def main(course_url, dest_dir):
    course_page_html = course_meta_data(course_url)
    course_info = extract_course_info(course_page_html)
    download_dir = create_download_dir(Path(dest_dir, course_info.get('title')))
    dump_data(course_info, download_dir)
    download_videos_multi_threads(course_info, download_dir)


if __name__ == '__main__':
    main()
