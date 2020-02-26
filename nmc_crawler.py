import os
import sys
import click
import time
import requests
import argparse
import logging
from bypy import ByPy
from bs4 import BeautifulSoup

LOG_MODE = 'INFO'
LOGFILE = 'nmc_crawler.log'
PROJECTDIR = os.path.dirname(os.path.abspath(__file__))
domain_name = 'http://www.nmc.cn'
base_mosaic_url = domain_name + '/publish/radar/chinaall.html'
base_station_url = domain_name + '/publish/radar/bei-jing/da-xing.htm'
base_wc_url = domain_name + '/publish/observations/china/dm/weatherchart-h000.htm'
base_ltng_url = domain_name + '/publish/observations/lighting.html'

# initialize the logger
logFile = os.path.join(PROJECTDIR, LOGFILE)
logModeDict = {
    'INFO': logging.INFO,
    'WARNING': logging.WARNING,
    'DEBUG': logging.DEBUG,
    'ERROR': logging.ERROR
    }

logFile = os.path.join(
    PROJECTDIR, LOGFILE)
logger = logging.getLogger(__name__)

fh = logging.FileHandler(logFile)
fh.setLevel(logModeDict[LOG_MODE])
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logModeDict[LOG_MODE])

formatterFh = logging.Formatter('%(asctime)s - ' +
                                '%(funcName)s - %(lineno)d - %(message)s')
formatterCh = logging.Formatter('%(message)s')
fh.setFormatter(formatterFh)
ch.setFormatter(formatterCh)

logger.addHandler(fh)
logger.addHandler(ch)

logger.setLevel(logModeDict['DEBUG'])

# Get all urls
class NMC(object):
    def __init__(self, kind, area, resolution, savepath, verbose):
        self.kind = kind
        self.area = area
        self.resolution = resolution
        self.savepath = savepath
        self.imgFiles = []
        self.verbose = verbose

        if self.area == 'region':
            self.suffix = 'html'
            self.base_url = base_mosaic_url

        elif self.area == 'station':
            self.suffix = 'htm'
            self.base_url = base_station_url

        elif self.area == 'china':
            if self.kind == 'ltng':
                self.base_url = base_ltng_url
                self.suffix = 'html'
            elif self.kind == 'weatherchart':
                self.base_url = base_wc_url
                self.suffix = 'htm'

    def get_urls(self):
    # Get urls
        htmls = []
        while htmls == []:
            try:
                base_page = requests.get(self.base_url, timeout=10)
                soup = BeautifulSoup(base_page.content, 'html.parser')
                for link in soup.findAll('a'):
                    sub_urls = link.get('href')
                    if self.kind == 'radar':
                        if sub_urls.startswith('/publish/radar/') & sub_urls.endswith(self.suffix):
                            htmls.append(sub_urls)
                    elif self.kind == 'weatherchart':
                        if sub_urls.startswith('/publish/observations/china/dm/weatherchart') & sub_urls.endswith(self.suffix):
                            htmls.append(sub_urls)
                    elif self.kind == 'ltng':
                        if sub_urls.startswith('/publish/observations/lighting') & sub_urls.endswith(self.suffix):
                            htmls.append(sub_urls)
            except:
                self.sleep_message('get_main_url')

        main_url = ['{}{}'.format(domain_name,html) for html in list(set(htmls))]

        if self.kind == 'ltng' or self.area == 'region':
            return main_url
        else:
            urls = []
            for url in main_url:
                urls.extend(self.get_sub_url(url))

            return urls

    def get_sub_url(self, url):
        htmls = []
        while htmls == []:
            try:
                base_page = requests.get(url, timeout=10)
                soup = BeautifulSoup(base_page.content, 'html.parser')
                for link in soup.findAll('a'):
                    sub_htmls = link.get('href')
                    if self.kind == 'radar':
                        if sub_htmls.startswith('/publish/radar/') & sub_htmls.endswith(self.suffix):
                            # Filter out Surpluses
                            if sub_htmls.split("/")[3] == url.split("/")[5]:
                                htmls.append(sub_htmls)
                    elif self.kind == 'weatherchart':
                        if sub_htmls.startswith('/publish/observations/') & sub_htmls.endswith(url[-8:]):
                            if sub_htmls.split("/")[3] == url.split("/")[5]:
                                htmls.append(sub_htmls)                    
            except:
                self.sleep_message('get_sub_url')

        return ['{}{}'.format(domain_name,html) for html in list(set(htmls))]

    def download(self, urls):
        # Download images to savepath
        savepath = os.path.join(self.savepath, self.kind, self.area)

        for url in urls:
            try:
                htmls = self.get_img_urls(url)

                # get dir_name and subdir_name
                if self.area == 'region':
                    dir_name = url.split("/")[5][:-5]
                    subdir_name = ''
                elif self.area == 'station':
                    dir_name = url.split("/")[5]
                    subdir_name = url.split("/")[6][:-4]
                elif self.area == 'china':
                    dir_name = ''
                    subdir_name = url.split("/")[-1][:-4].replace(".", "")

                if self.area == 'region':
                    logger.info('    Downloading {0} {1}'.format(dir_name, 'mosaics'))
                elif self.area == 'station':# and self.verbose > 0:
                    logger.info('    Downloading {0} {1}'.format(subdir_name, 'mosaics'))
                elif self.area == 'china':# and self.verbose > 0:
                    logger.info('    Downloading {0}'.format(subdir_name))

                # get name/url and download
                for html in htmls:
                    # get date for img_name
                    split_html = html.split("/")
                    date = ''.join(split_html[4:7])
                    if self.kind == 'ltng' or dir_name == 'chinaall' or self.area == 'china':
                        sdate = split_html[9].find(date)
                        edate = sdate + 12
                        name  = split_html[9][sdate:edate]
                    else:
                        sdate = split_html[8].find(date)
                        edate = sdate + 12
                        name  = split_html[8][sdate:edate]

                    # Check whether dirs of savepath exists. If not, create it.
                    if self.kind == 'radar':
                        full_savepath = os.path.join(savepath, dir_name, subdir_name, date[:-2], date[-4:])
                    elif self.kind in ['weatherchart', 'ltng']:
                        full_savepath = os.path.join(savepath, dir_name, date[:-2], subdir_name)

                    if not os.path.exists(full_savepath):
                        if self.verbose > 0:
                            logger.info('mkdir {0}'.format(full_savepath))
                        os.makedirs(full_savepath, exist_ok=True)

                    if self.kind == 'radar':
                        filename = name + '.png'
                        fullfilename = os.path.join(full_savepath, filename)
                    else:
                        filename = name + '.jpg'
                        fullfilename = os.path.join(full_savepath, name + '.jpg')

                    if os.path.isfile(fullfilename):
                        if self.verbose > 0:
                            logger.info('    {0} exists in {1} Skip!'.format(name, full_savepath))
                    else:
                        count = 1
                        try:
                            res = requests.get(html, timeout=10)
                        except Exception as e:
                            while count <= 3:
                                try:
                                    res = requests.get(html, timeout=10)
                                    break
                                except Exception as e:
                                    if self.verbose > 0:
                                        print('Failure in {0} try: {1}'.format(count, e))
                                    count = count + 1
                        
                        if count > 3:
                            if self.verbose > 0:
                                print('Failure in downloading {0}'.format(html))
                        else:
                            with open(fullfilename, 'wb') as fh:
                                fh.write(res.content)
                            self.imgFiles.append(
                                {
                                'savepath': self.savepath,
                                'kind': self.kind,
                                'area': self.area,
                                'dir_name': dir_name,
                                'date': date[:-2],
                                'subdir_name': subdir_name,
                                'file': filename
                                }
                            )
                            if self.verbose > 0:
                                logger.info('        Downloading {0}'.format(name))

            finally:
                finish_output = '    Finish. Save images to ' + os.path.join(savepath,dir_name)
                if self.area == 'region':
                    logger.info(finish_output)
        
        if self.area == 'station':
            logger.info(finish_output)

    def get_img_urls(self, url):
        page = ''
        while page == '':
            try:
                # Download the Response object and parse
                page = requests.get(url, timeout=10)
                soup = BeautifulSoup(page.content, 'html.parser')

                # Finding all instances of 'img' at once
                pics = soup.find_all('p', class_='img')

                # get url of each picture and save to a list
                img_urls = []
                for pic in pics:
                    img_url = pic.img.get('data-original')
                    img_urls.append(img_url)
                img_urls = [url.replace('small', self.resolution) for url in img_urls]
            except:
                self.sleep_message('get_img_urls')

        return img_urls

    def upload_bdy(self, bdy_path, delete):
        """
        upload imgs to BaiduNet Disk.
        """

        if bdy_path:
            # bdy_path was set!!!
            bp = ByPy()

            for img in self.imgFiles:
                BDY_Path = os.path.join(
                    bdy_path, img['kind'], img['area'],
                    img['dir_name'], img['date'], img['subdir_name'],
                    img['file']
                )
                local_Path = os.path.join(
                    img['savepath'], img['kind'], img['area'],
                    img['dir_name'], img['date'], img['subdir_name'],
                    img['file']
                )

                bp.upload(local_Path, BDY_Path)

                if delete:
                    os.remove(local_Path)


    def sleep_message(self, func_name):
        print('Connection of ' + func_name + ' refused by the server..')
        print('Let me sleep for 5 seconds')
        print('ZZzzzz...')
        time.sleep(5)

# -------------------------------------------------------
@click.command()
@click.option(
    '--kind',
    '-k',
    type=click.Choice(['radar', 'weatherchart', 'ltng']),
    help='Kind of data',
    required=True
)

@click.option(
    '--area',
    '-a',
    default = 'region',
    type=click.Choice(['all', 'region','station']),
    help='''
    Region of maps: 
    For weatherchart and ltng, you don't
    need to specify this parameter.
    ''',
    show_default=True
)

@click.option(
    '--resolution',
    '-r',
    default = 'medium',
    type=click.Choice(['medium', 'small']),
    help='Resolution of figures',
    show_default=True
)

@click.option(
    '--savepath',
    '-s',
    default = './',
    help='Savepath of figures',
    show_default=True
)

@click.option(
    '--bdy_path',
    '-b',
    default=None,
    help='BaiduNet Disk path. (If set, the files will be uploaded to cloud)',
    show_default=True
)

@click.option(
    '--delete',
    '-d',
    default=0,
    help='Whether to delete the local figures. If bdy_path was set, this will be set to 1.',
    show_default=True
)

@click.option(
    '--verbose',
    '-v',
    default = 0,
    help='verbose level',
    show_default=True
)
# -------------------------------------------------------

def main(kind, area, resolution, savepath, bdy_path, delete, verbose):
    """
    Download weathercharts and radar figures from NMC.
    """
    s = requests.session()
    s.keep_alive = False

    if kind == 'radar':

        # download radar mosaics
        if area == 'all':
            nmc = NMC(kind, 'region', resolution, savepath, verbose)
            all_urls = nmc.get_urls()
            nmc.download(all_urls)
            nmc.upload_bdy(bdy_path, delete)

            nmc = NMC(kind, 'station', resolution, savepath, verbose)
            all_urls = nmc.get_urls()
            nmc.download(all_urls)
            nmc.upload_bdy(bdy_path, delete)

        elif area in ['region', 'station']:
            nmc = NMC(kind, area, resolution, savepath, verbose)
            all_urls = nmc.get_urls()
            nmc.download(all_urls)
            nmc.upload_bdy(bdy_path, delete)

    elif kind == 'weatherchart':

        # download weathercharts
        nmc = NMC(kind, 'china', resolution, savepath, verbose)
        all_urls = nmc.get_urls()
        nmc.download(all_urls)
        nmc.upload_bdy(bdy_path, delete)

    elif kind == 'ltng':

        # download weathercharts
        nmc = NMC(kind, 'china', resolution, savepath, verbose)
        all_urls = nmc.get_urls()
        nmc.download(all_urls)
        nmc.upload_bdy(bdy_path, delete)


if __name__ == '__main__':
    main()
