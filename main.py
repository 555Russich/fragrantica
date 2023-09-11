import asyncio
import random
import time
import logging
from pathlib import Path

from aiohttp import ClientSession, CookieJar, DummyCookieJar
from bs4 import BeautifulSoup
from tenacity import (
    retry,
    wait_fixed,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
    after_log
)

from my_logging import get_logger

FILEPATH = Path('fragrantica.csv')
DOMAIN = 'https://www.fragrantica.com'
HEADERS = {
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Linux"',
}
COOKIES = {'rtyt45gh': 1}


class RequestBlocked(Exception):
    pass


class Scrapper:
    session: ClientSession = None

    async def get_countries(self) -> list[str]:
        html = await self._get(f'{DOMAIN}/country/')
        soup = BeautifulSoup(html, 'lxml')
        divs_countries = soup.find_all('div', class_='countrylist cell small-6 large-4')
        return [f'{DOMAIN}{div.find("a").get("href")}' for div in divs_countries]

    async def get_designers_from_country(self, url: str) -> list[tuple[str, str]]:
        html = await self._get(url)
        soup = BeautifulSoup(html, 'lxml')
        divs_designers = soup.find_all('div', class_='designerlist cell small-6 large-4')
        return [(f'{DOMAIN}{div.find("a").get("href")}', div.find('a').text.strip()) for div in divs_designers]

    async def get_fragrances(self, url: str) -> list[str]:
        html = await self._get(url)
        soup = BeautifulSoup(html, 'lxml')
        return [d.find('h3').text.strip() for d in
                soup.find_all('div', class_='cell text-left prefumeHbox px1-box-shadow')]

    @retry(
        wait=wait_fixed(60) + wait_exponential(3),
        retry=retry_if_exception_type((RequestBlocked, asyncio.TimeoutError)),
        before_sleep=before_sleep_log(logging.getLogger(), logging.DEBUG),
        after=after_log(logging.getLogger(), logging.DEBUG)
    )
    async def _get(self, url: str) -> str:
        cookies_old = self.session.cookie_jar._cookies.get(('', '/'))
        rtyt_old = cookies_old.get('rtyt45gh')
        # cookies_old = self.session.cookie_jar.filter_cookies(DOMAIN)

        async with self.session.get(url, headers=HEADERS) as response:
            rtyt_new = response.cookies.get('rtyt45gh')

            if rtyt_new and rtyt_new.value and rtyt_old.value != rtyt_new.value:
                logging.info(f'{rtyt_old.value=} | {rtyt_new.value=}')
                COOKIES['rtyt45gh'] = int(rtyt_new.value) - 1
                self.session.cookie_jar._cookies[('', '/')]['rtyt45gh'] = rtyt_new.value

            logging.info(f'{response.cookies=}')
            logging.info(f'{self.session.cookie_jar._cookies=}')
            # logging.info(f'{cookies_old=}')
            logging.info(f'{response=}')
            # if cookies_old != response.cookies:
            #     self.session.cookie_jar._cookies = response.cookies
            self.session.cookie_jar.update_cookies(response.cookies)

            if response.ok:
                return await response.text()

            await self._new_session()
            raise RequestBlocked

    async def _new_session(self):
        if self.session and self.session.closed is False:
            await self.session.close()

        self.session = await ClientSession(cookies=COOKIES).__aenter__()

    async def __aenter__(self):
        await self._new_session()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.session.close()


def save_to_csv(filepath: Path, designer: str, fragrances: list[str]) -> None:
    new_data = [';'.join([designer, f]) for f in fragrances]

    if filepath.exists() is False:
        data = [';'.join(['Designer', 'Fragrance'])] + new_data
    else:
        with open(filepath, 'r') as f:
            old_data = [l.strip() for l in f.readlines()]
        data = old_data + new_data

    with open(filepath, 'w') as f:
        f.write('\n'.join(data))


async def main():
    async with Scrapper() as scraper:
        urls_countries = await scraper.get_countries()
        for i_c, url_country in enumerate(urls_countries):
            designers = await scraper.get_designers_from_country(url_country)
            for i_d, designer_data in enumerate(set(designers)):
                url_designer, designer_name = designer_data
                fragrances = await scraper.get_fragrances(url_designer)
                save_to_csv(FILEPATH, designer=designer_name, fragrances=fragrances)
                logging.info(f'Country: {i_c}/{len(urls_countries)} {url_country.replace(DOMAIN, "")} | Designer: '
                             f'{i_d}/{len(designers)} {designer_name}| 'f'{len(fragrances)=}')
                # time.sleep(random.randint(30, 60))
                # break
            # break


if __name__ == '__main__':
    get_logger(Path('fragrantica.log'))
    asyncio.run(main())
