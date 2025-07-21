import json
import logging
import random
import asyncio
import subprocess
import sys
import schedule
import time
from datetime import datetime, timedelta
from playwright.async_api import async_playwright

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def install_playwright_browsers():
    logging.info("Проверка и установка браузеров Playwright")
    try:
        subprocess.run([sys.executable, "-m", "playwright", "install"], check=True)
        logging.info("Браузеры Playwright успешно установлены")
    except subprocess.CalledProcessError as e:
        logging.error(f"Ошибка при установке браузеров Playwright: {e}")
        sys.exit(1)

class GoogleMapsReviewsParser:
    def __init__(self, config_file):
        with open(config_file, "r", encoding="utf-8") as f:
            self.config = json.load(f)

    async def parse_reviews(self, business_name=None, address=None, url=None):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)  # Изменено на headless=True
            context = await browser.new_context()
            page = await context.new_page()

            try:
                if url:
                    await page.goto(url, wait_until="networkidle", timeout=60000)
                elif business_name and address:
                    await self.search_business(page, business_name, address)
                else:
                    raise ValueError("Необходимо предоставить либо URL, либо название и адрес организации")

                await self.wait_and_interact(page)

                card_data = await self.parse_card_data(page)
                reviews = await self.parse_reviews_data(page)

                return {
                    "card_data": card_data,
                    "reviews": reviews
                }
            except Exception as e:
                logging.error(f"Ошибка при парсинге данных: {e}")
                # Сохраняем HTML только при ошибке
                page_content = await page.content()
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                error_html_filename = f"error_page_content_{timestamp}.html"
                with open(error_html_filename, "w", encoding="utf-8") as f:
                    f.write(page_content)
                logging.info(f"HTML страницы с ошибкой сохранен в файл {error_html_filename}")
                return None
            finally:
                await browser.close()

    async def search_business(self, page, business_name, address):
        await page.goto("https://www.google.com/maps", wait_until="networkidle")
        search_box = await page.wait_for_selector("#searchboxinput")
        search_query = f"{business_name} {address}"
        await search_box.fill(search_query)
        await search_box.press("Enter")
        await page.wait_for_load_state("networkidle")
        await page.wait_for_timeout(5000)  # Дополнительное ожидание после загрузки результатов поиска

    async def wait_and_interact(self, page):
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight/2)")
        await asyncio.sleep(random.uniform(1, 2))
        await page.evaluate("window.scrollTo(0, 0)")
        await asyncio.sleep(random.uniform(1, 2))

    async def parse_card_data(self, page):
        card_data = {}
        try:
            name_element = await page.wait_for_selector("h1.DUwDvf")
            card_data['name'] = await name_element.inner_text()
            logging.info(f"Получено название организации: {card_data['name']}")
        except Exception as e:
            logging.error(f"Не удалось получить название организации: {e}")

        try:
            rating_element = await page.wait_for_selector("div.F7nice > span:nth-child(1)")
            rating_text = await rating_element.inner_text()
            card_data['rating'] = float(rating_text.replace(',', '.'))
            logging.info(f"Получен рейтинг: {card_data['rating']}")
        except Exception as e:
            logging.error(f"Не удалось получить рейтинг: {e}")

        try:
            reviews_count_element = await page.wait_for_selector("div.F7nice > span:nth-child(2)")
            reviews_count_text = await reviews_count_element.inner_text()
            card_data['reviews_count'] = int(''.join(filter(str.isdigit, reviews_count_text)))
            logging.info(f"Получено количество отзывов: {card_data['reviews_count']}")
        except Exception as e:
            logging.error(f"Не удалось получить количество отзывов: {e}")

        try:
            ratings_count_element = await page.wait_for_selector("div.F7nice")
            ratings_count_text = await ratings_count_element.inner_text()
            ratings_count = int(''.join(filter(str.isdigit, ratings_count_text.split('(')[1].split(')')[0])))
            card_data['ratings_count'] = ratings_count
            logging.info(f"Получено количество оценок: {card_data['ratings_count']}")
        except Exception as e:
            logging.error(f"Не удалось получить количество оценок: {e}")

        return card_data

    async def find_element_by_text(self, page, text, element_types=['div', 'span', 'button', 'a']):
        for element_type in element_types:
            elements = await page.query_selector_all(element_type)
            for element in elements:
                element_text = await element.inner_text()
                if text.lower() in element_text.lower():
                    return element
        return None

    async def parse_reviews_data(self, page):
            reviews = []
            logging.info("Начинаем адаптивный поиск и анализ отзывов")

            try:
                await page.wait_for_load_state("networkidle", timeout=60000)

                reviews_button = await page.wait_for_selector('button:has-text("Reviews")', timeout=30000)
                if reviews_button:
                    await reviews_button.click()
                    logging.info("Кнопка 'Отзывы' нажата")
                    await page.wait_for_timeout(5000)
                else:
                    logging.warning("Кнопка 'Отзывы' не найдена")

                reviews_container = await page.wait_for_selector('.m6QErb.DxyBCb.kA9KIf.dS8AEf', timeout=30000)
                if not reviews_container:
                    logging.error("Контейнер с отзывами не найден")
                    return reviews

                scroll_attempts = 0
                max_scroll_attempts = 80  # Ограничим количество попыток прокрутки

                while len(reviews) < 200 and scroll_attempts < max_scroll_attempts: # --------------------------------------------------------- Измените 100 на любое другое число
                    await page.evaluate('document.querySelector(".m6QErb.DxyBCb.kA9KIf.dS8AEf").scrollTop = document.querySelector(".m6QErb.DxyBCb.kA9KIf.dS8AEf").scrollHeight')
                    await page.wait_for_timeout(2000)

                    review_elements = await reviews_container.query_selector_all('.jftiEf.fontBodyMedium')
                    
                    new_reviews_found = False
                    for element in review_elements[len(reviews):]:
                        try:
                            date_element = await element.query_selector('.rsqaWe')
                            date_text = await date_element.inner_text() if date_element else 'Нет даты'
                            
                            review_date = self.parse_date(date_text)
                            
                            if review_date:
                                author_element = await element.query_selector('.d4r55')
                                author = await author_element.inner_text() if author_element else 'Нет имени'

                                rating_element = await element.query_selector('.kvMYJc')
                                if rating_element:
                                    aria_label = await rating_element.get_attribute('aria-label')
                                    rating = int(aria_label.split()[0]) if aria_label else 0
                                else:
                                    rating = 0

                                text_element = await element.query_selector('.MyEned .wiI7pd')
                                text = await text_element.inner_text() if text_element else 'Нет текста'

                                response_element = await element.query_selector('.CDe7pd .wiI7pd')
                                response_text = await response_element.inner_text() if response_element else ''

                                review = {
                                    'author': author,
                                    'date': date_text,
                                    'rating': rating,
                                    'text': text,
                                    'response_text': response_text,
                                    'parsed_date': review_date
                                }

                                reviews.append(review)
                                new_reviews_found = True
                                logging.info(f"Добавлен новый отзыв. Всего отзывов: {len(reviews)}")

                        except Exception as e:
                            logging.error(f"Ошибка при извлечении данных отзыва: {str(e)}")

                    if not new_reviews_found:
                        scroll_attempts += 1
                        logging.info(f"Попытка прокрутки {scroll_attempts} из {max_scroll_attempts}")
                    else:
                        scroll_attempts = 0

                # Сортируем отзывы по дате (от новых к старым) и берем первые 100
                reviews.sort(key=lambda x: x['parsed_date'], reverse=True)
                reviews = reviews[:200]

                # Удаляем временное поле parsed_date
                for review in reviews:
                    del review['parsed_date']

                if not reviews:
                    logging.warning("Не удалось найти ни одного отзыва")
                else:
                    logging.info(f"Собрано {len(reviews)} последних отзывов.")

            except Exception as e:
                logging.error(f"Ошибка при парсинге отзывов: {str(e)}")
                await page.screenshot(path='error_screenshot.png')
                logging.info("Скриншот ошибки сохранен как error_screenshot.png")

            logging.info(f"Всего собрано отзывов: {len(reviews)}")
            return reviews
    
    def parse_date(self, date_string):
        current_date = datetime.now()
        if 'minutes' in date_string or 'hour' in date_string:
            return current_date
        elif 'day' in date_string or 'day' in date_string or 'days' in date_string:
            days = int(date_string.split()[0])
            return current_date - timedelta(days=days)
        elif 'week' in date_string:
            weeks = int(date_string.split()[0])
            return current_date - timedelta(weeks=weeks)
        elif 'month' in date_string or 'months' in date_string or 'months' in date_string:
            months = int(date_string.split()[0])
            return current_date - timedelta(days=months*30)  # приблизительно
        elif 'year' in date_string or 'years' in date_string or 'year' in date_string:
            years = int(date_string.split()[0])
            return current_date - timedelta(days=years*365)  # приблизительно
        else:
            try:
                return datetime.strptime(date_string, "%d.%m.%Y")
            except:
                logging.error(f"Не удалось распознать дату: {date_string}")
                return None

async def run_parser():
    install_playwright_browsers()
    logging.info("Запуск парсера отзывов")
    parser = GoogleMapsReviewsParser("config.json")

    results = []
    for business in parser.config["businesses"]:
        try:
            logging.info(f"Начало обработки бизнеса: {business['name']}")
            result = await parser.parse_reviews(business_name=business['name'], address=business['address'], url=business['url'])
            if result:
                results.append(result)
                logging.info(f"Успешно обработан бизнес: {business['name']}")
            else:
                logging.warning(f"Не удалось получить данные для бизнеса: {business['name']}")
        except Exception as e:
            logging.error(f"Ошибка при обработке бизнеса {business['name']}: {e}", exc_info=True)

    if results:
        parsed_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        output_file = f"reviews_results_{parsed_time}.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        logging.info(f"Результаты сохранены в файл {output_file}")
    else:
        logging.info("Нет данных для сохранения")

    logging.info("Парсер отзывов завершил работу")

def job():
    asyncio.run(run_parser())

if __name__ == "__main__":
    logging.info("Начало работы скрипта")
    asyncio.run(run_parser())  # Первый запуск
    
    schedule.every(12).hours.do(job)
    
    while True:
        schedule.run_pending()
        time.sleep(1)