import asyncio
import csv
import dataclasses
import glob
import json
import os
import random
import re
import time
from contextlib import contextmanager
from typing import List, Tuple

import aiohttp
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager

# globals
BROWSER_PROFILE = "Default2"
ACCOUNT_NAME = "account3"
LATEST_YEAR = 2023
OLDEST_YEAR = 2000
CRAWL_ORDER_HISTORY = True
DOWNLOAD_INVOICE_PAGES = True
SCRAPE_INVOICE_PAGES = True
INTERACTIVE_CONSOLE_MODE = False
PARSE_ONLY = True  # Change to True if you ONLY want to parse existing files.


# Variables
browser: webdriver.Chrome = None
browser_running = False
root_url = "https://www.amazon.com"
invoice_urls = []
details_urls = []
digital_invoice_urls = []


@dataclasses.dataclass
class Order:
    order_number: str = ""
    order_placed_date: str = ""
    order_total: str = ""
    order_subtotal: str = ""
    order_shipping_and_handling: str = ""
    order_total_pre_tax: str = ""
    order_tax: str = ""
    order_grand_total: str = ""
    payment_method: str = ""
    credit_card: str = ""
    credit_card_charge_date: str = ""
    shipping_person: str = ""
    shipping_address: str = ""
    billing_person: str = ""
    billing_address: str = ""


@dataclasses.dataclass
class Item:
    order_number: str = ""
    item_quantity: str = ""
    item_description: str = ""
    item_seller: str = ""
    item_condition: str = ""
    item_price: str = ""


# Helper function
def vprint(*args):
    if INTERACTIVE_CONSOLE_MODE:
        print(*args)


# Wait times
def wait_abit(base_secs=0.5):
    sleep_secs = base_secs + random.uniform(-0.5, 0.5) * base_secs
    time.sleep(sleep_secs)


# go down/up one dir level
def push_dir(dir_name):
    if not os.path.exists(dir_name):
        os.mkdir(dir_name)
    os.chdir(dir_name)


def pop_dir():
    os.chdir("..")


def json_file_exists(basename):
    return os.path.exists(basename + ".json")


def write_to_json_file(basename, str_list):
    filename = basename + ".json"
    vprint("INFO: writing " + filename + "...\n", str_list)
    with open(filename, "w") as json_file:
        json.dump(str_list, json_file, indent=4)


def read_from_json_file(basename):
    filename = basename + ".json"
    vprint("INFO: reading " + filename + "...")
    with open(filename, "r") as jsonFile:
        str_list = json.load(jsonFile)
    return str_list


def get_web_page(url: str, scroll_to_end: bool = False) -> Tuple[BeautifulSoup, bool]:
    global browser
    global browser_running

    vprint("INFO: getting page", url, "...")

    if not browser_running:
        browser_startup()

    success = True
    html = ""
    try:
        browser.get(url)
        if scroll_to_end:
            browser.execute_script("window.scrollTo(0,document.body.scrollHeight)")
            wait_abit()
    except Exception as error:
        vprint('    ERROR: browser.get("%s,%s")' % url, error)
        success = False
        browser_shutdown()

    if success:
        html = browser.page_source

    return BeautifulSoup(html, features="html.parser"), success


def browser_startup():
    global browser, browser_running

    options = webdriver.ChromeOptions()
    options.add_experimental_option("excludeSwitches", ["test-type", "enable-automation", "enable-blink-features"])

    browser = webdriver.Chrome(options=options, service=ChromeService(ChromeDriverManager().install()))
    vprint(browser.capabilities)
    browser_running = True


def browser_shutdown():
    global browser
    global browser_running
    if browser is not None:
        browser.quit()
        browser = None
        browser_running = False


def add_to_url_list(url_list: List[str], url: str) -> bool:
    if url not in url_list:
        url_list.append(url)
        print("adding", url)
        return True
    else:
        return False


@contextmanager
def ignore(*exceptions):
    try:
        yield
    except Exception as exc:
        vprint("ignoring exception", exc)
        pass


def scrape_invoice_pages():
    order_fields = [f.name for f in dataclasses.fields(Order)]
    item_fields = [f.name for f in dataclasses.fields(Item)]

    with open("orders.csv", "w", newline="", encoding="utf-8") as ordersFile:
        writer = csv.DictWriter(ordersFile, order_fields)
        writer.writeheader()

    with open("items.csv", "w", newline="", encoding="utf-8") as itemsFile:
        writer = csv.DictWriter(itemsFile, item_fields)
        writer.writeheader()

    invoices = sorted(glob.glob("orders/*.html"))
    vprint(invoices)
    for invoice in invoices:
        vprint("invoice", invoice)
        with open(invoice) as file:
            html = file.read()
        page = BeautifulSoup(html, "html.parser")

        # ORDERS

        order = Order()
        with ignore(AttributeError):
            order.order_number = page.body.find(string=re.compile("Amazon.com order number:")).next_element.strip()
            order.order_placed_date = page.body.find(string=re.compile("Order Placed:")).next_element.strip()
            order.order_total = page.body.find(string=re.compile("Order Total")).parent.contents[0].split("$")[1]
            order.order_subtotal = (
                page.body.find(string=re.compile("Item\(s\) Subtotal:"))
                .next_element.next_element.contents[0]
                .replace("$", "")
            )
            order.order_shipping_and_handling = (
                page.body.find(string=re.compile("Shipping \& Handling:"))
                .next_element.next_element.contents[0]
                .replace("$", "")
            )
            order.order_total_pre_tax = (
                page.body.find(string=re.compile("Total before tax:"))
                .next_element.next_element.contents[0]
                .replace("$", "")
            )
            order.order_tax = (
                page.body.find(string=re.compile("Estimated tax to be collected:"))
                .next_element.next_element.contents[0]
                .replace("$", "")
            )
            order.order_grand_total = (
                page.body.find(string=re.compile("Grand Total:"))
                .next_element.next_element.contents[0]
                .contents[0]
                .replace("$", "")
            )
        with ignore(AttributeError):
            order.payment_method = page.body.find(
                string=re.compile("Payment Method:")
            ).next_element.next_element.next_element.strip()
            order.credit_card = page.body.find(string=re.compile("ending in")).split(":")[0].strip()
            order.credit_card_charge_date = page.body.find(string=re.compile("ending in")).split(":")[1].strip()

        with ignore(AttributeError):
            address_divs = page.find_all("div", class_="displayAddressDiv")
            for div in address_divs:
                address_type = get_address_type(div)
                name, street, city_state_zip, country = div.text.strip().splitlines()
                if address_type == "shipping":
                    order.shipping_person = name
                    order.shipping_address = f"{street}, {city_state_zip}"
                elif address_type == "billing":
                    order.billing_person = name
                    order.billing_address = f"{street}, {city_state_zip}"

        order_dict = dataclasses.asdict(order)

        with open("orders.csv", "a", newline="", encoding="utf-8") as orders_file:
            writer = csv.DictWriter(orders_file, fieldnames=order_fields)
            writer.writerow(order_dict)

        print("appended order %s to %s" % (order.order_number, os.getcwd() + "/orders.csv"))
        vprint(order_dict)

        # ITEMS

        item_rows = []
        item_quantities_raw = page.body.find_all(string=re.compile("of:"))
        item_descriptions_raw = page.body.find_all(string=re.compile("of:"))
        item_sellers_raw = page.body.find_all(string=re.compile("Sold by:"))
        item_conditions_raw = page.body.find_all(string=re.compile("Condition:"))
        item_prices_raw = page.body.find_all(string=re.compile("Condition:"))

        # Calculate item count
        item_count = max(
            [
                len(item_quantities_raw),
                len(item_descriptions_raw),
                len(item_sellers_raw),
                len(item_conditions_raw),
                len(item_prices_raw),
            ]
        )

        for i in range(item_count):
            item = Item(order_number=order.order_number)

            with ignore(AttributeError):
                item.item_quantity = item_quantities_raw[i].split("of:")[0].strip()

            with ignore(AttributeError):
                item.item_description = item_descriptions_raw[i].next_element.contents[0]

            with ignore(AttributeError):
                item.item_seller = re.sub(" \($", "", item_sellers_raw[i].split("Sold by:")[1].strip())

            with ignore(AttributeError):
                item.item_condition = item_conditions_raw[i].split("Condition:")[1].strip()

            with ignore(AttributeError):
                item.item_price = item_prices_raw[i].find_next(string=re.compile("\$")).strip().replace("$", "")

            item_rows.append(item)  # Append the Item object to the list

        with open("items.csv", "a", newline="", encoding="utf-8") as items_file:
            writer = csv.DictWriter(items_file, fieldnames=item_fields)  # Generate fieldnames from Item dataclass
            writer.writerows(
                dataclasses.asdict(item) for item in item_rows
            )  # Use list comprehension to convert item dataclasses to dictionaries

        print(f"appended {len(item_rows)} items from order {order.order_number} to {os.getcwd() + '/items.csv'}")


def get_address_type(address_div):
    # Find the previous sibling or parent element that contains the label text
    # This is a generic approach; you might need to adjust it based on the actual HTML structure
    label = address_div.find_previous_sibling("b") or address_div.parent.find_previous_sibling("b")

    if label and "shipping" in label.text.lower():
        return "shipping"
    elif label and "billing" in label.text.lower():
        return "billing"
    else:
        return "unknown"


def login_to_amazon():
    page, success = get_web_page("https://www.amazon.com/gp/css/order-history?ref_=nav_orders_first")
    input("Login to Amazon then press Enter here (in the terminal) to continue...")


async def populate_invoice_urls():
    global invoice_urls, details_urls, digital_invoice_urls

    for year in range(LATEST_YEAR, OLDEST_YEAR - 1, -1):
        year_str = str(year)
        page_num = 1
        more_pages = True
        num_orders = 0

        while more_pages:
            more_pages = False
            pagination_str = str(page_num - 1) + "_" + str(page_num)
            url = (
                root_url
                + "/gp/your-account/order-history/ref=ppx_yo_dt_b_pagination_"
                + pagination_str
                + "?ie=UTF8&orderFilter=year-"
                + year_str
                + "&search=&startIndex="
                + str((page_num - 1) * 10)
            )

            page, success = get_web_page(url)
            if not page or not success:
                continue

            if page_num == 1:
                # <span class="num-orders">93 orders</span> placed in
                spans = page.findAll("span", {"class": "num-orders"})
                if len(spans):
                    num_orders = int(spans[0].get_text().split(" ")[0])
                vprint("%d numOrders for %d" % (num_orders, year))

            for tag in page.findAll("a"):
                url = tag.get("href")
                if url is not None:
                    if "/gp/css/summary/print.html" in url:
                        more_pages = add_to_url_list(invoice_urls, url)
                    elif "/gp/your-account/order-details" in url:
                        more_pages = add_to_url_list(details_urls, url)
                    elif "/gp/digital/your-account/order-summary" in url:
                        more_pages = add_to_url_list(digital_invoice_urls, url)

            if more_pages:
                page_num += 1
                wait_abit(1.5)

        print("got %d invoices at year %d" % (len(invoice_urls), year))
        print("got %d order details at year %d" % (len(details_urls), year))
        print("got %d digital invoices at year %d" % (len(digital_invoice_urls), year))

        write_to_json_file("invoices", invoice_urls)
        write_to_json_file("order-details", details_urls)
        write_to_json_file("digital-invoices", digital_invoice_urls)


async def download_order(session, url):
    order_id = url.split("orderID=")[1]
    local_url = f"orders/{order_id}.html"
    if not url.startswith(root_url):
        url = root_url + url

    vprint(f"downloading {url} to {local_url}")

    # note the request is now async
    async with session.get(url) as resp:
        page = await resp.text()

    if page is not None:
        with open(local_url, "w", encoding="utf-8") as file:
            file.write(str(page))
        vprint("wrote file", local_url)


async def download_pages():
    invoice_urls = read_from_json_file("invoices")
    # create a single aiohttp session for all the requests
    async with aiohttp.ClientSession() as session:
        await asyncio.gather(*[download_order(session, url) for url in invoice_urls])


async def main():
    push_dir(ACCOUNT_NAME)

    if not PARSE_ONLY:
        browser_startup()
        login_to_amazon()

        if CRAWL_ORDER_HISTORY:
            await populate_invoice_urls()

        if DOWNLOAD_INVOICE_PAGES:
            await download_pages()

        browser_shutdown()

    if SCRAPE_INVOICE_PAGES:
        scrape_invoice_pages()

    pop_dir()

    vprint("\ndone!")


if __name__ == "__main__":
    asyncio.run(main())
