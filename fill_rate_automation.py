import pandas as pd
import re
import json
import random
import asyncio
import asyncpg
import os
import platform

from datetime import datetime, timedelta
from playwright.async_api import async_playwright


# =====================
# CONFIG
# =====================

INPUT_CSV = "routes.csv"
OUTPUT_JSON = "route_fill_rates.json"

# Use environment variable for GitHub Actions or fallback locally
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://neondb_owner:npg_BHgcKm7MnJ0N@ep-nameless-pond-ahiguu23-pooler.c-3.us-east-1.aws.neon.tech/neondb?sslmode=require"
)

# Chrome path (used locally only)
CHROME_PATH = r"C:\Program Files\Google\Chrome\Application\chrome.exe"

BUS_CAPACITY = 36
DATE_RANGE = 3
MAX_TABS = 3
SCROLL_COUNT = 6
MAX_RETRIES = 3


USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/119 Safari/537.36",
]


# =====================
# HELPERS
# =====================

def extract_available_seats(text):

    if not text:
        return 0

    match = re.search(r"\d+", text)

    return int(match.group()) if match else 0


def generate_dates(days):

    base = datetime.now()

    return [
        (base + timedelta(days=i)).strftime("%d-%b-%Y")
        for i in range(days)
    ]


def update_url_date(url, date):

    url = re.sub(
        r"onward=\d{2}-[A-Za-z]{3}-\d{4}",
        f"onward={date}",
        url
    )

    url = re.sub(
        r"doj=\d{2}-[A-Za-z]{3}-\d{4}",
        f"doj={date}",
        url
    )

    return url


async def apply_stealth(page):

    await page.add_init_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )


async def human_scroll(page):

    for _ in range(SCROLL_COUNT):

        await page.mouse.wheel(
            0,
            random.randint(2000, 5000)
        )

        await asyncio.sleep(
            random.uniform(1, 2)
        )


# =====================
# DATABASE FUNCTION
# =====================

async def save_to_neon(results):

    conn = await asyncpg.connect(DATABASE_URL)

    try:

        async with conn.transaction():

            print("Deleting existing data from Neon...")

            await conn.execute(
                "DELETE FROM redbus_fill_rates"
            )

            print("Old data deleted.")

            insert_query = """
            INSERT INTO redbus_fill_rates (
                route_name,
                travel_date,
                route_url,
                bus_count,
                total_capacity,
                available_seats,
                filled_seats,
                fill_rate_percent,
                scraped_at
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            """

            for r in results:

                await conn.execute(
                    insert_query,
                    r["route_name"],
                    datetime.strptime(
                        r["travel_date"],
                        "%d-%b-%Y"
                    ),
                    r["route_url"],
                    r["bus_count"],
                    r["total_capacity"],
                    r["available_seats"],
                    r["filled_seats"],
                    r["fill_rate_percent"],
                    datetime.fromisoformat(
                        r["scraped_at"]
                    )
                )

        print("New data inserted successfully.")

    except Exception as e:

        print("Database error:", e)

    finally:

        await conn.close()


# =====================
# SCRAPER FUNCTION
# =====================

async def scrape(context, route_name, base_url, date):

    url = update_url_date(
        base_url,
        date
    )

    for attempt in range(MAX_RETRIES):

        page = await context.new_page()

        await apply_stealth(page)

        try:

            print(f"Scraping: {route_name} | {date}")

            await page.goto(
                url,
                timeout=60000
            )

            await asyncio.sleep(
                random.uniform(4, 7)
            )

            await human_scroll(page)

            seats = await page.locator(
                "text=/Seat/i"
            ).all()

            total_available = 0
            bus_count = 0

            for seat in seats:

                text = await seat.inner_text()

                available = extract_available_seats(text)

                total_available += available

                bus_count += 1

            if bus_count == 0:

                raise Exception("Blocked")

            total_capacity = bus_count * BUS_CAPACITY

            filled = total_capacity - total_available

            fill_rate = round(
                (filled / total_capacity) * 100,
                2
            )

            result = {

                "route_name": route_name,
                "travel_date": date,
                "route_url": url,
                "bus_count": bus_count,
                "total_capacity": total_capacity,
                "available_seats": total_available,
                "filled_seats": filled,
                "fill_rate_percent": fill_rate,
                "scraped_at": datetime.now().isoformat()

            }

            print(
                f"Success {route_name} {date} Fill {fill_rate}%"
            )

            await page.close()

            return result

        except Exception as e:

            print("Retry:", e)

            await page.close()

            await asyncio.sleep(3)

    return None


# =====================
# MAIN FUNCTION
# =====================

async def main():

    routes = pd.read_csv(INPUT_CSV)

    dates = generate_dates(DATE_RANGE)

    results = []

    async with async_playwright() as p:

        # Detect environment (GitHub Actions vs Local)
        if platform.system() == "Windows":

            browser = await p.chromium.launch(

                executable_path=CHROME_PATH,

                headless=False,

                args=[
                    "--disable-blink-features=AutomationControlled"
                ]
            )

        else:

            # GitHub Actions / Linux
            browser = await p.chromium.launch(

                channel="chrome",

                headless=True,

                args=[
                    "--disable-blink-features=AutomationControlled"
                ]
            )

        context = await browser.new_context(

            viewport={
                "width": 1366,
                "height": 768
            },

            user_agent=random.choice(
                USER_AGENTS
            )
        )

        semaphore = asyncio.Semaphore(MAX_TABS)

        async def sem_task(route, url, date):

            async with semaphore:

                return await scrape(
                    context,
                    route,
                    url,
                    date
                )

        tasks = []

        for _, row in routes.iterrows():

            for date in dates:

                tasks.append(

                    sem_task(
                        row["Route_name"],
                        row["Route_link"],
                        date
                    )
                )

        responses = await asyncio.gather(*tasks)

        for r in responses:

            if r:
                results.append(r)

        await browser.close()

    # Save JSON backup
    with open(
        OUTPUT_JSON,
        "w",
        encoding="utf-8"
    ) as f:

        json.dump(
            results,
            f,
            indent=4
        )

    print("JSON backup saved.")

    # Save to Neon
    await save_to_neon(results)


# =====================
# RUN SCRIPT
# =====================

if __name__ == "__main__":

    asyncio.run(main())
