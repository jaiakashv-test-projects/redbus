import asyncio
import pandas as pd
import datetime
import re
import psycopg2
import os
from psycopg2.extras import execute_batch
from playwright.async_api import async_playwright

# Database Configuration
DB_CONN_STRING = os.getenv("DATABASE_URL", "postgresql://neondb_owner:npg_BHgcKm7MnJ0N@ep-nameless-pond-ahiguu23-pooler.c-3.us-east-1.aws.neon.tech/neondb?sslmode=require")
TABLE_NAME = "redbus_fill_rates"

def upload_to_neon(data_list):
    """Deletes old data and uploads new scraped data to Neon Postgres."""
    if not data_list:
        print("No data to upload.")
        return

    try:
        conn = psycopg2.connect(DB_CONN_STRING)
        cur = conn.cursor()

        # 1. Delete old data
        print(f"Clearing old data from {TABLE_NAME}...")
        cur.execute(f"TRUNCATE TABLE {TABLE_NAME};")

        # 2. Prepare Insert Query
        insert_query = f"""
            INSERT INTO {TABLE_NAME} (
                route_name, travel_date, route_url, bus_count, 
                total_capacity, available_seats, filled_seats, 
                fill_rate_percent, scraped_at, average_price
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # 3. Format data for insertion
        vals = [
            (
                d['routename'], d['travel_date'], d['route_url'], d['bus_count'],
                d['total_capacity'], d['available_seats'], d['filled_seats'],
                d['fill_rate_percentage'], d['scraped_at'], d['average_price']
            ) for d in data_list
        ]

        # 4. Execute batch upload
        print(f"Uploading {len(vals)} rows to Neon...")
        execute_batch(cur, insert_query, vals)
        
        conn.commit()
        print("Database upload successful!")

    except Exception as e:
        print(f"Database Error: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()

async def smooth_infinite_scroll(page, label):
    await page.wait_for_selector("ul[data-autoid='exact']", timeout=30000)
    for i in range(1, 81):
        await page.evaluate("window.scrollBy(0, 600)")
        await asyncio.sleep(1)
        if i % 20 == 0:
            print(f"[{label}] Scrolling step {i}...")

async def scrape_url(context, route_name, base_url, target_date, label):
    page = await context.new_page()
    date_str = target_date.strftime("%d-%b-%Y")
    url = f"{base_url}&onward={date_str}&doj={date_str}"
    
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await smooth_infinite_scroll(page, f"{route_name}-{label}")

        bus_cards = await page.locator("li.tupleWrapper___d5a78a").all()
        bus_count = len(bus_cards)
        if bus_count == 0: return None

        total_price, total_avail = 0, 0
        for card in bus_cards:
            try:
                p_text = await card.locator(".finalFare___0b90fc").inner_text()
                total_price += float(re.sub(r'[^\d.]', '', p_text))
                s_text = await card.locator(".totalSeats___4cda5d").inner_text()
                total_avail += int(re.search(r'\d+', s_text).group())
            except: continue

        capacity = bus_count * 40
        filled = max(0, capacity - total_avail)

        return {
            "routename": route_name,
            "travel_date": target_date.strftime("%Y-%m-%d"),
            "route_url": url,
            "bus_count": bus_count,
            "total_capacity": capacity,
            "available_seats": total_avail,
            "filled_seats": filled,
            "fill_rate_percentage": round((filled/capacity)*100, 2) if capacity > 0 else 0,
            "scraped_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "average_price": round(total_price / bus_count, 2)
        }
    except Exception as e:
        print(f"Error: {e}")
        return None
    finally:
        await page.close()

async def main():
    df_routes = pd.read_csv("routes.csv")
    all_results = []
    
    today = datetime.date.today()
    tomorrow = today + datetime.timedelta(days=1)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()

        for _, row in df_routes.iterrows():
            tasks = [
                scrape_url(context, row['Route_name'], row['Route_link'], today, "TODAY"),
                scrape_url(context, row['Route_name'], row['Route_link'], tomorrow, "TOMORROW")
            ]
            route_data = await asyncio.gather(*tasks)
            all_results.extend([r for r in route_data if r is not None])

        await browser.close()

    # Upload results to Neon DB
    if all_results:
        upload_to_neon(all_results)
        # Also save local backup
        pd.DataFrame(all_results).to_csv("last_scrape_backup.csv", index=False)

if __name__ == "__main__":
    asyncio.run(main())