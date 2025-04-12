import os
import re
import time
import random
import pandas as pd
from datetime import datetime, timedelta
from multiprocessing import Pool, Manager
from playwright.sync_api import sync_playwright, TimeoutError, Error as PlaywrightError

### Helper functions ###

def get_dates(start_date: datetime.date = None, end_date: datetime.date = None, los: int = 5):
    # Default: start today and end 30 days from today.
    if start_date is None:
        start_date = datetime.today().date()
    if end_date is None:
        end_date = start_date + timedelta(days=30)
    date_tuples = []
    current_date = start_date
    while current_date <= end_date:
        for j in range(1, los+1):
            check_out = current_date + timedelta(days=j)
            date_tuples.append((current_date, check_out))
        current_date += timedelta(days=1)
    return date_tuples


def fallback_click(page, x=10, y=10):
    """Fallback: perform a click at default coordinates if expected click fails."""
    try:
        page.mouse.click(x, y)
        print(f"Fallback click executed at ({x}, {y}).")
    except Exception as e:
        print("Fallback click failed:", e)

def human_like_scroll(page, total_scroll=1000, step=100, max_delay=500):
    """Scrolls down incrementally to mimic human behavior."""
    scrolled = 0
    while scrolled < total_scroll:
        page.evaluate(f"window.scrollBy(0, {step});")
        scrolled += step
        time.sleep(random.uniform(0.1, max_delay/1000))
    print("Human-like scrolled a total of", scrolled, "pixels.")

def full_scroll_bottom(page, attempts=3, pause=2):
    """Scrolls completely to the bottom of the page repeatedly."""
    for _ in range(attempts):
        # Check that document.body exists before scrolling
        page.evaluate("() => { if (document.body) { window.scrollTo(0, document.body.scrollHeight); } }")
        print("Full scroll to bottom executed.")
        time.sleep(pause)

def ensure_date_visible(page, date_str, max_attempts=12):
    """Clicks 'Next month' repeatedly until the checkbox for date_str is visible."""
    attempts = 0
    while not page.get_by_role("checkbox", name=date_str, exact=True).is_visible() and attempts < max_attempts:
        print(f"Date '{date_str}' not visible. Clicking 'Next month' (attempt {attempts+1}/{max_attempts}).")
        try:
            next_month_button = page.get_by_role("button", name="Next month")
            next_month_button.click()
            time.sleep(1)
        except Exception as e:
            print("Error clicking Next month button:", e)
            fallback_click(page)
        attempts += 1
    if attempts == max_attempts:
        print(f"Warning: Date '{date_str}' not visible after {max_attempts} attempts.")

def dismiss_genius_popup_if_present(page, timeout=3):
    """
    Attempts to dismiss the Genius sign‑in popup if it appears.
    Adjust the locators based on what you see in the browser.
    """
    try:
        popup_dialog = page.get_by_role("dialog").filter(has_text="Sign in, save money")
        popup_dialog.wait_for(state="visible", timeout=timeout*1000)
        close_button = popup_dialog.get_by_role("button", name="Close")
        close_button.click()
        print("Dismissed the Genius popup.")
    except TimeoutError:
        print("No Genius popup to dismiss (Timeout).")
    except PlaywrightError as e:
        if "Target closed" in str(e):
            print("Target closed while dismissing popup; skipping fallback.")
        else:
            print("Could not dismiss Genius popup:", e)
            fallback_click(page)
    except Exception as e:
        print("Could not dismiss Genius popup:", e)
        fallback_click(page)

def load_hotel_cards(page, min_count=100, max_clicks=5):
    """
    Scrolls and clicks 'Load more results' until at least min_count hotel cards are visible.
    """
    clicks = 0
    while clicks < max_clicks:
        full_scroll_bottom(page, attempts=3, pause=2)
        human_like_scroll(page, total_scroll=1000, step=100, max_delay=500)
        try:
            load_more_button = page.get_by_role("button", name="Load more results")
            load_more_button.wait_for(state="visible", timeout=5000)
            print("Load more results button is visible.")
        except TimeoutError:
            print("Load more results button not visible. Trying to dismiss popup...")
            dismiss_genius_popup_if_present(page, timeout=3)
            time.sleep(2)
            full_scroll_bottom(page, attempts=3, pause=2)
            try:
                load_more_button.wait_for(state="visible", timeout=5000)
                print("Load more results button is now visible.")
            except TimeoutError:
                print("Still no 'Load more results' button; continuing.")
                clicks += 1
                continue

        hotel_cards = page.locator("div[data-testid='property-card']")
        current_count = hotel_cards.count()
        print("Current hotel card count:", current_count)
        if current_count >= min_count:
            print("Desired number of hotel cards loaded.")
            break
        try:
            load_more_button.click()
            clicks += 1
            print("Clicked 'Load more results' button", clicks, "time(s).")
        except Exception as e:
            print("Error clicking 'Load more results':", e)
            fallback_click(page)
            clicks += 1
            continue
        try:
            page.wait_for_function(
                f"document.querySelectorAll('div[data-testid=\"property-card\"]').length > {current_count}",
                timeout=10000
            )
            print("New hotel cards loaded.")
        except TimeoutError:
            print("No new hotel cards loaded within 10 seconds.")
            clicks += 1
    full_scroll_bottom(page, attempts=2, pause=2)
    time.sleep(2)
    final_count = page.locator("div[data-testid='property-card']").count()
    print("Final hotel card count:", final_count)
    return page.locator("div[data-testid='property-card']")

def extract_hotel_data(page):
    hotel_cards = page.locator("div[data-testid='property-card']")
    count = hotel_cards.count()
    print("Extracting data from", count, "hotel cards (processing up to 100).")
    hotels_data = []
    for i in range(min(count, 100)):
        print(f"\n--- Processing hotel card {i+1}/{min(count, 100)} ---")
        card = hotel_cards.nth(i)
        try:
            hotel_name = card.locator("[data-testid='title']").inner_text(timeout=2000).strip()
            print("Hotel name:", hotel_name)
        except Exception as e:
            print("Error extracting hotel name:", e)
            hotel_name = "N/A"
        try:
            star_text = card.locator("div.b3f3c831be").get_attribute("aria-label")
            star_rating = int(star_text.split()[0]) if star_text else None
            print("Star rating:", star_rating, "(raw:", star_text, ")")
        except Exception as e:
            print("Error extracting star rating:", e)
            star_rating = None
        try:
            review_score_text = card.locator("[data-testid='review-score']").inner_text(timeout=2000)
            matches = re.findall(r"\d+\.\d+", review_score_text)
            rating_score = float(matches[0]) if matches else None
            print("Rating score:", rating_score, "(raw:", review_score_text, ")")
        except Exception as e:
            print("Error extracting rating score:", e)
            rating_score = None
        try:
            loc_score_text = card.locator("a[data-testid='secondary-review-score-link']").get_attribute("aria-label")
            match = re.search(r"Scored\s+(\d+\.\d+)", loc_score_text)
            location_score = float(match.group(1)) if match else None
            print("Location score:", location_score, "(raw:", loc_score_text, ")")
        except Exception as e:
            print("Error extracting location score:", e)
            location_score = None
        try:
            review_amount_text = card.locator("div.abf093bdfe.f45d8e4c32.d935416c47").inner_text(timeout=2000)
            review_amount = int(re.sub(r"[^\d]", "", review_amount_text))
            print("Review amount:", review_amount, "(raw:", review_amount_text, ")")
        except Exception as e:
            print("Error extracting review amount:", e)
            review_amount = None
        try:
            bed_info = card.locator("div[data-testid='availability-single'] ul.ba51609c35 li:nth-child(1) div.abf093bdfe").inner_text(timeout=2000).strip()
            print("Bed info:", bed_info)
        except Exception as e:
            print("Error extracting bed info:", e)
            bed_info = "N/A"
        try:
            price_text = card.locator("[data-testid='price-and-discounted-price']").inner_text(timeout=2000).strip()
            print("Price:", price_text)
        except Exception as e:
            print("Error extracting price:", e)
            price_text = "N/A"
        try:
            breakfast_included = card.locator("text=Breakfast included").is_visible()
            print("Breakfast included:", breakfast_included)
        except Exception as e:
            print("Error checking breakfast:", e)
            breakfast_included = False
        try:
            free_cancellation = card.locator("strong:has-text('Free cancellation')").is_visible()
            print("Free cancellation:", free_cancellation)
        except Exception as e:
            print("Error checking free cancellation:", e)
            free_cancellation = False
        try:
            no_prepayment_needed = card.locator("strong:has-text('No prepayment needed')").is_visible()
            print("No prepayment needed:", no_prepayment_needed)
        except Exception as e:
            print("Error checking no prepayment needed:", e)
            no_prepayment_needed = False
        try:
            card_text = card.inner_text().lower()
            centrally_located = ("centrally" in card_text) or ("subway access" in card_text)
            print("Centrally located:", centrally_located)
        except Exception as e:
            print("Error checking central location:", e)
            centrally_located = False
        try:
            sustainability_cert = card.locator("text=Sustainability certification").is_visible()
            print("Sustainability certification:", sustainability_cert)
        except Exception as e:
            print("Error checking sustainability:", e)
            sustainability_cert = False
        try:
            distance_from_downtown = card.locator("span[data-testid='distance']").inner_text(timeout=2000).strip()
            print("Distance from downtown:", distance_from_downtown)
        except Exception as e:
            print("Error extracting distance from downtown:", e)
            distance_from_downtown = ""
        hotel_dict = {
            "hotel_name": hotel_name,
            "star_rating": star_rating,
            "rating_score": rating_score,
            "location_score": location_score,
            "review_amount": review_amount,
            "bed_info": bed_info,
            "price": price_text,
            "breakfast_included": breakfast_included,
            "free_cancellation": free_cancellation,
            "no_prepayment_needed": no_prepayment_needed,
            "centrally_located": centrally_located,
            "sustainability_certification": sustainability_cert,
            "distance_from_downtown": distance_from_downtown,
        }
        hotels_data.append(hotel_dict)
    print("\nExtraction complete. Total hotels extracted:", len(hotels_data))
    return hotels_data

### Scraping functions with retry ###

def scrape_date_combination_sync(checkin, checkout):
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=False, slow_mo=0)
        context = browser.new_context()
        page = context.new_page()
        BASE_URL = "https://www.booking.com"
        page.goto(BASE_URL)
        page.get_by_role("combobox", name="Where are you going?").click()
        page.get_by_role("combobox", name="Where are you going?").fill("new york")
        page.get_by_role("button", name="New York New York, United").click()
        checkin_str = checkin.strftime("%-d %B %Y")
        checkout_str = checkout.strftime("%-d %B %Y")
        ensure_date_visible(page, checkin_str)
        page.get_by_role("checkbox", name=checkin_str, exact=True).click()
        ensure_date_visible(page, checkout_str)
        page.get_by_role("checkbox", name=checkout_str, exact=True).click()
        page.get_by_role("button", name="Search").click()
        time.sleep(3)
        load_hotel_cards(page, min_count=100, max_clicks=5)
        hotels = extract_hotel_data(page)
        for hotel in hotels:
            hotel["checkin"] = checkin_str
            hotel["checkout"] = checkout_str
        context.close()
        browser.close()
        return hotels

def scrape_date_combination_sync_retry(checkin, checkout):
    # Retry indefinitely until at least 100 hotels are scraped.
    while True:
        print(f"Scraping for date {checkin} - {checkout}...")
        hotels = scrape_date_combination_sync(checkin, checkout)
        if len(hotels) >= 100:
            print(f"Successfully scraped {len(hotels)} hotels for date {checkin} - {checkout}.")
            return hotels
        else:
            print(f"Only scraped {len(hotels)} hotels for date {checkin} - {checkout}. Retrying in a new browser instance...")
            time.sleep(2)

### CSV Writing with Lock ###

def write_to_csv(data, csv_filename, lock):
    df = pd.DataFrame(data)
    with lock:
        if not os.path.exists(csv_filename):
            df.to_csv(csv_filename, index=False, mode='w', header=True)
        else:
            df.to_csv(csv_filename, index=False, mode='a', header=False)
        print(f"Appended {len(data)} records to {csv_filename}.")

### Worker and Multiprocessing ###

def worker(args):
    checkin, checkout, csv_filename, lock = args
    hotels = scrape_date_combination_sync_retry(checkin, checkout)
    write_to_csv(hotels, csv_filename, lock)
    # Return the count for logging purposes
    return len(hotels)

def scrape_all_dates_sync(ttt: int, los: int, start_date: datetime.date = None, end_date: datetime.date = None):
    dates = get_dates(start_date, end_date, los)
    manager = Manager()
    lock = manager.Lock()
    run_date = datetime.now().strftime("%Y%m%d")
    csv_filename = f"booking_com_{run_date}.csv"
    # Remove any existing file so we start fresh.
    if os.path.exists(csv_filename):
        os.remove(csv_filename)
    # Prepare arguments for each worker.
    args_list = [(checkin, checkout, csv_filename, lock) for (checkin, checkout) in dates]
    all_counts = []
    with Pool(processes=24) as pool:
        all_counts = pool.map(worker, args_list)
    total = sum(all_counts)
    print("Total records scraped:", total)
    return csv_filename

def main():
    TTT = 30  # Total number of starting dates
    LOS = 5   # Length of stay
    csv_filename = scrape_all_dates_sync(TTT, LOS)
    df = pd.read_csv(csv_filename)
    print(f"Data saved to {csv_filename}. Total records: {len(df)}")

if __name__ == '__main__':
    main()
