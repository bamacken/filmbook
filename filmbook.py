import csv
import time
import logging
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from facebook_scraper import get_posts

# Setup logging
logging.basicConfig(level=logging.INFO)

# Define the list of group IDs or group names
GROUP_IDS = [
    '7326700164011262', # Sacramento Film Community
    '692852240768589',  # Post-Production/Editing Jobs
    '985604369146833',  # Low Budget Film Production
    '48912613748',      # Production Freelancer Gigs
    '239215361232454',  # Gaffer Salon
    '1793881927514491', # Film/TV Production Crew & Staffing
    '1655984921283409', # Crew Call
    '1444645689098503', # Crew Up
    '176667512350214',  # Bay Area Actors and Filmmakers
    '1500128923564468', # Casting Calls Bay Area
    '1529869143917780', # Sacramento Film/Video Editors
    '552669781511811',  # Freelance Film/Video Producers (Bay Area & SF Only)
    '1640820006416803', # Freelance Film Crew - Los Angeles
    '377349122367467',  # I need a DP!
    '122004781205401',  # Filmmaking: a low budget and independent collective (FLIC)
    '750716559091040',  # FilmJobs LA
    '593470327424017',  # Last Minute and Short Notice Film/TV Production Needs & Jobs-Casting Calls
    '766106866905791',  # PAID Film crew jobs Los Angeles
    '43111129255',      # MEDIA CREW NOW
    '358161860987531',  # Paid Film, Television & Commercial Jobs
    '1449029882058223', # Paid Film/TV Production Jobs: Los Angeles Area
    '933105603496025',  # I NEED A CAMERA OP - USA and International Group
    '119297628627073',  # Indie Filmmakers, Actors & Friends
    '1511102425837567', # Secret Videography and Photography Friends
    '1335034497330561', # Crew Call 2.0
    '300991356671651',  # DP or Director Wanted!
    '54602763478',      # Sacramento Film Makers
    '121116347562',     # Nor-Cal Film Makers
    '218827964824647',  # Bay Area Filmmakers & Actors
    '1481620895465287', # Bay Area TV, Film, and Commercial Network
    '180978851931837',  # Bay Area Film Community
    '245332639229934',  # Indie Feature Filmmakers & Non-Union Actors
    '200083253382681',  # SF BAY AREA & SACRAMENTO FILMMAKING, ACTING, WRITING etc.
    '1639858882960142', # Sacramento Film Community (SFC)
    '298042394276615'   # Sacramento Film Project
]

# Define the number of pages to scrape per group
PAGES_TO_SCRAPE = 5

# Path to cookies file (optional, needed for private groups)
COOKIES_FILE = 'cookies.txt'  # Use 'None' if not using cookies

# Define keywords to filter posts
HIRING = [
    'needed', 
    'need of', 
    'need a', 
    'iso', 
    'paid', 
    'rate', 
    'day rate', 
    'looking for', 
    'searching for', 
    'hiring', 
    'crew call', 
    'in search of',
    'seeking'
]

POSITIONS = [
    'dp', 
    'cinematographer', 
    'director of photography', 
    'gaffer', 
    'grip', 
    'pa ', 
    'production assistant', 
    'sound', 
    'camera operator', 
    'camera',
    'videographer',
    'sound operator',
    'sound mixer',
    'sound recordist',
    'boom operator',
    'boom op',
    'editor',
    'part 107',
    'drone operator',
    'drone op',
    'colorist',
    'dit',
    'digital image technician',
    'camera op',
    'camop',
    'cam op',
    'media manager',
    'script supervisor',
    'location manager',
    'unit production manager',
    'assistant director',
    '1st ad',
    '2nd ad',
    'line producer',
    'talent agent',
    'actor',
    'extra',
    'voice actor'
]

# Function to check if text contains any of the keywords and return the matched keyword
def contains_keywords(text, keywords):
    text_lower = text.lower()
    for keyword in keywords:
        if keyword.lower() in text_lower:
            return True, keyword
    return False, None

# Function to scrape Facebook group posts and save to CSV
def scrape_facebook_group_posts(group_id, pages, cookies=None):
    batch_size = 10  # Number of posts to collect before writing to CSV
    csv_file = f'{group_id}_posts.csv'
    post_list = []

    # Open CSV file for appending
    with open(csv_file, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        # Write CSV header if the file is empty
        if file.tell() == 0:
            writer.writerow(['time', 'matched_keyword', 'text', 'post_url'])

        # Scrape posts
        try:
            for post in get_posts(group=group_id, pages=pages, cookies=cookies, options={"whitelist_methods": ["extract_text", "extract_post_url", "extract_time"]}):
                # Get post details
                text = post.get('text', '')  # Use 'text' instead of 'post_text'
                post_time = post.get('time', '')
                post_url = post.get('post_url', '')

                # Check if post text contains any of the keywords
                matched, keyword = contains_keywords(text, HIRING)
                if matched:
                    found, position_keyword = contains_keywords(text, POSITIONS)
                    if found:
                        # Debug: print post information
                        logging.info(f'Text: {text[:50]}, Time: {post_time}, Matched Keyword: {position_keyword}')

                        # Add post information to list
                        post_list.append([post_time, position_keyword, text, post_url])

                        # Write to CSV in batches
                        if len(post_list) >= batch_size:
                            writer.writerows(post_list)
                            post_list.clear()  # Clear the list after writing

                # To avoid rate limiting, add a delay between requests
                time.sleep(0.5)
        except Exception as e:
            logging.error(f'An error occurred while scraping group {group_id}: {e}')

        # Write any remaining posts to CSV
        if post_list:
            writer.writerows(post_list)

if __name__ == '__main__':
    # Use ThreadPoolExecutor to scrape multiple groups in parallel
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(scrape_facebook_group_posts, group_id, PAGES_TO_SCRAPE, COOKIES_FILE) for group_id in GROUP_IDS]
        
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f'Error in thread: {e}')

    logging.info(f'Successfully scraped {PAGES_TO_SCRAPE} pages of posts from groups {GROUP_IDS}')