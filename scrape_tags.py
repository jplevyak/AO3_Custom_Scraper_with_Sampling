import configparser
import csv
import os
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import quote
from tqdm import tqdm

def load_config():
    config = configparser.ConfigParser(interpolation=None)
    if not os.path.exists('scrape_tags.ini'):
        print("Error: scrape_tags.ini file not found.")
        return None
    try:
        config.read('scrape_tags.ini')
        return config['Settings']
    except Exception as e:
        print(f"Error: Could not read the INI file. {e}")
        return None

def canonicalize_tag(tag):
    # Canonicalize tag for AO3 URL
    # AO3 substitutes specific characters with *code*
    # See: https://github.com/otwcode/otwarchive/blob/master/lib/tag_formatter.rb (conceptually)
    # Common known substitutions:
    # / -> *s*
    # & -> *a*
    # . -> *d*
    # ? -> *q*
    # # -> Removed (seems to be the case for hashtags like #thangyuxmas2025)
    
    tag = tag.replace("#", "")
    tag = tag.replace("/", "*s*")
    tag = tag.replace("&", "*a*")
    tag = tag.replace(".", "*d*")
    tag = tag.replace("?", "*q*")
    return tag

def get_tag_url(tag):
    # AO3 tag URLs use %20 for spaces and other URL encoding
    # First, canonicalize specific characters
    canonical_tag = canonicalize_tag(tag)
    # Then URL encode
    encoded_tag = quote(canonical_tag, safe='')
    return f"https://archiveofourown.org/tags/{encoded_tag}"

def scrape_tag_page(session, tag, delay, user_agent):
    url = get_tag_url(tag)
    headers = {'User-Agent': user_agent}
    
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            response = session.get(url, headers=headers, timeout=30)
            
            # Rate limiting handling
            if response.status_code == 429:
                print(f"Rate limit exceeded for {tag}. Waiting 60 seconds...")
                time.sleep(60)
                continue # Retry
            
            # 522 Connection Timed Out
            if response.status_code == 522:
                print(f"522 Connection Timed Out for {tag}. Retrying ({retry_count + 1}/{max_retries})...")
                retry_count += 1
                time.sleep(delay * 2) # Backoff a bit
                continue

            time.sleep(delay)
            
            if response.status_code != 200:
                print(f"Failed to fetch {url}. Status code: {response.status_code}")
                # For 404s or other 4xx/5xx that are not retriable/specific, we might just fail?
                # But let's stick to the current logic: return None
                return None

            soup = BeautifulSoup(response.content, 'html.parser')
        
            # Extract Parent tags
            parent_tags = []
            parent_tags_heading = soup.find('h3', class_='heading', string='Parent tags (more general):')
            if parent_tags_heading:
                parent_ul = parent_tags_heading.find_next_sibling('ul', class_='tags commas index group')
                if parent_ul:
                    for li in parent_ul.find_all('li'):
                        a_tag = li.find('a', class_='tag')
                        if a_tag:
                            parent_tags.append(a_tag.text)

            # Extract Synonyms (Tags with the same meaning)
            synonym_tags = []
            same_meaning_tags_heading = soup.find('h3', class_='heading', string='Tags with the same meaning:')
            if same_meaning_tags_heading:
                same_meaning_ul = same_meaning_tags_heading.find_next_sibling('ul', class_='tags commas index group')
                if same_meaning_ul:
                    for li in same_meaning_ul.find_all('li'):
                        a_tag = li.find('a', class_='tag')
                        if a_tag:
                            synonym_tags.append(a_tag.text)
                            
            # Extract Sub tags (Child tags)
            sub_tags = []
            sub_tags_heading = soup.find('h3', class_='heading', string='Sub tags:')
            if sub_tags_heading:
                 sub_ul = sub_tags_heading.find_next_sibling('ul', class_='tags commas index group')
                 if sub_ul:
                    for li in sub_ul.find_all('li'):
                        a_tag = li.find('a', class_='tag')
                        if a_tag:
                            sub_tags.append(a_tag.text)
            
            return {
                'Tag Name': tag,
                'URL': url,
                'Parent Tags': '; '.join(parent_tags),
                'Synonym Tags': '; '.join(synonym_tags),
                'Sub Tags': '; '.join(sub_tags)
            }

        except requests.exceptions.Timeout:
            print(f"Request timed out for {tag}. Retrying ({retry_count + 1}/{max_retries})...")
            retry_count += 1
            time.sleep(delay)
        except requests.exceptions.ConnectionError:
            print(f"Connection error for {tag}. Retrying ({retry_count + 1}/{max_retries})...")
            retry_count += 1
            time.sleep(delay)
        except Exception as e:
            print(f"Error scraping {tag}: {e}")
            return None
            
    print(f"Failed to scrape {tag} after {max_retries} retries.")
    return None

def main():
    print("Initializing Tag Scraper...")
    config = load_config()
    if config is None:
        return

    input_file = config.get('input_file', 'tags.txt')
    output_file = config.get('output_file', 'scraped_tags.csv')
    delay = int(config.get('delay', 5))
    user_agent = config.get('user_agent', 'AO3 Tag Scraper Bot')

    if not os.path.exists(input_file):
        print(f"Error: Input file {input_file} not found.")
        return

    tags_to_scrape = []
    with open(input_file, 'r', encoding='utf-8') as f:
        tags_to_scrape = [line.strip() for line in f if line.strip()]

    print(f"Found {len(tags_to_scrape)} tags to scrape.")

    session = requests.Session()
    
    # Check for existing CSV to append or write header
    file_exists = os.path.exists(output_file)
    mode = 'a' if file_exists else 'w'
    
    with open(output_file, mode, newline='', encoding='utf-8') as csvfile:
        fieldnames = ['Tag Name', 'URL', 'Parent Tags', 'Synonym Tags', 'Sub Tags']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        if not file_exists:
            writer.writeheader()

        # We need to filter out tags that are already in the CSV if we are appending?
        # For simplicity, assuming simple run or manual management for now, similar to main.py logic if adapted strictly
        # But main.py checks seen_work_ids. Let's implementing a simple "seen_tags" check.
        
        seen_tags = set()
        if file_exists:
             with open(output_file, 'r', encoding='utf-8') as f_read:
                reader = csv.DictReader(f_read)
                for row in reader:
                    seen_tags.add(row['Tag Name'])

        tags_to_process = [t for t in tags_to_scrape if t not in seen_tags]
        print(f"Skipping {len(seen_tags)} already scraped tags. {len(tags_to_process)} remaining.")

        for tag in tqdm(tags_to_process, desc="Scraping Tags"):
            data = scrape_tag_page(session, tag, delay, user_agent)
            if data:
                writer.writerow(data)
                csvfile.flush() # Ensure data is written
            else:
                with open('scrape_tags_failed.txt', 'a', encoding='utf-8') as f_failed:
                    f_failed.write(tag + '\n')
            
    print("Done.")

if __name__ == "__main__":
    main()
