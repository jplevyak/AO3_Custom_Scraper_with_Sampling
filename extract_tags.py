
import csv

def extract_tags(input_csv, output_txt):
    tags = set()
    
    try:
        with open(input_csv, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            
            # Check if 'Tags' column exists
            if 'Tags' not in reader.fieldnames:
                print(f"Error: 'Tags' column not found in {input_csv}.")
                return

            for row in reader:
                tag_string = row['Tags']
                if tag_string:
                    # Tags are sparated by semi-colons
                    row_tags = tag_string.split(';')
                    for tag in row_tags:
                        clean_tag = tag.strip()
                        if clean_tag:
                            tags.add(clean_tag)
                            
    except FileNotFoundError:
        print(f"Error: File {input_csv} not found.")
        return
    except Exception as e:
        print(f"An error occurred: {e}")
        return

    sorted_tags = sorted(list(tags))
    
    try:
        with open(output_txt, 'w', encoding='utf-8') as outfile:
            for tag in sorted_tags:
                outfile.write(tag + '\n')
        print(f"Successfully extracted {len(sorted_tags)} unique tags to {output_txt}.")
        
    except Exception as e:
        print(f"Error writing to {output_txt}: {e}")

if __name__ == "__main__":
    extract_tags('scraped_works.csv', 'tags.txt')
