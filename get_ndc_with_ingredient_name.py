import requests
import json
from datetime import datetime
import time
import pandas as pd
from tqdm import tqdm

def safe_get_json(url):
    """Make a request with no retries and without error messages"""
    try:
        response = requests.get(url)
        # Check if the response text is empty or contains only whitespace
        if not response.text.strip():
            return {}
        return response.json()
    except (requests.RequestException, json.JSONDecodeError):
        # Silently handle errors - no error message printed
        return {}

def timestamp():
    """Return current timestamp string"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def print_status(message):
    """Print a timestamped status message"""
    print(f"[{timestamp()}] {message}")

# List of ingredients to process
ingredients = [
    'Exenatide', 'Liraglutide', 'Semaglutide', 'Dulaglutide', 'Lixisenatide', 'Albiglutide', 'Tirzepatide'
]

# Define term types for drug products expected to have NDC codes
drug_product_term_types = ["SCD", "SBD", "BPCK", "GPCK"]

# Initialize a list to collect the results
results = []
total_rxcui_count = 0
total_ndc_count = 0

# print_status(f"Starting NDC retrieval for {len(ingredients)} ingredients")

# Iterate over each ingredient with progress tracking
for i, ingredient in enumerate(ingredients, 1):
    print_status(f"[{i}/{len(ingredients)}] Processing ingredient: {ingredient}")
    
    # Step 1: Retrieve the initial RxCUI for the ingredient
    initial_url = f"https://rxnav.nlm.nih.gov/REST/rxcui.json?name={ingredient}"
    print_status(f"  Fetching initial RxCUI from {initial_url}")
    initial_data = safe_get_json(initial_url)
    initial_rxcui_list = initial_data.get('idGroup', {}).get('rxnormId', [])
    
    if not initial_rxcui_list:
        print_status(f"  No RxCUI found for ingredient '{ingredient}'")
        continue

    print_status(f"  Found {len(initial_rxcui_list)} initial RxCUI(s)")
    
    # Dictionary to collect unique related RxCUI values with their description (rxcui description)
    related_rxcui_dict = {}
    
    # Step 2: For each initial RxCUI, get all related RxCUI values
    for rxcui_index, rxcui in enumerate(initial_rxcui_list):
        related_url = f"https://rxnav.nlm.nih.gov/REST/rxcui/{rxcui}/allrelated.json"
        print_status(f"  [{rxcui_index+1}/{len(initial_rxcui_list)}] Getting related RxCUIs for {rxcui}")
        related_data = safe_get_json(related_url)
        concept_groups = related_data.get('allRelatedGroup', {}).get('conceptGroup', [])
        
        rxcui_count = 0
        for group in concept_groups:
            concept_properties = group.get('conceptProperties', [])
            for cp in concept_properties:
                # Filter by term type to only include drug product types
                if cp.get('tty') in drug_product_term_types:
                    related_id = cp.get('rxcui')
                    name = cp.get('name')
                    if related_id and related_id not in related_rxcui_dict:
                        related_rxcui_dict[related_id] = name
                        rxcui_count += 1
        
        print_status(f"  Found {rxcui_count} related RxCUIs of valid types")
        time.sleep(0.1)  # Small delay to be respectful to the API

    # Step 3: Retrieve NDC codes for each filtered related RxCUI and get NDC properties
    total_rxcui_count += len(related_rxcui_dict)
    print_status(f"  Processing {len(related_rxcui_dict)} RxCUIs to find NDCs")
    
    for i, (rxcui, rxcui_description) in enumerate(tqdm(related_rxcui_dict.items(), desc=f"  {ingredient} RxCUIs")):
        ndc_url = f"https://rxnav.nlm.nih.gov/REST/rxcui/{rxcui}/ndcs.json"
        ndc_data = safe_get_json(ndc_url)
        ndc_list = ndc_data.get('ndcGroup', {}).get('ndcList', {}).get('ndc', [])
        
        # Only process if there are valid NDC codes
        if ndc_list:
            total_ndc_count += len(ndc_list)
            for ndc in ndc_list:
                ndc_prop_url = f"https://rxnav.nlm.nih.gov/REST/ndcproperties.json?ndc={ndc}"
                ndc_prop_data = safe_get_json(ndc_prop_url)
                ndc_description = None
                ndc_property = ndc_prop_data.get('ndcPropertyGroup', {}).get('ndcProperty', None)
                if isinstance(ndc_property, list) and ndc_property:
                    ndc_description = ndc_property[0].get('name')
                elif isinstance(ndc_property, dict):
                    ndc_description = ndc_property.get('name')
                
                results.append({
                    'ingredient': ingredient,
                    'rxcui': rxcui,  # Added rxcui column 
                    'ndc': ndc,
                    'rxcui description': rxcui_description,
                    'ndc description': ndc_description
                })
        else:
            results.append({
                'ingredient': ingredient,
                'rxcui': rxcui,  # Added rxcui column
                'ndc': None,
                'rxcui description': rxcui_description,
                'ndc description': None
            })
                
        # Be nice to the API
        if i % 10 == 0:
            time.sleep(0.2)

    print_status(f"  Completed processing for {ingredient}, found {len(ndc_list) if ndc_list else 0} NDCs")

    # Save intermediate results after each ingredient
    temp_df = pd.DataFrame(results)
    print_status(f"  Saved intermediate results ({len(results)} rows so far)")

# Step 4: Create a pandas DataFrame from the results and remove rows with missing NDC values
print_status(f"Creating final DataFrame from {len(results)} results")
df = pd.DataFrame(results)
