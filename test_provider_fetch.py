#!/usr/bin/env python3
""" Test script to prove we can fetch provider names 
from Dispatcharr Run this to see what provider names 
Dispatcharr has """ import json import sys from 
api_utils import DispatcharrAPI def 
test_provider_fetch():
    """Fetch and display provider names from 
    Dispatcharr"""
    
    print("="*70) print("TESTING PROVIDER NAME FETCH 
    FROM DISPATCHARR") print("="*70) print()
    
    # Connect to Dispatcharr
    print("Step 1: Connecting to Dispatcharr...") 
    try:
        api = DispatcharrAPI() api.login() print("✓ 
        Connected successfully\n")
    except Exception as e: print(f"✗ Failed to 
        connect: {e}") sys.exit(1)
    
    # Fetch M3U accounts (providers)
    print("Step 2: Fetching M3U accounts from 
    /api/channels/m3u-accounts/...") try:
        # Use the raw endpoint to see exactly what 
        # we get
        response = 
        api.get_raw('/api/channels/m3u-accounts/')
        
        print(f"✓ Response status: 
        {response.status_code}") print(f"✓ 
        Content-Type: 
        {response.headers.get('Content-Type')}\n")
        
        # Parse JSON
        data = response.json()
        
    except Exception as e: print(f"✗ Failed to 
        fetch: {e}") sys.exit(1)
    
    # Extract provider data
    print("Step 3: Extracting provider names...\n")
    
    # Handle both direct list and paginated response
    if isinstance(data, dict) and 'results' in data: 
        providers = data['results'] print(f"ℹ️ 
        Paginated response with {len(providers)} 
        providers")
    elif isinstance(data, list): providers = data 
        print(f"ℹ️ Direct list with {len(providers)} 
        providers")
    else: print(f"✗ Unexpected response format: 
        {type(data)}") print(f"Response: {data}") 
        sys.exit(1)
    
    print() print("="*70) print("PROVIDER NAMES 
    FOUND IN DISPATCHARR:") print("="*70) print()
    
    if not providers: print("⚠️ No providers found!") 
        print(" This might mean:") print(" - No M3U 
        accounts are configured in Dispatcharr") 
        print(" - The endpoint URL is wrong") 
        print(" - Permissions issue")
    else:
        # Display each provider
        provider_map = {}
        
        for provider in providers: provider_id = 
            provider.get('id') provider_name = 
            provider.get('name', 'Unknown')
            
            print(f" ID {provider_id:>3}: 
            {provider_name}")
            
            if provider_id and provider_name: 
                provider_map[str(provider_id)] = 
                provider_name
        
        print() print("="*70) print(f"✓ Successfully 
        fetched {len(provider_map)} provider 
        names!") print("="*70) print()
        
        # Show what would be saved to 
        # provider_map.json
        print("This would be saved to 
        provider_map.json:") print() 
        print(json.dumps(provider_map, indent=2)) 
        print()
        
        # Save it for real
        save = input("Save this to 
        provider_map.json? (y/n): ").strip().lower() 
        if save == 'y':
            with open('provider_map.json', 'w') as 
            f:
                json.dump(provider_map, f, indent=2) 
            print("✓ Saved to provider_map.json")
        else: print("Not saved")
    
    print() print("="*70) print("TEST COMPLETE") 
    print("="*70)
if __name__ == '__main__':
    test_provider_fetch()
