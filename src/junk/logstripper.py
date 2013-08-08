#!/usr/bin/python

import sys

"""
Renders target fields empty and outputs log
Stripped fields:
request_uri
user_agent
auction_id
google_verticals
domain
ip
tag_format
no_flash
within_iframe
google_verticals_slicer
is_test
useragent
user_language
referrer_domain
google_main_vertical
adnexus_inventory_class
google_adslot_id
"""
usage = "cat logfile | logstripper.py log_type  Ex: zcat rtb_bid_all.txt.gz | logstripper bid_all"

if len(sys.argv)!=2:
    print usage
    sys.exit(1)
    
no_bid_fields = set([1,2,3,4,6,8,28,19,25.94,26,31,101,85,22,29,37,27])
bid_all_fields = set([1,2,3,4,6,8,28,19,25.94,26,31,101,85,22,29,37,27])
imp_fields = set([1,2,3,4,6,10,30,21,27,96,28,33,103,87,24,31,39,29])
click_fields = set([1,2,3,4,6,10,30,21,27,96,28,33,103,87,24,31,39,29])
conversion_fields = set([1,2,3,4,6,10,30,21,27,96,28,33,103,87,24,31,39,29])
pixel_fields = set([])

log_type = sys.argv[1]

if log_type == 'no_bid':
    target_set = no_bid_fields
elif log_type == 'bid_all':
    target_set = bid_all_fields
elif log_type == 'imp':
    target_set = imp_fields
elif log_type == 'click':
    target_set = click_fields
elif log_type == 'conversion':
    target_set = conversion_fields
elif log_type == 'pixel':
    target_set = pixel_fields
else:
    print usage
    sys.exit(1)

for line in sys.stdin:
    elems = line.strip('\n').split('\t')
    
    for index in target_set:
        elems[index]=''
    
    # Yield stripped fields
    outstring = '\t'.join(elems)
    print outstring
   
