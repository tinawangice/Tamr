# Tamr


## Introduction

Find distinct suppliers from 4 million transaction records from USA 2016 spend dataset. 

Generate analysis from aggregation of distinct supplier.

For details of the project, please refer to: https://docs.google.com/document/d/1KTgcyUJ201Zr1GMGGQCWiO8uNTUyDhFoPTEK9hiaKh0/edit?usp=sharing


## Explanation of Codes

### main.py
* The main.


### data_wrangler.py

* Columns are selected as:  recipient_duns, recipient_name, recipient_parent_name, recipient_parent_duns, 
recipient_country_code, recipient_address_line_1, recipient_city_name, recipient_state_code, recipient_zip_4_code, 
recipient_phone_number, recipient_fax_number

* Use selected columns to create tuples, then write distinct tuples into dedupped_csv_file.csv

* Multi-processing is used to speed up deal with 5 seperate *.csv files.


### company_alias_grouper.py

* Records from dedupped_csv_file.csv are further classified into distinct companies. Companies considered as 
confidently/ not confidently the same are put in one "CompanyAliasGroup" (defined class), with match level remarked.

* Detailed rules to decide match level:
    1. if two companies' parent duns are not the same, return NOT_SURE

    2. if two companies' duns ids are the same, return CONFIDENT (we're confident that they're considered as the same company)
    
    3. if two companies' names are the same, return CONFIDENT
    
    4. if two companies' zip 4 codes are the same, return CONFIDENT.

    5. if two companies' names "fuzzy match", return INCONFIDENT (they're very likely to be the same company).
        for fuzzy matching, we use Rosette-api. https://developer.rosette.com/features-and-functions#introduction
        we can actually use other better proprietary apis for real enterprise use.

    6. else return NOT_SURE. They may be, may not be the same, but we don't know

* Multi-processing is used to speed up classifying "CompanyAliasGroup", 
noticing that only records with same parent_duns will be in one "CompanyAliasGroup". 

* 2 json files from confident-matching and fuzzy-matching are generated, example of 1 item in json:
 
      { uuid : {2: [[information of not confident aliases],[...]],
                    3: [[information of confident aliases],[...],[...]]}
         
         
### fuzzy_matching.py

* provide 2 fuzzy matching method:
    1.  fuzzy_match_with_rosette. Rosette(https://www.rosette.com/capability/name-matching/) is popular and mature commercial service 
  to caculate similarity between 2 names, but only offers 1,000 free comparasion. It "blends machine learning with traditional name matching techniques such as name lists, common key, 
  and rules to determine a match score".
  
    2.  fuzzy_match_with_fuzzywuzzy. Fuzzywuzzy is a python package, using "Levenshtein Distance to calculate the differences".


### classify_all_original_records.py

* To keep or not confident matching by choosing to compare against json file: inconfident_company_aliases/ confident_company_aliases.

* confident_company_aliases is arbitrarily selected, new csv file named "final.csv" is generated with
the column "group_id" added to original csv file.


### write_2_db.py

* A temperory server on AWS is used. (data will not be availble after 20-Apr-2018)

* Multi-threading and "add many in mysql" technics are used to speed up the storing.


## Some results: 

For those companies that are deemed as the “same company” on confident match level, we group them together and assign a group_id. 

I parsed and stored the original csv file into a MySQL DB, appending to each row a group_id corresponding to the company in this row.

### Example 1 : 

Find how many distinct suppliers with/without method of Confident Matching for each parent award agency. And the reduction for  parent award agencies with highest occurrences is found around 1% ~ 7%. 

![image](https://github.com/tinawangice/Tamr/blob/master/images/147101523943975_.pic_hd.jpg)

### Example 2 : 

Find number of companies under a parent company. As shown below, we can see Government of the United States are classified into 68 companies, even though it contains 150 distinct company duns numbers, 91 distinct company names and 161 zip_4_codes.

![image](https://github.com/tinawangice/Tamr/blob/master/images/147091523943972_.pic_hd.jpg)


### Example 3: 

Choose supplier “Government of the United States” to visualize its transaction map in United States. We can see it has most transactions in Kentucky and Texas.

![image](https://github.com/tinawangice/Tamr/blob/master/images/147111523943977_.pic_hd.jpg)












