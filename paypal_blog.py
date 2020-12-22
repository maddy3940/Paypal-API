import requests
import simplejson as json
import pyodbc
from datetime import datetime,timezone, timedelta
import azure.functions as func
import logging

# Connect to the database 
CONN_STR= 'Driver={ODBC Driver 17 for SQL Server};''Server=XXXXXX;''Database=paypal;''Trusted_Connection=yes;'
balance_affecting_records_only="Y"	#Default value is Y. Change it to N to get non-balance affecting transactions
span=1	# Keep this value between 1 and 31. Eg. Span = 1 will give you entries from start_date till start_date + 1 
page_size=500  # Max value is 500. 

# Get the maximum date of the transaction that was last updated and hence our start_date variable will be this value + 1.
def Max_transaction_update_date():
    db=pyodbc.connect(CONN_STR)
    cursor=db.cursor()
    sql='''SELECT MAX (transaction_updated_date) FROM paypal_transaction_info'''
    cursor.execute(sql)
    db_max_date=str(cursor.fetchone())
    #(datetime.datetime(2020, 9, 30, 21, 39, 6), ) - Format of fetched date from database
    cursor.close()
    db.close()
    db_max_date=formatting_date(db_max_date)	#To embed the dates in url for the api call we need to format the date
    return(db_max_date)

# Format the date string obtained from the database
def formatting_date(db_max_date):
	# (datetime.datetime(2020, 9, 30, 21, 39, 6), ) - Before formatting
    db_max_date=db_max_date[19:-4].split(',')
    # after the above step we get ['2020', ' 9', ' 30', ' 21', ' 39', ' 6']

    # So what happens is if the time that was returned from the database is has 0 in seconds place then it does not return 0.
    # In above example the seconds value was 6. But if it was 0 the the above returned value would have looked like this: (datetime.datetime(2020, 9, 30, 21, 39) --> ['2020', ' 9', ' 30', ' 21', ' 39']
    # Seconds positon is not present. So to handle that we check the length and append a " 00" in such case. Same thing happens when min or hrs is 0 
    if len(db_max_date)==5:
        db_max_date.append(" 00")
    elif len(db_max_date)==4:
        db_max_date.append(" 00")
        db_max_date.append(" 00")
    elif len(db_max_date)==3:
        db_max_date.append(" 00")
        db_max_date.append(" 00")
        db_max_date.append(" 00")
    elif len(db_max_date)==1:
		db_max_date=["2020"," 11"," 01"," 00"," 00"," 00"] 	# If database is empty. Assign a default value to the start_date in this list

    db_max_date=''.join([str(elem) for elem in db_max_date])
    # 2020 9 30 21 39 6 - eg of result after the above step

    y,m,d,h,mi,s=db_max_date.split(' ')		#split into year, month etc
    db_max_date=datetime(int(y),int(m),int(d),int(h),int(mi),int(s))		# convert to date type format
    db_max_date=db_max_date+timedelta(1) # We need entries from the next day 12AM uptil 11:59pm of end_date.

    db_max_date=db_max_date.strftime("%Y-%m-%dT00:00:00-0000") 	#convert to the type required in the url like this- 2020-10-28T00:50:47-0000

    return db_max_date

# Insert into paypal_transaction_info
def InsertTransaction(paypal_account_id,transaction_id,paypal_reference_id,paypal_reference_id_type,transaction_event_code,transaction_initiation_date,transaction_updated_date,transaction_amount_currency,transaction_amount_value,fee_amount,insurance_amount,shipping_amount,shipping_discount_amount,transaction_status,transaction_note,payment_tracking_id,bank_reference_id,transaction_subject,ending_balance,available_balance,invoice_id,custom_field,protection_eligibility,payer_account_id,email_address,address_status,payer_status,payer_name,country_code,shipping_info_name,shipping_info_address,item_details,shipping_info_method,shipping_info_sec_address):
    db=pyodbc.connect(CONN_STR)
    cursor=db.cursor()
    sql='''INSERT INTO paypal.dbo.paypal_transaction_info (paypal_account_id,transaction_id,paypal_reference_id,paypal_reference_id_type,transaction_event_code,transaction_initiation_date,transaction_updated_date,transaction_amount_currency,transaction_amount_value,fee_amount,insurance_amount,shipping_amount,shipping_discount_amount,transaction_status,bank_reference_id,transaction_subject,ending_balance,available_balance,invoice_id,custom_field,protection_eligibility,payer_account_id,email_address,address_status,payer_status,payer_name,country_code,shipping_info_name,shipping_info_address,item_details,shipping_info_method,shipping_info_sec_address,transaction_note,payment_tracking_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''

    cursor.execute(sql,(paypal_account_id,transaction_id,paypal_reference_id,paypal_reference_id_type,transaction_event_code,transaction_initiation_date,transaction_updated_date,transaction_amount_currency,transaction_amount_value,fee_amount,insurance_amount,shipping_amount,shipping_discount_amount,transaction_status,bank_reference_id,transaction_subject,ending_balance,available_balance,invoice_id,custom_field,protection_eligibility,payer_account_id,email_address,address_status,payer_status,payer_name,country_code,shipping_info_name,shipping_info_address,item_details,shipping_info_method,shipping_info_sec_address,transaction_note,payment_tracking_id))
    db.commit()
    cursor.close()
    db.close()

# Insert the cart_info for each transaction
def InsertTransaction_cart(transaction_id,transaction_updated_date,item_name,item_description,item_quantity,item_unit_price,item_amount_value,tax_percentage,invoice_id):
    db=pyodbc.connect(CONN_STR)
    cursor=db.cursor()
    sql='''INSERT INTO paypal.dbo.paypal_cart(transaction_id,transaction_updated_date,item_name,item_description,item_quantity,item_unit_price,item_amount_value,tax_percentage,invoice_id) VALUES(?,?,?,?,?,?,?,?,?)'''
    cursor.execute(sql,(transaction_id,transaction_updated_date,item_name,item_description,item_quantity,item_unit_price,item_amount_value,tax_percentage,invoice_id))
    db.commit()
    cursor.close()
    db.close()

#Insert count of entries received in a single call from start_date to end_date
def Insert_count(start_date,end_date,count):
    db=pyodbc.connect(CONN_STR)
    cursor=db.cursor()
    sql='''INSERT INTO paypal.dbo.paypal_daily_count (start_date,end_date,count) VALUES(?,?,?)'''
    cursor.execute(sql,(start_date,end_date,count))
    db.commit()
    cursor.close()
    db.close()

# Calculate end_date by adding span to start_date
def min_end_date(start_date):
    start_date=start_date[:-5]
    dt = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%S")
    end_date=dt+timedelta(span)
    end_date=end_date.strftime("%Y-%m-%dT23:59:59-0000")
    return(end_date)


# This main function is required if you want to run your code on Azure functions.
def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.utcnow().replace(
        tzinfo=timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

#Delete the lines above and remove the indentation for the code below if you are not running this code on azure functions app

    start_date=Max_transaction_update_date()	# Get the date of latest entry in the database. We need entries after that date 
    end_date=min_end_date(start_date)			# Add span to start date


    url = "https://api.paypal.com/v1/reporting/transactions?start_date="+start_date+"&end_date="+end_date+"&page=1&page_size="+str(page_size)+"&fields=all&balance_affecting_records_only="+balance_affecting_records_only
    payload='Content-Type=application/json'
    #Paste the access token in the header if you already have it else it will place a call for token later in this code
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    result= json.loads(response.text)

    # If the token is invalid then place a new token request
    if result.get('error',"0")=="invalid_token":
        payload='Content-Type=application/json'
        headers = {
            'Authorization': 'Basic XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX',	# Your credentials should be a Base64 encoded string. Get this using Postman. 
            																			# Under authorization tab paste your client id and secret key and click on code button to get the code in different languages
            																			
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        url = "https://api.paypal.com/v1/oauth2/token?grant_type=client_credentials"
        response = requests.request("POST", url, headers=headers, data=payload)
        result= json.loads(response.text)
        
        token=result["access_token"]
        #print(token) # If running locally print the token and plug it above to reuse the same token until it is expired
        url = "https://api.paypal.com/v1/reporting/transactions?start_date="+start_date+"&end_date="+end_date+"&page=1&page_size="+str(page_size)+"&fields=all&balance_affecting_records_only="+balance_affecting_records_only
        
        payload='Content-Type=application/json'
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer '+token
        }
        response = requests.request("GET", url, headers=headers, data=payload)

        result= json.loads(response.text)	# Formats the text response into json.

    total_items=result["total_items"]	# Get the total items in the response. Maximum value is 10,000. If its greater than that then it will raise an error. 
    									# If error pops up then reduce the span of start_date to end_date. 

    Insert_count(start_date[:-5],end_date[:-5],total_items)	# Insert start_date, end_date and count of entries that we got between those dates in the database 

    #Insert transaction in transaction table and each item_detail in a single transaction in another table using transaction_updated_date and transaction_id as primary key
    for transaction in result['transaction_details']:
        
        # In the response many times some of the keys can be skipped if a particular transaction has no value for that field in paypal's records.
        # In that case to avoid errors we use "get" function of dictionary to return empty dictionary or a default value 
        
        # The hierarchy is like this-
        #transaction_details(array) -> transaction_detail(object) -> cart_info(object) -> item_details(array)-> item_detail(object)

        cart= transaction.get('cart_info',{})	

        payer=transaction.get('payer_info',{})


        shipping=transaction.get('shipping_info',{})

        
        transaction=transaction.get('transaction_info',{})
        

        transaction_id=transaction.get('transaction_id',None)
        transaction_initiation_date=str(transaction.get('transaction_initiation_date',"2020-01-01T00:00:0000000"))[:-5]
        
        #transaction_info
        transaction_updated_date=str(transaction.get('transaction_updated_date',"2020-01-01T00:00:0000000"))[:-5]
        transaction_amount=transaction.get('transaction_amount',{})
        fee_amount=transaction.get('fee_amount',{})
        insurance_amount=transaction.get('insurance_amount',{})
        shipping_amount=transaction.get('shipping_amount',{})
        shipping_discount_amount=transaction.get('shipping_discount_amount',{})
        
        ending_balance=transaction.get('ending_balance',{})
        available_balance=transaction.get('available_balance',{})

        #payer_info
        payer_name=payer.get('payer_name',{})
        
        #shipping info
        shipping_address=shipping.get('address',{})
        secondary_shipping_address=shipping.get('secondary_shipping_address',{})

        item_details=cart.get('item_details',[])
        item_quantity=0

        # For each transaction there are many items associated with it. So parse the item_details array
        for item_detail in item_details:
            
            item_unit_price=item_detail.get('item_unit_price',{})
            item_amount=item_detail.get('item_amount',{})
            discount_amount=item_detail.get('discount_amount',{})
            adjustment_amount=item_detail.get('adjustment_amount',{})
            gift_wrap_amount=item_detail.get('gift_wrap_amount',{})
            basic_shipping_amount=item_detail.get('basic_shipping_amount',{})
            extra_shipping_amount=item_detail.get('extra_shipping_amount',{})
            handling_amount=item_detail.get('handling_amount',{})
            insurance_amount=item_detail.get('insurance_amount',{})
            total_item_amount=item_detail.get('total_item_amount',{})
            item_quantity+=1
            # Insert the item_detail in cart table. transaction_id and transaction_updated_date are the primary key 
            InsertTransaction_cart(transaction_id,transaction_updated_date,item_detail.get('item_name',None),item_detail.get('item_description',None),item_detail.get('item_quantity',None),item_unit_price.get('value',None),item_amount.get('value',None),item_detail.get('tax_percentage',None),item_detail.get('invoice_number',None))

        # Insert transaction_info                         
        InsertTransaction(transaction.get("paypal_account_id",None),transaction_id,transaction.get("paypal_reference_id",None),transaction.get("paypal_reference_id_type",None),transaction.get("transaction_event_code",None),transaction_initiation_date,transaction_updated_date,transaction_amount.get("currency_code",None),transaction_amount.get("value",None),fee_amount.get("value",None),insurance_amount.get("value",None),shipping_amount.get("value",None),shipping_discount_amount.get("value",None),transaction.get("transaction_status",None),transaction.get("transaction_note",None),transaction.get("payment_tracking_id",None),transaction.get("bank_reference_id",None),transaction.get("transaction_subject",None),ending_balance.get("value",None),available_balance.get("value",None),transaction.get("invoice_id",None),transaction.get("custom_field",None),transaction.get("protection_eligibility",None),payer.get("account_id",None),payer.get("email_address",None),payer.get("address_status",None),payer.get("payer_status",None),payer_name.get("alternate_full_name",None),payer.get("country_code",None),shipping.get("name",None),shipping_address.get("line1","")+shipping_address.get("line2","")+shipping_address.get("city","")+shipping_address.get("state","")+shipping_address.get("country_code","")+shipping_address.get("postal_code",""),item_quantity,shipping.get("method",None),secondary_shipping_address.get("line1","")+secondary_shipping_address.get("line2","")+secondary_shipping_address.get("city","")+secondary_shipping_address.get("state","")+secondary_shipping_address.get("country_code","")+secondary_shipping_address.get("postal_code",""))

    #Pagination
    # A single call contains maximum of 500 entries and contain links to the next page in the header section. Maximum pages is 20
    # We automatically iterate through all the pages using the links response in each call.

    total_pages=result["total_pages"]
    page=result["page"]
    links=result["links"]

    if total_pages>1:
        while(page!=total_pages):
            for link in links:
                if link["rel"]=="next":
                    url=link["href"]
                    break
            response = requests.request("GET", url, headers=headers, data=payload)

            result= json.loads(response.text)

            for transaction in result['transaction_details']:
                        
                cart= transaction.get('cart_info',{})

                payer=transaction.get('payer_info',{})
            

                shipping=transaction.get('shipping_info',{})
                
                
                transaction=transaction.get('transaction_info',{})
                
                transaction_id=transaction.get('transaction_id',None)
                transaction_initiation_date=str(transaction.get('transaction_initiation_date',"2020-01-01T00:00:0000000"))[:-5]
                
                #transaction_info
                transaction_updated_date=str(transaction.get('transaction_updated_date',"2020-01-01T00:00:0000000"))[:-5]
                transaction_amount=transaction.get('transaction_amount',{})
                fee_amount=transaction.get('fee_amount',{})
                insurance_amount=transaction.get('insurance_amount',{})
                shipping_amount=transaction.get('shipping_amount',{})
                shipping_discount_amount=transaction.get('shipping_discount_amount',{})
                
                ending_balance=transaction.get('ending_balance',{})
                available_balance=transaction.get('available_balance',{})

                #payer_info
                payer_name=payer.get('payer_name',{})
                
                #shipping info
                shipping_address=shipping.get('address',{})
                secondary_shipping_address=shipping.get('secondary_shipping_address',{})

                item_details=cart.get('item_details',[])#for loop
                
                item_quantity=0
                for item_detail in item_details:
                
                    item_unit_price=item_detail.get('item_unit_price',{})
                    item_amount=item_detail.get('item_amount',{})
                    discount_amount=item_detail.get('discount_amount',{})
                    adjustment_amount=item_detail.get('adjustment_amount',{})
                    gift_wrap_amount=item_detail.get('gift_wrap_amount',{})
                    basic_shipping_amount=item_detail.get('basic_shipping_amount',{})
                    extra_shipping_amount=item_detail.get('extra_shipping_amount',{})
                    handling_amount=item_detail.get('handling_amount',{})
                    insurance_amount=item_detail.get('insurance_amount',{})
                    total_item_amount=item_detail.get('total_item_amount',{})
                    item_quantity+=1
                    InsertTransaction_cart(transaction_id,transaction_updated_date,item_detail.get('item_name',None),item_detail.get('item_description',None),item_detail.get('item_quantity',None),item_unit_price.get('value',None),item_amount.get('value',None),item_detail.get('tax_percentage',None),item_detail.get('invoice_number',None))

                                
                InsertTransaction(transaction.get("paypal_account_id",None),transaction_id,transaction.get("paypal_reference_id",None),transaction.get("paypal_reference_id_type",None),transaction.get("transaction_event_code",None),transaction_initiation_date,transaction_updated_date,transaction_amount.get("currency_code",None),transaction_amount.get("value",None),fee_amount.get("value",None),insurance_amount.get("value",None),shipping_amount.get("value",None),shipping_discount_amount.get("value",None),transaction.get("transaction_status",None),transaction.get("transaction_note",None),transaction.get("payment_tracking_id",None),transaction.get("bank_reference_id",None),transaction.get("transaction_subject",None),ending_balance.get("value",None),available_balance.get("value",None),transaction.get("invoice_id",None),transaction.get("custom_field",None),transaction.get("protection_eligibility",None),payer.get("account_id",None),payer.get("email_address",None),payer.get("address_status",None),payer.get("payer_status",None),payer_name.get("alternate_full_name",None),payer.get("country_code",None),shipping.get("name",None),shipping_address.get("line1","")+shipping_address.get("line2","")+shipping_address.get("city","")+shipping_address.get("state","")+shipping_address.get("country_code","")+shipping_address.get("postal_code",""),item_quantity,shipping.get("method",None),secondary_shipping_address.get("line1","")+secondary_shipping_address.get("line2","")+secondary_shipping_address.get("city","")+secondary_shipping_address.get("state","")+secondary_shipping_address.get("country_code","")+secondary_shipping_address.get("postal_code",""))

            page=result["page"]
            links=result["links"]
        





