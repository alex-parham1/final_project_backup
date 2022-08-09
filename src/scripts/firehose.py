import json

def clean_date_time(value):
    value = str(value)

    value = value.split(" ")
    value[0] = value[0].split("/")
    date = value[0]
    time = value[1]
    date_time = str(f"{date[2]}-{date[1]}-{date[0]} {time}")
    return date_time

payload =  {'date': '07/08/2022 09:00', 'store': 'Uppingham', 'customer_name': 'John Shuler', 'order': '\[{"name": "Flavoured latte - Caramel", "size": "Large", "price": 2.85}, {"name": "Mocha", "size": "Large", "price": 2.7}]', 'payment_amount': 5.55, 'payment_type': 'CARD', 'card_number': 3399226887056164}

# -- expected results -- 

expected_cards = {'card_number':'6164'} #good to go 
expected_customers = {'name':'John Shuler'} #good to go
expected_product = {'name':['Flavoured latte','Mocha'],'flavour':['Caramel','None'],'size':['Large','Large'],'price':['2.85','2.7']}
expected_store = {'name':'Uppingham'}
expected_transaction = {'date_time':"2022-08-07 09:00",'customer':'John Shuler','store':'Uppingham','total':5.55, 'payment_method':'CARD'}

# -- cards customers and stores

card_dict = {'card_number':str(payload['card_number'])[-4:]}
customers_dict = {'name':str(payload['customer_name'])}
store_dict = {'name':str(payload['store'])}

# -- products cleaning --

products_dict = {'name':[],'flavour':[],'size':[],'price':[]}
order = payload['order']
order = order.replace('"','')
order = order.lstrip("'[{").rstrip("}]'").split('}, {')

for o in order:
    ind = order.index(o)
    order[ind] = o.split(', ')
    for r in order[ind]:
        dex = order[ind].index(r)
        order[ind][dex] = r.split(': ')

for a in order:
    ind = order.index(a)
    if ' - ' not in a[0][1]:
        a[0][1] += ' - None'

    order[ind][0][1] = a[0][1].split(' - ')

names = [order[0][0][1][0],order[1][0][1][0]]
flavours = [order[0][0][1][1],order[1][0][1][1]]
size = [order[0][1][1],order[1][1][1]]
price = [order[0][2][1],order[1][2][1]]
products = {'name':names,'flavour':flavours,'size':size,'price':price}

# -- transactions cleaning --

trans_dict = {'date_time': payload['date'],'customer':payload['customer_name'],'store':payload['store'],'total':payload['payment_amount'],'payment_method':payload['payment_type']}
trans_dict['date_time'] = clean_date_time(trans_dict['date_time'])

# -- quick tests --

if card_dict == expected_cards:
    print('cards correct')

if customers_dict == expected_customers:
    print('customers correct')

if store_dict == expected_store:
    print('stores correct')

if products == expected_product:
    print('products correct')

if trans_dict == expected_transaction:
    print('transactions correct')






