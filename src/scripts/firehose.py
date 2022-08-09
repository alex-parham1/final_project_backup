import json

def clean_date_time(value):
    value = str(value)

    value = value.split(" ")
    value[0] = value[0].split("/")
    date = value[0]
    time = value[1]
    date_time = str(f"{date[2]}-{date[1]}-{date[0]} {time}")
    return date_time

# -- cards customers and stores
def quick_cards_cust_store(card,customer,store):
    card_dict = {'card_number':str(card)[-4:]}
    customers_dict = {'name':str(customer)}
    store_dict = {'name':str(store)}
    return card_dict,customers_dict,store_dict

# -- products cleaning --
def clean_prods(p_order):
    order = p_order.replace('"','')
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
    return {'name':names,'flavour':flavours,'size':size,'price':price}

# -- transactions cleaning --
def clean_transactions(date,name,store,total,method,clean_date_time=clean_date_time):
    trans_dict = {'date_time': date,'customer':name,'store':store,'total':total,'payment_method':method}
    trans_dict['date_time'] = clean_date_time(trans_dict['date_time'])
    return trans_dict

#--- main ---
def main_clean(payload):
    products = clean_prods(payload['order'])
    card,customer,store = quick_cards_cust_store(payload['card_number'],payload['customer_name'],payload['store'])
    transactions = clean_transactions(payload['date'],payload['customer_name'],payload['store'],payload['payment_amount'],payload['payment_type'])
    return {'transactions': transactions, 'products':products, 'card':card, 'customer':customer,'store':store }






