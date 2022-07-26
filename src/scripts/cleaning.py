def clean_prods_csv(value: str):
    order_split = list(value.split(", "))
    
    ret_value = ''

    for prod in order_split:
        index = order_split.index(prod)
        order_split[index] = order_split[index].replace(' ', ' - ', 1)
        if order_split[index].count(' - ') == 2:
            order_split[index] = order_split[index][:-4]+'None - '+order_split[index][-4:] 

        ret_value+=order_split[index]+', '  
    
    ret_value = ret_value.rstrip(', ') 
    
    return ret_value
    

def clean_card_numbers(value):

    value = str(value)
    new_value = value.rstrip('.0')
    new_number = (new_value[-4:])
    
    return new_number
    
def clean_date_time(value):
    value = str(value)

    value = value.split(' ')
    value[0] = value[0].split('/')
    date = value[0]
    time = value[1]
    date_time = str(f"{date[2]}/{date[1]}/{date[0]} {time}")
    return date_time
