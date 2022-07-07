def get_labels_and_values(data:tuple):
    labels = []
    values = []
    for entry in data:
        if type(entry) != tuple:
            return ValueError
        labels.append(entry[0])
        values.append(entry[1])

    return labels,values

def get_card_vs_cash(cards:tuple,cash:tuple):
    combined = (cash[0]),(cards[0])
    labels,values = get_labels_and_values(combined)
    labels = ['cash','card']

    return labels,values