def get_labels_and_values(data: tuple):
    labels = []
    values = []
    for entry in data:
        if type(entry) != tuple:
            return ValueError
        labels.append(entry[0])
        values.append(entry[1])

    return labels, values
