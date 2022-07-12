import pandas as pd


def test_func():
    return "hello world"


# # each product consists of three values: size, name and price
# # this function separates each product from an transaction and makes it into a list, to be stored in another list
# def seperate_products(products_df):
#     rule = [1, 2, 3]
#     products = []
#     total = int(len(products_df) / 3)
#     for value in range(total):
#         buffer = []
#         for num in rule:
#             # add the first three entries to the buffer, popping each after they are used (three items each time)
#             buffer.append(products_df[0])
#             products_df.pop(0)
#         # append to prodcuts list, rinse an repeat until all productsin transaction are stored
#         products.append(buffer)
#     return products


# def seperate_products_new(products_df):
#     new_split = []
#     for prod in products_df:
#         new_split.append(prod.split(" - "))

#     for prod in new_split:
#         if len(prod) == 2:
#             index = new_split.index(prod)
#             new_split[index].insert(1, "None")

#     new_split_df = pd.DataFrame(new_split, columns=["product", "flavour", "price"])
#     new_split_df[["size", "name"]] = new_split_df["product"].str.split(
#         " ", n=1, expand=True
#     )

#     new_split_df = new_split_df.drop(columns=["product"])
#     new_split_df = new_split_df[["size", "name", "flavour", "price"]]

#     return new_split_df.values.tolist()


# # ----------
# # rule = [1, 2, 3]
# # products = []
# # total = int(len(products_df) / 3)
# # for value in range(total):
# #     buffer = []
# #     for num in rule:
# #         # add the first three entries to the buffer, popping each after they are used (three items each time)
# #         buffer.append(products_df[0])
# #         products_df.pop(0)
# #     # append to prodcuts list, rinse an repeat until all productsin transaction are stored
# #     products.append(buffer)
# # return products

# # "[[Large Speciality Tea , Green , 1.60] , [Regular Glass of milk , n/a , 0.70],[ Large Glass of milk , 1.10], [Regular Flavoured iced latte , Caramel , 2.75]]"

# test_list = [
#     "Large Speciality Tea - Green - 1.60",
#     "Regular Glass of milk - 0.70",
#     "Large Glass of milk - 1.10",
#     "Regular Flavoured iced latte - Caramel - 2.75",
# ]
# print(seperate_products_new(test_list))


def seperate_products(products_df):
    new_split = []
    for prod in products_df:
        print(prod)
        prod = prod.strip()
        print(prod)
        new_split.append(prod.split(" - "))

    for prod in new_split:
        if len(prod) == 2:
            index = new_split.index(prod)
            new_split[index].insert(1, "None")

    new_split_df = pd.DataFrame(new_split, columns=["product", "flavour", "price"])
    new_split_df[["size", "name"]] = new_split_df["product"].str.split(
        " ", n=1, expand=True
    )

    new_split_df = new_split_df.drop(columns=["product"])
    new_split_df = new_split_df[["size", "name", "flavour", "price"]]

    return new_split_df.values.tolist()


my_list = [
    "Regular Mocha - 2.30",
    " Regular Flavoured iced latte - Caramel - 2.75",
    " Regular Cortado - 2.05",
    " Large Flavoured iced latte - Caramel - 3.25",
    " Large Flavoured iced latte - Caramel - 3.25",
]
print(seperate_products(my_list))
