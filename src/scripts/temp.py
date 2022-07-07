def test_func():
    return "hello world"
def test(connection,mylist):
    cursor = connection.cursor()
    #mylist = [[1,2,3],[1,1,1],[1,1,1]]
    sql = 'insert into products (size, name, price) values (%s,%s,%s)'
    for y in mylist: 
        for x in y:
            values = 
            cursor.execute



   
