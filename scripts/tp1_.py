import psycopg2

con = psycopg2.connect(
    host = 'localhost',
    port = '5432',
    user = 'postgres',
    password = 'postgres'
)

con.autocommit = True
c = con.cursor()

sql = """DROP TABLE IF EXISTS product, review, category, similar_itens;

            CREATE TABLE product
            (
                id INT,
                id_asin VARCHAR(50),
                title VARCHAR(1000),
                salesrank INT,
                grupo VARCHAR(15),
                similar_totals INT,
                categories_totals INT,
                review_totals INT,
                avg_rating INT, 
                PRIMARY KEY (id),
                UNIQUE (id_asin)
            );


            CREATE TABLE review
            (
                id SERIAL,
                product_id INT,
                data VARCHAR(15),
                costumer_id VARCHAR(30),
                rating INT,
                vote INT,
                helpful INT,
                FOREIGN KEY (product_id) REFERENCES product(id)
            );

            CREATE TABLE category
            (
                product_id INT,
                name TEXT,
                FOREIGN KEY (product_id) REFERENCES product(id)
            );

            CREATE TABLE similar_itens
            (
            product_id INT,
            item VARCHAR(30),
            FOREIGN KEY (product_id) REFERENCES product(id)
            );
"""
c.execute(sql)

loc_arquivo = '../teste.txt'
arquivo = open(loc_arquivo)

for x in arquivo:
    if 'Id:   ' in x:
        y = x.replace('\n', '')
        id = y.split()[-1]
        print(id)
    if 'ASIN: ' in x:
        y = x.replace('\n', '')
        asin = y.split()[-1]
        print(asin)        
    if '  title: ' in x:
        y = x.replace('\n', '') 
        
        inicio = y.find('  title: ')
        fim = len('  title: ')
        title = y[inicio+fim:]
        print(title)  
    if '  group: ' in x:
        y = x.replace('\n', '') 
        inicio = y.find('  group: ')
        fim = len('  group: ')
        group = y[inicio+fim:] 
        print(group) 
    if '  similar: ' in x:
        y = x.replace('\n', '')
        inicio = y.find('  similar: ')
        fim = len('  similar: ')
        similar = y[inicio+fim:].split('  ')[1:]
        print(similar)
    if '  reviews: ' in x:
        reviews =     


                  