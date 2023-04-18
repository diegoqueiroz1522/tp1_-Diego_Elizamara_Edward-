import psycopg2
from psycopg2.extras import execute_values
import re 


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
                PRIMARY KEY (id),
                UNIQUE (id_asin)
            );


            CREATE TABLE review
            (
                product_id INT,
                data DATE,
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
                id_category INT,
                FOREIGN KEY (product_id) REFERENCES product(id)
            );

            CREATE TABLE similar_itens
            (
            product_id INT REFERENCES product(id),
            id_item VARCHAR(10)
            );
"""
c.execute(sql)

loc_arquivo = '../amazon-meta.txt'
arquivo = open(loc_arquivo)

list_product=[]
list_review=[]
list_category=[]
list_similar=[]

list_categorias=[]

reviews = False
enviar= False
cont = 0



group=''
salesrank = ''

for x in arquivo:

    if 'Id:   ' in x:
        y = x.replace('\n', '')
        id = y.split()[-1]

    elif 'ASIN: ' in x:
        y = x.replace('\n', '')
        asin = y.split()[-1]

    elif '  salesrank: ' in x:
        y = x.replace('\n', '')
        salesrank= y.split()[-1]
                 
    elif '  title: ' in x:
        y = x.replace('\n', '') 
        
        inicio = y.find('  title: ')
        fim = len('  title: ')
        title = y[inicio+fim:]
 
    elif '  group: ' in x:
        y = x.replace('\n', '') 
        inicio = y.find('  group: ')
        fim = len('  group: ')
        group = y[inicio+fim:] 
        
    if group and salesrank:
        list_product.append((id,asin, title, salesrank, group))

        group = ''
        salesrank = ''
      
    query= "INSERT INTO product (id, id_asin, title, salesrank, grupo) VALUES %s;"

    execute_values(c, query, list_product)
    del list_product
    list_product=[]

arquivo = open(loc_arquivo)
for x in arquivo:
    if reviews:
        if x != '\n':
            linha_review = x
            linha_review = linha_review.replace('    ', '')
            linha_review = linha_review.replace('\n', '')
            linha_review = linha_review.replace('  cutomer: ', ' ')
            linha_review = linha_review.replace('  rating: ', ' ')
            linha_review = linha_review.replace('  votes:   ', ' ')
            linha_review = linha_review.replace('  votes:  ', ' ')
            linha_review = linha_review.replace('  votes: ', ' ')
            linha_review = linha_review.replace('  helpful:   ', ' ')
            linha_review = linha_review.replace('  helpful:  ', ' ')
            linha_review = linha_review.replace('  helpful: ', ' ')


            linha_review = linha_review.split()
            data = linha_review[0]
            customer = linha_review[1]
            rating = linha_review[2]
            votes = linha_review[3]
            helpful = linha_review[4]
        else:
            reviews=False
            enviar=True

    elif 'Id:   ' in x:
        y = x.replace('\n', '')
        id = y.split()[-1]
        
        
    elif '  similar: ' in x:
        y = x.replace('\n', '')
        inicio = y.find('  similar: ')
        fim = len('  similar: ')
        similar = y[inicio+fim:].split('  ')[1:]
        
    elif '  reviews: ' in x:
        reviews = True

    elif '   |' in x:
        padrao = r"[(\d+)]"
        categoria = re.findall(padrao, x)
        a = x.split('|')[1:]
        for k in range(len(a)):
            a[k] = a[k].replace(']', '')
            a[k] = a[k].split('[')

            id_categoria = a[k][1]
            if tuple((categoria, id_categoria)) not in list_categorias:
                list_categorias.append((id, categoria, id_categoria))
                

    cont+=1 
    if enviar:        
        for s in similar:
            list_similar.append((id,s))
        
        list_review.append((id, data, customer, rating, votes, helpful)) 
        enviar= False
        
    
    if cont > 5000:

        query= "INSERT INTO similar_itens (product_id, id_item) VALUES %s;"

        execute_values(c, query, list_similar)
        del list_similar
        list_similar=[]

        query= "INSERT INTO category (product_id, name, id_category) VALUES %s;"

        execute_values(c, query, list_categorias)
        del list_categorias
        list_categorias=[]


        query= "INSERT INTO review (product_id, data, costumer_id, rating, vote, helpful) VALUES %s;"

        execute_values(c, query, list_review)
        del list_review
        list_review=[]

        cont = 0


query= "INSERT INTO similar_itens (product_id, id_item) VALUES %s;"

execute_values(c, query, list_similar)
del list_similar


query= "INSERT INTO category (product_id, name, id_category) VALUES %s;"

execute_values(c, query, list_categorias)
del list_categorias


query= "INSERT INTO review (product_id, data, costumer, rating, vote, helpful) VALUES %s;"

execute_values(c, query, list_review)
del list_review

print("Finalizado")
