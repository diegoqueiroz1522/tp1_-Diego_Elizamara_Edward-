#Fazer as consultas
#a) Dado um produto, listar os 5 comentários mais úteis e com maior avaliação e os 5 comentários mais úteis e com menor avaliação (feita)
cursor.execute("""
(SELECT * 
FROM Review
WHERE product_id = 'X.id'
ORDER BY helpful DESC, rating DESC
LIMIT 5) UNION (SELECT * 
                FROM Review 
                WHERE product_id = 'X.id'
                ORDER BY helpful DESC, rating ASC
                LIMIT 5)
                ;""")
# X é o produto dado
# X.id é o id do produto

#b) Dado um produto, listar os produtos similares com maiores vendas do que ele (feita)
cursor.execute("""
SELECT *
FROM Product
WHERE product_id <> 'X.id' AND salesrank > (SELECT salesrank FROM Product WHERE product_id = 'X.id')
ORDER BY salesrank DESC
;""")
# ## X é a row do produto dado
# ## X.salesrank é o rank de vendas do produto dado.
# ## X.id é o id do produto dado.

#c) Dado um produto, mostrar a evolução diária das médias de avaliação ao longo do intervalo de tempo coberto no arquivo de entrada (feito)
cursor.execute("""
SELECT data, avg(rating)
FROM Review
WHERE product_id = 'X.id'
GROUP BY data
ORDER BY data ASC
;""")

# X.id é o id do produto dado

#d) Listar os 10 produtos líderes de venda em cada grupo de produtos (feita)
cursor.execute("""
SELECT p.group, p.id, MAX(p.salesrank) AS max_salesrank
FROM Product p
WHERE p.salesrank IS NOT NULL
GROUP BY p.group, p.id
HAVING MAX(p.salesrank) >= ALL (
  SELECT p2.salesrank
  FROM Product p2
  WHERE p2.group = p.group AND p2.salesrank IS NOT NULL
  ORDER BY p2.salesrank DESC
  LIMIT 10
)
ORDER BY p.group, max_salesrank DESC
;""")

#e) Listar os 10 produtos com a maior média de avaliações úteis positivas por produto (feita)
cursor.execute("""
SELECT *
FROM Product NATURAL JOIN (SELECT id, avg(rating), avg(helpful)
                            FROM Review
                            GROUP BY id
                            ORDER BY avg(rating) DESC, avg(helpful) DESC
                            LIMIT 10)
                            ;""")

#f) Listar a 5 categorias de produto com a maior média de avaliações úteis positivas por produto (feita)
cursor.execute("""
SELECT name.category, avg(rating), avg(helpful)
FROM Category NATURAL JOIN Review
GROUP BY categoria
ORDER BY avg(rating) DESC, avg(helpful) DESC
LIMIT 5;""")

#g) Listar os 10 clientes que mais fizeram comentários por grupo de produto (feita)
cursor.execute("""
SELECT p.group, r.Customer_Id, COUNT(*) AS num_reviews
FROM Product p
INNER JOIN Review r ON p.Id = r.Product_Id
GROUP BY p.Group, r.Customer_Id
ORDER BY p.Group ASC, num_reviews DESC
LIMIT 10;""")

