"""
Arquivo que possui funções para

 * Extração dos dados 
 * Criação do esquema 
 * Povoamento das relações. 

"""

import gzip
import json
import psycopg2 as pgadmim
import sys
from tqdm import tqdm
import schema.sql



"""
Conexao do banco ou tentar ne 

"""

def conecta_db():
  con = pgadmim.connect(host='localhost',
                         port= '55432', 
                         database='amazon',
                         user='postgres', 
                         password='postgres')
  return con


# Função para criar tabela no banco

def criar_db(sql):
  con = conecta_db()
  cur = con.cursor()
  cur.execute(schema.sql)
  con.commit()
  con.close()



# Função para inserir dados no banco

def inserir_db(sql):
    con = conecta_db()
    cur = con.cursor()
    try:
        cur.execute(sql)
        con.commit()
    except (Exception, pgadmim.DatabaseError) as error:
        print("Error: %s" % error)
        con.rollback()
        cur.close()
        return 1
    cur.close()


"""
    Extração dos dados utilizando um parse adaptado do repositório

    https://github.com/chezou/amazon-movie-review

    Parser for Amazon product co-purchasing network metadata
 
    https://snap.stanford.edu/data/amazon-meta.html

"""


def get_line_number(file_path):
    sys.stderr.write("Counting line number of {}".format(file_path))

    with gzip.open(file_path, "rb") as file:
        for lines, l in enumerate(file):
            pass

    return lines


def parser(filename, total):
    IGNORE_FIELDS = ["Total items", "reviews"]
    f = gzip.open(filename, "r")
    entry = {}
    categories = []
    reviews = []
    similar_items = []

    for line in tqdm(f, total=total):
        line = line.decode("utf-8").strip()
        colonPos = line.find(":")

        if line.startswith("Id"):
            if reviews:
                entry["reviews"] = reviews
            if categories:
                entry["categories"] = categories

            yield entry
            entry = {}
            categories = []
            reviews = []
            rest = line[colonPos + 2 :]
            entry["id"] = rest.strip()

        elif line.startswith("similar"):
            similar_items = line.split()[2:]
            entry["similar_items"] = similar_items

        elif line.find("cutomer:") != -1:
            review_info = line.split()
            reviews.append(
                {
                    "data": review_info[0],
                    "customer_id": review_info[2],
                    "rating": int(review_info[4]),
                    "votes": int(review_info[6]),
                    "helpful": int(review_info[8]),
                }
            )

        elif line.startswith("|"):
            categories.append(line)

        elif colonPos != -1:
            eName = line[:colonPos]
            rest = line[colonPos + 2 :]

            if not eName in IGNORE_FIELDS:
                entry[eName] = rest.strip()

    if reviews:
        entry["reviews"] = reviews

    if categories:
        entry["categories"] = categories

    yield entry


if __name__ == "__main__":
    file_path = "amazon-meta.txt.gz"
    output_file = "amazon-meta-ex.json"

    import simplejson

    line_num = get_line_number(file_path)

    with open(output_file, "w") as f:

        for e in parser(file_path, total=line_num):
            json.dump(e, f)
            f.write("\n")


'''
This is an example document.

example = """Id:   1
ASIN: 0827229534
  title: Patterns of Preaching: A Sermon Sampler
  group: Book
  salesrank: 396585
  similar: 5  0804215715  156101074X  0687023955  0687074231  082721619X
  categories: 2
   |Books[283155]|Subjects[1000]|Religion & Spirituality[22]|Christianity[12290]|Clergy[12360]|Prea
ching[12368]
   |Books[283155]|Subjects[1000]|Religion & Spirituality[22]|Christianity[12290]|Clergy[12360]|Serm
ons[12370]
  reviews: total: 2  downloaded: 2  avg rating: 5
    2000-7-28  cutomer: A2JW67OY8U6HHK  rating: 5  votes:  10  helpful:   9
    2003-12-14  cutomer: A2VE83MZF98ITY  rating: 5  votes:   6  helpful:   5

Id:   8
ASIN: 0231118597
  title: Losing Matt Shepard
  group: Book
  salesrank: 277409
  similar: 5  B000067D0Y  0375727191  080148605X  1560232579  0300089023
  categories: 4
   |Books[283155]|Subjects[1000]|Gay & Lesbian[301889]|Nonfiction[10703]|General[10716]
   |Books[283155]|Subjects[1000]|Nonfiction[53]|Crime & Criminals[11003]|Criminology[11005]
   |Books[283155]|Subjects[1000]|Nonfiction[53]|Politics[11079]|General[11083]
   |Books[283155]|Subjects[1000]|Nonfiction[53]|Politics[11079]|U.S.[11117]
  reviews: total: 15  downloaded: 15  avg rating: 4.5
    2000-10-31  cutomer: A2F1X6YFCJZ1FH  rating: 5  votes:  10  helpful:   9
    2000-11-1  cutomer: A1OZQCZAK21S6M  rating: 5  votes:  13  helpful:  12
    2000-11-19  cutomer:  AL5D52NA8F67F  rating: 5  votes:  16  helpful:  13
    2001-4-16  cutomer:  AVFBIM1W41IXO  rating: 1  votes:   0  helpful:   0
    2001-5-10  cutomer: A3I6SOXDIE0M8R  rating: 5  votes:   6  helpful:   6
    2001-7-1  cutomer: A3559TE3F9RRNL  rating: 5  votes:   5  helpful:   5
    2001-8-25  cutomer:  ASPUU0H77LFXG  rating: 5  votes:   3  helpful:   3
    2001-9-13  cutomer: A3L902U49A6X5K  rating: 5  votes:   6  helpful:   6
    2001-10-25  cutomer:  AL5OEDM8TPTKV  rating: 5  votes:  10  helpful:  10
    2001-11-3  cutomer: A1R64WON03GTN4  rating: 5  votes:   1  helpful:   1
    2001-12-24  cutomer: A2WKESDGF2YC8S  rating: 5  votes:   8  helpful:   8
    2001-12-31  cutomer:  A71P2O8OMF8GY  rating: 5  votes:   7  helpful:   5
    2002-4-9  cutomer:  AB8HLDYSDI5M7  rating: 4  votes:  10  helpful:   4
    2002-9-23  cutomer: A37FDCXZLI0MAC  rating: 2  votes:  12  helpful:   5
    2003-9-3  cutomer:  AQE41QO3NEUMW  rating: 5  votes:   3  helpful:   2
"""

'''




"""
Funções para povoar o banco de dados

* Funções de insert 

"""

def read_jsonl(file_path):
    with open(file_path, "r") as f:
        return [json.loads(line) for line in f]

def generate_insert_table_product(item):
    
    avg_rating = int(round(
        sum([review.get("rating") for review in item.get("reviews", [])]) / 
        len(item.get("reviews", [1]))
        , 0
        ))

    columns = "(id, id_asin, title, salesrank, group, similar_totals, categories_totals, review_totals, avg_rating)"
    values = "("\
    f"{item.get('id')}, "\
    f"\'{item.get('ASIN')}\', "\
    f"\'{item.get('title')}\', "\
    f"{item.get('salesrank')}, "\
    f"\'{item.get('group')}\', "\
    f"{len(item.get('similar_totals', []))}, "\
    f"{len(item.get('categories_totals', []))}, "\
    f"{len(item.get('review_totals', []))}, "\
    f"{ avg_rating}"\
    ")"

    insert_value = f"INSERT INTO product {columns} VALUES {values}"
    
    return insert_value


def generate_insert_table_review(item):

    insert_reviews = []
    for review in item.get('reviews', []):
        columns = "(product_id, data, costumer_id, rating, vote, helpful)"
        values = "("\
        f"{item.get('id')}, "\
        f"\'{review.get('data',)}\', "\
        f"\'{review.get('customer_id')}\', "\
        f"{review.get('rating')}, "\
        f"{review.get('votes')}, "\
        f"{review.get('helpful')}, "\
        ")"

        insert_value = f"INSERT INTO review {columns} VALUES {values}"
        insert_reviews.append(insert_value)

    return insert_reviews


def generate_insert_table_similar(item):
    insert_similars= []
    for similar in item.get('similar_items', []):
        columns = "(product_id, item)"
        values = "("\
        f"{item.get('id')}, "\
        f"\'{similar}\'"\
        ")"
        

        insert_value = f"INSERT INTO similar {columns} VALUES {values}"
        insert_similars.append(insert_value)

    return insert_similars


def generate_insert_table_category(item):

    columns = "(product_id, name)"
    values = "("\
    f"{item.get('id')}, "\
    f"\'{item.get('categories',[])}\', "\
    ")"

    insert_value = f"INSERT INTO category {columns} VALUES {values}"

    return insert_value




"""
Chamada de funções

"""


if __name__ == '__main__':

    # file_path = "amazon-meta-ex.json"
    file_path = "amazon-meta-amostra.json" #utilizada apenas para teste
    dados_temp = read_jsonl(file_path)
    values_table_product =[]
    values_table_review = []
    values_table_similar = []
    values_table_category = []

    for item in dados_temp:
        values_table_product.append(generate_insert_table_product(item))
        values_table_review.extend(generate_insert_table_review(item))
        values_table_similar.extend(generate_insert_table_similar(item))
        values_table_category.append(generate_insert_table_category(item))



    
# Cria arquivos para análise das funcções de insert

    # output_file_product = "insert_table_product.txt"
    # with open(output_file_product, "w") as f:
    #     for value in values_table_product:
    #         f.write(value)
    #         f.write("\n")

    
    # output_file_review = "insert_table_review.txt"
    # with open(output_file_review, "w") as f:
    #     for value in values_table_review:
    #         f.write(value)
    #         f.write("\n")



    # output_file_similar = "insert_table_similars.txt"
    # with open(output_file_similar, "w") as f:
    #         for value in values_table_similar:
    #             f.write(str(value))
    #             f.write("\n")

    
    # output_file_category = "insert_table_category.txt"
    # with open(output_file_category, "w") as f:
    #         for value in values_table_category:
    #             f.write(value)
    #             f.write("\n")   claver