import pandas as pd
import mysql.connector
import re
from unidecode import unidecode
import json
import os


#slug
def slugify(input_str):
    if any(char.isalpha() for char in input_str):
        slug = unidecode(input_str).lower()
        slug = re.sub(r'[^a-z0-9]+', '-', slug)
        slug = slug.strip('-')
    else:
        slug = input_str.lower()
        slug = re.sub(r'[^a-z0-9]+', '-', slug)
        slug = slug.strip('-')
    return slug

#create categories string
def create_categories_string(A, B, C, D):
    result = ""
    
    if A is not None:
        result += A
    else:
        return result
    
    if B is not None:
        result += ", " + B
    else:
        return result
    
    if C is not None:
        result += ", " + C
    else:
        return result
    
    if D is not None:
        result += ", " + D
    else:
        return result
    
    return result

# Kết nối với cơ sở dữ liệu MySQL
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  port="3306",
  password="vuong",
  database="books_db_v1"
)


# Tạo một đối tượng cursor để thực hiện các truy vấn SQL
mycursor = mydb.cursor()

# Đọc dữ liệu từ file Excel vào DataFrame
# df = pd.read_excel('./data/product_info_in_giaokhoathamkhao_lop2_thamkhaolop1_4.xlsx')

# df = pd.read_csv('./data/product_info_in_giaokhoathamkhao_lop2_thamkhaolop1_4.csv', sep=' ')

csv_folder = "./data"
# Lấy danh sách các tệp CSV trong thư mục csv_folder
csv_files = [f for f in os.listdir(csv_folder) if f.endswith('.csv')]

for csv_file in csv_files:
    df = pd.read_csv(os.path.join(csv_folder, csv_file), sep=' ')
    # Lặp qua từng dòng của DataFrame và chèn dữ liệu vào bảng MySQL
    for index, row in df.iterrows():
        # Image
        img = str(row.iat[1]).replace("_x000D_", "").strip()
        # Title
        title = str(row.iat[2]).replace("_x000D_", "").strip()
        # Rating
        rating = str(row.iat[3]).replace("_x000D_", "").strip()
        rating = rating if rating != 'nan' else '0'
        #NumRatings
        num_rating_str = str(row.iat[4]).replace("_x000D_", "").strip()
        num_rating = re.search(r'\d+', num_rating_str).group() if num_rating_str != 'nan' else '0'
        #DataSuplier
        supplier = str(row.iat[5]).replace("_x000D_", "").strip()
        supplier = supplier if supplier != 'nan' else 'null'
        #DataAuthor
        author = str(row.iat[6]).replace("_x000D_", "").strip()
        author = author if author != 'nan' else 'null'
        #DataPublisher
        publisher = str(row.iat[7]).replace("_x000D_", "").strip()
        publisher = publisher if publisher != 'nan' else 'null'
        #DataPublishYear
        publisher_year = str(row.iat[8]).replace("_x000D_", "").strip()
        publisher_year = publisher_year if publisher_year != 'nan' else 'null'
        #DataSize
        size = str(row.iat[9]).replace("_x000D_", "").strip()
        size = size if size != 'nan' else 'null'
        #DataBookLayout
        layout = str(row.iat[10]).replace("_x000D_", "").strip()
        layout = layout if layout != 'nan' else 'null'
        #Des
        des = str(row.iat[11]).replace("_x000D_", "").strip()
        des = des if des != 'nan' else 'null'
        #DataQtyOfPage
        num_pages_str = str(row.iat[12]).replace("_x000D_", "").strip()
        num_pages = int(float(num_pages_str)) if num_pages_str.replace('.', '').isdigit() else 0
        #Cate0
        cate0 = str(row.iat[13]).replace("_x000D_", "").strip()
        cate0 = cate0 if cate0 != 'nan' else 'null'
        #Cate1
        cate1 = str(row.iat[14]).replace("_x000D_", "").strip()
        cate1 = cate1 if cate1 != 'nan' else 'null'
        #Cate2
        cate2 = str(row.iat[15]).replace("_x000D_", "").strip()
        cate2 = cate2 if cate2 != 'nan' else 'null'
        #Cate3
        cate3 = str(row.iat[16]).replace("_x000D_", "").strip()
        cate3 = cate3 if cate3 != 'nan' else 'null'
        #PriceSpecial
        spe_price_str = str(row.iat[17]).replace("_x000D_", "").strip()
        spe_price = float(spe_price_str.replace(" ", "").replace(".", "").replace("đ", ""))
        #PriceOld
        old_price_str = str(row.iat[18]).replace("_x000D_", "").strip()
        old_price = float(old_price_str.replace(" ", "").replace(".", "").replace("đ", ""))

        # print("Image:", img)
        # print("Title:", title)
        # print("Rating:", rating)
        # print("NumRatings:", num_rating)
        # print("DataSupplier:", supplier)
        # print("DataAuthor:", author)
        # print("DataPublisher:", publisher)
        # print("DataPublishYear:", publisher_year)
        # print("DataSize:", size)
        # print("DataBookLayout:", layout)
        # print("Des:", des)
        # print("DataQtyOfPage:", num_pages)
        # print("Cate0:", cate0)
        # print("Cate1:", cate1)
        # print("Cate2:", cate2)
        # print("Cate3:", cate3)
        # print("PriceSpecial:", spe_price)
        # print("PriceOld:", old_price)

        # Tìm Sách
        if title is not None:
            sql_find_book = "SELECT book_id FROM book WHERE book_title = %s LIMIT 1"
            mycursor.execute(sql_find_book, (title,))
            result = mycursor.fetchone()

            # Insert Sách
            book_id = None
            if result is None:
                categoties = []
                # Tìm Category
                if cate0 is not None:
                    sql_cate1 = "SELECT  cate1_id FROM category_1 WHERE cate1_name = %s LIMIT 1"
                    mycursor.execute(sql_cate1, (cate0,))
                    result = mycursor.fetchone()
                    if result is not None:
                        categoties.append(result[0])

                    if cate1 is not None:
                        sql_cate2 = "SELECT cate2_id FROM category_2 WHERE cate2_name = %s LIMIT 1"
                        mycursor.execute(sql_cate2, (cate1,))
                        result = mycursor.fetchone()
                        if result is not None:
                            categoties.append(result[0])

                        if cate2 is not None:
                            sql_cate3 = "SELECT cate3_id FROM category_3 WHERE cate3_name = %s LIMIT 1"
                            mycursor.execute(sql_cate3, (cate2,))
                            result = mycursor.fetchone()
                            if result is not None:
                                categoties.append(result[0])

                            if cate3 is not None:
                                sql_cate4 = "SELECT cate4_id FROM category_4 WHERE cate4_name = %s LIMIT 1"
                                mycursor.execute(sql_cate4, (cate3,))
                                result = mycursor.fetchone()
                                if result is not None:
                                    categoties.append(result[0])

                # Tìm Và Insert Tác Giả
                authors = []
                if author is not None:
                    temp_author = author.split(',')
                    for one_author in temp_author:
                        sql_find_author = "SELECT  author_id FROM author WHERE author_slug = %s LIMIT 1"
                        mycursor.execute(sql_find_author, (slugify(one_author),))
                        result = mycursor.fetchone()
                        if result is not None:
                            authors.append(result[0])

                        #Insert author nếu không tìm thấy
                        if result is None:
                            author_data = {
                                "author_sid": slugify(one_author),
                                "author_name": one_author,
                                "author_img": "",
                                "author_slug": slugify(one_author),
                                "author_des": "",
                            }

                            # Câu lệnh SQL INSERT
                            sql_insert_author = """
                                INSERT INTO author (author_sid, author_name, author_img, author_slug, author_des)
                                VALUES (%(author_sid)s, %(author_name)s, %(author_img)s, %(author_slug)s, %(author_des)s)
                            """
                            mycursor.execute(sql_insert_author, author_data)
                            new_author_id = mycursor.lastrowid
                            authors.append(new_author_id)

                #Tìm Và Insert NXB
                publisher_id = ''
                if publisher is not None:
                    sql_find_publisher = "SELECT pub_id FROM publisher WHERE pub_sid = %s LIMIT 1"
                    mycursor.execute(sql_find_publisher, (slugify(publisher),))
                    result = mycursor.fetchone()

                    if result is None:
                        publisher_data = {
                            "pub_sid": slugify(publisher),
                            "pub_name": publisher,
                            "pub_img": "",
                            "pub_slug": slugify(publisher),
                        }

                        # Câu lệnh SQL INSERT
                        sql_insert_publisher = """
                            INSERT INTO publisher (pub_sid, pub_name, pub_img, pub_slug)
                            VALUES (%(pub_sid)s, %(pub_name)s, %(pub_img)s, %(pub_slug)s)
                        """
                        mycursor.execute(sql_insert_publisher, publisher_data)
                        publisher_id = mycursor.lastrowid
                    else:
                        publisher_id = result[0]

                #Tìm và Insert Subplier
                supplier_id = ''
                if supplier is not None:
                    sql_find_supplier = "SELECT sup_id FROM supplier WHERE sup_sid = %s LIMIT 1"
                    mycursor.execute(sql_find_supplier, (slugify(supplier),))
                    result = mycursor.fetchone()

                    if result is None:
                        supplier_data = {
                            "sup_sid": slugify(supplier),
                            "sup_name": supplier,
                            "sup_img": "",
                            "sup_slug": slugify(supplier),
                        }

                        # Câu lệnh SQL INSERT
                        sql_insert_supplier = """
                            INSERT INTO supplier (sup_sid, sup_name, sup_img, sup_slug)
                            VALUES (%(sup_sid)s, %(sup_name)s, %(sup_img)s, %(sup_slug)s)
                        """
                        mycursor.execute(sql_insert_supplier, supplier_data)
                        supplier_id = mycursor.lastrowid
                    else:
                        supplier_id = result[0]

                #Tìm và Insert Layout
                layout_id = ''
                if layout is not None:
                    sql_find_layout = "SELECT layout_id FROM layout WHERE layout_name = %s LIMIT 1"
                    mycursor.execute(sql_find_layout, (layout,))
                    result = mycursor.fetchone()

                    if result is None:
                        layout_data = {
                            "layout_name": supplier,
                            "layout_slug": slugify(supplier),
                        }

                        # Câu lệnh SQL INSERT
                        sql_insert_layout = """
                            INSERT INTO layout (layout_name, layout_slug)
                            VALUES (%(layout_name)s, %(layout_slug)s)
                        """
                        mycursor.execute(sql_insert_layout, layout_data)
                        layout_id = mycursor.lastrowid
                    else:
                        layout_id = result[0]

                #Insert book
                book_data = {
                    "book_title": title,
                    "book_categories": json.dumps(categoties),
                    "book_authors": json.dumps(authors),
                    "book_publisherId": publisher_id,
                    "book_supplierId": supplier_id,
                    "book_layoutId": layout_id,
                    "book_img": img,
                    "book_avg_rating": rating,
                    "book_num_rating": num_rating,
                    "book_spe_price": spe_price,
                    "book_old_price": old_price,
                    "book_status": 1,
                    "is_deleted": 0,
                    "sort": 0
                }

                sql_insert_book = """
                    INSERT INTO book (book_title, book_categories, book_authors, book_publisherId, book_supplierId, book_layoutId,
                                    book_img, book_avg_rating, book_num_rating, book_spe_price, book_old_price, book_status, 
                                    is_deleted, sort)
                    VALUES (%(book_title)s, %(book_categories)s, %(book_authors)s, %(book_publisherId)s, %(book_supplierId)s, 
                            %(book_layoutId)s, %(book_img)s, %(book_avg_rating)s, %(book_num_rating)s, %(book_spe_price)s, 
                            %(book_old_price)s, %(book_status)s, %(is_deleted)s, %(sort)s)
                """
                mycursor.execute(sql_insert_book, book_data)
                book_id = mycursor.lastrowid
                if book_id is None:
                    break

                #Insert book_detail
                detail_book_data = {
                    "book_id": book_id,
                    "book_categories_name": create_categories_string(cate0, cate1, cate2, cate3),
                    "book_pulisherName": publisher,
                    "book_supplier": supplier,
                    "book_authors_name": author,
                    "book_publish_year": publisher_year,
                    "book_layout": layout,
                    "book_avg_rating": rating,
                    "book_num_ratings": num_rating,
                    "book_num_pages": num_pages,
                    "book_size": size,
                    "book_des": des
                }

                sql_insert_detail_book = """
                    INSERT INTO book_detail (book_id, book_categories_name, book_pulisherName, book_supplier, book_authors_name, book_publish_year,
                                    book_layout, book_avg_rating, book_num_ratings, book_num_pages, book_size, book_des)
                    VALUES (%(book_id)s, %(book_categories_name)s, %(book_pulisherName)s, %(book_supplier)s, %(book_authors_name)s, 
                            %(book_publish_year)s, %(book_layout)s, %(book_avg_rating)s, %(book_num_ratings)s, %(book_num_pages)s, 
                            %(book_size)s, %(book_des)s)
                """

                mycursor.execute(sql_insert_detail_book, detail_book_data)

                #Insert Inventoty
                inventory_data = {
                    "inven_book_id": book_id,
                    "inven_supplierId": supplier_id,
                    "inven_location": "",
                    "inven_stock": 100,
                    "inven_reservations": json.dumps([])
                }

                sql_insert_inventory = """
                    INSERT INTO inventory (inven_book_id, inven_supplierId, inven_location, inven_stock, inven_reservations)
                    VALUES (%(inven_book_id)s, %(inven_supplierId)s, %(inven_location)s, %(inven_stock)s, %(inven_reservations)s)
                """

                mycursor.execute(sql_insert_inventory, inventory_data)

                print(book_id, " :::::: " ,title)
            else:
                print(result[0], " :::::: " ,title)

    print('-------------', csv_file, '-------------')
    # Thực hiện commit để lưu thay đổi
    mydb.commit()

# Đóng kết nối MySQL
mydb.close()

print("Data imported successfully!")
