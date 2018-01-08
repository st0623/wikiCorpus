import sqlite3
import subprocess as sp
import os
import sys
import re

def firstrow_2_list(cursor):
    list_data = []
    for row in cursor:
        list_data.append(row[0])

    return list_data


def execute_sql(sql_statement):
    conn = sqlite3.connect('jawiki-20170801-pages-articles.db.sqlite')
    c = conn.cursor()
    sql_data = c.execute(sql_statement).fetchall()
    return firstrow_2_list(sql_data)


def parse_sqlite(cursor):
    category_add_quotes = ["\'" + str(s) + "\'" for s in cursor]
    category_separate_comma = ','.join(category_add_quotes)
    return category_separate_comma


def devide_category_title(cursor, title, categories, done_category):
    category_add_array = [s.replace("'","''") for s in cursor]

    title.extend(filter (lambda s: s.find("CATEGORY:"), category_add_array))
    done_category.extend(categories)
    category = list(filter (lambda s: not s.find("CATEGORY:"), category_add_array))
    category = list(map(lambda s: s.replace('CATEGORY:',''),category))

    return category


def make_dirs_delete_file(dir_name):
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)

    if os.path.exists(dir_name + "category.dat"):
        os.remove(dir_name + "category.dat")

    if os.path.exists(dir_name + "titles.dat"):
        os.remove(dir_name + "titles.dat")


def main():
    MAX_TITLE_NUM = sys.maxsize


    if len(sys.argv) < 2:
        print ("input category")
        print ("ex: get_category_articles.py 将棋")
        sys.exit()

    if len(sys.argv) >= 3:
        MAX_TITLE_NUM = int(sys.argv[2])

    CATEGORY_NAME = sys.argv[1]
    SAVE_DIR = "wikiData/" + CATEGORY_NAME + "/"

    make_dirs_delete_file(SAVE_DIR)

    category = [ CATEGORY_NAME ]
    title = []
    done_category = []

    hierarchy = 0

    while len(category) > 0 and len(title) < MAX_TITLE_NUM:
        print ("--- hierarchy: " + str(hierarchy) + ", title num: " + str(len(title)) + "--------")
        print (category)

        category_id = execute_sql("select id from categories where title in (%s)" % parse_sqlite(category))
        page_id = execute_sql("select page_id from page_categories where category_id in (%s)" % parse_sqlite(category_id))
        page_title = execute_sql("select title from pages where id in (%s)" % parse_sqlite(page_id))
        category = devide_category_title(page_title, title, category, done_category)
        done_category = list(set(done_category))
        category = list( set(category).difference(set(done_category)))
        title = list (set(title))
        hierarchy += 1

    print ("extract articles ... title num = " + str(len(title))  )

    get_article_sql = "select content from articles where title in (?)"
    articles = execute_sql("select content from articles where title in (%s)" % parse_sqlite(title))

    articles = list(map (lambda s: "".join(s.split("\n")[1:]), articles))
    with open(SAVE_DIR + "articles.dat", "w") as f:
        f.write("\n".join(articles))

    

if __name__ == '__main__':
    main()
