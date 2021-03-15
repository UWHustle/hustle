# This is a simple Python script to generate table data for Hustle SQL tests
import csv
import random
import os


def generate_test_data():
    random.seed(30)
    try:
        os.remove('lineorder.tbl')
    except OSError as error:
        pass
    with open('lineorder.tbl', 'wb') as file:
        writer = csv.writer(file, delimiter='|')
        line_key = 0
        for x in range(0, 200, 5):
            for p in range(100):
                writer.writerow([line_key, line_key, p, p, p, 1993121, random.randint(0, 30), random.randint(
                    0, 1000),  random.randint(0, 1000), random.randint(0, 5)])
                writer.writerow([line_key + 1, line_key, p, p, p, 1993121, random.randint(0, 30),
                                 random.randint(0, 1000),  random.randint(0, 1000), random.randint(0, 5)])
                writer.writerow([line_key + 2, line_key, p, p, p, 1994024, random.randint(25, 30),
                                 random.randint(0, 1000), random.randint(0, 1000), random.randint(3, 7)])
                writer.writerow([line_key + 3, line_key, p, p, p, 1994034, random.randint(25, 30),
                                 random.randint(0, 1000),  random.randint(0, 1000), random.randint(3, 7)])
                writer.writerow([line_key + 4, line_key, p, p, p, 1994035, random.randint(25, 30),
                                 random.randint(0, 1000),  random.randint(0, 1000), random.randint(4, 7)])
                line_key = line_key + 5
    try:
        os.remove('part.tbl')
    except OSError as error:
        pass
    with open('part.tbl', 'wb') as file:
        writer = csv.writer(file, delimiter='|')
        for p in range(100):
            writer.writerow(
                [p, "MFGR#" + str(p), "MFGR#" + str(p), "MFGR#" + str(p)])
    try:
        os.remove('supplier.tbl')
    except OSError as error:
        pass
    with open('supplier.tbl', 'wb') as file:
        writer = csv.writer(file, delimiter='|')
        for p in range(100):
            writer.writerow(
                [p, "SCITY" + str(p), "SNATION" + str(p), "SREGION" + str(p)])
    try:
        os.remove('customer.tbl')
    except OSError as error:
        pass
    with open('customer.tbl', 'wb') as file:
        writer = csv.writer(file, delimiter='|')
        for p in range(100):
            writer.writerow(
                [p, "CCITY" + str(p), "CNATION" + str(p), "CREGION" + str(p)])

    try:
        os.remove('date.tbl')
    except OSError as error:
        pass
    with open('date.tbl', 'wb') as file:
        writer = csv.writer(file, delimiter='|')
        for p in range(0, 30, 1):
            writer.writerow([int("199201"+str(p)), p % 7, p %
                             12, 1992,  p % 7, 199201, "Jan1992"])
            writer.writerow([int("199312"+str(p)), p % 7, p %
                             12, 1993,  p % 7, 199312, "Dec1993"])
            writer.writerow([int("199402"+str(p)), p % 7, p %
                             12, 1994,  6, 199402, "Feb1992"])
            writer.writerow([int("199403"+str(p)), p % 7, p %
                             12, 1994,  6, 199403, "Mar1992"])


if __name__ == '__main__':
    generate_test_data()
