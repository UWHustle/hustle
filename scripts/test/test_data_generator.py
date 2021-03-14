# This is a simple Python script to generate table data for Hustle SQL tests
import csv
import random

TEST_DATA_PATH = "../../test/data/"


def generate_test_data():
    with open(TEST_DATA_PATH + 'lineorder.tbl', 'w', newline='') as file:
        writer = csv.writer(file, delimiter='|')
        for x in range(0, 200, 5):
            for p in range(100):
                print("generate")
                writer.writerow([x, x, p, p, p, 1993121, random.randint(0, 30), random.randint(
                    0, 1000),  random.randint(0, 1000), random.randint(0, 5)])
                writer.writerow([x + 1, x, p, p, p, 1993121, random.randint(0, 30),
                                 random.randint(0, 1000),  random.randint(0, 1000), random.randint(0, 5)])
                writer.writerow([x + 2, x, p, p, p, 1994024, random.randint(25, 30),
                                 random.randint(0, 1000), random.randint(0, 1000), random.randint(3, 7)])
                writer.writerow([x + 3, x, p, p, p, 1994034, random.randint(25, 30),
                                 random.randint(0, 1000),  random.randint(0, 1000), random.randint(3, 7)])
                writer.writerow([x + 4, x, p, p, p, 1994035, random.randint(25, 30),
                                 random.randint(0, 1000),  random.randint(0, 1000), random.randint(4, 7)])

    with open(TEST_DATA_PATH + 'part.tbl', 'w', newline='') as file:
        writer = csv.writer(file, delimiter='|')
        for p in range(100):
            writer.writerow(
                [p, "MFGR#" + str(p), "MFGR#" + str(p), "MFGR#" + str(p)])

    with open(TEST_DATA_PATH + 'supplier.tbl', 'w', newline='') as file:
        writer = csv.writer(file, delimiter='|')
        for p in range(100):
            writer.writerow(
                [p, "SCITY" + str(p), "SNATION" + str(p), "SREGION" + str(p)])

    with open(TEST_DATA_PATH + 'customer.tbl', 'w', newline='') as file:
        writer = csv.writer(file, delimiter='|')
        for p in range(100):
            writer.writerow(
                [p, "CCITY" + str(p), "CNATION" + str(p), "CREGION" + str(p)])

    with open(TEST_DATA_PATH + 'date.tbl', 'w', newline='') as file:
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
